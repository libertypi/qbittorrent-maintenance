import pickle
import shutil
import sys
from collections import Counter
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path
from re import compile as re_compile
from typing import Iterable, Iterator, Mapping, Sequence, Tuple
from urllib.parse import urljoin

# Beautifulsoup and ortools are imported as needed
import pandas as pd
import requests
from jenkspy import jenks_breaks
from torrentool.api import Torrent as TorrentParser
from torrentool.exceptions import TorrentoolException

_debug: bool = False
NOW = pd.Timestamp.now()

byteSize: Mapping[str, int] = {
    k: v for kk, v in zip(((f"{c}B", f"{c}iB") for c in "KMGTP"), (1024 ** i for i in range(1, 6))) for k in kk
}


@dataclass
class Removable:
    """Removable torrents from qBittorrent list."""

    hash: str
    size: int
    peer: int
    title: str
    state: str
    weighted: bool


@dataclass
class Torrent:
    """Torrents to be downloaded from web."""

    id: str
    size: int
    peer: int
    link: str
    expire: str
    title: str
    hash: str = None


class Logger:
    """Record and write logs in reversed order."""

    __slots__ = "_log"

    def __init__(self) -> None:
        self._log = []

    def __str__(self) -> str:
        return "{:17}    {:8}    {:>11}    {}\n{:->80}\n{}".format(
            "Date", "Action", "Size", "Name", "", "".join(reversed(self._log))
        )

    def record(self, action: str, size: int, name: str):
        """Record a line of log."""

        self._log.append(
            "{:17}    {:8}    {:>11}    {}\n".format(
                pd.Timestamp.now().strftime("%D %T"),
                action,
                humansize(size),
                name,
            ),
        )

    def write(self, logfile: Path, copy_to: str = None):
        """Insert logs to the beginning of a logfile.

        If `copy_to` is a dir, logfile will be copied to that directory.
        """

        if not self._log or _debug:
            return

        try:
            with open(logfile, mode="r+", encoding="utf-8") as f:
                for _ in range(2):
                    f.readline()
                backup = f.read()
                f.seek(0)
                f.write(self.__str__())
                f.write(backup)
                f.truncate()
        except FileNotFoundError:
            with open(logfile, mode="w", encoding="utf-8") as f:
                f.write(self.__str__())

        try:
            shutil.copy(logfile, Path(copy_to))
        except TypeError:
            pass
        except OSError as e:
            print("Copying log failed:", e)


class qBittorrent:
    """The manager class for communicating with qBittorrent and data persistence."""

    def __init__(
        self,
        *,
        host: str,
        seedDir: str,
        diskQuota: float,
        speedThresh: Tuple[int, int],
        deadThresh: int,
        datafile: Path,
    ):

        self._api_base = urljoin(host, "api/v2/")
        maindata = self._request("sync/maindata").json()
        self.state: dict = maindata["server_state"]
        self.torrent: dict = maindata["torrents"]
        if self.state["connection_status"] not in ("connected", "firewalled"):
            raise RuntimeError("qBittorrent is not connected to the internet.")

        try:
            self.seedDir = Path(seedDir)
        except TypeError:
            self.seedDir = None

        values = self.torrent.values()
        self.stateCount = Counter(v["state"] for v in values)
        self._spaceOffset = sum(v["amount_left"] for v in values) + diskQuota * byteSize["GiB"]

        self._speedThresh = tuple(v * byteSize["KiB"] for v in speedThresh)
        self._deadThresh = deadThresh * byteSize["KiB"]
        self.datafile = datafile if isinstance(datafile, Path) else Path(datafile)

        self._load_data()
        self._record()

    def _request(self, path: str, *, method: str = "GET", **kwargs):
        """Communicate with qBittorrent API."""

        res = requests.request(method, self._api_base + path, timeout=7, **kwargs)
        res.raise_for_status()
        return res

    def _load_data(self):
        """Load data objects from pickle."""

        self.appData: pd.DataFrame
        self.torrentData: pd.DataFrame
        self.history: pd.DataFrame
        self.silence: pd.Timestamp
        self.session: requests.Session

        try:
            with self.datafile.open(mode="rb") as f:
                self.appData, self.torrentData, self.history, self.silence, self.session = pickle.load(f)

        except Exception as e:
            if self.datafile.exists():
                print(f"Reading '{self.datafile}' failed: {e}")
                if not _debug:
                    self.datafile.rename(f"{self.datafile}_{NOW.strftime('%y%m%d_%H%M%S')}")

            self.appData = self.torrentData = self.history = None
            self.silence = NOW
            self.init_session()

    def dump_data(self):
        """Save data to disk."""

        if _debug:
            return

        try:
            with self.datafile.open("wb") as f:
                pickle.dump((self.appData, self.torrentData, self.history, self.silence, self.session), f)

        except (OSError, pickle.PickleError) as e:
            msg = f"Writing data to disk failed: {e}"
            logger.record("Error", None, msg)
            print(msg)

    def init_session(self):
        """Instantiate a new requests session."""

        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0"}
        )
        return self.session

    def _record(self):
        """Record qBittorrent traffic info to pandas DataFrame."""

        # new rows for application and torrents
        appRow = pd.DataFrame({"upload": self.state["alltime_ul"], "download": self.state["alltime_dl"]}, index=(NOW,))
        torrentRow = pd.DataFrame({k: v["uploaded"] for k, v in self.torrent.items()}, index=(NOW,))

        # truncate application dataframe and append new row
        try:
            self.appData = self.appData.truncate(
                before=NOW - pd.Timedelta(1, unit="hours"),
                copy=False,
            ).append(appRow)
        except (TypeError, AttributeError):
            self.appData = appRow

        # cleanup deleted torrents and append new row
        try:
            df = self.torrentData.truncate(
                before=NOW - pd.Timedelta(1, unit="days"),
                copy=False,
            )

            delete = df.columns.difference(torrentRow.columns)
            if not delete.empty:
                df.drop(columns=delete, inplace=True, errors="ignore")
                df.dropna(how="all", inplace=True)

            self.torrentData = df.append(torrentRow)
        except (TypeError, AttributeError):
            self.torrentData = torrentRow

        # select expiration record of current torrents
        try:
            df = self.history
            df = df.loc[df.index.isin(torrentRow.columns), "expire"]
            self.expired = delete = df[df <= NOW].index
            if not delete.empty:
                self.history.loc[delete, "expire"] = pd.NaT
        except (TypeError, AttributeError, KeyError):
            self.history = pd.DataFrame(columns=("id", "add", "expire"))
            self.expired = self.history["expire"].index

    def clean_seedDir(self):
        """Clean files in seed dir which does not belong to qb download list."""

        try:
            iterdir = self.seedDir.iterdir()
        except AttributeError:
            return

        names = {v["name"] for v in self.torrent.values()}
        for path in iterdir:
            if path.name not in names:
                if path.suffix == ".!qB" and path.stem in names:
                    continue
                print("Cleanup:", path.name)
                try:
                    if _debug:
                        pass
                    elif path.is_dir():
                        shutil.rmtree(path)
                    else:
                        path.unlink()
                except OSError as e:
                    print("Deletion Failed:", e)
                else:
                    logger.record("Cleanup", None, path.name)

    def throttle_expires(self):
        """Throttle download speeds on expired limited-time free torrents."""

        if not (self.expired.empty or _debug):
            self._request(
                "torrents/setDownloadLimit",
                method="POST",
                data={"hashes": "|".join(self.expired), "limit": 1},
            )

    def get_free_space(self) -> int:
        """Return free space on seed_dir.

        `free_space` = `free_space_on_disk` - `disk_quota` - `amount_left_to_download`
        """
        real = self.state["free_space_on_disk"]

        try:
            f = shutil.disk_usage(self.seedDir).free
        except TypeError:
            pass
        else:
            if f > real:
                real = f

        self.freeSpace = int(real - self._spaceOffset)
        return self.freeSpace

    def get_speed(self) -> pd.Series:
        """Calculate qBittorrent last hour ul/dl speeds."""
        hi = self.appData.iloc[-1]
        lo = self.appData.iloc[0]
        return (hi - lo) / (hi.name - lo.name).total_seconds()

    def need_action(self) -> bool:
        """Whether the current situation requires further action (downloads or removals).

        True if:
        -   space is bellow threshold
        -   speed is bellow threshold and alt_speed is not enabled
        -   some limited-time free torrents have just expired

        False if:
        -   during silence period (explicitly set after a successful download)
        -   has torrents in download queue
        -   any other situations
        """

        speeds = self.get_speed()
        print("Last hour avg speed: UL: {upload}/s, DL: {download}/s.".format_map(speeds.apply(humansize)))

        if self.get_free_space() < 0:
            return True

        try:
            if NOW <= self.silence:
                return False
        except TypeError:
            self.silence = NOW

        return (
            (speeds.values < self._speedThresh).all()
            and not self.state["use_alt_speed_limits"]
            and not 0 < self.state["up_rate_limit"] < self._speedThresh[0]  # upload
            and "queuedDL" not in self.stateCount
            or not self.expired.empty
        )

    def get_remove_cands(self) -> Iterator[Removable]:
        """Discover the slowest torrents using jenks natural breaks."""

        # calculate avg speed for each torrent
        df = self.torrentData
        hi = df.iloc[-1]
        lo = df.apply(pd.Series.first_valid_index)
        try:
            speeds: pd.Series = (hi.values - df.lookup(lo, lo.index)) / (hi.name - lo).dt.total_seconds()
            speeds.dropna(inplace=True)
        except AttributeError:
            return

        # get 1/3 break point using jenks method
        try:
            c = speeds.size - 1
            if c > 3:
                c = 3
            breaks = jenks_breaks(speeds, nb_class=c)[1]
        except Exception as e:
            print("Jenkspy failed:", e)
            breaks = speeds.mean()

        # exclude those added less than 1 day
        yesterday = pd.Timestamp.now(tz="UTC").timestamp() - 86400

        for key, val in speeds[speeds <= breaks].items():
            t = self.torrent[key]
            if t["added_on"] < yesterday:
                yield Removable(
                    hash=key,
                    size=t["size"],
                    peer=t["num_incomplete"],
                    title=t["name"],
                    state=t["state"],
                    weighted=(val > self._deadThresh),
                )

    def remove_torrents(self, removeList: Sequence[Removable]):
        """Remove torrents and delete files."""

        if not removeList or _debug:
            return

        self._request(
            "torrents/delete",
            params={"hashes": "|".join(t.hash for t in removeList), "deleteFiles": True},
        )
        for t in removeList:
            logger.record("Remove", t.size, t.title)

    def add_torrent(self, downloadList: Sequence[Torrent], content: Mapping[str, bytes]):
        """Upload torrents to qBittorrents and record information."""

        if not content:
            return

        assert len(downloadList) == len(content), "Lengths of params should match."

        if not _debug:
            try:
                self._request("torrents/add", method="POST", files=content)
            except requests.RequestException as e:
                logger.record("Error", None, e)
                return

        # convert time strings to timestamp objects
        # read hash and name from torrent file
        for t in downloadList:
            try:
                t.expire = NOW + pd.Timedelta(t.expire)
            except ValueError:
                t.expire = pd.NaT

            try:
                torrent = TorrentParser.from_string(content[t.id])
                t.hash = torrent.info_hash
                t.title = torrent.name
                t.size = torrent.total_size
            except TorrentoolException as e:
                print("Torrentool error:", e)
                if not t.hash:
                    t.hash = t.id

            logger.record("Download", t.size, t.title)

        # cleanup outdated records (older than 30 days and not in app)
        df = self.history
        df = df[df.index.isin(self.torrentData.columns) | (df["add"] > NOW - pd.Timedelta(30, unit="days"))]

        # save new info to dataframe
        self.history = df.append(
            pd.DataFrame(
                ((t.id, NOW, t.expire) for t in downloadList),
                index=(t.hash for t in downloadList),
                columns=("id", "add", "expire"),
            )
        )

        # set n hours of silence
        self.silence = NOW + pd.Timedelta(len(downloadList), unit="hours")

    def get_preference(self, key: str):
        """Query qBittorrent preferences by key."""
        if not hasattr(self, "_preferences"):
            self._preferences = self._request("app/preferences").json()
        return self._preferences[key]

    def resume_paused(self):
        """If any torrent is paused, for any reason, resume."""

        paused = {"error", "missingFiles", "pausedUP", "pausedDL", "unknown"}
        if not paused.isdisjoint(self.stateCount):
            print("Resume torrents.")
            if not _debug:
                self._request("torrents/resume", params={"hashes": "all"})


class MTeam:
    """A cumbersome MTeam downloader.

    -   Minimum peer requirement subjects to:

        Peer >= A * Size(GiB) + B

        Where (A, B) is defined in config file and passed via `minPeer`.
    """

    domain = "https://pt.m-team.cc"

    def __init__(self, *, feeds: Iterable[str], account: Tuple[str, str], minPeer: Tuple[float, int], qb: qBittorrent):

        self.feeds = feeds
        self.account = account
        self.minPeer = minPeer[0] / byteSize["GiB"], minPeer[1]
        self.qb = qb
        self.session = qb.session

    def _get(self, path: str):

        url = urljoin(self.domain, path)
        for retry in range(3):
            try:
                response = self.session.get(url, timeout=(7, 28))
                response.raise_for_status()
                if "/login.php" not in response.url:
                    return response
                if retry < 2:
                    self._login()
                else:
                    print("Login failed.")
            except (requests.ConnectionError, requests.HTTPError, requests.Timeout) as e:
                print("Connection error:", e)
            except (requests.RequestException, AttributeError, TypeError):
                self.session = self.qb.init_session()

    def _login(self):

        if not hasattr(self, "loginParam"):
            self.loginParam = {
                "url": f"{self.domain}/takelogin.php",
                "data": {"username": self.account[0], "password": self.account[1]},
                "headers": {"referer": f"{self.domain}/login.php"},
            }
        self.session.post(**self.loginParam)

    def fetch(self) -> Iterator[Torrent]:

        from bs4 import BeautifulSoup

        cols = {}
        A, B = self.minPeer
        visited = set(self.qb.history["id"])
        transTable = str.maketrans({"日": "D", "時": "H", "分": "T"})

        re_download = re_compile(r"\bdownload\.php\?")
        re_details = re_compile(r"\bdetails\.php\?")
        re_nondigit = re_compile(r"[^0-9]+")
        re_size = re_compile(r"(?P<num>[0-9]+(\.[0-9]+)?)\s*(?P<unit>[KMGT]i?B)")
        re_id = re_compile(r"\bid=(?P<id>[0-9]+)")
        re_timelimit = re_compile(r"^\s*限時：")

        print(f"Connecting to M-Team... Pages: {len(self.feeds)}.")

        for feed in self.feeds:
            try:
                soup = BeautifulSoup(self._get(feed).content, "lxml")
                soup = (tr.find_all("td", recursive=False) for tr in soup.select("#form_torrent table.torrents > tr"))
                tr = next(soup)
            except AttributeError:
                print("Fetching failed:", feed)
                continue
            except StopIteration:
                print("Unable to locate table, css selector broken?", feed)
                continue
            else:
                print("Fetching success.")

            for i, td in enumerate(tr):
                title = td.find(title=True)
                title = title["title"] if title else td.get_text(strip=True)
                cols[title] = i

            colTitle = cols.pop("標題", 1)
            colSize = cols.pop("大小", 4)
            colUp = cols.pop("種子數", 5)
            colDown = cols.pop("下載數", 6)
            colProg = cols.pop("進度", 8)

            for tr in soup:
                try:
                    peer = int(re_nondigit.sub("", tr[colDown].get_text()))
                    size = re_size.search(tr[colSize].get_text())
                    size = int(float(size["num"]) * byteSize[size["unit"]])
                    if peer < A * size + B or "peer-active" in tr[colProg]["class"]:
                        continue

                    link = tr[colTitle].find("a", href=re_download)["href"]
                    tid = re_id.search(link)["id"]
                    if tid in visited or tr[colUp].get_text(strip=True) == "0":
                        continue

                    expire = tr[colTitle].find(string=re_timelimit)
                    if expire:
                        if "日" not in expire:
                            continue
                        expire = expire.split("：", 1)[1].translate(transTable)

                    title = tr[colTitle].find("a", href=re_details, string=True)
                    title = title["title"] if title.has_attr("title") else title.get_text(strip=True)

                except Exception as e:
                    print("Parsing page error:", e)
                else:
                    visited.add(tid)
                    yield Torrent(id=tid, size=size, peer=peer, link=link, expire=expire, title=title)

    def download(self, downloadList: Sequence[Torrent]):
        """Download torrents from mteam."""
        try:
            return {t.id: self._get(t.link).content for t in downloadList}
        except AttributeError:
            print(f"Downloading torrents failed.")


class MPSolver:
    """Using Google OR-Tools to find the optimal combination of downloads and removals.

    Maximize obtained peers under constraints.

    ### Constraints:
    -   `download_size` - `removed_size` <= `free_space`

        -   infeasible when `free_space` < -`removed_size`. Remove all
            removables to freeup space.

    -   `downloads` - `removes[downloading]` <= `max_active_downloads` - `total_downloading`

        -   never exceed qBittorrent max_active_downloads limit, if exists.
        -   to avoid problems when max_active_downloads < total_downloading
            (i.e. torrents force started by user), only implemented when
            downloads > 0. If we were to add new torrents, we ensure overall
            downloading bellow limit. Otherwise, leave it be.

    ### Objective:
    -   Maximize: `download_peer` - `removed_peer`

        -   if removed.weighted is False, removed.peer == 1
    """

    def __init__(self, *, removeCand: Iterable[Removable], downloadCand: Iterable[Torrent], qb: qBittorrent):

        self.downloadList = self.removeList = ()
        self.downloadCand = tuple(downloadCand)
        if not (self.downloadCand or qb.freeSpace < 0 or _debug):
            return

        self.removeCand = tuple(removeCand)
        self.removeCandSize = sum(t.size for t in self.removeCand)
        if qb.freeSpace <= -self.removeCandSize:
            self.removeList = self.removeCand
            return

        self.qb = qb
        self._solve()

    def _solve(self):

        from ortools.sat.python import cp_model

        downloadCand = self.downloadCand
        removeCand = self.removeCand
        qb = self.qb

        model = cp_model.CpModel()
        Sum = cp_model.LinearExpr.Sum
        ScalProd = cp_model.LinearExpr.ScalProd

        # download_size - removed_size <= free_space
        coef = [t.size for t in downloadCand]
        coef.extend(-t.size for t in removeCand)
        pool = [model.NewBoolVar(f"{i}") for i in range(len(coef))]
        model.Add(ScalProd(pool, coef) <= qb.freeSpace)

        # downloads - removes(downloading) <= max_active_downloads - total_downloading
        maxActive: int = qb.get_preference("max_active_downloads")
        if maxActive > 0:

            # intermediate boolean variable
            has_new = model.NewBoolVar("has_new")

            # implement has_new == (Sum(downloads) > 0)
            d = len(downloadCand)
            model.Add(Sum(pool[i] for i in range(d)) > 0).OnlyEnforceIf(has_new)
            model.Add(Sum(pool[i] for i in range(d)) == 0).OnlyEnforceIf(has_new.Not())

            # enforce only if has_new is true
            coef = [1] * d
            coef.extend(-(t.state == "downloading") for t in removeCand)
            model.Add(
                ScalProd(pool, coef) <= maxActive - qb.stateCount["downloading"],
            ).OnlyEnforceIf(has_new)

        # Maximize: download_peer - removed_peer
        # -x ** 0 == -1, -x ** 1 == -x
        coef = [t.peer for t in downloadCand]
        coef.extend(-t.peer ** t.weighted for t in removeCand)
        model.Maximize(ScalProd(pool, coef))

        solver = cp_model.CpSolver()
        status = solver.Solve(model)

        if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            self.status = {
                "status": solver.StatusName(status),
                "walltime": solver.WallTime(),
                "value": solver.ObjectiveValue(),
            }
            value = map(solver.BooleanValue, pool)
            self.downloadList = tuple(t for t in downloadCand if next(value))
            self.removeList = tuple(t for t in removeCand if next(value))
        else:
            self.status = solver.StatusName(status)

    def report(self):
        """Print report to stdout."""

        if not hasattr(self, "status"):
            print("Solver did not start: unnecessary condition.")
            return

        sepSlim = "-" * 50
        qb = self.qb
        removeSize = sum(t.size for t in self.removeList)
        downloadSize = sum(t.size for t in self.downloadList)
        finalFreeSpace = qb.freeSpace + removeSize - downloadSize

        print(sepSlim)
        print(
            "Disk free space: {}. Max available: {}.".format(
                humansize(qb.freeSpace),
                humansize(qb.freeSpace + self.removeCandSize),
            )
        )
        print(
            "Download candidates: {}. Total: {}.".format(
                len(self.downloadCand),
                humansize(sum(t.size for t in self.downloadCand)),
            )
        )
        print(
            "Remove candidates: {}/{}. Total: {}.".format(
                len(self.removeCand),
                len(qb.torrent),
                humansize(self.removeCandSize),
            )
        )
        for t in self.removeCand:
            print(f"[{humansize(t.size):>11}|{t.peer:3d} peers] {t.title}")

        print(sepSlim)
        if isinstance(self.status, dict):
            print("Solution: {status}. Walltime: {walltime:.5f}s. Objective value: {value}.".format_map(self.status))
        else:
            print("CP-SAT solver cannot find an optimal solution. Status:", self.status)

        print(f"Free space after operation: {humansize(qb.freeSpace)} => {humansize(finalFreeSpace)}.")

        for prefix in "remove", "download":
            final = getattr(self, prefix + "List")
            cand = getattr(self, prefix + "Cand")
            size = locals()[prefix + "Size"]
            print(sepSlim)
            print(
                "{}: {}/{}. Total: {}, {} peers.".format(
                    prefix.capitalize(),
                    len(final),
                    len(cand),
                    humansize(size),
                    sum(t.peer for t in final),
                )
            )
            for t in final:
                print(f"[{humansize(t.size):>11}|{t.peer:3d} peers] {t.title}")


def humansize(size: int) -> str:
    """Convert bytes to human readable sizes."""

    try:
        for suffix in ("KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"):
            size /= 1024
            if -1024 < size < 1024:
                return f"{size:.2f} {suffix}"
    except TypeError:
        pass
    return "NaN"


def read_config(configfile: Path):
    """Read or create config file."""

    global _debug
    parser = ConfigParser()

    if parser.read(configfile, encoding="utf-8"):

        basic = parser["DEFAULT"]

        for arg in sys.argv[1:]:
            if arg == "-d":
                _debug = True
            elif arg == "-r":
                _debug = True
                basic = parser["OVERRIDE"]
            else:
                raise ValueError(f"Unrecognized argument: '{arg}'")

        return basic, parser["MTEAM"]

    parser["DEFAULT"] = {
        "host": "http://localhost",
        "seed_dir": "",
        "disk_quota": "50",
        "up_rate_thresh": "2700",
        "dl_rate_thresh": "6000",
        "dead_torrent_up_thresh": "2",
        "log_backup_dir": "",
    }
    parser["MTEAM"] = {
        "account": "",
        "password": "",
        "peer_slope": "0.3",
        "peer_intercept": "30",
        "feeds": "\nexample1.php\nexample2.php",
    }
    parser["OVERRIDE"] = {
        "host": "http://localhost",
        "seed_dir": "",
    }
    with open(configfile, "w", encoding="utf-8") as f:
        parser.write(f)

    print("Please edit config.ini before running this script again.")
    sys.exit()


def main():

    root = Path(__file__)
    basic, mt = read_config(root.with_name("config.ini"))

    qb = qBittorrent(
        host=basic["host"],
        seedDir=basic["seed_dir"] or None,
        diskQuota=basic.getfloat("disk_quota"),
        speedThresh=(basic.getint("up_rate_thresh"), basic.getint("dl_rate_thresh")),
        deadThresh=basic.getint("dead_torrent_up_thresh"),
        datafile=root.with_name("data"),
    )
    qb.clean_seedDir()
    qb.throttle_expires()

    if qb.need_action() or _debug:

        mteam = MTeam(
            feeds=mt["feeds"].split(),
            account=(mt["account"], mt["password"]),
            minPeer=(mt.getfloat("peer_slope"), mt.getint("peer_intercept")),
            qb=qb,
        )
        solver = MPSolver(
            removeCand=qb.get_remove_cands(),
            downloadCand=mteam.fetch(),
            qb=qb,
        )

        qb.remove_torrents(solver.removeList)
        qb.add_torrent(solver.downloadList, mteam.download(solver.downloadList))
        solver.report()

    qb.resume_paused()
    qb.dump_data()

    logger.write(
        logfile=root.with_name("logfile.log"),
        copy_to=basic["log_backup_dir"] or None,
    )


logger = Logger()

if __name__ == "__main__":
    main()
