import os
import pickle
import re
import shutil
import sys
from collections import Counter
from configparser import ConfigParser
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Dict, Iterable, Iterator, Sequence, Tuple
from urllib.parse import urljoin

# Beautifulsoup, ortools, jenkspy, torrentool are imported as needed
import pandas as pd
import requests

_debug: bool = False

NOW = pd.Timestamp.now()

BYTESIZE: Dict[str, int] = {
    k: v for kk, v in zip(
        ((f"{c}B", f"{c}iB") for c in "KMGTP"),
        (1024**i for i in range(1, 6)),
    ) for k in kk
}


@dataclass
class Removable:
    """Removable torrents from qBittorrent."""

    hash: str
    size: int
    peer: int
    title: str
    state: str
    weight: int


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
            "Date", "Action", "Size", "Name", "", "".join(reversed(self._log)))

    def __bool__(self):
        return not not self._log

    def record(self, action: str, size: int, name: str):
        """Record a line of log."""

        self._log.append("{:17}    {:8}    {:>11}    {}\n".format(
            pd.Timestamp.now().strftime("%D %T"),
            action,
            humansize(size),
            name,
        ))

    def write(self, logfile: Path, copy_to: str = None):
        """Insert logs to the beginning of a logfile.

        If `copy_to` is a dir, logfile will be copied to that directory.
        """
        if _debug:
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
            if not isinstance(logfile, Path):
                logfile = Path(logfile)

            logfile.parent.mkdir(parents=True, exist_ok=True)
            with open(logfile, mode="w", encoding="utf-8") as f:
                f.write(self.__str__())

        if copy_to:
            try:
                shutil.copy(logfile, copy_to)
            except OSError as e:
                print(e)


class qBittorrent:
    """The manager class for communicating with qBittorrent and data persistence."""

    appData: pd.DataFrame
    torrentData: pd.DataFrame
    history: pd.DataFrame
    silence: pd.Timestamp
    session: requests.Session
    expired: pd.Index

    def __init__(
        self,
        *,
        host: str,
        seed_dir: str,
        disk_quota: float,
        speed_thresh: Tuple[int, int],
        dead_thresh: int,
        datafile: Path,
    ):

        self.seed_dir = Path(seed_dir) if seed_dir else None
        self.datafile = (datafile
                         if isinstance(datafile, Path) else Path(datafile))

        self._speed_thresh = (speed_thresh[0] * BYTESIZE["KiB"],
                              speed_thresh[1] * BYTESIZE["KiB"])
        self._dead_thresh = dead_thresh * BYTESIZE["KiB"]

        self._api_base = urljoin(host, "api/v2/")
        maindata: Dict[str, dict] = self._request("sync/maindata").json()

        self.server_state = d = maindata["server_state"]
        if d["connection_status"] not in ("connected", "firewalled"):
            print("qBittorrent is not connected to the internet.")
            sys.exit()

        self.torrents = d = maindata["torrents"]
        self.state_counter = Counter(v["state"] for v in d.values())
        self._space_offset = (sum(v["amount_left"] for v in d.values()) +
                              disk_quota * BYTESIZE["GiB"])
        self._freeSpace = self._preferences = None

        self._load_data()
        self._record()

    def _request(self, path: str, *, method: str = "GET", **kwargs):
        """Communicate with qBittorrent API."""

        res = requests.request(method,
                               self._api_base + path,
                               timeout=7,
                               **kwargs)
        res.raise_for_status()
        return res

    def _load_data(self):
        """Load data objects from pickle."""

        try:
            with open(self.datafile, mode="rb") as f:
                (
                    self.appData,
                    self.torrentData,
                    self.history,
                    self.silence,
                    self.session,
                ) = pickle.load(f)

        except (OSError, pickle.PickleError) as e:

            if self.datafile.exists():
                print(f"Reading '{self.datafile}' failed: {e}")
                if not _debug:
                    self.datafile.rename(
                        f"{self.datafile}_{NOW.strftime('%y%m%d_%H%M%S')}")

            self.appData = self.torrentData = self.history = None
            self.silence = NOW
            self.init_session()

    def dump_data(self):
        """Save data to disk."""

        if _debug:
            return
        try:
            with open(self.datafile, "wb") as f:
                pickle.dump((
                    self.appData,
                    self.torrentData,
                    self.history,
                    self.silence,
                    self.session,
                ), f)

        except (OSError, pickle.PickleError) as e:
            msg = f"Writing data to disk failed: {e}"
            logger.record("Error", None, msg)
            print(msg)

    def init_session(self):
        """Instantiate a new requests session."""
        from requests.adapters import HTTPAdapter

        session = self.session = requests.Session()
        session.headers.update({
            "User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0"
        })
        adapter = HTTPAdapter(max_retries=5)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _record(self):
        """Record qBittorrent traffic info to pandas DataFrame."""

        # new rows for application and torrents
        app_row = pd.DataFrame(
            {
                "upload": self.server_state["alltime_ul"],
                "download": self.server_state["alltime_dl"]
            },
            index=(NOW,),
        )
        torrent_row = pd.DataFrame(
            {k: v["uploaded"] for k, v in self.torrents.items()},
            index=(NOW,),
        )

        # qBittorrent overall ul/dl speeds
        df = self.appData
        try:
            last = df.iloc[-1]
            if last.name >= NOW or (last.values > app_row.values).any():
                raise ValueError

            self.appData = df.truncate(
                before=NOW - timedelta(hours=1),
                copy=False,
            ).append(app_row)

        except (AttributeError, ValueError):
            self.appData = app_row

        # upload speeds of each torrent
        df = self.torrentData
        try:
            last = df.iloc[-1]
            if last.name >= NOW or last.gt(torrent_row.iloc[0]).any():
                raise ValueError

            delete = df.columns.difference(torrent_row.columns)
            if not delete.empty:
                df.drop(columns=delete, inplace=True, errors="ignore")
                df.dropna(how="all", inplace=True)

            self.torrentData = df.truncate(
                before=NOW - timedelta(days=1),
                copy=False,
            ).append(torrent_row)

        except (AttributeError, ValueError):
            self.torrentData = torrent_row

        # expiration records of current torrents
        df = self.history
        try:
            df = df.loc[df.index.isin(torrent_row.columns), "expire"]

            self.expired = df.index[df.values <= NOW]
            if not self.expired.empty:
                self.history.loc[self.expired, "expire"] = pd.NaT

        except (AttributeError, ValueError):
            self.history = pd.DataFrame(columns=("id", "add", "expire"))
            self.expired = self.history.index

    def clean_seeddir(self):
        """Clean files in seed dir which does not belong to qb download list."""

        seed_dir = self.seed_dir
        if seed_dir is None:
            return

        torrents = {v["name"] for v in self.torrents.values()}

        for name in os.listdir(seed_dir):

            if name not in torrents:

                path = seed_dir.joinpath(name)
                if path.suffix == ".!qB" and path.stem in torrents:
                    continue

                print("Cleanup:", path)
                self._freeSpace = None
                try:
                    if _debug:
                        pass
                    elif path.is_dir():
                        shutil.rmtree(path)
                    else:
                        os.unlink(path)
                except OSError as e:
                    print(e)
                else:
                    logger.record("Cleanup", None, name)

    def need_action(self) -> bool:
        """Whether the current situation requires further action (downloads or
        removals).

        True if:
        -   space is bellow threshold
        -   speed is bellow threshold and alt_speed is not enabled
        -   some limited-time free torrents have just expired

        False if:
        -   qBittorrent is busy (checking, moving data...)
        -   during silence period (explicitly set after a successful download)
        -   queued downloading
        -   any other situations
        """

        speeds = self.speeds
        print("Last hour avg speed: UL: {}/s, DL: {}/s.".format(
            *map(humansize, speeds)))

        busy = {
            "checkingUP", "allocating", "checkingDL", "checkingResumeData",
            "moving"
        }
        if not busy.isdisjoint(self.state_counter):
            return False

        if self.freeSpace < 0:
            return True

        try:
            if NOW <= self.silence:
                return False
        except TypeError:
            self.silence = NOW

        return (
            (speeds < self._speed_thresh).all() and
            not self.server_state["use_alt_speed_limits"] and
            not 0 < self.server_state["up_rate_limit"] < self._speed_thresh[0]
            and "queuedDL" not in self.state_counter or not self.expired.empty)

    def get_remove_cands(self) -> Iterator[Removable]:
        """Discover the slowest torrents using jenks natural breaks."""

        from jenkspy import jenks_breaks

        # calculate avg speed for each torrent
        speeds = self.torrent_speed

        # get 1/3 break point using jenks method
        try:
            c = speeds.size - 1
            if c > 3:
                c = 3
            breaks = jenks_breaks(speeds, nb_class=c)[1]
        except Exception as e:
            print("Jenkspy failed:", e)
            breaks = speeds.mean()

        thresh = self._dead_thresh
        if breaks < thresh:
            breaks = thresh

        removes = speeds[speeds.values <= breaks]
        if not self.expired.empty:
            removes = removes.to_dict()
            removes.update({
                k: None
                for k in self.expired
                if self.torrents[k]["progress"] != 1
            })

        # exclude those added in less than 1 day
        yesterday = pd.Timestamp.now(tz="UTC").timestamp() - 86400

        # weight in Removables:
        # speed > deadThresh: None (use the same factor as new torrents)
        # speed <= deadThresh: 1 (minimum value to be considered)
        # expired when still downloading: 0 (delete unconditionally)
        for k, v in removes.items():
            t = self.torrents[k]
            if v is None:
                v = 0
            elif t["added_on"] > yesterday:
                continue
            elif v <= thresh:
                v = 1
            else:
                v = None
            yield Removable(
                hash=k,
                size=t["size"],
                peer=t["num_incomplete"],
                title=t["name"],
                state=t["state"],
                weight=v,
            )

    def remove_torrents(self, removeList: Sequence[Removable]):
        """Remove torrents and delete files."""

        if not removeList or _debug:
            return

        self._request("torrents/delete",
                      params={
                          "hashes": "|".join(t.hash for t in removeList),
                          "deleteFiles": True,
                      })
        for t in removeList:
            logger.record("Remove", t.size, t.title)

    def add_torrent(self, downloadList: Sequence[Torrent],
                    content: Dict[str, bytes]):
        """Upload torrents to qBittorrents and record information."""

        if not content:
            return
        assert len(downloadList) == len(
            content), "Lengths of params should match."

        from torrentool.api import Torrent as TorrentParser
        from torrentool.exceptions import TorrentoolException

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

            logger.record("Download", t.size, t.title)

        # cleanup outdated records (older than 30 days and not in app)
        df = self.history
        df = df[df.index.isin(self.torrentData.columns) |
                (df["add"].values > NOW - timedelta(days=30))]

        # save new info to dataframe
        self.history = df.append(
            pd.DataFrame(
                ((t.id, NOW, t.expire) for t in downloadList),
                index=(t.hash or t.id for t in downloadList),
                columns=("id", "add", "expire"),
            ))

        # set n hours of silence
        self.silence = NOW + timedelta(hours=len(downloadList))

    def get_preference(self, key: str):
        """Query qBittorrent preferences by key."""
        p = self._preferences
        if p is None:
            p = self._preferences = self._request("app/preferences").json()
        return p[key]

    def resume_paused(self):
        """If any torrent is paused, for any reason, resume."""
        paused = {"error", "missingFiles", "pausedUP", "pausedDL", "unknown"}
        if not paused.isdisjoint(self.state_counter):
            print("Resume torrents.")
            if not _debug:
                self._request("torrents/resume", params={"hashes": "all"})

    @property
    def freeSpace(self) -> int:
        """Return free space on seed_dir.

        `free_space` = `free_space_on_disk` - `disk_quota` - `amount_left_to_download`
        """
        f = self._freeSpace
        if f is None:
            real = self.server_state["free_space_on_disk"]
            try:
                f = shutil.disk_usage(self.seed_dir).free
            except TypeError:
                pass
            else:
                if f > real:
                    real = f
            f = self._freeSpace = int(real - self._space_offset)
        return f

    @property
    def speeds(self):
        """qBittorrent last hour ul/dl speeds.

        Returns: numpy.array([<ul>, <dl>])
        """
        df = self.appData
        hi = df.iloc[-1]
        lo = df.iloc[0]
        t = (hi.name - lo.name).total_seconds()
        if not t:
            return pd.array((None, None), dtype=float).to_numpy()
        return (hi.values - lo.values) / t

    @property
    def torrent_speed(self) -> pd.Series:
        """Avg speeds for each torrents in the last 24 hours.

        Torrents without meaningful speed (not enough records) will be removed
        from result.
        """
        df = self.torrentData
        hi = df.iloc[-1]
        lo = df.apply(pd.Series.first_valid_index)
        try:
            speeds: pd.Series
            speeds = ((hi.values - df.lookup(lo, lo.index)) /
                      (hi.name - lo).dt.total_seconds())
            speeds.dropna(inplace=True)
        except AttributeError:
            return pd.Series(dtype=float)
        return speeds


class MTeam:
    """A cumbersome MTeam downloader.

    -   Minimum peer requirement subjects to:

        Peer >= A * Size(GiB) + B

        Where (A, B) is defined in config file and passed via `minPeer`.
    """

    DOMAIN = "https://pt.m-team.cc/"

    def __init__(self, *, feeds: Sequence[str], username: str, password: str,
                 minPeer: Tuple[float, int], qb: qBittorrent):

        self.feeds = feeds
        self._account = {"username": username, "password": password}
        self.minPeer = minPeer[0] / BYTESIZE["GiB"], minPeer[1]
        self.qb = qb
        self.session = qb.session
        self._login = False

    def _get(self, path: str):

        try:
            response = self.session.get(urljoin(self.DOMAIN, path),
                                        timeout=(7, 28))
            response.raise_for_status()
            if "/login.php" not in response.url:
                return response
        except (requests.ConnectionError, requests.HTTPError,
                requests.Timeout) as e:
            print("Connection error:", e)
            return
        except (requests.RequestException, AttributeError):
            self.session = self.qb.init_session()

        if self._login:
            return
        print("Logging in..", end="", flush=True)

        try:
            response = self.session.post(
                url=self.DOMAIN + "takelogin.php",
                data=self._account,
                headers={"referer": self.DOMAIN + "login.php"},
            )
            response.raise_for_status()
        except requests.RequestException:
            print("failed.")
            return

        print("ok.")
        self._login = True
        return self._get(path)

    def fetch(self) -> Iterator[Torrent]:

        from bs4 import BeautifulSoup

        cols = {}
        A, B = self.minPeer
        visited = set(self.qb.history["id"])

        sub_nondigit = re.compile(r"\D").sub
        search_size = re.compile(
            r"(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>[KMGT]i?B)").search

        re_download = re.compile(r"\bdownload\.php\?")
        re_details = re.compile(r"\bdetails\.php\?")
        re_time = re.compile(
            r"^\W*限時：\W*(?:0*(\d+)\s*日)?\W*(?:(\d+)\s*時)?\W*(?:(\d+)\s*分)?")

        print(f"Connecting to M-Team... Pages: {len(self.feeds)}.")

        for feed in self.feeds:
            try:
                soup = (
                    tr.find_all("td", recursive=False)
                    for tr in BeautifulSoup(self._get(feed).content, "lxml").
                    select("#form_torrent table.torrents > tr"))
                row = next(soup)
            except AttributeError:
                print("Fetching failed:", feed)
                continue
            except StopIteration:
                print("Unable to locate table, css selector broken?", feed)
                continue
            else:
                print("Fetching success.")

            for i, td in enumerate(row):
                title = td.find(title=True)
                title = title["title"].strip() if title else td.get_text(
                    strip=True)
                cols[title] = i

            c_title = cols.pop("標題", 1)
            c_size = cols.pop("大小", 4)
            c_up = cols.pop("種子數", 5)
            c_down = cols.pop("下載數", 6)
            c_prog = cols.pop("進度", 8)

            for row in soup:
                try:
                    peer = int(sub_nondigit("", row[c_down].get_text()))
                    size = search_size(row[c_size].get_text())
                    size = int(float(size["num"]) * BYTESIZE[size["unit"]])
                    if (peer < A * size + B or
                            "peer-active" in row[c_prog]["class"] or
                            not int(sub_nondigit("", row[c_up].get_text()))):
                        continue

                    link = row[c_title].find("a", href=re_download)["href"]
                    tid = re.search(r"\bid=([0-9]+)", link)[1]
                    if tid in visited:
                        continue

                    expire = row[c_title].find(string=re_time)
                    if expire:
                        expire = re_time.search(expire).groups("0")
                        if expire[0] == "0":
                            continue
                        expire = "{}D{}H{}T".format(*expire)

                    title = row[c_title].find("a", href=re_details, string=True)
                    title = title.get("title") or title.get_text(strip=True)

                except Exception as e:
                    print("Parsing page error:", e)
                    continue

                visited.add(tid)
                yield Torrent(
                    id=tid,
                    size=size,
                    peer=peer,
                    link=link,
                    expire=expire,
                    title=title,
                )

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

        -   infeasible when `free_space` < `-removed_size`.

    -   `downloads` - `removes[downloading]` <= `max_active_downloads` - `total_downloading`

        -   never exceed qBittorrent max_active_downloads limit, if exists.
        -   to avoid problems when max_active_downloads < total_downloading
            (i.e. torrents force started by user), only implemented when
            downloads > 0. If we were to add new torrents, we ensure overall
            downloading bellow limit. Otherwise, leave it be.

    ### Objective:
    -   Maximize: `download_peer` - `removed_peer`
    """

    def __init__(self, *, removeCand: Iterable[Removable],
                 downloadCand: Iterable[Torrent], qb: qBittorrent):

        self.downloadList = self.removeList = ()
        self.downloadCand = tuple(downloadCand)
        self.removeCand = tuple(removeCand)
        self.qb = qb
        self.status = None

    def solve(self):

        from ortools.sat.python.cp_model import (FEASIBLE, OPTIMAL, CpModel,
                                                 CpSolver, LinearExpr)

        downloadCand = self.downloadCand
        removeCand = self.removeCand
        qb = self.qb

        model = CpModel()

        # download_size - removed_size <= free_space
        coef = [t.size for t in downloadCand]
        coef.extend(-t.size for t in removeCand)
        pool = tuple(model.NewBoolVar(f"{i}") for i in range(len(coef)))
        model.Add(LinearExpr.ScalProd(pool, coef) <= qb.freeSpace)

        # downloads - removes(downloading) <= max_active_downloads - total_downloading
        maxActive: int = qb.get_preference("max_active_downloads")
        if maxActive > 0:

            # intermediate boolean variable
            has_new = model.NewBoolVar("has_new")

            # implement has_new == (Sum(downloads) > 0)
            d = len(downloadCand)
            model.Add(LinearExpr.Sum(
                pool[i] for i in range(d)) > 0).OnlyEnforceIf(has_new)
            model.Add(LinearExpr.Sum(
                pool[i] for i in range(d)) == 0).OnlyEnforceIf(has_new.Not())

            # enforce only if has_new is true
            coef = [1] * d
            coef.extend(-(t.state == "downloading") for t in removeCand)
            model.Add(
                LinearExpr.ScalProd(pool, coef) <= maxActive -
                qb.state_counter["downloading"]).OnlyEnforceIf(has_new)

        # Maximize: download_peer - removed_peer
        factor = sum(t.peer for t in removeCand if t.weight == 1) + 1
        coef = [t.peer * factor for t in downloadCand]
        coef.extend(-t.peer * (factor if t.weight is None else t.weight)
                    for t in removeCand)
        model.Maximize(LinearExpr.ScalProd(pool, coef))

        solver = CpSolver()
        status = solver.Solve(model)

        if status in (OPTIMAL, FEASIBLE):
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

        if self.status is None:
            print("Solver did not start.")
            return

        sepSlim = "-" * 50
        removeCandSize = self._sumsize(self.removeCand)
        downloadCandSize = self._sumsize(self.downloadCand)
        removeSize = self._sumsize(self.removeList)
        downloadSize = self._sumsize(self.downloadList)
        freeSpace = self.qb.freeSpace
        finalFreeSpace = freeSpace + removeSize - downloadSize

        print(sepSlim)
        print("Disk free space: {}. Max available: {}.".format(
            humansize(freeSpace),
            humansize(freeSpace + removeCandSize),
        ))
        print("Download candidates: {}. Total: {}.".format(
            len(self.downloadCand),
            humansize(downloadCandSize),
        ))
        print("Remove candidates: {}/{}. Total: {}.".format(
            len(self.removeCand),
            len(self.qb.torrents),
            humansize(removeCandSize),
        ))
        for t in self.removeCand:
            print(f"[{humansize(t.size):>11}|{t.peer:4d}P] {t.title}")

        print(sepSlim)
        if isinstance(self.status, dict):
            print(
                "Solution: {status}. Walltime: {walltime:.5f}s. Objective value: {value}."
                .format_map(self.status))
        else:
            print("CP-SAT solver cannot find an solution. Status:", self.status)

        print("Free space after operation: {} => {}.".format(
            humansize(freeSpace), humansize(finalFreeSpace)))

        for prefix in "remove", "download":
            final = getattr(self, prefix + "List")
            cand = getattr(self, prefix + "Cand")
            size = locals()[prefix + "Size"]
            print(sepSlim)
            print("{}: {}/{}. Total: {}, {} peers.".format(
                prefix.capitalize(),
                len(final),
                len(cand),
                humansize(size),
                sum(t.peer for t in final),
            ))
            for t in final:
                print(f"[{humansize(t.size):>11}|{t.peer:4d}P] {t.title}")

    @staticmethod
    def _sumsize(obj: Iterable[Torrent]) -> int:
        return sum(t.size for t in obj)


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
    parser["DEFAULT"] = {
        "host": "http://localhost/",
        "seed_dir": "",
        "disk_quota": "50",
        "up_rate_thresh": "2700",
        "dl_rate_thresh": "6000",
        "dead_torrent_up_thresh": "2",
        "log_backup_dir": "",
    }
    parser["MTEAM"] = {
        "username": "",
        "password": "",
        "peer_slope": "0.3",
        "peer_intercept": "30",
        "feeds": "example1.php\nexample2.php",
    }
    parser["DEBUG"] = {
        "host": "http://localhost",
        "seed_dir": "",
    }

    if parser.read(configfile, encoding="utf-8"):

        basic = parser["DEFAULT"]

        for arg in sys.argv[1:]:
            if arg.startswith("-d"):
                _debug = True
            elif arg.startswith("-r"):
                _debug = True
                basic = parser["DEBUG"]
            else:
                raise ValueError(f"Unrecognized argument: '{arg}'")

        return basic, parser["MTEAM"]

    with open(configfile, "w", encoding="utf-8") as f:
        parser.write(f)

    print("Please edit config.ini before running this script again.")
    sys.exit()


def main():

    join_root = Path(__file__).with_name
    basic, mt = read_config(join_root("config.ini"))

    qb = qBittorrent(
        host=basic["host"],
        seed_dir=basic["seed_dir"],
        disk_quota=basic.getfloat("disk_quota"),
        speed_thresh=(basic.getint("up_rate_thresh"),
                      basic.getint("dl_rate_thresh")),
        dead_thresh=basic.getint("dead_torrent_up_thresh"),
        datafile=join_root("data"),
    )
    qb.clean_seeddir()

    if qb.need_action() or _debug:

        mteam = MTeam(
            feeds=mt["feeds"].split(),
            username=mt["username"],
            password=mt["password"],
            minPeer=(mt.getfloat("peer_slope"), mt.getint("peer_intercept")),
            qb=qb,
        )
        solver = MPSolver(
            removeCand=qb.get_remove_cands(),
            downloadCand=mteam.fetch(),
            qb=qb,
        )
        solver.solve()

        qb.remove_torrents(solver.removeList)
        qb.add_torrent(solver.downloadList,
                       content=mteam.download(solver.downloadList))
        solver.report()

    qb.resume_paused()
    qb.dump_data()

    if logger:
        logger.write(
            join_root("logfile.log"),
            copy_to=basic["log_backup_dir"],
        )


logger = Logger()

if __name__ == "__main__":
    main()
