import pickle
import re
import sys
from collections import Counter
from configparser import ConfigParser
from heapq import heappop, heappush
from pathlib import Path
from shutil import disk_usage, rmtree
from typing import Iterable, Iterator, Mapping, NamedTuple, Sequence, Union
from urllib.parse import urljoin

import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict

NOW = pd.Timestamp.now()
_debug: bool = False

byteSize: Mapping[str, int] = CaseInsensitiveDict(
    {k: v for kk, v in zip(((f"{c}B", f"{c}iB") for c in "KMGTP"), (1024 ** i for i in range(1, 6))) for k in kk}
)


class Removable(NamedTuple):
    """Removable torrents in qBittorrent list."""

    hash: str
    size: int
    peer: int
    title: str


class Torrent(NamedTuple):
    """Torrents to be downloaded from web."""

    tid: str
    size: int
    peer: int
    title: str
    link: str
    expire: str


class AlarmClock:
    """Create and manage alarms.

    Internally, alarms are stord in a heap, as tuples: (time, type)
    """

    SKIP = 0
    FETCH = 1

    def __init__(self) -> None:
        self._data = []

    def __len__(self) -> int:
        return len(self._data)

    def nearest(self):
        if self._data:
            alarm = self._data[0]
            return f'{alarm[0].left.strftime("%F %T")} [#{alarm[1]}]'

    def get_alarm(self) -> Union[int, None]:
        """Get the most recent alarm, if any.

        Returns SKIP during its duration, others only once.
        """
        data = self._data
        while data and NOW >= data[0][0].left:
            if NOW in data[0][0]:
                if data[0][1] == self.SKIP:
                    return self.SKIP
                return heappop(data)[1]
            heappop(data)

    def set_alarm(self, start: pd.Timestamp, span: str, _type: int):
        """Add a new alarm to the que.

        span can start with "+-".
        """
        if span.startswith("+-"):
            offset = pd.Timedelta(span.lstrip("+-"))
            start, stop = start - offset, start + offset
        else:
            offset = pd.Timedelta(span)
            stop = start + offset
            if start > stop:
                start, stop = stop, start

        heappush(self._data, (pd.Interval(start, stop, closed="both"), _type))

    def clear_current_alarm(self):
        while self._data and NOW in self._data[0][0]:
            heappop(self._data)


class Logger:
    """Record and write logs in reversed order."""

    def __init__(self) -> None:
        self._log = []

    def record(self, action: str, size: int, name: str):
        self._log.append(
            "{:20}{:12}{:14}{}\n".format(
                pd.Timestamp.now().strftime("%D %T"),
                action,
                humansize(size),
                name,
            ),
        )

    def write(self, logfile: Path):
        if not self._log or _debug:
            return

        header = "{:20}{:12}{:14}{}\n{}\n".format("Date", "Action", "Size", "Name", "-" * 80)
        content = reversed(self._log)

        try:
            with open(logfile, mode="r+", encoding="utf-8") as f:
                for _ in range(2):
                    f.readline()
                backup = f.read()
                f.seek(0)
                f.write(header)
                f.writelines(content)
                f.write(backup)
                f.truncate()
        except FileNotFoundError:
            with open(logfile, mode="w", encoding="utf-8") as f:
                f.write(header)
                f.writelines(content)


logger = Logger()


class qBittorrent:
    """The manager class to communicate with qBittorrent and manange data persistence."""

    def __init__(self, *, host: str, seedDir: str, speedThresh: Sequence[float], spaceQuota: float, datafile: Path):

        self.api_base = urljoin(host, "api/v2/")
        maindata = self._request("sync/maindata").json()

        self.state: dict = maindata["server_state"]
        self.torrents: dict = maindata["torrents"]
        if self.state["connection_status"] not in ("connected", "firewalled"):
            raise RuntimeError("qBittorrent is not connected to the internet.")

        try:
            self.seedDir = Path(seedDir)
        except TypeError:
            self.seedDir = None

        self.upSpeedThresh, self.dlSpeedThresh = (i * byteSize["MiB"] for i in speedThresh)
        self.spaceQuota = spaceQuota * byteSize["GiB"]
        self.stateCounter = Counter(i["state"] for i in self.torrents.values())

        self.datafile = datafile
        self._load_data()
        self._record()

    def _load_data(self):
        """Load data objects from pickle."""

        self.speedFrame: pd.DataFrame
        self.torrentFrame: pd.DataFrame
        self.alarmClock: AlarmClock
        self.mteamHistory: set
        self.session: requests.Session

        try:
            with self.datafile.open(mode="rb") as f:
                self.speedFrame, self.torrentFrame, self.alarmClock, self.mteamHistory, self.session = pickle.load(f)

        except Exception as e:
            if self.datafile.exists():
                print(f"Reading '{self.datafile}' failed: {e}")
                if not _debug:
                    self.datafile.rename(f"{self.datafile}_{NOW.strftime('%y%m%d_%H%M%S')}")

            self.speedFrame = self.torrentFrame = None
            self.alarmClock = AlarmClock()
            self.mteamHistory = set()
            self.init_session()

    def dump_data(self):
        """Save data to disk."""

        if _debug:
            return

        try:
            with self.datafile.open("wb") as f:
                pickle.dump((self.speedFrame, self.torrentFrame, self.alarmClock, self.mteamHistory, self.session), f)

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

        speedRow = pd.DataFrame(
            {"upload": self.state["alltime_ul"], "download": self.state["alltime_dl"]}, index=(NOW,)
        )
        torrentRow = pd.DataFrame({k: v["uploaded"] for k, v in self.torrents.items()}, index=(NOW,))

        try:
            self.speedFrame = self.speedFrame.truncate(before=(NOW - pd.Timedelta("1H")), copy=False).append(speedRow)
        except (TypeError, AttributeError):
            self.speedFrame = speedRow

        try:
            diff = self.torrentFrame.columns.difference(torrentRow.columns)
            if not diff.empty:
                self.torrentFrame.drop(columns=diff, inplace=True, errors="ignore")
                self.torrentFrame.dropna(how="all", inplace=True)

            self.torrentFrame = self.torrentFrame.append(torrentRow)
        except (TypeError, AttributeError):
            self.torrentFrame = torrentRow

    def _request(self, path: str, *, method: str = "GET", **kwargs):
        """Communicate with qBittorrent API."""

        res = requests.request(method, self.api_base + path, timeout=7, **kwargs)
        res.raise_for_status()
        return res

    def get_preference(self, key: str):
        """Query qBittorrent preferences by key."""

        if not hasattr(self, "_preferences"):
            self._preferences = self._request("app/preferences").json()

        return self._preferences[key]

    def clean_seedDir(self):
        """Clean files in seed dir which does not belong to qb download list."""

        try:
            listdir = self.seedDir.iterdir()
        except AttributeError:
            return

        names = {i["name"] for i in self.torrents.values()}
        for path in listdir:
            if path.name not in names:
                if path.suffix == ".!qB" and path.stem in names:
                    continue
                print("Cleanup:", path.name)
                try:
                    if _debug:
                        pass
                    elif path.is_dir():
                        rmtree(path)
                    else:
                        path.unlink()
                except OSError as e:
                    print("Deletion Failed:", e)
                else:
                    logger.record("Cleanup", None, path.name)

    def need_action(self) -> bool:
        """True if speed is low, space is low, or an alarm is close."""

        realSpace = self.state["free_space_on_disk"]
        try:
            realSpace = max(realSpace, disk_usage(self.seedDir).free)
        except TypeError:
            pass
        self.freeSpace = int(realSpace - self.spaceQuota - sum(i["amount_left"] for i in self.torrents.values()))
        if self.freeSpace < 0:
            return True

        if not hasattr(self, "thisAlarm"):
            self.thisAlarm = self.alarmClock.get_alarm()
        if self.thisAlarm is not None:
            return self.thisAlarm == AlarmClock.FETCH

        hi = self.speedFrame.iloc[-1]
        lo = self.speedFrame.iloc[0]
        speeds = (hi - lo) // (hi.name - lo.name).total_seconds()
        print("Last hour avg speed: UL: {upload}/s, DL: {download}/s.".format_map(speeds.apply(humansize)))

        return (
            0 <= speeds["upload"] < self.upSpeedThresh
            and 0 <= speeds["download"] < self.dlSpeedThresh
            and "queuedDL" not in self.stateCounter
            and not self.state["use_alt_speed_limits"]
            and not 0 < self.state["up_rate_limit"] < self.upSpeedThresh
        )

    def get_remove_cands(self) -> Iterator[Removable]:
        """Discover the slowest torrents using jenks natural breaks."""

        from jenkspy import jenks_breaks

        df = self.torrentFrame = self.torrentFrame.truncate(
            before=(NOW - pd.Timedelta("24H")),
            copy=False,
        )

        hi = df.iloc[-1]
        lo = df.apply(pd.Series.first_valid_index)

        speeds: pd.Series
        speeds = (hi.values - df.lookup(lo, lo.index)) // (hi.name - lo).dt.total_seconds()
        speeds.dropna(inplace=True)

        try:
            c = speeds.size - 1
            if c > 3:
                c = 3
            self.breaks = jenks_breaks(speeds, nb_class=c)[1]

        except Exception as e:
            print("Jenkspy failed:", e)
            self.breaks = speeds.mean()

        oneDayAgo = pd.Timestamp.now(tz="UTC").timestamp() - 86400

        for k in speeds.loc[speeds <= self.breaks].index:
            v = self.torrents[k]
            if v["added_on"] < oneDayAgo:
                yield Removable(
                    hash=k,
                    size=v["size"],
                    peer=v["num_incomplete"],
                    title=v["name"],
                )

    def remove_torrents(self, removeList: Sequence[Removable]):
        """Remove torrents and files."""

        if not removeList or _debug:
            return

        self._request(
            "torrents/delete",
            params={"hashes": "|".join(i.hash for i in removeList), "deleteFiles": True},
        )
        for v in removeList:
            logger.record("Remove", v.size, v.title)

    def add_torrent(self, downloadList: Sequence[Torrent], content: Mapping[str, bytes]):
        """Upload torrents and clear recent alarms.

        When a timelimited free torrent being added, an alarm will be set on its expiry date.
        When new downloads were made, alarms on current time will be cleared.
        """

        if not _debug:
            try:
                self._request("torrents/add", method="POST", files=content)
            except requests.RequestException as e:
                logger.record("Error", None, e)
                return False

        for t in downloadList:
            logger.record("Download", t.size, t.title)
            try:
                expire = NOW + pd.Timedelta(t.expire)
            except ValueError:
                continue
            if pd.notna(expire):
                self.alarmClock.set_alarm(expire, "2H", AlarmClock.FETCH)

        self.alarmClock.clear_current_alarm()
        self.alarmClock.set_alarm(NOW, f"{len(downloadList)}H", AlarmClock.SKIP)
        return True

    def resume_paused(self):
        """If any torrent is paused, for any reason, resume."""

        paused = {"error", "missingFiles", "pausedUP", "pausedDL", "unknown"}

        if not paused.isdisjoint(self.stateCounter):
            print("Resume torrents.")
            if not _debug:
                self._request("torrents/resume", params={"hashes": "all"})


class MTeam:
    """A cumbersome MTeam downloader."""

    domain = "https://pt.m-team.cc"

    def __init__(self, *, feeds: Sequence[str], account: Sequence[str], minPeer: Sequence[float], qb: qBittorrent):
        """The minimum peer requirement subjects to: Peer >= A * Size(GiB) + B
        Where (A, B) is defined in config file and passed via "minPeer"."""

        self.feeds = feeds
        self.account = account
        self.minPeer = minPeer[0] / byteSize["GiB"], minPeer[1]
        self.qb = qb
        self.session = qb.session
        self.history = qb.mteamHistory

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
            except (requests.ConnectionError, requests.HTTPError, requests.Timeout):
                pass
            except Exception:
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

        A, B = self.minPeer
        re_download = re.compile(r"\bdownload\.php\?")
        re_details = re.compile(r"\bdetails\.php\?")
        re_nondigit = re.compile(r"[^0-9]+")
        re_size = re.compile(r"(?P<num>[0-9]+(\.[0-9]+)?)\s*(?P<unit>[KMGT]i?B)", flags=re.I)
        re_tid = re.compile(r"\bid=(?P<tid>[0-9]+)")
        re_timelimit = re.compile(r"^\s*限時：")
        transTable = str.maketrans({"日": "D", "時": "H", "分": "T"})
        visited = set()
        cols = {}

        print(f"Connecting to M-Team... Feeds: {len(self.feeds)}.")

        for feed in self.feeds:
            try:
                soup = BeautifulSoup(self._get(feed).content, "html.parser")
                soup = (tr.find_all("td", recursive=False) for tr in soup.select("#form_torrent table.torrents > tr"))
                tr = next(soup)
            except StopIteration:
                print("Unable to locate table, css selector broken?", feed)
                continue
            except AttributeError:
                print("Fetching failed:", feed)
                continue
            except Exception as e:
                print("Parsing error:", e)
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

            for tr in soup:
                try:
                    peer = int(re_nondigit.sub("", tr[colDown].get_text()))
                    size = re_size.search(tr[colSize].get_text())
                    size = int(float(size["num"]) * byteSize[size["unit"]])
                    if peer < A * size + B:
                        continue

                    link = tr[colTitle].find("a", href=re_download)["href"]
                    tid = re_tid.search(link)["tid"]
                    if tid in self.history or tr[colUp].get_text(strip=True) == "0" or tid in visited:
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
                    yield Torrent(tid=tid, size=size, peer=peer, title=title, link=link, expire=expire)

    def download(self, downloadList: Sequence[Torrent]):
        """Download torrents from mteam, then pass to qBittorrent uploader."""

        if not downloadList:
            return

        try:
            content = {t.tid: self._get(t.link).content for t in downloadList}
        except AttributeError:
            print(f"Downloading torrents failed.")
            return

        if self.qb.add_torrent(downloadList, content):
            self.history.update(t.tid for t in downloadList)


class MPSolver:
    """Using Google OR-Tools to find the optimal choices of downloads and removals.
    The goal is to maximize obtained peers under several constraints.

    Constraints:
        1: downloadSize <= freeSpace + removedSize
            --> downloadSize - removedSize <= freeSpace
            When freeSpace + removedSize < 0, this become impossible to satisfy.
            So the algorithm should delete all remove candidates to free up space.
        2: total downloads <= qb max_active_downloads - current_downloading
            so we don't add torrents to the queue.

    Objective:
        Maximize: downloadPeer - 3/4 * removedPeer
    """

    def __init__(self, *, removeCand: Iterable[Removable], downloadCand: Iterable[Torrent], qb: qBittorrent):

        self.downloadList = self.removeList = ()
        self.freeSpace = qb.freeSpace
        self.downloadCand = tuple(downloadCand)
        if not (self.downloadCand or self.freeSpace < 0 or _debug):
            return

        self.removeCand = tuple(removeCand)
        self.removeCandSize = sum(i.size for i in self.removeCand)
        if self.freeSpace < -self.removeCandSize:
            self.removeList = self.removeCand
            return

        maxDownloads: int = qb.get_preference("max_active_downloads")
        if maxDownloads > 0:
            maxDownloads -= qb.stateCounter["downloading"]
            if maxDownloads <= 0:
                return
        self.maxDownloads = maxDownloads

        self._solve()

    def _solve(self):

        from ortools.sat.python import cp_model

        model = cp_model.CpModel()
        downloadCand = self.downloadCand
        removeCand = self.removeCand

        sizeCoef = [t.size for t in downloadCand]
        peerCoef = [4 * t.peer for t in downloadCand]
        pool = [model.NewBoolVar(f"DL_{i}") for i in range(len(downloadCand))]

        if self.maxDownloads > 0:
            model.Add(cp_model.LinearExpr.Sum(pool) <= self.maxDownloads)

        sizeCoef.extend(-t.size for t in removeCand)
        peerCoef.extend(-3 * t.peer for t in removeCand)
        pool.extend(model.NewBoolVar(f"RM_{i}") for i in range(len(removeCand)))

        model.Add(cp_model.LinearExpr.ScalProd(pool, sizeCoef) <= self.freeSpace)
        model.Maximize(cp_model.LinearExpr.ScalProd(pool, peerCoef))

        solver = cp_model.CpSolver()
        status = solver.Solve(model)

        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
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


def report(qb: qBittorrent, solver: MPSolver):
    """Output report information to stdout."""

    try:
        status = solver.status
    except AttributeError:
        print("Solver did not start: unnecessary conditions.")
        return

    sepSlim = "-" * 50
    removeSize = sum(i.size for i in solver.removeList)
    downloadSize = sum(i.size for i in solver.downloadList)
    finalFreeSpace = qb.freeSpace + removeSize - downloadSize

    print(sepSlim)
    print(
        "Alarm: {}. Nearest: {}. Current: [{}].".format(
            len(qb.alarmClock),
            qb.alarmClock.nearest(),
            qb.thisAlarm,
        )
    )
    print(
        "Disk free space: {}. Max avail space: {}.".format(
            humansize(qb.freeSpace),
            humansize(qb.freeSpace + solver.removeCandSize),
        )
    )
    print(
        "Download candidates: {}. Total: {}. Limit: {}.".format(
            len(solver.downloadCand),
            humansize(sum(i.size for i in solver.downloadCand)),
            solver.maxDownloads,
        )
    )
    print(
        "Remove candidates: {}/{}. Total: {}. Break: {}/s.".format(
            len(solver.removeCand),
            len(qb.torrents),
            humansize(solver.removeCandSize),
            humansize(qb.breaks),
        )
    )
    for v in solver.removeCand:
        print(f"[{humansize(v.size):>11}|{v.peer:3d} peers] {v.title}")

    print(sepSlim)
    if isinstance(status, dict):
        print("{status} solution found in {walltime:.5f} seconds, objective value: {value}.".format_map(status))
    else:
        print("CP-SAT solver cannot find an optimal solution. Status:", status)

    print(f"Free space left after operation: {humansize(qb.freeSpace)} => {humansize(finalFreeSpace)}.")

    for prefix in "remove", "download":
        final = getattr(solver, prefix + "List")
        cand = getattr(solver, prefix + "Cand")
        size = locals()[prefix + "Size"]
        print(sepSlim)
        print(
            "{}: {}/{}. Total: {}, {} peers.".format(
                prefix.capitalize(),
                len(final),
                len(cand),
                humansize(size),
                sum(i.peer for i in final),
            )
        )
        for v in final:
            print(f"[{humansize(v.size):>11}|{v.peer:3d} peers] {v.title}")


def init_config(configfile: Path):
    """Create config file with default values."""

    config = ConfigParser()

    config["DEFAULT"] = {
        "host": "http://localhost",
        "seed_dir": "",
        "space_quota": "50",
        "upspeed_thresh": "2.6",
        "dlspeed_thresh": "6",
    }
    config["MTEAM"] = {
        "account": "",
        "password": "",
        "peer_slope": "0.3",
        "peer_intercept": "30",
        "feeds": "\nexample1.php\nexample2.php",
    }
    config["OVERRIDE"] = {
        "host": "http://localhost",
        "seed_dir": "",
    }
    with configfile.open("w", encoding="utf-8") as f:
        config.write(f)

    print("Please edit config.ini before running this script again.")


def main():
    global _debug

    script_dir = Path(__file__).parent
    datafile = script_dir / "data"
    logfile = script_dir / "logfile.log"
    configfile = script_dir / "config.ini"

    config = ConfigParser()
    if not config.read(configfile, encoding="utf-8"):
        init_config(configfile)
        return

    basic = config["DEFAULT"]
    for arg in sys.argv[1:]:
        if arg == "-d":
            _debug = True
        elif arg == "-r":
            _debug = True
            basic = config["OVERRIDE"]
        else:
            raise ValueError(f"Unrecognized argument: '{arg}'")

    qb = qBittorrent(
        host=basic["host"],
        seedDir=basic["seed_dir"] or None,
        speedThresh=(basic.getfloat("upspeed_thresh"), basic.getfloat("dlspeed_thresh")),
        spaceQuota=basic.getfloat("space_quota"),
        datafile=datafile,
    )
    qb.clean_seedDir()

    if qb.need_action() or _debug:

        mt = config["MTEAM"]
        mteam = MTeam(
            feeds=mt["feeds"].split(),
            account=(mt["account"], mt["password"]),
            minPeer=(mt.getfloat("peer_slope"), mt.getfloat("peer_intercept")),
            qb=qb,
        )
        solver = MPSolver(
            removeCand=qb.get_remove_cands(),
            downloadCand=mteam.fetch(),
            qb=qb,
        )
        report(qb, solver)
        qb.remove_torrents(solver.removeList)
        mteam.download(solver.downloadList)

    qb.resume_paused()
    qb.dump_data()
    logger.write(logfile)


if __name__ == "__main__":
    main()
