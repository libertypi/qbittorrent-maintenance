import pickle
from collections import namedtuple
from configparser import ConfigParser
from heapq import heappop, heappush
from pathlib import Path
from re import compile as re_compile
from shutil import disk_usage, rmtree
from sys import argv
from urllib.parse import urljoin

import pandas as pd
import requests

Alert = namedtuple("Alert", ("interval", "type"))
Removable = namedtuple("Removable", ("hash", "size", "peer", "title"))
Torrent = namedtuple("Torrent", ("tid", "size", "peer", "title", "link", "expire"))

_debug = False
now = pd.Timestamp.now()
byteUnit = {u: s for us, s in zip(((f"{u}B", f"{u}iB") for u in "KMGTP"), (1024 ** s for s in range(1, 6))) for u in us}


class AlertQue(list):

    SKIP = 0
    FETCH = 1

    def get_alert(self):
        """Get the most recent alert, if any.

        SKIP will be returned during its duration, others only once.
        """
        while self and now >= self[0].interval.left:
            if now in self[0].interval:
                if self[0].type == self.SKIP:
                    return self.SKIP
                return heappop(self).type
            heappop(self)

    def add_alert(self, start: pd.Timestamp, span: str, _type: int):
        """Add a new alert to the que.

        span decide the duration, can also start with "-" and "+-".
        """
        offset = pd.Timedelta(span.lstrip("+-"))
        if span.startswith("+-"):
            start, stop = start - offset, start + offset
        elif span.startswith("-"):
            start, stop = start - offset, start
        else:
            stop = start + offset
        heappush(
            self,
            Alert(interval=pd.Interval(start, stop, closed="both"), type=_type),
        )

    def clear_current_alert(self):
        while self and now in self[0].interval:
            heappop(self)


class Data:
    def __init__(self):
        self.qBittorrentFrame = pd.DataFrame()
        self.torrentFrame = pd.DataFrame()
        self.mteamHistory = set()
        self.alertQue = AlertQue()
        self.init_session()

    def init_session(self):
        """Initialize a new requests session."""
        self.session = requests.session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0"}
        )
        return self.session

    def integrity_test(self):
        try:
            return all(
                isinstance(x, y)
                for x, y in (
                    (self.qBittorrentFrame, pd.DataFrame),
                    (self.torrentFrame, pd.DataFrame),
                    (self.alertQue, AlertQue),
                    (self.mteamHistory, set),
                    (self.session, requests.Session),
                )
            )
        except AttributeError:
            return False


class qBittorrent:
    def __init__(self, *, host: str, seedDir: str, speedThresh: tuple, spaceQuota: int, datafile: Path):

        self.api_base = urljoin(host, "api/v2/")
        maindata = self._request("sync/maindata").json()

        self.state = maindata["server_state"]
        self.torrents = maindata["torrents"]
        if self.state["connection_status"] not in ("connected", "firewalled"):
            raise RuntimeError("qBittorrent is not connected to the internet.")

        try:
            self.seedDir = Path(seedDir)
        except TypeError:
            self.seedDir = None

        self.upSpeedThresh, self.dlSpeedThresh = (int(i * byteUnit["MiB"]) for i in speedThresh)
        self.spaceQuota = int(spaceQuota * byteUnit["GiB"])
        self.torrentState = {i["state"] for i in self.torrents.values()}
        self._preferences = None

        self.datafile = datafile
        self.data = self._load_data()
        self.upSpeed, self.dlSpeed = self._record()

        print(
            "qBittorrent average speed last hour: UL: {}/s, DL: {}/s. ".format(
                humansize(self.upSpeed), humansize(self.dlSpeed)
            )
        )

    def _record(self):
        """Record qBittorrent traffic data to pandas DataFrame.

        Calculate the last hour avg UL/DL speeds."""

        data = self.data
        qBittorrentRow = pd.DataFrame(
            {"upload": self.state["alltime_ul"], "download": self.state["alltime_dl"]},
            index=(now,),
        )
        torrentRow = pd.DataFrame(
            {k: v["uploaded"] for k, v in self.torrents.items()},
            index=(now,),
        )

        try:
            data.qBittorrentFrame = data.qBittorrentFrame.truncate(
                before=(now - pd.Timedelta("1H")), copy=False
            ).append(qBittorrentRow)
        except (TypeError, AttributeError):
            data.qBittorrentFrame = qBittorrentRow

        try:
            diff = data.torrentFrame.columns.difference(torrentRow.columns)
            if not diff.empty:
                data.torrentFrame.drop(columns=diff, inplace=True, errors="ignore")
                data.torrentFrame.dropna(how="all", inplace=True)
            data.torrentFrame = data.torrentFrame.append(torrentRow)
        except (TypeError, AttributeError):
            data.torrentFrame = torrentRow

        hi = data.qBittorrentFrame.iloc[-1]
        lo = data.qBittorrentFrame.iloc[0]
        speeds = (hi - lo) // (hi.name - lo.name).total_seconds()
        return speeds["upload"], speeds["download"]

    def get_preference(self, key: str):
        if self._preferences is None:
            self._preferences = self._request("app/preferences").json()
        return self._preferences[key]

    def _request(self, path: str, **kwargs):
        res = requests.get(self.api_base + path, **kwargs, timeout=7)
        res.raise_for_status()
        return res

    def _load_data(self):
        """Load Data object from pickle."""
        try:
            with self.datafile.open(mode="rb") as f:
                data = pickle.load(f)
            assert data.integrity_test(), "Intergrity test failed."
        except (OSError, pickle.PickleError, AssertionError) as e:
            if self.datafile.exists() and not _debug:
                print(f"Loading data from '{self.datafile}' failed: {e}")
                self.datafile.rename(f"{self.datafile}_{now.strftime('%y%m%d_%H%M%S')}")
            data = Data()
        return data

    def clean_seedDir(self):
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
                    log.record("Cleanup", None, path.name)

    def need_action(self) -> bool:
        """True if speed is low, space is low, or an alert is near."""

        realSpace: int = self.state["free_space_on_disk"]
        try:
            realSpace = max(realSpace, disk_usage(self.seedDir).free)
        except TypeError:
            pass
        self.freeSpace = realSpace - self.spaceQuota - sum(i["amount_left"] for i in self.torrents.values())

        if self.freeSpace < 0:
            return True

        alert = self.data.alertQue.get_alert()
        if alert == AlertQue.SKIP:
            return False

        return (
            alert == AlertQue.FETCH
            or 0 <= self.upSpeed < self.upSpeedThresh
            and 0 <= self.dlSpeed < self.dlSpeedThresh
            and "queuedDL" not in self.torrentState
            and not self.state["use_alt_speed_limits"]
            and self.state["up_rate_limit"] > self.upSpeedThresh
        )

    def get_remove_cands(self):
        """Discover the slowest torrents using jenks natural breaks."""
        from jenkspy import jenks_breaks

        df = self.data.torrentFrame = self.data.torrentFrame.truncate(
            before=(now - pd.Timedelta("24H")),
            copy=False,
        )
        hi = df.iloc[-1]
        lo = df.apply(pd.Series.first_valid_index)
        speeds = (hi.values - df.lookup(lo, lo.index)) // (hi.name - lo).dt.total_seconds()
        speeds.dropna(inplace=True)

        try:
            c = speeds.size - 1
            if c > 3:
                c = 3
            breaks = jenks_breaks(speeds, nb_class=c)[1]
        except Exception as e:
            print("Jenkspy failed:", e)
            breaks = speeds.mean()

        self.breaks = breaks
        oneDayAgo = pd.Timestamp.now(tz="UTC").timestamp() - 86400

        for k in speeds.loc[speeds <= breaks].index:
            v = self.torrents[k]
            if v["added_on"] < oneDayAgo:
                yield Removable(hash=k, size=v["size"], peer=v["num_incomplete"], title=v["name"])

    def remove_torrents(self, removeList: tuple):
        if not removeList:
            return
        path = "torrents/delete"
        payload = {"hashes": "|".join(i.hash for i in removeList), "deleteFiles": True}
        self._request(path, params=payload)
        for v in removeList:
            log.record("Remove", v.size, v.title)

    def add_torrent(self, downloadList: tuple, contents: dict):
        """Upload torrents and clear recent alerts.

        When a timelimited free torrent being added, an alert will be set on its expiry date.
        When new downloads were made, alerts on current time will be cleared.
        """

        if not contents:
            return
        try:
            res = requests.post(self.api_base + "torrents/add", files=contents)
            res.raise_for_status()
        except requests.exceptions as e:
            log.record("Error", None, e)
            return

        alertQue = self.data.alertQue
        for t in downloadList:
            log.record("Download", t.size, t.title)
            try:
                expire = now + pd.Timedelta(t.expire)
            except ValueError:
                continue
            if pd.notna(expire):
                alertQue.add_alert(expire, "+-1H", AlertQue.FETCH)

        alertQue.clear_current_alert()
        alertQue.add_alert(now, f"{len(downloadList)}H", AlertQue.SKIP)

    def resume_paused(self):
        if not self.torrentState.isdisjoint({"error", "missingFiles", "pausedUP", "pausedDL", "unknown"}):
            print("Resume torrents.")
            if not _debug:
                path = "torrents/resume"
                payload = {"hashes": "all"}
                self._request(path, params=payload)

    def dump_data(self):
        if _debug:
            return
        try:
            with self.datafile.open("wb") as f:
                pickle.dump(self.data, f)
        except (OSError, pickle.PickleError) as e:
            msg = f"Writing data to disk failed: {e}"
            log.record("Error", None, msg)
            print(msg)


class MTeam:
    """A cumbersome MTeam downloader."""

    domain = "https://pt.m-team.cc"

    def __init__(self, *, feeds: list, account: tuple, minPeer: tuple, qb: qBittorrent):
        """The minimum peer requirement subjects to: Peer >= A * Size(GiB) + B
        Where (A, B) is defined in config file and passed via "minPeer"."""
        self.feeds = feeds
        self.account = account
        self.qb = qb
        self.session = qb.data.session
        self.history = qb.data.mteamHistory
        self.minPeer = (minPeer[0] / byteUnit["GiB"], minPeer[1])

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
                self.session = self.qb.data.init_session()

    def _login(self):
        if not hasattr(self, "loginParam"):
            self.loginParam = {
                "url": f"{self.domain}/takelogin.php",
                "data": {"username": self.account[0], "password": self.account[1]},
                "headers": {"referer": f"{self.domain}/login.php"},
            }
        self.session.post(**self.loginParam)

    def fetch(self):
        from bs4 import BeautifulSoup

        re_download = re_compile(r"\bdownload\.php\?")
        re_details = re_compile(r"\bdetails\.php\?")
        re_nondigit = re_compile(r"[^0-9]+")
        re_size = re_compile(r"(?P<num>[0-9]+(\.[0-9]+)?)\s*(?P<unit>[KMGT]i?B)")
        re_tid = re_compile(r"\bid=(?P<tid>[0-9]+)")
        re_timelimit = re_compile(r"^\s*限時：")
        transTable = str.maketrans({"日": "D", "時": "H", "分": "T"})
        cols = {}
        A, B = self.minPeer

        print(f"Connecting to M-Team... Feeds: {len(self.feeds)}.")

        for feed in self.feeds:
            try:
                soup = BeautifulSoup(self._get(feed).content, "html.parser")
                soup = (tr.find_all("td", recursive=False) for tr in soup.select("#form_torrent table.torrents > tr"))
                tr = next(soup)
            except StopIteration:
                print("Unable to locate torrent table, css selector broken?")
                continue
            except Exception as e:
                print("Fetching feed failed:", e)
                continue
            else:
                print("Fetching feed success.")

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
                    size = int(float(size["num"]) * byteUnit[size["unit"]])
                    if peer < A * size + B:
                        continue

                    link = tr[colTitle].find("a", href=re_download)["href"]
                    tid = re_tid.search(link)["tid"]
                    if tid in self.history or tr[colUp].get_text(strip=True) == "0":
                        continue

                    expire = tr[colTitle].find(string=re_timelimit)
                    if expire:
                        if "日" not in expire:
                            continue
                        expire = expire.split("：", 1)[1].translate(transTable)

                    title = tr[colTitle].find("a", href=re_details, string=True)
                    title = title["title"] if title.has_attr("title") else title.get_text(strip=True)

                    yield Torrent(tid=tid, size=size, peer=peer, title=title, link=link, expire=expire)
                except Exception as e:
                    print("Parsing page error:", e)

    def download(self, downloadList: tuple):
        try:
            content = {f"{t.tid}.torrent": self._get(t.link).content for t in downloadList}
        except AttributeError:
            print(f"Downloading torrents failed.")
        else:
            self.history.update(t.tid for t in downloadList)
            return content


class MPSolver:
    """Using Google OR-Tools to find the optimal choices of downloads and removals.
    The goal is to maximize obtained peers under several constraints.

    Constraints:
        1: downloadSize <= freeSpace + removedSize
            --> downloadSize - removedSize <= freeSpace
            When freeSpace + removedSize < 0, this become impossible to satisfy.
            So the algorithm should delete all remove candidates to free up space.
        2: total download <= qBittorrent max_active_downloads

    Objective:
        Maximize: downloadPeer * 2 - removedPeer
    """

    def __init__(self, *, removeCand, downloadCand, qb: qBittorrent):
        self.downloadList = self.removeList = ()
        self.downloadCand = tuple(downloadCand)
        self.freeSpace = qb.freeSpace
        if not (self.downloadCand or self.freeSpace < 0 or _debug):
            return

        self.removeCand = tuple(removeCand)
        self.removeCandSize = sum(i.size for i in self.removeCand)
        if self.freeSpace < -self.removeCandSize:
            self.removeList = self.removeCand
            return

        self.qb = qb
        self.maxDownloads = qb.get_preference("max_active_downloads")
        self._solve()

    def _solve(self):
        from ortools.sat.python import cp_model

        model = cp_model.CpModel()
        downloadCand = self.downloadCand
        removeCand = self.removeCand

        sizeCoef = [t.size for t in downloadCand]
        peerCoef = [t.peer * 2 for t in downloadCand]
        pool = [model.NewBoolVar(f"DL_{i}") for i in range(len(downloadCand))]

        if self.maxDownloads > 0:
            model.Add(cp_model.LinearExpr.Sum(pool) <= self.maxDownloads)

        sizeCoef.extend(-t.size for t in removeCand)
        peerCoef.extend(-t.peer for t in removeCand)
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

    def report(self):
        try:
            status = self.status
        except AttributeError:
            print("Solver did not start: unnecessary conditions.")
            return

        sepSlim = "-" * 50
        removeSize = sum(i.size for i in self.removeList)
        downloadSize = sum(i.size for i in self.downloadList)
        finalFreeSpace = self.freeSpace + removeSize - downloadSize
        alertQue = self.qb.data.alertQue

        print(sepSlim)
        print(
            "Alert que: {}. Nearest: {}.".format(
                len(alertQue),
                alertQue[0].interval.left.strftime("%F %T") if alertQue else "NaT",
            )
        )
        print(
            "Disk free space: {}. Max avail space: {}.".format(
                humansize(self.freeSpace),
                humansize(self.freeSpace + self.removeCandSize),
            )
        )
        print(
            "Download candidates: {}. Total: {}. Limit: {}.".format(
                len(self.downloadCand),
                humansize(sum(i.size for i in self.downloadCand)),
                self.maxDownloads,
            )
        )
        print(
            "Remove candidates: {}/{}. Total: {}. Break: {}/s.".format(
                len(self.removeCand),
                len(self.qb.torrents),
                humansize(self.removeCandSize),
                humansize(self.qb.breaks),
            )
        )
        for v in self.removeCand:
            print(f"[{humansize(v.size):>11}|{v.peer:3d} peers] {v.title}")

        print(sepSlim)
        if isinstance(status, dict):
            print("{status} solution found in {walltime:.5f} seconds, objective value: {value}.".format_map(status))
        else:
            print("CP-SAT solver cannot find an optimal solution. Status:", status)

        print(f"Free space left after operation: {humansize(self.freeSpace)} => {humansize(finalFreeSpace)}.")

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
                    sum(i.peer for i in final),
                )
            )
            for v in final:
                print(f"[{humansize(v.size):>11}|{v.peer:3d} peers] {v.title}")


class Log(list):
    def record(self, action, size, name):
        self.append("{:20}{:12}{:14}{}\n".format(pd.Timestamp.now().strftime("%D %T"), action, humansize(size), name))

    def write(self, logfile: Path):
        if not self or _debug:
            return

        header = "{:20}{:12}{:14}{}\n{}\n".format("Date", "Action", "Size", "Name", "-" * 80)
        content = reversed(self)

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


def humansize(size: int):
    """Convert bytes to human readable sizes."""
    try:
        for suffix in ("KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"):
            size /= 1024
            if -1024 < size < 1024:
                return f"{size:.2f} {suffix}"
    except TypeError:
        pass
    return "NaN"


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


def main():
    global _debug

    script_dir = Path(__file__).parent
    datafile = script_dir / "data"
    logfile = script_dir / "logfile.log"
    configfile = script_dir / "config.ini"

    config = ConfigParser()
    if not config.read(configfile, encoding="utf-8"):
        init_config(configfile)
        print("Please edit config.ini before running this script again.")
        return

    basic = config["DEFAULT"]
    for arg in argv[1:]:
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
        solver.report()

        if not _debug:
            qb.remove_torrents(solver.removeList)
            qb.add_torrent(solver.downloadList, mteam.download(solver.downloadList))

    qb.resume_paused()
    qb.dump_data()
    log.write(logfile)


log = Log()

if __name__ == "__main__":
    main()
