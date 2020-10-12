import os
import pickle
import re
import shutil
from collections import namedtuple
from sys import argv
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from jenkspy import jenks_breaks
from ortools.linear_solver import pywraplp

byteUnit = {u: s for us, s in zip(((f"{u}B", f"{u}iB") for u in "KMGTP"), (1024 ** s for s in range(1, 6))) for u in us}


class qBittorrent:

    Removable = namedtuple("Removable", ("hash", "size", "peer", "title"))

    def __init__(self, *, host: str, seedDir: str, watchDir: str, speedThresh: tuple, spaceQuota: int, datafile: str):

        self.api_baseurl = urljoin(host, "api/v2/")
        path = "sync/maindata"
        maindata = self._request(path).json()
        self.state = maindata["server_state"]
        self.torrents = maindata["torrents"]
        if self.state["connection_status"] not in ("connected", "firewalled"):
            raise RuntimeError("qBittorrent is not connected to the internet.")

        try:
            self.seedDir = os.path.abspath(seedDir)
            self.watchDir = os.path.abspath(watchDir)
        except TypeError as e:
            if not debug:
                raise ValueError("seedDir and watchDir were not set properly.") from e
            self.seedDir = self.watchDir = None

        self.upSpeedThresh, self.dlSpeedThresh = (int(i * byteUnit["MiB"]) for i in speedThresh)
        self.spaceQuota = int(spaceQuota * byteUnit["GiB"])

        self.datafile = datafile
        self.data = self._load_data()
        self.upSpeed, self.dlSpeed = self.data.record(self)
        print(f"qBittorrent average speed last hour: UL: {humansize(self.upSpeed)}/s, DL: {humansize(self.dlSpeed)}/s.")

    def _request(self, path: str, **kwargs):
        response = requests.get(urljoin(self.api_baseurl, path), **kwargs, timeout=7)
        response.raise_for_status()
        return response

    def _load_data(self):
        """Load Data object from pickle."""
        try:
            with open(self.datafile, mode="rb") as f:
                data = pickle.load(f)
            assert data.integrity_test(), "Intergrity test failed."
        except Exception as e:
            print(f"Loading data from '{self.datafile}' failed: {e}")
            if not debug:
                try:
                    os.rename(self.datafile, f"{self.datafile}_{pd.Timestamp.now().strftime('%y%m%d_%H%M%S')}")
                except OSError:
                    pass
            data = Data()
        return data

    def clean_seedDir(self):
        if self.seedDir is None:
            return
        re_ext = re.compile(r"\.!qB$")
        names = frozenset(i["name"] for i in self.torrents.values())
        with os.scandir(self.seedDir) as it:
            for entry in it:
                if re_ext.sub("", entry.name) not in names:
                    print("Cleanup:", entry.name)
                    try:
                        if not debug:
                            if entry.is_dir():
                                shutil.rmtree(entry.path)
                            else:
                                os.remove(entry.path)
                        log.record("Cleanup", None, entry.name)
                    except OSError as e:
                        print("Deletion Failed:", e)

    def need_action(self) -> bool:
        realSpace = self.state["free_space_on_disk"]
        try:
            realSpace = max(realSpace, shutil.disk_usage(self.seedDir).free)
        except TypeError:
            pass
        self.freeSpace = realSpace - sum(i["amount_left"] for i in self.torrents.values()) - self.spaceQuota

        return (
            0 <= self.upSpeed < self.upSpeedThresh
            and 0 <= self.dlSpeed < self.dlSpeedThresh
            and not self.state["use_alt_speed_limits"]
            and self.state["up_rate_limit"] > self.upSpeedThresh
        ) or self.freeSpace < 0

    def get_remove_cands(self):
        oneDayAgo = pd.Timestamp.now(tz="UTC").timestamp() - 86400
        for k in self.data.get_slows():
            v = self.torrents[k]
            if v["added_on"] < oneDayAgo:
                yield self.Removable(hash=k, size=v["size"], peer=v["num_incomplete"], title=v["name"])

    def remove_torrents(self, removeList: tuple):
        if removeList and not debug:
            path = "torrents/delete"
            payload = {"hashes": "|".join(i.hash for i in removeList), "deleteFiles": True}
            self._request(path, params=payload)
        for v in removeList:
            log.record("Remove", v.size, v.title)

    def add_torrent(self, filename: str, content: bytes) -> bool:
        """Save torrent to watchdir."""
        if not debug:
            try:
                path = os.path.join(self.watchDir, filename)
                with open(path, "wb") as f:
                    f.write(content)
            except OSError as e:
                try:
                    if not os.path.exists(self.watchDir):
                        os.mkdir(self.watchDir)
                        return self.add_torrent(filename, content)
                except OSError:
                    pass
                log.record("Error", None, f"Saving '{filename}' to '{self.watchDir}' failed: {e}")
                print("Saving failed:", e)
                return False
        return True

    def resume_paused(self):
        paused = {"error", "missingFiles", "pausedUP", "pausedDL", "unknown"}
        if any(i["state"] in paused for i in self.torrents.values()):
            print("Resume torrents.")
            if not debug:
                path = "torrents/resume"
                payload = {"hashes": "all"}
                self._request(path, params=payload)

    def dump_data(self, backupDir: str):
        if debug:
            return
        try:
            with open(self.datafile, "wb") as f:
                pickle.dump(self.data, f)
            copy_backup(self.datafile, backupDir)
        except (OSError, pickle.PickleError) as e:
            log.record("Error", None, f"Writing data to disk failed: {e}")
            print("Writing data to disk failed:", e)


class Data:
    def __init__(self):
        self.qBittorrentFrame = pd.DataFrame()
        self.torrentFrame = pd.DataFrame()
        self.mteamHistory = set()
        self.init_session()

    def init_session(self):
        """Initialize a new requests session."""
        self.session = requests.session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0"}
        )
        return self.session

    def integrity_test(self):
        return all(
            isinstance(x, y)
            for x, y in (
                (self.qBittorrentFrame, pd.DataFrame),
                (self.torrentFrame, pd.DataFrame),
                (self.mteamHistory, set),
                (self.session, requests.Session),
            )
        )

    def record(self, qb: qBittorrent):
        """Record qBittorrent traffic data to pandas DataFrame. Returns the last hour avg UL/DL speeds."""

        now = (pd.Timestamp.now(),)
        qBittorrentRow = pd.DataFrame({"upload": qb.state["alltime_ul"], "download": qb.state["alltime_dl"]}, index=now)
        torrentRow = pd.DataFrame({k: v["uploaded"] for k, v in qb.torrents.items()}, index=now)

        try:
            self.qBittorrentFrame = self.qBittorrentFrame.last("7D").append(qBittorrentRow)
        except (TypeError, AttributeError):
            self.qBittorrentFrame = qBittorrentRow
        try:
            difference = self.torrentFrame.columns.difference(torrentRow.columns)
            if not difference.empty:
                self.torrentFrame.drop(columns=difference, inplace=True, errors="ignore")
                self.torrentFrame.dropna(how="all", inplace=True)
            self.torrentFrame = self.torrentFrame.append(torrentRow)
        except (TypeError, AttributeError):
            self.torrentFrame = torrentRow

        speeds = self.qBittorrentFrame.last("H").resample("T").bfill().diff().mean().floordiv(60)
        return speeds["upload"], speeds["download"]

    def get_slows(self) -> pd.Index:
        """Discover the slowest torrents using jenks natural breaks method."""
        speeds = self.torrentFrame.last("D").resample("T").bfill().diff().mean()
        try:
            breaks = jenks_breaks(speeds, nb_class=min(speeds.count().item() - 1, 4))[1]
        except Exception:
            breaks = speeds.mean()
        return speeds.loc[speeds <= breaks].index


class MTeam:
    """An optimized MTeam downloader.

    The per torrent minimum peer requirement subjects to:
        peer = a * size(GiB) + b

    Where (a, b) is defined in config file and passed through parameter "minPeer".
    """

    domain = "https://pt.m-team.cc"
    Torrent = namedtuple("Torrent", ("tid", "size", "peer", "title", "link"))

    def __init__(self, *, feeds: tuple, account: tuple, minPeer: tuple, qb: qBittorrent):
        self.feeds = feeds
        self.loginPayload = {"username": account[0], "password": account[1]}
        self.qb = qb
        self.session = qb.data.session
        self.history = qb.data.mteamHistory
        self.loginPage = urljoin(self.domain, "takelogin.php")
        self.loginReferer = {"referer": urljoin(self.domain, "login.php")}
        try:
            a = minPeer[0] / byteUnit["GiB"]
            b = minPeer[1]
        except (IndexError, TypeError):
            a = b = 0
        self.bellowMinPeer = lambda size, peer: peer < a * size + b

    def _get(self, url: str):
        for i in range(3):
            try:
                response = self.session.get(url, timeout=(7, 28))
                response.raise_for_status()
                if "/login.php" not in response.url:
                    return response
                assert i < 2, "Login failed."
                print("Login...")
                self.session.post(self.loginPage, data=self.loginPayload, headers=self.loginReferer)
            except Exception as e:
                print(e)
                if i < 2:
                    print("Retrying... Attempt:", i + 1)
                    self.session = self.qb.data.init_session()

    def fetch(self):
        re_download = re.compile(r"\bdownload\.php\?")
        re_details = re.compile(r"\bdetails\.php\?")
        re_timelimit = re.compile(r"限時：[^日]*$")
        re_nondigit = re.compile(r"[^0-9]+")
        re_size = re.compile(r"(?P<num>[0-9]+(\.[0-9]+)?)\s*(?P<unit>[KMGT]i?B)")
        re_tid = re.compile(r"\bid=(?P<tid>[0-9]+)")
        cols = {}

        print(f"Connecting to M-Team... Feeds: {len(self.feeds)}.")

        for feed in self.feeds:
            try:
                response = self._get(urljoin(self.domain, feed))
                soup = (
                    tr.find_all("td", recursive=False)
                    for tr in BeautifulSoup(response.content, "html.parser").select("#form_torrent table.torrents > tr")
                )
                print("Fetching feed success, elapsed:", response.elapsed)
                tr = next(soup)
            except StopIteration:
                print("Unable to locate torrent table, css selector broken?")
                continue
            except Exception:
                print("Fetching feed failed.")
                continue

            for i, td in enumerate(tr):
                title = td.find(title=True)
                title = title["title"] if title else td.get_text(strip=True)
                cols[title] = i

            colTitle = cols.get("標題", 1)
            colSize = cols.get("大小", 4)
            colUp = cols.get("種子數", 5)
            colDown = cols.get("下載數", 6)
            cols.clear()

            for tr in soup:
                try:
                    peer = int(re_nondigit.sub("", tr[colDown].get_text()))
                    size = re_size.search(tr[colSize].get_text())
                    size = int(float(size["num"]) * byteUnit[size["unit"]])
                    if self.bellowMinPeer(size, peer):
                        continue

                    link = tr[colTitle].find("a", href=re_download)["href"]
                    tid = re_tid.search(link)["tid"]
                    if (
                        tid in self.history
                        or tr[colTitle].find(string=re_timelimit)
                        or tr[colUp].get_text(strip=True) == "0"
                    ):
                        continue

                    title = tr[colTitle].find("a", href=re_details, string=True)
                    title = title["title"] if title.has_attr("title") else title.get_text(strip=True)

                    yield self.Torrent(tid=tid, size=size, peer=peer, title=title, link=link)
                except Exception as e:
                    print("Parsing page error:", e)

    def download(self, downloadList: tuple):
        for t in downloadList:
            response = self._get(urljoin(self.domain, t.link))
            if response is not None and self.qb.add_torrent(f"{t.tid}.torrent", response.content):
                print("Download:", t.title)
                self.history.add(t.tid)
                log.record("Download", t.size, t.title)
            else:
                print("Failed:", t.title)


class MIPSolver:
    """Using OR-Tools from Google to find the best combination of downloads and removals.
    The goal is to maximize obtained peers under several constraints.

    Constraints:
        1, sum(downloadSize) <= freeSpace + sum(removedSize)
            --> sum(downloadSize) - sum(removedSize) <= freeSpace
            When freeSpace + sum(removedSize) < 0, this become impossible to satisfy.
            So the algorithm should delete all remove candidates to free up space.

    Objective:
        Maximize: sum(downloadPeer) - sum(removedPeer)
    """

    def __init__(self, *, removeCand, downloadCand, qb: qBittorrent):
        self.removeCand = tuple(removeCand)
        self.downloadCand = tuple(downloadCand)
        self.removeCandSize = sum(i.size for i in self.removeCand)
        self.qb = qb
        self.freeSpace = qb.freeSpace
        self._solve()

    def _solve(self):
        solver = pywraplp.Solver.CreateSolver("CBC")
        constSize = solver.Constraint(-solver.infinity(), self.freeSpace)
        objective = solver.Objective()

        removePool = tuple((solver.BoolVar(t.hash), t) for t in self.removeCand)
        downloadPool = tuple((solver.BoolVar(t.tid), t) for t in self.downloadCand)

        for v, t in removePool:
            constSize.SetCoefficient(v, -t.size)
            objective.SetCoefficient(v, -0.5 * t.peer)
        for v, t in downloadPool:
            constSize.SetCoefficient(v, t.size)
            objective.SetCoefficient(v, t.peer)

        objective.SetMaximization()

        if solver.Solve() == solver.OPTIMAL:
            self.wall_time = solver.wall_time()
            self.obj_value = objective.Value()
            self.removeList = tuple(t for v, t in removePool if v.solution_value() == 1)
            self.downloadList = tuple(t for v, t in downloadPool if v.solution_value() == 1)
        else:
            self.wall_time = self.obj_value = None
            self.removeList = self.removeCand if self.freeSpace < -self.removeCandSize else ()
            self.downloadList = ()

    def report(self):
        sepSlim = "-" * 50
        maxAvailSpace = self.freeSpace + self.removeCandSize
        removeSize = sum(i.size for i in self.removeList)
        downloadSize = sum(i.size for i in self.downloadList)
        finalFreeSpace = self.freeSpace + removeSize - downloadSize

        print(sepSlim)
        print(
            "Download candidates: {}. Total: {}.".format(
                len(self.downloadCand), humansize(sum(i.size for i in self.downloadCand))
            )
        )
        print(
            "Remove candidates: {}/{}. Total: {}.".format(
                len(self.removeCand), len(self.qb.torrents), humansize(self.removeCandSize)
            )
        )
        print(f"Disk free space: {humansize(self.freeSpace)}. Max avail space: {humansize(maxAvailSpace)}.")
        for v in self.removeCand:
            print(f"[{humansize(v.size):>11}|{v.peer:3d} peers] {v.title}")

        print(sepSlim)
        if self.wall_time is None:
            print("MIP solver cannot find an optimal solution.")
        else:
            print(f"Problem solved in {self.wall_time} milliseconds, objective value: {self.obj_value}.")

        print(f"Free space left after operation: {humansize(self.freeSpace)} => {humansize(finalFreeSpace)}.")

        for title, final, cand, size in (
            ("Download", self.downloadList, self.downloadCand, downloadSize),
            ("Remove", self.removeList, self.removeCand, removeSize),
        ):
            print(sepSlim)
            print(
                "{}: {}/{}. Total: {}, {} peers.".format(
                    title, len(final), len(cand), humansize(size), sum(i.peer for i in final)
                )
            )
            for v in final:
                print(f"[{humansize(v.size):>11}|{v.peer:3d} peers] {v.title}")
        print(sepSlim)


class Log(list):
    def record(self, action, size, name):
        self.append("{:20}{:12}{:14}{}\n".format(pd.Timestamp.now().strftime("%D %T"), action, humansize(size), name))

    def write(self, logfile: str, backupDir: str):
        if not self:
            return

        sep = "-" * 80
        header = "{:20}{:12}{:14}{}\n{}\n".format("Date", "Action", "Size", "Name", sep)
        content = reversed(self)

        if debug:
            print(sep)
            print(header, *content, sep="", end="")
        else:
            try:
                with open(logfile, mode="r", encoding="utf-8") as f:
                    oldLog = f.readlines()[2:]
            except (OSError, IndexError):
                oldLog = None
            with open(logfile, mode="w", encoding="utf-8") as f:
                f.write(header)
                f.writelines(content)
                if oldLog:
                    f.writelines(oldLog)
            copy_backup(logfile, backupDir)


def copy_backup(source: str, dest: str):
    try:
        shutil.copy(source, dest)
    except TypeError:
        pass
    except OSError as e:
        print(f'Copying "{source}" to "{dest}" failed: {e}')


def humansize(size: int):
    """Convert bytes to human readable sizes."""
    try:
        for suffix in ("KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"):
            size /= 1024
            if -1024 < size < 1024:
                return f"{size:.2f} {suffix}"
    except TypeError:
        pass
    return "---"


def main():
    import qbconfig

    global debug
    config = qbconfig.Config

    for arg in argv[1:]:
        if arg.startswith("-d"):
            debug = True
        elif arg.startswith("-r"):
            config = qbconfig.RemoteConfig
            debug = True

    script_dir = os.path.dirname(__file__)
    datafile = os.path.join(script_dir, "data")
    logfile = os.path.join(script_dir, "qb-maintenance.log")

    qb = qBittorrent(
        host=config.qBittorrentHost,
        seedDir=config.seedDir,
        watchDir=config.watchDir,
        speedThresh=config.speedThresh,
        spaceQuota=config.spaceQuota,
        datafile=datafile,
    )
    qb.clean_seedDir()

    if qb.need_action() or debug:

        mteam = MTeam(
            feeds=config.mteamFeeds,
            account=config.mteamAccount,
            minPeer=config.newTorrentMinPeer,
            qb=qb,
        )
        mipsolver = MIPSolver(
            removeCand=qb.get_remove_cands(),
            downloadCand=mteam.fetch(),
            qb=qb,
        )
        mipsolver.report()

        qb.remove_torrents(mipsolver.removeList)
        mteam.download(mipsolver.downloadList)

    qb.resume_paused()
    qb.dump_data(backupDir=config.backupDir)
    log.write(logfile, backupDir=config.backupDir)


log = Log()
debug = False

if __name__ == "__main__":
    main()
