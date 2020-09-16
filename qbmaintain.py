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
        except Exception:
            self.seedDir = self.watchDir = None

        self.upSpeedThresh, self.dlSpeedThresh = (i * byteUnit["MiB"] for i in speedThresh)
        self.spaceQuota = spaceQuota * byteUnit["GiB"]

        self.datafile = datafile
        self.data = self._load_data()
        self.upSpeed, self.dlSpeed = self.data.record(self)

    def _request(self, path: str, **kwargs):
        response = requests.get(urljoin(self.api_baseurl, path), **kwargs, timeout=7)
        response.raise_for_status()
        return response

    def _load_data(self):
        """Load Data object from pickle."""
        try:
            with open(self.datafile, mode="rb") as f:
                data = pickle.load(f)
            data.integrity_test()
        except Exception as e:
            print(f"Loading data from '{self.datafile}' failed: {e}")
            if not debug:
                try:
                    os.rename(self.datafile, f"{self.datafile}_{pd.Timestamp.now().strftime('%y%m%d_%H%M%S')}")
                except Exception:
                    pass
            data = Data()
        return data

    def clean_seedDir(self):
        try:
            watchDir = os.path.split(self.watchDir)
        except Exception:
            return

        names = set(i["name"] for i in self.torrents.values())
        if watchDir[0] == self.seedDir:
            names.add(watchDir[1])
        re_ext = re.compile(r"\.!qB$")

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
                    except Exception as e:
                        print("Deletion Failed:", e)

    def _init_freeSpace(self):
        try:
            realSpace = shutil.disk_usage(self.seedDir).free
        except Exception:
            realSpace = self.state["free_space_on_disk"]
        self.freeSpace = realSpace - sum(i["amount_left"] for i in self.torrents.values()) - self.spaceQuota

    def need_action(self) -> bool:
        self._init_freeSpace()
        print(
            "qBittorrent average speed last hour: UL: {}/s, DL: {}/s.".format(
                humansize(self.upSpeed), humansize(self.dlSpeed)
            )
        )
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
            payload = {"hashes": "|".join(v.hash for v in removeList), "deleteFiles": True}
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
            except Exception as e:
                try:
                    if not os.path.exists(self.watchDir):
                        os.mkdir(self.watchDir)
                        return self.add_torrent(filename, content)
                except Exception:
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

    def dump_data(self, backupDir=None):
        if debug:
            return
        try:
            with open(self.datafile, "wb") as f:
                pickle.dump(self.data, f)
            if backupDir:
                copy_backup(self.datafile, backupDir)
        except Exception as e:
            log.record("Error", None, f"Writing data to disk failed: {e}")
            print("Writing data to disk failed:", e)


class Data:
    def __init__(self):
        self.qBittorrentFrame = pd.DataFrame()
        self.torrentFrame = pd.DataFrame()
        self.mteamHistory = set()
        self.init_session()

    def init_session(self) -> requests.session:
        """Initialize a new requests session."""
        self.session = requests.session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0"}
        )
        return self.session

    def integrity_test(self):
        attrs = (
            (self.qBittorrentFrame, pd.DataFrame),
            (self.torrentFrame, pd.DataFrame),
            (self.mteamHistory, set),
            (self.session, requests.Session),
        )
        if not all(isinstance(x, y) for x, y in attrs):
            raise Exception("Intergrity test failed.")

    def record(self, qb: qBittorrent):
        """Record qBittorrent traffic data to pandas DataFrame. Returns the last hour avg UL/DL speeds."""

        now = pd.Timestamp.now()
        qBittorrentRow = pd.DataFrame(
            {"upload": qb.state["alltime_ul"], "download": qb.state["alltime_dl"]}, index=[now]
        )
        torrentRow = pd.DataFrame({k: v["uploaded"] for k, v in qb.torrents.items()}, index=[now])

        try:
            self.qBittorrentFrame = self.qBittorrentFrame.last("7D").append(qBittorrentRow)
        except Exception:
            self.qBittorrentFrame = qBittorrentRow
        try:
            difference = self.torrentFrame.columns.difference(torrentRow.columns)
            if not difference.empty:
                self.torrentFrame.drop(columns=difference, inplace=True, errors="ignore")
                self.torrentFrame.dropna(how="all", inplace=True)
            self.torrentFrame = self.torrentFrame.append(torrentRow)
        except Exception:
            self.torrentFrame = torrentRow

        speeds = self.qBittorrentFrame.last("H").resample("T").bfill().diff().mean().floordiv(60)
        return speeds["upload"], speeds["download"]

    def get_slows(self) -> pd.Index:
        """Discover the slowest torrents using jenks natural breaks method."""
        speeds = self.torrentFrame.last("D").resample("T").bfill().diff().mean()
        try:
            breaks = speeds.count().item() - 1
            breaks = jenks_breaks(speeds, nb_class=(4 if breaks >= 4 else breaks))[1]
        except Exception:
            breaks = speeds.mean()
        return speeds.loc[speeds <= breaks].index


class MTeam:
    """An optimized MTeam downloader."""

    domain = "https://pt.m-team.cc"
    Torrent = namedtuple("Torrent", ("tid", "size", "peer", "title", "link"))

    def __init__(self, *, feeds: tuple, account: tuple, minPeer: int, qb: qBittorrent) -> None:
        self.feeds = feeds
        self.loginPayload = {"username": account[0], "password": account[1]}
        self.newTorrentMinPeer = minPeer if isinstance(minPeer, int) else 0
        self.qb = qb
        self.session = qb.data.session
        self.history = qb.data.mteamHistory
        self.loginPage = urljoin(self.domain, "takelogin.php")
        self.loginReferer = {"referer": urljoin(self.domain, "login.php")}

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
        re_tid = re.compile(r"\bid=(?P<tid>[0-9]+)")
        newTorrentMinPeer = self.newTorrentMinPeer
        cols = {}

        print(f"Connecting to M-Team... Feeds: {len(self.feeds)}, minimum peer requirement: {newTorrentMinPeer}")

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
                title = title["title"] if title else td.get_text()
                cols[title.strip()] = i

            colTitle = cols.get("標題", 1)
            colSize = cols.get("大小", 4)
            colUp = cols.get("種子數", 5)
            colDown = cols.get("下載數", 6)
            cols.clear()

            for tr in soup:
                try:
                    peer = int(re_nondigit.sub("", tr[colDown].get_text()))
                    if peer < newTorrentMinPeer:
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
                    size = self.size_convert(tr[colSize].get_text())

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

    @staticmethod
    def size_convert(string: str) -> int:
        """Convert human readable size to bytes.

        Example: size_convert('15GB') -> 16106127360.
        Should be wrapped inside a try...except block.
        """
        m = re.search(r"(?P<num>[0-9]+(\.[0-9]+)?)\s*(?P<unit>[TGMK]i?B)", string)
        return int(float(m["num"]) * byteUnit[m["unit"]])


class MIPSolver:
    """Using OR-Tools from Google to find the best combination of downloads and removals.
    The goal is to maximize obtained peers under several constraints.

    Constraints:
        1, sum(downloadSize) <= freeSpace + sum(removedSize)
            --> sum(downloadSize) - sum(removedSize) <= freeSpace
            When freeSpace + sum(removedSize) < 0, this become impossible to satisfy.
            So the algorithm should delete all remove candidates to free up space.
        2, The amount of downloads should be limited to a sane number, like 3.

    Objective:
        Maximize: sum(downloadPeer) - sum(removedPeer)
    """

    def __init__(self, *, removeCand, downloadCand, maxDownload, qb: qBittorrent) -> None:
        self.removeCand = tuple(removeCand)
        self.downloadCand = tuple(downloadCand)
        self.removeCandSize = sum(i.size for i in self.removeCand)
        self.qb = qb
        self.freeSpace = qb.freeSpace
        self.sepSlim = "-" * 50
        self.solver = pywraplp.Solver.CreateSolver("TorrentOptimizer", "CBC")
        self.maxDownload = maxDownload if isinstance(maxDownload, int) else self.solver.infinity()

    def solve(self):
        solver = self.solver
        constSize = solver.Constraint(-solver.infinity(), self.freeSpace)
        constMax = solver.Constraint(0, self.maxDownload)
        objective = solver.Objective()
        objective.SetMaximization()

        removePool = tuple((solver.BoolVar(t.hash), t) for t in self.removeCand)
        downloadPool = tuple((solver.BoolVar(t.tid), t) for t in self.downloadCand)

        for v, t in removePool:
            constSize.SetCoefficient(v, -t.size)
            objective.SetCoefficient(v, -t.peer)
        for v, t in downloadPool:
            constSize.SetCoefficient(v, t.size)
            constMax.SetCoefficient(v, 1)
            objective.SetCoefficient(v, t.peer)

        self.optimal = solver.Solve() == solver.OPTIMAL

        if self.optimal:
            self.removeList = tuple(t for v, t in removePool if v.solution_value() == 1)
            self.downloadList = tuple(t for v, t in downloadPool if v.solution_value() == 1)
        else:
            self.removeList = self.removeCand if self.freeSpace < -self.removeCandSize else tuple()
            self.downloadList = tuple()

    def report(self):
        maxAvailSpace = self.freeSpace + self.removeCandSize
        removeSize = sum(i.size for i in self.removeList)
        downloadSize = sum(i.size for i in self.downloadList)
        finalFreeSpace = self.freeSpace + removeSize - downloadSize

        print(self.sepSlim)
        print(f"Torrents fetched: {len(self.downloadCand)}. Maximum downloads: {self.maxDownload}.")
        print(
            "Remove candidates: {}/{}. Size: {}.".format(
                len(self.removeCand), len(self.qb.torrents), humansize(self.removeCandSize)
            )
        )
        print(f"Disk free space: {humansize(self.freeSpace)}. Max avail space: {humansize(maxAvailSpace)}.")
        for v in self.removeCand:
            print(f"[{humansize(v.size):>11}|{v.peer:4d} peers] {v.title}")

        print(self.sepSlim)
        if self.optimal:
            print(
                "Problem solved in {} milliseconds, objective value: {}.".format(
                    self.solver.wall_time(), self.solver.Objective().Value()
                )
            )
        else:
            print("MIP solver cannot find an optimal solution.")

        print(f"Free space left after operation: {humansize(self.freeSpace)} ==> {humansize(finalFreeSpace)}.")

        for title, final, cand, size in (
            ("Download", self.downloadList, self.downloadCand, downloadSize),
            ("Remove", self.removeList, self.removeCand, removeSize),
        ):
            print(self.sepSlim)
            print(
                "{}: {}/{}. Total: {}, {} peers.".format(
                    title, len(final), len(cand), humansize(size), sum(i.peer for i in final)
                )
            )
            for v in final:
                print(f"[{humansize(v.size):>11}|{v.peer:4d} peers] {v.title}")
        print(self.sepSlim)

    @staticmethod
    def findMinSum(torrents: tuple, targetSize: int):
        """Find a minimum subset whose sum reaches the target size.

        Input should be a list of named tuples with a "size" field.
        Returns a new tuple of tuples.
        If the maximum sum is lesser than the target, returns the original input.
        """

        def _innerLoop(parentKeys=0, parentSum=0, i=0):
            nonlocal minKeys, minSum
            for n in range(i, length):
                currentSum = parentSum + nums[n]
                if currentSum < minSum:
                    currentKeys = parentKeys | masks[n]
                    if currentSum < targetSize:
                        _innerLoop(currentKeys, currentSum, n + 1)
                        continue
                    minKeys, minSum = currentKeys, currentSum
                break

        sTorrents = sorted(torrents, key=lambda i: i.size)
        nums = tuple(i.size for i in sTorrents)
        minSum = sum(nums)
        if minSum > targetSize:
            length = len(nums)
            minKeys = 2 ** length - 1
            masks = tuple(1 << i for i in range(length))
            _innerLoop()
            return tuple(i for n, i in enumerate(sTorrents) if minKeys & masks[n])
        return torrents


class Log(list):
    def record(self, action, size, name):
        self.append("{:20}{:12}{:14}{}\n".format(pd.Timestamp.now().strftime("%D %T"), action, humansize(size), name))

    def write(self, logfile: str, backupDir=None):
        if not self:
            return

        sep = "-" * 80
        header = "{:20}{:12}{:14}{}\n{}\n".format("Date", "Action", "Size", "Name", sep)
        self.reverse()

        if debug:
            print(sep)
            print(header, *self, sep="", end="")
        else:
            try:
                with open(logfile, mode="r", encoding="utf-8") as f:
                    oldLog = f.readlines()[2:]
            except Exception:
                oldLog = None
            with open(logfile, mode="w", encoding="utf-8") as f:
                f.write(header)
                f.writelines(self)
                if oldLog:
                    f.writelines(oldLog)
            if backupDir:
                copy_backup(logfile, backupDir)


def copy_backup(source, dest):
    try:
        shutil.copy(source, dest)
    except Exception as e:
        print(f'Copying "{source}" to "{dest}" failed: {e}')


def humansize(size, suffixes=("KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")):
    try:
        for suffix in suffixes:
            size /= 1024
            if size < 1024:
                return "{:.2f} {}".format(size, suffix)
    except Exception:
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
            maxDownload=config.maxDownload,
            qb=qb,
        )
        mipsolver.solve()
        mipsolver.report()

        qb.remove_torrents(mipsolver.removeList)
        mteam.download(mipsolver.downloadList)

    qb.resume_paused()
    qb.dump_data(backupDir=config.backupDir)
    log.write(logfile, backupDir=config.backupDir)


byteUnit = {u: s for us, s in zip(((f"{u}B", f"{u}iB") for u in "KMGT"), (1024 ** s for s in range(1, 5))) for u in us}
log = Log()
debug = False

if __name__ == "__main__":
    main()
