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

    def __init__(self, host: str, seedDir: str, watchDir: str, speedThresh: tuple, spaceQuota: int):

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
        self.freeSpace = self.upSpeed = self.dlSpeed = -1

    def _request(self, path, **kwargs):
        response = requests.get(urljoin(self.api_baseurl, path), **kwargs)
        response.raise_for_status()
        return response

    def clean_seedDir(self):
        if not self.seedDir:
            return

        re_ext = re.compile(r"\.!qB$")
        names = set(i["name"] for i in self.torrents.values())
        if os.path.dirname(self.watchDir) == self.seedDir:
            names.add(os.path.basename(self.watchDir))

        with os.scandir(self.seedDir) as it:
            for entry in it:
                if re_ext.sub("", entry.name) not in names:
                    print("Cleanup:", entry.name)
                    if not debug:
                        try:
                            if entry.is_dir():
                                shutil.rmtree(entry.path)
                            else:
                                os.remove(entry.path)
                        except Exception as e:
                            print("Deletion Failed:", e)
                            continue
                    log.record("Cleanup", None, entry.name)

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

    def get_remove_cands(self, data):
        oneDayAgo = pd.Timestamp.now().timestamp() - 86400
        for k in data.get_slows():
            v = self.torrents[k]
            if v["added_on"] < oneDayAgo:
                yield self.Removable(hash=k, size=v["size"], peer=v["num_incomplete"], title=v["name"])

    def remove(self, removeList: tuple):
        if removeList and not debug:
            path = "torrents/delete"
            payload = {"hashes": "|".join(v.hash for v in removeList), "deleteFiles": True}
            self._request(path, params=payload)
        for v in removeList:
            log.record("Remove", v.size, v.title)

    def upload(self, torrentFiles):
        """
        Upload torrent to qBittorrent watch dir.
        : torrentFiles: namedtuple(Torrent), content(bytes)
        """
        if debug:
            return

        try:
            if not os.path.exists(self.watchDir):
                os.mkdir(self.watchDir)
        except Exception:
            return

        for t, content in torrentFiles:
            path = os.path.join(self.watchDir, f"{t.tid}.torrent")
            try:
                with open(path, "wb") as f:
                    f.write(content)
                log.record("Download", t.size, t.title)
            except Exception as e:
                log.record("Error", None, f"Saving torrent '{t.title}' to '{self.watchDir}' failed: {e}")

    def resume_paused(self):
        paused = {"error", "missingFiles", "pausedUP", "pausedDL", "unknown"}
        if any(i["state"] in paused for i in self.torrents.values()):
            print("Resume torrents.")
            if not debug:
                path = "torrents/resume"
                payload = {"hashes": "all"}
                self._request(path, params=payload)


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
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
            }
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
            raise Exception("Intergrity check failed.")

    def record(self, qb: qBittorrent):
        """Record qBittorrent traffic data to pandas DataFrame."""
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
        qb.upSpeed = speeds["upload"]
        qb.dlSpeed = speeds["download"]

    def get_slows(self):
        """Discover the slowest torrents using jenks natural breaks method."""
        speeds = self.torrentFrame.last("D").resample("T").bfill().diff().mean()
        try:
            breaks = speeds.count().item() - 1
            breaks = jenks_breaks(speeds, nb_class=(4 if breaks >= 4 else breaks))[1]
        except Exception:
            breaks = speeds.mean()
        return speeds.loc[speeds <= breaks].index

    def dump(self, datafile: str, backupDir=None):
        if debug:
            return
        try:
            with open(datafile, "wb") as f:
                pickle.dump(self, f)
        except Exception as e:
            log.record("Error", None, f"Writing data to disk failed: {e}")
            return
        if backupDir:
            copy_backup(datafile, backupDir)


class MTeam:
    """An optimized MTeam downloader."""

    domain = "https://pt.m-team.cc"
    Torrent = namedtuple("Torrent", ("tid", "size", "peer", "title", "link"))

    def __init__(self, feeds: tuple, account: tuple, data: Data, minPeer: int) -> None:
        self.feeds = feeds
        self.loginPayload = {"username": account[0], "password": account[1]}
        self.data = data
        self.newTorrentMinPeer = minPeer if isinstance(minPeer, int) else 0
        self.loginPage = urljoin(self.domain, "takelogin.php")
        self.loginReferer = {"referer": urljoin(self.domain, "login.php")}

    def fetch(self):
        session = self.data.session
        mteamHistory = self.data.mteamHistory
        newTorrentMinPeer = self.newTorrentMinPeer
        re_download = re.compile(r"\bdownload.php\?")
        re_details = re.compile(r"\bdetails.php\?")
        re_timelimit = re.compile(r"限時：[^日]*$")
        re_nondigit = re.compile(r"[^0-9]+")
        re_tid = re.compile(r"id=([0-9]+)")
        cols = {}

        print(f"Connecting to M-Team... Feeds: {len(self.feeds)}, minimum peer requirement: {self.newTorrentMinPeer}")

        for feed in self.feeds:
            feed = urljoin(self.domain, feed)

            for i in range(5):
                try:
                    response = session.get(feed)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, "html.parser")
                    if "login.php" in response.url or "登錄" in soup.title.string:
                        assert i < 4, "login failed."
                        print("login...")
                        session.post(self.loginPage, data=self.loginPayload, headers=self.loginReferer)
                    else:
                        break
                except Exception as e:
                    print("Error:", e)
                    if i < 4:
                        print("Retrying... Attempt:", i + 1)
                        session = self.data.init_session()
            else:
                return

            print("Fetching feed success, elapsed:", response.elapsed)

            for i, td in enumerate(soup.select("#form_torrent table.torrents > tr:nth-of-type(1) > td")):
                title = td.find(title=True)
                title = title["title"] if title else td.get_text(strip=True)
                cols[title] = i

            colTitle = cols.get("標題", 1)
            colSize = cols.get("大小", 4)
            colUp = cols.get("種子數", 5)
            colDown = cols.get("下載數", 6)
            cols.clear()

            for tr in soup.select("#form_torrent table.torrents > tr:not(:nth-of-type(1))"):
                try:
                    td = tr.find_all("td", recursive=False)

                    peer = int(re_nondigit.sub("", td[colDown].get_text()))
                    if peer < newTorrentMinPeer:
                        continue
                    link = td[colTitle].find("a", href=re_download)["href"]
                    tid = re_tid.search(link).group(1)
                    if (
                        tid in mteamHistory
                        or td[colTitle].find(string=re_timelimit)
                        or td[colUp].get_text(strip=True) == "0"
                    ):
                        continue

                    title = td[colTitle].find("a", href=re_details, string=True)
                    title = title["title"] if title.has_attr("title") else title.get_text(strip=True)
                    size = self.size_convert(td[colSize].get_text())

                    yield self.Torrent(tid=tid, size=size, peer=peer, title=title, link=link)
                except Exception as e:
                    print("Parsing page error:", e)

    def download(self, downloadList: tuple):
        try:
            for t in downloadList:
                response = self.data.session.get(urljoin(self.domain, t.link))
                response.raise_for_status()
                self.data.mteamHistory.add(t.tid)
                yield t, response.content
        except Exception as e:
            print("Downloading torrents failed:", e)

    @staticmethod
    def size_convert(string):
        """Should be wrapped inside a try...except block"""
        m = re.search(r"(?P<num>[0-9]+(\.[0-9]+)?)\s*(?P<unit>[TGMK]i?B)", string)
        return int(float(m["num"]) * byteUnit[m["unit"]])


class MIPSolver:
    """
    Using OR-Tools from Google to find the best combination of downloads and removals.
    The goal is to maximize obtained peers under several constraints.

    Constraints:
        1, sum(downloadSize) < freeSpace + sum(removedSize)
            --> sum(downloadSize) - sum(removedSize) < freeSpace
            When freeSpace < -sum(removedSize) < 0, this become impossible to satisfy.
            So the second algorithm findMinSum will be used.
        2, When free space > 0: sum(downloadPeer) > sum(removedPeer)
            This constraint will be neutralized when freeSpace < 0, so old torrents can be
            deleted without new complement.
        3, The amount of downloads should be limited to a sane number, like 3.

    Objective:
        Maximize: sum(downloadPeer) - sum(removedPeer)
    """

    def __init__(self, removeCand, downloadCand, qb: qBittorrent, maxDownload) -> None:
        self.solver = pywraplp.Solver.CreateSolver("TorrentOptimizer", "CBC")
        self.qb = qb
        self.freeSpace = qb.freeSpace
        self.removeCand = tuple(removeCand)
        self.downloadCand = tuple(downloadCand)
        self.maxDownload = maxDownload if isinstance(maxDownload, int) else self.solver.infinity()
        self.sepSlim = "-" * 50

    def solve(self):
        solver = self.solver
        infinity = solver.infinity()

        constSize = solver.Constraint(-infinity, self.freeSpace)
        constPeer = solver.Constraint(0 if self.freeSpace > 0 else -infinity, infinity)
        constMax = solver.Constraint(0, self.maxDownload)
        objective = solver.Objective()
        objective.SetMaximization()

        removePool = tuple((solver.BoolVar(t.hash), t) for t in self.removeCand)
        downloadPool = tuple((solver.BoolVar(t.tid), t) for t in self.downloadCand)

        for v, t in removePool:
            constSize.SetCoefficient(v, -t.size)
            constPeer.SetCoefficient(v, -t.peer)
            objective.SetCoefficient(v, -t.peer)
        for v, t in downloadPool:
            constSize.SetCoefficient(v, t.size)
            constPeer.SetCoefficient(v, t.peer)
            constMax.SetCoefficient(v, 1)
            objective.SetCoefficient(v, t.peer)

        self.status = solver.Solve()

        if self.status == solver.OPTIMAL:
            self.removeList = tuple(t for v, t in removePool if v.solution_value() == 1)
            self.downloadList = tuple(t for v, t in downloadPool if v.solution_value() == 1)
        else:
            self.removeList = self.findMinSum(self.removeCand, -self.freeSpace) if self.freeSpace < 0 else tuple()
            self.downloadList = tuple()

        return self.removeList, self.downloadList

    @staticmethod
    def findMinSum(torrents: tuple, targetSize: int):
        """
        Find the minimum sum of a list of numbers to reach the target size.
        Input should be a list of named tuples with a "size" field.
        Returns a new tuple of tuples.
        If the sum is lesser than the target, return the original input.
        """

        def _innerLoop(parentKeys=0, parentSum=0, i=0):
            nonlocal minKeys, minSum
            for n in range(i, length):
                currentSum = parentSum + nums[n]
                if currentSum < minSum:
                    currentKeys = parentKeys | masks[n]
                    if currentSum < targetSize:
                        _innerLoop(currentKeys, currentSum, n + 1)
                    else:
                        minKeys, minSum = currentKeys, currentSum

        nums = tuple(i.size for i in torrents)
        minSum = sum(nums)
        if minSum > targetSize:
            length = len(nums)
            minKeys = 2 ** length - 1
            masks = tuple(1 << i for i in range(length))
            _innerLoop()
            optTorrents = tuple(i for n, i in enumerate(torrents) if minKeys & masks[n])
            return optTorrents
        return torrents

    def prologue(self):
        removeCandSize = sum(i.size for i in self.removeCand)
        maxAvailSpace = self.freeSpace + removeCandSize

        print(self.sepSlim)
        print(
            "Remove candidates: {}/{}. Size: {}. Disk free space: {}. Max avail space: {}.".format(
                len(self.removeCand),
                len(self.qb.torrents),
                humansize(removeCandSize),
                humansize(self.freeSpace),
                humansize(maxAvailSpace),
            )
        )
        for v in self.removeCand:
            print(f"[{humansize(v.size):>11}|{v.peer:4d} peers] {v.title}")

    def report(self):
        downloadSize = sum(i.size for i in self.downloadList)
        removeSize = sum(i.size for i in self.removeList)
        finalFreeSpace = self.freeSpace + removeSize - downloadSize

        print(self.sepSlim)
        if self.status == self.solver.OPTIMAL:
            print(
                "Problem solved in {} milliseconds, objective value: {}.".format(
                    self.solver.wall_time(), self.solver.Objective().Value()
                )
            )
        else:
            print(f"MIP Solver failed, status: {self.status}.")

        print(f"Post-operation free space: {humansize(self.freeSpace)} ==> {humansize(finalFreeSpace)}.")

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


class Log(list):
    def record(self, action, size, name):
        self.append("{:20}{:12}{:14}{}\n".format(pd.Timestamp.now().strftime("%D %T"), action, humansize(size), name))

    def write(self, logfile: str, backupDir=None):
        if not self or debug:
            return

        try:
            with open(logfile, mode="r", encoding="utf-8") as f:
                oldLog = f.readlines()[2:]
        except Exception:
            oldLog = None

        with open(logfile, mode="w", encoding="utf-8") as f:
            f.write("{:20}{:12}{:14}{}\n{}\n".format("Date", "Action", "Size", "Name", "-" * 80))
            f.writelines(reversed(self))
            if oldLog:
                f.writelines(oldLog)

        if backupDir:
            copy_backup(logfile, backupDir)


def load_data(datafile: str):
    """Load Data object from pickle."""
    try:
        with open(datafile, mode="rb") as f:
            data = pickle.load(f)
        data.integrity_test()
    except Exception as e:
        print(f"Loading data from {datafile} failed: {e}")
        if not debug:
            try:
                os.rename(datafile, f"{datafile}_{pd.Timestamp.now().strftime('%y%m%d_%H%M%S')}")
            except Exception:
                pass
        data = Data()
    return data


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
    )
    data = load_data(datafile)

    data.record(qb)
    qb.clean_seedDir()

    if qb.need_action() or debug:

        mteam = MTeam(
            feeds=config.mteamFeeds,
            account=config.mteamAccount,
            data=data,
            minPeer=config.newTorrentMinPeer,
        )
        mipsolver = MIPSolver(
            removeCand=qb.get_remove_cands(data),
            downloadCand=mteam.fetch(),
            qb=qb,
            maxDownload=config.maxDownload,
        )
        mipsolver.prologue()
        removeList, downloadList = mipsolver.solve()
        mipsolver.report()

        qb.remove(removeList)
        qb.upload(mteam.download(downloadList))

    qb.resume_paused()
    data.dump(datafile, config.backupDir)
    log.write(logfile, config.backupDir)


byteUnit = {u: s for us, s in zip(((f"{u}B", f"{u}iB") for u in "KMGT"), (1024 ** s for s in range(1, 5))) for u in us}
log = Log()
debug = False

if __name__ == "__main__":
    main()
