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
from ortools.algorithms import pywrapknapsack_solver
from ortools.linear_solver import pywraplp

byteUnit = {u: s for us, s in zip(((f"{u}B", f"{u}iB") for u in "KMGT"), (1024 ** s for s in range(1, 5))) for u in us}


class qBittorrent:
    spaceQuota = 50 * byteUnit["GiB"]
    upSpeedThresh = 2.6 * byteUnit["MiB"]
    dlSpeedThresh = 8 * byteUnit["MiB"]

    def __init__(self, qBittorrentHost: str, seedDir: str, watchDir: str):
        self.api_baseurl = urljoin(qBittorrentHost, "api/v2/")
        try:
            self.seedDir = os.path.abspath(seedDir)
            self.watchDir = os.path.abspath(watchDir)
        except Exception:
            self.seedDir = self.watchDir = None

        path = "sync/maindata"
        maindata = self._request(path).json()
        self.state = maindata["server_state"]
        self.torrents = maindata["torrents"]
        if self.state["connection_status"] not in ("connected", "firewalled"):
            raise RuntimeError("qBittorrent is not connected to the internet.")

        self.Removable = namedtuple("Removable", ("hash", "speed", "size", "name"))
        self.removeCand = None
        self.newTorrent = {}
        self.removableSize = self.demandSize = 0
        self.upSpeed = self.dlSpeed = -1
        self._init_freeSpace()

    def _request(self, path, **kwargs):
        response = requests.get(urljoin(self.api_baseurl, path), **kwargs)
        response.raise_for_status()
        return response

    def _init_freeSpace(self):
        try:
            free_space_on_disk = shutil.disk_usage(self.seedDir).free
        except Exception:
            free_space_on_disk = self.state["free_space_on_disk"]
        self.freeSpace = self.availSpace = (
            free_space_on_disk - sum(i["amount_left"] for i in self.torrents.values()) - self.spaceQuota
        )

    def _update_availSpace(self):
        self.availSpace = self.freeSpace + self.removableSize - self.demandSize

    def clean_seedDir(self):
        if not self.seedDir:
            return

        refresh = False
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
                        refresh = True
                    log.record("Cleanup", None, entry.name)
        if refresh:
            self._init_freeSpace()

    def need_action(self) -> bool:
        print(
            "qBittorrent average speed last hour:\nUL: {}/s, DL: {}/s. Free space: {}".format(
                humansize(self.upSpeed), humansize(self.dlSpeed), humansize(self.freeSpace)
            )
        )
        return (
            0 <= self.upSpeed < self.upSpeedThresh
            and 0 <= self.dlSpeed < self.dlSpeedThresh
            and not self.state["use_alt_speed_limits"]
            and self.state["up_rate_limit"] > self.upSpeedThresh
        ) or self.freeSpace < 0

    def build_remove_lists(self, data):
        trs = self.torrents
        oneDayAgo = pd.Timestamp.now().timestamp() - 86400

        self.removeCand = tuple(
            self.Removable(hash=k, speed=v, size=trs[k]["size"], name=trs[k]["name"])
            for k, v in data.get_slows()
            if trs[k]["added_on"] < oneDayAgo
        )
        self.removableSize = sum(v.size for v in self.removeCand)
        self._update_availSpace()

        print(
            "Remove candidates info:\nCount: {}/{}. Size: {}. Max avail space: {}.".format(
                len(self.removeCand),
                len(trs),
                humansize(self.removableSize),
                humansize(self.availSpace),
            )
        )
        for v in self.removeCand:
            print(f"[{humansize(v.size):>11}] {v.name}")

    def add_torrent(self, filename: str, content: bytes, size: int):
        if filename not in self.newTorrent:
            self.newTorrent[filename] = content
            self.demandSize += size
            self._update_availSpace()
            return True
        print(f"Error: {filename} has already been added.")
        return False

    def apply_removes(self):
        targetSize = self.demandSize - self.freeSpace
        if targetSize <= 0:
            return

        print(
            f"Demand space: {humansize(self.demandSize)},",
            f"free space: {humansize(self.freeSpace)},",
            f"space to free: {humansize(targetSize)}",
        )

        try:
            removeList, sizeSum = self.findMinSpeed(self.removeCand, targetSize)
        except Exception:
            removeList, sizeSum = self.findMinSum(self.removeCand, targetSize)

        print(f"Removals: {len(removeList)}, size: {humansize(sizeSum)}.")

        if not debug:
            path = "torrents/delete"
            payload = {"hashes": "|".join(v.hash for v in removeList), "deleteFiles": True}
            self._request(path, params=payload)
        for v in removeList:
            log.record("Remove", v.size, v.name)

    def upload_torrent(self):
        if not self.newTorrent or debug:
            return

        try:
            if not os.path.exists(self.watchDir):
                os.mkdir(self.watchDir)
            for filename, content in self.newTorrent.items():
                path = os.path.join(self.watchDir, filename)
                with open(path, "wb") as f:
                    f.write(content)
        except Exception as e:
            log.record("Error", None, f"Writing torrent to {self.watchDir} failed: {e}")

    def resume_paused(self):
        paused = {"error", "missingFiles", "pausedUP", "pausedDL", "unknown"}
        if any(i["state"] in paused for i in self.torrents.values()):
            print("Resume torrents.")
            if not debug:
                path = "torrents/resume"
                payload = {"hashes": "all"}
                self._request(path, params=payload)

    @staticmethod
    def findMinSpeed(torrents: tuple, targetSize: int):
        """
        Find a subset of the list which satisfies the targetSize,
        while minimizes the total speed cost.
        Input should be a list of named tuples with a "size" and a "speed" field.
        Returns a new tuple and the sum.
        """
        solver = pywraplp.Solver.CreateSolver("findMinSpeed", "CBC")
        constraint = solver.Constraint(targetSize, solver.infinity())
        objective = solver.Objective()
        varPool = tuple((solver.BoolVar(t.hash), t) for t in torrents)

        for v, t in varPool:
            constraint.SetCoefficient(v, t.size)
            objective.SetCoefficient(v, t.speed)
        objective.SetMinimization()

        if solver.Solve() == solver.OPTIMAL:
            optTorrents = tuple(t for v, t in varPool if v.solution_value() == 1)
            sizeSum = sum(t.size for t in optTorrents)
            return optTorrents, sizeSum

    @staticmethod
    def findMinSum(torrents: tuple, targetSize: int):
        """
        Find the minimum sum of a list of numbers to reach the target size.
        Input should be a list of named tuples with a "size" field.
        Returns a new tuple and the sum.
        If the sum is lesser than the target, return the original pairs.
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
            return optTorrents, minSum
        return torrents, minSum


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
        """
        Trying to discover slow torrents using jenks natural breaks method.
        Returns (hash, speed) pairs.
        The speed unit is bytes/min, but doesn't matter.
        """
        speeds = self.torrentFrame.last("D").resample("T").bfill().diff().mean()
        try:
            breaks = speeds.count() - 1
            breaks = jenks_breaks(speeds, nb_class=(4 if breaks >= 4 else breaks.item()))[1]
        except Exception:
            breaks = speeds.mean()
        return speeds.loc[speeds <= breaks].iteritems()

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


class Log(list):
    def record(self, action, size, name):
        self.append(
            "{:20}{:12}{:14}{}\n".format(
                pd.Timestamp.now().strftime("%D %T"),
                action,
                humansize(size),
                name,
            )
        )

    def write(self, logfile: str, backupDir=None):
        if not self:
            return

        self.reverse()
        header = "{:20}{:12}{:14}{}\n{}\n".format("Date", "Action", "Size", "Name", "-" * 80)

        if debug:
            print(header, *self, end="")
            return

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


class MTeam:
    """An optimized MTeam downloader."""

    domain = "https://pt.m-team.cc"
    newTorrentMinPeer = 30

    def __init__(self, mteamFeeds: tuple, mteamAccount: tuple) -> None:
        self.mteamFeeds = mteamFeeds
        self.loginPayload = {"username": mteamAccount[0], "password": mteamAccount[1]}
        self.loginPage = urljoin(self.domain, "takelogin.php")
        self.loginReferer = {"referer": urljoin(self.domain, "login.php")}
        self.Torrent = namedtuple("Torrent", ("title", "link", "tid", "size", "peer"))

    def run(self, maxItem=None):
        session = data.session
        mteamHistory = data.mteamHistory
        newTorrentMinPeer = self.newTorrentMinPeer
        re_download = re.compile(r"\bdownload.php\?")
        re_details = re.compile(r"\bdetails.php\?")
        re_timelimit = re.compile(r"限時：[^日]*$")
        re_nondigit = re.compile(r"[^0-9]+")
        re_tid = re.compile(r"id=([0-9]+)")
        torrents = []
        cols = {}

        print(f"Connecting to M-Team... Minimum peer: {newTorrentMinPeer}. Maximum items: {maxItem}.")

        for feed in self.mteamFeeds:
            feed = urljoin(self.domain, feed)

            for i in range(5):
                try:
                    response = session.get(feed)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, "html.parser")
                    if "login.php" in response.url or "登錄" in soup.title.string:
                        assert i < 4, "login failed."
                        print("Login...")
                        session.post(self.loginPage, data=self.loginPayload, headers=self.loginReferer)
                    else:
                        break
                except Exception as e:
                    if i == 4:
                        print(e)
                        return
                    print(f"Retrying... Attempt: {i+1}, Error: {e}")
                    session = data.init_session()

            print("Fetching feed success.")

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

                    torrents.append(self.Torrent(title=title, link=link, tid=tid, size=size, peer=peer))
                except Exception as e:
                    print("Parsing page error:", e)

        if not torrents:
            print("No new torrent found.")
            return

        optWeight, optValue, optTorrents = self.knapsack(torrents, qb.availSpace, maxItem)

        print(
            "{}/{} torrents selected. Total: {}, {} peers.".format(
                len(optTorrents), len(torrents), humansize(optWeight), optValue
            )
        )

        for t in optTorrents:
            try:
                response = session.get(urljoin(self.domain, t.link))
                response.raise_for_status()
            except Exception as e:
                print("Downloading torrents failed.", e)
                return

            if qb.add_torrent(f"{t.tid}.torrent", response.content, t.size):
                print(f"[{t.peer:4} peers] {t.title}")
                log.record("Download", t.size, t.title)
                if not debug:
                    mteamHistory.add(t.tid)

    @staticmethod
    def knapsack(torrents: list, capacity: int, maxItem=None):
        """
        Using OR-Tools from Google to solve the 0-1 knapsack problem.
        To find a subset of the list whose counts is under maxItem and
        fits inside capacity and maximizes the total value (peer counts).
        Input should be a list of named tuples with size and peer fields.
        return a tuple of the optimal total size, total peers, torrent list.
        """

        weights = tuple(t.size for t in torrents)
        values = tuple(t.peer for t in torrents)
        if maxItem is None:
            weights = [weights]
            capacities = (capacity,)
        else:
            weights = [weights, (1,) * len(torrents)]
            capacities = (capacity, maxItem)

        solver = pywrapknapsack_solver.KnapsackSolver(
            pywrapknapsack_solver.KnapsackSolver.KNAPSACK_MULTIDIMENSION_BRANCH_AND_BOUND_SOLVER, "Knapsack"
        )
        solver.Init(values, weights, capacities)
        optValue = solver.Solve()
        optTorrents = tuple(t for i, t in enumerate(torrents) if solver.BestSolutionContains(i))
        optWeight = sum(t.size for t in optTorrents)

        return optWeight, optValue, optTorrents

    @staticmethod
    def size_convert(string):
        """Should be wrapped inside a try...except block"""
        m = re.search(r"(?P<num>[0-9]+(\.[0-9]+)?)\s*(?P<unit>[TGMK]i?B)", string)
        return int(float(m["num"]) * byteUnit[m["unit"]])


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


if __name__ == "__main__":
    import qbconfig

    debug = False
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

    qb = qBittorrent(config.qBittorrentHost, config.seedDir, config.watchDir)
    log = Log()
    data = load_data(datafile)

    data.record(qb)
    qb.clean_seedDir()

    if qb.need_action() or debug:
        qb.build_remove_lists(data)
        if qb.availSpace > 0:
            MTeam(config.mteamFeeds, config.mteamAccount).run(maxItem=3)
        qb.apply_removes()
        qb.upload_torrent()
    else:
        print("System is healthy, no action needed.")

    qb.resume_paused()
    data.dump(datafile, config.backupDir)
    log.write(logfile, config.backupDir)

else:
    raise ImportError
