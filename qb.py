import os
import pickle
import re
import shutil
from sys import argv

import pandas as pd
import requests
from bs4 import BeautifulSoup
from jenkspy import jenks_breaks
from pandas.util import hash_pandas_object
from requests.compat import urljoin

sizes = {u: s for us, s in zip(((f"{u}B", f"{u}iB") for u in "KMGT"), (1024 ** s for s in range(1, 5))) for u in us}


class qBittorrent:
    spaceQuota = 50 * sizes["GiB"]
    upSpeedThresh = 2.6 * sizes["MiB"]
    dlSpeedThresh = 8 * sizes["MiB"]

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

        self.removeList = self.removeCand = None
        self.newTorrent = {}
        self.newTorrentMinPeer = 40
        self.removableSize = self.demandSize = 0
        self.upSpeed = self.dlSpeed = None
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

    def clean_seedDir(self):
        if not self.seedDir:
            return

        refresh = False
        names = set(i["name"] for i in self.torrents.values())
        if os.path.dirname(self.watchDir) == self.seedDir:
            names.add(os.path.basename(self.watchDir))

        with os.scandir(self.seedDir) as it:
            for entry in it:
                name = os.path.splitext(entry.name)[0] if entry.name.endswith(".!qB") else entry.name
                if name not in names:
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
                    log.append("Cleanup", None, entry.name)
        if refresh:
            self._init_freeSpace()

    def action_needed(self) -> bool:
        print(
            "qBittorrent average speed last hour, ul: {}/s, dl: {}/s. Free space: {}".format(
                humansize(self.upSpeed), humansize(self.dlSpeed), humansize(self.freeSpace)
            )
        )
        if self.freeSpace < 0:
            return True
        if self.state["up_rate_limit"] <= self.upSpeedThresh or self.state["use_alt_speed_limits"]:
            return False
        return 0 <= self.upSpeed < self.upSpeedThresh and 0 <= self.dlSpeed < self.dlSpeedThresh

    def build_remove_lists(self, data):
        trs = self.torrents
        oneDayAgo = pd.Timestamp.now().timestamp() - 86400

        self.removeCand = tuple((k, trs[k]["size"]) for k in data.get_slows() if trs[k]["added_on"] < oneDayAgo)
        self.removableSize = sum(i[1] for i in self.removeCand)
        self.newTorrentMinPeer = max(*(trs[k[0]]["num_incomplete"] for k in self.removeCand), self.newTorrentMinPeer)
        self._update_availSpace()

        print(f"Remove candidates: {humansize(self.removableSize)}")
        for k, v in self.removeCand:
            print(f'Name: {trs[k]["name"]}, size: {humansize(v)}')

    def add_torrent(self, filename: str, content: bytes, size: int):
        if filename not in self.newTorrent:
            self.newTorrent[filename] = content
            self.demandSize += size
            self._update_availSpace()
            return True
        print(f"Error: {filename} has already been added.")
        return False

    def _update_availSpace(self):
        self.availSpace = self.freeSpace + self.removableSize - self.demandSize

    def promote_candidates(self):
        targetSize = self.demandSize - self.freeSpace
        if targetSize > 0:
            print(
                f"Demand space: {humansize(self.demandSize)},",
                f"free space: {humansize(self.freeSpace)},",
                f"space to free: {humansize(targetSize)}",
            )
            self.removeList, minSum = self.findMinSum(self.removeCand, targetSize)
            print(f"Removals: {len(self.removeList)}, size: {humansize(minSum)}")

    @staticmethod
    def findMinSum(pairs: tuple, targetSize: int):
        """Find the minimum sum of a list of numbers to reach the target size.
        Input should be a list of key-number pairs. Returns the pairs and the sum.
        If the sum is lesser than the target, return the original pairs.
        """

        def _innerLoop(parentKeys=0, parentSum=0, i=0):
            nonlocal minKeys, minSum
            for n in range(i, len(nums)):
                currentSum = parentSum + nums[n]
                if currentSum < minSum:
                    currentKeys = parentKeys | masks[n]
                    if currentSum < targetSize:
                        _innerLoop(currentKeys, currentSum, n + 1)
                    else:
                        minKeys, minSum = currentKeys, currentSum

        nums = tuple(i[1] for i in pairs)
        minSum = sum(nums)
        if minSum > targetSize:
            minKeys = 2 ** len(nums) - 1
            masks = tuple(1 << i for i in range(len(nums)))
            _innerLoop()
            minKeys = tuple(i for n, i in enumerate(pairs) if minKeys & masks[n])
            return minKeys, minSum
        return pairs, minSum

    def apply_removes(self):
        if self.removeList:
            if not debug:
                path = "torrents/delete"
                payload = {"hashes": "|".join(i[0] for i in self.removeList), "deleteFiles": True}
                self._request(path, params=payload)
            for k, v in self.removeList:
                log.append("Remove", v, self.torrents[k]["name"])

    def upload_torrent(self):
        if self.newTorrent:
            if debug:
                return
            try:
                if not os.path.exists(self.watchDir):
                    os.mkdir(self.watchDir)
                for filename, content in self.newTorrent.items():
                    path = os.path.join(self.watchDir, filename)
                    with open(path, "wb") as f:
                        f.write(content)
            except Exception as e:
                log.append("Error", None, f"Writing torrent to {self.watchDir} failed: {e}")

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
        self.frameHash = None
        self.mteamHistory = set()
        self.init_session()

    def integrity_test(self):
        attrs = (
            "qBittorrentFrame",
            "torrentFrame",
            "frameHash",
            "mteamHistory",
            "session",
        )
        return all(hasattr(self, attr) for attr in attrs) and self._hash_dataframe() == self.frameHash

    def _hash_dataframe(self):
        return (hash_pandas_object(self.qBittorrentFrame).sum(), hash_pandas_object(self.torrentFrame).sum())

    def init_session(self) -> requests.session:
        """Initialize a new requests session."""
        self.session = requests.session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
            }
        )
        return self.session

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
        self.frameHash = self._hash_dataframe()

        speeds = self.qBittorrentFrame.last("H").resample("S").bfill().diff().mean()
        qb.upSpeed = speeds["upload"]
        qb.dlSpeed = speeds["download"]

    def get_slows(self) -> pd.Index:
        """Trying to discover slow torrents using jenks natural breaks method."""
        speeds = self.torrentFrame
        speeds = speeds.last("D").resample("S").bfill().diff().mean()
        breaks = speeds.count() - 1
        if breaks >= 2:
            breaks = jenks_breaks(speeds, nb_class=(4 if breaks > 4 else breaks))[1]
        else:
            breaks = speeds.mean()
        return speeds.loc[speeds <= breaks].index

    def dump(self, datafile: str, backupDir=None):
        if debug:
            return
        try:
            with open(datafile, "wb") as f:
                pickle.dump(self, f)
        except Exception as e:
            log.append("Error", None, f"Writing data to disk failed: {e}")
            return
        if backupDir:
            copy_backup(datafile, backupDir)


class Log:
    def __init__(self):
        self.log = []

    def append(self, action, size, name):
        self.log.append(
            "{:20}{:12}{:14}{}\n".format(
                pd.Timestamp.now().strftime("%D %T"),
                action,
                humansize(size),
                name,
            )
        )

    def write(self, logfile: str, backupDir=None):
        if not self.log:
            return

        if debug:
            print("Logs:")
            for log in reversed(self.log):
                print(log, end="")
            return

        try:
            with open(logfile, mode="r", encoding="utf-8") as f:
                oldLog = f.readlines()[2:]
        except Exception:
            oldLog = None

        with open(logfile, mode="w", encoding="utf-8") as f:
            f.write("{:20}{:12}{:14}{}\n{}\n".format("Date", "Action", "Size", "Name", "-" * 80))
            f.writelines(reversed(self.log))
            if oldLog:
                f.writelines(oldLog)

        if backupDir:
            copy_backup(logfile, backupDir)


def load_data(datafile: str):
    """Load Data object from pickle."""
    try:
        with open(datafile, mode="rb") as f:
            data = pickle.load(f)
        assert data.integrity_test(), "Intergrity check failed."
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


def mteam_download(mteamFeeds: tuple, mteamAccount: tuple, maxDownloads: int):

    # These two functions should be wrapped inside a try...except block
    def to_int(string):
        return int(re.sub(r"[^0-9]+", "", string))

    def size_convert(string):
        m = re.search(r"(?P<num>[0-9]+(\.[0-9]+)?)\s*(?P<unit>[TGMK]i?B)", string)
        return int(float(m["num"]) * sizes[m["unit"]])

    domain = "https://pt.m-team.cc"
    login_page = urljoin(domain, "takelogin.php")
    login_referer = {"referer": urljoin(domain, "login.php")}
    payload = {"username": mteamAccount[0], "password": mteamAccount[1]}
    re_download = re.compile(r"\bdownload.php\?")
    re_details = re.compile(r"\bdetails.php\?")
    re_timelimit = re.compile(r"限時：[^日]*$")
    re_date = re.compile(r"20[0-9]{2}(\W[0-9]{2}){2}\s+([0-9]{2}\W){2}[0-9]{2}")
    session = data.session
    mteamHistory = data.mteamHistory
    newTorrentMinPeer = qb.newTorrentMinPeer
    results = []

    print(f"Connecting to M-Team... Max avail space: {humansize(qb.availSpace)}, minimum peers: {newTorrentMinPeer}")
    for feed in mteamFeeds:
        feed = urljoin(domain, feed)

        for i in range(5):
            try:
                response = session.get(feed)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")
                if "login.php" in response.url or "登錄" in soup.title.string:
                    assert i < 4, "login failed."
                    print("Login...")
                    session.post(login_page, data=payload, headers=login_referer)
                else:
                    break
            except Exception as e:
                if i == 4:
                    print(e)
                    return
                print(f"Retrying... Attempt: {i+1}, Error: {e}")
                session = data.init_session()

        print("Fetching feed success.")

        cols = {}
        for i, td in enumerate(soup.select("#form_torrent table.torrents > tr:nth-of-type(1) > td")):
            title = td.find(title=True)
            if title:
                title = title["title"]
            else:
                title = td.get_text(strip=True)
            cols[title] = i
        colTitle = cols.get("標題", 1)
        colDate = cols.get("存活時間", 3)
        colSize = cols.get("大小", 4)
        colUp = cols.get("種子數", 5)
        colDown = cols.get("下載數", 6)

        for tr in soup.select("#form_torrent table.torrents > tr:not(:nth-of-type(1))"):
            try:
                td = tr.find_all("td", recursive=False)
                downloader = to_int(td[colDown].get_text())
                if downloader <= newTorrentMinPeer:
                    continue
                link = td[colTitle].find("a", href=re_download)["href"]
                tid = re.search(r"id=([0-9]+)", link).group(1)
                if (
                    tid in mteamHistory
                    or td[colTitle].find(string=re_timelimit)
                    or td[colUp].get_text(strip=True) == "0"
                ):
                    continue

                title = td[colTitle].find("a", href=re_details, string=True)
                title = title["title"] if title.has_attr("title") else title.get_text(strip=True)
                date = td[colDate].find(title=re_date)["title"]
                date = pd.Timestamp(date).timestamp()
                size = size_convert(td[colSize].get_text())

                results.append((downloader, date, size, tid, title, link))
            except Exception as e:
                print("Parsing page error:", e)

    results.sort(reverse=True)
    print(f"Done. {len(results)} torrents collected.")
    for downloader, date, size, tid, title, link in results:
        if size > qb.availSpace:
            continue

        try:
            response = session.get(urljoin(domain, link))
            response.raise_for_status()
        except Exception as e:
            print(f"Downloading {title} torrent failed. {e}")
            return

        filename = f"{tid}.torrent"
        if qb.add_torrent(filename, response.content, size):
            log.append("Download", size, title)
            if not debug:
                mteamHistory.add(tid)
            print(f"New torrent: {title}, size: {humansize(size)}, max avail: {humansize(qb.availSpace)}")

        if len(qb.newTorrent) >= maxDownloads:
            return


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

    if qb.action_needed() or debug:
        qb.build_remove_lists(data)
        if qb.availSpace > 0:
            mteam_download(config.mteamFeeds, config.mteamAccount, maxDownloads=2)
        qb.promote_candidates()
        qb.apply_removes()
        qb.upload_torrent()
    else:
        print("System is healthy, no action needed.")

    qb.resume_paused()
    data.dump(datafile, config.backupDir)
    log.write(logfile, config.backupDir)

else:
    raise ImportError
