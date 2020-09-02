import os
import pickle
import re
import shutil
from sys import argv

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas.util import hash_pandas_object
from requests.compat import urljoin

import qbconfig

sizes = {}
sizes["TiB"] = sizes["TB"] = 1024 ** 4
sizes["GiB"] = sizes["GB"] = 1024 ** 3
sizes["MiB"] = sizes["MB"] = 1024 ** 2
sizes["KiB"] = sizes["KB"] = 1024


class qBittorrent:
    errors = frozenset(("error", "missingFiles", "pausedUP", "pausedDL", "unknown"))
    spaceQuota = 50 * sizes["GiB"]
    dlspeedThreshold = 8 * sizes["MiB"]
    upspeedThreshold = 2.6 * sizes["MiB"]

    def __init__(self, api_host: str, seed_dir: str, watch_dir: str):
        """api_host: http://localhost"""

        self.api_baseurl = urljoin(api_host, "api/v2/")
        self.seed_dir = os.path.abspath(seed_dir) if seed_dir else None
        self.watch_dir = os.path.abspath(watch_dir) if watch_dir else None
        self.isLocalhost = True if seed_dir and watch_dir else False

        path = "sync/maindata"
        maindata = self._request(path).json()
        if maindata["server_state"]["connection_status"] not in ("connected", "firewalled"):
            raise RuntimeError("qBittorrent is not connected to the internet.")
        self.state = maindata["server_state"]
        self.torrents = maindata["torrents"]
        self.hasErrors = any(i["state"] in self.errors for i in self.torrents.values())

        self.removeList = {}
        self.removeCand = {}
        self.newTorrent = {}
        self.removeListSize = 0
        self.removeCandSize = 0
        self.newTorrentSize = 0
        self.dlspeed = self.upspeed = None
        self._init_freeSpace(self.state["free_space_on_disk"])

    def _request(self, path, **kwargs):
        response = requests.get(urljoin(self.api_baseurl, path), **kwargs)
        response.raise_for_status()
        return response

    def _init_freeSpace(self, free_space_on_disk):
        self.freeSpace = self.availSpace = (
            free_space_on_disk - sum(i["amount_left"] for i in self.torrents.values()) - self.spaceQuota
        )

    def _update_availSpace(self):
        self.availSpace = self.freeSpace + self.removeListSize + self.removeCandSize - self.newTorrentSize

    def clean_seed_dir(self):
        if not self.isLocalhost:
            return

        refresh = False
        qb_ext = re.compile("\.!qB$")
        names = set(i["name"] for i in self.torrents.values())
        if os.path.dirname(self.watch_dir) == self.seed_dir:
            names.add(os.path.basename(self.watch_dir))

        with os.scandir(self.seed_dir) as it:
            for entry in it:
                if qb_ext.sub("", entry.name) not in names:
                    print("Cleanup:", entry.name)
                    if not debug:
                        try:
                            if entry.is_dir():
                                shutil.rmtree(entry.path)
                            else:
                                os.remove(entry.path)
                        except Exception:
                            print("Deletion Failed.")
                            continue
                    refresh = True
                    log.append("Cleanup", None, entry.name)
        if refresh:
            self._init_freeSpace(shutil.disk_usage(self.seed_dir).free)

    def action_needed(self):
        if debug or self.freeSpace < 0:
            return True
        return self.dlspeed < self.dlspeedThreshold and self.upspeed < self.upspeedThreshold

    def build_remove_lists(self, data):
        oneDayAgo = pd.Timestamp.now().value // pow(10, 9) - 86400  # Epoch timestamp
        singleSpeedThreshold = self.upspeedThreshold // (len(self.torrents) ** 2)

        for k, speed in data.speed_sorted().items():
            v = self.torrents[k]
            if v["added_on"] > oneDayAgo:
                continue

            if self.availSpace < 0:
                self.add_removes(k, v["size"], v["name"], direct=True)

            elif (
                speed <= singleSpeedThreshold
                or 0 < v["last_activity"] <= oneDayAgo
                or (v["num_incomplete"] <= 3 and v["dlspeed"] == v["upspeed"] == 0)
            ):
                self.add_removes(k, v["size"], v["name"], direct=False)

    def add_removes(self, key, size, name, direct=False):
        """direct: True: to removeList, False: to removeCand"""
        if direct:
            self.removeList[key] = size
            self.removeListSize += size
            displayName = "removes"
        else:
            self.removeCand[key] = size
            self.removeCandSize += size
            displayName = "remove candidates"

        self._update_availSpace()
        print("Add to {}: {}, size: {}".format(displayName, name, humansize(size)))

    def add_torrent(self, filename: str, content: bytes, size: int):
        self.newTorrent[filename] = content
        self.newTorrentSize += size
        self._update_availSpace()

    def promote_candidates(self):
        targetSize = self.newTorrentSize - self.freeSpace - self.removeListSize
        if targetSize > 0 and self.removeCand:
            print("Total size: {}, space to free: {}".format(humansize(self.newTorrentSize), humansize(targetSize)))

            minKeys, minSum = self.findMinSum(self.removeCand, targetSize)
            for k in minKeys:
                self.removeList[k] = self.removeCand.pop(k)
            self.removeCandSize -= minSum
            self.removeListSize += minSum
            self._update_availSpace()

            print("Removals: {}, size: {}".format(len(minKeys), humansize(minSum)))

    def apply_removes(self):
        if self.removeList:
            if not debug:
                path = "torrents/delete"
                payload = {"hashes": "|".join(self.removeList), "deleteFiles": True}
                self._request(path, params=payload)
            for k, v in self.removeList.items():
                log.append("Remove", v, self.torrents[k]["name"])

    def upload_torrent(self):
        if self.newTorrent and not debug:
            if not self.isLocalhost:
                print("Uploading torrent to remote host is not support yet.")
                return
            if not os.path.exists(self.watch_dir):
                os.mkdir(self.watch_dir)
            for filename, content in self.newTorrent.items():
                path = os.path.join(self.watch_dir, filename)
                with open(path, "wb") as f:
                    f.write(content)

    def resume_paused(self):
        if self.hasErrors:
            print("Resume torrents.")
            path = "torrents/resume"
            payload = {"hashes": "all"}
            self._request(path, params=payload)

    @staticmethod
    def findMinSum(candidates: dict, targetSize: int):
        """Find the minimum sum of a list of numbers to reach the target size.
        Returns the keys and the sum.
        Input should be a dict of key-number pairs.
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

        candidates = tuple(candidates.items())
        nums = tuple(i[1] for i in candidates)
        minKeys, minSum = 2 ** len(nums) - 1, sum(nums)
        if minSum >= targetSize:
            masks = tuple(pow(2, i) for i in range(len(candidates)))
            _innerLoop()
            minKeys = tuple(i[0] for n, i in enumerate(candidates) if minKeys & masks[n])
            return minKeys, minSum
        return None


class Data:
    def __init__(self):
        self.lastRecordTime = None
        self.lastAlltimeTraffic = None
        self.lastSessionTraffic = None
        self.speedFrame = pd.DataFrame()
        self.trafficFrame = pd.DataFrame()
        self.frameHash = None
        self.skipThisTime = True
        self.session = requests.session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
            }
        )
        self.mteamHistory = set()

    def integrity_test(self):
        attrs = (
            "lastRecordTime",
            "lastAlltimeTraffic",
            "lastSessionTraffic",
            "speedFrame",
            "trafficFrame",
            "frameHash",
            "skipThisTime",
            "session",
            "mteamHistory",
        )

        if all(hasattr(self, attr) for attr in attrs) and self._hash_dataframe() == self.frameHash:
            return True
        return False

    def _hash_dataframe(self):
        return (hash_pandas_object(self.trafficFrame).sum(), hash_pandas_object(self.speedFrame).sum())

    def record(self, qb: qBittorrent):
        now = pd.Timestamp.now()
        sessionTraffic = (qb.state["up_info_data"], qb.state["dl_info_data"])
        alltimeTraffic = (qb.state["alltime_ul"], qb.state["alltime_dl"])
        self.skipThisTime = True

        try:
            timeSpan = now - self.lastRecordTime
            if debug or (
                all(i >= j for i, j in zip(sessionTraffic, self.lastSessionTraffic))
                and qb.state["use_alt_speed_limits"] == False
                and qb.state["up_rate_limit"] > qb.up_threshold
                and pd.Timedelta(minutes=15) <= timeSpan <= pd.Timedelta(minutes=60)
            ):
                self.skipThisTime = False
        except Exception:
            pass

        for f in (self.speedFrame, self.trafficFrame):
            f.drop(columns=f.columns.difference(qb.torrents), inplace=True, errors="ignore")

        trafficRow = pd.DataFrame({k: v["uploaded"] for k, v in qb.torrents.items()}, index=[now])

        if not self.skipThisTime:
            timeSpan = timeSpan.seconds
            qb.upspeed, qb.dlspeed = ((i - j) // timeSpan for i, j in zip(alltimeTraffic, self.lastAlltimeTraffic))
            print(f"Average uploading speed: {qb.upspeed // 1024}k/s, time span: {timeSpan}s.")

            if qb.hasErrors:
                speedRow = trafficRow[[k for k, v in qb.torrents.items() if v["state"] not in qb.errors]]
            else:
                speedRow = trafficRow
            speedRow = speedRow.subtract(self.trafficFrame.iloc[-1]) // timeSpan
            self.speedFrame = self.speedFrame.append(speedRow, sort=False)

        self.trafficFrame = self.trafficFrame.append(trafficRow, sort=False)
        self.lastRecordTime = now
        self.lastSessionTraffic = sessionTraffic
        self.lastAlltimeTraffic = alltimeTraffic
        self.frameHash = self._hash_dataframe()

    def speed_sorted(self):
        return self.speedFrame.last("D").mean().sort_values()

    def dump(self, datafile: str, backup_dest=None):
        try:
            with open(datafile, "wb") as f:
                pickle.dump(self, f)
        except Exception as e:
            log.append("Error", None, f"Writing data to disk failed: {e}")
            return
        if backup_dest:
            copy_backup(datafile, backup_dest)


class Log:
    def __init__(self):
        self.log = []

    def append(self, action, size, name):
        self.log.append(
            "{:20}{:12}{:14}{}\n".format(
                pd.Timestamp.now().strftime("%D %T"), action, humansize(size) if size else "---", name,
            )
        )

    def write(self, logfile: str, backup_dest=None):
        if not self.log:
            return

        if debug:
            print("Logs:")
            for log in reversed(self.log):
                print(log, end="")
            return

        try:
            with open(logfile, mode="r", encoding="utf-8") as f:
                oldLog = "".join(f.readlines()[2:])
        except Exception:
            oldLog = None

        with open(logfile, mode="w", encoding="utf-8") as f:
            f.write("{:20}{:12}{:14}{}\n{}\n".format("Date", "Action", "Size", "Name", "-" * 80))
            f.writelines(reversed(self.log))
            if oldLog:
                f.write(oldLog)

        if backup_dest:
            copy_backup(logfile, backup_dest)


def copy_backup(source, dest):
    try:
        shutil.copy(source, dest)
    except Exception as e:
        print(f'Copying "{source}" to "{dest}" failed: {e}')


def load_data(datafile: str):
    """Load Data object from pickle."""
    try:
        with open(datafile, mode="rb") as f:
            data = pickle.load(f)
        assert data.integrity_test()
    except Exception:
        try:
            os.rename(datafile, f"{datafile}_{pd.Timestamp.now().strftime('%y%m%d_%H%M%S')}")
        except Exception:
            pass
        data = Data()
    return data


def humansize(size, suffixes=("KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")):
    for suffix in suffixes:
        size /= 1024
        if size < 1024:
            return "{:.2f} {}".format(size, suffix)
    return "too large for me"


def mteam_download(feed_url, username, password, qb: qBittorrent, maxDownloads=1):
    def to_int(string):
        return int(re.sub(r"[^0-9]+", "", string))

    def size_convert(string):
        num = float(re.search(r"[0-9.]+", string).group())
        unit = re.search(r"[TGMK]i?B", string).group()
        return int(num * sizes[unit])

    if len(qb.newTorrent) >= maxDownloads:
        return

    domain = "https://pt.m-team.cc"

    login_index = urljoin(domain, "login.php")
    login_page = urljoin(domain, "takelogin.php")
    feed_url = urljoin(domain, feed_url)
    payload = {"username": username, "password": password}
    session = data.session

    print("Connecting to M-Team... Max avail:", humansize(qb.availSpace))

    for i in range(5):
        try:
            response = session.get(feed_url)
            soup = BeautifulSoup(response.content, "html.parser")
            assert "login.php" not in response.url and "登錄" not in soup.title.string, "login invalid."
            print("Session valid.")
            break
        except Exception as e:
            if i == 4:
                print("Login Failed.")
                return
            print(f"Login... Attempt: {i+1}, Error: {e}")
            session.post(login_page, data=payload, headers={"referer": login_index})

    re_download = re.compile(r"\bdownload.php\?")
    re_details = re.compile(r"\bdetails.php\?")
    re_tid = re.compile(r"id=([0-9]+)")
    re_xs = re.compile(r"限時：")

    for tr in soup.select("#form_torrent table.torrents > tr:not(:nth-of-type(1))"):
        try:
            row = tr.find_all("td", recursive=False)

            link = row[1].find("a", href=re_download)["href"]
            tid = re_tid.search(link).group(1)
            if not tid or tid in data.mteamHistory:
                continue

            size = size_convert(row[4].text)
            uploader = to_int(row[5].text)
            downloader = to_int(row[6].text)
            xs = row[1].find(string=re_xs)
            if size > qb.availSpace or uploader == 0 or downloader < 20 or (xs and "日" not in xs):
                continue

            title = row[1].find("a", href=re_details, title=True)["title"]

            response = session.get(urljoin(domain, link))
            if not response.ok:
                continue

            try:
                filename = re.search(
                    r"filename=['\"]*(.+?\.torrent)\b", response.headers["Content-Disposition"]
                ).group(1)
                filename = requests.utils.unquote(filename)
            except Exception:
                filename = f"{tid}.torrent"

            qb.add_torrent(filename, response.content, size)
            log.append("Download", size, title)
            if not debug:
                data.mteamHistory.add(tid)

            print(
                "New torrent: {}, size: {}, space remains: {}".format(title, humansize(size), humansize(qb.availSpace))
            )

        except Exception:
            continue

        if len(qb.newTorrent) >= maxDownloads:
            return


if __name__ == "__main__":
    debug = True if len(argv) > 1 and argv[1].startswith("-d") else False
    qb = qBittorrent(qbconfig.api_host, qbconfig.seed_dir, qbconfig.watch_dir)

    script_dir = os.path.dirname(__file__)
    datafile = os.path.join(script_dir, "data")
    logfile = os.path.join(script_dir, "qb-maintenance.log")

    log = Log()
    data = load_data(datafile)
    data.record(qb)
    qb.clean_seed_dir()

    if not data.skipThisTime and qb.action_needed():
        qb.build_remove_lists(data)
        if qb.availSpace > 0:
            mteam_download(qbconfig.feed_url, qbconfig.username, qbconfig.password, qb, maxDownloads=1)
            qb.promote_candidates()
        qb.apply_removes()
        qb.upload_torrent()
    else:
        print("System is healthy, no action needed.")

    qb.resume_paused()
    data.dump(datafile, qbconfig.backup_dest)
    log.write(logfile, qbconfig.backup_dest)

else:
    raise ImportError
