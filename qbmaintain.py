import pickle
import re
import shutil
from collections import namedtuple
from pathlib import Path
from sys import argv
from urllib.parse import urljoin

import pandas as pd
import requests
from jenkspy import jenks_breaks

byteUnit = {u: s for us, s in zip(((f"{u}B", f"{u}iB") for u in "KMGTP"), (1024 ** s for s in range(1, 6))) for u in us}


class qBittorrent:

    Removable = namedtuple("Removable", ("hash", "size", "peer", "title"))

    def __init__(self, *, host: str, seedDir: str, watchDir: str, speedThresh: tuple, spaceQuota: int, datafile: Path):

        self.api_baseurl = urljoin(host, "api/v2/")
        self._session = requests.session()
        path = "sync/maindata"
        maindata = self._request(path).json()

        self.state = maindata["server_state"]
        self.torrents = maindata["torrents"]
        if self.state["connection_status"] not in ("connected", "firewalled"):
            raise RuntimeError("qBittorrent is not connected to the internet.")

        try:
            self.seedDir = Path(seedDir)
            self.watchDir = Path(watchDir)
        except TypeError as e:
            if not debug:
                raise ValueError("seedDir and watchDir are not set properly.")
            self.seedDir = self.watchDir = None

        self.upSpeedThresh, self.dlSpeedThresh = (int(i * byteUnit["MiB"]) for i in speedThresh)
        self.spaceQuota = int(spaceQuota * byteUnit["GiB"])
        self.preferences = None

        self.datafile = datafile
        self.data = self._load_data()
        self.upSpeed, self.dlSpeed = self.data.record(self)
        print(f"qBittorrent average speed last hour: UL: {humansize(self.upSpeed)}/s, DL: {humansize(self.dlSpeed)}/s.")

    def get_preference(self, key: str):
        if self.preferences is None:
            self.preferences = self._request("app/preferences").json()
        return self.preferences[key]

    def _request(self, path: str, **kwargs):
        response = self._session.get(urljoin(self.api_baseurl, path), **kwargs, timeout=7)
        response.raise_for_status()
        return response

    def _load_data(self):
        """Load Data object from pickle."""
        try:
            with self.datafile.open(mode="rb") as f:
                data = pickle.load(f)
            assert data.integrity_test(), "Intergrity test failed."
        except (OSError, pickle.PickleError, AssertionError) as e:
            print(f"Loading data from '{self.datafile}' failed: {e}")
            if not debug:
                try:
                    self.datafile.rename(f"{self.datafile}_{pd.Timestamp.now().strftime('%y%m%d_%H%M%S')}")
                except OSError:
                    pass
            data = Data()
        return data

    def clean_seedDir(self):
        try:
            listdir = self.seedDir.iterdir()
        except AttributeError:
            return

        names = frozenset(i["name"] for i in self.torrents.values())
        for path in listdir:
            if path.name not in names:
                if path.suffix == ".!qB" and path.stem in names:
                    continue
                print("Cleanup:", path.name)
                try:
                    if debug:
                        pass
                    elif path.is_dir():
                        shutil.rmtree(path)
                    else:
                        path.unlink()
                except OSError as e:
                    print("Deletion Failed:", e)
                else:
                    log.record("Cleanup", None, path.name)

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
        try:
            path = self.watchDir / filename
            with path.open("wb") as f:
                f.write(content)
        except OSError as e:
            try:
                if not self.watchDir.exists():
                    self.watchDir.mkdir()
                    return self.add_torrent(filename, content)
            except OSError:
                pass

            msg = f"Saving '{filename}' to '{self.watchDir}' failed: {e}"
            log.record("Error", None, msg)
            print(msg)
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

    def dump_data(self):
        if debug:
            return
        try:
            with self.datafile.open("wb") as f:
                pickle.dump(self.data, f)
        except (OSError, pickle.PickleError) as e:
            msg = f"Writing data to disk failed: {e}"
            log.record("Error", None, msg)
            print(msg)


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
        try:
            return all(
                isinstance(x, y)
                for x, y in (
                    (self.qBittorrentFrame, pd.DataFrame),
                    (self.torrentFrame, pd.DataFrame),
                    (self.mteamHistory, set),
                    (self.session, requests.Session),
                )
            )
        except AttributeError:
            return False

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
            breaks = jenks_breaks(speeds, nb_class=min(speeds.count().item() - 1, 3))[1]
        except Exception as e:
            print("Jenkspy failed:", e)
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
        self.account = account
        self.qb = qb
        self.session = qb.data.session
        self.history = qb.data.mteamHistory
        self.loginParam = None
        self.minPeer = (minPeer[0] / byteUnit["GiB"], minPeer[1])

    def _get(self, path: str):
        url = urljoin(self.domain, path)
        for i in range(3):
            try:
                response = self.session.get(url, timeout=(7, 28))
                response.raise_for_status()
                if "/login.php" not in response.url:
                    return response
                if i < 2:
                    self._login()
            except requests.HTTPError:
                pass
            except Exception:
                self.session = self.qb.data.init_session()

    def _login(self):
        if not self.loginParam:
            self.loginParam = {
                "url": urljoin(self.domain, "takelogin.php"),
                "data": {"username": self.account[0], "password": self.account[1]},
                "headers": {"referer": urljoin(self.domain, "login.php")},
            }
        self.session.post(**self.loginParam)

    def fetch(self):
        from bs4 import BeautifulSoup

        re_download = re.compile(r"\bdownload\.php\?")
        re_details = re.compile(r"\bdetails\.php\?")
        re_timelimit = re.compile(r"限時：[^日]*$")
        re_nondigit = re.compile(r"[^0-9]+")
        re_size = re.compile(r"(?P<num>[0-9]+(\.[0-9]+)?)\s*(?P<unit>[KMGT]i?B)")
        re_tid = re.compile(r"\bid=(?P<tid>[0-9]+)")
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
                    if peer < A * size + B:
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
            if not debug:
                try:
                    content = self._get(t.link).content
                    assert self.qb.add_torrent(f"{t.tid}.torrent", content)
                except (AttributeError, AssertionError):
                    print("Failed:", t.title)
                    continue
            self.history.add(t.tid)
            log.record("Download", t.size, t.title)
            print("Download:", t.title)


class MPSolver:
    """Using OR-Tools from Google to find the best combination of downloads and removals.
    The goal is to maximize obtained peers under several constraints.

    Constraints:
        1: sum(downloadSize) <= freeSpace + sum(removedSize)
            --> sum(downloadSize) - sum(removedSize) <= freeSpace
            When freeSpace + sum(removedSize) < 0, this become impossible to satisfy.
            So the algorithm should delete all remove candidates to free up space.
        2: total download <= qBittorrent max_active_downloads

    Objective:
        Maximize: sum(downloadPeer) - sum(removedPeer) * 0.5
    """

    def __init__(self, *, removeCand, downloadCand, qb: qBittorrent):
        self.removeCand = tuple(removeCand)
        self.downloadCand = tuple(downloadCand)
        self.removeCandSize = sum(i.size for i in self.removeCand)
        self.qb = qb
        self.freeSpace = qb.freeSpace
        self.maxDownloads = qb.get_preference("max_active_downloads")
        self.wall_time = None
        self._solve()

    def _solve(self):
        if self.freeSpace < -self.removeCandSize:
            self.removeList = self.removeCand
            self.downloadList = ()
            return

        from ortools.sat.python import cp_model

        model = cp_model.CpModel()

        sizeCoef = [-t.size for t in self.removeCand]
        peerCoef = [-t.peer for t in self.removeCand]
        sizeCoef.extend(t.size for t in self.downloadCand)
        peerCoef.extend(t.peer * 2 for t in self.downloadCand)
        pool = tuple(model.NewBoolVar(f"{i}") for i in range(len(sizeCoef)))

        model.Add(cp_model.LinearExpr.ScalProd(pool, sizeCoef) <= self.freeSpace)
        model.Add(cp_model.LinearExpr.Sum(pool) <= self.maxDownloads)
        model.Maximize(cp_model.LinearExpr.ScalProd(pool, peerCoef))

        solver = cp_model.CpSolver()
        if solver.Solve(model) == cp_model.OPTIMAL:
            self.wall_time = solver.WallTime()
            self.obj_value = solver.ObjectiveValue()

            split = len(self.removeCand)
            self.removeList = tuple(t for t, v in zip(self.removeCand, pool) if solver.Value(v))
            self.downloadList = tuple(t for t, v in zip(self.downloadCand, pool[split:]) if solver.Value(v))
        else:
            self.removeList = self.downloadList = ()

    def report(self):
        sepSlim = "-" * 50
        maxAvailSpace = self.freeSpace + self.removeCandSize
        removeSize = sum(i.size for i in self.removeList)
        downloadSize = sum(i.size for i in self.downloadList)
        finalFreeSpace = self.freeSpace + removeSize - downloadSize

        print(sepSlim)
        print(
            "Download candidates: {}. Total: {}. Limit: {}.".format(
                len(self.downloadCand),
                humansize(sum(i.size for i in self.downloadCand)),
                self.maxDownloads,
            )
        )
        print(
            "Remove candidates: {}/{}. Total: {}.".format(
                len(self.removeCand),
                len(self.qb.torrents),
                humansize(self.removeCandSize),
            )
        )
        print(
            "Disk free space: {}. Max avail space: {}.".format(
                humansize(self.freeSpace),
                humansize(maxAvailSpace),
            )
        )
        for v in self.removeCand:
            print(f"[{humansize(v.size):>11}|{v.peer:3d} peers] {v.title}")

        print(sepSlim)
        if self.wall_time is None:
            print("CP-SAT solver cannot find an optimal solution.")
        else:
            print(f"Problem solved in {self.wall_time:5f} seconds, objective value: {self.obj_value}.")

        print(f"Free space left after operation: {humansize(self.freeSpace)} => {humansize(finalFreeSpace)}.")

        for title, final, cand, size in (
            ("Download", self.downloadList, self.downloadCand, downloadSize),
            ("Remove", self.removeList, self.removeCand, removeSize),
        ):
            print(sepSlim)
            print(
                "{}: {}/{}. Total: {}, {} peers.".format(
                    title,
                    len(final),
                    len(cand),
                    humansize(size),
                    sum(i.peer for i in final),
                )
            )
            for v in final:
                print(f"[{humansize(v.size):>11}|{v.peer:3d} peers] {v.title}")
        print(sepSlim)


class Log(list):
    def record(self, action, size, name):
        self.append("{:20}{:12}{:14}{}\n".format(pd.Timestamp.now().strftime("%D %T"), action, humansize(size), name))

    def write(self, logfile: Path):
        if not self:
            return

        sep = "-" * 80
        header = "{:20}{:12}{:14}{}\n{}\n".format("Date", "Action", "Size", "Name", sep)
        content = reversed(self)

        if debug:
            print(sep)
            print(header, *content, sep="", end="")
            return

        try:
            with logfile.open(mode="r+", encoding="utf-8") as f:
                try:
                    oldLog = f.readlines()[2:]
                except IndexError:
                    oldLog = ()
                f.seek(0)
                f.truncate()
                f.write(header)
                f.writelines(content)
                f.writelines(oldLog)
        except FileNotFoundError:
            with logfile.open(mode="w", encoding="utf-8") as f:
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

    script_dir = Path(__file__).parent
    datafile = script_dir / "data"
    logfile = script_dir / "qb-maintenance.log"

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
        mipsolver = MPSolver(
            removeCand=qb.get_remove_cands(),
            downloadCand=mteam.fetch(),
            qb=qb,
        )
        mipsolver.report()

        qb.remove_torrents(mipsolver.removeList)
        mteam.download(mipsolver.downloadList)

    qb.resume_paused()
    qb.dump_data()
    log.write(logfile)


log = Log()
debug = False

if __name__ == "__main__":
    main()
