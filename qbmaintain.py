import json
import os
import os.path as op
import pickle
import re
import shutil
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, Sequence
from urllib.parse import urljoin

# Beautifulsoup, ortools, jenkspy, torrentool are imported as needed
import pandas as pd
import requests
from pandas import DataFrame, Timestamp

NOW = Timestamp.now()
BYTESIZE: Dict[str, int] = {
    k: v for kk, v in (
        ((c + "iB", c + "B"), 1024**i) for i, c in enumerate("KMGTP", 1))
    for k in kk
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
    expire: timedelta
    title: str
    hash: str = None


class Logger:
    """Record and write logs in reversed order."""

    __slots__ = "_log"

    def __init__(self) -> None:
        self._log = []

    def __str__(self) -> str:
        return "{:17}    {:8}    {:>11}    {}\n{}\n{}".format(
            "Date", "Action", "Size", "Name", "-" * 80,
            "".join(reversed(self._log)))

    def __bool__(self):
        return not not self._log

    def record(self, action: str, content: str, size: int = None):
        """Record one line of log."""
        self._log.append("{:17}    {:8}    {:>11}    {}\n".format(
            datetime.now().strftime("%D %T"),
            action,
            humansize(size),
            content,
        ))

    def write(self, logfile: str, copy_to: str = None):
        """Insert logs at the beginning of a logfile.

        If `copy_to` is not None, logfile will be copied there.
        """
        if not self._log:
            return
        content = self.__str__()
        print(" Logs ".center(50, "-"), content, sep="\n", end="")
        if dryrun:
            return
        try:
            try:
                with open(logfile, "r+", encoding="utf-8") as f:
                    f.readline()
                    f.readline()
                    backup = f.read()
                    f.seek(0)
                    f.write(content)
                    f.write(backup)
                    f.truncate()
            except FileNotFoundError:
                with open(logfile, "w", encoding="utf-8") as f:
                    f.write(content)
            if copy_to:
                shutil.copy(logfile, copy_to)
        except OSError as e:
            print(e, file=sys.stderr)


class qBittorrent:
    """The manager class for communicating with qBittorrent and data persistence."""

    server_data: DataFrame
    torrent_data: DataFrame
    history: DataFrame
    silence: Timestamp
    is_ready: bool
    requires_download: bool

    _BUSY = {"checkingUP", "checkingDL", "checkingResumeData", "moving"}
    _PAUSED = {"error", "missingFiles", "pausedUP", "pausedDL", "unknown"}
    _DOWNLOADING = {"downloading", "metaDL", "allocating"}
    _STALED = {"stalledDL", "queuedDL"}

    def __init__(self, *, host: str, disk_quota: float, up_rate_thresh: int,
                 dl_rate_thresh: int, dead_up_thresh: int, seed_dir: str,
                 **kwargs):

        self._api_base = urljoin(host, "/api/v2/")
        self.seed_dir = op.normpath(seed_dir) if seed_dir else None
        speed_thresh = (up_rate_thresh * BYTESIZE["KiB"],
                        dl_rate_thresh * BYTESIZE["KiB"])
        self._dead_thresh = dead_up_thresh * BYTESIZE["KiB"]
        self._datafile = "data"
        self._usable_space = self._pref = self._session = None
        self._load_data()

        try:
            maindata: Dict[str, dict] = self._request("sync/maindata").json()
        except Exception as e:
            sys.exit(f"API error: {e}")

        self.server_state = d = maindata["server_state"]
        if d["connection_status"] == "disconnected":
            sys.exit("qBittorrent is disconnected from the internet.")

        throttled = (d["use_alt_speed_limits"] or
                     0 < d["up_rate_limit"] < speed_thresh[0])
        if throttled:
            self._set_silence(hours=1)

        self.torrents = d = maindata["torrents"]
        self.states = Counter(v["state"] for v in d.values())
        self._space_offset = (sum(v["amount_left"] for v in d.values()) +
                              disk_quota * BYTESIZE["GiB"])
        self._record()

        speeds = self.speeds
        print("Avg speed (last hour): UL: {}/s, DL: {}/s".format(
            humansize(speeds[0]), humansize(speeds[1])))

        # Whether qBittorrent is ready for maintenance. The value should be
        # checked before taking any actions.
        # False if:
        #   during silence period (explicitly set after a successful action)
        #   qBittorrent is busy checking, moving data...etc
        self.is_ready = (self.silence <= NOW and
                         self._BUSY.isdisjoint(self.states))
        # Whether qBittorrent requires downloading new torrents.
        # True if:
        #   server speed is bellow threshold (not throttled by user)
        #   more than one limit-free torrents have expired
        self.requires_download = (not throttled and
                                  (speeds < speed_thresh).all() or
                                  self.expired.size > 1)
        # `requires_remove` is a method because it depends on the disk usage
        # that would be changed by calling `clean_seeddir`

    def _request(self, path, *, method="GET", ignore_error=False, **kwargs):
        """Communicate with qBittorrent API."""
        try:
            r = requests.request(
                method,
                self._api_base + path,
                timeout=9.1,
                **kwargs,
            )
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if not ignore_error:
                raise
            print(e, file=sys.stderr)

    def _load_data(self):
        """Load data objects from pickle."""

        types = (DataFrame, DataFrame, DataFrame, Timestamp,
                 requests.cookies.RequestsCookieJar)
        try:
            with open(self._datafile, mode="rb") as f:
                data = pickle.load(f)
            if all(map(isinstance, data, types)):
                (self.server_data, self.torrent_data, self.history,
                 self.silence, self.cookiejar) = data
                return
            raise TypeError("wrong object type")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.record("Error", f"Reading data failed: {e}")
            if not dryrun:
                f = self._datafile
                try:
                    os.rename(f, f"{f}_{NOW.strftime('%y%m%d%H%M%S')}")
                except OSError:
                    pass
        self.server_data = self.torrent_data = self.history = None
        self.silence = NOW
        self.reset_session()

    def dump_data(self):
        """Save data to disk."""
        if dryrun:
            return
        try:
            with open(self._datafile, "wb") as f:
                pickle.dump((self.server_data, self.torrent_data, self.history,
                             self.silence, self.cookiejar), f)
        except Exception as e:
            logger.record("Error", f"Saving data failed: {e}")

    def reset_session(self):
        """Reset and return a new requests session."""
        self._session = self.cookiejar = None
        return self.session

    @property
    def session(self):
        session = self._session
        if session is None:
            session = self._session = requests.Session()
            session.headers.update({
                "User-Agent":
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) "
                    "Gecko/20100101 Firefox/80.0"
            })
            adapter = requests.adapters.HTTPAdapter(max_retries=5)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            if self.cookiejar is None:
                self.cookiejar = session.cookies
            else:
                session.cookies = self.cookiejar
        return session

    def _set_silence(self, hours=0, minutes=0):
        """Set a silent period in which the class would return `is_ready` as
        False.

        The silent period will only be set if it surpass the current setting.
        """
        silence = NOW + timedelta(hours=hours, minutes=minutes)
        if silence > self.silence:
            self.silence = silence

    def _record(self):
        """Record qBittorrent traffic info to pandas DataFrame."""

        # new rows for application and torrents
        app_row = DataFrame(
            {
                "upload": self.server_state["alltime_ul"],
                "download": self.server_state["alltime_dl"]
            },
            index=(NOW,),
        )
        torrent_row = DataFrame(
            {k: v["uploaded"] for k, v in self.torrents.items()},
            index=(NOW,),
        )

        # qBittorrent overall ul/dl speeds
        df = self.server_data
        try:
            last = df.iloc[-1]
            if last.name >= NOW or (last.values > app_row.values).all():
                raise ValueError
            self.server_data = df.truncate(
                before=NOW - timedelta(hours=1),
                copy=False,
            ).append(app_row)
        except Exception:
            self.server_data = app_row

        # upload speeds of each torrent
        df = self.torrent_data
        try:
            last = df.iloc[-1]
            if last.name >= NOW or last.gt(torrent_row.iloc[0]).all():
                raise ValueError
            delete = df.columns.difference(torrent_row.columns)
            if not delete.empty:
                df.drop(columns=delete, inplace=True, errors="ignore")
                df.dropna(how="all", inplace=True)
            self.torrent_data = df.truncate(
                before=NOW - timedelta(days=1),
                copy=False,
            ).append(torrent_row)
        except Exception:
            self.torrent_data = torrent_row

        # expiration records of current torrents
        df = self.history
        try:
            df = df.loc[df.index.isin(torrent_row.columns), "expire"]
            self.expired = df.index[df.values <= NOW]
            if not self.expired.empty:
                self.history.loc[self.expired, "expire"] = pd.NaT
        except Exception:
            self.history = DataFrame(columns=("id", "add", "expire"))
            self.expired = self.history.index

    def clean_seeddir(self):
        """Clean files in seed dir which does not belong to qb download list."""

        seed_dir = self.seed_dir
        if not seed_dir:
            return
        names = {v["name"] for v in self.torrents.values()}
        for name in os.listdir(seed_dir):
            if name not in names and re.sub(r"\.!qB$", "", name) not in names:
                self._usable_space = None
                path = op.join(seed_dir, name)
                print(f"Cleanup: {path}")
                try:
                    if dryrun:
                        pass
                    elif op.isdir(path):
                        # have to set ignore_errors=True so that one failure
                        # does not blow the whole thing up
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        os.unlink(path)
                except OSError as e:
                    print(e, file=sys.stderr)
                else:
                    logger.record("Cleanup", name)

    def requires_remove(self) -> bool:
        """Whether some torrents may need to be deleted.

        True if:
        -   space is bellow threshold
        -   any limited-free torrent has expired
        """
        return self.usable_space < 0 or not self.expired.empty

    def get_remove_cands(self) -> Iterator[Removable]:
        """Discover the slowest torrents using jenks natural breaks."""

        from jenkspy import jenks_breaks

        # calculate avg speed for each torrent
        speeds = self.torrent_speed

        # get 1/3 break point using jenks method
        try:
            breaks = jenks_breaks(speeds, nb_class=min(speeds.size - 1, 3))[1]
        except Exception as e:
            if speeds.size > 2:
                print(f"Jenkspy failed: {e}", file=sys.stderr)
            breaks = speeds.mean()

        torrents = self.torrents
        thresh = self._dead_thresh
        if breaks < thresh:
            breaks = thresh

        removes = speeds[speeds.values <= breaks]
        if not self.expired.empty:
            removes = removes.to_dict()
            removes.update(
                {k: None for k in self.expired if torrents[k]["progress"] < 1})

        # exclude those added in less than 1 day
        yesterday = datetime.now().timestamp() - 86400

        # weight in Removables:
        # speed > deadThresh: None (use the same factor as new torrents)
        # speed <= deadThresh: 1 (minimum value to be considered)
        # expired when still downloading: 0 (delete unconditionally)
        for k, v in removes.items():
            t = torrents[k]
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

    def get_max_dl(self):
        """Max download slots available.

        Returns: None if unlimited. Otherwise it is "max_active_downloads" -
        "current downloading", always >= 0.

        Torrents in stalled-like states are not counted because whether they are
        subjected to limits depends on `dont_count_slow_torrents` setting and
        thresholds. We would promote new torrents to the top of the queue
        afterwards if such torrents exist.
        """

        if self.server_state["queueing"]:
            max_dl: int = self.get_pref("max_active_downloads")
            if max_dl > 0:
                ct = self.states
                max_dl -= sum(ct[k] for k in self._DOWNLOADING)
                return max_dl if max_dl > 0 else 0

    def get_pref(self, key: str):
        """Query qBittorrent preferences by key."""
        p = self._pref
        if p is None:
            p = self._pref = self._request("app/preferences").json()
        return p[key]

    def remove_torrents(self, removeList: Sequence[Removable]):
        """Remove torrents and delete files."""

        if not removeList:
            return
        if not dryrun:
            params = {
                "hashes": "|".join(t.hash for t in removeList),
                "deleteFiles": True
            }
            self._request("torrents/delete", params=params)
        for t in removeList:
            logger.record("Remove", t.title, t.size)
        self._set_silence(minutes=30)

    def add_torrent(self, downloadList: Sequence[Torrent], downloader):
        """Download torrents and upload to qBittorrent.

        `downloader` should have a `get` method which takes a path and returns a
        requests response object. Torrents info is recorded into history after
        succesful uploads.
        """

        if not downloadList:
            return
        downloader = downloader.get
        try:
            content = {t.id: downloader(t.link).content for t in downloadList}
        except AttributeError:
            return
        if not dryrun:
            try:
                self._request("torrents/add", method="POST", files=content)
            except requests.RequestException as e:
                logger.record("Error", f"Uploading torrent failed: {e}")
                return
            if not self._STALED.isdisjoint(
                    self.states) and self.server_state["queueing"]:
                # In case the queue being blocked by stalled torrents, set new
                # torrents to the top priority.
                self._request(
                    "torrents/topPrio",
                    ignore_error=True,
                    params={"hashes": "|".join(t.hash for t in downloadList)})

        from torrentool.api import Torrent as Torrentool

        # convert timedelta to timestamp
        # read hash and name from torrent file
        data = []
        index = []
        for t in downloadList:
            if isinstance(t.expire, timedelta):
                t.expire = NOW + t.expire
            else:
                t.expire = pd.NaT
            try:
                torrent = Torrentool.from_string(content[t.id])
                t.hash = torrent.info_hash
                t.title = torrent.name
                t.size = torrent.total_size
            except Exception as e:
                print(f"Torrentool error: {e}", file=sys.stderr)

            logger.record("Download", t.title, t.size)
            data.append((t.id, NOW, t.expire))
            index.append(t.hash or t.id)

        # cleanup outdated records (older than 30 days and not in app)
        df = self.history
        df = df[df.index.isin(self.torrent_data.columns) |
                (df["add"].values > NOW - timedelta(days=30))]

        # save new info to dataframe
        self.history = df.append(
            DataFrame(data, index=index, columns=("id", "add", "expire")))

        # set n hours of silence
        self._set_silence(hours=len(downloadList))

    def resume_paused(self):
        """If any torrent is paused, for any reason, resume."""
        if not self._PAUSED.isdisjoint(self.states):
            print("Resume torrents.")
            if not dryrun:
                self._request("torrents/resume",
                              ignore_error=True,
                              params={"hashes": "all"})

    @property
    def usable_space(self) -> int:
        """Return usable space on seed_dir.

        usable_space = free_space_on_disk - disk_quota - amount_left_to_download
        """
        s = self._usable_space
        if s is None:
            free = self.server_state["free_space_on_disk"]
            try:
                s = shutil.disk_usage(self.seed_dir).free
            except TypeError:
                pass
            else:
                if s > free:
                    free = s
            s = self._usable_space = int(free - self._space_offset)
        return s

    @property
    def speeds(self):
        """qBittorrent last hour ul/dl speeds.

        Returns: numpy.array([<ul>, <dl>])
        """
        df = self.server_data
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
        df = self.torrent_data
        hi = df.iloc[-1]
        lo = df.apply(pd.Series.first_valid_index)
        try:
            speeds = ((hi.values - df.lookup(lo, lo.index)) /
                      (hi.name - lo).dt.total_seconds())
            speeds.dropna(inplace=True)
        except AttributeError:
            return pd.Series(dtype=float)
        return speeds


class MTeam:
    """A cumbersome MTeam downloader."""

    def __init__(self, *, domain: str, username: str, password: str,
                 peer_slope: float, peer_intercept: float, pages: Sequence[str],
                 qb: qBittorrent):
        """Minimum peer requirement subjects to:

        Peer >= A * Size(GiB) + B

        Where A, B is defined by params `peer_slope` and `peer_intercept`.
        """

        self.domain = domain
        self.account = {"username": username, "password": password}
        self.A = peer_slope / BYTESIZE["GiB"]
        self.B = peer_intercept
        self.pages = pages
        self.qb = qb
        self.session = qb.session

    def get(self, path: str):

        url = urljoin(self.domain, path)
        print(f'Connecting: {self._shorten(url)} ..', end="", flush=True)
        try:
            response = self.session.get(url, timeout=(6.1, 30))
            response.raise_for_status()
        except (requests.ConnectionError, requests.HTTPError,
                requests.Timeout) as e:
            print(e, file=sys.stderr)
            return
        except Exception as e:
            self.session = self.qb.reset_session()
        else:
            if "/login.php" not in response.url:
                print("ok")
                return response

        print("logging in..", end="", flush=True)
        try:
            response = self.session.post(
                url=urljoin(self.domain, "takelogin.php"),
                data=self.account,
                headers={"referer": urljoin(self.domain, "login.php")})
            response.raise_for_status()
            response = self.session.get(url, timeout=(6.1, 30))
            response.raise_for_status()
        except Exception as e:
            print(e, file=sys.stderr)
            return
        if "/login.php" not in response.url:
            print("ok")
            return response

        print("invalid login", file=sys.stderr)
        self.get = lambda path: print(
            f"Skipped: {self._shorten(urljoin(self.domain, path))}")

    def scan(self) -> Iterator[Torrent]:

        from bs4 import BeautifulSoup

        cols = {}
        A = self.A
        B = self.B
        visited = set(self.qb.history["id"])

        sub_nondigit = re.compile(r"\D").sub
        search_size = re.compile(
            r"(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>[KMGTP]i?B)").search

        re_download = re.compile(r"\bdownload\.php\?")
        re_details = re.compile(r"\bdetails\.php\?")
        re_time = re.compile(
            r"^\W*限時：\W*(?:(\d+)\s*日)?\W*(?:(\d+)\s*時)?\W*(?:(\d+)\s*分)?")

        for page in self.pages:
            try:
                soup = (tr.find_all("td", recursive=False)
                        for tr in BeautifulSoup(
                            self.get(page).content, "html.parser").select(
                                "#form_torrent table.torrents > tr"))
                row = next(soup)
            except AttributeError:
                continue
            except StopIteration:
                print(f"CSS selector broken: {page}", file=sys.stderr)
                continue

            for i, td in enumerate(row):
                title = td.find(title=True)
                title = title["title"] if title else td.get_text()
                cols[title.strip()] = i

            c_title = cols.pop("標題", 1)
            c_size = cols.pop("大小", 4)
            c_up = cols.pop("種子數", 5)
            c_down = cols.pop("下載數", 6)
            c_prog = cols.pop("進度", 8)

            for row in soup:
                try:
                    peer = int(sub_nondigit("", row[c_down].text))
                    size = search_size(row[c_size].text)
                    size = int(float(size["num"]) * BYTESIZE[size["unit"]])
                    if (peer < A * size + B or
                            "peer-active" in row[c_prog]["class"] or
                            not int(sub_nondigit("", row[c_up].text))):
                        continue

                    link = row[c_title].find("a", href=re_download)["href"]
                    tid = re.search(r"\bid=([0-9]+)", link)[1]
                    if tid in visited:
                        continue

                    expire = row[c_title].find(string=re_time)
                    if expire:
                        expire = re_time.search(expire).groups(default=0)
                        expire = timedelta(days=int(expire[0]),
                                           hours=int(expire[1]),
                                           minutes=int(expire[2]))
                        if expire.days < 1:
                            continue

                    title = row[c_title].find("a", href=re_details, string=True)
                    title = title.get("title") or title.get_text(strip=True)

                except Exception as e:
                    print(f"Parsing error: {e}", file=sys.stderr)

                else:
                    visited.add(tid)
                    yield Torrent(
                        id=tid,
                        size=size,
                        peer=peer,
                        link=link,
                        expire=expire,
                        title=title,
                    )

    @staticmethod
    def _shorten(s: str, k: int = 60):
        return s if len(s) <= k else s[:k - 4] + "[...]"


class MPSolver:
    """Using Google OR-Tools to find the optimal combination of downloads and removals.

    Maximize obtained peers under constraints.

    ### Constraints:
    -   download_size - removed_size <= usable_space

        infeasible when usable_space + removed_size < 0

    -   download_count - removed_downloading <= max_download_slot

        never exceed qBittorrent max_active_downloads limit, if exists.

    ### Objective:
        Maximize: download_peer - removed_peer
    """

    def __init__(self, *, removeCand: Iterable[Removable],
                 downloadCand: Iterable[Torrent], qb: qBittorrent):

        self.downloadCand = downloadCand = tuple(downloadCand)
        self.removeCand = removeCand = tuple(removeCand)
        self.qb = qb

        from ortools.sat.python import cp_model

        ScalProd = cp_model.LinearExpr.ScalProd
        model = cp_model.CpModel()

        # download_size - removed_size <= usable_space
        coef = [t.size for t in downloadCand]
        coef.extend(-t.size for t in removeCand)
        pool = tuple(model.NewBoolVar(f"{i}") for i in range(len(coef)))
        model.Add(ScalProd(pool, coef) <= qb.usable_space)

        # downloads - removes(downloading) <= max download slot
        max_dl = qb.get_max_dl() if downloadCand else None
        if max_dl is not None:
            dl_state = qb._DOWNLOADING
            coef = [1] * len(downloadCand)
            coef.extend(-(t.state in dl_state) for t in removeCand)
            model.Add(ScalProd(pool, coef) <= max_dl)

        # Maximize: download_peer - removed_peer
        factor = sum(t.peer for t in removeCand if t.weight == 1) + 1
        coef = [t.peer * factor for t in downloadCand]
        coef.extend(-t.peer * (factor if t.weight is None else t.weight)
                    for t in removeCand)
        model.Maximize(ScalProd(pool, coef))

        solver = cp_model.CpSolver()
        status = solver.Solve(model)

        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            self.status = "Solution: {}, walltime: {:.5f}, objective value: {}".format(
                solver.StatusName(status), solver.WallTime(),
                solver.ObjectiveValue())
            value = map(solver.BooleanValue, pool)
            self.downloadList = tuple(t for t in downloadCand if next(value))
            self.removeList = tuple(t for t in removeCand if next(value))
        else:
            self.status = "CP-SAT solver cannot find a solution. Status: {}".format(
                solver.StatusName(status))
            self.downloadList = ()
            self.removeList = self.removeCand if qb.usable_space < 0 else ()

    def report(self):
        """Print report to stdout."""

        sepSlim = "-" * 50
        removeCandSize = self._sumsize(self.removeCand)
        downloadCandSize = self._sumsize(self.downloadCand)
        removeSize = self._sumsize(self.removeList)
        downloadSize = self._sumsize(self.downloadList)
        usable_space = self.qb.usable_space
        final_usable_space = usable_space + removeSize - downloadSize

        print(sepSlim)
        print("Usable space: {}, max avail: {}".format(
            humansize(usable_space),
            humansize(usable_space + removeCandSize),
        ))
        print("Download candidates: {}, size: {}".format(
            len(self.downloadCand),
            humansize(downloadCandSize),
        ))
        print("Remove candidates: {}/{}, size: {}".format(
            len(self.removeCand),
            len(self.qb.torrents),
            humansize(removeCandSize),
        ))
        for t in self.removeCand:
            print(f"[{humansize(t.size):>11}|{t.peer:4d}P] {t.title}")

        for prefix in "remove", "download":
            final = getattr(self, prefix + "List")
            cand = getattr(self, prefix + "Cand")
            size = locals()[prefix + "Size"]
            print("{}\n{}: {}/{}, size: {}, peer: {}".format(
                sepSlim,
                prefix.capitalize(),
                len(final),
                len(cand),
                humansize(size),
                sum(t.peer for t in final),
            ))
            for t in final:
                print(f"[{humansize(t.size):>11}|{t.peer:4d}P] {t.title}")

        print(sepSlim)
        print(self.status)
        print("Usable space change: {} => {}".format(
            humansize(usable_space), humansize(final_usable_space)))

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


def parse_args():
    """Parse command line arguments."""
    args = {"dryrun": False, "force": False}
    try:
        for arg in sys.argv[1:]:
            if arg.startswith("-"):
                for c in arg[1:]:
                    if c == "d":
                        args["dryrun"] = True
                    elif c == "f":
                        args["force"] = True
                    elif c == "h":
                        raise ValueError
                    else:
                        break
                else:
                    continue
            raise ValueError(f"\n\nerror: unrecognized argument -- '{arg}'")
    except ValueError as e:
        sys.exit(f"usage: {__file__} [-h] [-d] [-f]\n\n"
                 "The Ultimate qBittorrent Maintenance Tool\n"
                 "Author: David Pi\n\n"
                 "optional arguments:\n"
                 "  -h    show this help message and exit\n"
                 "  -d    dry run\n"
                 "  -f    force download"
                 f"{e}")
    else:
        return args


def parse_config(configfile: str) -> Dict[str, dict]:
    """Read or create config file."""
    try:
        with open(configfile, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        default = {
            "server": {
                "host": "http://localhost",
                "disk_quota": 50,
                "up_rate_thresh": 2700,
                "dl_rate_thresh": 6000,
                "dead_up_thresh": 2,
                "seed_dir": None,
                "log_backup_path": None,
            },
            "mteam": {
                "domain": "https://pt.m-team.cc",
                "username": "",
                "password": "",
                "peer_slope": 0.3,
                "peer_intercept": 30,
                "pages": [
                    "/adult.php?spstate=2&sort=8&type=desc",
                    "/torrents.php?spstate=2&sort=8&type=desc"
                ]
            },
        } # yapf: disable
        configfile = op.abspath(configfile)
        with open(configfile, "w", encoding="utf-8") as f:
            json.dump(default, f, indent=4)
        sys.exit(f'Please edit "{configfile}" before running me again.')
    except (OSError, ValueError) as e:
        sys.exit(f"Error in config file: {e}")


def main():

    global dryrun
    args = parse_args()
    dryrun = args["dryrun"]

    os.chdir(op.dirname(__file__))
    config = parse_config("config.json")

    qb = qBittorrent(**config["server"])

    if qb.is_ready or args["force"]:

        qb.clean_seeddir()

        if qb.requires_download or args["force"]:
            mteam = MTeam(**config["mteam"], qb=qb)
            downloadCand = tuple(mteam.scan())
        else:
            downloadCand = ()

        if downloadCand or qb.requires_remove():
            solver = MPSolver(
                removeCand=qb.get_remove_cands(),
                downloadCand=downloadCand,
                qb=qb,
            )
            qb.remove_torrents(solver.removeList)
            if downloadCand:
                qb.add_torrent(solver.downloadList, mteam)  # type: ignore
            solver.report()

    qb.resume_paused()
    qb.dump_data()
    logger.write("logfile.log", config["server"]["log_backup_path"])


dryrun = False
logger = Logger()

if __name__ == "__main__":
    main()
