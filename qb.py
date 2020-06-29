import os
import pickle
import re
import shutil
from collections import deque
from datetime import datetime
from itertools import combinations
from sys import argv

import requests
from bs4 import BeautifulSoup

import qbconfig


class qBittorrent:
    def __init__(self, api_baseurl, seed_dir, watch_dir):
        self.api_baseurl = api_baseurl
        self.seed_dir = seed_dir
        self.watch_dir = watch_dir
        self.space_threshold = 50 * sizes['GB']
        self.removeList = set()
        self.removeCand = set()
        self.newTorrentPath = []
        self.removeListSize = 0
        self.removeCandSize = 0
        self.newTorrentSize = 0
        self.dlspeed = None
        self.upspeed = None

        para = '/sync/maindata'
        self.maindata = self.request(para).json()
        assert self.maindata['server_state']['connection_status'] != 'disconnected'
        self.torrents = self.maindata['torrents']
        self.state = self.maindata['server_state']
        self.errors = {'error', 'missingFiles',
                       'pausedUP', 'pausedDL', 'unknown'}

    def request(self, para):
        response = requests.get(self.api_baseurl + para)
        response.raise_for_status()
        return response

    def clean_seed_dir(self):
        names = set(i['name'] for i in self.torrents.values())
        names.add(self.watch_dir)
        qb_ext = re.compile('\.!qB$')
        with os.scandir(self.seed_dir) as it:
            for entry in it:
                if not qb_ext.sub('', entry.name) in names:
                    print('Cleanup:', entry.name)
                    try:
                        if not debug:
                            if entry.is_dir():
                                shutil.rmtree(entry.path)
                            else:
                                os.remove(entry.path)
                        logs.append('Cleanup', None, entry.name)
                    except:
                        print('Deletion Failed.')

    def update_freeSpace(self):
        self.free_space = shutil.disk_usage(self.seed_dir)[2] - sum(i['amount_left'] for i in self.torrents.values()) - self.space_threshold
        self.update_availSpace()

    def update_availSpace(self):
        self.availSpace = self.free_space + self.removeListSize + self.removeCandSize - self.newTorrentSize

    def action_needed(self):
        if self.free_space < 0:
            return True
        elif self.dlspeed is None:
            return False
        else:
            dl_threshold = 8 * sizes['MB']
            up_threshold = 2.6 * sizes['MB']
            return self.dlspeed < dl_threshold and self.upspeed < up_threshold

    def set_removeList(self, key, size, name):
        self.removeList.add(key)
        self.removeListSize += size
        self.update_availSpace()
        print('Add to remove list: {}, size: {}'.format(name, humansize(size)))

    def set_removeCand(self, key, size, name):
        self.removeCand.add(key)
        self.removeCandSize += size
        self.update_availSpace()
        print('Add to remove candidates: {}, size: {}'.format(name, humansize(size)))

    def add_torrent(self, path, size):
        self.newTorrentPath.append(path)
        self.newTorrentSize += size
        self.update_availSpace()

    def calcMinRemoves(self):
        targetSize = self.newTorrentSize - self.free_space - self.removeListSize
        if targetSize > 0 and self.removeCand:
            print('Total size: {}, space to free: {}'.format(
                humansize(self.newTorrentSize), humansize(targetSize)
            ))

            keys = removeSize = None
            for i in range(1, len(self.removeCand)+1):
                for j in combinations(self.removeCand, i):
                    s = sum(self.torrents[k]['size'] for k in j)
                    if s >= targetSize and (not removeSize or removeSize > s):
                        keys, removeSize = j, s

            print('Result torrents: {}, size: {}'.format(
                len(keys), humansize(removeSize)
            ))
            self.removeCand.difference_update(keys)
            self.removeList.update(keys)
            self.removeCandSize -= removeSize
            self.removeListSize += removeSize

    def remove_inactive(self):
        if self.removeList:
            if not debug:
                para = '/torrents/delete?hashes=' + '|'.join(self.removeList) + '&deleteFiles=true'
                self.request(para)
            for i in self.removeList:
                logs.append('Remove', self.torrents[i]['size'], self.torrents[i]['name'])

    def copyToWatchDir(self):
        if self.newTorrentPath and not debug:
            watch_dir = os.path.join(self.seed_dir, self.watch_dir)
            for path in self.newTorrentPath:
                shutil.copy(path, watch_dir)

    def resume_paused(self):
        if list(i for i in self.torrents.values() if i['state'] in self.errors):
            print('Resume torrents.')
            para = '/torrents/resume?hashes=all'
            self.request(para)


class Data:
    '''Data(datafile)'''

    def __init__(self, datafile):
        self.file = os.path.join(script_dir, datafile)
        try:
            with open(self.file, mode='rb') as f:
                self.data = pickle.load(f)
            assert 'session' in self.data
            assert isinstance(self.data['torrents'], dict)
        except:
            print('Initializing Data.')
            self.data = {
                'session': None,
                'last_record': None,
                'alltime_dl': None,
                'alltime_ul': None,
                'up_info_data': None,
                'torrents': dict()
            }

    def record(self, qb):
        try:
            assert self.data.get('last_record') is not None
            span = now - self.data['last_record']
            assert qb.state['up_info_data'] >= self.data['up_info_data'] and 60 <= span <= 3600
            qb.dlspeed = (qb.state['alltime_dl'] - self.data['alltime_dl']) // span
            qb.upspeed = (qb.state['alltime_ul'] - self.data['alltime_ul']) // span
            print(
                'Average uploading speed: {}k/s, time span: {}s.'.format(qb.upspeed // 1024, span)
            )
        except:
            span = None
            print('Skip recording.')

        hashs_old = set(self.data['torrents'].keys())
        hashs_new = set(qb.torrents.keys())
        for key in hashs_old.difference(hashs_new):
            del self.data['torrents'][key]

        span_limit = now - 86400
        for key, torrent in qb.torrents.items():
            try:
                assert key in self.data['torrents']
                while len(self.data['torrents'][key]['speed']) > 0 and self.data['torrents'][key]['speed'][0][0] < span_limit:
                    self.data['torrents'][key]['speed'].popleft()
                if span and torrent['state'] not in qb.errors:
                    self.data['torrents'][key]['speed'].append(
                        (now, (torrent['uploaded'] - self.data['torrents'][key]['uploaded']) // span)
                    )
            except:
                self.data['torrents'][key] = {
                    'speed': deque(), 'uploaded': None}

            self.data['torrents'][key]['uploaded'] = torrent['uploaded']

        self.data['last_record'] = now
        self.data['alltime_dl'] = qb.state['alltime_dl']
        self.data['alltime_ul'] = qb.state['alltime_ul']
        self.data['up_info_data'] = qb.state['up_info_data']

    def get_session(self):
        return self.data['session']

    def save_session(self, session):
        self.data['session'] = session

    def dump(self):
        with open(self.file, 'wb') as f:
            pickle.dump(self.data, f)


class Log:
    def __init__(self, logfile, logbackup):
        self.logfile = logfile
        self.logbackup = logbackup
        self.logs = []

    def append(self, action, size, name):
        self.logs.append('{:20}{:12}{:14}{}\n'.format(
            datetime.now().strftime('%D %T'), action,
            humansize(size) if size else '---',
            name))

    def write(self):
        if len(self.logs) > 0:
            if debug:
                print('Logs:')
                for log in reversed(self.logs):
                    print(log, end='')
            else:
                try:
                    with open(self.logfile, mode='r', encoding='utf-8') as f:
                        backup = ''.join(f.readlines()[2:])
                except:
                    backup = None

                with open(self.logfile, mode='w', encoding='utf-8') as f:
                    f.write('{:20}{:12}{:14}{}\n{}\n'.format(
                        'Date', 'Action', 'Size', 'Name',
                        '-------------------------------------------------------------------------------')
                    )
                    for log in reversed(self.logs):
                        f.write(log)
                    if backup:
                        f.write(backup)
                shutil.copy(self.logfile, self.logbackup)


def humansize(size, suffixes=('KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB')):
    for suffix in suffixes:
        size /= 1024
        if size < 1024:
            return '{:.2f} {}'.format(size, suffix)


def build_remove_list():

    def is_inactive():
        if 0 < torrent['last_activity'] < time_threshold:
            return True
        elif torrent['num_incomplete'] <= 3 and torrent['dlspeed'] == torrent['upspeed'] == 0:
            return True
        else:
            record = data.data['torrents'][key]['speed']
            if len(record) > 0 and record[-1][0] - record[0][0] >= span_limit:
                return sum(i[1] for i in record) / len(record) < speed_threshold
        return False

    time_threshold = now - 86400
    speed_threshold = 26 * 1024
    span_limit = 3600 * 18

    for key, torrent in sorted(qb.torrents.items(), key=lambda x: (x[1]['num_incomplete'], x[1]['last_activity'])):
        if not time_threshold <= torrent['added_on'] <= now:
            if qb.availSpace < 0:
                qb.set_removeList(key, torrent['size'], torrent['name'])
            elif is_inactive():
                qb.set_removeCand(key, torrent['size'], torrent['name'])


def download_torrent(feed_url, username, password, qb):

    def to_int(string):
        return int(re.sub('[^0-9]+', '', string))

    def size_convert(string):
        num = float(re.search('[0-9.]+', string).group())
        unit = re.search('[TGM]B', string).group()
        return int(num * sizes[unit]) if num and unit else None

    if len(qb.newTorrentPath) >= 2:
        return

    domain = 'https://pt.m-team.cc/'
    login_index = domain + 'login.php'
    login_page = domain + 'takelogin.php'
    feed_url = domain + feed_url
    payload = {'username': username, 'password': password}

    torrent_dir = os.path.join(script_dir, 'torrent')
    if not os.path.exists(torrent_dir):
        os.mkdir(torrent_dir)

    print('Connecting to M-Team... Max avail:', humansize(qb.availSpace))

    session = data.get_session()

    for i in range(5):
        try:
            page = session.get(feed_url)
            assert page is not None
            page = BeautifulSoup(page.content, 'html.parser')
            assert '登錄' not in page.title.string
            print('Session valid.')
            break
        except:
            if i == 4:
                print('Login Failed.')
                return
            print('Login... Attempt:', i+1)
            session = requests.session()
            session.post(
                login_page,
                data=payload,
                headers=dict(referer=login_index)
            )

    re_download = re.compile('^download.php\?')
    re_details = re.compile('^details.php\?')
    re_tid = re.compile('id=([0-9]+)')
    re_xs = re.compile('限時：')

    for torrent in page.find('table', class_='torrents').find_all('tr', recursive=False):
        try:
            row = torrent.find_all('td', recursive=False)

            size = size_convert(row[4].text)
            assert 0 < size <= qb.availSpace

            uploader = to_int(row[5].text)
            downloader = to_int(row[6].text)
            assert uploader > 0 and downloader >= 10

            xs = row[1].find(string=re_xs)
            if xs is not None:
                assert '日' in xs

            link = row[1].find('a', href=re_download)['href']
            tid = re_tid.search(link).group(1)
            path = os.path.join(torrent_dir, tid + '.torrent')
            assert link and tid and not os.path.exists(path)
        except:
            continue

        name = row[1].find('a', href=re_details, title=True)['title']

        if not debug:
            file = session.get(
                domain + link, allow_redirects=True)
            if file.ok:
                with open(path, 'wb') as f:
                    f.write(file.content)
            else:
                continue

        qb.add_torrent(path, size)
        logs.append('Download', size, name)
        print('New torrent: {}, size: {}, space remains: {}'.format(
            name, humansize(size), humansize(qb.availSpace)
        ))

        if len(qb.newTorrentPath) == 2:
            break

    data.save_session(session)


if __name__ == '__main__':
    debug = True if len(argv) > 1 and argv[1] == '-d' else False

    script_dir = os.path.dirname(__file__)
    now = int(datetime.now().timestamp())
    sizes = {'TB': 1024 ** 4, 'GB': 1024 ** 3, 'MB': 1024 ** 2}

    qb = qBittorrent(qbconfig.api_baseurl, qbconfig.seed_dir, qbconfig.watch_dir)

    data = Data('data')
    data.record(qb)

    logfile = os.path.join(script_dir, 'qBittorrent.log')
    logs = Log(logfile, qbconfig.logbackup)

    qb.clean_seed_dir()
    qb.update_freeSpace()

    if qb.action_needed() or debug:
        build_remove_list()
        if qb.availSpace > 0:
            download_torrent(qbconfig.feed_url, qbconfig.username, qbconfig.password, qb)
        qb.calcMinRemoves()
        qb.remove_inactive()
        qb.copyToWatchDir()
    else:
        print('System is healthy, no action needed.')

    qb.resume_paused()
    data.dump()
    logs.write()
