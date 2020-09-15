#!/usr/bin/env python3

import random
import unittest

import qbmaintain

Removable = qbmaintain.qBittorrent.Removable
Torrent = qbmaintain.MTeam.Torrent


class DuckQB:
    def __init__(self, freeSpace) -> None:
        self.freeSpace = freeSpace


class TestMIP(unittest.TestCase):

    sizeSum = lambda self, x: sum(i.size for i in x)
    peerSum = lambda self, x: sum(i.peer for i in x)

    @staticmethod
    def generate_cands(a, b, c, d, e, f, g, h):
        while True:
            removeCand = tuple(
                Removable(f"{peer}", size, peer, f"{peer}")
                for size, peer in zip(random.choices(range(a, b), k=5), random.choices(range(c, d), k=5))
            )
            downloadCand = tuple(
                Torrent(f"{peer}", size, peer, f"{peer}", f"{peer}")
                for size, peer in zip(random.choices(range(e, f), k=5), random.choices(range(g, h), k=5))
            )
            if len(set(i[0] for j in (removeCand, downloadCand) for i in j)) == 10:
                return removeCand, downloadCand

    def test_impossible(self):
        removeCand, downloadCand = self.generate_cands(1, 1000, 1, 100, 1, 1000, 1, 100)
        freeSpace = -self.sizeSum(removeCand) - 1
        mipsolver = qbmaintain.MIPSolver(
            removeCand=removeCand, downloadCand=downloadCand, maxDownload=None, qb=DuckQB(freeSpace)
        )
        mipsolver.solve()
        self.assertEqual(mipsolver.optimal, False)
        self.assertEqual(mipsolver.removeList, removeCand)
        self.assertEqual(len(mipsolver.downloadList), 0)

    def test_peer_greater(self):
        removeCand, downloadCand = self.generate_cands(1, 100, 1, 100, 1, 100, 600, 1000)
        freeSpace = 100
        mipsolver = qbmaintain.MIPSolver(
            removeCand=removeCand, downloadCand=downloadCand, maxDownload=None, qb=DuckQB(freeSpace)
        )
        mipsolver.solve()
        self.assertEqual(mipsolver.optimal, True)
        self.assertGreaterEqual(self.sizeSum(mipsolver.removeList) + freeSpace, self.sizeSum(mipsolver.downloadList))
        self.assertGreater(self.peerSum(mipsolver.downloadList), self.peerSum(mipsolver.removeList))

    def test_peer_lesser(self):
        removeCand, downloadCand = self.generate_cands(600, 1000, 600, 1000, 1, 100, 1, 100)
        freeSpace = 0
        mipsolver = qbmaintain.MIPSolver(
            removeCand=removeCand, downloadCand=downloadCand, maxDownload=None, qb=DuckQB(freeSpace)
        )
        mipsolver.solve()
        self.assertEqual(mipsolver.optimal, True)
        self.assertEqual(len(mipsolver.removeList), 0)
        self.assertEqual(len(mipsolver.downloadList), 0)

    def test_size_greater(self):
        removeCand, downloadCand = self.generate_cands(1, 100, 1, 100, 600, 1000, 600, 1000)
        freeSpace = 0
        mipsolver = qbmaintain.MIPSolver(
            removeCand=removeCand, downloadCand=downloadCand, maxDownload=None, qb=DuckQB(freeSpace)
        )
        mipsolver.solve()
        self.assertEqual(mipsolver.optimal, True)
        self.assertEqual(len(mipsolver.removeList), 0)
        self.assertEqual(len(mipsolver.downloadList), 0)

    def test_size_lesser(self):
        removeCand, downloadCand = self.generate_cands(600, 1000, 1, 100, 1, 100, 1, 100)
        freeSpace = 0
        mipsolver = qbmaintain.MIPSolver(
            removeCand=removeCand, downloadCand=downloadCand, maxDownload=None, qb=DuckQB(freeSpace)
        )
        mipsolver.solve()
        self.assertEqual(mipsolver.optimal, True)
        self.assertGreaterEqual(self.sizeSum(mipsolver.removeList) + freeSpace, self.sizeSum(mipsolver.downloadList))
        self.assertGreater(self.peerSum(mipsolver.downloadList), self.peerSum(mipsolver.removeList))

    def test_freespace_enough(self):
        removeCand, downloadCand = self.generate_cands(0, 100, 600, 1000, 600, 1000, 1, 100)
        freeSpace = 1000
        mipsolver = qbmaintain.MIPSolver(
            removeCand=removeCand, downloadCand=downloadCand, maxDownload=None, qb=DuckQB(freeSpace)
        )
        mipsolver.solve()
        self.assertEqual(mipsolver.optimal, True)
        self.assertEqual(len(mipsolver.removeList), 0)
        self.assertGreater(len(mipsolver.downloadList), 0)

    def test_max_item(self):
        removeCand, downloadCand = self.generate_cands(600, 1000, 1, 100, 1, 100, 600, 1000)
        freeSpace = 1
        mipsolver = qbmaintain.MIPSolver(
            removeCand=removeCand, downloadCand=downloadCand, maxDownload=3, qb=DuckQB(freeSpace)
        )
        mipsolver.solve()
        self.assertEqual(mipsolver.optimal, True)
        self.assertGreaterEqual(self.sizeSum(mipsolver.removeList) + freeSpace, self.sizeSum(mipsolver.downloadList))
        self.assertGreater(self.peerSum(mipsolver.downloadList), self.peerSum(mipsolver.removeList))
        self.assertEqual(len(mipsolver.downloadList), 3)


if __name__ == "__main__":
    unittest.main()
