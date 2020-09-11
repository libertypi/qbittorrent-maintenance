#!/usr/bin/env python3

import unittest

import qbmaintain

Removable = qbmaintain.qBittorrent.Removable
Torrent = qbmaintain.MTeam.Torrent


class DuckQB:
    def __init__(self, freeSpace) -> None:
        self.freeSpace = freeSpace


class TestMIP(unittest.TestCase):
    def test_short(self):
        values = (
            (
                ("a", 100, 20, "a"),
                (),
                -150,
                ((Removable(hash="a", size=100, peer=20, title="a"),), ()),
            ),
            (
                ("a", 100, 10, "a"),
                ("x", 110, 20, "x", "x"),
                20,
                (
                    (Removable(hash="a", size=100, peer=10, title="a"),),
                    (Torrent(tid="x", size=110, peer=20, title="x", link="x"),),
                ),
            ),
            (
                ("a", 100, 20, "a"),
                ("x", 110, 10, "x", "x"),
                20,
                ((), ()),
            ),
            (
                ("a", 100, 20, "a"),
                ("x", 100, 10, "x", "x"),
                -10,
                ((Removable(hash="a", size=100, peer=20, title="a"),), ()),
            ),
            (
                ("a", 100, 20, "a"),
                (),
                100,
                ((), ()),
            ),
            (
                (),
                ("x", 100, 10, "x", "x"),
                90,
                ((), ()),
            ),
            (
                (),
                ("x", 100, 10, "x", "x"),
                120,
                ((), (Torrent(tid="x", size=100, peer=10, title="x", link="x"),)),
            ),
        )

        i = 1
        for removable, torrent, freeSpace, answer in values:
            removable = (Removable(*removable),) if removable else ()
            torrent = (Torrent(*torrent),) if torrent else ()
            duckqb = DuckQB(freeSpace)

            mipsolver = qbmaintain.MIPSolver(removable, torrent, duckqb, None)
            result = mipsolver.solve()
            print("Testing", i)
            # print(result)
            if mipsolver.status != 0:
                print(mipsolver.status)
            self.assertEqual(result, answer)
            i += 1


if __name__ == "__main__":
    unittest.main()
