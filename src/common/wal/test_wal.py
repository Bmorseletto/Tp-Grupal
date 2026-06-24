import os
import tempfile
import shutil
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from common.wal import WAL


def test_fresh_wal():
    tmpdir = tempfile.mkdtemp(prefix="wal_test_")
    try:
        wal = WAL(tmpdir)
        assert wal.last_seq() == 0
        assert wal.orphan_tx_ids() == set()
        assert wal.processed_ids == set()
        wal.close()
    finally:
        shutil.rmtree(tmpdir)


def test_append_and_read():
    tmpdir = tempfile.mkdtemp(prefix="wal_test_")
    try:
        wal = WAL(tmpdir)
        s1 = wal.append("src_0_0", {"type": "data", "client_id": 1, "amount": 100})
        s2 = wal.append("src_0_1", {"type": "data", "client_id": 1, "amount": 200})
        assert s1 == 1 and s2 == 2
        assert wal.last_seq() == 2

        entries = list(wal.read_entries(after_seq=0))
        assert len(entries) == 2
        assert entries[0][2]["client_id"] == 1
        assert entries[1][2]["client_id"] == 1

        entries2 = list(wal.read_entries(after_seq=1))
        assert len(entries2) == 1
        assert entries2[0][0] == 2
        wal.close()
    finally:
        shutil.rmtree(tmpdir)


def test_tx_begin_commit():
    tmpdir = tempfile.mkdtemp(prefix="wal_test_")
    try:
        wal = WAL(tmpdir)
        wal.tx_begin("src_0_2")
        assert wal.orphan_tx_ids() == {"src_0_2"}
        wal.tx_commit("src_0_2")
        assert wal.orphan_tx_ids() == set()
        wal.close()
    finally:
        shutil.rmtree(tmpdir)


def test_orphan_tx_survives_crash():
    tmpdir = tempfile.mkdtemp(prefix="wal_test_")
    try:
        wal = WAL(tmpdir)
        wal.tx_begin("src_0_3")
        wal.close()

        wal2 = WAL(tmpdir)
        assert wal2.orphan_tx_ids() == {"src_0_3"}
        wal2.close()
    finally:
        shutil.rmtree(tmpdir)


def test_backup_save_load_3tuple():
    tmpdir = tempfile.mkdtemp(prefix="wal_test_")
    try:
        wal = WAL(tmpdir)
        state = {"count": {1: 300}, "workers": {1: {"w1", "w2"}}, "__msg_counters": {"src": 5}}
        pids = {"src_0_0", "src_0_1"}
        wal.processed_ids = pids
        wal.backup_save(state, 2)

        loaded_state, loaded_seq, loaded_pids = wal.backup_load(default=({}, 0, set()))
        assert loaded_state == state, f"expected {state}, got {loaded_state}"
        assert loaded_seq == 2
        assert loaded_pids == pids
        assert isinstance(loaded_state["workers"][1], set)
        assert isinstance(loaded_pids, set)
        wal.close()
    finally:
        shutil.rmtree(tmpdir)


def test_backup_load_default_3tuple():
    tmpdir = tempfile.mkdtemp(prefix="wal_test_")
    try:
        wal = WAL(tmpdir)
        default = ({"x": 1}, 0, set())
        state, seq, pids = wal.backup_load(default=default)
        assert state == {"x": 1}
        assert seq == 0
        assert pids == set()
        wal.close()
    finally:
        shutil.rmtree(tmpdir)


def test_recover_with_apply_fn():
    tmpdir = tempfile.mkdtemp(prefix="wal_test_")
    try:
        wal = WAL(tmpdir)
        wal.append("src_0_0", {"type": "data", "client_id": 1, "amount": 100})
        wal.append("src_0_1", {"type": "data", "client_id": 1, "amount": 200})
        wal.tx_begin("src_0_3")

        state = {"count": {1: 300}, "workers": {1: {"w1", "w2"}}}
        wal.backup_save(state, 2)

        wal.append("src_0_10", {"type": "data", "client_id": 2, "amount": 50})
        wal.append("src_0_11", {"type": "data", "client_id": 2, "amount": 75})
        wal.close()

        wal2 = WAL(tmpdir)

        def apply_fn(record, st):
            cid = record["client_id"]
            st["count"][cid] = st["count"].get(cid, 0) + record["amount"]

        backup_state, _, _ = wal2.backup_load(default=({}, 0, set()))
        orphans = wal2.recover(apply_fn, backup_state)
        assert backup_state["count"][1] == 300
        assert backup_state["count"][2] == 125
        assert "src_0_3" in orphans
        wal2.close()
    finally:
        shutil.rmtree(tmpdir)


def test_recover_populates_processed_ids():
    tmpdir = tempfile.mkdtemp(prefix="wal_test_")
    try:
        wal = WAL(tmpdir)
        wal.append("src_0_0", {"type": "data", "client_id": 1, "amount": 100})
        wal.append("src_0_1", {"type": "data", "client_id": 1, "amount": 200})
        state = {"count": {}}
        wal.backup_save(state, 2, processed_ids={"src_0_0", "src_0_1"})
        wal.append("src_0_10", {"type": "data", "client_id": 2, "amount": 50})
        wal.close()

        wal2 = WAL(tmpdir)
        backup_state, _, _ = wal2.backup_load(default=({}, 0, set()))
        assert wal2.processed_ids == set()
        wal2.recover(lambda record, st: None, backup_state)
        assert "src_0_0" in wal2.processed_ids
        assert "src_0_1" in wal2.processed_ids
        assert "src_0_10" in wal2.processed_ids
        wal2.close()
    finally:
        shutil.rmtree(tmpdir)


def test_truncate():
    tmpdir = tempfile.mkdtemp(prefix="wal_test_")
    try:
        wal = WAL(tmpdir)
        wal.append("src_0_0", {"client_id": 1})
        wal.append("src_0_1", {"client_id": 1})
        wal.append("src_0_2", {"client_id": 2})

        wal.truncate(2)
        entries = list(wal.read_entries(after_seq=0))
        assert len(entries) == 1
        assert entries[0][0] == 3
        wal.close()
    finally:
        shutil.rmtree(tmpdir)


def test_clear():
    tmpdir = tempfile.mkdtemp(prefix="wal_test_")
    try:
        wal = WAL(tmpdir)
        wal.append("src_0_0", {"client_id": 1})
        wal.tx_begin("src_0_1")
        wal.backup_save({"x": 1}, 1)
        wal.processed_ids.add("src_0_0")

        wal.clear()
        assert wal.last_seq() == 0
        assert wal.orphan_tx_ids() == set()
        assert wal.processed_ids == set()
        assert wal.backup_load(default=None) is None
        wal.close()
    finally:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    test_fresh_wal()
    test_append_and_read()
    test_tx_begin_commit()
    test_orphan_tx_survives_crash()
    test_backup_save_load_3tuple()
    test_backup_load_default_3tuple()
    test_recover_with_apply_fn()
    test_recover_populates_processed_ids()
    test_truncate()
    test_clear()
    print("ALL TESTS PASSED")
