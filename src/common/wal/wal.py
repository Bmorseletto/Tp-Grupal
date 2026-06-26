import json
import os
import time
import tempfile
import shutil
import logging
import threading

logger = logging.getLogger(__name__)
ORPHAN_TTL = 1800
ORPHAN_CLEANUP_INTERVAL_SECONDS = 1800


def _json_serial(obj):
    if isinstance(obj, set):
        return {"__set__": sorted(list(obj), key=str)}
    if isinstance(obj, frozenset):
        return {"__frozenset__": sorted(list(obj), key=str)}
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _json_hook(obj):
    if isinstance(obj, dict):
        if "__set__" in obj:
            return set(obj["__set__"])
        if "__frozenset__" in obj:
            return frozenset(obj["__frozenset__"])
        converted = {}
        for k, v in obj.items():
            try:
                converted[int(k)] = v
            except (ValueError, TypeError):
                converted[k] = v
        return converted
    return obj


def _dumps(obj):
    return json.dumps(obj, default=_json_serial)


def _loads(s):
    return json.loads(s, object_hook=_json_hook)


class WAL:
    r"""Write-Ahead Log for crash recovery in stateful services.

    Design
    ------
    1. **Append-only log** — every processed message is appended as a
       single line (``msg_id\\tpayload``).  Appending is O(1); no full-state
       serialization per message.

    2. **Transaction file per send** — ``tx_begin(msg_id)`` creates
       ``<msg_id>.tx`` on disk *before* the downstream send.
       ``tx_commit(msg_id)`` removes it *after* the ack.  On restart,
       any ``.tx`` file that survived means the crash happened between
       send and ack — the message is considered already processed
       (conservative, avoids duplicates).

    3. **Backup (checkpoint)** — ``backup_save(state, last_seq)`` atomically
       writes the full reconstructed state together with the log sequence
       number up to which the backup is valid.  This allows truncating old
       log entries via ``truncate(last_seq)``.

    4. **Recovery** — ``recover(apply_fn)`` loads the backup, then replays
       every log entry whose sequence is *after* the backup point by
       calling ``apply_fn(entry, state)`` for each one.  Orphan ``.tx``
       files are collected so the service can decide whether to skip
       or re-process those messages.

    File layout on disk (all under *base_dir*):
        base_dir/
            backup.json            — checkpoint: {state, last_seq}
            wal.log                — append-only log (one entry per line)
            tx/                    — one .tx file per in-flight send
                <msg_id>.tx

    Usage in a stateful service
    ---------------------------
        wal = WAL("/data/agg_q5_wal")

        # ---- startup / recovery ----
        state, _, processed_ids = wal.backup_load(default=({"count": {},"workers": {}}, 0, set()))
        wal.recover(apply_fn=lambda entry, st: my_apply(entry, st),
                    state=state)
        # wal.processed_ids is now populated from backup + replayed log

        # ---- on receive ----
        def on_message(msg, ack, nack, ctx):
            msg_id = ctx["msg_id"]
            if msg_id in wal.processed_ids:
                ack()
                return
            # update state, send downstream
            wal.append(msg_id, record)
            wal.tx_begin(msg_id)
            output.send(serialized)
            wal.tx_commit(msg_id)
            wal.processed_ids.add(msg_id)
            ack()

        # ---- periodic checkpoint ----
        wal.backup_save(state, wal.last_seq())
        wal.truncate(wal.last_seq())
    """

    def __init__(self, base_dir):
        self._dir = base_dir
        self._log_path = os.path.join(self._dir, "wal.log")
        self._backup_path = os.path.join(self._dir, "backup.json")
        self._tx_dir = os.path.join(self._dir, "tx")
        os.makedirs(self._tx_dir, exist_ok=True)
        self._lock = threading.Lock()
        self._last_seq = 0
        self._log_fd = None
        self.processed_ids = set()
        self._orphan_ttl_seconds = int(ORPHAN_TTL)
        self._cleanup_interval_seconds = int(os.environ.get("ORPHAN_TX_CLEANUP_INTERVAL_SECONDS", str(ORPHAN_CLEANUP_INTERVAL_SECONDS)))
        self._cleanup_stop = threading.Event()
        self._init_log()
        self.logs = []
        self._cleanup_thread = threading.Thread(target=self._orphan_cleanup_loop, daemon=True)
        self._cleanup_thread.start()

    def _init_log(self):
        if os.path.exists(self._log_path):
            with open(self._log_path, "rb") as f:
                for line in f:
                    line = line.rstrip(b"\n")
                    if line:
                        parts = line.split(b"\t", 1)
                        if parts and parts[0].strip():
                            try:
                                seq = int(parts[0])
                            except ValueError:
                                continue
                            if seq > self._last_seq:
                                self._last_seq = seq
        self._log_fd = open(self._log_path, "ab")

    def append(self, msg_id, record):
        """Append a log entry atomically.  *record* must be JSON-serializable.

        Each line in the log has the format:
            \<seq>\\t\<msg_id>\\t\<payload_json>

        Returns the sequence number assigned to this entry.
        """
        entry = _dumps(record)
        with self._lock:
            self._last_seq += 1
            seq = self._last_seq
            line = f"{seq}\t{msg_id}\t{entry}\n".encode('utf-8')
            self._log_fd.write(line)
            self._log_fd.flush()
            os.fsync(self._log_fd.fileno())
        return seq

    def last_seq(self):
        """Return the sequence number of the last appended entry."""
        return self._last_seq

    def read_entries(self, after_seq=0):
        """Yield ``(seq, msg_id, record)`` for every entry whose seq >
        *after_seq*.  Used during recovery to replay missed entries.
        """
        with open(self._log_path, "rb") as f:
            for line in f:
                line = line.rstrip(b"\n")
                if not line:
                    continue
                parts = line.split(b"\t", 2)
                if len(parts) != 3:
                    continue
                try:
                    seq = int(parts[0])
                except ValueError:
                    continue
                if seq <= after_seq:
                    continue
                msg_id = parts[1].decode('utf-8')
                try:
                    record = _loads(parts[2])
                except Exception:
                    logger.warning("skipping corrupt WAL entry seq=%d", seq)
                    continue
                yield seq, msg_id, record

    def truncate(self, up_to_seq):
        try:
            with open(self._log_path, "rb") as f:
                lines = f.readlines()
        except FileNotFoundError:
            return
        kept = []
        for l in lines:
            parts = l.split(b"\t", 1)
            if not parts or not parts[0].strip():
                continue
            try:
                seq = int(parts[0])
            except ValueError:
                continue
            if seq > up_to_seq:
                kept.append(l)
        if self._log_fd and not self._log_fd.closed:
            self._log_fd.flush()
            self._log_fd.close()
        with open(self._log_path, "wb") as f:
            f.writelines(kept)
        self._log_fd = open(self._log_path, "ab")
        logger.debug("truncated log up to seq %d (%d entries kept)",
                     up_to_seq, len(kept))

    def checkpoint(self, state, interval=500):
        if self._last_seq % interval == 0:
            self.backup_save(state, self._last_seq)
            self.truncate(self._last_seq)
            self.cleanup_expired_orphan_txs()

    def tx_begin(self, msg_id):
        """Create a transaction file for *msg_id* BEFORE sending downstream.

        If the service crashes after the send but before the ack, the
        ``.tx`` file will survive and ``orphan_tx_ids()`` will report it
        on the next startup.
        """
        path = os.path.join(self._tx_dir, f"{msg_id}.tx")
        fd, tmp = tempfile.mkstemp(dir=self._tx_dir, prefix=".tx_")
        try:
            os.write(fd, msg_id.encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, path)

    def tx_commit(self, msg_id):
        """Remove the transaction file for *msg_id* AFTER the ack succeeds.

        If the ``.tx`` file is already gone (e.g. previous commit), this
        is a no-op.
        """
        path = os.path.join(self._tx_dir, f"{msg_id}.tx")
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def _orphan_cleanup_loop(self):
        while not self._cleanup_stop.wait(self._cleanup_interval_seconds):
            try:
                self.cleanup_expired_orphan_txs()
            except Exception:
                logger.exception("periodic orphan tx cleanup failed")

    def cleanup_expired_orphan_txs(self, ttl_seconds=None):
        """Remove orphan ``.tx`` files older than the configured TTL.

        Returns the set of cleaned msg_ids.
        """
        if ttl_seconds is None:
            ttl_seconds = self._orphan_ttl_seconds
        if ttl_seconds < 0:
            return set()

        if not os.path.isdir(self._tx_dir):
            return set()

        now = time.time()
        cleaned = set()
        for name in os.listdir(self._tx_dir):
            if not name.endswith(".tx") or name.startswith("."):
                continue
            path = os.path.join(self._tx_dir, name)
            try:
                if now - os.path.getmtime(path) <= ttl_seconds:
                    continue
            except OSError:
                continue
            msg_id = name[:-3]
            try:
                os.remove(path)
                cleaned.add(msg_id)
            except FileNotFoundError:
                pass

        if cleaned:
            logger.info("cleanup: removed %d expired orphan tx(s): %s",
                        len(cleaned), cleaned)
        return cleaned

    def orphan_tx_ids(self):
        """Return the set of msg_ids with orphan ``.tx`` files.

        These represent sends that may have succeeded on a previous run
        (crash happened between send and ack).  The service should treat
        these messages as already-processed to avoid duplicates.
        """
        result = set()
        if not os.path.isdir(self._tx_dir):
            return result
        for name in os.listdir(self._tx_dir):
            if name.endswith(".tx") and not name.startswith("."):
                msg_id = name[:-3]
                result.add(msg_id)
        return result

    def backup_save(self, state, last_seq, processed_ids=None):
        """Atomically write a checkpoint: the full reconstructed *state*,
        the log *last_seq* up to which the backup is valid, and the
        set of *processed_ids* seen so far (for dedup on redelivery).
        """
        if processed_ids is None:
            processed_ids = self.processed_ids
        payload = _dumps({"state": state, "last_seq": last_seq, "processed_ids": processed_ids}).encode("utf-8")
        fd, tmp = tempfile.mkstemp(dir=self._dir, prefix=".bak_")
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self._backup_path)
        logger.debug("backup saved at seq %d (%d bytes)", last_seq, len(payload))

    def backup_load(self, default=None):
        """Load the latest checkpoint.

        Returns ``(state, last_seq, processed_ids)``.  If no backup exists,
        returns *default* (typically ``(initial_state, 0, set())``).
        Legacy backups without ``processed_ids`` return an empty set.
        """
        try:
            with open(self._backup_path, "rb") as f:
                data = _loads(f.read().decode("utf-8"))
            return data["state"], data["last_seq"], data.get("processed_ids", set())
        except FileNotFoundError:
            return default
        except (json.JSONDecodeError, KeyError, OSError) as e:
            logger.warning("corrupt backup, starting fresh: %s", e)
            return default

    def recover(self, apply_fn, state):
        """Recover state after a crash.

        1. Load backup's ``processed_ids`` into ``self.processed_ids``.
        2. Orphan ``.tx`` files are discovered (their msg_ids are
           available via ``orphan_tx_ids()``).  The service uses this to
           skip already-processed messages.
        3. Log entries after the backup point are replayed into *state*
           by calling ``apply_fn(entry, state)`` for each one.  Each
           entry's ``msg_id`` (if not ``None``) is added to
           ``self.processed_ids`` for redelivery dedup.

        Returns the set of orphan msg_ids found.
        """
        self.cleanup_expired_orphan_txs()
        orphans = self.orphan_tx_ids()
        backup_result = self.backup_load(default=(None, 0, set()))
        _, backup_seq, backup_processed = backup_result
        if backup_seq is None:
            backup_seq = 0
        if backup_processed:
            self.processed_ids = backup_processed

        replayed = 0
        for _seq, _msg_id, record in self.read_entries(after_seq=backup_seq):
            try:
                apply_fn(record, state)
                replayed += 1
                if _msg_id and _msg_id != "None":
                    self.processed_ids.add(_msg_id)
            except Exception:
                logger.warning("failed to replay log entry seq=%d msg_id=%s",
                               _seq, _msg_id, exc_info=True)

        if replayed:
            logger.info("recovery: replayed %d log entries from seq %d",
                        replayed, backup_seq)
        if orphans:
            logger.info("recovery: %d orphan tx(s) found: %s",
                        len(orphans), orphans)
        return orphans

    def close(self):
        """Stop background cleanup and close the log file descriptor."""
        try:
            self._cleanup_stop.set()
            if hasattr(self, "_cleanup_thread"):
                self._cleanup_thread.join(timeout=2)
        except Exception:
            pass

        if self._log_fd and not self._log_fd.closed:
            try:
                self._log_fd.flush()
                self._log_fd.close()
            except Exception:
                pass

    def clear(self):
        """Remove all WAL files (backup, log, tx dir).  Use only for
        clean-shutdown or testing.
        """
        self.close()
        for path in (self._backup_path, self._log_path):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        if os.path.isdir(self._tx_dir):
            shutil.rmtree(self._tx_dir)
            os.makedirs(self._tx_dir, exist_ok=True)
        self._last_seq = 0
        self.processed_ids = set()
        self._log_fd = open(self._log_path, "ab")
