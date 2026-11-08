"""Post-recording uploader for tgstream.

Pushes each finished library recording to a configurable Telegram chat
(a channel, a DM, or Saved Messages), splitting recordings that exceed the
upload size limit into independently playable MP4 segments re-muxed with
``-c copy`` (no re-encode, original quality).

Design notes - ideas informed by media_downloader / livestream-recorder,
implemented from scratch for Telethon:
  * Telethon splits the byte stream into 512 KiB parts itself; the only
    size limit we must respect is the account-level upload cap (2000 MiB
    without Premium). Anything larger must therefore be pre-split into
    smaller *files*, not raw byte chunks, so the receiver gets playable
    segments instead of binary shards.
  * Every completed file leaves a ``.uploaded.json`` sidecar marker; a
    restart or salvage re-run will never re-send a recording.
  * FloodWait is honoured exactly; other RPC/connection errors retry with
    exponential backoff. Local files are only deleted once every segment
    is confirmed sent.
"""

import asyncio
import datetime
import glob
import json
import os
import re
import shutil
import subprocess
import time

from telethon import errors as tg_errors
from telethon.tl.types import PeerChannel, PeerUser

# Non-premium MTProto upload cap. Keep the chunk target below it.
MAX_UPLOAD_MIB = 2000


def log(msg):
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} [UPLOAD] {msg}", flush=True)


class UploadSettings:
    """Parse UPLOAD_* environment variables once at attach time."""

    def __init__(self):
        self.enabled = (
            os.environ.get("UPLOAD_ENABLED", "true").lower() == "true"
            and bool(os.environ.get("UPLOAD_CHAT", "").strip())
        )
        self.chat = os.environ.get("UPLOAD_CHAT", "").strip()
        try:
            chunk = int(os.environ.get("UPLOAD_CHUNK_MIB", "1900"))
        except ValueError:
            chunk = 1900
        # Clamp: below the account cap with headroom for container overhead.
        self.chunk_mib = max(100, min(chunk, MAX_UPLOAD_MIB - 100))
        self.delete_after = (
            os.environ.get("UPLOAD_DELETE_AFTER", "true").lower() == "true")
        self.part_gap_secs = int(os.environ.get("UPLOAD_PART_GAP_SECS", "2"))


class Uploader:
    """Attach to a connected TelegramClient and schedule uploads.

    ``schedule()`` is safe to call from any thread (harvest runs in a
    daemon thread; the client lives on the asyncio loop).
    """

    def __init__(self, client, settings=None):
        self.client = client
        self.loop = client.loop
        self.settings = settings or UploadSettings()
        self.entity = None
        self._busy = False

    # -- configuration ------------------------------------------------------

    @classmethod
    def if_enabled(cls, client):
        settings = UploadSettings()
        if not settings.enabled:
            log("disabled (set UPLOAD_CHAT to enable)")
            return None
        return cls(client, settings)

    # -- scheduling ---------------------------------------------------------

    def schedule(self, path):
        """Fire-and-forget upload for a finished recording (any thread)."""
        if self._busy:
            # Serialize: one recording at a time, in call order.
            log(f"queueing behind a running upload: {os.path.basename(path)}")
        fut = asyncio.run_coroutine_threadsafe(
            self._upload_pipeline(path), self.loop)
        fut.add_done_callback(self._log_result)

    @staticmethod
    def _log_result(fut):
        try:
            exc = fut.exception()
        except asyncio.CancelledError:
            return
        if exc:
            log(f"pipeline failed: {type(exc).__name__}: {exc}")

    # -- pipeline -----------------------------------------------------------

    async def _upload_pipeline(self, path):
        if self._busy:
            await asyncio.sleep(3)
        self._busy = True
        try:
            if os.path.exists(path + ".uploaded.json"):
                log(f"already uploaded (marker), skipping: "
                    f"{os.path.basename(path)}")
                return
            if not os.path.exists(path):
                log(f"file vanished before upload: {path}")
                return
            await self._resolve_chat()
            sent = await self._send_file_or_segments(path)
            self._write_marker(path, sent)
            if self.settings.delete_after:
                self._cleanup(path, sent)
            log(f"done: {os.path.basename(path)} "
                f"({1 if sent is None else len(sent)} message(s))")
        finally:
            self._busy = False

    async def _resolve_chat(self):
        chat = self.settings.chat
        if chat == "me":
            self.entity = "me"
            return
        if chat.startswith("@") or "t.me/" in chat:
            self.entity = await self.client.get_entity(chat)
            return
        # Numeric id: seed the session's entity cache from dialogs first -
        # private channels/users only resolve with their access_hash.
        await self.client.get_dialogs()
        try:
            n = int(chat)
        except ValueError:
            self.entity = await self.client.get_entity(chat)
            return
        cands = [n]
        s = str(n)
        if s.startswith("-100"):
            cands.append(PeerChannel(int(s[4:])))
        elif n < 0:
            cands += [PeerChannel(-n), int(f"-100{-n}")]
        else:
            cands.append(PeerUser(n))
        for cand in cands:
            try:
                self.entity = await self.client.get_entity(cand)
                return
            except (ValueError, tg_errors.RPCError):
                continue
        raise ValueError(
            f"cannot resolve UPLOAD_CHAT '{chat}' - add the account to "
            "that chat first")

    async def _send_file_or_segments(self, path):
        size = os.path.getsize(path)
        limit = self.settings.chunk_mib * 1024 * 1024
        stem = os.path.splitext(os.path.basename(path))[0]
        if size <= limit:
            msg = await self._send_with_retry(path, stem)
            return [msg.id]
        log(f"{size / 1024 / 1024:.0f} MiB exceeds {self.settings.chunk_mib} "
            "MiB cap, splitting into segments")
        parts = await asyncio.get_running_loop().run_in_executor(
            None, self._segment, path, limit)
        try:
            total = len(parts)
            msg_ids = []
            for i, part in enumerate(parts, 1):
                caption = f"{stem} ({i}/{total})" if total > 1 else stem
                msg = await self._send_with_retry(part, caption)
                msg_ids.append(msg.id)
                log(f"part {i}/{total} sent ({os.path.getsize(part) / 1024 / 1024:.0f} MiB)")
                if i < total and self.settings.part_gap_secs:
                    await asyncio.sleep(self.settings.part_gap_secs)
            return msg_ids
        finally:
            shutil.rmtree(os.path.dirname(parts[0]), ignore_errors=True)

    async def _send_with_retry(self, path, caption):
        """FloodWait sleeps exactly; other errors back off exponentially."""
        delays = (5, 15, 45)
        attempt = 0
        while True:
            try:
                msg = await self.client.send_file(
                    self.entity, path, caption=caption,
                    supports_streaming=True)
                return msg
            except tg_errors.FloodWaitError as e:
                log(f"FloodWait {e.seconds}s during "
                    f"{os.path.basename(path)}, waiting")
                await asyncio.sleep(e.seconds + 2)
            except (tg_errors.RPCError, ConnectionError, asyncio.TimeoutError) as e:
                if attempt >= len(delays):
                    raise
                wait = delays[attempt]
                attempt += 1
                log(f"{type(e).__name__} on {os.path.basename(path)}, "
                    f"retry {attempt} in {wait}s")
                await asyncio.sleep(wait)

    # -- splitting ----------------------------------------------------------

    def _segment(self, path, limit_bytes):
        """Re-mux into playable segments with -c copy (no re-encode).

        Segment boundaries land on source keyframes, so a segment can
        overshoot the target by up to one GOP. If any part still exceeds
        the cap, shrink the estimated duration proportionally and redo
        (bounded retries instead of failing the whole upload).
        """
        target = int(limit_bytes * 0.9)
        secs = self._segment_seconds(path, target)
        work = os.path.join(
            os.path.dirname(path),
            f".chunks-{os.path.basename(path)}-{int(time.time())}")
        parts = []
        for attempt in range(3):
            shutil.rmtree(work, ignore_errors=True)
            os.makedirs(work, exist_ok=True)
            pattern = os.path.join(work, "part%03d.mp4")
            subprocess.run(
                ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                 "-i", path, "-c", "copy", "-f", "segment",
                 "-segment_time", str(secs), "-segment_format", "mp4",
                 "-reset_timestamps", "1", pattern],
                check=True)
            parts = sorted(glob.glob(os.path.join(work, "part*.mp4")))
            if not parts:
                raise RuntimeError("segmenter produced no output")
            worst = max(os.path.getsize(p) for p in parts)
            if worst <= limit_bytes:
                log(f"split into {len(parts)} segments "
                    f"(~{secs:.0f}s each at measured bitrate)")
                return parts
            if attempt == 2:
                break
            secs = max(5, int(secs * limit_bytes / worst * 0.8))
            log(f"keyframe overshoot, retrying with {secs}s segments")
        raise RuntimeError(
            f"segment {os.path.basename(max(parts, key=os.path.getsize))} "
            f"still exceeds the upload cap after retries; "
            f"lower UPLOAD_CHUNK_MIB and re-run")

    @staticmethod
    def _segment_seconds(path, target_bytes):
        """Derive segment length from the file's measured bitrate."""
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "format=duration,bit_rate", "-of", "json", path],
            check=True, capture_output=True, text=True)
        fmt = json.loads(probe.stdout).get("format", {})
        try:
            bit_rate = float(fmt["bit_rate"])
        except (KeyError, ValueError):
            duration = float(fmt["duration"])
            bit_rate = os.path.getsize(path) * 8 / max(duration, 1)
        secs = target_bytes * 8 / max(bit_rate, 1)
        # Floor only guards against a degenerate tiny estimate; raising it
        # further would push segments over the size cap on high-bitrate
        # streams (segment must satisfy secs*bitrate <= limit).
        return max(10, int(secs))

    # -- bookkeeping --------------------------------------------------------

    @staticmethod
    def _write_marker(path, msg_ids):
        with open(path + ".uploaded.json", "w") as m:
            json.dump({
                "file": os.path.basename(path),
                "messages": [] if msg_ids is None else msg_ids,
                "uploaded_at": datetime.datetime.now().isoformat(),
            }, m, indent=1)

    def _cleanup(self, path, msg_ids):
        if msg_ids is None:
            log("nothing confirmed sent, keeping local file")
            return
        try:
            os.remove(path)
            log(f"deleted local file after upload: {os.path.basename(path)}")
        except OSError as exc:
            log(f"could not delete {path}: {exc}")


# Wire-up point for capture.py. The Recorder harvest thread calls
# schedule_upload(); calls made before Capture.run() attaches a client
# (startup salvage) are buffered and flushed on attach.
SCHEDULER = None
_PENDING = []


def attach(client):
    global SCHEDULER
    SCHEDULER = Uploader.if_enabled(client)
    if SCHEDULER is not None:
        for path in _PENDING:
            SCHEDULER.schedule(path)
        _PENDING.clear()
    return SCHEDULER is not None


def schedule_upload(path):
    if SCHEDULER is not None:
        SCHEDULER.schedule(path)
    else:
        _PENDING.append(path)
        log(f"client not attached yet, buffering: {os.path.basename(path)}")
