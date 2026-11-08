#!/usr/bin/env python3
"""tgstream capture: pull one Telegram channel's live stream directly over
MTProto (no browser) and serve it as HLS + MPEG-TS for Plex/Jellyfin, while
recording each stream to the library.

Telegram RTMP broadcasts are delivered as ~1s standalone-MP4 chunks over
MTProto (join the call for presence, then upload.getFile the chunks). Each
chunk is remuxed (-c copy, no re-encode) to an MPEG-TS HLS segment on a
gapless timeline. Detection, join, download and recording are all API calls -
no Chromium, no Xvfb, no MediaMTX.
"""

import asyncio
import datetime
import html
import json
import os
import re
import io
import struct
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import av
from av.bitstream import BitStreamFilterContext
from telethon import TelegramClient, functions, types
from telethon.errors import RPCError, SessionPasswordNeededError
from uploader import attach as attach_uploader, schedule_upload


def env(name, default=None, required=False):
    v = os.environ.get(name, "")
    if not v:
        if required:
            raise SystemExit(f"CRITICAL ERROR: {name} is required")
        return default
    return v


CFG = {
    "slug": env("TG_SLUG", required=True),
    "peer": env("TG_PEER", required=True),
    "name": env("TG_NAME") or env("TG_SLUG", required=True),
    "channel_number": int(env("TG_CHANNEL_NUMBER", "1")),
    "api_id": int(env("API_ID", required=True)),
    "api_hash": env("API_HASH", required=True),
    "record": env("RECORD", "true").lower() == "true",
    "public_host": env("PUBLIC_HOST", "localhost"),
    "http_port": int(env("HTTP_PORT", "8409")),
    "public_http_port": int(env("PUBLIC_HTTP_PORT", env("HTTP_PORT", "8409"))),
    "poll_interval": float(env("POLL_INTERVAL", "5")),
    "buffer_ms": int(env("BUFFER_MS", "2000")),
    "window": int(env("HLS_WINDOW", "16")),
    "grace": int(env("HLS_GRACE", "40")),
    "video_quality": int(env("VIDEO_QUALITY", "2")),
    # Chunk fetches kept in flight. Sequential fetch turns per-request
    # latency straight into lost content (10s/chunk = 90% loss); parallel
    # requests ride out slow stream servers (2026-08-21 sprint quali).
    "prefetch": int(env("PREFETCH", "8")),
    "state_dir": env("STATE_DIR", "/state"),
    "library_dir": env("LIBRARY_DIR", "/library"),
    "run_dir": env("RUN_DIR", "/run/tgstream"),
}

PATH = f"tg-{CFG['slug']}"
HLS_DIR = os.path.join(CFG["run_dir"], "hls")
# Recordings grow in /state (persistent volume), NOT tmpfs: an in-progress
# recording must survive container restarts - salvage() harvests leftovers
# on startup. tmpfs also meant RAM filling at stream bitrate.
REC_DIR = os.path.join(CFG["state_dir"], "rec")
BASE_URL = f"http://{CFG['public_host']}:{CFG['public_http_port']}"
STREAM_URL = f"{BASE_URL}/stream.m3u8"
LOGO_SRC = os.path.join(CFG["state_dir"], "logo.jpg")     # square avatar
LOGO_FILE = os.path.join(CFG["state_dir"], "logo.png")    # 16:9 landscape
POSTER_FILE = os.path.join(CFG["state_dir"], "poster.png")  # 2:3 portrait
XMLTV_FMT = "%Y%m%d%H%M%S %z"
JOIN_SSRC = 0x50000000


def logo_url():
    return f"{BASE_URL}/logo.png" if os.path.exists(LOGO_FILE) else ""


def log(msg):
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} [CAPTURE] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Minimal MP4 parser: audio-track content duration (seconds).
#
# Telegram chunks carry ~1003ms of audio (47 AAC frames) but 1000ms of video.
# Advancing the output timeline by the AUDIO duration keeps audio perfectly
# gapless and A/V synced (shared offset), leaving only an imperceptible ~3ms
# video micro-gap. The audio track's mdhd has timescale 48000.
# ---------------------------------------------------------------------------

def audio_duration(mp4: bytes) -> float:
    best = None
    i = 0
    n = len(mp4)
    # Scan for every 'mdhd' box and read its timescale/duration; the audio
    # track is the one with timescale == the audio sample rate (48000).
    while True:
        j = mp4.find(b"mdhd", i)
        if j < 0:
            break
        i = j + 4
        try:
            ver = mp4[j + 4]
            if ver == 1:
                ts = struct.unpack(">I", mp4[j + 4 + 20:j + 4 + 24])[0]
                dur = struct.unpack(">Q", mp4[j + 4 + 24:j + 4 + 32])[0]
            else:
                ts = struct.unpack(">I", mp4[j + 4 + 12:j + 4 + 16])[0]
                dur = struct.unpack(">I", mp4[j + 4 + 16:j + 4 + 20])[0]
            if ts in (48000, 44100, 24000, 16000) and dur:
                best = dur / ts
        except (struct.error, IndexError):
            continue
    return best if best else 1.0


def sanitize_title(title):
    title = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "", title).strip().rstrip(".")
    return title[:120] or "Stream"


# ---------------------------------------------------------------------------
# Shared state (HTTP reads it, the capture loop writes it)
# ---------------------------------------------------------------------------

class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.state = "starting"     # starting|needs-login|idle|live
        self.title = None
        self.since = None
        self.last_error = None
        self.qr_png = None
        self.qr_token = None
        self.password_needed = False
        self.pending_password = None
        self.last_poll = None
        self.last_media = None

    def poll_tick(self):
        with self.lock:
            self.last_poll = time.time()

    def media_tick(self):
        with self.lock:
            self.last_media = time.time()

    def set(self, state=None, title=None, error=None):
        with self.lock:
            if state is not None and state != self.state:
                self.state = state
                self.since = datetime.datetime.now(datetime.timezone.utc)
                log(f"State -> {state}" + (f" ({title})" if title else ""))
            if title is not None:
                self.title = title or None
            if error is not None:
                self.last_error = error
                log(f"Error: {error}")

    def snapshot(self):
        with self.lock:
            return {
                "slug": CFG["slug"], "name": CFG["name"], "path": PATH,
                "channel_number": CFG["channel_number"], "state": self.state,
                "title": self.title,
                "since": self.since.isoformat() if self.since else None,
                "since_ts": self.since.timestamp() if self.since else None,
                "url": STREAM_URL, "logo": logo_url(), "record": CFG["record"],
                "last_error": self.last_error,
                "last_poll_age": round(time.time() - self.last_poll, 1)
                                 if self.last_poll else None,
                "last_media_age": round(time.time() - self.last_media, 1)
                                  if self.last_media else None,
            }


STATE = State()


# ---------------------------------------------------------------------------
# HLS: rolling playlist of MPEG-TS segments written by the streamer.
# ---------------------------------------------------------------------------

class Hls:
    """Owns the segment directory and the live m3u8. Segments are written by
    remux(); the playlist advertises a rolling WINDOW, files are kept GRACE
    longer so a lagging player never 404s."""

    def __init__(self):
        self.lock = threading.Lock()
        self.idx = 0
        self.seq = 0
        self.segs = []          # (index, duration) in the playlist window
        self.off = 0.0          # running timeline offset (seconds)
        self.disc_pending = False
        os.makedirs(HLS_DIR, exist_ok=True)

    def reset_offset(self):
        # Called on (re)join / slate<->live transitions: next segment carries
        # an EXT-X-DISCONTINUITY so players re-baseline their clock.
        self.disc_pending = True

    def add(self, mp4_bytes, record_writer=None):
        """Remux one chunk to a segment, publish it, return its duration."""
        idx = self.idx
        out_ts = os.path.join(HLS_DIR, f"s{idx}.ts")
        part = out_ts + ".part"
        dur = audio_duration(mp4_bytes)
        # In-process packet remux (PyAV): copy the already-encoded H.264/AAC
        # packets onto the running gapless timeline. This produces
        # player-clean MPEG-TS - the ffmpeg-CLI -c copy path left VLC/Plex
        # without audio.
        try:
            inp = av.open(io.BytesIO(mp4_bytes), mode="r")
            out = av.open(part, mode="w", format="mpegts")
            omap = {}
            bsf = None
            for s in inp.streams:
                if s.type in ("video", "audio"):
                    try:
                        omap[s.index] = out.add_stream_from_template(s)
                    except AttributeError:
                        omap[s.index] = out.add_stream(template=s)
                    if s.type == "video":
                        # Explicit AVCC -> Annex-B. libavformat's automatic
                        # conversion sniffs the first packet's bytes and is
                        # fooled when an AVCC length prefix happens to look
                        # like a startcode (e.g. a leading 1-byte NAL =
                        # 00 00 00 01): the whole segment is then written
                        # length-prefixed - unplayable video, intact audio.
                        # The bsf passes real Annex-B input through untouched.
                        bsf = BitStreamFilterContext("h264_mp4toannexb", s)
            for pkt in inp.demux():
                if pkt.dts is None or pkt.stream.index not in omap:
                    continue
                oidx = pkt.stream.index
                if bsf is not None and pkt.stream.type == "video":
                    pkts = bsf.filter(pkt)
                else:
                    pkts = (pkt,)
                for p in pkts:
                    off = int(round(self.off / float(p.time_base)))
                    p.pts = (p.pts or 0) + off
                    p.dts = (p.dts or 0) + off
                    p.stream = omap[oidx]
                    out.mux(p)
            out.close()
            inp.close()
        except Exception as exc:  # noqa: BLE001
            STATE.set(error=f"remux: {exc}")
            return dur
        try:
            os.replace(part, out_ts)
        except OSError:
            return dur
        if record_writer is not None:
            record_writer(out_ts)
        with self.lock:
            disc = self.disc_pending
            self.disc_pending = False
            self.segs.append((idx, dur, disc))
            if len(self.segs) > CFG["window"]:
                self.segs.pop(0)
                self.seq += 1
            self._write_playlist()
            self.off += dur
            self.idx += 1
        old = os.path.join(HLS_DIR, f"s{idx - CFG['window'] - CFG['grace']}.ts")
        if os.path.exists(old):
            try:
                os.remove(old)
            except OSError:
                pass
        return dur

    def _write_playlist(self):
        target = max((int(d) + 1 for _, d, _ in self.segs), default=1)
        lines = ["#EXTM3U", "#EXT-X-VERSION:3",
                 f"#EXT-X-TARGETDURATION:{target}",
                 f"#EXT-X-MEDIA-SEQUENCE:{self.seq}"]
        for i, d, disc in self.segs:
            if disc:
                lines.append("#EXT-X-DISCONTINUITY")
            lines.append(f"#EXTINF:{d:.3f},")
            lines.append(f"s{i}.ts")
        tmp = os.path.join(HLS_DIR, "index.m3u8.tmp")
        with open(tmp, "w") as f:
            f.write("\n".join(lines) + "\n")
        os.replace(tmp, os.path.join(HLS_DIR, "index.m3u8"))

    def playlist_bytes(self):
        p = os.path.join(HLS_DIR, "index.m3u8")
        try:
            with open(p, "rb") as f:
                return f.read()
        except OSError:
            return None

    def segment_path(self, name):
        if not re.fullmatch(r"s\d+\.ts", name):
            return None
        return os.path.join(HLS_DIR, name)

    def latest_index(self):
        with self.lock:
            return self.idx


HLS = Hls()


# ---------------------------------------------------------------------------
# Recording: archive live segments, concat to mp4 on stream end.
# ---------------------------------------------------------------------------

class Recorder:
    """Append each segment's bytes to one growing MPEG-TS on disk (segments
    share a continuous timeline, so byte-append concatenates cleanly), then
    remux to mp4 on stream end. A single growing file - not 1000s of small
    tmpfs files - so long streams don't pile up."""

    def __init__(self):
        self.active = False
        self.fh = None
        self.tmp_ts = None
        self.title = None
        self.start_ts = None

    def begin(self, title):
        if not CFG["record"]:
            return
        os.makedirs(REC_DIR, exist_ok=True)
        self.title = title or CFG["name"]
        self.start_ts = time.time()
        # Unique per recording: a fixed name raced the harvest thread when a
        # stream flapped (begin() truncated the file _harvest was reading,
        # then _harvest deleted the new recording out from under us).
        self.tmp_ts = os.path.join(
            REC_DIR, f"recording-{int(self.start_ts * 1000)}.ts")
        try:
            self.fh = open(self.tmp_ts, "wb")
            # Sidecar with the title so a post-restart salvage can name the
            # file properly (the in-memory title dies with the process).
            with open(self.tmp_ts + ".title", "w") as m:
                m.write(self.title)
        except OSError as exc:
            STATE.set(error=f"record open failed: {exc}")
            return
        self.active = True
        log(f"Recording started: {self.title}")

    def add(self, ts_path):
        if not self.active or self.fh is None:
            return
        try:
            with open(ts_path, "rb") as s:
                self.fh.write(s.read())
        except OSError as exc:
            STATE.set(error=f"record append failed: {exc}")

    def finish(self):
        if not self.active:
            return
        self.active = False
        try:
            self.fh.close()
        except OSError:
            pass
        self.fh = None
        if not self.tmp_ts or not os.path.exists(self.tmp_ts) \
                or os.path.getsize(self.tmp_ts) == 0:
            return
        threading.Thread(
            target=self._harvest, args=(self.title, self.start_ts, self.tmp_ts),
            daemon=True).start()

    def _harvest(self, title, start_ts, ts_file):
        try:
            stamp = datetime.datetime.fromtimestamp(start_ts) \
                .strftime("%Y-%m-%d %H-%M")
            # Write straight into /library (no per-channel subfolder); mount a
            # subfolder as /library if per-channel separation is wanted.
            out_dir = CFG["library_dir"]
            os.makedirs(out_dir, exist_ok=True)
            base = f"{CFG['name']} - {stamp}"
            clean_title = sanitize_title(title)
            # Skip the title when it just repeats the channel name (common on
            # 24/7 streams where the stream title is the channel title).
            if clean_title.lower() != CFG["name"].lower():
                base += f" - {clean_title}"
            out = os.path.join(out_dir, f"{base}.mp4")
            n = 2
            while os.path.exists(out):
                out = os.path.join(out_dir, f"{base} ({n}).mp4")
                n += 1
            tmp = out + ".part"
            log(f"Harvesting recording -> {out}")
            subprocess.run(
                ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                 "-i", ts_file, "-c", "copy", "-movflags", "+faststart",
                 "-f", "mp4", tmp],
                check=True)
            os.replace(tmp, out)
            log(f"Harvest complete: {out}")
            schedule_upload(out)
        except Exception as exc:  # noqa: BLE001
            STATE.set(error=f"harvest failed: {exc}")
        finally:
            for f in (ts_file, ts_file + ".title"):
                try:
                    os.remove(f)
                except OSError:
                    pass

    def salvage(self):
        """Harvest recordings a previous container run left behind (restart,
        crash, redeploy mid-stream). Runs once at startup, before any new
        recording begins. Truncated TS still remuxes - that is why the
        recording format is TS."""
        try:
            leftovers = sorted(
                f for f in os.listdir(REC_DIR)
                if re.fullmatch(r"recording-\d+\.ts", f))
        except OSError:
            return
        for name in leftovers:
            path = os.path.join(REC_DIR, name)
            if os.path.getsize(path) == 0:
                for f in (path, path + ".title"):
                    try:
                        os.remove(f)
                    except OSError:
                        pass
                continue
            start_ts = int(name[len("recording-"):-len(".ts")]) / 1000.0
            try:
                with open(path + ".title") as m:
                    title = m.read().strip()
            except OSError:
                title = ""
            log(f"Salvaging interrupted recording {name} "
                f"({os.path.getsize(path)} bytes)")
            threading.Thread(
                target=self._harvest, args=(title, start_ts, path),
                daemon=True).start()


REC = Recorder()


# ---------------------------------------------------------------------------
# HTTP endpoints
# ---------------------------------------------------------------------------

def render_playlist():
    s = STATE.snapshot()
    logo = f' tvg-logo="{s["logo"]}"' if s.get("logo") else ""
    return "\n".join([
        "#EXTM3U",
        (f'#EXTINF:-1 tvg-id="{s["slug"]}" tvg-name="{s["name"]}" '
         f'tvg-chno="{s["channel_number"]}"{logo} '
         f'group-title="Telegram",{s["name"]}'),
        s["url"], ""])


def render_epg():
    s = STATE.snapshot()
    now = datetime.datetime.now(datetime.timezone.utc).replace(
        minute=0, second=0, microsecond=0)
    out = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<tv generator-info-name="tgstream">',
           f'  <channel id="{s["slug"]}">',
           f'    <display-name>{html.escape(s["name"])}</display-name>']
    if s.get("logo"):
        out.append(f'    <icon src="{html.escape(s["logo"])}" />')
    out.append('  </channel>')

    def prog(a, b, title):
        out.append(f'  <programme start="{a.strftime(XMLTV_FMT)}" '
                   f'stop="{b.strftime(XMLTV_FMT)}" channel="{s["slug"]}">')
        out.append(f'    <title>{html.escape(title)}</title>')
        # Programme-level icon: Plex's guide grid / "Shows On Now" use the
        # programme artwork, not the channel logo.
        if s.get("logo"):
            out.append(f'    <icon src="{html.escape(s["logo"])}" />')
        out.append('  </programme>')

    # Horizon must exceed the servers' guide-refresh interval (24h by
    # default) or the grid goes empty between refreshes.
    if s["state"] == "live" and s["since"]:
        since = datetime.datetime.fromisoformat(s["since"])
        prog(since, now + datetime.timedelta(hours=6),
             s["title"] or f"{s['name']} live")
        for i in range(6, 36):
            prog(now + datetime.timedelta(hours=i),
                 now + datetime.timedelta(hours=i + 1),
                 f"{s['name']} (no stream)")
    else:
        for i in range(36):
            prog(now + datetime.timedelta(hours=i),
                 now + datetime.timedelta(hours=i + 1),
                 f"{s['name']} (no stream)")
    out.append('</tv>')
    return "\n".join(out)


def render_login_page():
    s = STATE.snapshot()
    if s["state"] != "needs-login":
        return ("<p>✓ Logged in.</p>"
                f"<p>State: <b>{html.escape(s['state'])}</b></p>"), 30
    with STATE.lock:
        pw = STATE.password_needed
        token = STATE.qr_token
    if pw:
        return ("<p>Two-factor password required:</p>"
                '<form method="post" action="/login/password">'
                '<input type="password" name="password" autofocus>'
                ' <button>Submit</button></form>'), 0
    if token:
        return ("<p>Scan with Telegram: <b>Settings &rarr; Devices &rarr; "
                "Link Desktop Device</b></p>"
                '<p><img src="/qr.png" width="300" height="300"'
                ' style="image-rendering:pixelated"></p>'
                f"<p><code>{html.escape(token)}</code></p>"), 3
    return "<p>Waiting for QR…</p>", 2


class Handler(BaseHTTPRequestHandler):
    def _send(self, body, ctype, code=200):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/stream.m3u8", "/index.m3u8"):
            pl = HLS.playlist_bytes()
            if pl is None:
                self.send_error(404)
            else:
                self._send(pl, "application/vnd.apple.mpegurl")
            return
        if path == "/stream.ts":
            self._serve_continuous_ts()
            return
        if path in ("/logo.png", "/poster.png", "/logo.jpg"):
            f_ = {"/logo.png": LOGO_FILE, "/poster.png": POSTER_FILE,
                  "/logo.jpg": LOGO_SRC}[path]
            ct = "image/jpeg" if path.endswith(".jpg") else "image/png"
            if os.path.exists(f_):
                with open(f_, "rb") as f:
                    self._send(f.read(), ct)
            else:
                self.send_error(404)
            return
        if re.fullmatch(r"/s\d+\.ts", path):
            seg = HLS.segment_path(path.lstrip("/"))
            if seg and os.path.exists(seg):
                with open(seg, "rb") as f:
                    self._send(f.read(), "video/mp2t")
            else:
                self.send_error(404)
            return
        if path == "/qr.png":
            with STATE.lock:
                png = STATE.qr_png
            if png is None:
                self.send_error(404)
            else:
                self._send(png, "image/png")
            return
        if path == "/login":
            body, refresh = render_login_page()
            meta = f'<meta http-equiv="refresh" content="{refresh}">' if refresh else ""
            page = (f"<!doctype html><title>{html.escape(CFG['name'])} login"
                    f"</title>{meta}<body style='font-family:sans-serif;"
                    f"max-width:40em;margin:2em auto'>"
                    f"<h2>tgstream: {html.escape(CFG['name'])}</h2>{body}")
            self._send(page.encode(), "text/html")
            return
        routes = {
            "/status": (lambda: json.dumps(STATE.snapshot()), "application/json"),
            "/playlist.m3u": (render_playlist, "audio/x-mpegurl"),
            "/epg.xml": (render_epg, "application/xml"),
        }
        r = routes.get(path)
        if r is None:
            self.send_error(404)
        else:
            self._send(r[0]().encode(), r[1])

    def do_POST(self):
        if self.path.split("?")[0] != "/login/password":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", 0))
        import urllib.parse
        form = urllib.parse.parse_qs(self.rfile.read(length).decode())
        pw = (form.get("password") or [""])[0]
        if pw:
            with STATE.lock:
                STATE.pending_password = pw
        self.send_response(303)
        self.send_header("Location", "/login")
        self.end_headers()

    # MPEG-TS null packet (PID 0x1fff): demuxers discard it, but it counts
    # as data on the wire.
    TS_NULL = b"\x47\x1f\xff\x10" + b"\xff" * 184

    def _serve_continuous_ts(self):
        # Never-ending MPEG-TS for Plex's HDHomeRun client: stream segment
        # files back to back as they are produced, starting near live.
        self.send_response(200)
        self.send_header("Content-Type", "video/mp2t")
        self.end_headers()
        # Start 8 segments back: a client tuning in mid-stall needs enough
        # buffered real media to establish its pipeline (Plex's MKV remux
        # stage fails on a ~3s burst followed by null-packet filler).
        nxt = max(0, HLS.latest_index() - 8)
        idle = 0
        while True:
            seg = os.path.join(HLS_DIR, f"s{nxt}.ts")
            if os.path.exists(seg):
                try:
                    with open(seg, "rb") as f:
                        self.wfile.write(f.read())
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return
                nxt += 1
                idle = 0
            else:
                if nxt < HLS.latest_index() - CFG["window"] - CFG["grace"]:
                    nxt = HLS.latest_index()  # fell too far behind
                time.sleep(0.2)
                idle += 1
                if idle > 3000:
                    return
                if idle % 10 == 0:
                    # Source stall: feed TS null packets so the client's
                    # read timeout (Plex Transcoder: 30s) doesn't kill the
                    # session before real data resumes.
                    try:
                        self.wfile.write(self.TS_NULL * 16)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        return

    def log_message(self, *a):
        pass


def start_http():
    srv = ThreadingHTTPServer(("0.0.0.0", CFG["http_port"]), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log(f"HTTP on :{CFG['http_port']} (stream {STREAM_URL})")


# ---------------------------------------------------------------------------
# Slate: publish a static card as HLS when no stream is live, so Plex never
# sees a dead tuner. Generated once by make-slate into /state/slate.ts.
# ---------------------------------------------------------------------------

def slate_loop(stop_evt):
    slate = os.path.join(CFG["state_dir"], "slate.ts")
    if not os.path.exists(slate):
        log("No slate.ts - idle channel will have no filler")
        return
    with open(slate, "rb") as f:
        data = f.read()
    dur = 10.0
    try:
        p = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=nk=1:nw=1", slate],
            capture_output=True, text=True)
        dur = float(p.stdout.strip() or 10.0)
    except (ValueError, OSError):
        pass
    HLS.reset_offset()
    while not stop_evt.is_set():
        idx = HLS.idx
        out_ts = os.path.join(HLS_DIR, f"s{idx}.ts")
        part = out_ts + ".part"
        subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
             "-i", slate, "-c", "copy", "-muxdelay", "0",
             "-output_ts_offset", f"{HLS.off:.3f}", "-f", "mpegts", part],
            check=False)
        try:
            os.replace(part, out_ts)
        except OSError:
            break
        with HLS.lock:
            HLS.segs.append((idx, dur, HLS.disc_pending))
            HLS.disc_pending = False
            if len(HLS.segs) > CFG["window"]:
                HLS.segs.pop(0)
                HLS.seq += 1
            HLS._write_playlist()
            HLS.off += dur
            HLS.idx += 1
        old = os.path.join(HLS_DIR, f"s{idx - CFG['window'] - CFG['grace']}.ts")
        if os.path.exists(old):
            try:
                os.remove(old)
            except OSError:
                pass
        stop_evt.wait(dur * 0.8)


# ---------------------------------------------------------------------------
# MTProto capture
# ---------------------------------------------------------------------------

class Capture:
    def __init__(self):
        self.client = None
        self.entity = None
        # Group-call streaming requests must run on the DC the call lives
        # on with one consistent auth (join state is per auth); Telethon
        # has no CALL_MIGRATE handling, so call_invoke() re-routes via an
        # exported sender (populated on first migrate error).
        self._call_dc = None

    async def qr_login(self):
        import base64
        if await self.client.is_user_authorized():
            return
        STATE.set(state="needs-login")
        qr = await self.client.qr_login()
        last_printed = None
        while True:
            STATE.set(title="")
            url = qr.url
            png = self._qr_png(url)
            with STATE.lock:
                STATE.qr_png = png
                STATE.qr_token = url
            if url != last_printed:
                self._print_qr(url)
                last_printed = url
            # 2FA password, if the user submitted one via /login.
            with STATE.lock:
                pw = STATE.pending_password
                STATE.pending_password = None
            try:
                if await qr.wait(timeout=20):
                    break
            except asyncio.TimeoutError:
                pass
            except SessionPasswordNeededError:
                with STATE.lock:
                    STATE.password_needed = True
                if pw:
                    await self.client.sign_in(password=pw)
                    break
            await qr.recreate()
        with STATE.lock:
            STATE.qr_png = None
            STATE.qr_token = None
            STATE.password_needed = False
        log("Login complete - session saved in /state")

    def _qr_png(self, url):
        try:
            p = subprocess.run(["qrencode", "-t", "PNG", "-o", "-", "-s", "6",
                                url], capture_output=True)
            return p.stdout or None
        except OSError:
            return None

    def _print_qr(self, url):
        try:
            qr = subprocess.run(["qrencode", "-t", "UTF8", url],
                                capture_output=True, text=True).stdout
        except OSError:
            qr = url
        login = (f"http://{CFG['public_host']}:{CFG['public_http_port']}/login")
        print(f"\n==== TELEGRAM LOGIN REQUIRED ({CFG['name']}) ====\n"
              f"Scan with Telegram: Settings -> Devices -> Link Desktop Device\n"
              f"(or open {login} )\n\n{qr}\n"
              f"Code rotates ~every 30s.\n", flush=True)

    async def fetch_logo(self):
        # Download the channel's (square) profile photo, then derive the
        # aspect ratios Plex uses: 16:9 landscape (/logo.png, guide + On Now,
        # advertised as tvg-logo / <icon>) and 2:3 portrait (/poster.png, home
        # posters). The square avatar is centered on a transparent canvas.
        try:
            path = await self.client.download_profile_photo(
                self.entity, file=LOGO_SRC)
        except RPCError as exc:
            log(f"logo fetch failed: {exc}")
            return
        for f in (LOGO_FILE, POSTER_FILE):
            try:
                os.path.exists(f) and os.remove(f)
            except OSError:
                pass
        if not path:
            return
        pad = ("format=rgba,scale={sw}:{sh},"
               "pad={w}:{h}:(ow-iw)/2:(oh-ih)/2:color=#00000000")
        variants = [
            (LOGO_FILE, pad.format(sw=-1, sh=270, w=480, h=270)),   # 16:9
            (POSTER_FILE, pad.format(sw=360, sh=-1, w=360, h=540)),  # 2:3
        ]
        for out, vf in variants:
            subprocess.run(
                ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                 "-i", LOGO_SRC, "-vf", vf, out], check=False)
        log("Channel logo saved (square, 16:9, 2:3)")

    async def resolve_peer(self):
        peer = CFG["peer"].strip()
        if peer.startswith("@") or "t.me/" in peer:
            return await self.client.get_entity(peer)
        # Numeric channel id. Telegram Web hashes it as "-<channel_id>";
        # bot-API marks it "-100<channel_id>". Populate the entity cache
        # (access_hash for private channels) then resolve.
        await self.client.get_dialogs()
        try:
            n = int(peer)
        except ValueError:
            return await self.client.get_entity(peer)
        for cand in self._id_candidates(n):
            try:
                return await self.client.get_entity(cand)
            except (ValueError, RPCError):
                continue
        raise SystemExit(f"CRITICAL ERROR: cannot resolve TG_PEER '{peer}' - "
                         "is the account a member of the channel?")

    @staticmethod
    def _id_candidates(n):
        s = str(n)
        cands = [n]
        if s.startswith("-100"):
            cands.append(types.PeerChannel(int(s[4:])))
        elif n < 0:
            cid = -n
            cands += [types.PeerChannel(cid), int(f"-100{cid}")]
        else:
            cands += [types.PeerChannel(n), int(f"-100{n}")]
        return cands

    @staticmethod
    def _migrate_dc(exc):
        # FILE_MIGRATE: chunk fetches name the file's DC directly.
        for text in (str(exc), getattr(exc, "message", "")):
            m = re.search(r"(?:CALL|PHONE|NETWORK|FILE)_MIGRATE_(\d+)", text)
            if m:
                return int(m.group(1))
        return None

    async def call_invoke(self, request):
        """Invoke a call-bound request, migrating DCs when told to.

        Group-call traffic (join/keepalive/stream channels/chunk GetFile)
        must run on the DC the call lives on AND with one consistent
        authorization - join state is per auth, so a fetch from a
        different exported auth fails with JOIN_MISSING. Telethon's
        _borrow_exported_sender caches one sender per DC (with proper
        InvokeWithLayer init), which satisfies both.
        """
        if self._call_dc is not None:
            try:
                sender = await self.client._borrow_exported_sender(
                    self._call_dc)
                return await self.client._call(sender, request)
            except (RPCError, ValueError, ConnectionError,
                    asyncio.TimeoutError) as e:
                dc = self._migrate_dc(e)
                # Re-exporting on every error floods Telegram's auth-export
                # API, which terminates the whole session for abuse. Only
                # follow a migration to a *different* DC.
                if dc is None or dc == self._call_dc:
                    raise
                getattr(self.client, "_borrowed_senders", {}).pop(
                    self._call_dc, None)
                self._call_dc = None
        try:
            return await self.client(request)
        except RPCError as e:
            dc = self._migrate_dc(e)
            if dc is None:
                raise
            sender = await self.client._borrow_exported_sender(dc)
            res = await self.client._call(sender, request)
            self._call_dc = dc
            log(f"group call traffic migrated to DC {dc}")
            return res

    async def live_call(self):
        # Same hard cap as fetch(): a hanging request here silently freezes
        # detection (state stuck "idle", no error, healthcheck green).
        full = await asyncio.wait_for(self.client(
            functions.channels.GetFullChannelRequest(channel=self.entity)), 15)
        STATE.poll_tick()
        return full.full_chat.call

    async def join(self, call):
        params = json.dumps({"fingerprints": [], "pwd": "", "ssrc": JOIN_SSRC,
                             "ssrc-groups": [], "ufrag": ""})
        await self.call_invoke(functions.phone.JoinGroupCallRequest(
            call=call, join_as=types.InputPeerSelf(), muted=True,
            video_stopped=True, params=types.DataJSON(data=params)))

    async def fetch(self, call, ts, scale, quality=None):
        """Returns (status, data, seconds-the-request-took). The duration
        feeds the latency-aware quality fallback."""
        loc = types.InputGroupCallStream(
            call=call, time_ms=ts, scale=scale, video_channel=1,
            video_quality=CFG["video_quality"] if quality is None else quality)
        t0 = time.monotonic()
        try:
            # Hard cap: a hanging request on a degraded Telegram server is
            # dead air the lag watchdog can't see (it only ticks on results).
            # 30s, not 15: the 2026-08-22 sprint showed Telegram serving
            # chunks in 15-25s all morning - a 15s cap turned every one of
            # those slow-but-completing requests into a total loss.
            res = await asyncio.wait_for(
                self.client(functions.upload.GetFileRequest(
                    location=loc, offset=0, limit=1024 * 1024)), 30)
            return ("ok", res.bytes[32:], time.monotonic() - t0)
        except RPCError as e:
            s = str(e)
            for k, tag in (("TIME_TOO_BIG", "big"), ("TIME_TOO_SMALL", "small"),
                           ("TIME_INVALID", "rejoin"),
                           ("GROUPCALL_JOIN_MISSING", "rejoin")):
                if k in s:
                    if tag == "rejoin":
                        log(f"rejoin cause: {k}")
                    return (tag, None, time.monotonic() - t0)
            if "JoinMissing" in repr(e):
                log("rejoin cause: JoinMissing")
                return ("rejoin", None, time.monotonic() - t0)
            return ("err", repr(e), time.monotonic() - t0)
        except (ValueError, ConnectionError, asyncio.TimeoutError) as e:
            # Telethon raises a bare ValueError ('Request was unsuccessful
            # N time(s)') after exhausting its internal retries when
            # Telegram's stream servers misbehave. That is a transient, not
            # a reason to tear the stream down to slate.
            return ("err", repr(e), time.monotonic() - t0)

    async def keepalive(self, get_call, ended):
        # Telegram drops a joined member after ~90s without activity, which
        # made getFile fail and forced a full reconnect (slate + split
        # recording). checkGroupCall keeps our presence alive; if the server
        # says our ssrc is gone, re-join in place.
        while not ended["v"]:
            await asyncio.sleep(15)
            call = get_call()
            try:
                r = await self.call_invoke(functions.phone.CheckGroupCallRequest(
                    call=call, sources=[JOIN_SSRC]))
                if JOIN_SSRC not in r:
                    log("keepalive: ssrc gone, re-joining")
                    await self.join(call)
            except (RPCError, ValueError, ConnectionError,
                    asyncio.TimeoutError) as e:
                log(f"keepalive: {type(e).__name__}, re-joining")
                try:
                    await self.join(call)
                except (RPCError, ValueError, ConnectionError,
                        asyncio.TimeoutError):
                    pass

    async def stream_call(self, call):
        """Publish a live call, riding out reconnects seamlessly (no slate,
        no split recording); return only when the call is truly gone."""
        state = {"call": call, "ended": False}
        REC.begin(STATE.title)
        STATE.set(state="live")
        HLS.reset_offset()

        queue = asyncio.Queue(maxsize=8)
        loop = asyncio.get_event_loop()
        ended = {"v": False}

        async def producer():
            nochan = 0
            chanerrs = 0
            # Per-stream effective quality: dropped a tier on sustained slow
            # fetches (degraded resolution beats losing minutes of content).
            quality = CFG["video_quality"]
            slow_errs = 0
            while not ended["v"]:
                try:
                    ch = await self.call_invoke(
                        functions.phone.GetGroupCallStreamChannelsRequest(
                            call=state["call"]))
                except (RPCError, ValueError, ConnectionError,
                        asyncio.TimeoutError) as e:
                    # This retry loop was silent too - a call that keeps
                    # rejecting GetGroupCallStreamChannels spins here forever
                    # looking exactly like a healthy live capture.
                    chanerrs += 1
                    if chanerrs <= 2 or chanerrs % 30 == 0:
                        log(f"NO-MEDIA: stream channels query failed "
                            f"(x{chanerrs}): {e!r}")
                    await asyncio.sleep(1)
                    try:
                        c = await self.live_call()
                    except (RPCError, ValueError, ConnectionError,
                            asyncio.TimeoutError):
                        continue
                    if c is None:
                        break
                    state["call"] = c
                    try:
                        await self.join(c)
                    except (RPCError, ValueError, ConnectionError,
                            asyncio.TimeoutError):
                        pass
                    continue
                chanerrs = 0
                if not ch.channels:
                    # Call exists but serves no stream channels (broadcaster
                    # not sending, or a WebRTC video chat). Silent before -
                    # made FP1's 7-minute source gap undiagnosable.
                    nochan += 1
                    if nochan == 5 or nochan % 30 == 0:
                        log(f"NO-MEDIA: call live but no stream channels "
                            f"(~{nochan}s)")
                    await asyncio.sleep(1)
                    continue
                nochan = 0
                scale = ch.channels[0].scale
                seg = 1000 >> scale
                t = ((ch.channels[0].last_timestamp_ms // seg) * seg
                     - CFG["buffer_ms"])
                big = 0
                bigtot = 0
                errs = 0
                smalls = 0
                refetch = False
                # Lag watchdog baseline: wall clock vs media time fetched.
                base_wall = time.monotonic()
                base_t = t
                # Sliding window of in-flight chunk fetches. Ordered delivery:
                # only the head result drives the state machine; prefetches
                # just warm the pipeline so per-request latency stops gating
                # throughput.
                pending = {}

                def spawn(ts):
                    if ts not in pending:
                        pending[ts] = asyncio.ensure_future(
                            self.fetch(state["call"], ts, scale, quality))

                def cancel_pending():
                    for task in pending.values():
                        task.cancel()
                    pending.clear()

                while not ended["v"] and not refetch:
                    spawn(t)
                    if big == 0:
                        # Prefetch only while flowing: at the live edge the
                        # lookahead would just burn TIME_TOO_BIG round-trips.
                        for k in range(1, CFG["prefetch"]):
                            spawn(t + k * seg)
                    status, data, dur = await pending.pop(t)
                    if status == "ok":
                        if bigtot >= 60:
                            log(f"MEDIA-RESUME: after ~{bigtot * 0.5:.0f}s at "
                                "the live edge with no new chunks")
                        bigtot = 0
                        big = 0
                        errs = 0
                        smalls = 0
                        # Latency-aware: a fetch that succeeds but takes many
                        # seconds is the degraded-server signature. Counting
                        # only failures (and decaying on every ok) meant the
                        # 2026-08-22 sprint - ok/timeout interleaved ~1:1 all
                        # morning - fired the fallback once, 10s before the
                        # stream ended.
                        if dur > 5:
                            slow_errs += 1
                        elif dur < 2:
                            slow_errs = max(0, slow_errs - 1)
                        STATE.media_tick()
                        await queue.put(data)
                        t += seg
                        if slow_errs >= 15 and quality > 0:
                            # With the 30s cap most slow fetches now succeed,
                            # so the fallback must also fire from here, not
                            # just from the error branch.
                            quality -= 1
                            slow_errs = 0
                            log(f"QUALITY-FALLBACK: sustained slow fetches, "
                                f"dropping video quality to {quality}")
                            refetch = True
                            continue
                        # Slow fetches (degraded Telegram stream servers)
                        # starve the output long before chunks expire; once
                        # we drift too far behind realtime, re-seek to the
                        # live edge instead of limping for minutes.
                        lag = ((time.monotonic() - base_wall)
                               - (t - base_t) / 1000.0)
                        if lag > 20:
                            # Salvage the contiguous run of already-completed
                            # prefetches first: reseeking used to cancel them,
                            # discarding fetched content (~22s/reseek in the
                            # 2026-08-21 soak). Each salvaged chunk also
                            # shrinks the lag - if enough landed, no reseek.
                            while True:
                                nxt = pending.get(t)
                                if nxt is None or not nxt.done() \
                                        or nxt.cancelled() \
                                        or nxt.result()[0] != "ok":
                                    break
                                await queue.put(pending.pop(t).result()[1])
                                STATE.media_tick()
                                t += seg
                            lag = ((time.monotonic() - base_wall)
                                   - (t - base_t) / 1000.0)
                            if lag > 20:
                                log(f"LAG: {lag:.0f}s behind realtime, "
                                    "re-seeking live edge")
                                refetch = True
                    elif status == "big":
                        cancel_pending()
                        await asyncio.sleep(0.2)
                        # At the live edge (or a source pause) media time
                        # legitimately stops advancing - reset the lag
                        # baseline so it only measures fetch slowness.
                        base_wall = time.monotonic()
                        base_t = t
                        big += 1
                        bigtot += 1
                        if bigtot == 60 or bigtot % 600 == 0:
                            log(f"NO-MEDIA: at live edge, no new chunks for "
                                f"~{bigtot * 0.5:.0f}s (source paused?)")
                        if big > 150:
                            try:
                                if (await self.live_call()) is None:
                                    ended["v"] = True
                            except (RPCError, ValueError, ConnectionError,
                                    asyncio.TimeoutError):
                                pass
                            big = 0
                    elif status == "small":
                        # Input-side content drop: the chunk expired before we
                        # fetched it (we fell behind Telegram's buffer).
                        log(f"INPUT-SKIP: chunk t={t} expired (TIME_TOO_SMALL)")
                        t += seg
                        smalls += 1
                        if smalls >= 5:
                            # A long expired run means we are far behind the
                            # retention window; skipping chunk-by-chunk costs
                            # one round-trip per second of content and races
                            # the live edge at ~break-even. Jump instead.
                            log("SEEK-LIVE: expired run, re-seeking live edge")
                            refetch = True
                    elif status == "rejoin":
                        cancel_pending()
                        log("RECONNECT: presence/call rotated, continuing live")
                        # Presence dropped or call rotated: re-resolve and
                        # carry on. The output timeline (HLS.off) is already
                        # continuous across the rejoin, so no discontinuity
                        # marker - tagging one made players re-baseline and
                        # visibly glitch. Discontinuities are only for
                        # slate<->live switches.
                        try:
                            c = await self.live_call()
                        except (RPCError, ValueError, ConnectionError,
                                asyncio.TimeoutError):
                            await asyncio.sleep(1)
                            refetch = True
                            continue
                        if c is None:
                            ended["v"] = True
                            break
                        state["call"] = c
                        try:
                            await self.join(c)
                        except (RPCError, ValueError, ConnectionError,
                                asyncio.TimeoutError):
                            pass
                        refetch = True
                    else:
                        errs += 1
                        slow_errs += 1
                        if errs <= 2 or errs % 40 == 0:
                            log(f"getFile: {data} (x{errs})")
                        if slow_errs >= 15 and quality > 0:
                            quality -= 1
                            slow_errs = 0
                            log(f"QUALITY-FALLBACK: sustained slow fetches, "
                                f"dropping video quality to {quality}")
                            refetch = True
                            continue
                        # A later chunk already landed while the head failed:
                        # skip the head immediately (1s hole). Requiring 3
                        # failures first (soak 2026-08-21) meant 45s of head
                        # retries, so the 20s LAG watchdog always won and
                        # re-seeked away ~22s of fetched backlog each time.
                        nxt = pending.get(t + seg)
                        if nxt is not None and nxt.done() \
                                and not nxt.cancelled() \
                                and nxt.result()[0] == "ok":
                            log(f"INPUT-SKIP: chunk t={t} unfetchable, "
                                "next is ready - skipping")
                            t += seg
                            errs = 0
                            continue
                        if "VIDEO_CHANNEL_INVALID" in (data or "") \
                                and errs >= 3:
                            # Source paused its video / changed stream params.
                            # Re-resolve the stream channels and re-seek to
                            # the live edge instead of hammering the same
                            # timestamp forever (which froze the output for
                            # the whole outage).
                            log("VIDEO-PAUSE: re-resolving stream channels")
                            await asyncio.sleep(1)
                            refetch = True
                        elif errs % 20 == 0:
                            # Sustained errors (e.g. Telethon retry
                            # exhaustion during Telegram server trouble):
                            # re-resolve and re-seek instead of hammering
                            # the same timestamp.
                            await asyncio.sleep(1)
                            refetch = True
                        else:
                            await asyncio.sleep(0.5)
                cancel_pending()
            ended["v"] = True
            await queue.put(None)

        async def consumer():
            while True:
                data = await queue.get()
                if data is None:
                    break
                await loop.run_in_executor(
                    None, HLS.add, data, REC.add if REC.active else None)

        try:
            await asyncio.gather(
                self.keepalive(lambda: state["call"], ended),
                producer(), consumer())
        finally:
            # Also on unexpected errors - otherwise the next begin() clobbers
            # an unfinished recording and the captured stream is lost.
            REC.finish()

    async def run(self):
        os.makedirs(CFG["state_dir"], exist_ok=True)
        self.client = TelegramClient(
            os.path.join(CFG["state_dir"], "session"),
            CFG["api_id"], CFG["api_hash"])
        await self.client.connect()
        await self.qr_login()
        self.entity = await self.resolve_peer()
        attach_uploader(self.client)
        await self.fetch_logo()

        stop_slate = threading.Event()
        slate_thread = None

        def start_slate():
            nonlocal slate_thread
            stop_slate.clear()
            slate_thread = threading.Thread(
                target=slate_loop, args=(stop_slate,), daemon=True)
            slate_thread.start()

        def stop_slate_fn():
            stop_slate.set()

        STATE.set(state="idle")
        start_slate()
        while True:
            try:
                call = await self.live_call()
            except (RPCError, ValueError, ConnectionError,
                    asyncio.TimeoutError) as e:
                # ValueError covers Telethon's bare 'Request was unsuccessful'
                # after exhausting internal retries; before this catch a
                # connection blip at poll time killed the whole process.
                STATE.set(error=f"detect: {e!r}")
                await asyncio.sleep(CFG["poll_interval"])
                continue
            if call is None:
                if STATE.snapshot()["state"] != "idle":
                    STATE.set(state="idle", title="")
                    start_slate()
                await asyncio.sleep(CFG["poll_interval"])
                continue
            # Live: stop slate, join, stream until it ends.
            stop_slate_fn()
            try:
                await self.join(call)
            except RPCError as e:
                STATE.set(error=f"join: {e!r}")
                await asyncio.sleep(3)
                start_slate()
                continue
            try:
                await self.stream_call(call)
            except Exception as e:  # noqa: BLE001
                STATE.set(error=f"stream: {e!r}")
            STATE.set(state="idle", title="")
            start_slate()
            await asyncio.sleep(2)


def main():
    start_http()
    REC.salvage()
    Capture_ = Capture()
    asyncio.run(Capture_.run())


if __name__ == "__main__":
    main()
