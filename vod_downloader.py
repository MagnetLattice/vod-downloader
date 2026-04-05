#!/usr/bin/env python3
"""
Twitch VOD Downloader and YouTube Uploader.

Downloads archived Twitch VODs using yt-dlp and uploads them to YouTube.
Tracks progress in per-folder CSV files.

Usage:
    python vod_downloader.py                     # Run all steps
    python vod_downloader.py --update            # Only update CSV trackers
    python vod_downloader.py --download          # Only download VODs
    python vod_downloader.py --upload            # Only upload to YouTube
    python vod_downloader.py --update --download # Update and download only
"""

import argparse
import configparser
import csv
import http.server
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.parse
import webbrowser
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

try:
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
    YOUTUBE_AVAILABLE = True
except ImportError:
    YOUTUBE_AVAILABLE = False

# ===========================================================================
# Constants
# ===========================================================================

MAX_VOD_SECONDS = 11 * 3600  # 11 hours
OVERLAP_SECONDS = 60
YT_BAD_CHARS = re.compile(r'[<>\\|$%^#@]')
MAX_TITLE_LEN = 100

TWITCH_API = "https://api.twitch.tv/helix"
TWITCH_AUTH_TOKEN = "https://id.twitch.tv/oauth2/token"
TWITCH_AUTH_AUTHORIZE = "https://id.twitch.tv/oauth2/authorize"
TWITCH_REDIRECT_PORT = 17563  # fixed port; must match Twitch app's redirect URI
TWITCH_USER_TOKEN_FILE = "twitch_token.json"
YT_SCOPES = ["https://www.googleapis.com/auth/youtube"]

NOBLEJURY_URL = "https://archive.nova.noblejury.com/mc/"
# Estimation: 11 hours of stream ≈ 9.5 GiB
BYTES_PER_SECOND_ESTIMATE = 9.5 * 1024**3 / (11 * 3600)

MAX_RETRIES = 5
BACKOFF = 2
TIMEOUT = 30

CSV_FIELDS = [
    "download_status",
    "upload_status",
    "stream_datetime",
    "muted_segments",
    "deleted",
    "streamer_name",
    "stream_title",
    "stream_url",
    "noblejury_url",
    "stream_duration",
    "vod_section",
    "vod_filename",
    "noblejury_filename",
    "vod_title",
    "vod_description",
    "vod_url",
]

# ===========================================================================
# Utilities
# ===========================================================================


def parse_duration(dur):
    """Parse Twitch duration string like '3h8m33s' to total seconds."""
    m = re.match(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$", dur)
    if not m:
        return 0
    return int(m[1] or 0) * 3600 + int(m[2] or 0) * 60 + int(m[3] or 0)


def to_hhmmss(secs):
    """Convert integer seconds to HH:MM:SS."""
    secs = int(secs)
    return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}:{secs % 60:02d}"


def calc_sections(total):
    """
    Split a VOD into sections if longer than 11 hours.

    Returns a list of "HH:MM:SS-HH:MM:SS" / "HH:MM:SS-inf" strings,
    or [None] if no splitting is needed.

    Sections are roughly equal length with ~1 minute of overlap.
    Example: 11:25:20 -> ["00:00:00-05:43:10", "05:42:10-inf"]
    """
    if total <= MAX_VOD_SECONDS:
        return [None]

    n = math.ceil(total / MAX_VOD_SECONDS)
    base = total / n
    half = OVERLAP_SECONDS / 2
    sections = []
    for i in range(n):
        start = to_hhmmss(max(0, i * base - half))
        end = "inf" if i == n - 1 else to_hhmmss((i + 1) * base + half)
        sections.append(f"{start}-{end}")
    return sections


def sanitize(text):
    """Strip YouTube-disallowed characters."""
    return YT_BAD_CHARS.sub("", text)


def make_filename(date, name, order, day_total, part=None, parts=None):
    """
    Build VOD filename.
    Format: YYYY-MM-DD - name[ - #][ - part_#_of_#].mp4
    The day-order number is only included when there are multiple streams
    by the same streamer on the same day.
    """
    s = f"{date} - {name}"
    if day_total > 1:
        s += f" - {order}"
    if part is not None and parts is not None and parts > 1:
        s += f" - part_{part}_of_{parts}"
    return s + ".mp4"


def make_title(date, name, order, day_total, part, parts, title):
    """
    Build YouTube title, max 100 chars with trailing '...' if truncated.
    Format: (YYYY/MM/DD) - name VOD[ #][(#/#)] - title
    """
    d = date.replace("-", "/")
    t = f"({d}) - {name} VOD"
    if day_total > 1:
        t += f" {order}"
    if part is not None and parts is not None and parts > 1:
        t += f"({part}/{parts})"
    t += f" - {title}"
    t = sanitize(t)
    if len(t) > MAX_TITLE_LEN:
        t = t[: MAX_TITLE_LEN - 3] + "..."
    return t


def make_description(name, dt, title, part=None, parts=None):
    """Build YouTube description."""
    d = f"{dt.strftime('%B')} {dt.day}, {dt.year}"
    desc = f"Originally streamed on https://www.twitch.tv/{name} on {d}."
    if part and parts and parts > 1:
        desc += f" This is part {part} of {parts} of the VOD."
    desc += f' The stream was originally titled "{title}"'
    desc += (
        "\n\nThis is not my VOD; I am posting it here for archival purposes. "
        "This account is not monetized. If a content creator would like me to "
        "stop uploading their VODs or otherwise credit them differently, "
        "please feel free to contact me."
    )
    return sanitize(desc)


def make_capture_filename(date, name, order, day_total, part=None, parts=None):
    """
    Build capture VOD filename.
    Format: YYYY-MM-DD - name[ - #][ - part_#_of_#] - capture.mp4
    """
    s = f"{date} - {name}"
    if day_total > 1:
        s += f" - {order}"
    if part is not None and parts is not None and parts > 1:
        s += f" - part_{part}_of_{parts}"
    return s + " - capture.mp4"


def make_capture_description(name, dt, title, part=None, parts=None,
                             is_deleted=False):
    """Build YouTube description for a NobleJury capture."""
    d = f"{dt.strftime('%B')} {dt.day}, {dt.year}"
    desc = f"Originally streamed on https://www.twitch.tv/{name} on {d}."
    if is_deleted:
        desc += " This stream was deleted from Twitch; this is a live capture."
    else:
        desc += " This is an unmuted live capture of the stream."
    if part and parts and parts > 1:
        desc += f" This is part {part} of {parts}."
    desc += f' The stream was originally titled "{title}"'
    desc += (
        "\n\nThis is not my VOD; I am posting it here for archival purposes. "
        "This account is not monetized. If a content creator would like me to "
        "stop uploading their VODs or otherwise credit them differently, "
        "please feel free to contact me."
    )
    return sanitize(desc)


def _parse_size(s):
    """Parse a human-readable file size like '2.4 GiB' to bytes."""
    m = re.match(r"([\d.]+)\s*(GiB|MiB|KiB|B)", s.strip())
    if not m:
        return 0
    val = float(m.group(1))
    mult = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}
    return int(val * mult.get(m.group(2), 1))


def _has_muting(muted_str):
    """Check if a muted_segments CSV value indicates muting."""
    if not muted_str or muted_str == "No":
        return False
    return True  # "Yes" or JSON data


def _parse_muted_segments(muted_str):
    """
    Parse the muted_segments CSV field.
    Returns list of {offset, duration} dicts, empty list if "Yes" (no detail),
    or None if no muting / unknown.
    """
    if not muted_str or muted_str == "No":
        return None
    if muted_str == "Yes":
        return []  # muted but detail unavailable
    try:
        return json.loads(muted_str)
    except (json.JSONDecodeError, TypeError):
        return None


def _parts_with_muting(sections, muted_data):
    """
    Return list of 1-based part indices whose time ranges overlap
    with muted segments. If muted_data is empty (detail unavailable),
    returns all part indices.
    """
    if not muted_data:
        # No detail — assume all parts are muted
        return list(range(1, len(sections) + 1))

    def _hms_s(hms):
        p = hms.split(":")
        return int(p[0]) * 3600 + int(p[1]) * 60 + int(p[2])

    result = []
    for i, sec in enumerate(sections):
        if sec is None:
            sec_start, sec_end = 0, float("inf")
        else:
            parts = sec.split("-")
            sec_start = _hms_s(parts[0])
            sec_end = float("inf") if parts[1] == "inf" else _hms_s(parts[1])
        for seg in muted_data:
            seg_start = seg["offset"]
            seg_end = seg["offset"] + seg["duration"]
            if seg_start < sec_end and seg_end > sec_start:
                result.append(i + 1)
                break
    return result


def _progress_bar(fraction, width=30):
    """Render a text progress bar like [████████░░░░░░░░]."""
    filled = int(width * fraction)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def _format_eta(seconds):
    """Format seconds as a human-readable ETA string."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h}h {m}m"


def api_get(url, max_retries=MAX_RETRIES, **kwargs):
    """GET with retry + exponential backoff."""
    kwargs.setdefault("timeout", TIMEOUT)
    for attempt in range(max_retries):
        try:
            r = requests.get(url, **kwargs)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", BACKOFF ** attempt))
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                raise requests.exceptions.HTTPError(f"Server error {r.status_code}")
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            wait = BACKOFF ** attempt
            print(f"  Request error ({e}), retrying in {wait}s...")
            time.sleep(wait)


def api_post(url, max_retries=MAX_RETRIES, **kwargs):
    """POST with retry + exponential backoff."""
    kwargs.setdefault("timeout", TIMEOUT)
    for attempt in range(max_retries):
        try:
            r = requests.post(url, **kwargs)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", BACKOFF ** attempt))
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                raise requests.exceptions.HTTPError(f"Server error {r.status_code}")
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            wait = BACKOFF ** attempt
            print(f"  Request error ({e}), retrying in {wait}s...")
            time.sleep(wait)


# ===========================================================================
# NobleJury Archive
# ===========================================================================


def fetch_noblejury_archive(streamer_names):
    """
    Fetch and parse the NobleJury MCYT archive listing.
    Returns a list of dicts: {username, start_utc, size_bytes, url, filename}
    Only returns entries matching one of the given streamer_names.

    Handles server being slow/unresponsive with generous timeouts and retries.
    """
    lower_names = {n.lower() for n in streamer_names}
    print("  Fetching NobleJury archive listing...")
    try:
        r = api_get(NOBLEJURY_URL, max_retries=3, timeout=60)
    except Exception as e:
        print(f"  Warning: could not fetch NobleJury archive: {e}")
        return []

    entries = []
    # Each row: <a ... title="name-YYYY-MM-DDTHH:MM±HH:MM.ts">...</a>
    #           <td class="size">2.4 GiB</td>
    for m in re.finditer(
        r'title="([^"]+\.ts)"[^<]*</a></td>\s*<td class="size">([^<]+)</td>',
        r.text,
    ):
        filename = m.group(1)
        size_str = m.group(2)

        # Parse: streamername-YYYY-MM-DDTHH:MM±HH:MM.ts
        fm = re.match(
            r"(.+)-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}[+-]\d{2}:\d{2})\.ts$",
            filename,
        )
        if not fm:
            continue

        username = fm.group(1).lower()
        if username not in lower_names:
            continue

        try:
            dt = datetime.fromisoformat(fm.group(2))
            dt_utc = dt.astimezone(timezone.utc)
        except ValueError:
            continue

        url = NOBLEJURY_URL + urllib.parse.quote(filename)
        entries.append({
            "username": username,
            "start_utc": dt_utc,
            "size_bytes": _parse_size(size_str),
            "url": url,
            "filename": filename,
        })

    if entries:
        print(f"  Found {len(entries)} relevant capture(s) on NobleJury.")
    else:
        print(f"  No matching captures on NobleJury.")
    return entries


# ===========================================================================
# Config
# ===========================================================================


def load_config(path="config.ini"):
    if not os.path.exists(path):
        print(f"Error: Config file '{path}' not found.")
        print("Copy config.example.ini to config.ini and fill in your credentials.")
        sys.exit(1)
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


def load_channels(path="channels.json"):
    if not os.path.exists(path):
        print(f"Error: Channels file '{path}' not found.")
        print("Copy channels.example.json to channels.json and configure it.")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ===========================================================================
# Twitch API
# ===========================================================================


class _TwitchOAuthHandler(http.server.BaseHTTPRequestHandler):
    """Tiny HTTP handler to catch the Twitch OAuth redirect."""

    def do_GET(self):
        qs = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(qs)
        self.server.auth_code = params.get("code", [None])[0]
        self.server.auth_error = params.get("error_description",
                                            params.get("error", [None]))[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        if self.server.auth_code:
            self.wfile.write(
                b"<h1>Twitch authorized!</h1><p>You can close this tab.</p>"
            )
        else:
            msg = self.server.auth_error or "Unknown error"
            self.wfile.write(f"<h1>Error: {msg}</h1>".encode())

    def log_message(self, *_args):
        pass  # suppress console noise


class TwitchAPI:
    """
    Twitch Helix API client.

    Supports two auth modes:
    - App token (client credentials): always works, but muted_segments
      data is unavailable due to a long-standing Twitch API bug.
    - User token (OAuth Authorization Code flow): returns real
      muted_segments data. Requires a one-time browser authorization
      via --twitch-auth.

    If a saved user token exists (twitch_token.json), it is loaded and
    refreshed automatically. Otherwise falls back to an app token.
    """

    def __init__(self, client_id, client_secret, user_auth=False,
                 open_browser=True):
        self.cid = client_id
        self.secret = client_secret
        self.token = None
        self.has_user_token = False
        self.open_browser = open_browser

        if user_auth:
            # Explicit --twitch-auth: always (re)do the browser flow
            self._do_user_auth()
        elif self._try_load_user_token():
            pass  # loaded & refreshed user token
        else:
            self._app_auth()

    # ---- App token (client credentials) -----------------------------------

    def _app_auth(self):
        print("Authenticating with Twitch (app token)...")
        r = api_post(
            TWITCH_AUTH_TOKEN,
            params={
                "client_id": self.cid,
                "client_secret": self.secret,
                "grant_type": "client_credentials",
            },
        )
        self.token = r.json()["access_token"]
        self.has_user_token = False
        print("  Twitch auth OK (app token).")
        print("  Note: muted segment data is unavailable with app tokens.")
        print("  Run with --twitch-auth once to enable it via user OAuth.")

    # ---- User token (authorization code flow) -----------------------------

    def _try_load_user_token(self):
        """Load saved user token from disk; refresh if expired."""
        if not os.path.exists(TWITCH_USER_TOKEN_FILE):
            return False
        try:
            with open(TWITCH_USER_TOKEN_FILE, "r") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return False

        # If expired, try refreshing
        if time.time() >= data.get("expires_at", 0):
            rt = data.get("refresh_token")
            if not rt:
                return False
            return self._refresh_user_token(rt)

        self.token = data["access_token"]
        self.has_user_token = True
        print("Authenticating with Twitch (saved user token)...")
        print("  Twitch auth OK (user token — muted segments available).")
        return True

    def _refresh_user_token(self, refresh_token):
        """Refresh an expired user token."""
        print("Refreshing Twitch user token...")
        try:
            r = api_post(
                TWITCH_AUTH_TOKEN,
                data={
                    "client_id": self.cid,
                    "client_secret": self.secret,
                    "grant_type": "refresh_token",
                    "refresh_token": urllib.parse.quote(refresh_token,
                                                       safe=""),
                },
                headers={"Content-Type":
                         "application/x-www-form-urlencoded"},
            )
            tok = r.json()
            if "access_token" not in tok:
                print(f"  Refresh failed: {tok}")
                return False
            self._save_user_token(tok)
            self.token = tok["access_token"]
            self.has_user_token = True
            print("  Twitch user token refreshed OK.")
            return True
        except Exception as e:
            print(f"  Failed to refresh Twitch user token: {e}")
            return False

    def _save_user_token(self, tok):
        """Persist user token to disk."""
        save = {
            "access_token": tok["access_token"],
            "refresh_token": tok["refresh_token"],
            "expires_at": time.time() + tok.get("expires_in", 3600) - 300,
        }
        with open(TWITCH_USER_TOKEN_FILE, "w") as f:
            json.dump(save, f, indent=2)

    def _do_user_auth(self):
        """Run the OAuth Authorization Code flow in a browser."""
        print("Authenticating with Twitch (user OAuth)...")

        # Use a fixed port — Twitch requires the redirect URI to match
        # exactly what's registered in the app settings.
        redirect_uri = f"http://localhost:{TWITCH_REDIRECT_PORT}"

        try:
            srv = http.server.HTTPServer(
                ("localhost", TWITCH_REDIRECT_PORT), _TwitchOAuthHandler
            )
        except OSError as e:
            print(f"  Error: could not listen on port {TWITCH_REDIRECT_PORT}: {e}")
            print("  Is another instance of this script already running?")
            print("  Falling back to app token.")
            self._app_auth()
            return

        auth_url = (
            f"{TWITCH_AUTH_AUTHORIZE}"
            f"?client_id={self.cid}"
            f"&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
            f"&response_type=code"
            f"&scope="
            f"&force_verify=true"
        )

        if self.open_browser:
            print("  A browser window will open for Twitch authorization.")
            print("  (Use --no-browser to get a URL to open manually.)")
            webbrowser.open(auth_url)
        else:
            print("  Open this URL in your browser to authorize:")
            print(f"  {auth_url}")

        try:
            srv.handle_request()
        except KeyboardInterrupt:
            print("\n  Authorization cancelled.")
            sys.exit(1)
        finally:
            srv.server_close()

        code = getattr(srv, "auth_code", None)
        error = getattr(srv, "auth_error", None)

        if not code:
            if error and "redirect_mismatch" in str(error):
                print(f"  Redirect URI mismatch!")
                print(f"  Your Twitch app's OAuth Redirect URL must include:")
                print(f"    {redirect_uri}")
                print(f"  Add it at https://dev.twitch.tv/console under your app's settings.")
            else:
                print(f"  Authorization failed: {error or 'no code received'}")
            print("  Falling back to app token.")
            self._app_auth()
            return

        # Exchange code for token
        try:
            r = api_post(
                TWITCH_AUTH_TOKEN,
                data={
                    "client_id": self.cid,
                    "client_secret": self.secret,
                    "code": code,
                    "grant_type": "authorization_code",
                    "redirect_uri": redirect_uri,
                },
                headers={"Content-Type":
                         "application/x-www-form-urlencoded"},
            )
            tok = r.json()
            if "access_token" not in tok:
                print(f"  Token exchange failed: {tok}")
                print("  Falling back to app token.")
                self._app_auth()
                return
            self._save_user_token(tok)
            self.token = tok["access_token"]
            self.has_user_token = True
            print("  Twitch user auth OK (muted segments available).")
        except Exception as e:
            print(f"  Token exchange error: {e}")
            print("  Falling back to app token.")
            self._app_auth()

    # ---- API calls --------------------------------------------------------

    def _headers(self):
        return {
            "Client-ID": self.cid,
            "Authorization": f"Bearer {self.token}",
        }

    def _get_page(self, user_id, cursor=None):
        """Fetch one page of archived videos for a user."""
        params = {
            "user_id": user_id,
            "type": "archive",
            "first": 100,
            "sort": "time",
        }
        if cursor:
            params["after"] = cursor
        r = api_get(f"{TWITCH_API}/videos", headers=self._headers(), params=params)
        data = r.json()
        return data.get("data", []), data.get("pagination", {}).get("cursor")

    def get_new_videos(self, user_id, known_urls):
        """
        Fetch new archived VODs for a user.
        Paginates newest-first, stops when it finds a known URL.
        Returns list in ascending (oldest-first) order.
        """
        result = []
        cursor = None
        while True:
            vids, cursor = self._get_page(user_id, cursor)
            if not vids:
                break
            stop = False
            for v in vids:
                if v["url"] in known_urls:
                    stop = True
                    break
                result.append(v)
            if stop or not cursor:
                break
        result.reverse()
        return result


# ===========================================================================
# CSV Tracker
# ===========================================================================


class VODTracker:
    """Manages a per-folder CSV file that tracks VOD status."""

    def __init__(self, output_folder):
        self.folder = Path(output_folder)
        self.name = self.folder.name
        self.csv_path = self.folder / f"{self.name} vod tracker.csv"
        self.rows = []
        self._load()

    def _load(self):
        if not self.csv_path.exists():
            return
        with open(self.csv_path, "r", encoding="utf-8", newline="") as f:
            self.rows = list(csv.DictReader(f))
        # Backward compat: ensure all current fields exist in old CSVs
        for row in self.rows:
            for field in CSV_FIELDS:
                row.setdefault(field, "")

    def save(self):
        self.folder.mkdir(parents=True, exist_ok=True)
        with open(self.csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, CSV_FIELDS, quoting=csv.QUOTE_ALL)
            w.writeheader()
            w.writerows(self.rows)

    def known_urls(self):
        """Set of all Twitch stream URLs already in the tracker."""
        return {r["stream_url"] for r in self.rows if r.get("stream_url")}

    def known_noblejury_urls(self):
        """Set of all NobleJury URLs already in the tracker."""
        return {r["noblejury_url"] for r in self.rows if r.get("noblejury_url")}

    # ---- Stream identity helpers ------------------------------------------

    @staticmethod
    def _stream_id(r):
        """
        Unique stream identifier for day-order counting.
        Deleted captures (no Twitch URL) use their noblejury_url.
        Muted captures share the Twitch stream's identity.
        """
        if r.get("deleted") == "Yes":
            return "nj:" + r.get("noblejury_url", "")
        return r.get("stream_url", "")

    @staticmethod
    def _part_group_key(r):
        """
        Key for grouping rows that are parts of the same download.
        Capture rows are grouped by noblejury_url (separate from Twitch parts).
        """
        if r.get("noblejury_url"):
            return "nj:" + r["noblejury_url"]
        return r.get("stream_url", "")

    # ---- Adding new videos ------------------------------------------------

    def add_videos(self, videos, streamer, muted_reliable=True):
        """
        Add new Twitch VODs to the tracker.
        Handles >11h splitting into parts and day-order numbering.

        muted_reliable: if False (app token), muted_segments is left blank
        because the Twitch API always returns null with app tokens.
        Returns the list of newly added row dicts.
        """
        if not videos:
            return []

        new_rows = []
        for v in videos:
            dt = datetime.fromisoformat(v["created_at"].replace("Z", "+00:00"))
            dur = parse_duration(v["duration"])
            if muted_reliable:
                segs = v.get("muted_segments")
                muted = json.dumps(segs) if segs else "No"
            else:
                muted = ""  # unknown — app token can't report this
            sections = calc_sections(dur)
            n_parts = len(sections) if sections[0] is not None else 1

            for i, sec in enumerate(sections):
                p = (i + 1) if sec is not None else None
                tp = n_parts if sec is not None else None
                new_rows.append(
                    {
                        "download_status": "",
                        "upload_status": "",
                        "stream_datetime": v["created_at"],
                        "muted_segments": muted,
                        "deleted": "",
                        "streamer_name": streamer,
                        "stream_title": v["title"],
                        "stream_url": v["url"],
                        "noblejury_url": "",
                        "stream_duration": v["duration"],
                        "vod_section": sec or "",
                        "vod_filename": "",  # set by _recalc
                        "noblejury_filename": "",
                        "vod_title": "",  # set by _recalc
                        "vod_description": make_description(
                            streamer, dt, v["title"], p, tp
                        ),
                        "vod_url": "",
                    }
                )

        self.rows.extend(new_rows)
        self._recalc()
        self.save()
        return new_rows

    def add_deleted_capture(self, capture, streamer):
        """
        Add rows for a deleted VOD discovered via NobleJury capture.
        Estimates duration from file size; sections are filled during download.
        """
        dt = capture["start_utc"]
        est_duration = capture["size_bytes"] / BYTES_PER_SECOND_ESTIMATE
        n_parts = max(1, math.ceil(est_duration / MAX_VOD_SECONDS))
        title = "Captured Deleted Stream"

        new_rows = []
        for i in range(n_parts):
            p = (i + 1) if n_parts > 1 else None
            tp = n_parts if n_parts > 1 else None
            new_rows.append({
                "download_status": "",
                "upload_status": "",
                "stream_datetime": dt.isoformat(),
                "muted_segments": "",
                "deleted": "Yes",
                "streamer_name": streamer,
                "stream_title": title,
                "stream_url": "",
                "noblejury_url": capture["url"],
                "stream_duration": "",
                "vod_section": "",  # filled during download
                "vod_filename": "",  # set by _recalc
                "noblejury_filename": "",  # set by _recalc
                "vod_title": "",  # set by _recalc
                "vod_description": make_capture_description(
                    streamer, dt, title, p, tp, is_deleted=True
                ),
                "vod_url": "",
            })

        self.rows.extend(new_rows)
        self._recalc()
        self.save()
        print(f"    Added deleted capture: {capture['filename']} "
              f"({n_parts} part(s))")
        return new_rows

    def add_muted_captures(self, capture, streamer, stream_url,
                           twitch_video_data):
        """
        Add capture rows for a muted VOD that has a NobleJury capture.
        Only adds rows for parts that actually contain muted segments.
        """
        muted_data = _parse_muted_segments(
            twitch_video_data.get("_muted_str", "")
        )
        dur = parse_duration(twitch_video_data["duration"])
        sections = calc_sections(dur)
        muted_parts = _parts_with_muting(sections, muted_data)

        if not muted_parts:
            return []

        dt = datetime.fromisoformat(
            twitch_video_data["created_at"].replace("Z", "+00:00")
        )
        title = twitch_video_data["title"]

        new_rows = []
        n_cap_parts = len(muted_parts)
        for cap_i, _part_num in enumerate(muted_parts):
            p = (cap_i + 1) if n_cap_parts > 1 else None
            tp = n_cap_parts if n_cap_parts > 1 else None
            new_rows.append({
                "download_status": "",
                "upload_status": "",
                "stream_datetime": twitch_video_data["created_at"],
                "muted_segments": "",
                "deleted": "",
                "streamer_name": streamer,
                "stream_title": title,
                "stream_url": stream_url,
                "noblejury_url": capture["url"],
                "stream_duration": twitch_video_data["duration"],
                "vod_section": "",  # filled during download
                "vod_filename": "",  # set by _recalc
                "noblejury_filename": "",  # set by _recalc
                "vod_title": "",  # set by _recalc
                "vod_description": make_capture_description(
                    streamer, dt, title, p, tp, is_deleted=False
                ),
                "vod_url": "",
            })

        self.rows.extend(new_rows)
        self._recalc()
        self.save()
        print(f"    Added muted capture: {capture['filename']} "
              f"({n_cap_parts} part(s) for {len(muted_parts)} muted section(s))")
        return new_rows

    def update_capture_parts(self, noblejury_url, actual_duration_s):
        """
        After probing a capture's actual duration, adjust the number of
        rows and fill in vod_sections.  Returns updated row indices.
        """
        indices = [i for i, r in enumerate(self.rows)
                   if r.get("noblejury_url") == noblejury_url]
        if not indices:
            return []

        sections = calc_sections(actual_duration_s)
        needed = len(sections)
        current = len(indices)

        template = dict(self.rows[indices[0]])

        if needed > current:
            for i in range(needed - current):
                new_row = dict(template)
                new_row["vod_section"] = ""
                new_row["download_status"] = ""
                new_row["upload_status"] = ""
                new_row["vod_url"] = ""
                self.rows.insert(indices[-1] + 1 + i, new_row)
        elif needed < current:
            for i in range(current - needed):
                idx = indices[current - 1 - i]
                del self.rows[idx]

        # Re-find indices after insertions/deletions
        indices = [i for i, r in enumerate(self.rows)
                   if r.get("noblejury_url") == noblejury_url]

        # Fill in sections and duration
        h = int(actual_duration_s // 3600)
        m = int(actual_duration_s % 3600 // 60)
        s = int(actual_duration_s % 60)
        dur_str = f"{h}h{m}m{s}s"
        for i, idx in enumerate(indices):
            self.rows[idx]["vod_section"] = sections[i] or ""
            self.rows[idx]["stream_duration"] = dur_str

        self._recalc()
        self.save()
        return indices

    def get_time_ranges(self, streamer):
        """
        Return list of (start_utc, end_utc) for all known Twitch VODs
        by a streamer. Used to detect deleted VODs.
        """
        ranges = []
        seen_urls = set()
        for r in self.rows:
            url = r.get("stream_url", "")
            if (r["streamer_name"].lower() != streamer.lower()
                    or not url or url in seen_urls):
                continue
            seen_urls.add(url)
            try:
                start = datetime.fromisoformat(
                    r["stream_datetime"].replace("Z", "+00:00")
                )
            except ValueError:
                continue
            dur = parse_duration(r.get("stream_duration", ""))
            if dur <= 0:
                dur = MAX_VOD_SECONDS  # fallback
            end = start + timedelta(seconds=dur)
            ranges.append((start, end))
        return ranges

    def _recalc(self):
        """
        Recalculate vod_filename, noblejury_filename, vod_title, and
        vod_description for every row.  Handles day-order numbering,
        part numbering, and capture vs normal filename formats.
        Renames already-downloaded files on disk if needed.
        """
        # 1. Build day-order map using stream identity.
        #    Muted captures share their Twitch stream's identity.
        #    Deleted captures use their noblejury_url as identity.
        day_map = defaultdict(dict)  # (streamer, date) -> {stream_id: dt_str}
        for r in self.rows:
            key = (r["streamer_name"], r["stream_datetime"][:10])
            sid = self._stream_id(r)
            if sid not in day_map[key]:
                day_map[key][sid] = r["stream_datetime"]

        order_map = {}  # (streamer, date, stream_id) -> (order, day_total)
        for (streamer, date), id_times in day_map.items():
            sorted_ids = [
                s for s, _ in sorted(id_times.items(), key=lambda x: x[1])
            ]
            total = len(sorted_ids)
            for idx, sid in enumerate(sorted_ids):
                order_map[(streamer, date, sid)] = (idx + 1, total)

        # 2. Group row indices by part_group_key for part ordering.
        part_groups = defaultdict(list)
        for i, r in enumerate(self.rows):
            part_groups[self._part_group_key(r)].append(i)

        # 3. Update each row.
        for _pgk, indices in part_groups.items():
            n_parts = len(indices)
            for part_i, row_i in enumerate(indices):
                r = self.rows[row_i]
                streamer = r["streamer_name"]
                date = r["stream_datetime"][:10]
                sid = self._stream_id(r)
                order, day_total = order_map[(streamer, date, sid)]

                p = (part_i + 1) if n_parts > 1 else None
                tp = n_parts if n_parts > 1 else None
                is_capture = bool(r.get("noblejury_url"))

                old_fn = r["vod_filename"]
                if is_capture:
                    new_fn = make_capture_filename(
                        date, streamer, order, day_total, p, tp)
                else:
                    new_fn = make_filename(
                        date, streamer, order, day_total, p, tp)

                # Rename file on disk if it was already downloaded
                if old_fn and old_fn != new_fn:
                    old_path = self.folder / old_fn
                    new_path = self.folder / new_fn
                    if old_path.exists() and not new_path.exists():
                        old_path.rename(new_path)
                        print(f"  Renamed: {old_fn} -> {new_fn}")

                r["vod_filename"] = new_fn
                r["noblejury_filename"] = new_fn if is_capture else ""

                # Title — add "(unmuted capture)" for non-deleted captures
                display_title = r["stream_title"]
                if is_capture and r.get("deleted") != "Yes":
                    display_title += " (unmuted capture)"
                r["vod_title"] = make_title(
                    date, streamer, order, day_total, p, tp, display_title
                )

                # Description
                dt = datetime.fromisoformat(
                    r["stream_datetime"].replace("Z", "+00:00")
                )
                if is_capture:
                    r["vod_description"] = make_capture_description(
                        streamer, dt, r["stream_title"], p, tp,
                        is_deleted=(r.get("deleted") == "Yes"),
                    )
                else:
                    r["vod_description"] = make_description(
                        streamer, dt, r["stream_title"], p, tp
                    )

    # ---- Field updates ----------------------------------------------------

    def set_field(self, idx, field, value):
        """Update one field in a row and save immediately."""
        self.rows[idx][field] = value
        self.save()


# ===========================================================================
# Downloader (yt-dlp)
# ===========================================================================


def _section_target_seconds(section):
    """Return the target duration in seconds for a section string, or None."""
    if not section:
        return None
    parts = section.split("-")
    if len(parts) != 2 or parts[1] == "inf":
        return None
    def _hms(s):
        p = s.split(":")
        return int(p[0]) * 3600 + int(p[1]) * 60 + int(p[2])
    return _hms(parts[1]) - _hms(parts[0])


def download_vod(ytdlp, url, output_path, section=None, best_quality=False):
    """
    Download a VOD (or a section of one) using yt-dlp.
    Returns True on success.

    By default downloads at 720p max height with best fps.
    Pass best_quality=True for highest available resolution.
    """
    cmd = [ytdlp, "-o", str(output_path), "--merge-output-format", "mp4",
           "--newline"]
    if not best_quality:
        cmd += ["-S", "height:720,fps"]
    if section:
        cmd += ["--download-sections", f"*{section}"]
    cmd.append(url)

    print(f"  Command: {' '.join(cmd)}")
    start_time = time.time()
    target_s = _section_target_seconds(section)
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        output_lines = []
        showed_bar = False
        for line in proc.stdout:
            line = line.rstrip()
            output_lines.append(line)

            # --- HLS fragment progress (no --download-sections) ---
            # [download]  0.3% of ~  97.55GiB at  38.76MiB/s ETA 39:28 (frag 12/4220)
            m = re.search(
                r"\[download\]\s+([\d.]+)%.*?at\s+(\S+)\s+ETA\s+(\S+)", line
            )
            if m:
                pct = float(m.group(1)) / 100.0
                speed, eta = m.group(2), m.group(3)
                bar = _progress_bar(pct)
                eta_part = f"ETA: {eta}" if eta != "Unknown" else ""
                print(f"\r    {bar} {int(pct * 100):3d}%  "
                      f"at {speed}  {eta_part}       ",
                      end="", flush=True)
                showed_bar = True
                continue

            # [download] 100% of ...
            if re.search(r"\[download\]\s+100%", line):
                elapsed = time.time() - start_time
                print(f"\r    {_progress_bar(1.0)} 100%  "
                      f"({_format_eta(elapsed)} elapsed)          ",
                      flush=True)
                showed_bar = False  # already printed newline via flush
                continue

            # --- ffmpeg progress (with --download-sections) ---
            # frame= 2998 ... time=00:00:49.94 ... speed=98.4x
            m = re.search(
                r"time=(\d+):(\d+):([\d.]+).*?speed=\s*([\d.]+)x", line
            )
            if m:
                cur_s = (int(m.group(1)) * 3600 + int(m.group(2)) * 60
                         + float(m.group(3)))
                speed_x = float(m.group(4))
                if target_s and target_s > 0:
                    pct = min(cur_s / target_s, 1.0)
                    remaining = ((target_s - cur_s) / max(speed_x, 0.01))
                    bar = _progress_bar(pct)
                    print(f"\r    {bar} {int(pct * 100):3d}%  "
                          f"at {m.group(4)}x  "
                          f"ETA: {_format_eta(remaining)}       ",
                          end="", flush=True)
                else:
                    # Unknown target (section ends with "inf")
                    elapsed = time.time() - start_time
                    print(f"\r    Downloading... "
                          f"{_format_eta(cur_s)} processed  "
                          f"at {m.group(4)}x  "
                          f"({_format_eta(elapsed)} elapsed)       ",
                          end="", flush=True)
                showed_bar = True
                continue

        proc.wait(timeout=14400)

        if showed_bar:
            elapsed = time.time() - start_time
            if proc.returncode == 0:
                print(f"\r    {_progress_bar(1.0)} 100%  "
                      f"({_format_eta(elapsed)} elapsed)          ")
            else:
                print()  # newline after progress bar

        if proc.returncode != 0:
            err_text = "\n".join(output_lines[-20:])
            print(f"  yt-dlp error output:\n{err_text[:2000]}")
            return False
        return True
    except subprocess.TimeoutExpired:
        proc.kill()
        print("\n  yt-dlp timed out.")
        return False
    except FileNotFoundError:
        print(f"  Error: yt-dlp not found at '{ytdlp}'.")
        print("  Set ytdlp_path in config.ini or add yt-dlp to your PATH.")
        return False


# ===========================================================================
# NobleJury Capture Download / Convert
# ===========================================================================


def _find_tool(ytdlp_path, name):
    """Find ffmpeg/ffprobe next to yt-dlp or in PATH."""
    ytdlp_dir = Path(ytdlp_path).parent
    for suffix in [name, f"{name}.exe"]:
        candidate = ytdlp_dir / suffix
        if candidate.exists():
            return str(candidate)
    return name  # hope it's in PATH


def probe_duration(filepath, ytdlp_path="ffprobe"):
    """Get video duration in seconds using ffprobe. Returns float or None."""
    ffprobe = _find_tool(ytdlp_path, "ffprobe")
    cmd = [
        ffprobe, "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(filepath),
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        pass
    return None


def download_capture_file(url, output_path):
    """
    Download a file from a URL (NobleJury) with progress bar and retry.
    Uses generous timeouts since the server can be slow.
    """
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, stream=True, timeout=(30, 300))
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))

            downloaded = 0
            start = time.time()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    elapsed = time.time() - start
                    if total > 0 and elapsed > 0:
                        pct = downloaded / total
                        speed = downloaded / elapsed
                        if pct > 0.001:
                            eta = elapsed / pct * (1 - pct)
                        else:
                            eta = 0
                        bar = _progress_bar(pct)
                        speed_str = f"{speed / 1024 / 1024:.1f} MiB/s"
                        print(
                            f"\r    {bar} {int(pct * 100):3d}%  "
                            f"at {speed_str}  ETA: {_format_eta(eta)}       ",
                            end="", flush=True,
                        )

            elapsed = time.time() - start
            print(f"\r    {_progress_bar(1.0)} 100%  "
                  f"({_format_eta(elapsed)} elapsed)          ")
            return True
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = BACKOFF ** (attempt + 1)
                print(f"\n    Download error ({e}), retrying in {wait}s...")
                time.sleep(wait)
                # Remove partial file
                if Path(output_path).exists():
                    Path(output_path).unlink()
            else:
                print(f"\n    Failed to download after {MAX_RETRIES} "
                      f"attempts: {e}")
                return False
    return False


def convert_ts_to_mp4(input_path, output_path, ytdlp_path="ffmpeg",
                      estimated_duration=None):
    """
    Convert a .ts capture to .mp4 via ffmpeg (re-encode, not stream copy).
    Shows a progress bar. Returns True on success.
    """
    ffmpeg = _find_tool(ytdlp_path, "ffmpeg")

    # Get duration for progress bar if not estimated
    if not estimated_duration:
        estimated_duration = probe_duration(input_path, ytdlp_path)

    cmd = [
        ffmpeg, "-i", str(input_path),
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        "-y",
        str(output_path),
    ]

    print(f"    ffmpeg command: {' '.join(cmd)}")
    start_time = time.time()
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        showed_bar = False
        for line in proc.stdout:
            m = re.search(
                r"time=(\d+):(\d+):([\d.]+).*?speed=\s*([\d.]+)x", line
            )
            if m and estimated_duration and estimated_duration > 0:
                cur = (int(m.group(1)) * 3600 + int(m.group(2)) * 60
                       + float(m.group(3)))
                speed_x = float(m.group(4))
                pct = min(cur / estimated_duration, 1.0)
                remaining = ((estimated_duration - cur)
                             / max(speed_x, 0.01))
                bar = _progress_bar(pct)
                print(f"\r    {bar} {int(pct * 100):3d}%  "
                      f"at {m.group(4)}x  "
                      f"ETA: {_format_eta(remaining)}       ",
                      end="", flush=True)
                showed_bar = True

        proc.wait(timeout=86400)  # 24h timeout for long encodes
        if showed_bar:
            elapsed = time.time() - start_time
            if proc.returncode == 0:
                print(f"\r    {_progress_bar(1.0)} 100%  "
                      f"({_format_eta(elapsed)} elapsed)          ")
            else:
                print()
        if proc.returncode != 0:
            print(f"    ffmpeg conversion failed (exit {proc.returncode}).")
            return False
        return True
    except subprocess.TimeoutExpired:
        proc.kill()
        print("\n    ffmpeg timed out.")
        return False
    except FileNotFoundError:
        print(f"    Error: ffmpeg not found. Install it or place it next "
              f"to yt-dlp.")
        return False


def split_capture(input_path, section, output_path, ytdlp_path="ffmpeg"):
    """Split a section from an mp4 using ffmpeg -c copy."""
    ffmpeg = _find_tool(ytdlp_path, "ffmpeg")
    parts = section.split("-")
    start = parts[0]
    end = parts[1] if len(parts) > 1 else "inf"

    cmd = [ffmpeg, "-i", str(input_path), "-ss", start]
    if end != "inf":
        cmd += ["-to", end]
    cmd += ["-c", "copy", "-y", str(output_path)]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=14400
        )
        if result.returncode != 0:
            print(f"    Split error: {result.stderr[-500:]}")
            return False
        return True
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"    Split failed: {e}")
        return False


# ===========================================================================
# YouTube Uploader
# ===========================================================================


class YouTubeUploader:
    """Handles YouTube authentication, video upload, and playlist insertion."""

    CHUNK = 10 * 1024 * 1024  # 10 MB chunks for resumable upload

    def __init__(self, client_secrets, token_file="youtube_token.json",
                 open_browser=True, reauth=False):
        if not YOUTUBE_AVAILABLE:
            print(
                "Error: YouTube API libraries not installed.\n"
                "Run: pip install google-api-python-client "
                "google-auth-oauthlib google-auth-httplib2"
            )
            sys.exit(1)
        self.secrets = client_secrets
        self.token_file = token_file
        self.open_browser = open_browser
        self.svc = None
        self.channel_id = None
        self.channel_title = None
        if reauth and os.path.exists(self.token_file):
            os.remove(self.token_file)
            print("  Removed old YouTube token; will re-authorize.")
        self._auth()

    def _auth(self):
        creds = None
        if os.path.exists(self.token_file):
            creds = Credentials.from_authorized_user_file(self.token_file, YT_SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("Refreshing YouTube token...")
                try:
                    creds.refresh(GoogleAuthRequest())
                except Exception:
                    print("  Token expired or revoked; re-authorizing...")
                    os.remove(self.token_file)
                    creds = None
            if not creds or not creds.valid:
                if not os.path.exists(self.secrets):
                    print(f"Error: client_secrets file '{self.secrets}' not found.")
                    print("See config.example.ini for YouTube setup instructions.")
                    sys.exit(1)
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.secrets, YT_SCOPES
                )
                if self.open_browser:
                    print("YouTube authorization required. A browser window will open.")
                    print("  (Use --no-browser to get a URL you can open in a specific "
                          "browser profile or container instead.)")
                else:
                    print("YouTube authorization required.")
                    print("  The auth URL will be printed below. Open it in your "
                          "preferred browser / container tab.")
                try:
                    creds = flow.run_local_server(
                        port=0, open_browser=self.open_browser
                    )
                except KeyboardInterrupt:
                    print("\n  Authorization cancelled by user.")
                    sys.exit(1)
            with open(self.token_file, "w") as f:
                f.write(creds.to_json())
        self.svc = build("youtube", "v3", credentials=creds)
        self._show_channel()

    def _show_channel(self):
        """Query and display the authenticated YouTube channel."""
        try:
            resp = self.svc.channels().list(part="snippet", mine=True).execute()
            items = resp.get("items", [])
            if items:
                self.channel_id = items[0]["id"]
                self.channel_title = items[0]["snippet"]["title"]
                print(f"  YouTube auth OK — channel: {self.channel_title} "
                      f"({self.channel_id})")
            else:
                print("  YouTube auth OK — (no channel found for this account)")
                print("  WARNING: uploads will fail without a YouTube channel.")
        except Exception as e:
            print(f"  YouTube auth OK — (could not fetch channel info: {e})")

    def upload(self, filepath, title, description, recording_date=None,
               audio_language="en", privacy="unlisted"):
        """Upload a video. Returns the YouTube video ID, or None on failure."""
        body = {
            "snippet": {
                "title": title,
                "description": description,
                "categoryId": "20",  # Gaming
                "defaultLanguage": "en",
                "defaultAudioLanguage": audio_language,
            },
            "status": {"privacyStatus": privacy},
        }
        if recording_date:
            body["recordingDetails"] = {"recordingDate": recording_date}

        parts = "snippet,status"
        if recording_date:
            parts += ",recordingDetails"

        media = MediaFileUpload(filepath, chunksize=self.CHUNK, resumable=True)
        req = self.svc.videos().insert(
            part=parts, body=body, media_body=media
        )

        # Get file size for ETA calculation
        file_size = os.path.getsize(filepath)
        size_mb = file_size / (1024 * 1024)
        print(f"  Uploading: {title} ({size_mb:.0f} MB)")

        response = None
        retries = 0
        start_time = time.time()
        while response is None:
            try:
                status, response = req.next_chunk()
                if status:
                    pct = status.progress()
                    elapsed = time.time() - start_time
                    if pct > 0:
                        eta = elapsed / pct * (1 - pct)
                        eta_str = _format_eta(eta)
                    else:
                        eta_str = "calculating..."
                    bar = _progress_bar(pct)
                    print(f"\r    {bar} {int(pct * 100):3d}%  ETA: {eta_str}  ",
                          end="", flush=True)
                retries = 0
            except HttpError as e:
                if e.resp.status in (500, 502, 503, 504) and retries < MAX_RETRIES:
                    retries += 1
                    wait = BACKOFF ** retries
                    print(f"\n  Upload error ({e.resp.status}), retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print()  # newline after progress bar
                    raise
            except KeyboardInterrupt:
                print("\n  Upload interrupted by user.")
                return None
            except Exception as e:
                if retries < MAX_RETRIES:
                    retries += 1
                    wait = BACKOFF ** retries
                    print(f"\n  Upload error ({e}), retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print()  # newline after progress bar
                    raise

        elapsed = time.time() - start_time
        print(f"\r    {_progress_bar(1.0)} 100%  ({_format_eta(elapsed)} elapsed)  ")
        vid_id = response["id"]
        print(f"  Upload complete: https://youtu.be/{vid_id}")
        return vid_id

    def add_to_playlist(self, video_id, playlist_id):
        """Insert a video into a YouTube playlist."""
        body = {
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {"kind": "youtube#video", "videoId": video_id},
            }
        }
        for attempt in range(MAX_RETRIES):
            try:
                self.svc.playlistItems().insert(part="snippet", body=body).execute()
                print(f"  Added to playlist {playlist_id}")
                return True
            except HttpError as e:
                if e.resp.status in (500, 502, 503, 504) and attempt < MAX_RETRIES - 1:
                    wait = BACKOFF ** attempt
                    print(f"  Playlist error ({e.resp.status}), retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise
        return False


# ===========================================================================
# Orchestration
# ===========================================================================


def group_by_folder(channels):
    """Group channel configs by output_folder."""
    groups = defaultdict(list)
    for ch in channels:
        groups[ch["output_folder"]].append(ch)
    return groups


def step_update(config, channels, twitch):
    """Pull new VODs from Twitch and update CSV trackers."""
    print("\n=== Updating VOD trackers ===")
    grouped = group_by_folder(channels)

    # Fetch NobleJury archive once for all channels
    all_usernames = [ch["username"] for ch in channels]
    nj_entries = fetch_noblejury_archive(all_usernames)
    # Index by lowercase username
    nj_by_user = defaultdict(list)
    for e in nj_entries:
        nj_by_user[e["username"].lower()].append(e)

    for folder, chs in grouped.items():
        print(f"\nFolder: {folder}")
        tracker = VODTracker(folder)
        known = tracker.known_urls()

        # Keep video data in memory for muted capture detection
        all_new_vids = {}  # stream_url -> video dict (with _muted_str)

        for ch in chs:
            uname = ch["username"]
            uid = ch["user_id"]
            print(f"  Checking {uname} (ID: {uid})...")

            try:
                new_vids = twitch.get_new_videos(uid, known)
            except Exception as e:
                print(f"    Error fetching videos for {uname}: {e}")
                continue

            if not new_vids:
                print(f"    No new VODs.")
            else:
                print(f"    Found {len(new_vids)} new VOD(s).")
                added = tracker.add_videos(new_vids, uname,
                                           muted_reliable=twitch.has_user_token)
                known = tracker.known_urls()

                for row in added:
                    if _has_muting(row.get("muted_segments", "")):
                        print(f"    WARNING: Muted segments in: "
                              f"{row['stream_url']}")

                # Save video data for muted capture matching
                for v in new_vids:
                    muted_str = ""
                    if twitch.has_user_token:
                        segs = v.get("muted_segments")
                        muted_str = json.dumps(segs) if segs else "No"
                    v["_muted_str"] = muted_str
                    all_new_vids[v["url"]] = v

            # --- NobleJury: detect deleted VODs ---
            captures = nj_by_user.get(uname.lower(), [])
            if captures:
                known_nj = tracker.known_noblejury_urls()
                time_ranges = tracker.get_time_ranges(uname)

                for cap in captures:
                    if cap["url"] in known_nj:
                        continue
                    cap_start = cap["start_utc"]
                    # Check if this capture falls within any known VOD
                    covered = False
                    for vod_start, vod_end in time_ranges:
                        if vod_start <= cap_start <= vod_end:
                            covered = True
                            break
                    if not covered:
                        tracker.add_deleted_capture(cap, uname)

            # --- NobleJury: detect muted VOD captures ---
            if captures:
                known_nj = tracker.known_noblejury_urls()
                time_ranges = tracker.get_time_ranges(uname)

                for row in tracker.rows:
                    if (row["streamer_name"].lower() != uname.lower()
                            or not _has_muting(row.get("muted_segments", ""))
                            or not row.get("stream_url")):
                        continue
                    # Check if capture rows already exist for this stream
                    stream_url = row["stream_url"]
                    has_capture = any(
                        r.get("noblejury_url") and r.get("stream_url") == stream_url
                        for r in tracker.rows
                    )
                    if has_capture:
                        continue

                    # Find a matching capture
                    try:
                        vod_start = datetime.fromisoformat(
                            row["stream_datetime"].replace("Z", "+00:00")
                        )
                    except ValueError:
                        continue
                    dur = parse_duration(row.get("stream_duration", ""))
                    if dur <= 0:
                        continue
                    vod_end = vod_start + timedelta(seconds=dur)

                    for cap in captures:
                        if cap["url"] in known_nj:
                            continue
                        if vod_start <= cap["start_utc"] <= vod_end:
                            # Build video data dict for muted capture
                            vid_data = all_new_vids.get(stream_url)
                            if not vid_data:
                                vid_data = {
                                    "created_at": row["stream_datetime"],
                                    "title": row["stream_title"],
                                    "duration": row["stream_duration"],
                                    "_muted_str": row.get(
                                        "muted_segments", ""),
                                }
                            tracker.add_muted_captures(
                                cap, uname, stream_url, vid_data
                            )
                            known_nj = tracker.known_noblejury_urls()
                            break

        print(f"  Tracker saved: {tracker.csv_path}")


def _clean_path(raw):
    """Strip quotes and Python raw-string prefixes from a config path value."""
    s = raw.strip()
    # Strip leading r/R before quotes: r"..." or r'...'
    if len(s) >= 3 and s[0] in "rR" and s[1] in "\"'":
        s = s[1:]
    # Strip surrounding quotes
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        s = s[1:-1]
    return s


def _download_capture_group(tracker, nj_url, ytdlp_path):
    """
    Download and process a NobleJury capture for all its parts.
    Downloads the .ts, converts to .mp4, probes duration, adjusts parts,
    and splits if needed.
    """
    indices = [i for i, r in enumerate(tracker.rows)
               if r.get("noblejury_url") == nj_url and not r["download_status"]]
    if not indices:
        return

    first_row = tracker.rows[indices[0]]
    fn = first_row["vod_filename"]

    # Build base name (strip part info) for the .ts and unsplit .mp4
    base = re.sub(r" - part_\d+_of_\d+", "", fn)
    ts_name = base.replace(" - capture.mp4", " - capture.ts")
    mp4_full_name = base  # e.g. "2026-03-20 - streamer - capture.mp4"

    ts_path = tracker.folder / ts_name
    mp4_full_path = tracker.folder / mp4_full_name

    # 1. Download .ts from NobleJury
    if not ts_path.exists():
        print(f"  Downloading capture: {ts_name}")
        if not download_capture_file(nj_url, ts_path):
            print(f"    FAILED to download capture.")
            return
    else:
        print(f"  Capture .ts already exists: {ts_name}")

    # 2. Convert .ts to .mp4 (re-encode — not stream copy)
    if not mp4_full_path.exists():
        print(f"  Converting: {ts_name} -> {mp4_full_name}")
        if not convert_ts_to_mp4(ts_path, mp4_full_path,
                                 ytdlp_path=ytdlp_path):
            print(f"    FAILED to convert capture.")
            return
    else:
        print(f"  Capture .mp4 already exists: {mp4_full_name}")

    # 3. Probe actual duration
    duration = probe_duration(mp4_full_path, ytdlp_path)
    if not duration:
        print(f"    Could not determine duration of {mp4_full_name}.")
        return
    print(f"    Duration: {to_hhmmss(int(duration))}")

    # 4. Update CSV with actual duration and parts
    indices = tracker.update_capture_parts(nj_url, duration)
    if not indices:
        return

    # 5. If multiple parts, split
    n_parts = len(indices)
    if n_parts > 1:
        print(f"  Splitting {mp4_full_name} into {n_parts} parts...")
        for idx in indices:
            row = tracker.rows[idx]
            section = row["vod_section"]
            part_path = tracker.folder / row["vod_filename"]
            if not part_path.exists():
                print(f"    Splitting: {row['vod_filename']}")
                if not split_capture(mp4_full_path, section, part_path,
                                     ytdlp_path=ytdlp_path):
                    print(f"    FAILED to split: {row['vod_filename']}")
                    return
    elif n_parts == 1:
        # Single part — rename the full mp4 to the final filename
        final_path = tracker.folder / tracker.rows[indices[0]]["vod_filename"]
        if mp4_full_path != final_path:
            if not final_path.exists():
                mp4_full_path.rename(final_path)

    # 6. Mark all parts as Saved
    for idx in indices:
        tracker.set_field(idx, "download_status", "Saved")
    print(f"    Capture processed successfully.")


def step_download(config, channels, best_quality=False):
    """Download all VODs that have blank download_status."""
    print("\n=== Downloading VODs ===")
    if best_quality:
        print("  (Using best available quality)")
    else:
        print("  (Using 720p; pass --best-quality for highest resolution)")
    ytdlp = _clean_path(config.get("general", "ytdlp_path", fallback="yt-dlp"))
    grouped = group_by_folder(channels)

    for folder, _chs in grouped.items():
        tracker = VODTracker(folder)
        pending = [
            (i, r) for i, r in enumerate(tracker.rows) if not r["download_status"]
        ]

        if not pending:
            print(f"  {folder}: nothing to download.")
            continue

        print(f"\n  {folder}: {len(pending)} VOD(s) to download.")

        # --- Normal (non-capture) downloads ---
        for idx, row in pending:
            if row.get("noblejury_url"):
                continue  # handled below as capture group
            fn = row["vod_filename"]
            out = tracker.folder / fn
            section = row["vod_section"] or None
            url = row["stream_url"]

            if _has_muting(row.get("muted_segments", "")):
                print(f"  WARNING: '{fn}' has muted segments.")

            print(f"  Downloading: {fn}")
            if download_vod(ytdlp, url, out, section,
                            best_quality=best_quality):
                tracker.set_field(idx, "download_status", "Saved")
                print(f"    Saved.")
            else:
                print(f"    FAILED to download {fn}.")

        # --- Capture downloads (grouped by noblejury_url) ---
        seen_nj = set()
        for _idx, row in pending:
            nj_url = row.get("noblejury_url")
            if not nj_url or nj_url in seen_nj:
                continue
            seen_nj.add(nj_url)
            _download_capture_group(tracker, nj_url, ytdlp)


def step_upload(config, channels, open_browser=True, reauth=False,
                yt_uploader=None):
    """Upload all downloaded VODs that have blank upload_status."""
    print("\n=== Uploading VODs to YouTube ===")
    secrets = config.get("youtube", "client_secrets_file", fallback="client_secrets.json")

    # Build per-streamer config lookup.
    # Streamers without a youtube_playlist_id key are download-only.
    streamer_cfg = {}
    for ch in channels:
        streamer_cfg[ch["username"]] = ch
    upload_streamers = {
        name for name, ch in streamer_cfg.items()
        if "youtube_playlist_id" in ch
    }
    if not upload_streamers:
        print("  No channels configured for YouTube upload.")
        return

    grouped = group_by_folder(channels)

    # Check whether there's anything to upload before authenticating
    any_pending = False
    for folder, _chs in grouped.items():
        tracker = VODTracker(folder)
        if any(
            not r["upload_status"]
            and r["download_status"] == "Saved"
            and r["streamer_name"] in upload_streamers
            for r in tracker.rows
        ):
            any_pending = True
            break

    if not any_pending:
        print("  Nothing to upload.")
        return

    # Reuse pre-authorized uploader if available, otherwise create one
    yt = yt_uploader or YouTubeUploader(secrets, open_browser=open_browser,
                                        reauth=reauth)

    for folder, _chs in grouped.items():
        tracker = VODTracker(folder)
        pending = [
            (i, r)
            for i, r in enumerate(tracker.rows)
            if not r["upload_status"]
            and r["download_status"] == "Saved"
            and r["streamer_name"] in upload_streamers
        ]

        if not pending:
            continue

        print(f"\n  {folder}: {len(pending)} VOD(s) to upload.")

        for idx, row in pending:
            fn = row["vod_filename"]
            filepath = tracker.folder / fn

            if not filepath.exists():
                print(f"  Skipping {fn}: file not found on disk.")
                continue

            tracker.set_field(idx, "upload_status", "Uploading")

            # Look up per-streamer settings
            ch_cfg = streamer_cfg.get(row["streamer_name"], {})
            audio_lang = ch_cfg.get("audio_language", "en")

            # Convert stream datetime to ISO 8601 for recordingDate
            recording_date = row["stream_datetime"]

            # Deleted VODs upload as private (may contain sensitive content)
            privacy = "private" if row.get("deleted") == "Yes" else "unlisted"

            try:
                vid_id = yt.upload(
                    str(filepath),
                    row["vod_title"],
                    row["vod_description"],
                    recording_date=recording_date,
                    audio_language=audio_lang,
                    privacy=privacy,
                )
                if not vid_id:
                    print(f"    FAILED to upload {fn}.")
                    tracker.set_field(idx, "upload_status", "")
                    continue

                yt_url = f"https://youtu.be/{vid_id}"
                tracker.set_field(idx, "vod_url", yt_url)

                playlist = ch_cfg.get("youtube_playlist_id", "")
                if playlist:
                    try:
                        yt.add_to_playlist(vid_id, playlist)
                    except Exception as e:
                        print(f"    Warning: failed to add to playlist: {e}")

                tracker.set_field(idx, "upload_status", "Uploaded")

            except Exception as e:
                print(f"    Error uploading {fn}: {e}")
                tracker.set_field(idx, "upload_status", "")


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Download Twitch VODs and upload them to YouTube.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "If none of --update, --download, --upload are given, all three run.\n"
            "See config.example.ini for API setup instructions."
        ),
    )
    parser.add_argument(
        "--update", action="store_true", help="Update CSV trackers with new Twitch VODs"
    )
    parser.add_argument(
        "--download", action="store_true", help="Download pending VODs with yt-dlp"
    )
    parser.add_argument(
        "--upload", action="store_true", help="Upload downloaded VODs to YouTube"
    )
    parser.add_argument(
        "--best-quality",
        action="store_true",
        help="Download at highest available resolution (default: 720p)",
    )
    parser.add_argument(
        "--twitch-auth",
        action="store_true",
        help="Authorize Twitch via user OAuth (enables muted segment detection). "
             "Only needed once; the token is saved and refreshed automatically.",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Print OAuth URLs instead of auto-opening a browser "
             "(useful for Firefox containers or alternate browser profiles)",
    )
    parser.add_argument(
        "--reauth",
        action="store_true",
        help="Force YouTube re-authorization (use if uploading to the wrong channel "
             "or if your token has expired)",
    )
    parser.add_argument(
        "--config", default="config.ini", help="Path to config file (default: config.ini)"
    )
    parser.add_argument(
        "--channels",
        default="channels.json",
        help="Path to channels file (default: channels.json)",
    )
    args = parser.parse_args()

    run_all = not (args.update or args.download or args.upload)
    will_upload = run_all or args.upload
    will_download = run_all or args.download
    open_browser = not args.no_browser

    config = load_config(args.config)
    channels = load_channels(args.channels)

    # Pre-authorize YouTube early if we'll upload later, so the user
    # doesn't have to wait through a long download only to hit an auth
    # prompt at the end.
    yt_uploader = None
    if will_upload:
        secrets = config.get("youtube", "client_secrets_file",
                             fallback="client_secrets.json")
        # Check if any channels are configured for upload
        has_upload_channels = any(
            "youtube_playlist_id" in ch for ch in channels
        )
        if has_upload_channels:
            print("=== Pre-authorizing YouTube ===")
            try:
                yt_uploader = YouTubeUploader(
                    secrets, open_browser=open_browser,
                    reauth=args.reauth,
                )
            except SystemExit:
                raise
            except Exception as e:
                print(f"  YouTube auth failed: {e}")
                if not run_all:
                    # User explicitly asked for --upload, so fail
                    sys.exit(1)
                print("  Skipping upload step; run with --upload to "
                      "re-authorize.")
                will_upload = False

    if run_all or args.update:
        twitch = TwitchAPI(
            config.get("twitch", "client_id"),
            config.get("twitch", "client_secret"),
            user_auth=args.twitch_auth,
            open_browser=open_browser,
        )
        step_update(config, channels, twitch)

    if will_download:
        step_download(config, channels, best_quality=args.best_quality)

    if will_upload:
        step_upload(config, channels, open_browser=open_browser,
                    reauth=args.reauth, yt_uploader=yt_uploader)

    print("\nDone.")


if __name__ == "__main__":
    main()
