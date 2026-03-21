# Twitch VOD Downloader & YouTube Uploader

Automatically downloads archived Twitch VODs using yt-dlp and uploads them to YouTube. Tracks everything in per-folder CSV files so you can run it repeatedly and it only processes new VODs.
This is designed for Windows 11 and I have not tested it on other operating systems.

## Requirements

- Python 3.9+
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) (for downloading)
- A Twitch application (for API access)
- A Google Cloud project with YouTube Data API v3 enabled (for uploading)

## Setup

### 1. Install Python dependencies

```
pip install -r requirements.txt
```

### 2. Install yt-dlp

Download from https://github.com/yt-dlp/yt-dlp/releases or install via pip:
```
pip install yt-dlp
```

### 3. Set up Twitch API credentials

1. Go to https://dev.twitch.tv/console and log in
2. Click **Register Your Application**
3. Fill in:
   - **Name**: anything (e.g., "VOD Downloader")
   - **OAuth Redirect URLs**: `http://localhost:17563`
   - **Category**: Application Integration
4. Click **Create**, then **Manage** on your new app
5. Copy the **Client ID**
6. Click **New Secret** and copy the **Client Secret**

### 4. Set up YouTube API credentials

1. Go to https://console.cloud.google.com
2. Create a new project (or select an existing one)
3. Go to **APIs & Services > Library**, search for **YouTube Data API v3**, and enable it
4. Go to **APIs & Services > OAuth consent screen**:
   - User Type: **External**
   - Fill in app name and your email
   - Add scope: `.../auth/youtube`
   - **Add yourself as a test user** (under "Test users")
5. Go to **APIs & Services > Credentials**:
   - Click **Create Credentials > OAuth client ID**
   - Application type: **Desktop app**
   - Click **Create**
6. Click the download icon to download the JSON file
7. Save it as `client_secrets.json` next to the script

### 5. Create config files

```
cp config.example.ini config.ini
cp channels.example.json channels.json
```

Edit `config.ini` with your Twitch client ID/secret and the path to yt-dlp if it's not on your PATH.

Edit `channels.json` with the streamers you want to track. Each entry needs:
- `username`: Twitch login name (lowercase)
- `user_id`: Twitch numeric user ID (look up at https://www.streamweasels.com/tools/convert-twitch-username-to-user-id/)
- `output_folder`: where to save videos (forward slashes work on Windows too)
- `youtube_playlist_id` (optional): YouTube playlist to add uploads to. Omit this field entirely to make a channel download-only.
- `audio_language` (optional): ISO language code for the audio track, defaults to `"en"`

## Usage

```
python vod_downloader.py                     # run all steps (update, download, upload)
python vod_downloader.py --update            # only check Twitch for new VODs and update CSVs
python vod_downloader.py --download          # only download pending VODs
python vod_downloader.py --upload            # only upload to YouTube
python vod_downloader.py --update --download # update and download (skip upload)
```

### Options

| Flag | Description |
|------|-------------|
| `--update` | Pull new VODs from Twitch and update CSV trackers |
| `--download` | Download pending VODs with yt-dlp |
| `--upload` | Upload downloaded VODs to YouTube |
| `--best-quality` | Download at full resolution (default is 720p) |
| `--twitch-auth` | Authorize Twitch via user OAuth to enable muted segment detection. Only needed once; the token is saved and refreshed automatically. |
| `--no-browser` | Print OAuth URLs instead of auto-opening a browser. Useful if you use Firefox Multi-Account Containers or a specific browser profile. |
| `--reauth` | Force YouTube re-authorization. Use this if uploads went to the wrong channel. |
| `--config PATH` | Path to config file (default: `config.ini`) |
| `--channels PATH` | Path to channels file (default: `channels.json`) |

If none of `--update`, `--download`, `--upload` are specified, all three run.

### First run

On the first run that includes uploading, a browser window will open asking you to authorize YouTube access. The token is saved to `youtube_token.json` for future runs. After authorizing, the script prints the channel name and ID so you can verify it's the right one.

If you need to authorize in a specific browser profile or Firefox container, use `--no-browser` and copy-paste the printed URL into the right browser/tab.

If you have multiple YouTube channels (e.g., brand accounts) and the script authorized the wrong one, use `--reauth` to delete the saved token and re-authorize. During the Google consent screen, make sure to select the correct account/channel.

### Enabling muted segment detection

By default, the script uses a Twitch app token which cannot detect muted segments (this is a [known Twitch API bug](https://github.com/twitchdev/issues/issues/501)). To get accurate muted segment warnings, authorize once with a Twitch user token:

```
python vod_downloader.py --update --twitch-auth
```

This opens a browser window for Twitch authorization (no special permissions are requested). The token is saved to `twitch_token.json` and refreshed automatically on future runs. You only need to use `--twitch-auth` once; after that, the saved token is used automatically.

To re-authorize (e.g., if the token becomes invalid), just run `--twitch-auth` again. To use a specific browser profile or Firefox container, combine with `--no-browser`.

## How it works

1. **Update**: Queries the Twitch API for each streamer's archived VODs (newest first, stopping at the first VOD already in the CSV). New VODs are added to the per-folder CSV tracker.
2. **Download**: Goes through CSV rows with blank `download_status` and downloads each with yt-dlp. Marks rows as "Saved" on success.
3. **Upload**: Goes through CSV rows with `download_status` = "Saved" and blank `upload_status`. Uploads to YouTube as unlisted, adds to the configured playlist, and records the YouTube URL.

### VOD splitting

VODs longer than 11 hours are automatically split into roughly equal parts with ~1 minute of overlap. Each part gets its own CSV row, filename, and YouTube upload.

### Day ordering

If a streamer has multiple streams on the same day, filenames include a number (e.g., `2024-01-15 - streamer - 2.mp4`). If a later run discovers additional streams for a date, existing files are automatically renamed.

### CSV tracker

Each output folder gets a CSV file named `{foldername} vod tracker.csv` with columns for download/upload status, stream metadata, filenames, titles, descriptions, and YouTube URLs.

## Troubleshooting

### yt-dlp not found

Set the full path to yt-dlp in `config.ini`. Use forward slashes or double backslashes — **don't** wrap the path in quotes or use Python `r""` syntax:
```ini
# Good:
ytdlp_path = D:/Videos/yt-dlp.exe
ytdlp_path = D:\\Videos\\yt-dlp.exe

# Bad (will not work):
ytdlp_path = "D:\Videos\yt-dlp.exe"
ytdlp_path = r"D:\Videos\yt-dlp.exe"
```

Note: the script does try to strip stray quotes as a safety net, but plain paths are best.

### YouTube OAuth errors

- **"Access blocked: app has not completed the Google verification process"**: Make sure you added yourself as a **test user** in the Google Cloud Console under OAuth consent screen > Test users.
- **Token expired**: Run with `--reauth` to re-authorize, or manually delete `youtube_token.json` and run again.
- **Ctrl+C during OAuth**: The script tries to handle Ctrl+C to kill the process during the authorization flow and exit cleanly, but if it fails you will need to close the terminal and open a new one to try again.

### Uploading to the wrong YouTube channel

If you have multiple YouTube channels on your Google account (e.g., personal + brand accounts), the OAuth flow may have authorized the wrong one. The script prints the channel name after authorization so you can check. To fix:

```
python vod_downloader.py --upload --reauth
# or with --no-browser if you need a specific browser/container:
python vod_downloader.py --upload --reauth --no-browser
```

During the Google consent screen, make sure to select the correct Google account that owns the channel you want to upload to. The YouTube API always uploads to the channel associated with the authorized account — there's no way to specify a target channel separately.

### Playlist "Forbidden" / "playlistItemsNotAccessible" error

This means the video was uploaded to a channel that doesn't own the target playlist. This usually happens when you're authenticated as the wrong channel (see above). Fix with `--reauth`.

### YouTube upload quota

The YouTube Data API has a daily quota (default 10,000 units). Each video upload costs 1,600 units (and inserting a video to a playlist takes 50), so you can upload about 6 videos per day with the default quota. (Note: I think this may have changed and you can do more now but I can't find it in the docs; it looks like it might be "video insert" which costs 100, which would allow 66 videos a day.) You can request a quota increase from the Google Cloud Console, but this costs money.

### Muted segments warning

If Twitch has muted segments in a VOD (due to DMCA), the script prints a warning. The video will still download, but muted portions will be silent.

### Download-only channels

To track and download a streamer's VODs without uploading them, simply omit the `youtube_playlist_id` field from their entry in `channels.json`.
