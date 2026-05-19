# ytran

YouTube transcript fetcher and summarizer.

Fetches YouTube video transcripts via transcriptapi.com, cleans them up with
Claude, and stores everything (metadata, raw transcript, formatted transcript,
short summary, full summary) in a local SQLite database.

## Dependencies

- A [transcriptapi.com](https://transcriptapi.com) API key (`~/.youtubetotranscript_api_key`)
- An [Anthropic](https://console.anthropic.com) API key (`~/.anthropic_api_key`)
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) (for channel URL expansion)

## Installation

```
./configure && make && make install
```

See [INSTALL](INSTALL) for details, including API key setup.

## Usage

```
ytran                          # Browse the transcript database (default)
ytran <url-or-id> [...]        # Fetch and summarize one or more videos
ytran --browse [url ...]       # Browse the transcript database
ytran -n/--no-summary <url>    # Fetch metadata and transcript only (no Claude)
ytran --fix                    # Fill in missing fields in existing entries
ytran --full                   # Like --fix, but also promotes raw-only entries
ytran --skip ID1,ID2 --fix     # Skip specific video IDs during --fix
ytran @channel_handle          # Download transcripts for an entire channel
ytran --haiku <url>            # Use claude-haiku-4-5 instead of sonnet
ytran --sonnet <url>           # Use claude-sonnet-4-6 (default)
ytran --model MODEL <url>      # Use a specific Claude model
```

Channel URLs (`/@`, `/channel/`, `/c/`, `/user/`) are auto-expanded
via yt-dlp, and already-downloaded videos are skipped.

Options and URLs may be intermixed in any order.

Batch modes support rate-limiting:
```
ytran --min-delay 10 --initial-delay 30 --max-backoff 900 --fix
```

## Database

Stored at `$XDG_DATA_HOME/ytran/youtube-transcripts.db`
(defaults to `~/.local/share/ytran/youtube-transcripts.db`).

### Tables

| Table | Purpose |
|-------|---------|
| `videos` | One row per video with all metadata and text |
| `channel_names` | Historical channel name/handle tracking |
| `channels` | Per-channel flags (e.g. `bulkdl` for bulk-downloaded) |
| `queue_state` | Cursor position for batch-mode resume |
| `_browse_config` | browse-sqlite3 display preferences (sort, column order) |

### Timestamps

`videos` has three timestamp columns:

| Column | Semantics |
|--------|-----------|
| `upload_date` | When YouTube says the video was uploaded |
| `fetched_at` | When video info was first downloaded by ytran |
| `updated_at` | Last time data changed (seeded from `fetched_at`, bumped by `--fix`/`--full`) |

### Default sort

The `_browse_config` table seeds a default sort of `updated_at DESC`
for the videos table, so browse-sqlite3 shows most-recently-modified
videos first.

## License

BSD 2-Clause. See [COPYING](COPYING).
