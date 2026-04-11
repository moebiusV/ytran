# ytran

YouTube transcript fetcher and summarizer.

Fetches YouTube video transcripts via transcriptapi.com, cleans them up with
Claude, and stores everything (metadata, raw transcript, formatted transcript,
short summary, full summary) in a local SQLite database.

## Dependencies

- A [transcriptapi.com](https://transcriptapi.com) API key (`~/.youtubetotranscript_api_key`)
- An [Anthropic](https://console.anthropic.com) API key (`~/.anthropic_api_key`)
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) (required by `ytchan`)

## Installation

```
./configure && make && make install
```

See [INSTALL](INSTALL) for details, including API key setup.

## Usage

```
ytran                        # Browse the transcript database (default)
ytran <url-or-id> [...]      # Fetch and summarize one or more videos
ytran -b/--browse [url ...]  # Browse the transcript database
ytran -n/--no-summary <url>  # Fetch metadata and transcript only (no Claude)
ytran --fix                  # Fill in missing fields in existing entries
ytran --fix --force          # Also process raw-only (--no-summary) entries
ytran --skip ID1,ID2 --fix   # Skip specific video IDs during --fix
ytchan <channel_url>         # Download transcripts for an entire channel
```

Options and URLs may be intermixed in any order.

## Database

Stored at `$XDG_DATA_HOME/ytran/youtube-transcripts.db`
(defaults to `~/.local/share/ytran/youtube-transcripts.db`).

| Table | Purpose |
|-------|---------|
| `videos` | One row per video with all metadata and text |
| `channel_names` | Historical channel name/handle tracking |

## License

BSD 2-Clause. See [COPYING](COPYING).
