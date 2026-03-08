# ytran

YouTube transcript fetcher and summarizer.

Fetches YouTube video transcripts via transcriptapi.com, cleans them up with
Claude, and stores everything (metadata, raw transcript, formatted transcript,
short summary, full summary) in a local SQLite database.

## Dependencies

- Python 3.8+
- [anthropic](https://pypi.org/project/anthropic/) Python SDK
- A [transcriptapi.com](https://transcriptapi.com) API key (`~/.youtubetotranscript_api_key`)
- An [Anthropic](https://console.anthropic.com) API key (`~/.anthropic_api_key`)

## Installation

```
./configure && make && make install
```

See [INSTALL](INSTALL) for details, including API key setup.

## Usage

```
ytran <youtube-url-or-id>    # Fetch and summarize a video
ytran --browse               # Browse the transcript database
ytran --fix                  # Fill in missing fields in existing entries
ytran --skip ID1,ID2         # Skip specific video IDs during --fix
```

## Database

Stored at `$XDG_DATA_HOME/ytran/youtube-transcripts.db`
(defaults to `~/.local/share/ytran/youtube-transcripts.db`).

| Table | Purpose |
|-------|---------|
| `videos` | One row per video with all metadata and text |
| `channel_names` | Historical channel name/handle tracking |

## License

GNU General Public License v3 or later. See [COPYING](COPYING).
