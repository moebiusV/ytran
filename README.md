# ytran

YouTube transcript fetcher and summarizer.

Fetches YouTube video transcripts via transcriptapi.com, cleans them up with
an LLM (Anthropic Claude by default, any OpenAI-compatible provider supported),
and stores everything (metadata, raw transcript, formatted transcript, short
summary, full summary) in a local SQLite database.

## Dependencies

- A [transcriptapi.com](https://transcriptapi.com) API key (`~/.youtubetotranscript_api_key`)
- An LLM API key (see [Configuration](#configuration) below)
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) (for channel URL expansion)

## Installation

```
./configure && make && make install
```

See [INSTALL](INSTALL) for details.

## Configuration

Model settings live in `$XDG_CONFIG_HOME/ytran/` (defaults to `~/.config/ytran/`).

### Default model

`~/.config/ytran/config` sets the default model:
```ini
default = pro
```

If absent, the compiled default (`claude-sonnet-4-6`) is used.

### Model definitions

`~/.config/ytran/models` defines each model in INI-style sections:

```ini
[pro]
name = deepseek-v4-pro
endpoint = https://api.deepseek.com/anthropic/v1/messages
api_key_file = .deepseek_api_key
input_price = 0.55
output_price = 2.19

[sonnet]
name = claude-sonnet-4-6
api_key_file = .anthropic_api_key
input_price = 3.0
output_price = 15.0
```

Each section's short name (e.g. `pro`, `sonnet`) is what `--pro`, `--sonnet`,
and `default =` refer to.  The `name` field is the full model string sent to the API.

Supported fields per section: `name`, `endpoint`, `api_key`, `api_key_file`,
`input_price`, `cache_write_price`, `cache_read_price`, `output_price`.
All optional — missing fields fall back to auto-detection from the model name
prefix (`deepseek-` → DeepSeek endpoint + `~/.deepseek_api_key`, `claude-` →
Anthropic endpoint + `~/.anthropic_api_key`).

The `--api-key FILE` CLI flag overrides whatever the model config says.

## Usage

```
ytran                          # Browse the transcript database (default)
ytran <url-or-id> [...]        # Fetch and summarize one or more videos
ytran --browse [url ...]       # Browse the transcript database
ytran -n/--no-summary <url>    # Fetch metadata and transcript only (no LLM)
ytran --fix                    # Fill in missing fields in existing entries
ytran --full                   # Like --fix, but also promotes raw-only entries
ytran --skip ID1,ID2 --fix     # Skip specific video IDs during --fix
ytran @channel_handle          # Download transcripts for an entire channel
ytran --sonnet <url>           # Use claude-sonnet-4-6 (default)
ytran --pro <url>              # Use deepseek-v4-pro
ytran --model MODEL <url>      # Use a specific model
ytran --api-key FILE <url>     # Use a specific API key file
```

Shortcut flags: `--opus`, `--fable`, `--haiku`, `--sonnet`, `--flash`, `--pro`.

Channel URLs (`/@`, `/channel/`, `/c/`, `/user/`) are auto-expanded
via yt-dlp, and already-downloaded videos are skipped.

Options and URLs may be intermixed in any order.

## Database

Stored at `$XDG_DATA_HOME/ytran/youtube-transcripts.db`
(defaults to `~/.local/share/ytran/youtube-transcripts.db`).

## License

BSD 2-Clause. See [COPYING](COPYING).
