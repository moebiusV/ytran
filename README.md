ytran — YouTube Transcript Fetcher and Summarizer

Fetches YouTube video transcripts via transcriptapi.com, cleans them up with
Claude, and stores everything (metadata, raw transcript, formatted transcript,
short summary, full summary) in a local SQLite database.

DEPENDENCIES

  Python 3.8+
  anthropic (Python SDK)
  A transcriptapi.com API key (~/.youtubetotranscript_api_key)
  An Anthropic API key (~/.anthropic_api_key)

INSTALLATION

  ./configure && make && make install

  See INSTALL for details.

USAGE

  ytran <youtube-url-or-id>    Fetch and summarize a video
  ytran --browse               Browse the transcript database
  ytran --fix                  Fill in missing fields in existing entries
  ytran --skip ID1,ID2         Skip specific video IDs during --fix

DATABASE

  Stored at $XDG_DATA_HOME/ytran/youtube-transcripts.db (defaults to
  ~/.local/share/ytran/youtube-transcripts.db).

  Tables:
    videos         — one row per video with all metadata and text
    channel_names  — historical channel name/handle tracking

LICENSE

  GNU General Public License v3 or later.  See COPYING.
