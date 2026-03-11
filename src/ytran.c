/* SPDX-License-Identifier: BSD-2-Clause */
/* Copyright (c) 2026 David Walther */
/*
 * ytran - YouTube transcript fetcher and summarizer
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <getopt.h>
#include <curl/curl.h>
#include <sqlite3.h>
#include <cjson/cJSON.h>

/* ── Types ── */

typedef struct {
	char *data;
	size_t len;
	size_t cap;
} Buf;

/* ── Pricing ($/MTok): input, cache_write, cache_read, output ── */

static const struct { const char *model; double pi, pcw, pcr, po; } pricing[] = {
	{"claude-opus-4-6",   5.0, 6.25, 0.50, 25.0},
	{"claude-sonnet-4-5", 3.0, 3.75, 0.30, 15.0},
	{"claude-haiku-4-5",  1.0, 1.25, 0.10,  5.0},
	{NULL, 5.0, 6.25, 0.50, 25.0}
};

/* ── Globals ── */

static double total_cost;
static double g_last_cost;  /* cost of most recent API call */
static char *transcript_api_key;
static char *anthropic_api_key;
static const char *model = "claude-sonnet-4-5";

/* ── Buffer ── */

static void buf_init(Buf *b) { memset(b, 0, sizeof(*b)); }

static void buf_append(Buf *b, const char *s, size_t n)
{
	if (b->len + n + 1 > b->cap) {
		b->cap = (b->len + n + 1) * 2;
		if (b->cap < 4096) b->cap = 4096;
		b->data = realloc(b->data, b->cap);
	}
	memcpy(b->data + b->len, s, n);
	b->len += n;
	b->data[b->len] = '\0';
}

static void buf_free(Buf *b) { free(b->data); memset(b, 0, sizeof(*b)); }

__attribute__((format(printf, 2, 3)))
static void buf_printf(Buf *b, const char *fmt, ...)
{
	char *s = NULL;
	va_list ap;
	va_start(ap, fmt);
	int n = vasprintf(&s, fmt, ap);
	va_end(ap);
	if (n >= 0) { buf_append(b, s, n); free(s); }
}

/* ── String helpers ── */

/* Strip invalid UTF-8 sequences (surrogates, overlong, truncated) in place. */
static void sanitize_utf8(char *s)
{
	unsigned char *r = (unsigned char *)s, *w = (unsigned char *)s;
	while (*r) {
		if (r[0] < 0x80) {
			*w++ = *r++;
		} else if ((r[0] & 0xE0) == 0xC0 && (r[1] & 0xC0) == 0x80) {
			unsigned cp = ((r[0] & 0x1F) << 6) | (r[1] & 0x3F);
			if (cp >= 0x80) { *w++ = *r++; *w++ = *r++; }
			else r += 2; /* overlong */
		} else if ((r[0] & 0xF0) == 0xE0 && (r[1] & 0xC0) == 0x80 &&
		           (r[2] & 0xC0) == 0x80) {
			unsigned cp = ((r[0] & 0x0F) << 12) | ((r[1] & 0x3F) << 6) |
			              (r[2] & 0x3F);
			if (cp >= 0x0800 && (cp < 0xD800 || cp > 0xDFFF)) {
				*w++ = *r++; *w++ = *r++; *w++ = *r++;
			} else r += 3; /* surrogate or overlong */
		} else if ((r[0] & 0xF8) == 0xF0 && (r[1] & 0xC0) == 0x80 &&
		           (r[2] & 0xC0) == 0x80 && (r[3] & 0xC0) == 0x80) {
			unsigned cp = ((r[0] & 0x07) << 18) | ((r[1] & 0x3F) << 12) |
			              ((r[2] & 0x3F) << 6) | (r[3] & 0x3F);
			if (cp >= 0x10000 && cp <= 0x10FFFF) {
				*w++ = *r++; *w++ = *r++; *w++ = *r++; *w++ = *r++;
			} else r += 4;
		} else {
			r++; /* invalid lead byte, skip */
		}
	}
	*w = '\0';
}

/* Return malloc'd string between two markers, or NULL. */
static char *find_between(const char *hay, const char *before, const char *after)
{
	const char *s = strstr(hay, before);
	if (!s) return NULL;
	s += strlen(before);
	const char *e = strstr(s, after);
	if (!e) return NULL;
	return strndup(s, e - s);
}

static void html_decode_inplace(char *s)
{
	static const struct { const char *ent; char ch; int len; } ents[] = {
		{"&amp;",  '&', 5}, {"&lt;",   '<', 4}, {"&gt;",   '>', 4},
		{"&quot;", '"', 6}, {"&#39;",  '\'', 5}, {"&apos;", '\'', 6},
	};
	char *r = s, *w = s;
	while (*r) {
		if (*r == '&') {
			bool found = false;
			for (int i = 0; i < (int)(sizeof(ents)/sizeof(ents[0])); i++)
				if (strncmp(r, ents[i].ent, ents[i].len) == 0) {
					*w++ = ents[i].ch;
					r += ents[i].len;
					found = true;
					break;
				}
			if (!found) *w++ = *r++;
		} else {
			*w++ = *r++;
		}
	}
	*w = '\0';
}

/* Strip " - YouTube" suffix in place. */
static void strip_yt_suffix(char *s)
{
	size_t len = strlen(s);
	const char *suf = " - YouTube";
	size_t slen = strlen(suf);
	if (len >= slen && strcmp(s + len - slen, suf) == 0)
		s[len - slen] = '\0';
}

/* ── File I/O ── */

static char *read_file_trimmed(const char *path)
{
	FILE *f = fopen(path, "r");
	if (!f) return NULL;
	fseek(f, 0, SEEK_END);
	long len = ftell(f);
	fseek(f, 0, SEEK_SET);
	char *s = malloc(len + 1);
	len = fread(s, 1, len, f);
	fclose(f);
	while (len > 0 && (s[len-1] == '\n' || s[len-1] == '\r' || s[len-1] == ' '))
		len--;
	s[len] = '\0';
	return s;
}

static char *xdg_data_home(void)
{
	const char *env = getenv("XDG_DATA_HOME");
	if (env && *env) return strdup(env);
	char *path = NULL;
	if (asprintf(&path, "%s/.local/share", getenv("HOME")) < 0) return NULL;
	return path;
}

/* ── Fix-mode state ── */
static bool g_fix_mode = false;
static char g_last_http_error[64];  /* empty if OK, else error description */

/* ── HTTP ── */

static size_t curl_write_cb(void *ptr, size_t size, size_t nmemb, void *ud)
{
	Buf *b = ud;
	buf_append(b, ptr, size * nmemb);
	return size * nmemb;
}

static char *http_fetch(const char *url, const char *post_body,
                        struct curl_slist *headers, long timeout)
{
	CURL *c = curl_easy_init();
	Buf resp;
	buf_init(&resp);

	curl_easy_setopt(c, CURLOPT_URL, url);
	curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, curl_write_cb);
	curl_easy_setopt(c, CURLOPT_WRITEDATA, &resp);
	curl_easy_setopt(c, CURLOPT_TIMEOUT, timeout);
	curl_easy_setopt(c, CURLOPT_USERAGENT,
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36");
	if (headers)
		curl_easy_setopt(c, CURLOPT_HTTPHEADER, headers);
	if (post_body)
		curl_easy_setopt(c, CURLOPT_POSTFIELDS, post_body);

	CURLcode res = curl_easy_perform(c);
	long code = 0;
	curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &code);
	curl_easy_cleanup(c);

	if (res != CURLE_OK) {
		if (g_fix_mode)
			snprintf(g_last_http_error, sizeof(g_last_http_error),
				"%s", curl_easy_strerror(res));
		else
			fprintf(stderr, "HTTP error: %s\n", curl_easy_strerror(res));
		buf_free(&resp);
		return NULL;
	}
	if (code >= 400) {
		if (g_fix_mode) {
			snprintf(g_last_http_error, sizeof(g_last_http_error),
				"HTTP %ld", code);
		} else {
			fprintf(stderr, "HTTP %ld: ", code);
			/* Try to extract JSON error message */
			bool printed = false;
			if (resp.data) {
				cJSON *err = cJSON_Parse(resp.data);
				if (err) {
					cJSON *eo = cJSON_GetObjectItem(err, "error");
					if (eo) {
						cJSON *msg = cJSON_GetObjectItem(eo, "message");
						if (cJSON_IsString(msg)) {
							fprintf(stderr, "%s\n", msg->valuestring);
							printed = true;
						}
					}
					cJSON_Delete(err);
				}
				if (!printed)
					fprintf(stderr, "%s\n", url);
			} else {
				fprintf(stderr, "%s\n", url);
			}
		}
		buf_free(&resp);
		return NULL;
	}
	return resp.data;
}

/* ── YouTube ID extraction ── */

static char *extract_youtube_id(const char *s)
{
	/* ?v=XXXXXXXXXXX */
	const char *p = strstr(s, "v=");
	if (p) {
		p += 2;
		const char *e = strchrnul(p, '&');
		return strndup(p, e - p);
	}
	/* youtu.be/XXXXXXXXXXX */
	p = strstr(s, "youtu.be/");
	if (p) {
		p += 9;
		const char *e = strchrnul(p, '?');
		return strndup(p, e - p);
	}
	/* bare 11-char ID */
	size_t len = strlen(s);
	if (len == 11) {
		for (size_t i = 0; i < len; i++) {
			char c = s[i];
			if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			      (c >= '0' && c <= '9') || c == '_' || c == '-'))
				goto bad;
		}
		return strdup(s);
	}
bad:
	fprintf(stderr, "Invalid YouTube URL or ID: %s\n", s);
	return NULL;
}

/* ── Extract JSON object starting at '{' with brace matching ── */

static char *extract_json_object(const char *start)
{
	if (*start != '{') return NULL;
	int depth = 0;
	bool in_str = false;
	for (const char *p = start; *p; p++) {
		if (in_str) {
			if (*p == '\\') p++;
			else if (*p == '"') in_str = false;
		} else {
			if (*p == '"') in_str = true;
			else if (*p == '{') depth++;
			else if (*p == '}' && --depth == 0)
				return strndup(start, p - start + 1);
		}
	}
	return NULL;
}

/* ── YouTube metadata ── */

typedef struct {
	char *title, *upload_date, *duration, *description;
	char *channel_id, *channel_name, *channel_handle;
} Metadata;

static Metadata fetch_youtube_metadata(const char *video_id)
{
	Metadata m = {0};
	char url[128];
	snprintf(url, sizeof(url), "https://www.youtube.com/watch?v=%s", video_id);
	char *page = http_fetch(url, NULL, NULL, 30);
	if (!page) return m;
	sanitize_utf8(page);

	/* Title */
	m.title = find_between(page, "<title>", "</title>");
	if (m.title) {
		html_decode_inplace(m.title);
		strip_yt_suffix(m.title);
	}

	/* Upload date */
	m.upload_date = find_between(page, "itemprop=\"uploadDate\" content=\"", "\"");

	/* Duration */
	m.duration = find_between(page, "itemprop=\"duration\" content=\"", "\"");

	/* Description: prefer full from ytInitialPlayerResponse JSON */
	const char *pr = strstr(page, "var ytInitialPlayerResponse");
	if (pr) {
		const char *brace = strchr(pr, '{');
		if (brace) {
			char *json_str = extract_json_object(brace);
			if (json_str) {
				cJSON *root = cJSON_Parse(json_str);
				if (root) {
					cJSON *vd = cJSON_GetObjectItem(root, "videoDetails");
					if (vd) {
						cJSON *sd = cJSON_GetObjectItem(vd, "shortDescription");
						if (cJSON_IsString(sd))
							m.description = strdup(sd->valuestring);
					}
					cJSON_Delete(root);
				}
				free(json_str);
			}
		}
	}
	if (!m.description)
		m.description = find_between(page, "<meta name=\"description\" content=\"", "\"");

	/* Channel ID and handle */
	const char *bid = strstr(page, "\"browseId\":\"UC");
	if (bid) {
		bid += strlen("\"browseId\":\"");
		m.channel_id = strndup(bid, 24); /* UC + 22 chars */
		const char *cb = strstr(bid, "\"canonicalBaseUrl\":\"");
		if (cb) {
			cb += strlen("\"canonicalBaseUrl\":\"");
			const char *e = strchr(cb, '"');
			if (e) {
				char *handle = strndup(cb, e - cb);
				/* strip leading / */
				if (handle[0] == '/') {
					m.channel_handle = strdup(handle + 1);
					free(handle);
				} else {
					m.channel_handle = handle;
				}
			}
		}
	}

	/* Channel name */
	char *cn = find_between(page, "\"ownerChannelName\":\"", "\"");
	if (cn) m.channel_name = cn;

	free(page);
	return m;
}

static void metadata_free(Metadata *m)
{
	free(m->title); free(m->upload_date); free(m->duration);
	free(m->description); free(m->channel_id);
	free(m->channel_name); free(m->channel_handle);
	memset(m, 0, sizeof(*m));
}

/* ── Transcript fetch ── */

static char *fetch_raw_transcript(const char *video_id, char **out_language)
{
	char url[256];
	snprintf(url, sizeof(url),
		"https://transcriptapi.com/api/v2/youtube/transcript?video_url=%s&format=json",
		video_id);
	char hdr[256];
	snprintf(hdr, sizeof(hdr), "Authorization: Bearer %s", transcript_api_key);
	struct curl_slist *headers = curl_slist_append(NULL, hdr);
	char *body = http_fetch(url, NULL, headers, 30);
	curl_slist_free_all(headers);
	if (!body) return NULL;

	cJSON *root = cJSON_Parse(body);
	free(body);
	if (!root) { fprintf(stderr, "Error parsing transcript JSON\n"); return NULL; }

	cJSON *lang = cJSON_GetObjectItem(root, "language");
	if (out_language && cJSON_IsString(lang))
		*out_language = strdup(lang->valuestring);

	Buf transcript;
	buf_init(&transcript);
	cJSON *segs = cJSON_GetObjectItem(root, "transcript");
	cJSON *seg;
	cJSON_ArrayForEach(seg, segs) {
		cJSON *start = cJSON_GetObjectItem(seg, "start");
		cJSON *text = cJSON_GetObjectItem(seg, "text");
		if (cJSON_IsNumber(start) && cJSON_IsString(text)) {
			char line[4096];
			int n = snprintf(line, sizeof(line), "%.2f: %s\n",
				start->valuedouble, text->valuestring);
			buf_append(&transcript, line, n);
		}
	}
	cJSON_Delete(root);
	return transcript.data;
}

/* ── Strip timestamp prefixes from transcript ── */

static char *strip_timestamps(const char *text)
{
	Buf out;
	buf_init(&out);
	const char *p = text;
	while (*p) {
		const char *lp = p;
		while (*lp >= '0' && *lp <= '9') lp++;
		if (*lp == '.') { lp++; while (*lp >= '0' && *lp <= '9') lp++; }
		if (*lp == ':') { lp++; while (*lp == ' ') lp++; p = lp; }
		const char *nl = strchr(p, '\n');
		if (nl) {
			buf_append(&out, p, nl - p + 1);
			p = nl + 1;
		} else {
			buf_append(&out, p, strlen(p));
			break;
		}
	}
	return out.data;
}

/* ── Word wrap (equivalent to fold -s -w 80) ── */

static char *wordwrap(const char *text)
{
	if (!text || !*text) return strdup("");
	size_t len = strlen(text);
	char *out = malloc(len * 2 + 1);
	size_t oi = 0;
	int col = 0, last_sp = -1;
	for (size_t i = 0; i < len; i++) {
		if (text[i] == '\n') {
			out[oi++] = '\n';
			col = 0;
			last_sp = -1;
			continue;
		}
		if (text[i] == ' ') last_sp = oi;
		out[oi++] = text[i];
		col++;
		if (col >= 80 && last_sp >= 0) {
			out[last_sp] = '\n';
			col = oi - last_sp - 1;
			last_sp = -1;
		}
	}
	out[oi] = '\0';
	return out;
}

/* ── Claude API ── */

static char *generate_with_claude(const char *prompt, const char *cached_prefix,
                                  const char *model, int max_tokens)
{
	/* Build request JSON */
	cJSON *req = cJSON_CreateObject();
	cJSON_AddStringToObject(req, "model", model);
	cJSON_AddNumberToObject(req, "max_tokens", max_tokens);

	if (cached_prefix) {
		cJSON *sys = cJSON_AddArrayToObject(req, "system");
		cJSON *block = cJSON_CreateObject();
		cJSON_AddStringToObject(block, "type", "text");
		cJSON_AddStringToObject(block, "text", cached_prefix);
		cJSON *cc = cJSON_CreateObject();
		cJSON_AddStringToObject(cc, "type", "ephemeral");
		cJSON_AddItemToObject(block, "cache_control", cc);
		cJSON_AddItemToArray(sys, block);
	}

	cJSON *msgs = cJSON_AddArrayToObject(req, "messages");
	cJSON *msg = cJSON_CreateObject();
	cJSON_AddStringToObject(msg, "role", "user");
	cJSON_AddStringToObject(msg, "content", prompt);
	cJSON_AddItemToArray(msgs, msg);

	char *body = cJSON_PrintUnformatted(req);
	cJSON_Delete(req);

	/* HTTP headers */
	char auth_hdr[256];
	snprintf(auth_hdr, sizeof(auth_hdr), "x-api-key: %s", anthropic_api_key);
	struct curl_slist *headers = NULL;
	headers = curl_slist_append(headers, auth_hdr);
	headers = curl_slist_append(headers, "anthropic-version: 2023-06-01");
	headers = curl_slist_append(headers, "anthropic-beta: prompt-caching-2024-07-31");
	headers = curl_slist_append(headers, "content-type: application/json");

	char *resp = http_fetch("https://api.anthropic.com/v1/messages",
		body, headers, 300);
	free(body);
	curl_slist_free_all(headers);
	if (!resp) return strdup("");

	/* Parse response */
	cJSON *root = cJSON_Parse(resp);
	free(resp);
	if (!root) { fprintf(stderr, "Error parsing Claude response\n"); return strdup(""); }

	char *result = strdup("");
	cJSON *content = cJSON_GetObjectItem(root, "content");
	if (cJSON_IsArray(content)) {
		cJSON *first = cJSON_GetArrayItem(content, 0);
		cJSON *text = first ? cJSON_GetObjectItem(first, "text") : NULL;
		if (cJSON_IsString(text)) {
			free(result);
			result = strdup(text->valuestring);
		}
	}

	/* Cost tracking */
	cJSON *usage = cJSON_GetObjectItem(root, "usage");
	if (usage) {
		int in_tok = 0, out_tok = 0, cw_tok = 0, cr_tok = 0;
		cJSON *v;
		if ((v = cJSON_GetObjectItem(usage, "input_tokens")))
			in_tok = v->valueint;
		if ((v = cJSON_GetObjectItem(usage, "output_tokens")))
			out_tok = v->valueint;
		if ((v = cJSON_GetObjectItem(usage, "cache_creation_input_tokens")))
			cw_tok = v->valueint;
		if ((v = cJSON_GetObjectItem(usage, "cache_read_input_tokens")))
			cr_tok = v->valueint;

		double pi = 5.0, pcw = 6.25, pcr = 0.50, po = 25.0;
		for (int i = 0; pricing[i].model; i++)
			if (strcmp(pricing[i].model, model) == 0) {
				pi = pricing[i].pi; pcw = pricing[i].pcw;
				pcr = pricing[i].pcr; po = pricing[i].po;
				break;
			}
		double cost = (in_tok * pi + cw_tok * pcw + cr_tok * pcr + out_tok * po) / 1e6;
		total_cost += cost;
		g_last_cost = cost;
		/* Cost is printed inline by the caller */
	}

	cJSON_Delete(root);
	return result;
}

/* ── SQLite init ── */

static void db_init(sqlite3 *db)
{
	sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS videos ("
		"video_id TEXT PRIMARY KEY, title TEXT, upload_date TEXT, "
		"duration TEXT, description TEXT, channel_id TEXT, language TEXT, "
		"raw_transcript TEXT, transcript_formatted TEXT, "
		"summary_short TEXT, summary_full TEXT, fetched_at TEXT)", NULL, NULL, NULL);

	/* Migrate old channel_names schema if needed */
	sqlite3_stmt *st;
	if (sqlite3_prepare_v2(db,
		"SELECT name FROM sqlite_master WHERE type='table' AND name='channel_names'",
		-1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) {
		sqlite3_finalize(st);

		bool has_display_name = false;
		bool has_handle = false;
		if (sqlite3_prepare_v2(db, "PRAGMA table_info(channel_names)", -1, &st, NULL) == SQLITE_OK) {
			while (sqlite3_step(st) == SQLITE_ROW) {
				const char *col = (const char *)sqlite3_column_text(st, 1);
				if (strcmp(col, "display_name") == 0) has_display_name = true;
				if (strcmp(col, "handle") == 0) has_handle = true;
			}
			sqlite3_finalize(st);
		}
		if (has_display_name) {
			sqlite3_exec(db,
				"CREATE TABLE channel_names_new (channel_id TEXT, name_type TEXT, "
				"name TEXT, timestamp TEXT, PRIMARY KEY (channel_id, name_type, timestamp))",
				NULL, NULL, NULL);
			sqlite3_exec(db,
				"INSERT INTO channel_names_new SELECT channel_id, 'fullname', "
				"display_name, timestamp FROM channel_names WHERE display_name IS NOT NULL",
				NULL, NULL, NULL);
			if (has_handle)
				sqlite3_exec(db,
					"INSERT OR IGNORE INTO channel_names_new (channel_id, name_type, name, timestamp) "
					"SELECT channel_id, 'handle', handle, timestamp FROM channel_names "
					"WHERE handle IS NOT NULL AND handle <> ''", NULL, NULL, NULL);
			sqlite3_exec(db, "DROP TABLE channel_names", NULL, NULL, NULL);
			sqlite3_exec(db, "ALTER TABLE channel_names_new RENAME TO channel_names", NULL, NULL, NULL);
		}
	} else {
		sqlite3_finalize(st);
	}

	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS channel_names ("
		"channel_id TEXT, name_type TEXT, name TEXT, timestamp TEXT, "
		"PRIMARY KEY (channel_id, name_type, timestamp))", NULL, NULL, NULL);

	/* Add missing columns */
	const char *cols[] = {"title","upload_date","duration","description","channel_id",
		"language","raw_transcript","transcript_formatted","summary_short","summary_full","fetched_at"};
	for (int i = 0; i < (int)(sizeof(cols)/sizeof(cols[0])); i++) {
		char sql[128];
		snprintf(sql, sizeof(sql), "ALTER TABLE videos ADD COLUMN %s TEXT", cols[i]);
		sqlite3_exec(db, sql, NULL, NULL, NULL); /* ignore error if exists */
	}

	/* Drop vestigial transcript column */
	sqlite3_exec(db, "ALTER TABLE videos DROP COLUMN transcript", NULL, NULL, NULL);
}

/* ── Helpers for nullable DB strings ── */

static char *db_text(sqlite3_stmt *st, int col)
{
	const char *s = (const char *)sqlite3_column_text(st, col);
	return (s && *s) ? strdup(s) : NULL;
}

#define EMPTY(s) (!(s) || !*(s))

/* Replace dst with src if dst is empty. Takes ownership of src. */
static void fill(char **dst, char *src)
{
	if (EMPTY(*dst) && !EMPTY(src)) { free(*dst); *dst = src; }
	else free(src);
}

/* ── process_video ── */

static void process_video(const char *video_id, sqlite3 *db)
{
	char *title = NULL, *upload_date = NULL, *duration = NULL;
	char *description = NULL, *channel_id = NULL, *language = NULL;
	char *raw_transcript = NULL, *transcript_formatted = NULL;
	char *summary_short = NULL, *summary_full = NULL, *fetched_at = NULL;
	char *channel_name = NULL, *channel_handle = NULL;

	/* Load existing row */
	sqlite3_stmt *st;
	sqlite3_prepare_v2(db,
		"SELECT title, upload_date, duration, description, channel_id, "
		"language, raw_transcript, transcript_formatted, summary_short, "
		"summary_full, fetched_at FROM videos WHERE video_id = ?", -1, &st, NULL);
	sqlite3_bind_text(st, 1, video_id, -1, SQLITE_STATIC);
	if (sqlite3_step(st) == SQLITE_ROW) {
		title = db_text(st, 0); upload_date = db_text(st, 1);
		duration = db_text(st, 2); description = db_text(st, 3);
		channel_id = db_text(st, 4); language = db_text(st, 5);
		raw_transcript = db_text(st, 6); transcript_formatted = db_text(st, 7);
		summary_short = db_text(st, 8); summary_full = db_text(st, 9);
		fetched_at = db_text(st, 10);
	}
	sqlite3_finalize(st);

	if (!fetched_at) {
		time_t now = time(NULL);
		struct tm *tm = localtime(&now);
		fetched_at = malloc(32);
		strftime(fetched_at, 32, "%Y-%m-%dT%H:%M:%S", tm);
	}

	/* In fix mode, collect step results into one line */
	Buf fix_line;
	if (g_fix_mode) buf_init(&fix_line);
	bool had_title = !EMPTY(title);

	/* Fix metadata if missing */
	bool desc_truncated = description && strlen(description) <= 200;
	if (EMPTY(title) || EMPTY(upload_date) || EMPTY(duration) ||
	    EMPTY(description) || EMPTY(channel_id) || desc_truncated) {
		if (!g_fix_mode)
			printf("Fixing metadata for %s\n", video_id);
		g_last_http_error[0] = '\0';
		Metadata m = fetch_youtube_metadata(video_id);
		if (g_fix_mode) {
			/* Print title now if we just learned it */
			if (!had_title && !EMPTY(m.title))
				printf("%s", m.title);
			if (g_last_http_error[0])
				buf_printf(&fix_line, " metadata:%s", g_last_http_error);
			else
				buf_printf(&fix_line, " metadata");
		}
		fill(&title, m.title); m.title = NULL;
		fill(&upload_date, m.upload_date); m.upload_date = NULL;
		fill(&duration, m.duration); m.duration = NULL;
		if (EMPTY(description) || (desc_truncated && m.description &&
		    strlen(m.description) > strlen(description))) {
			free(description);
			description = m.description;
		} else {
			free(m.description);
		}
		m.description = NULL;
		fill(&channel_id, m.channel_id); m.channel_id = NULL;
		channel_name = m.channel_name; m.channel_name = NULL;
		channel_handle = m.channel_handle; m.channel_handle = NULL;
		metadata_free(&m);
	}

	/* Channel names tracking */
	if (!EMPTY(channel_id)) {
		if (!channel_name) {
			sqlite3_prepare_v2(db,
				"SELECT name FROM channel_names WHERE channel_id = ? "
				"AND name_type = 'fullname' ORDER BY timestamp DESC LIMIT 1",
				-1, &st, NULL);
			sqlite3_bind_text(st, 1, channel_id, -1, SQLITE_STATIC);
			bool found = sqlite3_step(st) == SQLITE_ROW;
			sqlite3_finalize(st);
			if (!found) {
				if (!g_fix_mode)
					printf("Fetching channel name for %s\n", video_id);
				g_last_http_error[0] = '\0';
				Metadata m = fetch_youtube_metadata(video_id);
				if (g_fix_mode && g_last_http_error[0])
					buf_printf(&fix_line, " channel:%s", g_last_http_error);
				channel_name = m.channel_name; m.channel_name = NULL;
				channel_handle = m.channel_handle; m.channel_handle = NULL;
				metadata_free(&m);
			}
		}
		if (channel_name) {
			sqlite3_prepare_v2(db,
				"SELECT name FROM channel_names WHERE channel_id = ? "
				"AND name_type = 'fullname' ORDER BY timestamp DESC LIMIT 1",
				-1, &st, NULL);
			sqlite3_bind_text(st, 1, channel_id, -1, SQLITE_STATIC);
			bool update = true;
			if (sqlite3_step(st) == SQLITE_ROW) {
				const char *cur = (const char *)sqlite3_column_text(st, 0);
				if (cur && strcmp(cur, channel_name) == 0) update = false;
			}
			sqlite3_finalize(st);
			if (update) {
				sqlite3_prepare_v2(db,
					"INSERT INTO channel_names (channel_id, name_type, name, timestamp) "
					"VALUES (?, 'fullname', ?, ?)", -1, &st, NULL);
				sqlite3_bind_text(st, 1, channel_id, -1, SQLITE_STATIC);
				sqlite3_bind_text(st, 2, channel_name, -1, SQLITE_STATIC);
				sqlite3_bind_text(st, 3, fetched_at, -1, SQLITE_STATIC);
				sqlite3_step(st);
				sqlite3_finalize(st);
			}
		}
		if (channel_handle) {
			sqlite3_prepare_v2(db,
				"SELECT name FROM channel_names WHERE channel_id = ? "
				"AND name_type = 'handle' ORDER BY timestamp DESC LIMIT 1",
				-1, &st, NULL);
			sqlite3_bind_text(st, 1, channel_id, -1, SQLITE_STATIC);
			bool update = true;
			if (sqlite3_step(st) == SQLITE_ROW) {
				const char *cur = (const char *)sqlite3_column_text(st, 0);
				if (cur && strcmp(cur, channel_handle) == 0) update = false;
			}
			sqlite3_finalize(st);
			if (update) {
				sqlite3_prepare_v2(db,
					"INSERT INTO channel_names (channel_id, name_type, name, timestamp) "
					"VALUES (?, 'handle', ?, ?)", -1, &st, NULL);
				sqlite3_bind_text(st, 1, channel_id, -1, SQLITE_STATIC);
				sqlite3_bind_text(st, 2, channel_handle, -1, SQLITE_STATIC);
				sqlite3_bind_text(st, 3, fetched_at, -1, SQLITE_STATIC);
				sqlite3_step(st);
				sqlite3_finalize(st);
			}
		}
	}

	/* Fix raw transcript if missing */
	if (EMPTY(raw_transcript)) {
		if (!g_fix_mode)
			printf("Fetching transcript for %s\n", video_id);
		g_last_http_error[0] = '\0';
		char *lang = NULL;
		char *rt = fetch_raw_transcript(video_id, &lang);
		if (g_fix_mode) {
			if (g_last_http_error[0])
				buf_printf(&fix_line, " transcript:%s", g_last_http_error);
			else
				buf_printf(&fix_line, " transcript");
		}
		if (rt) { free(raw_transcript); raw_transcript = rt; }
		if (lang) { free(language); language = lang; }
	}

	/* Build cached prefix for Claude calls */
	char *cached_prefix = NULL;
	bool needs_format = EMPTY(transcript_formatted) && !EMPTY(raw_transcript);
	bool needs_full = EMPTY(summary_full) && (!EMPTY(transcript_formatted) || needs_format);
	bool needs_short = EMPTY(summary_short) && (!EMPTY(summary_full) || needs_full);

	if (!EMPTY(raw_transcript) && (needs_format || needs_full || needs_short)) {
		char *stripped = strip_timestamps(raw_transcript);
		if (asprintf(&cached_prefix, "Title: %s\nDescription: %s\n\nTranscript:\n%s",
			title ? title : "", description ? description : "", stripped) < 0)
			cached_prefix = NULL;
		else
			sanitize_utf8(cached_prefix);
		free(stripped);
	}

	/* Generate formatted transcript */
	if (needs_format) {
		if (!g_fix_mode) {
			printf("Formatting transcript for %s", video_id);
			fflush(stdout);
		}
		g_last_http_error[0] = '\0';
		g_last_cost = 0;
		char *raw = generate_with_claude(
			"Clean up this AI-generated transcript for readability. "
			"Correct AI transcription artifacts like misheard names "
			"(e.g., 'Dr. Brite' to 'Dr. Bright'), spelling errors "
			"(e.g., 'capiscum' to 'capsicum'), grammar, and punctuation. "
			"Group into logical paragraphs, break run-on sentences, "
			"remove fillers where they disrupt flow. Keep content faithful.",
			cached_prefix, model, 16000);
		if (g_fix_mode) {
			if (g_last_http_error[0])
				buf_printf(&fix_line, " format:%s", g_last_http_error);
			else
				buf_printf(&fix_line, " format:$%.4f", g_last_cost);
		} else {
			printf(" $%.4f\n", g_last_cost);
		}
		free(transcript_formatted);
		transcript_formatted = wordwrap(raw);
		free(raw);
	}

	/* Generate full summary */
	if (needs_full && !EMPTY(transcript_formatted)) {
		if (!g_fix_mode) {
			printf("Generating full summary for %s", video_id);
			fflush(stdout);
		}
		g_last_http_error[0] = '\0';
		g_last_cost = 0;
		char *raw = generate_with_claude(
			"Create a detailed full summary of this video. Cover all major "
			"sections, arguments, and insights in a structured narrative. "
			"Make it nicely formatted with headings, paragraphs, and bullets "
			"where appropriate.",
			cached_prefix, model, 16000);
		if (g_fix_mode) {
			if (g_last_http_error[0])
				buf_printf(&fix_line, " summary_full:%s", g_last_http_error);
			else
				buf_printf(&fix_line, " summary_full:$%.4f", g_last_cost);
		} else {
			printf(" $%.4f\n", g_last_cost);
		}
		free(summary_full);
		summary_full = wordwrap(raw);
		free(raw);
	}

	/* Generate short summary */
	if (needs_short && (!EMPTY(summary_full) || !EMPTY(transcript_formatted))) {
		if (!g_fix_mode) {
			printf("Generating short summary for %s", video_id);
			fflush(stdout);
		}
		g_last_http_error[0] = '\0';
		g_last_cost = 0;
		char *raw = generate_with_claude(
			"Create a concise one-paragraph summary of this video. "
			"Capture key points, main ideas, and conclusions.",
			cached_prefix, model, 500);
		if (g_fix_mode) {
			if (g_last_http_error[0])
				buf_printf(&fix_line, " summary_short:%s", g_last_http_error);
			else
				buf_printf(&fix_line, " summary_short:$%.4f", g_last_cost);
		} else {
			printf(" $%.4f\n", g_last_cost);
		}
		free(summary_short);
		summary_short = wordwrap(raw);
		free(raw);
	}

	free(cached_prefix);

	/* In fix mode, print collected actions as one line */
	if (g_fix_mode && fix_line.len > 0) {
		printf(" %s\n", fix_line.data);
		buf_free(&fix_line);
	} else if (g_fix_mode) {
		printf("\n");
		buf_free(&fix_line);
	}

	/* Upsert */
	sqlite3_prepare_v2(db,
		"INSERT OR REPLACE INTO videos "
		"(video_id, title, upload_date, duration, description, channel_id, "
		"language, raw_transcript, transcript_formatted, summary_short, "
		"summary_full, fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
		-1, &st, NULL);
	sqlite3_bind_text(st,  1, video_id, -1, SQLITE_STATIC);
	sqlite3_bind_text(st,  2, title, -1, SQLITE_STATIC);
	sqlite3_bind_text(st,  3, upload_date, -1, SQLITE_STATIC);
	sqlite3_bind_text(st,  4, duration, -1, SQLITE_STATIC);
	sqlite3_bind_text(st,  5, description, -1, SQLITE_STATIC);
	sqlite3_bind_text(st,  6, channel_id, -1, SQLITE_STATIC);
	sqlite3_bind_text(st,  7, language, -1, SQLITE_STATIC);
	sqlite3_bind_text(st,  8, raw_transcript, -1, SQLITE_STATIC);
	sqlite3_bind_text(st,  9, transcript_formatted, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 10, summary_short, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 11, summary_full, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 12, fetched_at, -1, SQLITE_STATIC);
	sqlite3_step(st);
	sqlite3_finalize(st);

	free(title); free(upload_date); free(duration); free(description);
	free(channel_id); free(language); free(raw_transcript);
	free(transcript_formatted); free(summary_short); free(summary_full);
	free(fetched_at); free(channel_name); free(channel_handle);
}

/* ── main ── */

int main(int argc, char **argv)
{
	bool browse = false, fix = false;
	const char *skip_str = NULL;

	static struct option longopts[] = {
		{"browse",  no_argument, NULL, 'b'},
		{"fix",     no_argument, NULL, 'f'},
		{"model",   required_argument, NULL, 'm'},
		{"skip",    required_argument, NULL, 's'},
		{"help",    no_argument, NULL, 'h'},
		{"version", no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int opt;
	while ((opt = getopt_long(argc, argv, "bfm:s:hV", longopts, NULL)) != -1) {
		switch (opt) {
		case 'b': browse = true; break;
		case 'f': fix = true; break;
		case 'm': model = optarg; break;
		case 's': skip_str = optarg; break;
		case 'V':
			printf("ytran 1.0\n");
			return 0;
		case 'h':
			printf("Usage: ytran [OPTIONS] [VIDEO_URL_OR_ID ...]\n"
			       "  --browse        Browse the transcript database\n"
			       "  --fix           Fill in missing fields\n"
			       "  --model MODEL   Claude model [%s]\n"
			       "  --skip IDs      Comma-separated video IDs to skip\n"
			       "  --version       Show version\n", model);
			return 0;
		default: return 1;
		}
	}

	/* Build DB path */
	char *xdg = xdg_data_home();
	char *db_dir = NULL, *db_file = NULL;
	if (asprintf(&db_dir, "%s/ytran", xdg) < 0 ||
	    asprintf(&db_file, "%s/youtube-transcripts.db", db_dir) < 0) {
		fprintf(stderr, "Out of memory\n");
		return 1;
	}
	free(xdg);

	if (browse) {
		execlp("browse-sqlite3", "browse-sqlite3", db_file, "videos", (char *)NULL);
		perror("browse-sqlite3");
		return 1;
	}

	int nvids = argc - optind;
	if (!fix && nvids < 1) {
		fprintf(stderr, "Usage: ytran [--browse|--fix] [VIDEO_URL_OR_ID ...]\n");
		return 1;
	}

	/* Read API keys */
	char *home = getenv("HOME");
	char path[512];
	snprintf(path, sizeof(path), "%s/.youtubetotranscript_api_key", home);
	transcript_api_key = read_file_trimmed(path);
	snprintf(path, sizeof(path), "%s/.anthropic_api_key", home);
	anthropic_api_key = read_file_trimmed(path);
	if (!transcript_api_key) {
		fprintf(stderr, "Error: %s/.youtubetotranscript_api_key not found\n", home);
		fprintf(stderr, "Get a key at https://transcriptapi.com and save it to that file.\n");
		return 1;
	}
	if (!anthropic_api_key) {
		fprintf(stderr, "Error: %s/.anthropic_api_key not found\n", home);
		fprintf(stderr, "Get a key at https://console.anthropic.com/settings/keys and save it to that file.\n");
		return 1;
	}

	/* Create DB directory and open */
	mkdir(db_dir, 0755);
	sqlite3 *db;
	if (sqlite3_open(db_file, &db) != SQLITE_OK) {
		fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
		return 1;
	}
	db_init(db);

	curl_global_init(CURL_GLOBAL_DEFAULT);

	/* Parse skip IDs into array */
	char **skip_ids = NULL;
	int nskip = 0;
	if (skip_str) {
		char *tmp = strdup(skip_str);
		for (char *tok = strtok(tmp, ","); tok; tok = strtok(NULL, ",")) {
			while (*tok == ' ') tok++;
			skip_ids = realloc(skip_ids, (nskip + 1) * sizeof(char *));
			skip_ids[nskip++] = strdup(tok);
		}
		free(tmp);
	}

	if (fix) {
		g_fix_mode = true;
		/* Collect all IDs first to avoid modifying table while iterating */
		sqlite3_stmt *st;
		sqlite3_prepare_v2(db,
			"SELECT video_id, title FROM videos WHERE "
			"title = '' OR upload_date = '' OR duration = '' OR description = '' OR "
			"channel_id = '' OR language = '' OR raw_transcript = '' OR "
			"transcript_formatted = '' OR summary_short = '' OR summary_full = '' OR "
			"title IS NULL OR upload_date IS NULL OR duration IS NULL OR "
			"description IS NULL OR channel_id IS NULL OR language IS NULL OR "
			"raw_transcript IS NULL OR transcript_formatted IS NULL OR "
			"summary_short IS NULL OR summary_full IS NULL OR length(description) <= 200",
			-1, &st, NULL);
		typedef struct { char *id; char *title; } FixEntry;
		FixEntry *fix_entries = NULL;
		int nfix = 0;
		while (sqlite3_step(st) == SQLITE_ROW) {
			const char *vid = (const char *)sqlite3_column_text(st, 0);
			bool skip = false;
			for (int i = 0; i < nskip; i++)
				if (strcmp(vid, skip_ids[i]) == 0) { skip = true; break; }
			if (!skip) {
				fix_entries = realloc(fix_entries, (nfix + 1) * sizeof(FixEntry));
				fix_entries[nfix].id = strdup(vid);
				const char *t = (const char *)sqlite3_column_text(st, 1);
				fix_entries[nfix].title = (t && *t) ? strdup(t) : NULL;
				nfix++;
			}
		}
		sqlite3_finalize(st);
		for (int i = 0; i < nfix; i++) {
			printf("%s %s", fix_entries[i].id,
				fix_entries[i].title ? fix_entries[i].title : "");
			fflush(stdout);
			process_video(fix_entries[i].id, db);
			free(fix_entries[i].id);
			free(fix_entries[i].title);
		}
		free(fix_entries);

		/* Populate channel_names for channels missing entries */
		typedef struct { char *ch_id; char *vid; } ChFix;
		ChFix *ch_fixes = NULL;
		int nch = 0;
		sqlite3_prepare_v2(db,
			"SELECT v.channel_id, MIN(v.video_id) "
			"FROM videos v LEFT JOIN channel_names cn ON v.channel_id = cn.channel_id "
			"WHERE v.channel_id IS NOT NULL AND v.channel_id <> '' "
			"AND cn.channel_id IS NULL GROUP BY v.channel_id",
			-1, &st, NULL);
		while (sqlite3_step(st) == SQLITE_ROW) {
			ch_fixes = realloc(ch_fixes, (nch + 1) * sizeof(ChFix));
			ch_fixes[nch].ch_id = strdup((const char *)sqlite3_column_text(st, 0));
			ch_fixes[nch].vid = strdup((const char *)sqlite3_column_text(st, 1));
			nch++;
		}
		sqlite3_finalize(st);
		for (int i = 0; i < nch; i++) {
			g_last_http_error[0] = '\0';
			Metadata m = fetch_youtube_metadata(ch_fixes[i].vid);
			if (g_last_http_error[0])
				printf("channel %s via %s: %s\n", ch_fixes[i].ch_id, ch_fixes[i].vid, g_last_http_error);
			else
				printf("channel %s via %s\n", ch_fixes[i].ch_id, ch_fixes[i].vid);
			time_t now = time(NULL);
			struct tm *tm = localtime(&now);
			char ts[32];
			strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%S", tm);
			if (m.channel_name) {
				sqlite3_stmt *ins;
				sqlite3_prepare_v2(db,
					"INSERT INTO channel_names (channel_id, name_type, name, timestamp) "
					"VALUES (?, 'fullname', ?, ?)", -1, &ins, NULL);
				sqlite3_bind_text(ins, 1, ch_fixes[i].ch_id, -1, SQLITE_STATIC);
				sqlite3_bind_text(ins, 2, m.channel_name, -1, SQLITE_STATIC);
				sqlite3_bind_text(ins, 3, ts, -1, SQLITE_STATIC);
				sqlite3_step(ins);
				sqlite3_finalize(ins);
			}
			if (m.channel_handle) {
				sqlite3_stmt *ins;
				sqlite3_prepare_v2(db,
					"INSERT INTO channel_names (channel_id, name_type, name, timestamp) "
					"VALUES (?, 'handle', ?, ?)", -1, &ins, NULL);
				sqlite3_bind_text(ins, 1, ch_fixes[i].ch_id, -1, SQLITE_STATIC);
				sqlite3_bind_text(ins, 2, m.channel_handle, -1, SQLITE_STATIC);
				sqlite3_bind_text(ins, 3, ts, -1, SQLITE_STATIC);
				sqlite3_step(ins);
				sqlite3_finalize(ins);
			}
			metadata_free(&m);
			free(ch_fixes[i].ch_id);
			free(ch_fixes[i].vid);
		}
		free(ch_fixes);
		printf("Fix complete\n");
	}

	if (!fix && nvids > 0) {
		for (int vi = optind; vi < argc; vi++) {
			char *video_id = extract_youtube_id(argv[vi]);
			if (!video_id) continue;

			/* Check if already exists */
			sqlite3_stmt *st;
			sqlite3_prepare_v2(db,
				"SELECT raw_transcript FROM videos WHERE video_id = ?", -1, &st, NULL);
			sqlite3_bind_text(st, 1, video_id, -1, SQLITE_STATIC);
			bool exists = false;
			if (sqlite3_step(st) == SQLITE_ROW) {
				const char *rt = (const char *)sqlite3_column_text(st, 0);
				if (rt && *rt) exists = true;
			}
			sqlite3_finalize(st);

			if (exists) {
				printf("Transcript already downloaded for %s. Skipping.\n", video_id);
			} else {
				process_video(video_id, db);
			}
			free(video_id);
		}
	}

	sqlite3_close(db);
	curl_global_cleanup();
	if (total_cost > 0)
		printf("Total API cost: $%.4f\n", total_cost);
	printf("Database updated: %s\n", db_file);
	free(db_dir);
	free(db_file);
	for (int i = 0; i < nskip; i++) free(skip_ids[i]);
	free(skip_ids);
	free(transcript_api_key);
	free(anthropic_api_key);
	return 0;
}
