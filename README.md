# Aurora Ingestion Pipeline (`aurora-ingestion`)

`aurora-ingestion` is a standalone Python service for Aurora's External Data Ingestion Pipeline.

It bridges external data sources (Email, Web, PDFs, YouTube, RSS, etc.) and Aurora's Obsidian vault brain. The service fetches external items, extracts content, formats notes according to Aurora's Markdown & YAML frontmatter standards, manages attachments, prevents duplicate ingestions using SQLite tracking, and writes files directly into the configured Obsidian vault.

Downstream indexing, embedding into ChromaDB, and semantic search are handled automatically by Aurora's `VaultManager` and file watcher.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   EXTERNAL DATA SOURCES                     │
│  Email · Web Pages · PDFs · YouTube · RSS · Notion · etc.   │
└───────────────────────────┬─────────────────────────────────┘
                            │
              aurora-ingestion Pipeline
              1. BaseSource connector fetches items
              2. DeduplicationTracker (SQLite) checks hash
                 - UNCHANGED: Skip
                 - NEW: Assign unique filename & write
                 - CHANGED: Overwrite note in-place
              3. Converter formats YAML frontmatter & Markdown
              4. Attachments stored in Attachments/Ingested/
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                 OBSIDIAN VAULT (.md FILES)                  │
│       Dropped into: ${AURORA_VAULT_PATH}/Ingested/<source>/ │
└───────────────────────────┬─────────────────────────────────┘
                            │
                    AURORA BACKEND
                            ▼
┌─────────────────────────────────────────────────────────────┐
│     VaultManager → FileWatcher → ChromaDB → AI Context      │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
aurora-ingestion/
├── main.py               # CLI entrypoint and IngestionPipeline orchestrator
├── config.py             # Vault path resolution, folder mappings, and security rules
├── models.py             # Data models (SourceItem, MarkdownNote, Attachment, TrackingRecord)
├── tracker.py            # SQLite deduplication store and state tracking
├── converter.py          # Markdown note writer, frontmatter generator, attachment handler
├── exceptions.py         # Custom exception hierarchy
├── sources/
│   ├── __init__.py       # Sources package export
│   └── base.py           # BaseSource abstract interface, SourceRegistry, async_retry
├── tests/
│   ├── __init__.py
│   ├── test_config.py      # Vault path & folder configuration tests
│   ├── test_converter.py   # Sanitization, frontmatter, and writer tests
│   ├── test_tracker.py     # SQLite tracker lifecycle and deduplication tests
│   ├── test_attachments.py # Attachment saving and embed tests
│   ├── test_pipeline.py    # Pipeline orchestration and retry tests
│   └── test_cli.py         # CLI argument parser tests
├── requirements.txt      # Project dependencies
├── .env.example          # Environment variable template
└── README.md             # Service documentation
```

---

## Vault Folder Topology

Notes are routed based on source type:

```
${AURORA_VAULT_PATH}/
├── Inbox/                    ← Fallback / uncategorized notes
├── Ingested/
│   ├── Email/                ← Emails (Gmail, Outlook)
│   ├── Web/                  ← Web clippings & articles
│   ├── PDF/                  ← Extracted PDF content
│   ├── YouTube/              ← YouTube transcripts & summaries
│   ├── RSS/                  ← RSS feed articles
│   ├── Social/               ← Twitter/X, Reddit
│   ├── Chat/                 ← Slack, Discord, Telegram
│   ├── Voice/                ← Transcribed voice memos
│   ├── Screenshots/          ← OCR'd screenshots
│   ├── Code/                 ← GitHub issues, gists, snippets
│   └── Notes/                ← Notion, Google Keep
└── Attachments/
    └── Ingested/             ← Media and files referenced via ![[filename]]
```

> **Security Rule**: The service strictly forbids writes into `.obsidian`, `.trash`, `.git`, or hidden dot-folders.

---

## Markdown & Frontmatter Standard

Every note conforms to the following structure:
- **Valid YAML Frontmatter**:
  - `title`: Display title
  - `date`: Creation/publication date (ISO 8601)
  - `source`: Source identifier
  - `tags`: Enforces `ingested` as the first tag
  - `ingested_at`: UTC timestamp of ingestion
  - Recommended & source metadata: `source_url`, `author`, `status`, `summary`, `attachments`, etc.
- **Body**:
  - Starts with `# Title` matching frontmatter
  - Source attribution blockquote (`> **Source**: ...`)
  - Pure Markdown (no HTML)
  - Attachment embeds formatted as `![[filename.ext]]`
  - UTF-8 encoding with no BOM
- **Naming**:
  - Format: `<YYYY-MM-DD>_<source>_<sanitized-title>.md`
  - Title portion truncated to at most 80 characters
  - Spaces replaced with `_`
  - Forbidden characters (`? : * " < > | / \`) stripped
  - New note collisions resolved with `_2`, `_3`, etc.
  - Updates overwrite the existing file in-place.

---

## Setup & Installation

### 1. Create and Activate Virtual Environment

```bash
python -m venv .venv

# Windows:
.\.venv\Scripts\activate

# macOS / Linux:
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Configure `AURORA_VAULT_PATH` in `.env`:

```env
AURORA_VAULT_PATH=C:/path/to/your/ObsidianVault
# Or on Linux/macOS:
# AURORA_VAULT_PATH=/path/to/your/ObsidianVault
```

---

## CLI Usage

### Initialize Vault Folders

Pre-creates the standard `Ingested/*` and `Attachments/Ingested` folder hierarchy:

```bash
python main.py init-vault
```

### List Registered Connectors

```bash
python main.py list-sources
```

### Check Ingestion Status

Inspect tracked items in the SQLite database:

```bash
python main.py status
python main.py status --source email
```

### Run Ingestion

```bash
# Ingest a PDF document:
python main.py ingest --source pdf --file "path/to/report.pdf"

# Ingest all PDFs from a folder:
python main.py ingest --source pdf --file "path/to/pdf_folder"

# Ingest a Web article:
python main.py ingest-web "https://example.com/article"
# Or using generic CLI syntax:
python main.py ingest --source web --url "https://example.com/article"

# Ingest a YouTube video:
python main.py ingest-youtube "https://www.youtube.com/watch?v=VIDEO_ID"
# Or using generic CLI syntax:
python main.py ingest --source youtube --url "https://www.youtube.com/watch?v=VIDEO_ID"

# Ingest an Email thread (Gmail):
python main.py ingest-email --provider gmail --thread-id "GMAIL_THREAD_ID"

# Ingest an Email conversation (Outlook):
python main.py ingest-email --provider outlook --thread-id "OUTLOOK_CONVERSATION_ID"
# Or using generic CLI syntax:
python main.py ingest --source email --provider gmail --thread-id "GMAIL_THREAD_ID"
```

---

## PDF Source Connector (`sources/pdf_source.py`)

The PDF connector is the first production data source implemented for the Aurora pipeline.

### Capabilities:
- **Text & Metadata Extraction**: Powered by `pymupdf` (`fitz`), extracting document title, author, creation date, and page count.
- **Table Extraction**: Uses `pdfplumber` to detect tables on each page and convert them to standard Markdown tables.
- **Obsidian Note Generation**: Notes are written to `${AURORA_VAULT_PATH}/Ingested/PDF/` with page anchors (`[p.1]`, `[p.2]`) and document structure.
- **Original Document Attachment**: The original PDF binary is preserved under `${AURORA_VAULT_PATH}/Attachments/Ingested/<filename>.pdf` and embedded into the note via `![[filename.pdf]]`.
- **SHA-256 Deduplication**: Calculates the exact SHA-256 hash of the PDF file.
  - **First Run**: Note created, attachment saved, tracked in SQLite.
  - **Subsequent Run (Unchanged)**: Skipped completely without disk writes.
  - **Subsequent Run (Modified)**: Overwrites the existing Markdown note in-place and updates the tracker.
- **Batch / Directory Processing**: Can ingest a single `.pdf` file or an entire directory of `.pdf` files.

### Known Limitations:
- **Large Document Splitting**: PDFs over 5,000 words are currently kept in a single consolidated note. Sub-document splitting by chapter/section will be added in a future enhancement.
- **Scanned Images (OCR)**: Scanned PDFs containing only raster images without embedded text layers require the upcoming OCR connector (`pytesseract` / vision LLM).

---

## Web Source Connector (`sources/web_source.py`)

The Web connector ingests online articles, blog posts, and documentation into Aurora's Obsidian vault.

### Capabilities:
- **Main Article Extraction**: Uses `trafilatura` to extract primary article text while discarding navigational chrome, ads, footers, sidebars, and cookie popups.
- **Fallback Extraction**: Employs `BeautifulSoup` and `markdownify` when encountering atypical HTML layouts or lightweight single-page markup.
- **Rich Content Preservation**: Preserves ATX headings, ordered/unordered lists, code snippets, inline links, and blockquotes.
- **Image Discovery & Downloading**: Detects embedded images, skips tracking pixels (`1x1`, `pixel.gif`, analytics badges) and data URIs, downloads article images into `${AURORA_VAULT_PATH}/Attachments/Ingested/`, and embeds them using Obsidian `![[filename.ext]]` syntax.
- **Resilient Image Fetching**: If an image fails to download (e.g. 404 or CDN timeout), a warning is logged without failing note ingestion.
- **URL Normalization**: Normalizes URLs by lowercasing schemes and hostnames, stripping anchor fragments (`#section`), and trimming trailing slashes.
- **Deduplication & In-Place Updates**:
  - `source_id`: `web:<normalized-url>`
  - `content_hash`: SHA-256 of extracted content + downloaded image digests.
  - **Unchanged**: Skips re-ingestion if content is unmodified.
  - **Changed**: Overwrites the existing note in-place in `${AURORA_VAULT_PATH}/Ingested/Web/` and updates tracking records.

### Known Limitations:
- **JavaScript Single-Page Apps (SPAs)**: Heavy client-rendered SPAs requiring headless browser execution (Puppeteer/Playwright) are not executed; standard server-rendered HTML or static pages are processed.
- **Paywalled / Authenticated Content**: Sites requiring user authentication or subscription sessions require future cookie or session-sharing integration.

---

## YouTube Source Connector (`sources/youtube_source.py`)

The YouTube connector ingests video metadata, transcripts, and thumbnails into Aurora's Obsidian vault.

### Capabilities:
- **Flexible URL Parsing & Normalization**: Supports standard (`watch?v=`), shortlink (`youtu.be/`), and Shorts (`shorts/`) formats with query parameters. Normalizes to canonical video identity `youtube:<video_id>`.
- **Metadata Extraction**: Powered by `yt-dlp` to extract title, channel/author, upload date, description, and duration.
- **Transcript Extraction with Timestamps**: Uses `youtube-transcript-api` to pull transcripts (prioritizing manual transcripts before auto-generated transcripts) and groups speech segments into readable paragraphs marked with human-readable timestamps (`**[00:00]**`, `**[00:35]**`).
- **Thumbnail Attachment Handling**: Automatically downloads the video thumbnail into `${AURORA_VAULT_PATH}/Attachments/Ingested/` and embeds it in the note via `![[video_id_thumbnail.jpg]]`.
- **Fault-Tolerant Thumbnail Ingestion**: If a thumbnail fails to download, a warning is logged and ingestion completes successfully without failing the note.
- **Deduplication & In-Place Updates**:
  - `source_id`: `youtube:<video_id>`
  - `content_hash`: SHA-256 of title + body (transcript & metadata) + thumbnail bytes.
  - **Unchanged**: Skips re-ingestion if transcript and metadata remain identical.
  - **Changed**: Overwrites the existing note in-place in `${AURORA_VAULT_PATH}/Ingested/YouTube/` and updates tracking records.

### Known Limitations:
- **Playlists Not Supported**: Ingests single video URLs. Ingesting whole playlists as a batch or single note is not supported.
- **Videos Without Transcripts**: Videos with disabled or unavailable transcripts cannot be ingested since transcripts are the core content.

---

## Email Source Connector (`sources/email_source.py` and `sources/email/`)

The Email connector ingests full email threads and conversations from Gmail and Microsoft Outlook into Aurora's Obsidian vault.

### Capabilities:
- **Provider Architecture**: Abstract `EmailProvider` base class with concrete implementations for `GmailProvider` (Google Gmail API v1) and `OutlookProvider` (Microsoft Graph API v1.0).
- **Thread Consolidation**: Consolidates entire email conversations into a single Markdown note (`Ingested/Email/<note>.md`), presenting messages chronologically with sender headers (`### From: Name <email>`), timestamps, and clean body text.
- **HTML & Plain Text Sanitization**: Parses multi-part MIME messages, converting HTML emails to clean Markdown (`markdownify`) while stripping tracking pixels, script tags, style blocks, and boilerplate.
- **Attachment Ingestion & Obsidian Embeds**: Extracts non-tracking attachments (PDFs, images, docs) into `${AURORA_VAULT_PATH}/Attachments/Ingested/` with collision-safe naming and embeds them via `![[filename.ext]]`.
- **Inline Image Handling**: Resolves `cid:` references in HTML emails to the corresponding extracted attachments.
- **Tracking Pixel Filtering**: Filters out 1x1 tracking pixels, analytics beacons, spacer GIFs, and sub-100-byte images based on filename patterns, MIME types, and dimensions.
- **Stable Identity**:
  - Gmail: `email:gmail:<thread_id>`
  - Outlook: `email:outlook:<conversation_id>`
- **Deduplication & In-Place Update Lifecycle**:
  - `content_hash`: SHA-256 of thread subject + concatenated message contents + attachment digests.
  - **Unchanged**: Skips re-ingestion if no new replies or edits occurred.
  - **Changed**: Overwrites the existing note in-place in `${AURORA_VAULT_PATH}/Ingested/Email/` when replies are added to the thread, updating tracking records without creating duplicate notes.
- **OAuth Authentication**:
  - Gmail: Supports `credentials.json` with token refresh flow (`token.json`), or environment variables `GMAIL_CREDENTIALS_PATH` / `GMAIL_TOKEN_PATH`.
  - Outlook: Supports direct pre-acquired delegated user access token (`OUTLOOK_ACCESS_TOKEN`) or interactive delegated Device Code Flow via MSAL `PublicClientApplication` with `OUTLOOK_CLIENT_ID` and `OUTLOOK_TENANT_ID` (default: `common`). Requests read-only `Mail.Read` delegated scope with persistent token cache (`OUTLOOK_TOKEN_CACHE_PATH`). No client secrets are needed or logged.

### Known Limitations:
- **Encrypted S/MIME Messages**: S/MIME and PGP encrypted email payloads cannot be decrypted without local client private keys.
- **Calendar Invites**: `.ics` calendar invitation payloads are preserved as attachments rather than converted to interactive calendar events.

---

## Running Tests

Run the complete test suite with `pytest`:

```bash
pytest -v
```

All 93 unit and integration tests verify:
- Vault path expansion and validation
- Sanitization and 80-character title truncation
- Frontmatter generation with `ingested` tag enforcement
- Markdown body structure (H1, attribution block, UTF-8 no BOM)
- Collision handling and unique filename suffixes
- In-place file overwrites on content change
- Excluded folder write prevention (`.obsidian`, `.trash`, `.git`)
- Attachment saving and Obsidian embed syntax (`![[...]]`)
- SQLite deduplication lifecycle (`NEW`, `UNCHANGED`, `CHANGED`)
- Async retry decorator with exponential backoff
- PDF text, metadata, and table extraction via `pymupdf` & `pdfplumber`
- PDF SHA-256 deduplication and in-place update lifecycle
- PDF invalid, corrupt, and empty file error handling
- PDF CLI ingestion and directory batching
- Web connector registration and URL normalization
- Web content extraction and metadata fallbacks
- Web image download and tracking pixel filtering
- Web image download positive flow and vault attachment saving
- Web note generation under `Ingested/Web/`
- Web deduplication (unchanged vs. changed update lifecycle)
- Web HTTP error handling (404, 403, 429, timeouts, connection errors)
- Web CLI subcommands (`ingest-web` and `ingest --source web --url`)
- YouTube connector registration and URL parsing (standard, short, shorts, query params)
- YouTube invalid URL, missing ID, and playlist rejection
- YouTube duration and transcript timestamp formatting (`[00:00]`, `[00:35]`)
- YouTube metadata and transcript extraction
- YouTube thumbnail download and vault attachment saving
- YouTube thumbnail download failure resilience
- YouTube missing transcript and unavailable video error handling
- YouTube deduplication and in-place update lifecycle
- YouTube CLI subcommands (`ingest-youtube` and `ingest --source youtube --url`)
- Email connector registration and CLI source listing
- Email date parsing across RFC 2822 and ISO 8601 formats
- Email HTML to Markdown conversion and sanitization
- Email tracking pixel and analytics beacon filtering
- Gmail MIME structure parsing, base64url decoding, and thread identity
- Gmail error handling (401, 403, 404, network errors)
- Outlook Graph API message parsing, base64 attachment extraction, and conversation identity
- Outlook error handling (401, 403, 404, network errors)
- Outlook explicit access token parameters and environment variable override
- Outlook MSAL delegated Device Code Flow authentication and authority resolution
- Outlook MSAL token caching and silent acquisition
- Outlook MSAL error mapping to SourceError
- Outlook authentication security (zero token leaking in stdout, stderr, or logs)
- Email pipeline end-to-end processing and attachment saving for Gmail and Outlook
- Email thread deduplication and in-place update lifecycle on new replies
- Email attachment collision resolution and embed reference updating
- Email CLI commands (`ingest-email --provider gmail|outlook` and `ingest --source email`)

---

## Implementation Status & Next Steps

### Completed:
- [x] Foundation architecture (`config`, `models`, `tracker`, `converter`, `exceptions`).
- [x] Extensible configuration supporting `AURORA_VAULT_PATH` & `${AURORA_VAULT_PATH}`.
- [x] Abstract `BaseSource` interface and `SourceRegistry`.
- [x] SQLite deduplication tracker (`NEW`, `UNCHANGED`, `CHANGED`).
- [x] **PDF Source Connector** (`sources/pdf_source.py`):
  - [x] Text extraction with `pymupdf`
  - [x] Table formatting with `pdfplumber`
  - [x] Metadata (title, author, creation date, page count, file size)
  - [x] Preserving original PDF as attachment in `Attachments/Ingested/`
  - [x] SHA-256 deduplication & in-place note updates
  - [x] CLI `--file` ingestion
- [x] **Web Source Connector** (`sources/web_source.py`):
  - [x] Main content extraction with `trafilatura` & `markdownify`
  - [x] Metadata extraction (title, author, publish date, description)
  - [x] Image downloading to `Attachments/Ingested/` with tracking pixel filtering
  - [x] Markdown notes saved to `Ingested/Web/` with frontmatter and Obsidian embeds
  - [x] URL normalization and SHA-256 deduplication lifecycle
  - [x] CLI `ingest-web` and `ingest --source web --url` commands
- [x] **YouTube Source Connector** (`sources/youtube_source.py`):
  - [x] Video ID extraction for standard, youtu.be, and Shorts URLs
  - [x] Metadata extraction with `yt-dlp` (title, channel, date, description, duration)
  - [x] Transcript extraction with `youtube-transcript-api` and timestamp formatting
  - [x] Thumbnail download to `Attachments/Ingested/` and Markdown embed
  - [x] Notes saved to `Ingested/YouTube/` with YAML frontmatter
  - [x] Video ID-based SHA-256 deduplication & in-place update lifecycle
  - [x] CLI `ingest-youtube` and `ingest --source youtube --url` commands
- [x] **Email Source Connector** (`sources/email_source.py` and `sources/email/`):
  - [x] Gmail thread ingestion via Gmail API with MIME traversal
  - [x] Outlook conversation ingestion via Microsoft Graph API
  - [x] Delegated MSAL Device Code Flow & token caching (`Mail.Read` read-only scope)
  - [x] Clean Markdown conversion for multi-part messages
  - [x] Attachment saving to `Attachments/Ingested/` with tracking pixel filtering
  - [x] Notes saved to `Ingested/Email/` with YAML frontmatter
  - [x] Thread-based SHA-256 deduplication & in-place note updates
  - [x] CLI `ingest-email` and `ingest --source email` commands
- [x] 100% test pass rate across all 93 tests.

### Next Connectors to Implement:
1. **Tier 2 Connectors**: RSS, Notion, Google Keep, Readwise.
2. **Tier 3 Connectors**: Twitter/X, Reddit, Slack/Discord/Telegram, Audio Whisper transcription, OCR screenshots, GitHub.
