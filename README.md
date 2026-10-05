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
│   ├── base.py           # BaseSource abstract interface, SourceRegistry, async_retry
│   ├── email_source.py   # Email thread connector (Gmail, Outlook)
│   ├── notion_source.py  # Notion page connector
│   ├── pdf_source.py     # PDF document connector
│   ├── rss_source.py     # RSS/Atom syndication connector
│   ├── web_source.py     # Web article connector
│   └── youtube_source.py # YouTube video connector
├── tests/
│   ├── __init__.py
│   ├── test_attachments.py # Attachment saving and embed tests
│   ├── test_cli.py         # CLI argument parser tests
│   ├── test_config.py      # Vault path & folder configuration tests
│   ├── test_converter.py   # Sanitization, frontmatter, and writer tests
│   ├── test_email_source.py# Email connector tests
│   ├── test_notion_source.py# Notion connector tests
│   ├── test_pdf_source.py  # PDF connector tests
│   ├── test_pipeline.py    # Pipeline orchestration and retry tests
│   ├── test_rss_source.py  # RSS connector tests
│   ├── test_tracker.py     # SQLite tracker lifecycle and deduplication tests
│   ├── test_web_source.py  # Web connector tests
│   └── test_youtube_source.py # YouTube connector tests
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

# Ingest an RSS or Atom feed:
python main.py ingest-rss "https://example.com/feed.xml"
# Or using generic CLI syntax:
python main.py ingest --source rss --url "https://example.com/feed.xml"

# Ingest accessible Notion pages:
python main.py ingest-notion
# Ingest a specific Notion page by ID:
python main.py ingest-notion --page-id "PAGE_ID"
# Or using generic CLI syntax:
python main.py ingest --source notion
python main.py ingest --source notion --page-id "PAGE_ID"
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

## RSS / Atom Source Connector (`sources/rss_source.py`)

The RSS connector ingests articles and posts from RSS 2.0 and Atom syndication feeds into Aurora's Obsidian vault.

### Capabilities:
- **RSS 2.0 & Atom Feed Parsing**: Powered by `feedparser`, extracting feed-level metadata (feed title, feed URL, description, language) and item-level metadata (title, link, GUID/id, published/updated dates, author, summary, content, tags/categories).
- **Intelligent Article Content Resolution**:
  - **Case A (Full Content in Feed)**: When feeds supply full article HTML/text, it is converted directly to clean Markdown via `markdownify`.
  - **Case B (Summary / Excerpt Only)**: When feeds only provide a teaser or summary, fetches the article URL and uses `trafilatura` to extract the full article body.
  - **Case C (Fallback)**: If article URL is unavailable or full text extraction fails, gracefully falls back to the feed summary without failing note ingestion.
- **Obsidian Note Generation**: Notes are written to `${AURORA_VAULT_PATH}/Ingested/RSS/<note>.md` with YAML frontmatter, an H1 header matching the title, and an attribution blockquote (`> **Source**: [title](url) · **Feed**: [feed](url) · **Author**: ... · **Published**: ...`).
- **Image Discovery & Attachment Handling**: Scans article HTML for images, filters out tracking pixels (`1x1`, `pixel.gif`, analytics badges), downloads article images into `${AURORA_VAULT_PATH}/Attachments/Ingested/` with collision-safe naming, and embeds them via `![[filename.ext]]`.
- **URL Normalization**: Normalizes feed and article URLs by lowercasing scheme/host, stripping fragments (`#section`), removing default ports, and trimming trailing slashes while preserving necessary query parameters.
- **Category & Tag Sanitization**: Extracts feed and article tags/categories, sanitizes them to safe kebab-case Obsidian tags, and enforces the `rss` and `ingested` tags.
- **Deduplication & In-Place Updates**:
  - `source_id`: Prefers `rss:<guid>` if a GUID exists, falls back to `rss:url:<normalized_url>`, or stable feed URL + content hash fallback.
  - `content_hash`: SHA-256 of item title + body content + image digests.
  - **Unchanged**: Skips re-ingestion if item content remains identical.
  - **Changed**: Overwrites the existing note in-place in `${AURORA_VAULT_PATH}/Ingested/RSS/` and updates tracking records without creating duplicate notes.
- **Batch Processing & Fault Isolation**: Feeds containing multiple items are processed independently. An error in one article does not abort the remaining items. CLI reports discovered, created, skipped, updated, and failed item counts.

### Known Limitations:
- **Paywalled / Bot-Protected Articles**: When expanding summary-only items, sites behind Cloudflare challenge pages or subscription paywalls cannot be fetched by `trafilatura` (gracefully falls back to feed summary).
- **Podcast Enclosures**: Audio enclosures in podcast RSS feeds are not downloaded as local media attachments; episode show notes and metadata are captured as standard notes.

---

## Notion Source Connector (`sources/notion_source.py`)

The Notion connector ingests pages and database records accessible to a Notion internal integration token into Aurora's Obsidian vault.

### Capabilities:
- **Authentication via Integration Token**: Uses the official `notion-client` Python SDK with an internal integration token configured via `NOTION_TOKEN` (or `NOTION_API_KEY`). Tokens are securely handled, never logged, and dependency-injected for testing.
- **Page Discovery & Search**: Discovers accessible pages via Notion API search (`client.search`), with full cursor-based pagination. Individual pages can also be targeted by ID via `--page-id`.
- **Target Folder**: Writes notes directly to `${AURORA_VAULT_PATH}/Ingested/Notes/<YYYY-MM-DD>_notion_<title>.md`.
- **Comprehensive Block-to-Markdown Conversion**:
  - **Paragraphs**: Text with full inline rich text formatting.
  - **Headings**: `heading_1` (`#`), `heading_2` (`##`), `heading_3` (`###`).
  - **Bullet & Numbered Lists**: `bulleted_list_item` (`- item`), `numbered_list_item` (`1. item` with sequential numbering).
  - **To-Do Checklists**: `to_do` (`- [ ]` / `- [x]`).
  - **Toggles**: Converted to clean pure Markdown (`### Toggle Title` followed by content) without `<details>` or raw HTML.
  - **Quotes**: `quote` (`> text` with multi-line support).
  - **Callouts**: `callout` (`> 💡 text` with emoji icon preservation, without raw HTML).
  - **Code Blocks**: `code` (fenced code block with preserved language identifier).
  - **Tables**: `table` / `table_row` rendered as Markdown pipe tables with column alignment and padding.
  - **Rich Text Formatting**: Bold (`**`), italic (`*`), strikethrough (`~~`), code (`` ` ``), inline math (`$formula$`), and links (`[text](url)`).
  - **Images & Files**: Downloads images into `${AURORA_VAULT_PATH}/Attachments/Ingested/` with collision disambiguation and embeds them via `![[filename.ext]]`.
  - **Equations & Bookmarks**: Block math equations (`$$formula$$`) and bookmark/link previews (`[url](url)`).
  - **Child Pages**: Converted to descriptive Markdown references.
  - **Unsupported Blocks**: Handled gracefully without aborting note rendering.
  - **Recursive Children**: Recursively fetches child blocks with pagination support (`client.blocks.children.list`).
- **Metadata & Attribution**:
  - Frontmatter: `title`, `date`, `source: "notion"`, `tags: [ingested, notion, ...]`, `ingested_at`, `source_url`, `author`, `last_edited_time`, `page_id`, and `word_count`.
  - Attribution Block: `> **Source**: [Notion](url) · **Author**: ... · **Last Edited**: ...`.
- **Deduplication & In-Place Updates**:
  - `source_id`: `notion:<page_id>`.
  - `content_hash`: SHA-256 of title + body content + image digests.
  - **Unchanged**: Skips re-ingestion if `last_edited_time` and content are unchanged.
  - **Changed**: Overwrites the existing note in-place in `${AURORA_VAULT_PATH}/Ingested/Notes/` and updates tracking records without creating duplicate notes.
- **Batch Processing & Fault Isolation**: Multi-page search ingestion isolates failures to individual pages, allowing healthy pages to continue processing.

### Setup & Connecting Pages:
1. Go to [Notion Developers - My Integrations](https://www.notion.so/my-integrations) and create an **Internal Integration**.
2. Copy the "Internal Integration Secret" and set it in your environment:
   ```bash
   export NOTION_TOKEN="secret_..."
   # Or in .env:
   # NOTION_TOKEN=secret_...
   ```
3. In Notion, open any page or database you want Aurora to ingest, click `...` (Page Menu) → **Connections** / **Add connections**, and select your integration.

### Known Limitations:
- **Database Rows**: Database rows are ingested as individual page notes per row rather than a single database view.
- **Complex Relations**: Relation and rollup properties are represented as summarized text rather than bi-directional links.
- **Comments**: Inline comments and page discussion threads are not ingested.
- **Synced Blocks**: Notion `synced_block` containers are currently skipped rather than cross-referenced.

---

## Running Tests

Run the complete test suite with `pytest`:

```bash
pytest -v
```

All 152 unit and integration tests verify:
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
- RSS connector registration and URL scheme validation
- RSS 2.0 and Atom feed parsing with feedparser
- RSS metadata extraction (feed title, feed URL, author, published date)
- RSS content cases: Case A (full content), Case B (trafilatura expansion), Case C (summary fallback)
- RSS image downloading and tracking pixel filtering
- RSS stable identity derivation (GUID, normalized URL, and fallback hash)
- RSS category and tag sanitization
- RSS note generation under `Ingested/RSS/`
- RSS deduplication and in-place update lifecycle
- RSS empty, invalid, and malformed feed error handling
- RSS network and HTTP error handling (404, 500, timeouts)
- RSS batch feed processing with partial failure isolation
- RSS CLI commands (`ingest-rss` and `ingest --source rss --url`)
- RSS long summary without content field triggering trafilatura extraction
- RSS short content field using feed content without external network requests
- RSS duplicate image basename resolution with unique filenames and embed synchronization
- Notion connector registration (`notion`) and CLI source listing
- Notion authentication error handling on missing token
- Notion page discovery and pagination via `client.search`
- Notion page title extraction across Name, title, and fallbacks
- Notion frontmatter metadata (`title`, `date`, `source`, `tags`, `page_id`, `last_edited_time`, `word_count`)
- Notion block conversion: paragraphs, headings, bullet lists, numbered lists, to-dos
- Notion toggle conversion to pure Markdown headings without `<details>` HTML
- Notion quote and callout block conversion with emoji icon preservation
- Notion code blocks with language tag preservation
- Notion table and table row formatting as pipe tables with column padding
- Notion inline rich text formatting (bold, italic, strikethrough, code, equations, links)
- Notion image block download and Obsidian embed formatting (`![[filename.ext]]`)
- Notion attachment collision disambiguation for identical basenames
- Notion hosted and external file block download and Obsidian embed formatting (`![[filename.ext]]`)
- Notion file block download failure fallback to normal Markdown link without aborting note
- Notion file attachment filename collision disambiguation (`report.pdf`, `report_2.pdf`)
- Notion file extension inference from Content-Type header when extension is missing
- Notion file attachments inclusion in `MarkdownNote.attachments`
- Notion unsupported block graceful fallback
- Notion block children pagination traversal (`client.blocks.children.list`)
- Notion page URL attribution block formatting
- Notion new page ingestion in `Ingested/Notes/`
- Notion unchanged page deduplication skipping
- Notion modified page in-place note overwrite without duplicate notes
- Notion batch isolation preventing single-page failure from aborting search batch
- Notion API error, 401 auth, 403 permissions, 404 not found, and 429 rate limit mapping to SourceError
- Notion CLI commands (`ingest-notion` with `--page-id` and generic `ingest --source notion`)

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
- [x] **RSS / Atom Source Connector** (`sources/rss_source.py`):
  - [x] RSS 2.0 and Atom feed parsing with `feedparser`
  - [x] Feed and item metadata extraction (title, link, GUID, dates, author, summary, tags)
  - [x] Full content vs. `trafilatura` summary expansion vs. summary fallback
  - [x] Image downloading to `Attachments/Ingested/` with tracking pixel filtering
  - [x] Notes saved to `Ingested/RSS/` with frontmatter and attribution blockquotes
  - [x] Stable identity (`rss:<guid>` / `rss:url:<normalized_url>`) & SHA-256 deduplication lifecycle
  - [x] Independent item batch processing with failure isolation
  - [x] CLI `ingest-rss` and `ingest --source rss --url` commands
- [x] **Notion Source Connector** (`sources/notion_source.py`):
  - [x] Integration token authentication via `NOTION_TOKEN` (secure, never logged)
  - [x] Page discovery and pagination (`client.search`)
  - [x] Rich block tree conversion to pure Markdown (paragraphs, headings, lists, todos, toggles, quotes, callouts, code, tables)
  - [x] Image and file downloading to `Attachments/Ingested/` with collision-safe naming and Obsidian embeds
  - [x] Notes saved to `Ingested/Notes/` with YAML frontmatter and attribution blockquotes
  - [x] Stable identity (`notion:<page_id>`) & in-place update lifecycle
  - [x] Batch processing with individual page error isolation
  - [x] CLI `ingest-notion` and `ingest --source notion` commands
- [x] 100% test pass rate across all 152 tests.

### Next Connectors to Implement:
1. **Tier 2 Connectors**: Google Keep, Readwise, Pocket, Instapaper.
2. **Tier 3 Connectors**: Twitter/X, Reddit, Slack/Discord/Telegram, Audio Whisper transcription, OCR screenshots, GitHub.


