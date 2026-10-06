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
│   ├── discord_source.py # Discord message and thread connector
│   ├── email_source.py   # Email thread connector (Gmail, Outlook)
│   ├── github_source.py  # GitHub issues and gists connector
│   ├── instapaper_source.py # Instapaper bookmark and highlight connector
│   ├── keep_source.py    # Google Keep Takeout JSON connector
│   ├── notion_source.py  # Notion page connector
│   ├── pdf_source.py     # PDF document connector
│   ├── readwise_source.py# Readwise highlight and book connector
│   ├── reddit_source.py  # Reddit saved posts and comments connector
│   ├── rss_source.py     # RSS/Atom syndication connector
│   ├── slack_source.py   # Slack conversation and thread connector
│   ├── telegram_source.py# Telegram bot and chat message connector
│   ├── voice_source.py   # Voice / Audio local Whisper transcription connector
│   ├── screenshot_source.py # Screenshots / OCR local Tesseract connector
│   ├── web_source.py     # Web article connector
│   └── youtube_source.py # YouTube video connector
├── tests/
│   ├── __init__.py
│   ├── test_attachments.py # Attachment saving and embed tests
│   ├── test_cli.py         # CLI argument parser tests
│   ├── test_config.py      # Vault path & folder configuration tests
│   ├── test_converter.py   # Sanitization, frontmatter, and writer tests
│   ├── test_discord_source.py # Discord connector tests
│   ├── test_email_source.py# Email connector tests
│   ├── test_github_source.py # GitHub connector tests
│   ├── test_instapaper_source.py # Instapaper connector tests
│   ├── test_keep_source.py # Google Keep connector tests
│   ├── test_notion_source.py# Notion connector tests
│   ├── test_pdf_source.py  # PDF connector tests
│   ├── test_pipeline.py    # Pipeline orchestration and retry tests
│   ├── test_readwise_source.py # Readwise connector tests
│   ├── test_reddit_source.py   # Reddit connector tests
│   ├── test_rss_source.py  # RSS connector tests
│   ├── test_screenshot_source.py # Screenshots / OCR connector tests
│   ├── test_slack_source.py# Slack connector tests
│   ├── test_telegram_source.py# Telegram connector tests
│   ├── test_tracker.py     # SQLite tracker lifecycle and deduplication tests
│   ├── test_voice_source.py# Voice connector tests
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

# Ingest Google Keep notes from Google Takeout export directory:
python main.py ingest-google-keep --path "path/to/Takeout/Keep"
# Ingest a single Google Keep JSON file:
python main.py ingest-google-keep --path "path/to/Takeout/Keep/Note.json"
# Or using generic CLI syntax:
python main.py ingest --source google-keep --path "path/to/Takeout/Keep"

# Ingest highlights and articles from Readwise:
python main.py ingest-readwise
# Ingest a specific book/article by Readwise ID:
python main.py ingest-readwise --book-id "123456"
# Ingest items updated after a specific date:
python main.py ingest-readwise --updated-after "2026-01-01"
# Or using generic CLI syntax:
python main.py ingest --source readwise
python main.py ingest --source readwise --book-id "123456"

# Ingest bookmarks and highlights from Instapaper:
python main.py ingest-instapaper
# Ingest bookmarks from a specific folder (unread, archive, starred):
python main.py ingest-instapaper --folder unread
python main.py ingest-instapaper --folder archive
python main.py ingest-instapaper --folder starred
# Ingest with a limit:
python main.py ingest-instapaper --limit 50
# Or using generic CLI syntax:
python main.py ingest --source instapaper
python main.py ingest --source instapaper --folder archive --limit 25
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

## Google Keep Source Connector (`sources/keep_source.py`)

The Google Keep connector enables batch ingestion of notes, lists, checklists, and attachments from Google Takeout Keep JSON archives.

### Capabilities:
- **Input Flexibility**: Accepts a directory containing Google Takeout Keep `.json` export files or a direct path to a single `.json` file.
- **Deterministic Processing**: Discovers `.json` files in deterministic, case-insensitive filename order and filters out unrelated files.
- **Target Folder**: Writes notes directly to `${AURORA_VAULT_PATH}/Ingested/Notes/<YYYY-MM-DD>_google-keep_<title>.md`.
- **Timestamp Fidelity**: Parses Google Keep microsecond timestamps (`createdTimestampUsec` and `userEditedTimestampUsec`) into ISO 8601 strings and note dates (`YYYY-MM-DD`), with graceful fallback to file system timestamps.
- **Rich Checklists & Lists**:
  - Converts `listContent` checklist items into standard Markdown `- [ ]` (unchecked) and `- [x]` (checked) tasks.
  - Automatically deduplicates matching lines between `textContent` and `listContent` to prevent repeating checklist text in the note body.
- **Labels & Tags**:
  - Extracts labels from the `labels` array, sanitizes any leading `#` symbols, and maps them to frontmatter `tags`.
  - Enforces mandatory `ingested` tag along with `google-keep`.
- **Color Metadata Preservation**:
  - Preserves Google Keep card color names (e.g. `RED`, `BLUE`, `YELLOW`) in frontmatter as `keep_color`.
  - Appends a `keep-<color>` tag (e.g. `keep-red`, `keep-blue`) for visual filtering and styling in Obsidian.
- **Archived & Trashed State Tracking**:
  - Notes marked `isArchived: true` receive frontmatter `archived: true` and `status: "archived"`.
  - Notes marked `isTrashed: true` receive frontmatter `trashed: true` and `status: "trashed"`.
- **Local Attachment Copying**:
  - Discovers image and file attachments referenced in the Takeout JSON (`attachments[].filePath`) relative to the export directory.
  - Copies attachments into `${AURORA_VAULT_PATH}/Attachments/Ingested/`.
  - Handles filename collisions (`diagram.png`, `diagram_2.png`) across notes and updates embed tags accordingly.
  - Embeds attachments into the Markdown note using `![[filename.ext]]`.
  - Tolerates missing or unexported attachment files by logging warnings without aborting note processing.
- **Deduplication & In-Place Updates**:
  - Derives stable source ID: `google-keep:<id>` (from note ID) or `google-keep:<createdTimestampUsec>` (immutable timestamp), falling back to filename stem.
  - `content_hash`: SHA-256 of title, body, and attachment contents.
  - **Unchanged**: Skips re-processing if content hash matches existing record.
  - **Changed**: Overwrites note in-place in `${AURORA_VAULT_PATH}/Ingested/Notes/` and updates tracking records without creating duplicate notes.
- **Batch Isolation**: Corrupt or malformed JSON files are logged with `SourceError` while allowing all healthy notes in the batch to complete successfully.

### Setup & Export Workflow:
1. Visit [Google Takeout](https://takeout.google.com/).
2. Deselect all services and check **Google Keep**.
3. Export and download the `.zip` archive.
4. Extract the archive (usually extracts to a folder named `Takeout/Keep`).
5. Configure the export path in `.env` or via CLI:
   ```bash
   export GOOGLE_KEEP_EXPORT_PATH="/path/to/Takeout/Keep"
   # Or in .env:
   # GOOGLE_KEEP_EXPORT_PATH=C:\path\to\Takeout\Keep
   ```
6. Run ingestion:
   ```bash
   python main.py ingest-google-keep
   ```

### Known Limitations:
- **Takeout Export Required**: Ingestion is based on Google Takeout JSON exports; real-time Keep sync via the private/unofficial Google Keep API (`gkeepapi`) is not supported in this version.
- **Audio Recordings**: Audio attachments in Takeout are preserved as file attachments but not automatically transcribed into text (handled in Tier 3 Whisper integration).
- **Drawings & Ink**: Google Keep drawing notes are exported by Google as `.png` images and embedded as image attachments.

---

## Readwise Source Connector (`sources/readwise_source.py`)

The Readwise connector synchronizes saved books, articles, tweets, and podcasts along with their highlighted passages and personal notes directly from the official Readwise v2 Export API.

### Capabilities:
- **API Pagination**: Automatically traverses the official Readwise v2 Export API (`https://readwise.io/api/v2/export/`) using cursor-based pagination (`nextPageCursor` and `next` URL fallback) to retrieve all items without truncation.
- **Granular Filtering**: Supports optional `--book-id` filtering and incremental sync via `--updated-after` ISO 8601 timestamps.
- **Target Folder**: Writes notes directly to `${AURORA_VAULT_PATH}/Ingested/Web/<YYYY-MM-DD>_readwise_<sanitized-title>.md`.
- **Comprehensive Frontmatter**:
  - `title`, `date`, `source: "readwise"`, `tags: [ingested, readwise, ...]`, `ingested_at`, `source_url`, `author`, `readwise_id`, `category`, and `num_highlights`.
- **Highlights as Blockquotes**:
  - Highlights are cleanly rendered as standard Markdown `> blockquotes`.
  - Multi-line highlights have every line properly quoted.
  - Location and page numbers are preserved inline (e.g. `*Location: page 42*` or `*Location: 105*`).
- **Inline Personal Notes**:
  - User notes/memos attached to highlights in Readwise are rendered inline (`**My note:** ...`).
  - Highlights without notes do not generate redundant or empty note sections.
- **Document Summaries & Notes**:
  - Top-level document summaries and document notes are preserved under `## Summary` and `## Document Note`.
- **Tag Normalization**:
  - Extracts tags from items and highlights, sanitizes any leading `#` symbols, removes duplicates, and ensures mandatory `ingested` and `readwise` tags.
- **Deduplication & In-Place Updates**:
  - `source_id`: `readwise:<user_book_id>` (stable immutable identifier).
  - `content_hash`: SHA-256 digest of title, body, highlights, locations, and personal notes.
  - **Unchanged**: Skips re-writing when content is identical.
  - **Changed**: Overwrites existing note at the exact same path in `${AURORA_VAULT_PATH}/Ingested/Web/` when new highlights or notes are added, without creating duplicate files.
- **Batch Fault Isolation**:
  - Malformed API items or individual conversion issues are safely logged and skipped, allowing healthy documents to continue ingestion.

### Setup & Authentication:
1. Obtain an API token from [Readwise Access Token](https://readwise.io/access_token).
2. Configure the token in your environment:
   ```bash
   export READWISE_TOKEN="readwise_token_here"
   # Or in .env:
   # READWISE_TOKEN=readwise_token_here
   ```
3. Run ingestion:
   ```bash
   python main.py ingest-readwise
   ```

---

## Instapaper Source Connector (`sources/instapaper_source.py`)

The Instapaper connector synchronizes saved bookmarks, articles, highlights, and user notes directly from Instapaper into Aurora's Obsidian vault.

### Capabilities:
- **Dual API Support**:
  - Full compatibility with the modern Instapaper API v2 (`https://www.instapaper.com/api/2`) supporting Personal Access Tokens / Bearer tokens (`Authorization: Bearer <token>`).
  - Backward compatibility with Instapaper API v1 (`https://www.instapaper.com/api/1/bookmarks/list`) with basic username & password authentication.
- **Folder Filtering**:
  - Filter bookmarks by folder: `unread` (default), `archive`, or `starred`.
  - Configurable batch size / count limit via `--limit N`.
- **Target Folder**:
  - Writes notes directly to `${AURORA_VAULT_PATH}/Ingested/Web/<YYYY-MM-DD>_instapaper_<sanitized-title>.md`.
- **Comprehensive Frontmatter**:
  - `title`, `date`, `source: "instapaper"`, `tags: [ingested, instapaper, ...]`, `ingested_at`, `source_url`, `author`, `bookmark_id`, `folder`, `progress`, `starred`, and `num_highlights`.
- **Highlights as Blockquotes & Personal Notes**:
  - Highlights are cleanly rendered as standard Markdown `> blockquotes`.
  - Multi-line highlights have each line prefixed with `>`.
  - User notes and annotations attached to highlights are rendered inline below the quote as `**My note:** note_text`.
  - When no highlights exist on a bookmark, the highlights section is omitted completely.
- **Content & Excerpt Structure**:
  - Full article content (`content`, `text`, or `html`) is normalized: HTML is safely detected and converted to clean Markdown via `markdownify` with `<script>` and `<style>` elements and contents stripped (no raw HTML emitted), while plain text and Markdown formatting are preserved without destruction.
  - If full article text is absent, article descriptions or excerpts are rendered under `## Excerpt`.
  - If neither is available, a clear placeholder (`*No excerpt or content provided by Instapaper.*`) is provided.
- **Tag Normalization**:
  - Extracts tags from bookmarks, strips leading `#` symbols, removes duplicates, and ensures mandatory `ingested` and `instapaper` tags.
- **Deduplication & In-Place Updates**:
  - `source_id`: `instapaper:<bookmark_id>` (stable immutable identifier; never derives identity from mutable titles or URLs).
  - `content_hash`: SHA-256 digest of title, body/excerpt, highlights, and notes.
  - **Unchanged**: Skips re-writing when content is identical.
  - **Changed**: Overwrites the existing note at the exact same path in `${AURORA_VAULT_PATH}/Ingested/Web/` when new highlights or content changes occur, without creating duplicate files.
- **Defensive Networking & Batch Fault Isolation**:
  - Defends pagination against duplicate cursor loops and repeated batch fingerprints.
  - Individual malformed API records or single-item conversion errors are safely logged and skipped, allowing all healthy bookmarks to continue ingestion.
  - Secret scrubbing: Tokens, passwords, and authorization headers are never logged or exposed in exceptions or note frontmatter.

### Setup & Authentication:
1. Configure credentials using either method:
   - **API Token (Recommended)**:
     ```bash
     export INSTAPAPER_TOKEN="your_instapaper_token_here"
     # Or in .env:
     # INSTAPAPER_TOKEN=your_instapaper_token_here
     ```
   - **Username & Password (v1 fallback)**:
     ```bash
     export INSTAPAPER_USERNAME="your_username_or_email"
     export INSTAPAPER_PASSWORD="your_password"
     # Or in .env:
     # INSTAPAPER_USERNAME=your_username_or_email
     # INSTAPAPER_PASSWORD=your_password
     ```
2. Run ingestion:
   ```bash
   python main.py ingest-instapaper
   # Or with options:
   python main.py ingest-instapaper --folder archive --limit 50
   ```

---

## GitHub Source Connector (`sources/github_source.py`)

The GitHub connector synchronizes repository issues and user gists from the official GitHub REST API into Aurora's Obsidian vault.

### Capabilities:
- **Repository Issues**:
  - Ingest issues across one or multiple configured repositories (`owner/repository`, e.g. `chintasankarraj/aurora-ingestion`).
  - Pull requests are strictly filtered out (`pull_request` key detection).
  - Configurable state filtering: `all` (default), `open`, or `closed`.
  - Issue comments fetched in chronological order (`GET /repos/{owner}/{repo}/issues/{issue_number}/comments`) and rendered under `## Comments` with author and timestamp (`### {user} — {created_at}`).
  - Issue comments can be excluded with `--no-comments`.
  - Metadata section: State, Labels, Assignees, and Milestone rendered under `## GitHub Metadata`.
- **User Gists**:
  - Ingest public and secret gists for the authenticated user (`GET /gists`).
  - Gist files ordered deterministically with content displayed under `## Files`.
  - Dynamic code block fences (`format_code_fence`): adapts fence backtick count (e.g. ` ```` `) to safely enclose code containing backticks without breaking out of Markdown blocks.
  - Truncated files flagged with notice `*(File content truncated by GitHub API)*`.
  - Gist metadata (visibility, creation, update dates) rendered under `## Gist Metadata`.
- **Target Folder**:
  - Notes written directly to `${AURORA_VAULT_PATH}/Ingested/Code/<YYYY-MM-DD>_github_<sanitized-title>.md`.
- **YAML Frontmatter**:
  - `title`, `date`, `source: "github"`, `tags: [ingested, github, code, ...]`, `ingested_at`, `source_url`, `author`, `item_type: "issue"` or `"gist"`, `issue_number`, `repository`, `state`, `gist_id`, `is_public`.
- **Tag Normalization**:
  - Extracts labels/tags from issues, sanitizes spaces and `#` characters, and enforces mandatory `ingested`, `github`, and `code` tags.
- **Deduplication & In-Place Updates**:
  - Immutable stable identifiers:
    - Issue: `github:issue:<owner>/<repo>#<number>`
    - Gist: `github:gist:<gist_id>`
    - Identity is never derived from mutable titles or URLs.
  - Content hash: SHA-256 digest of issue body/comments or gist files and metadata.
  - Unchanged items are skipped; updated issues/gists overwrite the existing note at the exact same vault path in `Ingested/Code/`.
- **Defensive Networking & Batch Fault Isolation**:
  - Token and credential secrecy: tokens are scrubbed from logs, exceptions, and frontmatter.
  - Pagination loop defense with duplicate batch fingerprint tracking.
  - Batch isolation across repositories and individual malformed records.
  - Graceful handling of API rate limits (HTTP 403/429 with reset time extraction) and network errors.

### Setup & Authentication:
1. Generate a GitHub Personal Access Token (classic `repo` / `gist` scope or fine-grained PAT) and configure:
   ```bash
   export GITHUB_TOKEN="ghp_your_token_here"
   # Or in .env:
   # GITHUB_TOKEN=ghp_your_token_here
   ```
2. Run ingestion:
   ```bash
   # Ingest issues for a single repository:
   python main.py ingest-github --repo chintasankarraj/aurora-ingestion

   # Ingest open issues across multiple repositories:
   python main.py ingest-github --repos "owner/repo1,owner/repo2" --state open

   # Ingest both issues and user gists:
   python main.py ingest-github --repo owner/repo --include-gists

   # Ingest user gists only:
   python main.py ingest-github --gists-only

   # Generic CLI syntax:
   python main.py ingest --source github --repo owner/repo
   ```

---

## Reddit Source Connector (`sources/reddit_source.py`)

The Reddit connector synchronizes posts and their comment discussions from configured subreddits via the official Reddit REST API into Aurora's Obsidian vault.

### Capabilities:
- **Subreddit Posts**:
  - Ingest posts across one or multiple subreddits (`programming`, `r/MachineLearning`, `LocalLLaMA`).
  - Supports both self-posts (full Markdown text) and link posts (preserving Reddit permalink and external source URL).
  - Automatically filters advertisements and promoted posts (`promoted: true`).
  - Metadata section: Subreddit, Author, Score, Comment count, Creation timestamp, Flair, Edited status, and External URL rendered under `## Metadata`.
  - Content normalization: converts HTML elements via `markdownify`, strips `<script>` and `<style>` blocks, unescapes Reddit HTML entities (`&gt;`, `&lt;`, `&amp;`), and strictly preserves ordinary Markdown, headings, lists, tables, comparisons (`<` and `>`), and code fences.
- **Comments Traversal & Formatting**:
  - Fetches comments in chronological order using Reddit's `sort=old` API parameter.
  - Deterministically sorts comments locally by `created_utc` ascending and comment `id` ascending tie-breaker.
  - Safely flattens nested comment trees into readable Obsidian blockquotes:
    - Top-level comments formatted as `### @author — YYYY-MM-DD (Score: X)`.
    - Nested replies rendered as blockquotes: `> **↳ @reply_user** — YYYY-MM-DD (Score: X) *(reply to @parent_author)*:`.
  - Configurable comment cap via `--max-comments` (default: 50) and comment omission via `--no-comments`.
  - Fault-tolerant comment parsing: malformed comment objects and deleted authors/bodies are handled without failing the post or thread.
- **Target Folder**:
  - Notes written directly to `${AURORA_VAULT_PATH}/Ingested/Social/<YYYY-MM-DD>_reddit_<sanitized-title>.md`.
- **YAML Frontmatter**:
  - `title`, `date`, `source: "reddit"`, `tags: [ingested, reddit, social]`, `ingested_at`, `source_url`, `author`, `subreddit`, `reddit_id`, `score`, `comment_count`, `flair`, `external_url`.
- **Deduplication & In-Place Updates**:
  - Immutable stable identifier: `reddit:post:<post_id>`.
  - Identity is never derived from mutable post titles or URLs.
  - Title changes overwrite the existing note in-place without creating duplicate notes.
  - Content hash: SHA-256 digest of post body, metadata, and comments.
- **Defensive Networking & Batch Fault Isolation**:
  - Credentials and tokens are never logged or exposed in exceptions.
  - Subreddit-level fault isolation: a 404 or missing subreddit does not abort remaining healthy subreddits.
  - Post-level fault isolation: malformed posts or comment fetch errors do not abort the subreddit batch.
  - Rate limit handling: parses `x-ratelimit-reset` on HTTP 429 and provides user-friendly reset duration messages.

### Setup & Authentication:
1. Create a script application at [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps) to obtain a Client ID and Client Secret.
2. Configure credentials in your environment or `.env`:
   ```bash
   export REDDIT_CLIENT_ID="your_client_id"
   export REDDIT_CLIENT_SECRET="your_client_secret"
   export REDDIT_USER_AGENT="aurora-ingestion:v1.0.0 (by /u/your_reddit_username)"
   # Optional direct token override:
   # export REDDIT_ACCESS_TOKEN="your_bearer_token"
   ```
3. Run ingestion:
   ```bash
   # Ingest hot posts from a single subreddit:
   python main.py ingest-reddit --subreddit programming

   # Ingest posts across multiple subreddits:
   python main.py ingest-reddit --subreddits "programming,MachineLearning,LocalLLaMA"

   # Ingest top posts with custom limits:
   python main.py ingest-reddit --subreddit artificial --listing top --limit 20 --max-comments 30

   # Ingest without comments:
   python main.py ingest-reddit --subreddit programming --no-comments

   # Generic CLI syntax:
   python main.py ingest --source reddit --subreddit programming
   ```

### Limitations:
- **Application-Only Scope**: OAuth2 `client_credentials` grant accesses public subreddit data only. Private subreddits and user-specific actions (e.g. upvoting, saved posts, private messages) are not accessible.
- **Comment Tree Placeholders**: Enormous threads containing deep `more` objects are flattened up to loaded comments within `--max-comments` to avoid unbounded network requests and memory usage.
- **Rate Limits**: Subject to Reddit's standard OAuth rate limit (typically 60-100 requests per minute).

---

## Slack Ingestion Connector (`SlackSource`)

The Slack connector connects to the official Slack Web API using Bearer token authentication (`SLACK_TOKEN` or `SLACK_BOT_TOKEN`). It discovers accessible public and private channels, fetches channel message histories, traverses threaded replies, normalizes Slack mrkdwn and HTML entities, and compiles each message or thread into an Obsidian-compatible Markdown note under `Ingested/Social/`.

### Key Features:
- **Scope & Targets**:
  - Connects to public channels and accessible private channels.
  - Captures messages and threaded replies into unified, coherent notes.
  - Target vault folder: `${AURORA_VAULT_PATH}/Ingested/Social/<YYYY-MM-DD>_slack_<sanitized-title>.md`.
  - Tags: `[ingested, slack, social]`.
- **Authentication**:
  - Bearer token authentication via `Authorization: Bearer <token>`.
  - Configurable via `SLACK_TOKEN`, `SLACK_BOT_TOKEN`, or `--token` CLI argument.
  - Secret scrubbing: tokens and Authorization headers are automatically redacted from all exceptions, logs, and notes.
- **Required OAuth Scopes**:
  - Public channels: `channels:history`, `channels:read`
  - Private channels: `groups:history`, `groups:read`
  - User resolution: `users:read`
- **Channel Discovery & Selection**:
  - Discovers channels via `conversations.list` with cursor-based pagination.
  - Accepts channel names (with or without `#`) or direct Slack channel IDs (e.g. `C12345678`).
  - Supports multiple channels via repeated `--channel` / `-c` or comma-separated `--channels`.
  - Fault isolation: a missing, archived, or inaccessible channel reports a warning without halting ingestion of remaining channels.
- **Message Ingestion & Subtype Filtering**:
  - Ingests channel message history via `conversations.history` with cursor pagination.
  - Filters system noise (`channel_join`, `channel_leave`, `channel_topic`, `channel_purpose`, `channel_name`, etc.).
  - Handles `message_changed` by extracting the updated inner message.
  - Skips `message_deleted` messages without fabricating content.
  - Handles bot messages (`bot_message` subtype or `bot_id`).
- **Thread Ingestion & Representation**:
  - For messages with replies (`reply_count > 0`), retrieves full threads via `conversations.replies`.
  - Renders parent message and replies as a single coherent document:
    - Root message under `## Message`.
    - Chronologically ordered replies under `## Thread` with `### @user — YYYY-MM-DD HH:MM` and `#### @reply_user — YYYY-MM-DD HH:MM`.
  - Omission option: `--no-threads` to ingest parent messages only.
  - Thread fault isolation: if replies fail to fetch, the parent message note is safely preserved.
- **User Resolution**:
  - In-memory cache loaded via `users.list` mapping user IDs to display names or real names.
  - On-demand `users.info` fallback for unlisted members with graceful fallback to `unknown`.
- **Text & Markup Normalization**:
  - User mentions: `<@U123456>` or `<@U123456|alice>` → `@alice`.
  - Channel mentions: `<#C123456|name>` or `<#C123456>` → `#name`.
  - Broadcasts: `<!here>` → `@here`, `<!channel>` → `@channel`, `<!everyone>` → `@everyone`.
  - Links: `<https://example.com|Label>` → `[Label](https://example.com)` (strictly validated for `http` and `https` schemes; unsafe schemes like `javascript:`, `data:`, `file:` rendered as plain text).
  - Unescapes Slack entities (`&amp;`, `&lt;`, `&gt;`) and preserves comparison operators (`<`, `>`).
  - Pure Markdown normalization via `markdownify` stripping raw HTML and `<script>`/`<style>` blocks.
- **Stable Identity & Deduplication**:
  - Stable identifier: `slack:message:<channel_id>:<root_ts>`.
  - Both root messages and threads use the root message timestamp (`thread_ts` or `ts`).
  - Edits and new thread replies overwrite the existing note in-place without producing duplicate notes.
- **Defensive Rate-Limit Handling**:
  - Evaluates HTTP 429 and `{"ok": false, "error": "ratelimited"}` responses.
  - Parses `Retry-After` header and surfaces user-friendly reset duration messages.

### Setup & Authentication:
1. Create a Slack App in your workspace at [api.slack.com/apps](https://api.slack.com/apps).
2. Under **OAuth & Permissions**, add the required Bot or User token scopes:
   - `channels:history`
   - `channels:read`
   - `groups:history`
   - `groups:read`
   - `users:read`
3. Install the app to your workspace and copy the Bot User OAuth Token (`xoxb-...`).
4. Set credentials in your environment or `.env`:
   ```bash
   export SLACK_TOKEN="xoxb-your-slack-bot-token"
   # Optional default channels:
   # export SLACK_CHANNELS="general,engineering"
   ```
5. Invite the bot to the channels you wish to ingest (`/invite @YourBotName`).
6. Run ingestion:
   ```bash
   # Ingest a single channel:
   python main.py ingest-slack --channel general

   # Ingest multiple channels:
   python main.py ingest-slack --channels "general,engineering,announcements"

   # Ingest with message limit and thread reply cap:
   python main.py ingest-slack --channel engineering --limit 20 --max-replies 30

   # Ingest without thread replies:
   python main.py ingest-slack --channel general --no-threads

   # Generic CLI syntax:
   python main.py ingest --source slack --channel general
   ```

### Limitations & Rate Limit Warning:
- **Newer App Rate Limits**: Slack imposes strict Tier 3 / Tier 2 rate limits on `conversations.history` and `conversations.replies`. For newer non-Marketplace apps, Slack may restrict requests to as low as **1 request per minute** and **max 15 messages per request**. Bulk historical ingestion across large workspaces may therefore be slow due to Slack's API throttling.
- **Attachments**: Binary file attachments are not downloaded in this version; file titles, MIME types, and permalinks are preserved as note metadata.
- **Direct Messages**: DMs (`im:history`) and multi-person DMs (`mpim:history`) are out of scope for this version and require additional scopes.

---

## Discord Ingestion Connector (`DiscordSource`)

The Discord connector connects to the official Discord REST API v10 using official Bot token authentication (`DISCORD_BOT_TOKEN` or `--token`). It discovers configured guild channels, fetches channel message histories, traverses threaded replies, normalizes Discord Markdown, mentions, and HTML entities, and compiles each message or thread into an Obsidian-compatible Markdown note under `Ingested/Social/`.

### Key Features & Design:
- **Official Bot Authentication**: Uses standard Discord Bot authorization (`Authorization: Bot <token>`). User tokens and self-bot accounts are strictly prohibited and rejected per Discord Developer Policy.
- **Vault Topology & File Routing**:
  - Target vault folder: `${AURORA_VAULT_PATH}/Ingested/Social/<YYYY-MM-DD>_discord_<sanitized-title>.md`.
  - Tags: `[ingested, discord, social]`.
- **Channel Discovery & Filtering**:
  - Ingests text, forum, and announcement channels (types 0: `GUILD_TEXT`, 5: `GUILD_ANNOUNCEMENT`, 15: `GUILD_FORUM`, 16: `GUILD_MEDIA`).
  - Voice channels (type 2: `GUILD_VOICE`, 13: `GUILD_STAGE_VOICE`) and category headers (type 4) are strictly excluded.
  - Accepts channel Snowflake IDs or channel names (with or without `#`) when paired with a configured guild.
- **Privileged Intents & Content Safeguards**:
  - Requires the **MESSAGE CONTENT INTENT** enabled in the Discord Developer Portal under Bot settings.
  - If message content is unavailable because the bot lacks this intent, the connector returns a clear, actionable error explaining the exact Developer Portal configuration required, preventing silent ingestion of blank notes.
- **Thread Reply Traversal**:
  - Thread starter messages and active threads are fetched via `GET /channels/{thread_id}/messages`.
  - Replies are ordered chronologically and appended to the root message note under `## Thread`, preventing redundant separate notes for thread replies.
- **Discord Markdown & Mention Normalization**:
  - Mentions: `<@123>` and `<@!123>` resolve to `@username`, `<#456>` resolves to `#channel-name`, `<@&789>` resolves to `@role`.
  - Custom emojis: `<:blob:123>` and `<a:party:456>` normalize cleanly to `:blob:` and `:party:`.
  - URL safety: HTTP and HTTPS URLs are rendered as clickable Markdown links; unsafe schemes (`javascript:`, `data:`, `file:`, `ftp:`) are rendered as plain/code text.
  - Clean Markdown: Strips raw HTML tags and `<script>`/`<style>` blocks using `markdownify`, preserving mathematical comparisons (`<`, `>`), code fences, and inline code.
- **Stable Identity & Deduplication**:
  - Stable identifier: `discord:message:<channel_id>:<message_id>`.
  - In-place update lifecycle: Edited messages or new thread replies update the existing note in-place.
- **Rate Limit Resilience & Bounded Backoff**:
  - Automatically handles HTTP 429 rate limit responses with bounded authoritative backoff.
  - Respects authoritative `retry_after` from JSON bodies and `Retry-After` headers.
  - Configurable `max_retries` (default: 3) and dependency-injected sleep handler for deterministic testing.
  - Inspects `X-RateLimit-Scope` (`global`, `user`, `shared`) to preserve upstream API limits without busy-waiting.

### Bot Setup & Permissions:
1. Create a Discord Application at the [Discord Developer Portal](https://discord.com/developers/applications).
2. Under the **Bot** tab:
   - Create a Bot and copy the Bot Token.
   - Under **Privileged Gateway Intents**, enable **MESSAGE CONTENT INTENT** (available directly via toggle for bots below 10,000 guild-installed users).
3. Under **OAuth2 > URL Generator**:
   - Scopes: `bot`.
   - Bot Permissions: `View Channels`, `Read Message History`.
   - Use the generated invite URL to add the bot to your Discord server.

### Configuration & CLI Usage:
```bash
# Set bot token in environment:
export DISCORD_BOT_TOKEN="your_discord_bot_token"

# Ingest channel by name with guild ID:
python main.py ingest-discord --guild 123456789012345678 --channel general

# Ingest multiple channels by ID:
python main.py ingest-discord --channels "234567890123456789,345678901234567890"

# Ingest with message limit and thread reply limit:
python main.py ingest-discord --channel 234567890123456789 --limit 50 --max-replies 25

# Ingest without fetching thread replies:
python main.py ingest-discord --channel 234567890123456789 --no-threads

# Generic CLI syntax:
python main.py ingest --source discord --guild 123456789012345678 --channel general
```

### Limitations & Platform Restrictions:
- **Privileged Intent Requirement**: Message content cannot be read by bots lacking the privileged Message Content Intent. Aurora enforces this requirement and displays clear setup guidance if content is missing. Under current Discord developer policy, bots in fewer than 10,000 guilds (or below 10,000 installed users) can enable the Message Content Intent directly via the toggle in the Developer Portal without verification. For bots installed in 10,000 or more guilds, Discord requires formal App Verification and privileged intent approval via App Review.
- **Attachments**: Binary media files are not downloaded in v1; filenames, sizes, MIME types, and secure links are preserved in note metadata.
- **Direct Messages & Private Channels**: User DMs and group DMs are out of scope. The bot can only ingest channels in guilds where it has been granted access.
- **No Scraping**: In strict compliance with Discord Developer Policy, Aurora accesses Discord data exclusively via official Bot REST endpoints. Self-bots, user token automation, and credential scraping are not supported.

---

## Telegram Ingestion Connector (`TelegramSource`)

The Telegram connector connects to the official Telegram Bot API (`https://api.telegram.org/bot<TOKEN>/`) using official Bot token authentication (`TELEGRAM_BOT_TOKEN` or `--token`). It retrieves messages, edited messages, channel posts, and captions from explicitly configured Telegram chats, channels, or groups, formats structured entities, normalizes HTML, preserves safe media metadata, and compiles each message into an Obsidian-compatible Markdown note under `Ingested/Social/`.

### Key Features & Design:
- **Official Bot API Integration**: Communicates exclusively via standard Telegram Bot API endpoint URL routing (`https://api.telegram.org/bot<TOKEN>/<method>`). Tokens are strictly protected and never logged. Personal user accounts, session hijacking, or MTProto scraping are not used.
- **Vault Topology & File Routing**:
  - Target vault folder: `${AURORA_VAULT_PATH}/Ingested/Social/<YYYY-MM-DD>_telegram_<sanitized-title>.md`.
  - Tags: `[ingested, telegram, social]`.
- **Supported Message Retrieval (`getUpdates`) & Safe Offset Acknowledgement**:
  - Ingestion operates over messages queued via Telegram's official `getUpdates` endpoint.
  - **Queue & Confirmation Semantics**: Telegram's Bot API treats `getUpdates` as a temporary update queue. An update is confirmed on Telegram's servers only when a subsequent `getUpdates` call is made with an `offset` greater than that update's ID.
  - **Strict Consumed Boundary**: Aurora tracks `last_consumed_update_id` and advances `offset = last_consumed_update_id + 1` only for updates Aurora has intentionally processed (converted to `SourceItem`, deduplicated, or deliberately ignored due to unconfigured chat filtering or unsupported update type).
  - **Safe `--limit` Handling**: Aurora requests only as many updates as needed (`min(100, remaining_needed)`) and never advances the offset past unconsumed updates. If a batch contains more updates than the configured limit, the unconsumed remainder is preserved in memory and remains unacknowledged on Telegram's server, ensuring subsequent runs receive all pending messages without data loss.
  - **Configured Chat Filtering**: Updates from chats not configured in `--chat` / `TELEGRAM_CHAT_ID` are deterministically skipped and acknowledged so they do not block queue progression, while ensuring updates from configured chats are never skipped due to earlier limit boundaries.
  - **Platform Retention Limitation**: Telegram retains incoming updates only for a limited period (typically up to 24 hours), and does not provide an arbitrary historical backfill endpoint (`getChatHistory`). Aurora ingests queued updates and does not claim arbitrary historical backfill.
- **Chat Discovery & Flexible Identification**:
  - Requires explicit chat configuration (`--chat`, `--chats`, `TELEGRAM_CHAT_ID`, `TELEGRAM_CHAT_IDS`).
  - Supports negative IDs (supergroups and channels, e.g. `-1001234567890`, `-123456789`), positive IDs (private chats), public usernames (e.g. `@my_channel` or `my_channel`), and exact chat titles.
- **Webhook Conflict Guard**:
  - If a webhook is active on the bot, Telegram rejects polling with HTTP 409 Conflict. Aurora detects this and raises an actionable error explaining that the external webhook must be deleted if polling is desired, without silently or unexpectedly mutating external bot webhooks.
- **UTF-16 Entity Normalization**:
  - Accurately converts Telegram UTF-16 code unit offsets and lengths to Python character indices to prevent emoji/surrogate pair drift.
  - Formats: `bold`, `italic`, `underline`, `strikethrough`, `code`, `pre` (fenced blocks with language tags), `text_link`, `url`, `spoiler`, and mentions.
  - Cleans raw HTML using `markdownify` while preserving mathematical comparisons (`a < b`, `x > y`) and code fences.
- **URL Safety Validation**:
  - HTTP and HTTPS URLs are rendered as clickable Markdown links; unsafe schemes (`javascript:`, `data:`, `file:`, `ftp:`) are rendered as plain/code text.
- **Replies & Forward Attribution**:
  - Preserves reply hierarchy with formatted blockquotes (`## Replying to`) and frontmatter metadata (`reply_to_message_id`).
  - Preserves forwarded channel and user attribution (`**Forwarded from**: ...`).
- **Media Metadata & Caption Ingestion**:
  - Media captions are formatted with full entity support identically to text messages.
  - Safe metadata preserved for photos, documents, videos, audio, voice notes, animations, stickers, polls, locations, venues, and contacts.
  - Binary media downloading is out of scope in v1; contact phone numbers and vcards are strictly excluded from output for privacy.
- **Stable Identity & Deduplication**:
  - Stable identifier: `telegram:message:<chat_id>:<message_id>`.
  - In-place note updates on edited messages (`edited_message`, `edited_channel_post`).
- **Rate Limit Resilience & Bounded Backoff**:
  - Detects HTTP 429 Too Many Requests and Telegram flood wait errors, extracting authoritative delay from `parameters.retry_after` or `Retry-After` headers.
  - Implements bounded retries with configurable `max_retries` (default: 3) and dependency-injected sleep handlers.

### Bot Setup & Permissions:
1. Open [@BotFather](https://t.me/BotFather) on Telegram and send `/newbot` to create your bot and obtain the Bot Token.
2. Configure Bot Privacy for Groups:
   - By default, Telegram enables **Group Privacy Mode**, meaning bots in groups only receive commands (`/...`), mentions, or replies.
   - To ingest all messages in a group, send `/setprivacy` to @BotFather and set it to **Disable**, or promote the bot to **Administrator** in the group.
3. Configure Channels:
   - Add the bot to your channel as an **Administrator** with permission to read and post messages so it can receive `channel_post` updates.

### Configuration & CLI Usage:
```bash
# Set bot token in environment:
export TELEGRAM_BOT_TOKEN="123456789:ABCdefGHIjklMNOpqrsTUVwxyz123456789"

# Ingest channel by username:
python main.py ingest-telegram --chat @my_channel

# Ingest multiple chats by ID (including negative supergroup IDs):
python main.py ingest-telegram --chats "-1001234567890,-1009876543210"

# Ingest with message limit and starting offset:
python main.py ingest-telegram --chat -1001234567890 --limit 50 --offset 10050

# Generic CLI syntax:
python main.py ingest --source telegram --chat -1001234567890
```

### Limitations & Platform Restrictions:
- **No Arbitrary History Traversal**: Telegram Bot API only delivers updates queued via `getUpdates` while the bot was a chat member. It cannot backfill years of historical chat history from before the bot was created.
- **Webhook Mutual Exclusivity**: Polling via `getUpdates` cannot run concurrently with an active webhook. Aurora alerts the user on HTTP 409 without deleting external webhooks.
- **Media Downloads**: Binary media files (images, audio, video) are not downloaded in v1; file IDs, MIME types, and dimensions are preserved in metadata.
- **Direct User Messages**: Arbitrary private user DMs cannot be monitored; the bot only accesses chats where it is explicitly added.

---

## Voice / Audio Ingestion Connector (`VoiceSource`)

The Voice connector enables local-first speech-to-text transcription of voice memos, meetings, lectures, and audio recordings using local Whisper-compatible models (`faster-whisper`). It requires **zero paid external transcription APIs**, operates completely offline without API keys, and formats transcriptions into clean Obsidian-compatible Markdown notes under `Ingested/Voice/`.

### Key Capabilities:
- **Local-First & Offline**: Uses `faster-whisper` (CTranslate2 backend) running directly on your machine. No OpenAI, Google Cloud Speech, AWS Transcribe, or other paid APIs are required or used.
- **Supported Audio Formats**: Supports `.wav`, `.mp3`, `.m4a`, `.flac`, `.ogg`, `.opus`, `.aac`, and `.webm`. PyAV (`av`) handles container demuxing and decoding across formats.
- **Deterministic Content Hashing**: Computes streaming 64 KB chunked SHA-256 digests (`voice:sha256:<content_hash>`), ensuring identical audio recordings (even if renamed or moved) are deduplicated without loading entire audio files into memory.
- **Model Flexibility**: Supports all Whisper model sizes (`tiny`, `base`, `small`, `medium`, `large-v3`, etc.) or custom local model directories. Defaults to `small` for a balanced accuracy-speed tradeoff.
- **Execution Hardware (`--device auto|cuda|cpu`)**: Supports CPU execution (default) and GPU CUDA execution. `--device auto` probes CUDA availability via CTranslate2, selecting `cuda` if available and safely falling back to `cpu` otherwise. Explicit `--device cuda` validates CUDA presence and raises actionable errors if unavailable.
- **Readable Timestamp Formatting**: Renders structured speaker/speech segments in human-readable time intervals (`[00:01:23 - 00:01:45]`), automatically scaling to seconds, minutes, and hours (`HH:MM:SS`).
- **Voice Activity Detection (VAD)**: Optional Silero VAD filtering via `--vad` to suppress background noise, music, and non-speech silence.
- **Preflight Duration Checking & Safety Guards**: Rejects files exceeding `--max-duration` (default: 7200s / 2 hours) **before** calling the Whisper transcriber whenever container duration metadata is exposed (via PyAV container probing), preventing wasted compute and memory allocation. If container duration metadata is absent, safely falls back to post-transcription duration validation. Maximum file size cap enforced at 2 GB (`MAX_FILE_SIZE_BYTES`).
- **Fault-Isolated Directory Ingestion**: Ingests individual files (`--file`), multiple files (`--files`), or entire directories (`--directory`). Corrupt or unreadable files in directory mode are logged and skipped without aborting the batch.
- **Note Formatting & Attribution**: Notes are stored in `${AURORA_VAULT_PATH}/Ingested/Voice/<date>_voice_<title>.md` with frontmatter tags `[ingested, voice, audio]` and a formatted blockquote displaying filename, language, duration, model, and date.
- **Title Determination**: Prefers embedded audio metadata title tags (ID3/container tags) when available, falling back to the sanitized audio filename stem (strictly capped at 80 characters without forbidden characters).

### Model Weights & Offline Usage:
Faster-whisper and CTranslate2 do **not** bundle gigabytes of pre-trained neural network weights directly inside the Python package installation:
- **Named Models (`--model tiny|base|small|medium|large-v3`)**:
  When a named model string is specified, `faster-whisper` checks the local Hugging Face model cache (`~/.cache/huggingface/hub`). If the model weights have not been downloaded yet, `faster-whisper` will automatically download the required model weights on first use. Subsequent invocations will load the cached local weights without network traffic.
- **Local Offline Model Directory (`--model <local-model-path>`)**:
  To operate completely air-gapped without automatic downloads on first run, specify the path to a directory containing pre-downloaded CTranslate2 Whisper model files:
  ```bash
  python main.py ingest-voice --file meeting.mp3 --model /path/to/local/whisper-small-ct2
  ```

### Configuration & CLI Usage:

The Voice connector can be executed via the dedicated `ingest-voice` subcommand or the generic `ingest --source voice` command:

```bash
# Ingest and transcribe a single audio recording:
python main.py ingest-voice --file meeting.mp3

# Transcribe with a specific model and language:
python main.py ingest-voice --file lecture.m4a --model medium --language en

# Enable Voice Activity Detection (VAD) filter:
python main.py ingest-voice --file interview.wav --vad

# Automatically select CUDA GPU if available, or fall back to CPU:
python main.py ingest-voice --file audio.wav --device auto

# Run on GPU with float16 precision:
python main.py ingest-voice --file audio.flac --device cuda --compute-type float16

# Ingest an entire directory of audio recordings (non-recursive):
python main.py ingest-voice --directory ./recordings

# Ingest multiple specific audio files:
python main.py ingest-voice --files ./notes/memo1.mp3,./notes/memo2.m4a

# Enforce a custom maximum duration (e.g. reject recordings > 30 minutes before transcribing):
python main.py ingest-voice --file recording.wav --max-duration 1800

# Use a pre-downloaded offline local model directory:
python main.py ingest-voice --file meeting.mp3 --model /path/to/local/model-dir

# Generic CLI syntax:
python main.py ingest --source voice --file interview.mp3 --model small
```

### Model Selection & Hardware Recommendations:
- **`tiny` / `base`**: Fastest execution, minimal RAM usage (< 1 GB), suitable for quick personal voice notes on lightweight CPU hardware.
- **`small` (Default)**: Excellent English and multilingual accuracy, runs comfortably on CPU (~2 GB RAM), recommended default for desktop use.
- **`medium` / `large-v3`**: Highest transcription accuracy for technical jargon, accents, or difficult audio; recommended with GPU acceleration (`--device cuda`) or modern multi-core CPUs.

---

## Running Tests

Run the complete test suite with `pytest`:

```bash
pytest -v
```

All 680 unit and integration tests verify:
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
- Google Keep connector registration (`google-keep`) and CLI source listing
- Google Keep missing and invalid export path error handling (`SourceError`)
- Google Keep single JSON file and directory ingestion
- Google Keep deterministic file discovery order
- Google Keep malformed JSON isolation without aborting batch
- Google Keep title extraction and fallback logic
- Google Keep plain text paragraph structure preservation
- Google Keep microsecond timestamp conversion (`createdTimestampUsec`, `userEditedTimestampUsec`)
- Google Keep checklist conversion to Markdown `- [ ]` and `- [x]` tasks
- Google Keep plain text and checklist deduplication
- Google Keep labels extraction, `#` sanitization, and frontmatter tags mapping
- Google Keep note color preservation and `keep-*` tags
- Google Keep archived and trashed state tracking
- Google Keep stable source identity derivation (`google-keep:<id>`)
- Google Keep note generation under `Ingested/Notes/`
- Google Keep deduplication and in-place update lifecycle
- Google Keep filename collision disambiguation
- Google Keep local attachment copying to `Attachments/Ingested/` and Obsidian embeds
- Google Keep attachment collision resolution and embed synchronization
- Google Keep missing attachment resilience
- Google Keep CLI commands (`ingest-google-keep` and `ingest --source google-keep --path`)
- Readwise connector registration (`readwise`) and CLI source listing
- Readwise missing token error handling (`SourceError`)
- Readwise token authentication via `READWISE_TOKEN`, constructor, or CLI `--token`
- Readwise token secrecy (tokens and authorization headers never written to logs or error messages)
- Readwise v2 Export API retrieval and cursor-based pagination (`nextPageCursor` and `next` URL)
- Readwise pagination loop defense against repeated cursors and duplicate or cyclic next URLs
- Readwise empty response and empty highlights resilience
- Readwise API error handling (401 unauthorized, 429 rate limit, 500 server error, timeouts, connection errors)
- Readwise batch fault isolation (individual malformed records skipped without aborting healthy items)
- Readwise duplicate item deduplication within runs
- Readwise YAML frontmatter generation (`title`, `date`, `source`, `tags`, `author`, `source_url`, `readwise_id`, `category`, `num_highlights`)
- Readwise highlights rendered as Markdown `> blockquotes` with multi-line support
- Readwise highlight location and page number preservation (`*Location: page 42*`)
- Readwise inline personal notes preservation (`**My note:** ...`)
- Readwise omission of empty fake notes when personal notes are absent
- Readwise top-level document summary and document note sections (`## Summary`, `## Document Note`)
- Readwise tag extraction and normalization (symbol `#` stripped, duplicate-free)
- Readwise note generation targeted directly under `Ingested/Web/`
- Readwise filename sanitization, 80-character title truncation, and collision handling (`_2.md`)
- Readwise deduplication lifecycle (NEW creates note, UNCHANGED skips, CHANGED overwrites in-place at same path)
- Readwise stable immutable source ID (`user_book_id`, `id`, `book_id`, stable URL fallback, and rejection of records lacking ID or URL to prevent title drift)
- Readwise edge case handling (missing title fallback, missing author/URL, Unicode emojis/symbols)
- Readwise CLI commands (`ingest-readwise` with `--book-id` and `--updated-after`, and generic `ingest --source readwise`)
- Instapaper connector registration (`instapaper`) and CLI source listing
- Instapaper missing credentials error handling (`SourceError`)
- Instapaper authentication via `INSTAPAPER_TOKEN` (Bearer token) and username/password fallback
- Instapaper token and credential secrecy (tokens and passwords never logged or included in exceptions or frontmatter)
- Instapaper API v2 (`bookmarks` dictionary) and API v1 (mixed bookmark and highlight list) response parsing
- Instapaper pagination loop defense (cursor tracking, duplicate batch fingerprint detection, maximum page limits)
- Instapaper empty response and empty bookmark list resilience
- Instapaper API error handling (401 unauthorized, 403 forbidden, 404 not found, 429 rate limit, 500 server error, timeouts, connection errors)
- Instapaper batch fault isolation (individual malformed records skipped without aborting healthy items)
- Instapaper bookmark deduplication within ingestion runs
- Instapaper YAML frontmatter generation (`title`, `date`, `source`, `tags`, `author`, `source_url`, `bookmark_id`, `folder`, `progress`, `starred`, `num_highlights`)
- Instapaper highlight extraction and standard Markdown `> blockquotes` formatting with multi-line support
- Instapaper personal notes attached to highlights formatted inline as `**My note:** note_text`
- Instapaper omission of highlights section when no highlights exist
- Instapaper article content vs. excerpt extraction (`## Content`, `## Excerpt`, and placeholder fallback)
- Instapaper tag extraction and normalization (symbol `#` stripped, duplicate-free, mandatory `ingested` tag)
- Instapaper note generation targeted directly under `Ingested/Web/`
- Instapaper filename sanitization, 80-character title truncation, and collision handling (`_2.md`)
- Instapaper deduplication lifecycle (NEW creates note, UNCHANGED skips, CHANGED overwrites in-place at same path)
- Instapaper stable immutable source ID (`instapaper:<bookmark_id>`, rejecting items without bookmark ID to prevent title drift)
- Instapaper edge case handling (missing title fallback, missing author/URL, Unicode emojis/symbols)
- Instapaper CLI commands (`ingest-instapaper` with `--folder` and `--limit`, and generic `ingest --source instapaper`)
- GitHub connector registration (`github`) and CLI source listing
- GitHub authentication via `GITHUB_TOKEN` and token secrecy (never logged, exposed in exceptions, or written to frontmatter)
- GitHub repository issues retrieval with pull requests strictly filtered out
- GitHub issue comment pagination and chronological markdown formatting (`### author — date`)
- GitHub user gists retrieval with deterministically ordered files and dynamic code block fences
- GitHub issue and gist YAML frontmatter generation (`item_type: "issue"` or `"gist"`, `issue_number`, `repository`, `state`, `gist_id`, `is_public`)
- GitHub metadata sections (`## GitHub Metadata` and `## Gist Metadata`)
- GitHub tag extraction with label normalization and mandatory `ingested`, `github`, and `code` tags
- GitHub note generation targeted directly under `Ingested/Code/`
- GitHub filename sanitization, 80-character title truncation, and collision handling (`_2.md`)
- GitHub deduplication lifecycle (NEW creates note, UNCHANGED skips, CHANGED overwrites in-place at same path)
- GitHub stable immutable source IDs (`github:issue:<owner>/<repo>#<number>`, `github:gist:<gist_id>`, rejecting missing identifiers to prevent title drift)
- GitHub API error handling (401 unauthorized, 403 forbidden, 404 repo isolation, 429 rate limit with reset time, 500 server error, timeouts, connection errors)
- GitHub batch fault isolation across repositories and individual records
- GitHub CLI commands (`ingest-github` with `--repo`, `--repos`, `--state`, `--include-gists`, `--gists-only`, `--no-comments`, `--limit`, and generic `ingest --source github`)
- GitHub HTML content normalization via `markdownify` ensuring pure Markdown output without raw HTML tags or script/style elements, while preserving ordinary Markdown, headings, lists, tables, code blocks, and comparison operators (`<` and `>`)
- GitHub issue comments explicit query parameters (`sort=created`, `direction=asc`) and local chronological sorting by `created_at` ascending and `id` ascending
- GitHub malformed comment resilience (safe author extraction for missing, None, non-dict, or login-less user objects without aborting the issue or valid comments)
- GitHub truncated gist file recovery via `raw_url` (fetching complete code on HTTP 200, preserving language, falling back to partial content with truncation notice on failure, secret-safe logging)
- GitHub gist body single `## Files` section header enforcement and duplicate prevention
- Reddit connector registration (`reddit`) and CLI source listing
- Reddit OAuth2 application-only authentication (`client_credentials`) and direct Bearer token support
- Reddit credential and token secrecy (credentials scrubbed from logs, errors, and frontmatter)
- Reddit subreddit name validation, `r/` prefix stripping, and comma-separated list parsing
- Reddit posts retrieval across configured subreddits (`hot`, `new`, `top`, `rising`)
- Reddit advertisement and sponsored post filtering (`promoted: true`)
- Reddit self-posts full Markdown text extraction and link posts external URL attribution
- Reddit comment tree recursive traversal with configurable cap (`--max-comments`) and omission (`--no-comments`)
- Reddit comment chronological ordering (`sort=old` API request and local `(created_utc, id)` sorting)
- Reddit nested replies formatted as readable blockquotes with reply attribution (`> **↳ @user** ... *(reply to @parent)*:`)
- Reddit deleted author (`[deleted]`) and deleted/removed body handling
- Reddit pure Markdown content normalization via `markdownify` stripping raw HTML and `<script>`/`<style>` blocks
- Reddit HTML entities unescaping (`&gt;`, `&lt;`, `&amp;`) and comparisons (`<`, `>`) preservation
- Reddit cursor-based pagination loop defense and repeated cursor cycle prevention
- Reddit notes saved directly to `Ingested/Social/` with YAML frontmatter and attribution blockquotes
- Reddit stable immutable source IDs (`reddit:post:<post_id>`) and in-place update lifecycle
- Reddit error mapping (401 auth, 403 forbidden, 404 not found, 429 rate limit with reset time, 500 server error, timeouts, connection errors)
- Reddit batch fault isolation across subreddits, individual posts, and comment threads
- Reddit CLI commands (`ingest-reddit` with `--subreddit`, `--subreddits`, `--listing`, `--limit`, `--max-comments`, `--no-comments`, and generic `ingest --source reddit`)
- Slack connector registration (`slack`) and CLI source listing
- Slack Bearer token authentication (`SLACK_TOKEN`, `SLACK_BOT_TOKEN`, `--token`) and header construction
- Slack secret and token scrubbing (tokens redacted from errors, logs, frontmatter, and markdown)
- Slack conversation discovery across public and private channels (`conversations.list`) with cursor pagination
- Slack channel resolution by ID or name (with or without `#`)
- Slack message history ingestion (`conversations.history`) with cursor pagination and limit enforcement
- Slack system message filtering (`channel_join`, `channel_leave`, `channel_topic`, etc.)
- Slack message subtypes handling (`message_changed` inner message extraction, `message_deleted` skipping)
- Slack thread reply retrieval (`conversations.replies`) and chronological sorting (`ts` ascending)
- Slack thread single-document Markdown rendering with parent and reply hierarchy
- Slack user resolution caching via `users.list` and on-demand `users.info` fallback
- Slack mrkdwn normalization (`<@U...>`, `<#C...>`, `<!here>`, `<!channel>`, `<!everyone>`)
- Slack URL safety validation (HTTP/HTTPS linkified, unsafe schemes rendered as plain text)
- Slack pure Markdown normalization via `markdownify` stripping raw HTML and `<script>`/`<style>` elements
- Slack comparisons (`<`, `>`) and code block preservation
- Slack cursor loop defense and repeated cursor cycle prevention
- Slack notes saved directly to `Ingested/Social/` with YAML frontmatter and attribution blockquotes
- Slack stable immutable source IDs (`slack:message:<channel_id>:<root_ts>`) and in-place update lifecycle
- Slack error mapping (HTTP 200 `ok: false`, `invalid_auth`, `missing_scope`, `channel_not_found`, `not_in_channel`, `token_revoked`)
- Slack rate limit defense (HTTP 429 and `ratelimited` error handling with `Retry-After` reset duration)
- Slack fault isolation across channels, individual messages, and thread reply retrieval
- Slack CLI commands (`ingest-slack` with `--channel`, `--channels`, `--token`, `--limit`, `--max-messages`, `--max-replies`, `--no-threads`, and generic `ingest --source slack`)
- Discord connector registration (`discord`) and CLI source listing
- Discord Bot token authentication (`DISCORD_BOT_TOKEN`, `--token`) and header construction (`Bot <token>`)
- Discord secret and token scrubbing (tokens redacted from errors, logs, frontmatter, and markdown)
- Discord channel discovery across configured guilds (`/guilds/{id}/channels`) and direct channel resolution
- Discord text channel filtering and voice/stage channel exclusion (types 2 and 13)
- Discord message history ingestion (`/channels/{id}/messages`) with `before` cursor pagination and limit enforcement
- Discord Message Content Intent verification and clear setup guidance preventing blank notes
- Discord thread reply retrieval (`/channels/{thread_id}/messages`) and chronological ordering
- Discord thread single-document Markdown rendering with parent and reply hierarchy
- Discord author resolution (`global_name` -> `username` -> `user_id` -> `[user unavailable]`)
- Discord markup normalization (`<@...>`, `<#...>`, `<@&...>`, custom emojis `<:name:id>`)
- Discord URL safety validation (HTTP/HTTPS linkified, unsafe schemes rendered as plain/code text)
- Discord pure Markdown normalization via `markdownify` stripping raw HTML and `<script>`/`<style>` elements
- Discord comparisons (`<`, `>`) and code block preservation
- Discord notes saved directly to `Ingested/Social/` with YAML frontmatter and attribution blockquotes
- Discord stable immutable source IDs (`discord:message:<channel_id>:<message_id>`) and in-place update lifecycle
- Discord error mapping (401, 403 `50001` missing access, `50013` missing permissions, 404 `10003` channel, `10004` guild, 5xx server errors, timeout)
- Discord rate limit defense (HTTP 429 with `Retry-After`, `retry_after` JSON, and `X-RateLimit-Scope` global/route/shared)
- Discord fault isolation across channels, individual messages, and thread reply retrieval
- Discord CLI commands (`ingest-discord` with `--guild`, `--guilds`, `--channel`, `--channels`, `--token`, `--limit`, `--max-messages`, `--max-replies`, `--no-threads`, and generic `ingest --source discord`)
- Telegram connector registration (`telegram`) and CLI source listing
- Telegram Bot token authentication (`TELEGRAM_BOT_TOKEN`, `--token`) and URL construction
- Telegram secret and token scrubbing (tokens redacted from errors, logs, frontmatter, and markdown)
- Telegram message updates retrieval (`getUpdates`) with offset advancement and limit enforcement
- Telegram chat filtering by numeric ID, username (`@channel`), or chat title
- Telegram webhook conflict detection (HTTP 409) with actionable configuration instructions
- Telegram UTF-16 code unit offset alignment for entity extraction with emoji stability
- Telegram entity formatting (bold, italic, code, pre with language tags, strikethrough, spoiler, links)
- Telegram URL safety validation (HTTP/HTTPS linkified, unsafe schemes rendered as plain text)
- Telegram pure Markdown normalization via `markdownify` stripping raw HTML and `<script>`/`<style>` elements
- Telegram media metadata extraction (photos, documents, videos, audio, voice, stickers, polls, venues, locations)
- Telegram contact privacy sanitization (phone numbers and vCards redacted)
- Telegram reply preview blockquotes and forward attribution formatting
- Telegram notes saved directly to `Ingested/Social/` with YAML frontmatter and attribution blockquotes
- Telegram stable immutable source IDs (`telegram:message:<chat_id>:<message_id>`) and in-place update lifecycle
- Telegram rate limit defense (HTTP 429 flood wait with `parameters.retry_after` and `Retry-After` header backoff)
- Telegram fault isolation across updates and individual message conversions
- Telegram CLI commands (`ingest-telegram` with `--chat`, `--chats`, `--token`, `--limit`, `--max-messages`, `--max-retries`, `--offset`, and generic `ingest --source telegram`)
- Voice / Audio connector registration (`voice`) and CLI source listing
- Voice input validation (missing file, directory passed as file, unsupported formats, 0-byte files, file size limits, permissions)
- Supported audio formats validation (.wav, .mp3, .m4a, .flac, .ogg, .opus, .aac, .webm)
- Voice chunked streaming SHA-256 calculation stability and memory safety
- Voice content-based deduplication (`voice:sha256:<content_hash>`) across file moves and renames
- Voice timestamp and duration formatting (`format_timestamp`, `format_voice_duration`, seconds, minutes, hours)
- Voice transcript formatting with human-readable timestamp intervals (`[HH:MM:SS - HH:MM:SS]`)
- Voice empty audio and silence handling (`*No speech detected in audio file.*`)
- Voice note title sanitization (stripping forbidden characters, whitespace collapsing, 80-char cap, embedded title tag preference)
- Audio container metadata probing via PyAV (`av`) with graceful fallback
- Speech-to-text transcription parsing with injected mock transcriber (dependency injection)
- Language handling: automatic language detection vs. explicit language override
- Safety threshold enforcement: maximum audio duration limit (`--max-duration`)
- Model execution and hardware handling: CPU defaults, quantization, and CUDA validation error mapping
- Transcription error mapping (out of memory, CUDA driver failure, decoder error, generic runtime errors)
- Audio batch processing across single files, multiple files (`--files`), and directories (`--directory`)
- Directory scanning non-recursively with per-file fault isolation (skipping corrupt files without aborting batch)
- Voice notes saved directly to `Ingested/Voice/` with YAML frontmatter tags `[ingested, voice, audio]`
- Attribution block rendering displaying source file, duration, language, model, and date
- Special markdown characters in transcripts preserved safely without corrupting frontmatter
- CLI subcommands (`ingest-voice` with `--file`, `--files`, `--directory`, `--model`, `--language`, `--device`, `--compute-type`, `--beam-size`, `--vad`, `--max-duration`, and generic `ingest --source voice`)

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
- [x] **Google Keep Source Connector** (`sources/keep_source.py`):
  - [x] Google Takeout JSON directory and single file discovery
  - [x] Deterministic processing order and malformed JSON fault isolation
  - [x] Title extraction and microsecond timestamp parsing
  - [x] Checklist conversion (`- [ ]` / `- [x]`) and text-checklist deduplication
  - [x] Labels to tags mapping with `#` symbol sanitization
  - [x] Color metadata preservation (`keep_color` and `keep-<color>` tags)
  - [x] Archived and trashed state tracking
  - [x] Local attachment copying to `Attachments/Ingested/` with collision handling and Obsidian embeds
  - [x] Notes saved to `Ingested/Notes/` with YAML frontmatter
  - [x] Stable identity (`google-keep:<id>` / `google-keep:<timestamp>`) & SHA-256 deduplication lifecycle
  - [x] CLI `ingest-google-keep` and `ingest --source google-keep --path` commands
- [x] **Readwise Source Connector** (`sources/readwise_source.py`):
  - [x] Official Readwise v2 Export API integration with cursor-based pagination
  - [x] Secure authentication via `READWISE_TOKEN` (zero token leaking in logs or exceptions)
  - [x] Individual item batch fault isolation
  - [x] Highlights rendered as clean Markdown `> blockquotes` with location/page numbers
  - [x] Inline personal notes preservation (`**My note:** ...`)
  - [x] Document summary and document note sections (`## Summary`, `## Document Note`)
  - [x] Tag normalization and `#` symbol sanitization
  - [x] Notes saved directly to `Ingested/Web/` with YAML frontmatter and attribution blockquotes
  - [x] Stable identity (`readwise:<user_book_id>`) & in-place update lifecycle
  - [x] CLI `ingest-readwise` with `--book-id` and `--updated-after`, and generic `ingest --source readwise`
- [x] **Instapaper Source Connector** (`sources/instapaper_source.py`):
  - [x] Dual Instapaper API v2 (Bearer token / Personal Access Token) and v1 (username/password) support
  - [x] Secure authentication via `INSTAPAPER_TOKEN` (zero token leaking in logs or exceptions)
  - [x] Folder filtering (`unread`, `archive`, `starred`) and limit options
  - [x] Individual item batch fault isolation and pagination loop defense
  - [x] Article content conversion via `markdownify` and excerpt fallback
  - [x] Highlights rendered as clean Markdown `> blockquotes` with personal notes (`**My note:** ...`)
  - [x] Tag normalization and `#` symbol sanitization
  - [x] Notes saved directly to `Ingested/Web/` with YAML frontmatter and attribution blockquotes
  - [x] Stable identity (`instapaper:<bookmark_id>`) & in-place update lifecycle
  - [x] CLI `ingest-instapaper` with `--folder` and `--limit`, and generic `ingest --source instapaper`
- [x] **GitHub Source Connector** (`sources/github_source.py`):
  - [x] GitHub REST API integration for repository issues and user gists
  - [x] Pull request filtering (`pull_request` key detection)
  - [x] Issue comments retrieval and chronological formatting (`### author — date`)
  - [x] User gists ingestion with dynamic code fences (`format_code_fence`) adapting to file backtick counts
  - [x] Secure authentication via `GITHUB_TOKEN` (zero token leaking in logs, exceptions, or frontmatter)
  - [x] Tag normalization and `#` symbol sanitization with mandatory `ingested`, `github`, and `code` tags
  - [x] Notes saved directly to `Ingested/Code/` with YAML frontmatter and attribution blockquotes
  - [x] Stable identity (`github:issue:<owner>/<repo>#<number>`, `github:gist:<gist_id>`) & in-place update lifecycle
  - [x] Batch fault isolation across repositories and individual malformed records
  - [x] Defensive pagination with duplicate batch fingerprint tracking
  - [x] Pure Markdown content normalization via `markdownify` stripping raw HTML and `<script>`/`<style>` elements
  - [x] Explicit chronological comment querying (`sort=created`, `direction=asc`) and local sorting (`created_at` asc, `id` asc)
  - [x] Non-fatal malformed comment and user data resilience (missing/None/non-dict/login-less user objects default safely to `unknown`)
  - [x] Truncated gist file recovery via `raw_url` with partial content fallback and truncation notices
  - [x] CLI `ingest-github` with `--repo`, `--repos`, `--state`, `--include-gists`, `--gists-only`, `--no-comments`, `--limit`, and generic `ingest --source github`
- [x] **Reddit Source Connector** (`sources/reddit_source.py`):
  - [x] Reddit REST API integration with OAuth2 Application-Only authentication
  - [x] Subreddit post retrieval (`hot`, `new`, `top`, `rising`) with ad filtering (`promoted: true`)
  - [x] Comment tree traversal with chronological ordering (`sort=old`) and local sorting (`(created_utc, id)` asc)
  - [x] Nested comment flattening into readable blockquotes with reply attribution
  - [x] Markdown content normalization via `markdownify` stripping raw HTML and unescaping entities
  - [x] Secure authentication with credentials scrubbed from logs, errors, and frontmatter
  - [x] Notes saved directly to `Ingested/Social/` with YAML frontmatter and attribution blockquotes
  - [x] Stable identity (`reddit:post:<post_id>`) & in-place update lifecycle
  - [x] Batch fault isolation across subreddits, individual posts, and comment threads
  - [x] CLI `ingest-reddit` with `--subreddit`, `--subreddits`, `--listing`, `--limit`, `--max-comments`, `--no-comments`, and generic `ingest --source reddit`
- [x] **Slack Source Connector** (`sources/slack_source.py`):
  - [x] Slack Web API integration with Bearer token authentication (`SLACK_TOKEN`, `SLACK_BOT_TOKEN`)
  - [x] Channel discovery (`conversations.list`) across public and private channels with cursor pagination
  - [x] Message history fetching (`conversations.history`) with system noise filtering and subtype handling
  - [x] Thread reply retrieval (`conversations.replies`) with chronological ordering and single-document rendering
  - [x] In-memory user resolution cache (`users.list`, `users.info`)
  - [x] Text normalization for `<@U...>`, `<#C...>`, `<!here>`, and HTTP/HTTPS safe URL links
  - [x] Pure Markdown content normalization via `markdownify` stripping raw HTML and script/style tags
  - [x] Secure authentication with credentials scrubbed from logs, errors, and frontmatter
  - [x] Notes saved directly to `Ingested/Social/` with YAML frontmatter and attribution blockquotes
  - [x] Stable identity (`slack:message:<channel_id>:<root_ts>`) & in-place update lifecycle
  - [x] Rate limit handling (HTTP 429 / `ratelimited` with `Retry-After` header extraction)
  - [x] Fault isolation across channels, individual messages, and thread replies
  - [x] CLI `ingest-slack` with `--channel`, `--channels`, `--token`, `--limit`, `--max-messages`, `--max-replies`, `--no-threads`, and generic `ingest --source slack`
- [x] **Discord Source Connector** (`sources/discord_source.py`):
  - [x] Official Discord REST API v10 integration with Bot token authentication (`DISCORD_BOT_TOKEN`, `--token`)
  - [x] Guild channel discovery (`/guilds/{id}/channels`) and direct channel resolution
  - [x] Voice channel exclusion (types 2 and 13) and category channel filtering
  - [x] Message history fetching (`/channels/{id}/messages`) with `before` backwards pagination
  - [x] Message Content Intent verification preventing silent ingestion of blank notes
  - [x] Thread reply retrieval (`/channels/{thread_id}/messages`) with chronological sorting
  - [x] Single-document Markdown rendering with root message and reply hierarchy
  - [x] Author resolution (`global_name` -> `username` -> `user_id` -> `[user unavailable]`)
  - [x] Markup normalization for `<@...>`, `<#...>`, `<@&...>`, and custom emojis
  - [x] Safe URL validation (HTTP/HTTPS linkified, unsafe schemes rendered as plain/code text)
  - [x] Pure Markdown normalization via `markdownify` stripping raw HTML and `<script>`/`<style>`
  - [x] Comparisons (`<`, `>`) and code block preservation
  - [x] Notes saved directly to `Ingested/Social/` with YAML frontmatter and attribution blockquotes
  - [x] Stable identity (`discord:message:<channel_id>:<message_id>`) & in-place update lifecycle
  - [x] Rate limit handling (HTTP 429 with `Retry-After` header, JSON `retry_after`, and `X-RateLimit-Scope`)
  - [x] Fault isolation across channels, individual messages, and thread replies
- [x] **Telegram Source Connector** (`sources/telegram_source.py`):
  - [x] Official Telegram Bot API integration with Bot token authentication (`TELEGRAM_BOT_TOKEN`, `--token`)
  - [x] Updates ingestion via `getUpdates` with offset advancement (`offset = update_id + 1`) and batch limit enforcement
  - [x] Webhook conflict detection (HTTP 409) with actionable resolution guidance
  - [x] Chat filtering across private chats, groups, supergroups, and channels (numeric ID, `@username`, title)
  - [x] Group privacy mode guidance and channel administrator requirements
  - [x] Accurate UTF-16 code unit entity parsing preventing emoji-induced formatting offsets
  - [x] Entity rendering (bold, italic, code, pre with language tags, underline, strikethrough, spoiler, text_link, url)
  - [x] URL safety validation (HTTP/HTTPS linkified, unsafe schemes rendered as plain text)
  - [x] Pure Markdown normalization via `markdownify` stripping raw HTML and script/style tags
  - [x] Reply previews (`## Replying to`) and forward attribution preservation
  - [x] Media metadata extraction without binary downloads; contact phone number redaction
  - [x] Notes saved directly to `Ingested/Social/` with YAML frontmatter and attribution blockquotes
  - [x] Stable identity (`telegram:message:<chat_id>:<message_id>`) & in-place update lifecycle
  - [x] Rate limit handling (HTTP 429 flood wait with bounded backoff and retry)
  - [x] Fault isolation across updates and individual messages
  - [x] CLI `ingest-telegram` with `--chat`, `--chats`, `--token`, `--limit`, `--max-messages`, `--max-retries`, `--offset`, and generic `ingest --source telegram`
- [x] **Voice / Audio Source Connector** (`sources/voice_source.py`):
  - [x] Local-first Whisper transcription using `faster-whisper` (CTranslate2 backend) with zero paid API dependencies
  - [x] Supported formats: `.wav`, `.mp3`, `.m4a`, `.flac`, `.ogg`, `.opus`, `.aac`, `.webm`
  - [x] Streaming chunked 64 KB SHA-256 deduplication identity (`voice:sha256:<content_hash>`)
  - [x] Model options: `tiny`, `base`, `small` (default), `medium`, `large-v3`, or local path
  - [x] Quantization & compute types: `int8`, `float16`, `float32`, `default`
  - [x] Device support: `cpu` default, `cuda` with availability validation
  - [x] Formatted timestamp intervals: `[HH:MM:SS - HH:MM:SS]` scaling across seconds, minutes, and hours
  - [x] Embedded title tag preference with fallback to sanitized filename stem (capped at 80 chars)
  - [x] Silence detection fallback (`*No speech detected in audio file.*`)
  - [x] Voice Activity Detection (VAD) filter option (`--vad`)
  - [x] Safety guards: preflight `--max-duration` rejection before transcribing, post-transcription duration validation fallback, and 2 GB file size limits
  - [x] Execution hardware: `--device auto` with dynamic CUDA detection and safe CPU fallback, validated `--device cuda`, and default `--device cpu`
  - [x] Fault-isolated directory scanning (`--directory`) skipping bad files without aborting
  - [x] Notes saved directly to `Ingested/Voice/` with frontmatter tags `[ingested, voice, audio]`
  - [x] Attribution block displaying source file, duration, language, model, and date
  - [x] CLI `ingest-voice` with `--file`, `--files`, `--directory`, `--model`, `--language`, `--device`, `--compute-type`, `--beam-size`, `--vad`, `--max-duration`, and generic `ingest --source voice`
- [x] **Screenshots / OCR Source Connector** (`sources/screenshot_source.py`):
  - [x] Local-first OCR using Tesseract (`pytesseract`) with zero paid cloud API dependencies
  - [x] Supported formats: `.png`, `.jpg`, `.jpeg`, `.webp`, `.bmp`, `.tiff`, `.tif`, `.gif`
  - [x] Streaming chunked 64 KB SHA-256 deduplication identity (`screenshot:sha256:<content_hash>`)
  - [x] Image validation: non-empty file, size limits (default 100 MB), decompression bomb pixel limits (default 100M pixels), Pillow header integrity verification
  - [x] In-memory preprocessing: RGBA/LA alpha composite onto solid white background, grayscale conversion, Lanczos upscaling for small images (< 600px), 1.5x contrast enhancement (original file copied unmodified)
  - [x] Privacy safeguards: OCR text never logged to application logs; EXIF GPS geolocation tags stripped
  - [x] Text normalization & cleaning: control character removal, trailing whitespace stripping, newline collapse
  - [x] Missing text handling: `ocr_status: "no_text"`, `ocr_word_count: 0`, and placeholder `*No text detected.*`
  - [x] Attachment management: copies original image to `Attachments/Ingested/` with collision-safe naming (`_2`, `_3`) and automatic `![[image]]` body embed update
  - [x] Notes saved directly to `Ingested/Screenshots/` with YAML frontmatter tags `[ingested, screenshots, ocr]`
  - [x] Attribution block displaying source image filename, dimensions (`WxH`), format, OCR status, word count, and date
  - [x] Configurable OCR language (`--ocr-lang`, `SCREENSHOT_OCR_LANG`), timeout (`--ocr-timeout`, `SCREENSHOT_OCR_TIMEOUT`), and custom binary path (`--tesseract-cmd`, `SCREENSHOT_TESSERACT_CMD`)
  - [x] Fault-isolated directory scanning (`--directory`, `--recursive`) skipping bad images without aborting
  - [x] CLI `ingest-screenshots` with positional/flag directory, `--file`, `--files`, `--recursive`, `--ocr-lang`, `--tesseract-cmd`, `--ocr-timeout`, `--max-file-size`, `--max-pixels`, and generic `ingest --source screenshots`
- [x] 100% test pass rate across all 735 tests (55 dedicated Screenshots / OCR tests).

### Next Connectors to Implement:
1. **Tier 3 Connectors**: Twitter/X.






