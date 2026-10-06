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
│   ├── github_source.py  # GitHub issues and gists connector
│   ├── instapaper_source.py # Instapaper bookmark and highlight connector
│   ├── keep_source.py    # Google Keep Takeout JSON connector
│   ├── notion_source.py  # Notion page connector
│   ├── pdf_source.py     # PDF document connector
│   ├── readwise_source.py# Readwise highlight and book connector
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
│   ├── test_github_source.py # GitHub connector tests
│   ├── test_instapaper_source.py # Instapaper connector tests
│   ├── test_keep_source.py # Google Keep connector tests
│   ├── test_notion_source.py# Notion connector tests
│   ├── test_pdf_source.py  # PDF connector tests
│   ├── test_pipeline.py    # Pipeline orchestration and retry tests
│   ├── test_readwise_source.py # Readwise connector tests
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

## Running Tests

Run the complete test suite with `pytest`:

```bash
pytest -v
```

All 397 unit and integration tests verify:
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
- [x] 100% test pass rate across all 397 tests.

### Next Connectors to Implement:
1. **Tier 3 Connectors**: Twitter/X, Slack/Discord/Telegram, Audio Whisper transcription, OCR screenshots.




