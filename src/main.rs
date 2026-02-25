use chrono::{DateTime, Utc};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use rayon::prelude::*;
use regex::Regex;
use rusqlite::{Connection, params};
use scraper::{Html, Selector};
use sentencex::segment;
use spider::compact_str::CompactString;
use spider::configuration::Configuration;
use spider::tokio;
use spider::website::Website;
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fs;
use std::io::{Write, stdin, stdout};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use termios::{ECHO, TCSANOW, Termios, tcsetattr};
use url::Url;

const BASE_PATH: &str = "/Users/hariharprasad/MyDocuments/Code/Rust/deep-spyder";
const DEFAULT_TOP_K: usize = 5;
const DEFAULT_CONTEXT_WINDOW: usize = 0;
const DEFAULT_EMBEDDING_MODEL: EmbeddingModel = EmbeddingModel::MxbaiEmbedLargeV1Q;

static CONTENT_SELECTORS: LazyLock<Vec<Selector>> = LazyLock::new(|| {
    [
        "article",
        "main",
        r#"[role="main"]"#,
        ".content",
        ".docs-content",
        ".doc-content",
        ".markdown-body",
        ".theme-doc-markdown",
        "#content",
        "#main-content",
        "body",
    ]
    .iter()
    .filter_map(|s| Selector::parse(s).ok())
    .collect()
});

static HTML_CLEANUP_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"(?is)<!--.*?-->",
        r"(?is)<script\b[^>]*>.*?</script>",
        r"(?is)<style\b[^>]*>.*?</style>",
        r"(?is)<noscript\b[^>]*>.*?</noscript>",
        r"(?is)<template\b[^>]*>.*?</template>",
        r"(?is)<nav\b[^>]*>.*?</nav>",
        r"(?is)<header\b[^>]*>.*?</header>",
        r"(?is)<footer\b[^>]*>.*?</footer>",
        r"(?is)<aside\b[^>]*>.*?</aside>",
        r#"(?is)<(div|section)[^>]*(id|class)\s*=\s*["'][^"']*(nav|menu|sidebar|footer|header|toc|breadcrumb|pagination|cookie|consent|search|feedback|promo|banner|advert|ads|social|share)[^"']*["'][^>]*>.*?</\1>"#,
        r"(?is)<button\b[^>]*>.*?</button>",
        r"(?is)<form\b[^>]*>.*?</form>",
    ]
    .iter()
    .filter_map(|p| Regex::new(p).ok())
    .collect()
});

static MARKDOWN_LINE_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"(?im)^\s*was this helpful\?\s*$",
        r"(?im)^\s*copy page\s*$",
        r"(?im)^\s*menu\s*$",
        r"(?im)^\s*send\s*$",
        r"(?im)^\s*latest version\s*$",
        r"(?im)^\s*supported\.\s*$",
    ]
    .iter()
    .filter_map(|p| Regex::new(p).ok())
    .collect()
});

static MULTI_NEWLINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\n{3,}").expect("valid regex"));

static HTML_REGEX_HINTS: &[&str] = &[
    "<!--",
    "<script",
    "<style",
    "<noscript",
    "<template",
    "<nav",
    "<header",
    "<footer",
    "<aside",
    "sidebar",
    "breadcrumb",
    "cookie",
    "<button",
    "<form",
];

static MARKDOWN_HINTS: &[&str] = &[
    "was this helpful?",
    "copy page",
    "menu",
    "send",
    "latest version",
    "supported.",
];

struct TerminalEchoGuard {
    fd: i32,
    original: Termios,
}

impl TerminalEchoGuard {
    fn new() -> Option<Self> {
        let fd = stdin().as_raw_fd();
        let mut current = Termios::from_fd(fd).ok()?;
        let original = current.clone();
        current.c_lflag &= !ECHO;
        tcsetattr(fd, TCSANOW, &current).ok()?;
        Some(Self { fd, original })
    }
}

impl Drop for TerminalEchoGuard {
    fn drop(&mut self) {
        let _ = tcsetattr(self.fd, TCSANOW, &self.original);
    }
}

#[derive(Clone, Copy, Debug)]
enum SearchMode {
    Hybrid,
    Vector,
    Keyword,
}

impl SearchMode {
    fn from_str(input: &str) -> Self {
        match input.to_ascii_lowercase().as_str() {
            "vector" => Self::Vector,
            "keyword" => Self::Keyword,
            _ => Self::Hybrid,
        }
    }
}

#[derive(Debug, Clone)]
struct ChunkRecord {
    id: i64,
    library_name: String,
    source_url: String,
    source_page_order: i64,
    chunk_index_in_page: i64,
    global_chunk_index: i64,
    content: String,
    embedding: Vec<f32>,
}

#[derive(Debug, Clone)]
struct ScoredChunk {
    chunk: ChunkRecord,
    vector_score: f32,
    keyword_score: f32,
    final_score: f32,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let _echo_guard = TerminalEchoGuard::new();

    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        print_help();
        return Ok(());
    }

    fs::create_dir_all(artifacts_root())?;
    let conn = init_db(&app_root().join("plshelp.db"))?;
    let command = args[0].as_str();

    match command {
        "add" => {
            if args.len() < 3 {
                return Err("Usage: plshelp add <library_name> <source_url>".into());
            }
            add_library(&conn, &args[1], &args[2]).await?;
            println!("Done.");
        }
        "crawl" => {
            if args.len() < 3 {
                return Err("Usage: plshelp crawl <library_name> <source_url>".into());
            }
            crawl_library(&conn, &args[1], &args[2], "crawl").await?;
            println!("Done.");
        }
        "index" => {
            if args.len() < 2 {
                return Err("Usage: plshelp index <library_name> [--file /path/to/file]".into());
            }
            let file = parse_index_file_flag(&args[2..]);
            index_library(&conn, &args[1], file.as_deref(), "index")?;
            println!("Done.");
        }
        "refresh" => {
            if args.len() < 2 {
                return Err("Usage: plshelp refresh <library_name>".into());
            }
            refresh_library(&conn, &args[1]).await?;
            println!("Done.");
        }
        "query" => {
            if args.len() < 3 {
                return Err(
                    "Usage: plshelp query <library_name> \"<question>\" [--mode hybrid|vector|keyword] [--top-k N] [--context N]"
                        .into(),
                );
            }
            let (mode, top_k, context) = parse_query_flags(&args[3..])?;
            query_library(&conn, &args[1], &args[2], mode, top_k, context, false)?;
        }
        "trace" => {
            if args.len() < 3 {
                return Err(
                    "Usage: plshelp trace <library_name> \"<question>\" [--mode hybrid|vector|keyword] [--top-k N] [--context N]"
                        .into(),
                );
            }
            let (mode, top_k, context) = parse_query_flags(&args[3..])?;
            query_library(&conn, &args[1], &args[2], mode, top_k, context, true)?;
        }
        "ask" => {
            if args.len() < 2 {
                return Err(
                    "Usage: plshelp ask \"<question>\" [--libraries a,b,c] [--mode ...] [--top-k N] [--context N]"
                        .into(),
                );
            }
            ask_libraries(&conn, &args[1], &args[2..])?;
        }
        "alias" => {
            if args.len() < 3 {
                return Err("Usage: plshelp alias <library_name> <alias>".into());
            }
            add_alias(&conn, &args[1], &args[2])?;
            println!("Done.");
        }
        "list" => list_libraries(&conn)?,
        "show" => {
            if args.len() < 2 {
                return Err("Usage: plshelp show <library_name>".into());
            }
            show_library(&conn, &args[1])?;
        }
        "remove" => {
            if args.len() < 2 {
                return Err("Usage: plshelp remove <library_name>".into());
            }
            remove_library(&conn, &args[1])?;
            println!("Done.");
        }
        "open" => {
            if args.len() < 2 {
                return Err("Usage: plshelp open <chunk_id>".into());
            }
            let chunk_id: i64 = args[1].parse()?;
            open_chunk(&conn, chunk_id)?;
        }
        "help" | "--help" | "-h" => print_help(),
        _ => {
            if args.len() < 2 {
                return Err("Usage: plshelp <library_name> \"<question>\"".into());
            }
            query_library(
                &conn,
                &args[0],
                &args[1],
                SearchMode::Hybrid,
                DEFAULT_TOP_K,
                DEFAULT_CONTEXT_WINDOW,
                false,
            )?;
        }
    }

    Ok(())
}

fn print_help() {
    println!("plshelp <command>");
    println!("  add <library_name> <source_url>");
    println!("  crawl <library_name> <source_url>");
    println!("  index <library_name> [--file /path/to/file]");
    println!("  refresh <library_name>");
    println!(
        "  query <library_name> \"<question>\" [--mode hybrid|vector|keyword] [--top-k N] [--context N]"
    );
    println!("  <library_name> \"<question>\"   # query alias");
    println!("  ask \"<question>\" [--libraries a,b,c] [--mode ...] [--top-k N] [--context N]");
    println!("  alias <library_name> <alias>");
    println!("  list");
    println!("  show <library_name>");
    println!("  remove <library_name>");
    println!("  open <chunk_id>");
    println!("  trace <library_name> \"<question>\" [--mode ...] [--top-k N] [--context N]");
}

fn app_root() -> PathBuf {
    PathBuf::from(BASE_PATH)
}

fn artifacts_root() -> PathBuf {
    app_root().join("artifacts")
}

fn compiled_dir(library_name: &str) -> PathBuf {
    artifacts_root().join(library_name)
}

fn now_epoch() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    secs.to_string()
}

fn human_time(epoch: &str) -> String {
    if let Ok(secs) = epoch.parse::<i64>() {
        if let Some(dt) = DateTime::<Utc>::from_timestamp(secs, 0) {
            return dt.format("%B %-d, %Y").to_string();
        }
    }
    epoch.to_string()
}

fn init_db(db_path: &Path) -> Result<Connection, Box<dyn Error>> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS libraries (
            library_name TEXT PRIMARY KEY,
            source_url TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_refreshed_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS library_aliases (
            alias TEXT PRIMARY KEY,
            library_name TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            library_name TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_page_order INTEGER NOT NULL,
            chunk_index_in_page INTEGER NOT NULL,
            global_chunk_index INTEGER NOT NULL,
            content TEXT NOT NULL,
            embedding BLOB NOT NULL,
            token_count INTEGER NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            library_name TEXT NOT NULL,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            message TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_chunks_library_name ON chunks(library_name);
        CREATE INDEX IF NOT EXISTS idx_chunks_library_page ON chunks(library_name, source_url, chunk_index_in_page);
        ",
    )?;
    Ok(conn)
}

fn start_job(conn: &Connection, library_name: &str, job_type: &str) -> Result<i64, Box<dyn Error>> {
    conn.execute(
        "INSERT INTO jobs (library_name, job_type, status, started_at) VALUES (?1, ?2, 'running', ?3)",
        params![library_name, job_type, now_epoch()],
    )?;
    Ok(conn.last_insert_rowid())
}

fn finish_job(
    conn: &Connection,
    job_id: i64,
    status: &str,
    message: &str,
) -> Result<(), Box<dyn Error>> {
    conn.execute(
        "UPDATE jobs SET status = ?1, ended_at = ?2, message = ?3 WHERE id = ?4",
        params![status, now_epoch(), message, job_id],
    )?;
    Ok(())
}

fn parse_query_flags(flags: &[String]) -> Result<(SearchMode, usize, usize), Box<dyn Error>> {
    let mut mode = SearchMode::Hybrid;
    let mut top_k = DEFAULT_TOP_K;
    let mut context = DEFAULT_CONTEXT_WINDOW;
    let mut i = 0usize;
    while i < flags.len() {
        match flags[i].as_str() {
            "--mode" if i + 1 < flags.len() => {
                mode = SearchMode::from_str(&flags[i + 1]);
                i += 2;
            }
            "--top-k" if i + 1 < flags.len() => {
                top_k = flags[i + 1].parse()?;
                i += 2;
            }
            "--context" if i + 1 < flags.len() => {
                context = flags[i + 1].parse()?;
                i += 2;
            }
            _ => i += 1,
        }
    }
    Ok((mode, top_k, context))
}

fn ask_flags(
    flags: &[String],
) -> Result<(SearchMode, usize, usize, Option<Vec<String>>), Box<dyn Error>> {
    let mut mode = SearchMode::Hybrid;
    let mut top_k = DEFAULT_TOP_K;
    let mut context = DEFAULT_CONTEXT_WINDOW;
    let mut libraries: Option<Vec<String>> = None;
    let mut i = 0usize;
    while i < flags.len() {
        match flags[i].as_str() {
            "--mode" if i + 1 < flags.len() => {
                mode = SearchMode::from_str(&flags[i + 1]);
                i += 2;
            }
            "--top-k" if i + 1 < flags.len() => {
                top_k = flags[i + 1].parse()?;
                i += 2;
            }
            "--context" if i + 1 < flags.len() => {
                context = flags[i + 1].parse()?;
                i += 2;
            }
            "--libraries" if i + 1 < flags.len() => {
                libraries = Some(
                    flags[i + 1]
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect(),
                );
                i += 2;
            }
            _ => i += 1,
        }
    }
    Ok((mode, top_k, context, libraries))
}

fn parse_index_file_flag(flags: &[String]) -> Option<String> {
    let mut i = 0usize;
    while i < flags.len() {
        if flags[i] == "--file" && i + 1 < flags.len() {
            return Some(flags[i + 1].clone());
        }
        i += 1;
    }
    None
}

fn resolve_library_name(conn: &Connection, input: &str) -> Result<String, Box<dyn Error>> {
    if let Ok(name) = conn.query_row(
        "SELECT library_name FROM libraries WHERE library_name = ?1",
        params![input],
        |row| row.get::<_, String>(0),
    ) {
        return Ok(name);
    }
    let name = conn.query_row(
        "SELECT library_name FROM library_aliases WHERE alias = ?1",
        params![input],
        |row| row.get::<_, String>(0),
    )?;
    Ok(name)
}

async fn add_library(
    conn: &Connection,
    library_name: &str,
    source_url: &str,
) -> Result<(), Box<dyn Error>> {
    let exists: i64 = conn.query_row(
        "SELECT COUNT(*) FROM libraries WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    if exists > 0 {
        return Err(format!("Library '{}' already exists.", library_name).into());
    }
    crawl_library(conn, library_name, source_url, "add-crawl").await?;
    index_library(conn, library_name, None, "add-index")?;
    Ok(())
}

async fn refresh_library(conn: &Connection, input_name: &str) -> Result<(), Box<dyn Error>> {
    let library_name = resolve_library_name(conn, input_name)?;
    let source_url: String = conn.query_row(
        "SELECT source_url FROM libraries WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    crawl_library(conn, &library_name, &source_url, "refresh-crawl").await?;
    index_library(conn, &library_name, None, "refresh-index")?;
    Ok(())
}

// ---- Crawl pipeline: restored to your original behavior ----
fn normalize_seed_url(seed_url: &str) -> Result<String, String> {
    let mut parsed =
        Url::parse(seed_url).map_err(|e| format!("Invalid seed URL '{}': {}", seed_url, e))?;
    let path = parsed.path();
    if !path.ends_with('/') {
        let normalized_path = if path.is_empty() {
            "/".to_string()
        } else {
            format!("{}/", path)
        };
        parsed.set_path(&normalized_path);
    }
    Ok(parsed.to_string())
}

fn whitelist_for_url(seed_url: &str) -> Result<Vec<CompactString>, String> {
    let parsed =
        Url::parse(seed_url).map_err(|e| format!("Invalid seed URL '{}': {}", seed_url, e))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| format!("Seed URL '{}' has no host", seed_url))?;
    let scheme_pattern = regex::escape(parsed.scheme());
    let authority = match parsed.port() {
        Some(port) => format!("{}:{}", host, port),
        None => host.to_string(),
    };
    let authority_pattern = regex::escape(&authority);
    let trimmed_path = parsed.path().trim_end_matches('/');
    let regex_pattern = if trimmed_path.is_empty() {
        format!(r"^{}://{}(/|$)", scheme_pattern, authority_pattern)
    } else {
        let path_pattern = regex::escape(trimmed_path);
        format!(
            r"^{}://{}{}(/|$)",
            scheme_pattern, authority_pattern, path_pattern
        )
    };
    Ok(vec![CompactString::new(regex_pattern)])
}

fn extract_content_html(html: &str) -> String {
    let document = Html::parse_document(html);
    let mut best_html: Option<String> = None;
    let mut best_text_len = 0usize;

    for selector in CONTENT_SELECTORS.iter() {
        for node in document.select(selector) {
            // Avoid allocating a full string only to estimate section size.
            let text_len: usize = node.text().map(|s| s.trim().len()).sum();
            if text_len > best_text_len {
                let selected_html = node.html();
                if !selected_html.trim().is_empty() {
                    best_text_len = text_len;
                    best_html = Some(selected_html);
                }
            }
        }
    }

    let mut cleaned = best_html.unwrap_or_else(|| html.to_string());
    let lower = cleaned.to_ascii_lowercase();
    if HTML_REGEX_HINTS.iter().any(|h| lower.contains(h)) {
        for re in HTML_CLEANUP_REGEXES.iter() {
            cleaned = re.replace_all(&cleaned, "").into_owned();
        }
    }
    cleaned
}

fn cleanup_markdown(markdown: &str) -> String {
    let mut cleaned = markdown.to_string();
    let lower = cleaned.to_ascii_lowercase();
    if MARKDOWN_HINTS.iter().any(|h| lower.contains(h)) {
        for re in MARKDOWN_LINE_REGEXES.iter() {
            cleaned = re.replace_all(&cleaned, "").into_owned();
        }
    }
    if cleaned.contains("\n\n\n") {
        cleaned = MULTI_NEWLINE_RE.replace_all(&cleaned, "\n\n").into_owned();
    }

    cleaned.trim().to_string()
}

async fn crawl_library(
    conn: &Connection,
    library_name: &str,
    source_url: &str,
    job_type: &str,
) -> Result<(), Box<dyn Error>> {
    let job_id = start_job(conn, library_name, job_type)?;

    let run_result = async {
        let normalized_seed_url =
            normalize_seed_url(source_url).map_err(|e| format!("URL error: {e}"))?;
        let whitelist =
            whitelist_for_url(&normalized_seed_url).map_err(|e| format!("Whitelist error: {e}"))?;

        let mut config = Configuration::new();
        config
            .with_limit(5_000)
            .with_depth(25)
            .with_subdomains(false)
            .with_tld(false)
            .with_user_agent(Some("DocumentationScraper/1.0"))
            .with_whitelist_url(Some(whitelist));

        let mut website = Website::new(&normalized_seed_url)
            .with_config(config)
            .build()
            .map_err(|e| format!("Failed to build website: {e}"))?;

        let done = Arc::new(AtomicBool::new(false));
        let stage = Arc::new(Mutex::new(String::from("Downloading")));
        let done_for_spinner = Arc::clone(&done);
        let stage_for_spinner = Arc::clone(&stage);
        let spinner_handle = tokio::spawn(async move {
            let frames = ["|", "/", "-", "\\"];
            let mut idx = 0usize;
            let mut last_len = 0usize;
            while !done_for_spinner.load(Ordering::Relaxed) {
                let current_stage = stage_for_spinner
                    .lock()
                    .map(|s| s.clone())
                    .unwrap_or_else(|_| String::from("Working"));
                let line = format!("{}... {}", current_stage, frames[idx % frames.len()]);
                let padding = " ".repeat(last_len.saturating_sub(line.len()));
                print!("\r{}{}", line, padding);
                let _ = stdout().flush();
                last_len = line.len();
                idx += 1;
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            }
        });

        if let Ok(mut s) = stage.lock() {
            *s = String::from("Downloading files");
        }
        website.scrape().await;

        if let Ok(mut s) = stage.lock() {
            *s = String::from("Converting files");
        }
        let pages = match website.get_pages() {
            Some(p) => p,
            None => {
                done.store(true, Ordering::Relaxed);
                let _ = spinner_handle.await;
                print!("\r                    \r");
                let _ = stdout().flush();
                return Err("No pages collected".into());
            }
        };

        if let Ok(mut s) = stage.lock() {
            *s = String::from("Writing files");
        }
        let page_htmls: Vec<String> = pages.iter().map(|p| p.get_html()).collect();
        // Indexed parallel iterators preserve order on collect.
        let converted: Vec<String> = page_htmls
            .into_par_iter()
            .map(|html| {
                let extracted_html = extract_content_html(&html);
                let markdown = cleanup_markdown(&html2md::parse_html(&extracted_html));
                markdown
            })
            .collect();
        let mut compiled = converted.join("\n\n");
        if !compiled.is_empty() {
            compiled.push_str("\n\n");
        }

        let out_dir = compiled_dir(library_name);
        fs::create_dir_all(&out_dir)?;
        fs::write(out_dir.join("docs.txt"), &compiled)?;
        // Keep docs.md as a direct copy for now.
        fs::write(out_dir.join("docs.md"), &compiled)?;

        if let Ok(mut s) = stage.lock() {
            *s = String::from("Finalizing");
        }
        done.store(true, Ordering::Relaxed);
        let _ = spinner_handle.await;
        print!("\r                    \r");
        let _ = stdout().flush();

        let now = now_epoch();
        conn.execute(
            "INSERT OR REPLACE INTO libraries (library_name, source_url, created_at, updated_at, last_refreshed_at)
             VALUES (
               ?1, ?2,
               COALESCE((SELECT created_at FROM libraries WHERE library_name = ?1), ?3),
               ?3, ?3
             )",
            params![library_name, source_url, now],
        )?;

        Ok::<String, Box<dyn Error>>(format!("Crawled {} pages.", pages.len()))
    }
    .await;

    match run_result {
        Ok(msg) => {
            finish_job(conn, job_id, "success", &msg)?;
            Ok(())
        }
        Err(err) => {
            let msg = format!("{err}");
            let _ = finish_job(conn, job_id, "failed", &msg);
            Err(err)
        }
    }
}

fn merge_qa_pairs(sentences: Vec<String>) -> Vec<String> {
    let mut merged = Vec::new();
    let mut i = 0usize;
    while i < sentences.len() {
        if i + 1 < sentences.len() && sentences[i].trim_end().ends_with('?') {
            merged.push(format!("{} {}", sentences[i], sentences[i + 1]));
            i += 2;
        } else {
            merged.push(sentences[i].clone());
            i += 1;
        }
    }
    merged
}

fn embedding_to_bytes(embedding: &[f32]) -> Vec<u8> {
    embedding.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn bytes_to_embedding(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut a_norm = 0.0f32;
    let mut b_norm = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        a_norm += x * x;
        b_norm += y * y;
    }
    if a_norm == 0.0 || b_norm == 0.0 {
        return 0.0;
    }
    dot / (a_norm.sqrt() * b_norm.sqrt())
}

fn keyword_overlap_score(query: &str, text: &str) -> f32 {
    let query_terms: HashSet<String> = query
        .split(|c: char| !c.is_alphanumeric())
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    if query_terms.is_empty() {
        return 0.0;
    }
    let text_terms: HashSet<String> = text
        .split(|c: char| !c.is_alphanumeric())
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    let hits = query_terms.intersection(&text_terms).count() as f32;
    hits / query_terms.len() as f32
}

fn index_library(
    conn: &Connection,
    input_name: &str,
    custom_file: Option<&str>,
    job_type: &str,
) -> Result<(), Box<dyn Error>> {
    let resolved_name = resolve_library_name(conn, input_name);
    let (library_name, source_url) = match resolved_name {
        Ok(name) => {
            let source_url: String = conn.query_row(
                "SELECT source_url FROM libraries WHERE library_name = ?1",
                params![name],
                |row| row.get(0),
            )?;
            (name, source_url)
        }
        Err(_) => {
            let file_path = custom_file.ok_or_else(|| {
                format!(
                    "Library '{}' not found. Use add/crawl first, or pass --file.",
                    input_name
                )
            })?;
            let canonical_path = PathBuf::from(file_path).canonicalize()?;
            let source_url = format!("file://{}", canonical_path.display());
            let now = now_epoch();
            conn.execute(
                "INSERT INTO libraries (library_name, source_url, created_at, updated_at, last_refreshed_at)
                 VALUES (?1, ?2, ?3, ?3, ?3)",
                params![input_name, source_url, now],
            )?;
            (input_name.to_string(), source_url)
        }
    };

    let job_id = start_job(conn, &library_name, job_type)?;
    let result = (|| -> Result<String, Box<dyn Error>> {
        let source_text = if let Some(file_path) = custom_file {
            fs::read_to_string(file_path)?
        } else {
            let out_dir = compiled_dir(&library_name);
            let txt = out_dir.join("docs.txt");
            let md = out_dir.join("docs.md");
            if txt.exists() {
                fs::read_to_string(txt)?
            } else if md.exists() {
                fs::read_to_string(md)?
            } else {
                return Err(format!(
                    "No compiled docs found for '{}'. Run crawl/add first.",
                    library_name
                )
                .into());
            }
        };

        let mut chunks: Vec<String> = segment("en", &source_text)
            .into_iter()
            .map(|s| s.to_owned())
            .collect();
        chunks = merge_qa_pairs(chunks);
        chunks.retain(|c| !c.trim().is_empty());
        if chunks.is_empty() {
            return Err("No chunks generated from input.".into());
        }

        let mut model = TextEmbedding::try_new(
            InitOptions::new(DEFAULT_EMBEDDING_MODEL).with_show_download_progress(true),
        )?;
        let embeddings = model.embed(&chunks, None)?;
        if embeddings.len() != chunks.len() {
            return Err("Embedding count mismatch.".into());
        }

        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM chunks WHERE library_name = ?1",
            params![library_name],
        )?;

        let now = now_epoch();
        for (i, chunk) in chunks.iter().enumerate() {
            let token_count = chunk.split_whitespace().count() as i64;
            tx.execute(
                "INSERT INTO chunks (
                    library_name, source_url, source_page_order, chunk_index_in_page,
                    global_chunk_index, content, embedding, token_count, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    library_name,
                    source_url,
                    0i64,
                    i as i64,
                    i as i64,
                    chunk,
                    embedding_to_bytes(&embeddings[i]),
                    token_count,
                    now
                ],
            )?;
        }
        tx.commit()?;
        Ok(format!("Indexed {} chunks.", chunks.len()))
    })();

    match result {
        Ok(msg) => {
            finish_job(conn, job_id, "success", &msg)?;
            Ok(())
        }
        Err(err) => {
            let msg = format!("{err}");
            let _ = finish_job(conn, job_id, "failed", &msg);
            Err(err)
        }
    }
}

fn load_chunks_for_library(
    conn: &Connection,
    library_name: &str,
) -> Result<Vec<ChunkRecord>, Box<dyn Error>> {
    let mut stmt = conn.prepare(
        "SELECT id, library_name, source_url, source_page_order, chunk_index_in_page,
                global_chunk_index, content, embedding
         FROM chunks
         WHERE library_name = ?1",
    )?;
    let rows = stmt.query_map(params![library_name], |row| {
        let bytes: Vec<u8> = row.get(7)?;
        Ok(ChunkRecord {
            id: row.get(0)?,
            library_name: row.get(1)?,
            source_url: row.get(2)?,
            source_page_order: row.get(3)?,
            chunk_index_in_page: row.get(4)?,
            global_chunk_index: row.get(5)?,
            content: row.get(6)?,
            embedding: bytes_to_embedding(&bytes),
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn score_chunks(
    chunks: &[ChunkRecord],
    query: &str,
    mode: SearchMode,
    query_embedding: Option<&[f32]>,
) -> Vec<ScoredChunk> {
    let mut scored = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let vector_score = match (mode, query_embedding) {
            (SearchMode::Keyword, _) => 0.0,
            (_, Some(embed)) => cosine_similarity(embed, &chunk.embedding),
            _ => 0.0,
        };
        let keyword_score = match mode {
            SearchMode::Vector => 0.0,
            _ => keyword_overlap_score(query, &chunk.content),
        };
        let final_score = match mode {
            SearchMode::Vector => vector_score,
            SearchMode::Keyword => keyword_score,
            SearchMode::Hybrid => 0.85 * vector_score + 0.15 * keyword_score,
        };
        scored.push(ScoredChunk {
            chunk: chunk.clone(),
            vector_score,
            keyword_score,
            final_score,
        });
    }
    scored.sort_by(|a, b| {
        b.final_score
            .partial_cmp(&a.final_score)
            .unwrap_or(CmpOrdering::Equal)
    });
    scored
}

fn neighbors(
    conn: &Connection,
    library_name: &str,
    source_url: &str,
    chunk_index_in_page: i64,
    context: usize,
) -> Result<Vec<ChunkRecord>, Box<dyn Error>> {
    if context == 0 {
        return Ok(Vec::new());
    }
    let low = chunk_index_in_page - context as i64;
    let high = chunk_index_in_page + context as i64;
    let mut stmt = conn.prepare(
        "SELECT id, library_name, source_url, source_page_order, chunk_index_in_page,
                global_chunk_index, content, embedding
         FROM chunks
         WHERE library_name = ?1 AND source_url = ?2 AND chunk_index_in_page BETWEEN ?3 AND ?4
         ORDER BY chunk_index_in_page ASC",
    )?;
    let rows = stmt.query_map(params![library_name, source_url, low, high], |row| {
        let bytes: Vec<u8> = row.get(7)?;
        Ok(ChunkRecord {
            id: row.get(0)?,
            library_name: row.get(1)?,
            source_url: row.get(2)?,
            source_page_order: row.get(3)?,
            chunk_index_in_page: row.get(4)?,
            global_chunk_index: row.get(5)?,
            content: row.get(6)?,
            embedding: bytes_to_embedding(&bytes),
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn embed_query(mode: SearchMode, question: &str) -> Result<Option<Vec<f32>>, Box<dyn Error>> {
    if let SearchMode::Keyword = mode {
        return Ok(None);
    }
    let mut model = TextEmbedding::try_new(
        InitOptions::new(DEFAULT_EMBEDDING_MODEL).with_show_download_progress(false),
    )?;
    let prompt = format!("Represent this sentence for searching relevant passages: {question}");
    let embedding = model.embed([prompt], None)?;
    Ok(embedding.first().cloned())
}

fn query_library(
    conn: &Connection,
    input_name: &str,
    question: &str,
    mode: SearchMode,
    top_k: usize,
    context: usize,
    trace: bool,
) -> Result<(), Box<dyn Error>> {
    let library_name = resolve_library_name(conn, input_name)?;
    let chunks = load_chunks_for_library(conn, &library_name)?;
    if chunks.is_empty() {
        println!("No chunks indexed for '{}'.", library_name);
        return Ok(());
    }
    let query_embedding = embed_query(mode, question)?;
    let mut scored = score_chunks(&chunks, question, mode, query_embedding.as_deref());
    scored.truncate(top_k);

    for (rank, hit) in scored.iter().enumerate() {
        println!("{}. [{}] {}", rank + 1, hit.chunk.id, hit.chunk.source_url);
        if trace {
            println!(
                "   scores: final={:.4} vector={:.4} keyword={:.4}",
                hit.final_score, hit.vector_score, hit.keyword_score
            );
            println!(
                "   location: page_order={} chunk_in_page={} global_index={}",
                hit.chunk.source_page_order,
                hit.chunk.chunk_index_in_page,
                hit.chunk.global_chunk_index
            );
            println!("   library: {}", hit.chunk.library_name);
        }
        println!("{}", hit.chunk.content);
        if context > 0 {
            let around = neighbors(
                conn,
                &library_name,
                &hit.chunk.source_url,
                hit.chunk.chunk_index_in_page,
                context,
            )?;
            if !around.is_empty() {
                println!("--- context ---");
                for c in around {
                    if c.id != hit.chunk.id {
                        println!("[{}] {}", c.id, c.content);
                    }
                }
            }
        }
        println!();
    }
    Ok(())
}

fn ask_libraries(
    conn: &Connection,
    question: &str,
    flags: &[String],
) -> Result<(), Box<dyn Error>> {
    let (mode, top_k, context, filter) = ask_flags(flags)?;
    let libraries = if let Some(libs) = filter {
        let mut out = Vec::new();
        for lib in libs {
            out.push(resolve_library_name(conn, &lib)?);
        }
        out
    } else {
        let mut stmt =
            conn.prepare("SELECT library_name FROM libraries ORDER BY library_name ASC")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        out
    };
    if libraries.is_empty() {
        println!("No libraries indexed.");
        return Ok(());
    }

    let query_embedding = embed_query(mode, question)?;
    let mut combined = Vec::new();
    for lib in libraries {
        let chunks = load_chunks_for_library(conn, &lib)?;
        if chunks.is_empty() {
            continue;
        }
        let mut scored = score_chunks(&chunks, question, mode, query_embedding.as_deref());
        scored.truncate(top_k);
        combined.extend(scored);
    }
    combined.sort_by(|a, b| {
        b.final_score
            .partial_cmp(&a.final_score)
            .unwrap_or(CmpOrdering::Equal)
    });
    combined.truncate(top_k);

    for (rank, hit) in combined.iter().enumerate() {
        println!(
            "{}. [{}] {} ({})",
            rank + 1,
            hit.chunk.id,
            hit.chunk.source_url,
            hit.chunk.library_name
        );
        println!("{}", hit.chunk.content);
        if context > 0 {
            let around = neighbors(
                conn,
                &hit.chunk.library_name,
                &hit.chunk.source_url,
                hit.chunk.chunk_index_in_page,
                context,
            )?;
            if !around.is_empty() {
                println!("--- context ---");
                for c in around {
                    if c.id != hit.chunk.id {
                        println!("[{}] {}", c.id, c.content);
                    }
                }
            }
        }
        println!();
    }
    Ok(())
}

fn add_alias(conn: &Connection, input_name: &str, alias: &str) -> Result<(), Box<dyn Error>> {
    let library_name = resolve_library_name(conn, input_name)?;
    let collision: i64 = conn.query_row(
        "SELECT COUNT(*) FROM libraries WHERE library_name = ?1",
        params![alias],
        |row| row.get(0),
    )?;
    if collision > 0 {
        return Err(format!("Alias '{}' conflicts with an existing library name.", alias).into());
    }
    conn.execute(
        "INSERT OR REPLACE INTO library_aliases (alias, library_name, created_at) VALUES (?1, ?2, ?3)",
        params![alias, library_name, now_epoch()],
    )?;
    Ok(())
}

fn list_libraries(conn: &Connection) -> Result<(), Box<dyn Error>> {
    let mut stmt = conn.prepare(
        "SELECT library_name, source_url, last_refreshed_at FROM libraries ORDER BY library_name ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    for row in rows {
        let (library_name, source_url, refreshed) = row?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM chunks WHERE library_name = ?1",
            params![library_name],
            |r| r.get(0),
        )?;
        println!("{library_name}");
        println!("  source: {source_url}");
        println!("  chunks: {count}");
        println!("  last refreshed: {}", human_time(&refreshed));
    }
    Ok(())
}

fn show_library(conn: &Connection, input_name: &str) -> Result<(), Box<dyn Error>> {
    let library_name = resolve_library_name(conn, input_name)?;
    let (source_url, refreshed): (String, String) = conn.query_row(
        "SELECT source_url, last_refreshed_at FROM libraries WHERE library_name = ?1",
        params![library_name],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM chunks WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    println!("library_name: {library_name}");
    println!("source_url: {source_url}");
    println!("chunk_count: {count}");
    println!("last_refreshed_at: {}", human_time(&refreshed));
    let mut alias_stmt = conn
        .prepare("SELECT alias FROM library_aliases WHERE library_name = ?1 ORDER BY alias ASC")?;
    let alias_rows = alias_stmt.query_map(params![library_name], |row| row.get::<_, String>(0))?;
    let mut aliases = Vec::new();
    for row in alias_rows {
        aliases.push(row?);
    }
    if !aliases.is_empty() {
        println!("aliases: {}", aliases.join(", "));
    }
    Ok(())
}

fn remove_library(conn: &Connection, input_name: &str) -> Result<(), Box<dyn Error>> {
    let library_name = resolve_library_name(conn, input_name)?;
    conn.execute(
        "DELETE FROM library_aliases WHERE library_name = ?1",
        params![library_name],
    )?;
    conn.execute(
        "DELETE FROM chunks WHERE library_name = ?1",
        params![library_name],
    )?;
    conn.execute(
        "DELETE FROM jobs WHERE library_name = ?1",
        params![library_name],
    )?;
    conn.execute(
        "DELETE FROM libraries WHERE library_name = ?1",
        params![library_name],
    )?;
    let dir = compiled_dir(&library_name);
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    Ok(())
}

fn open_chunk(conn: &Connection, chunk_id: i64) -> Result<(), Box<dyn Error>> {
    let (library_name, source_url, content): (String, String, String) = conn.query_row(
        "SELECT library_name, source_url, content FROM chunks WHERE id = ?1",
        params![chunk_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    println!("chunk_id: {chunk_id}");
    println!("library_name: {library_name}");
    println!("source_url: {source_url}");
    println!();
    println!("{content}");
    Ok(())
}
