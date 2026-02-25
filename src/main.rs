use chrono::{DateTime, Utc};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use rayon::prelude::*;
use regex::Regex;
use rusqlite::{Connection, OptionalExtension, params};
use scraper::{Html, Selector};
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

    let conn = init_db(&app_root().join("plshelp.db"))?;
    let command = args[0].as_str();

    match command {
        "add" => {
            if args.len() < 3 {
                return Err(
                    "Usage: plshelp add <library_name> <source_url> [--include-artifacts[=/path]]"
                        .into(),
                );
            }
            let include_artifacts = parse_include_artifacts_flag(&args[3..], &args[1]);
            add_library(&conn, &args[1], &args[2], include_artifacts).await?;
            println!("Done.");
        }
        "crawl" => {
            if args.len() < 3 {
                return Err(
                    "Usage: plshelp crawl <library_name> <source_url> [--include-artifacts[=/path]]"
                        .into(),
                );
            }
            let include_artifacts = parse_include_artifacts_flag(&args[3..], &args[1]);
            crawl_library(&conn, &args[1], &args[2], "crawl", include_artifacts).await?;
            println!("Done.");
        }
        "index" => {
            if args.len() < 2 {
                return Err("Usage: plshelp index <library_name> [--file /path/to/file]".into());
            }
            let file = parse_index_file_flag(&args[2..]);
            index_library(&conn, &args[1], file.as_deref())?;
            println!("Done.");
        }
        "chunk" => {
            if args.len() < 2 {
                return Err("Usage: plshelp chunk <library_name> [--file /path/to/file]".into());
            }
            let file = parse_index_file_flag(&args[2..]);
            chunk_targets(&conn, &args[1], file.as_deref(), "chunk")?;
            println!("Done.");
        }
        "embed" => {
            if args.len() < 2 {
                return Err("Usage: plshelp embed <library_name>".into());
            }
            embed_library(&conn, &args[1], "embed")?;
            println!("Done.");
        }
        "refresh" => {
            refresh_stats(&conn, &args[1..])?;
            println!("Done.");
        }
        "merge" => {
            if args.len() < 4 {
                return Err("Usage: plshelp merge <new_library_name> <library1> <library2> [library3 ...] [--replace] [--include-artifacts[=/path]]".into());
            }
            let (members, replace, include_artifacts) = parse_merge_args(&args[2..], &args[1])?;
            merge_libraries(
                &conn,
                &args[1],
                &members,
                replace,
                include_artifacts.as_deref(),
            )?;
            println!("Done.");
        }
        "export" => {
            if args.len() < 2 {
                return Err("Usage: plshelp export <library_name> [path]".into());
            }
            let output_dir = if args.len() >= 3 {
                Some(PathBuf::from(args[2].clone()))
            } else {
                None
            };
            export_library(&conn, &args[1], output_dir.as_deref())?;
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
    println!("  add <library_name> <source_url> [--include-artifacts[=/path]]");
    println!("  crawl <library_name> <source_url> [--include-artifacts[=/path]]");
    println!("  index <library_name> [--file /path/to/file]");
    println!("  chunk <library_name> [--file /path/to/file]");
    println!("  embed <library_name>");
    println!("  refresh [library_name ...]   # recompute/backfill stats; no crawl");
    println!(
        "  merge <new_library_name> <library1> <library2> [library3 ...] [--replace] [--include-artifacts[=/path]]"
    );
    println!("  export <library_name> [path]");
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
            last_refreshed_at TEXT NOT NULL,
            content_size_chars INTEGER NOT NULL DEFAULT 0,
            page_count INTEGER NOT NULL DEFAULT 0,
            chunk_count INTEGER NOT NULL DEFAULT 0,
            embedded_chunk_count INTEGER NOT NULL DEFAULT 0,
            empty_page_count INTEGER NOT NULL DEFAULT 0,
            min_chunks_per_page INTEGER NOT NULL DEFAULT 0,
            max_chunks_per_page INTEGER NOT NULL DEFAULT 0
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
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            library_name TEXT NOT NULL,
            page_order INTEGER NOT NULL,
            source_url TEXT NOT NULL,
            content TEXT NOT NULL,
            content_size_chars INTEGER NOT NULL,
            crawled_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS library_groups (
            group_name TEXT NOT NULL,
            member_library_name TEXT NOT NULL,
            member_order INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (group_name, member_library_name)
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
        CREATE TABLE IF NOT EXISTS library_texts (
            library_name TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_chunks_library_name ON chunks(library_name);
        CREATE INDEX IF NOT EXISTS idx_chunks_library_page ON chunks(library_name, source_url, chunk_index_in_page);
        CREATE INDEX IF NOT EXISTS idx_pages_library_name ON pages(library_name);
        CREATE INDEX IF NOT EXISTS idx_pages_library_order ON pages(library_name, page_order);
        CREATE INDEX IF NOT EXISTS idx_library_groups_name ON library_groups(group_name);
        ",
    )?;
    run_db_migrations(&conn)?;
    Ok(conn)
}

fn run_db_migrations(conn: &Connection) -> Result<(), Box<dyn Error>> {
    let mut has_page_size_bytes = false;
    let mut has_page_size_chars = false;
    let mut library_columns = HashSet::new();

    let mut stmt = conn.prepare("PRAGMA table_info(pages)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for row in rows {
        let col = row?;
        if col == "content_size_bytes" {
            has_page_size_bytes = true;
        }
        if col == "content_size_chars" {
            has_page_size_chars = true;
        }
    }
    if has_page_size_bytes && !has_page_size_chars {
        conn.execute_batch(
            "ALTER TABLE pages RENAME COLUMN content_size_bytes TO content_size_chars;",
        )?;
    }

    let mut stmt = conn.prepare("PRAGMA table_info(libraries)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for row in rows {
        library_columns.insert(row?);
    }
    if !library_columns.contains("content_size_chars") {
        conn.execute_batch(
            "ALTER TABLE libraries ADD COLUMN content_size_chars INTEGER NOT NULL DEFAULT 0;",
        )?;
    }
    if !library_columns.contains("page_count") {
        conn.execute_batch(
            "ALTER TABLE libraries ADD COLUMN page_count INTEGER NOT NULL DEFAULT 0;",
        )?;
    }
    if !library_columns.contains("chunk_count") {
        conn.execute_batch(
            "ALTER TABLE libraries ADD COLUMN chunk_count INTEGER NOT NULL DEFAULT 0;",
        )?;
    }
    if !library_columns.contains("embedded_chunk_count") {
        conn.execute_batch(
            "ALTER TABLE libraries ADD COLUMN embedded_chunk_count INTEGER NOT NULL DEFAULT 0;",
        )?;
    }
    if !library_columns.contains("empty_page_count") {
        conn.execute_batch(
            "ALTER TABLE libraries ADD COLUMN empty_page_count INTEGER NOT NULL DEFAULT 0;",
        )?;
    }
    if !library_columns.contains("min_chunks_per_page") {
        conn.execute_batch(
            "ALTER TABLE libraries ADD COLUMN min_chunks_per_page INTEGER NOT NULL DEFAULT 0;",
        )?;
    }
    if !library_columns.contains("max_chunks_per_page") {
        conn.execute_batch(
            "ALTER TABLE libraries ADD COLUMN max_chunks_per_page INTEGER NOT NULL DEFAULT 0;",
        )?;
    }

    Ok(())
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

fn parse_include_artifacts_flag(flags: &[String], library_name: &str) -> Option<PathBuf> {
    let mut artifacts: Option<PathBuf> = None;
    let mut i = 0usize;
    while i < flags.len() {
        if flags[i] == "--include-artifacts" {
            if i + 1 < flags.len() && !flags[i + 1].starts_with("--") {
                artifacts = Some(PathBuf::from(flags[i + 1].clone()));
                i += 2;
            } else {
                artifacts = Some(compiled_dir(library_name));
                i += 1;
            }
            continue;
        }
        if let Some(raw_path) = flags[i].strip_prefix("--include-artifacts=") {
            if raw_path.is_empty() {
                artifacts = Some(compiled_dir(library_name));
            } else {
                artifacts = Some(PathBuf::from(raw_path));
            }
            i += 1;
            continue;
        }
        i += 1;
    }
    artifacts
}

fn parse_merge_args(
    args: &[String],
    group_name: &str,
) -> Result<(Vec<String>, bool, Option<PathBuf>), Box<dyn Error>> {
    let mut members = Vec::new();
    let mut replace = false;
    let mut include_artifacts: Option<PathBuf> = None;
    let mut i = 0usize;
    while i < args.len() {
        let token = &args[i];
        if token == "--replace" {
            replace = true;
            i += 1;
            continue;
        }
        if token == "--include-artifacts" {
            if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                include_artifacts = Some(PathBuf::from(args[i + 1].clone()));
                i += 2;
            } else {
                include_artifacts = Some(compiled_dir(group_name));
                i += 1;
            }
            continue;
        }
        if let Some(raw_path) = token.strip_prefix("--include-artifacts=") {
            include_artifacts = if raw_path.is_empty() {
                Some(compiled_dir(group_name))
            } else {
                Some(PathBuf::from(raw_path))
            };
            i += 1;
            continue;
        }
        members.push(token.clone());
        i += 1;
    }
    if members.len() < 2 {
        return Err("merge requires at least two source libraries.".into());
    }
    Ok((members, replace, include_artifacts))
}

fn write_artifacts(output_dir: &Path, content: &str) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(output_dir)?;
    fs::write(output_dir.join("docs.txt"), content)?;
    fs::write(output_dir.join("docs.md"), content)?;
    Ok(())
}

fn export_library(
    conn: &Connection,
    input_name: &str,
    output_dir: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    let members = resolve_target_libraries(conn, input_name)?;
    let output_dir = output_dir
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| compiled_dir(input_name));
    let mut compiled_parts = Vec::new();
    for member in &members {
        compiled_parts.push(compiled_text_for_library(conn, member)?);
    }
    let mut compiled = compiled_parts.join("\n\n");
    if !compiled.is_empty() {
        compiled.push_str("\n\n");
    }

    write_artifacts(&output_dir, &compiled)?;
    Ok(())
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

fn group_members(conn: &Connection, group_name: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut stmt = conn.prepare(
        "SELECT member_library_name
         FROM library_groups
         WHERE group_name = ?1
         ORDER BY member_order ASC",
    )?;
    let rows = stmt.query_map(params![group_name], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn resolve_target_libraries(conn: &Connection, input: &str) -> Result<Vec<String>, Box<dyn Error>> {
    if let Ok(name) = resolve_library_name(conn, input) {
        return Ok(vec![name]);
    }
    let members = group_members(conn, input)?;
    if members.is_empty() {
        return Err(format!("Unknown library or merged group '{}'.", input).into());
    }
    Ok(members)
}

fn compiled_text_for_library(
    conn: &Connection,
    input_name: &str,
) -> Result<String, Box<dyn Error>> {
    let library_name = resolve_library_name(conn, input_name)?;
    let mut stmt = conn.prepare(
        "SELECT content
         FROM pages
         WHERE library_name = ?1
         ORDER BY page_order ASC",
    )?;
    let rows = stmt.query_map(params![library_name], |row| row.get::<_, String>(0))?;
    let mut page_contents = Vec::new();
    for row in rows {
        page_contents.push(row?);
    }
    if !page_contents.is_empty() {
        let mut text = page_contents.join("\n\n");
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        return Ok(text);
    }
    let fallback: Option<String> = conn
        .query_row(
            "SELECT content FROM library_texts WHERE library_name = ?1",
            params![library_name],
            |row| row.get(0),
        )
        .optional()?;
    fallback.ok_or_else(|| {
        format!(
            "No crawled content found for '{}'. Run crawl/add first.",
            input_name
        )
        .into()
    })
}

fn backfill_pages_from_chunks(
    conn: &Connection,
    library_name: &str,
    now: &str,
) -> Result<(), Box<dyn Error>> {
    let page_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM pages WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    if page_count > 0 {
        return Ok(());
    }

    let chunk_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM chunks WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    if chunk_count == 0 {
        return Ok(());
    }

    let mut page_stmt = conn.prepare(
        "SELECT DISTINCT source_page_order, source_url
         FROM chunks
         WHERE library_name = ?1
         ORDER BY source_page_order ASC, source_url ASC",
    )?;
    let page_rows = page_stmt.query_map(params![library_name], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut page_pairs = Vec::new();
    for row in page_rows {
        page_pairs.push(row?);
    }

    let tx = conn.unchecked_transaction()?;
    for (page_order, source_url) in page_pairs {
        let mut chunk_stmt = tx.prepare(
            "SELECT content
             FROM chunks
             WHERE library_name = ?1 AND source_page_order = ?2 AND source_url = ?3
             ORDER BY chunk_index_in_page ASC",
        )?;
        let chunk_rows = chunk_stmt
            .query_map(params![library_name, page_order, source_url], |r| {
                r.get::<_, String>(0)
            })?;
        let mut parts = Vec::new();
        for c in chunk_rows {
            parts.push(c?);
        }
        let content = parts.join("\n\n");
        tx.execute(
            "INSERT INTO pages (
                library_name, page_order, source_url, content, content_size_chars, crawled_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                library_name,
                page_order,
                source_url,
                content,
                content.chars().count() as i64,
                now
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

fn backfill_library_text(
    conn: &Connection,
    library_name: &str,
    now: &str,
) -> Result<(), Box<dyn Error>> {
    let exists: i64 = conn.query_row(
        "SELECT COUNT(*) FROM library_texts WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    if exists > 0 {
        return Ok(());
    }

    let mut stmt = conn.prepare(
        "SELECT content
         FROM pages
         WHERE library_name = ?1
         ORDER BY page_order ASC",
    )?;
    let rows = stmt.query_map(params![library_name], |row| row.get::<_, String>(0))?;
    let mut page_contents = Vec::new();
    for row in rows {
        page_contents.push(row?);
    }
    if page_contents.is_empty() {
        return Ok(());
    }

    let mut compiled = page_contents.join("\n\n");
    if !compiled.is_empty() {
        compiled.push_str("\n\n");
    }
    conn.execute(
        "INSERT OR REPLACE INTO library_texts (library_name, content, updated_at) VALUES (?1, ?2, ?3)",
        params![library_name, compiled, now],
    )?;
    Ok(())
}

fn compute_library_chars(conn: &Connection, library_name: &str) -> Result<i64, Box<dyn Error>> {
    let from_pages: i64 = conn.query_row(
        "SELECT COALESCE(SUM(content_size_chars), 0) FROM pages WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    if from_pages > 0 {
        return Ok(from_pages);
    }

    let from_text: Option<i64> = conn
        .query_row(
            "SELECT LENGTH(content) FROM library_texts WHERE library_name = ?1",
            params![library_name],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(v) = from_text {
        return Ok(v);
    }

    let from_chunks: i64 = conn.query_row(
        "SELECT COALESCE(SUM(LENGTH(content)), 0) FROM chunks WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    Ok(from_chunks)
}

fn compute_page_chunk_rollups(
    conn: &Connection,
    library_name: &str,
) -> Result<(i64, i64, i64, i64, i64, i64), Box<dyn Error>> {
    let page_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM pages WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    let chunk_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM chunks WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    let embedded_chunk_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM chunks WHERE library_name = ?1 AND LENGTH(embedding) > 0",
        params![library_name],
        |row| row.get(0),
    )?;

    if page_count == 0 {
        return Ok((0, chunk_count, embedded_chunk_count, 0, 0, 0));
    }

    let mut stmt = conn.prepare(
        "SELECT p.page_order, COUNT(c.id) AS chunk_count
         FROM pages p
         LEFT JOIN chunks c
           ON c.library_name = p.library_name
          AND c.source_page_order = p.page_order
         WHERE p.library_name = ?1
         GROUP BY p.page_order
         ORDER BY p.page_order ASC",
    )?;
    let rows = stmt.query_map(params![library_name], |row| row.get::<_, i64>(1))?;
    let mut min_chunks = i64::MAX;
    let mut max_chunks = i64::MIN;
    let mut empty_pages = 0i64;
    let mut seen = 0i64;
    for row in rows {
        let count = row?;
        if count == 0 {
            empty_pages += 1;
        }
        if count < min_chunks {
            min_chunks = count;
        }
        if count > max_chunks {
            max_chunks = count;
        }
        seen += 1;
    }
    if seen == 0 {
        min_chunks = 0;
        max_chunks = 0;
    }
    Ok((
        page_count,
        chunk_count,
        embedded_chunk_count,
        empty_pages,
        min_chunks,
        max_chunks,
    ))
}

fn update_library_rollups(conn: &Connection, library_name: &str) -> Result<(), Box<dyn Error>> {
    let content_size_chars = compute_library_chars(conn, library_name)?;
    let (
        page_count,
        chunk_count,
        embedded_chunk_count,
        empty_page_count,
        min_chunks_per_page,
        max_chunks_per_page,
    ) = compute_page_chunk_rollups(conn, library_name)?;
    conn.execute(
        "UPDATE libraries
         SET content_size_chars = ?1,
             page_count = ?2,
             chunk_count = ?3,
             embedded_chunk_count = ?4,
             empty_page_count = ?5,
             min_chunks_per_page = ?6,
             max_chunks_per_page = ?7,
             updated_at = ?8
         WHERE library_name = ?9",
        params![
            content_size_chars,
            page_count,
            chunk_count,
            embedded_chunk_count,
            empty_page_count,
            min_chunks_per_page,
            max_chunks_per_page,
            now_epoch(),
            library_name
        ],
    )?;
    Ok(())
}

fn merge_libraries(
    conn: &Connection,
    group_name: &str,
    member_inputs: &[String],
    replace: bool,
    include_artifacts: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    if resolve_library_name(conn, group_name).is_ok() {
        return Err(format!("'{}' already exists as a library/alias.", group_name).into());
    }

    let mut resolved_members = Vec::new();
    let mut seen = HashSet::new();
    for input in member_inputs {
        for name in resolve_target_libraries(conn, input)? {
            if seen.insert(name.clone()) {
                resolved_members.push(name);
            }
        }
    }
    if resolved_members.len() < 2 {
        return Err("Merged group must contain at least two distinct libraries.".into());
    }

    let exists: i64 = conn.query_row(
        "SELECT COUNT(*) FROM library_groups WHERE group_name = ?1",
        params![group_name],
        |row| row.get(0),
    )?;
    if exists > 0 && !replace {
        return Err(format!(
            "Merged group '{}' already exists. Use --replace to overwrite membership.",
            group_name
        )
        .into());
    }

    let tx = conn.unchecked_transaction()?;
    if exists > 0 {
        tx.execute(
            "DELETE FROM library_groups WHERE group_name = ?1",
            params![group_name],
        )?;
    }
    let now = now_epoch();
    for (idx, member) in resolved_members.iter().enumerate() {
        tx.execute(
            "INSERT INTO library_groups (group_name, member_library_name, member_order, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![group_name, member, idx as i64, now],
        )?;
    }
    tx.commit()?;

    if let Some(path) = include_artifacts {
        let mut compiled_parts = Vec::new();
        for member in &resolved_members {
            compiled_parts.push(compiled_text_for_library(conn, member)?);
        }
        let mut compiled = compiled_parts.join("\n\n");
        if !compiled.is_empty() {
            compiled.push_str("\n\n");
        }
        write_artifacts(path, &compiled)?;
    }

    Ok(())
}

async fn add_library(
    conn: &Connection,
    library_name: &str,
    source_url: &str,
    include_artifacts: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let exists: i64 = conn.query_row(
        "SELECT COUNT(*) FROM libraries WHERE library_name = ?1",
        params![library_name],
        |row| row.get(0),
    )?;
    if exists == 0 {
        crawl_library(
            conn,
            library_name,
            source_url,
            "add-crawl",
            include_artifacts.clone(),
        )
        .await?;
    } else {
        let page_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pages WHERE library_name = ?1",
            params![library_name],
            |row| row.get(0),
        )?;
        if page_count == 0 {
            crawl_library(
                conn,
                library_name,
                source_url,
                "add-crawl",
                include_artifacts.clone(),
            )
            .await?;
        }
    }
    index_library(conn, library_name, None)?;
    Ok(())
}

fn refresh_stats(conn: &Connection, input_names: &[String]) -> Result<(), Box<dyn Error>> {
    let mut targets = Vec::new();
    let mut seen = HashSet::new();
    if input_names.is_empty() {
        let mut stmt =
            conn.prepare("SELECT library_name FROM libraries ORDER BY library_name ASC")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            let name = row?;
            if seen.insert(name.clone()) {
                targets.push(name);
            }
        }
    } else {
        for input in input_names {
            for name in resolve_target_libraries(conn, input)? {
                if seen.insert(name.clone()) {
                    targets.push(name);
                }
            }
        }
    }

    if targets.is_empty() {
        return Err("No libraries found to refresh.".into());
    }

    let now = now_epoch();
    for library_name in &targets {
        backfill_pages_from_chunks(conn, library_name, &now)?;
        backfill_library_text(conn, library_name, &now)?;
        update_library_rollups(conn, library_name)?;
        conn.execute(
            "UPDATE libraries SET last_refreshed_at = ?1 WHERE library_name = ?2",
            params![now, library_name],
        )?;
    }

    let job_id = start_job(conn, "_system", "refresh-stats")?;
    finish_job(
        conn,
        job_id,
        "success",
        &format!("Refreshed {} libraries.", targets.len()),
    )?;
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
    include_artifacts: Option<PathBuf>,
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
        let page_inputs: Vec<(String, String)> = pages
            .iter()
            .map(|p| (p.get_url().to_string(), p.get_html()))
            .collect();
        // Indexed parallel iterators preserve order on collect.
        let converted: Vec<(String, String)> = page_inputs
            .into_par_iter()
            .map(|(url, html)| {
                let extracted_html = extract_content_html(&html);
                let markdown = cleanup_markdown(&html2md::parse_html(&extracted_html));
                (url, markdown)
            })
            .collect();
        let mut compiled_parts = Vec::with_capacity(converted.len());
        let mut total_chars = 0i64;
        for (_, markdown) in &converted {
            total_chars += markdown.chars().count() as i64;
            compiled_parts.push(markdown.clone());
        }
        let mut compiled = compiled_parts.join("\n\n");
        if !compiled.is_empty() {
            compiled.push_str("\n\n");
        }

        let now = now_epoch();
        conn.execute(
            "INSERT OR REPLACE INTO library_texts (library_name, content, updated_at) VALUES (?1, ?2, ?3)",
            params![library_name, compiled, now],
        )?;

        if let Some(path) = include_artifacts.clone() {
            write_artifacts(&path, &compiled)?;
        }

        let tx = conn.unchecked_transaction()?;
        tx.execute("DELETE FROM pages WHERE library_name = ?1", params![library_name])?;
        for (page_order, (url, markdown)) in converted.iter().enumerate() {
            tx.execute(
                "INSERT INTO pages (
                    library_name, page_order, source_url, content, content_size_chars, crawled_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    library_name,
                    page_order as i64,
                    url,
                    markdown,
                    markdown.chars().count() as i64,
                    now
                ],
            )?;
        }
        tx.commit()?;

        if let Ok(mut s) = stage.lock() {
            *s = String::from("Finalizing");
        }
        done.store(true, Ordering::Relaxed);
        let _ = spinner_handle.await;
        print!("\r                    \r");
        let _ = stdout().flush();

        conn.execute(
            "INSERT OR REPLACE INTO libraries (
               library_name, source_url, created_at, updated_at, last_refreshed_at,
               content_size_chars, page_count, chunk_count, embedded_chunk_count,
               empty_page_count, min_chunks_per_page, max_chunks_per_page
             )
             VALUES (
               ?1, ?2,
               COALESCE((SELECT created_at FROM libraries WHERE library_name = ?1), ?3),
               ?3, ?3, ?4, ?5,
               COALESCE((SELECT chunk_count FROM libraries WHERE library_name = ?1), 0),
               COALESCE((SELECT embedded_chunk_count FROM libraries WHERE library_name = ?1), 0),
               COALESCE((SELECT empty_page_count FROM libraries WHERE library_name = ?1), 0),
               COALESCE((SELECT min_chunks_per_page FROM libraries WHERE library_name = ?1), 0),
               COALESCE((SELECT max_chunks_per_page FROM libraries WHERE library_name = ?1), 0)
             )",
            params![library_name, source_url, now, total_chars, converted.len() as i64],
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

fn strip_front_matter(input: &str) -> &str {
    if !input.starts_with("---\n") {
        return input;
    }
    if let Some(end) = input[4..].find("\n---\n") {
        let idx = 4 + end + 5;
        return &input[idx..];
    }
    input
}

fn preprocess_for_chunking(input: &str) -> String {
    let normalized = input.replace("\r\n", "\n");
    let stripped = strip_front_matter(&normalized);
    let setext_h1 = Regex::new(r"(?m)^([^\n][^\n]*)\n=+\s*$").expect("valid regex");
    let setext_h2 = Regex::new(r"(?m)^([^\n][^\n]*)\n-+\s*$").expect("valid regex");
    let out = setext_h1.replace_all(stripped, "# $1");
    setext_h2.replace_all(&out, "## $1").into_owned()
}

const CHUNK_MIN_CHARS: usize = 600;

fn chunk_markdown_page(content: &str) -> Vec<String> {
    let processed = preprocess_for_chunking(content);
    let mut out = Vec::new();
    let mut current = String::new();
    for line in processed.split_inclusive('\n') {
        let is_heading = line.trim_start().starts_with('#');
        if is_heading && !current.trim().is_empty() {
            out.push(std::mem::take(&mut current));
        }
        current.push_str(line);
    }
    if !current.trim().is_empty() {
        out.push(current);
    }

    // Final trim + dedupe pass.
    let mut cleaned = Vec::new();
    for chunk in out {
        let t = chunk.trim().to_string();
        if !t.is_empty() && cleaned.last() != Some(&t) {
            cleaned.push(t);
        }
    }

    // Lower-bound only top-up: merge tiny chunks forward until min size.
    let mut topped = Vec::new();
    let mut pending: Option<String> = None;
    for chunk in cleaned {
        match pending.take() {
            None => {
                pending = Some(chunk);
            }
            Some(prev) => {
                if prev.chars().count() >= CHUNK_MIN_CHARS {
                    topped.push(prev);
                    pending = Some(chunk);
                } else {
                    pending = Some(format!("{prev}\n\n{chunk}"));
                }
            }
        }
    }
    if let Some(last) = pending {
        topped.push(last);
    }
    topped
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

fn resolve_or_create_library_for_index(
    conn: &Connection,
    input_name: &str,
    custom_file: Option<&str>,
) -> Result<(String, String), Box<dyn Error>> {
    let resolved_name = resolve_library_name(conn, input_name);
    let custom_file_source_url = if let Some(file_path) = custom_file {
        let canonical_path = PathBuf::from(file_path).canonicalize()?;
        Some(format!("file://{}", canonical_path.display()))
    } else {
        None
    };
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
            let source_url = custom_file_source_url
                .clone()
                .unwrap_or_else(|| format!("file://{}", file_path));
            let now = now_epoch();
            conn.execute(
                "INSERT INTO libraries (library_name, source_url, created_at, updated_at, last_refreshed_at)
                 VALUES (?1, ?2, ?3, ?3, ?3)",
                params![input_name, source_url, now],
            )?;
            (input_name.to_string(), source_url)
        }
    };
    Ok((library_name, source_url))
}

fn load_page_inputs(
    conn: &Connection,
    library_name: &str,
    source_url: &str,
    custom_file: Option<&str>,
) -> Result<Vec<(i64, String, String)>, Box<dyn Error>> {
    let custom_file_source_url = if let Some(file_path) = custom_file {
        let canonical_path = PathBuf::from(file_path).canonicalize()?;
        Some(format!("file://{}", canonical_path.display()))
    } else {
        None
    };
    let page_inputs: Vec<(i64, String, String)> = if let Some(file_path) = custom_file {
        let source_text = fs::read_to_string(file_path)?;
        let page_url = custom_file_source_url
            .clone()
            .unwrap_or_else(|| source_url.to_string());
        vec![(0i64, page_url, source_text)]
    } else {
        let mut stmt = conn.prepare(
            "SELECT page_order, source_url, content
                 FROM pages
                 WHERE library_name = ?1
                 ORDER BY page_order ASC",
        )?;
        let rows = stmt.query_map(params![library_name], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        if !out.is_empty() {
            out
        } else {
            let from_db: Option<String> = conn
                .query_row(
                    "SELECT content FROM library_texts WHERE library_name = ?1",
                    params![library_name],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(content) = from_db {
                vec![(0i64, source_url.to_string(), content)]
            } else {
                let out_dir = compiled_dir(&library_name);
                let txt = out_dir.join("docs.txt");
                let md = out_dir.join("docs.md");
                if txt.exists() {
                    vec![(0i64, source_url.to_string(), fs::read_to_string(txt)?)]
                } else if md.exists() {
                    vec![(0i64, source_url.to_string(), fs::read_to_string(md)?)]
                } else {
                    return Err(format!(
                        "No crawled text found for '{}'. Run add first.",
                        library_name
                    )
                    .into());
                }
            }
        }
    };

    Ok(page_inputs)
}

fn chunk_library(
    conn: &Connection,
    input_name: &str,
    custom_file: Option<&str>,
    job_type: &str,
) -> Result<(), Box<dyn Error>> {
    let (library_name, source_url) =
        resolve_or_create_library_for_index(conn, input_name, custom_file)?;
    let job_id = start_job(conn, &library_name, job_type)?;
    let result = (|| -> Result<String, Box<dyn Error>> {
        let page_inputs = load_page_inputs(conn, &library_name, &source_url, custom_file)?;
        if page_inputs.is_empty() {
            return Err("No pages available for chunking.".into());
        }

        if custom_file.is_some() {
            let tx = conn.unchecked_transaction()?;
            tx.execute(
                "DELETE FROM pages WHERE library_name = ?1",
                params![library_name],
            )?;
            let now = now_epoch();
            for (page_order, page_url, content) in &page_inputs {
                tx.execute(
                    "INSERT INTO pages (
                        library_name, page_order, source_url, content, content_size_chars, crawled_at
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        library_name,
                        page_order,
                        page_url,
                        content,
                        content.chars().count() as i64,
                        now
                    ],
                )?;
            }
            tx.commit()?;
        }

        let mut chunk_rows: Vec<(i64, String, i64, String)> = Vec::new();
        let mut per_page_chunk_counts: Vec<i64> = Vec::with_capacity(page_inputs.len());
        for (page_order, page_url, page_content) in &page_inputs {
            let page_chunks = chunk_markdown_page(page_content);
            per_page_chunk_counts.push(page_chunks.len() as i64);
            for (chunk_index_in_page, chunk) in page_chunks.into_iter().enumerate() {
                chunk_rows.push((
                    *page_order,
                    page_url.clone(),
                    chunk_index_in_page as i64,
                    chunk,
                ));
            }
        }

        if chunk_rows.is_empty() {
            return Err("No chunks generated from input.".into());
        }

        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM chunks WHERE library_name = ?1",
            params![library_name],
        )?;

        let now = now_epoch();
        for (i, (source_page_order, chunk_source_url, chunk_index_in_page, chunk)) in
            chunk_rows.iter().enumerate()
        {
            let token_count = chunk.chars().count() as i64;
            tx.execute(
                "INSERT INTO chunks (
                    library_name, source_url, source_page_order, chunk_index_in_page,
                    global_chunk_index, content, embedding, token_count, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    library_name,
                    chunk_source_url,
                    source_page_order,
                    chunk_index_in_page,
                    i as i64,
                    chunk,
                    Vec::<u8>::new(),
                    token_count,
                    now
                ],
            )?;
        }
        tx.commit()?;

        let page_count = page_inputs.len() as i64;
        let total_chars: i64 = page_inputs
            .iter()
            .map(|(_, _, content)| content.chars().count() as i64)
            .sum();
        let chunk_count = chunk_rows.len() as i64;
        let empty_page_count = per_page_chunk_counts.iter().filter(|c| **c == 0).count() as i64;
        let min_chunks_per_page = per_page_chunk_counts.iter().copied().min().unwrap_or(0);
        let max_chunks_per_page = per_page_chunk_counts.iter().copied().max().unwrap_or(0);
        conn.execute(
            "UPDATE libraries
             SET content_size_chars = ?1,
                 page_count = ?2,
                 chunk_count = ?3,
                 embedded_chunk_count = 0,
                 empty_page_count = ?4,
                 min_chunks_per_page = ?5,
                 max_chunks_per_page = ?6,
                 updated_at = ?7
             WHERE library_name = ?8",
            params![
                total_chars,
                page_count,
                chunk_count,
                empty_page_count,
                min_chunks_per_page,
                max_chunks_per_page,
                now_epoch(),
                library_name
            ],
        )?;
        Ok(format!("Chunked {} chunks.", chunk_rows.len()))
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

fn chunk_targets(
    conn: &Connection,
    input_name: &str,
    custom_file: Option<&str>,
    job_type: &str,
) -> Result<(), Box<dyn Error>> {
    let targets = match resolve_target_libraries(conn, input_name) {
        Ok(t) => t,
        Err(_) if custom_file.is_some() => vec![input_name.to_string()],
        Err(_) => return Err(format!("Unknown library or merged group '{}'.", input_name).into()),
    };
    if targets.len() > 1 && custom_file.is_some() {
        return Err("Cannot use --file when target is a merged group.".into());
    }
    for target in targets {
        chunk_library(conn, &target, custom_file, job_type)?;
    }
    Ok(())
}

fn embed_library(
    conn: &Connection,
    input_name: &str,
    job_type: &str,
) -> Result<(), Box<dyn Error>> {
    let library_name = resolve_library_name(conn, input_name)?;
    let (chunk_count, _) = embedding_readiness(conn, &library_name)?;
    if chunk_count == 0 {
        return Err(format!(
            "No chunks found for '{}'. Run add or chunk first.",
            library_name
        )
        .into());
    }

    let job_id = start_job(conn, &library_name, job_type)?;
    let result = (|| -> Result<String, Box<dyn Error>> {
        let mut stmt = conn.prepare(
            "SELECT id, content FROM chunks WHERE library_name = ?1 AND LENGTH(embedding) = 0 ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![library_name], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut pending = Vec::new();
        for row in rows {
            pending.push(row?);
        }
        if pending.is_empty() {
            return Ok("All chunks already embedded.".to_string());
        }

        let mut model = TextEmbedding::try_new(
            InitOptions::new(DEFAULT_EMBEDDING_MODEL).with_show_download_progress(true),
        )?;
        let batch_size = 128usize;
        let tx = conn.unchecked_transaction()?;
        for batch in pending.chunks(batch_size) {
            let texts: Vec<String> = batch.iter().map(|(_, content)| content.clone()).collect();
            let embeds = model.embed(&texts, None)?;
            if embeds.len() != batch.len() {
                return Err("Embedding count mismatch.".into());
            }
            for (idx, (chunk_id, _)) in batch.iter().enumerate() {
                tx.execute(
                    "UPDATE chunks SET embedding = ?1 WHERE id = ?2",
                    params![embedding_to_bytes(&embeds[idx]), chunk_id],
                )?;
            }
        }
        tx.commit()?;
        update_library_rollups(conn, &library_name)?;
        Ok(format!("Embedded {} chunks.", pending.len()))
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

fn index_library(
    conn: &Connection,
    input_name: &str,
    custom_file: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let targets = match resolve_target_libraries(conn, input_name) {
        Ok(t) => t,
        Err(_) if custom_file.is_some() => vec![input_name.to_string()],
        Err(_) => return Err(format!("Unknown library or merged group '{}'.", input_name).into()),
    };
    if targets.len() > 1 && custom_file.is_some() {
        return Err("Cannot use --file when target is a merged group.".into());
    }

    for target_name in targets {
        let (total, _embedded) = embedding_readiness(conn, &target_name).unwrap_or((0, 0));
        if custom_file.is_some() || total == 0 {
            chunk_library(conn, &target_name, custom_file, "index-chunk")?;
        }
        let (total_after_chunk, embedded_after_chunk) =
            embedding_readiness(conn, &target_name).unwrap_or((0, 0));
        if total_after_chunk > 0 && embedded_after_chunk < total_after_chunk {
            embed_library(conn, &target_name, "index-embed")?;
        }
    }
    Ok(())
}

fn load_chunks_for_library(
    conn: &Connection,
    library_name: &str,
) -> Result<Vec<ChunkRecord>, Box<dyn Error>> {
    let mut stmt = conn.prepare(
        "SELECT id, library_name, source_url, source_page_order, chunk_index_in_page,
                global_chunk_index, content, embedding
         FROM chunks
         WHERE library_name = ?1 AND LENGTH(embedding) > 0",
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

fn embedding_readiness(
    conn: &Connection,
    library_name: &str,
) -> Result<(i64, i64), Box<dyn Error>> {
    let (total, embedded): (i64, i64) = conn.query_row(
        "SELECT COALESCE(chunk_count, 0), COALESCE(embedded_chunk_count, 0)
         FROM libraries
         WHERE library_name = ?1",
        params![library_name],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok((total, embedded))
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
    let target_libraries = resolve_target_libraries(conn, input_name)?;
    for library_name in &target_libraries {
        let (total, embedded) = embedding_readiness(conn, library_name)?;
        if total == 0 || embedded == 0 {
            println!(
                "Library '{}' is not embedded yet. Run `plshelp add {}` (or `plshelp index {}`) first.",
                library_name, library_name, library_name
            );
            return Ok(());
        }
        if embedded < total {
            println!(
                "Library '{}' has partial embeddings ({}/{}). Run `plshelp embed {}`.",
                library_name, embedded, total, library_name
            );
            return Ok(());
        }
    }
    let mut chunks = Vec::new();
    for library_name in &target_libraries {
        chunks.extend(load_chunks_for_library(conn, library_name)?);
    }
    if chunks.is_empty() {
        println!("No chunks indexed for '{}'.", input_name);
        return Ok(());
    }
    let query_embedding = embed_query(mode, question)?;
    let mut scored = score_chunks(&chunks, question, mode, query_embedding.as_deref());
    scored.truncate(top_k);

    for (rank, hit) in scored.iter().enumerate() {
        println!("{}. [{}] {}", rank + 1, hit.chunk.id, hit.chunk.source_url);
        if target_libraries.len() > 1 {
            println!("   library: {}", hit.chunk.library_name);
        }
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

fn ask_libraries(
    conn: &Connection,
    question: &str,
    flags: &[String],
) -> Result<(), Box<dyn Error>> {
    let (mode, top_k, context, filter) = ask_flags(flags)?;
    let libraries = if let Some(libs) = filter {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for lib in libs {
            for expanded in resolve_target_libraries(conn, &lib)? {
                if seen.insert(expanded.clone()) {
                    out.push(expanded);
                }
            }
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
        let (total, embedded) = embedding_readiness(conn, &lib)?;
        if total == 0 || embedded == 0 || embedded < total {
            continue;
        }
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

fn library_status(conn: &Connection, library_name: &str) -> Result<String, Box<dyn Error>> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM jobs WHERE library_name = ?1 ORDER BY id DESC LIMIT 1",
            params![library_name],
            |row| row.get(0),
        )
        .optional()?;
    Ok(status.unwrap_or_else(|| "unknown".to_string()))
}

fn latest_success_time_by_kind(
    conn: &Connection,
    library_name: &str,
    kind: &str,
) -> Result<Option<String>, Box<dyn Error>> {
    let pattern = format!("%{kind}%");
    let value: Option<String> = conn
        .query_row(
            "SELECT ended_at
             FROM jobs
             WHERE library_name = ?1 AND status = 'success' AND job_type LIKE ?2
             ORDER BY id DESC
             LIMIT 1",
            params![library_name, pattern],
            |row| row.get(0),
        )
        .optional()?;
    Ok(value)
}

fn latest_failed_message(
    conn: &Connection,
    library_name: &str,
) -> Result<Option<String>, Box<dyn Error>> {
    let msg: Option<String> = conn
        .query_row(
            "SELECT message FROM jobs WHERE library_name = ?1 AND status = 'failed' ORDER BY id DESC LIMIT 1",
            params![library_name],
            |row| row.get(0),
        )
        .optional()?;
    Ok(msg)
}

fn library_rollups(
    conn: &Connection,
    library_name: &str,
) -> Result<(i64, i64, i64, i64, i64, i64, i64), Box<dyn Error>> {
    let values = conn.query_row(
        "SELECT
            COALESCE(content_size_chars, 0),
            COALESCE(page_count, 0),
            COALESCE(chunk_count, 0),
            COALESCE(embedded_chunk_count, 0),
            COALESCE(empty_page_count, 0),
            COALESCE(min_chunks_per_page, 0),
            COALESCE(max_chunks_per_page, 0)
         FROM libraries
         WHERE library_name = ?1",
        params![library_name],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        },
    )?;
    Ok(values)
}

fn aggregate_rollups_for_libraries(
    conn: &Connection,
    libraries: &[String],
) -> Result<(i64, i64, i64, i64, i64, i64), Box<dyn Error>> {
    let mut total_chars = 0i64;
    let mut total_pages = 0i64;
    let mut total_chunks = 0i64;
    let mut total_empty_pages = 0i64;
    let mut min_chunks = i64::MAX;
    let mut max_chunks = i64::MIN;
    let mut saw_min_max = false;
    for lib in libraries {
        let (chars, pages, chunks, _embedded, empty_pages, min_per_page, max_per_page) =
            library_rollups(conn, lib)?;
        total_chars += chars;
        total_pages += pages;
        total_chunks += chunks;
        total_empty_pages += empty_pages;
        if pages > 0 {
            if !saw_min_max || min_per_page < min_chunks {
                min_chunks = min_per_page;
            }
            if !saw_min_max || max_per_page > max_chunks {
                max_chunks = max_per_page;
            }
            saw_min_max = true;
        }
    }
    if !saw_min_max {
        min_chunks = 0;
        max_chunks = 0;
    }
    Ok((
        total_chars,
        total_pages,
        total_chunks,
        total_empty_pages,
        min_chunks,
        max_chunks,
    ))
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
    let mut libraries = Vec::new();
    for row in rows {
        libraries.push(row?);
    }

    for (library_name, source_url, refreshed) in libraries {
        let (chars, pages, chunks, _embedded, _empty, _min, _max) =
            library_rollups(conn, &library_name)?;
        let status = library_status(conn, &library_name)?;
        println!("{library_name}");
        println!("  source: {source_url}");
        println!("  pages: {pages}");
        println!("  chunks: {chunks}");
        println!("  chars: {chars}");
        println!("  status: {status}");
        println!("  last refreshed: {}", human_time(&refreshed));
    }

    let mut group_stmt =
        conn.prepare("SELECT DISTINCT group_name FROM library_groups ORDER BY group_name ASC")?;
    let group_rows = group_stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut group_names = Vec::new();
    for row in group_rows {
        group_names.push(row?);
    }
    for group_name in group_names {
        let members = group_members(conn, &group_name)?;
        let (content_size_chars, pages, chunks, _empty, _min, _max) =
            aggregate_rollups_for_libraries(conn, &members)?;
        println!("{group_name}");
        println!("  source: merged group");
        println!("  pages: {pages}");
        println!("  chunks: {chunks}");
        println!("  chars: {content_size_chars}");
        println!("  status: merged");
        println!("  last refreshed: n/a");
    }
    Ok(())
}

fn show_library(conn: &Connection, input_name: &str) -> Result<(), Box<dyn Error>> {
    if let Ok(library_name) = resolve_library_name(conn, input_name) {
        let (source_url, refreshed): (String, String) = conn.query_row(
            "SELECT source_url, last_refreshed_at FROM libraries WHERE library_name = ?1",
            params![library_name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let (
            content_size_chars,
            pages,
            chunks,
            embedded_chunks,
            empty_pages,
            min_chunks,
            max_chunks,
        ) = library_rollups(conn, &library_name)?;
        let avg_chunks = if pages > 0 {
            chunks as f64 / pages as f64
        } else {
            0.0
        };
        let latest_status = library_status(conn, &library_name)?;
        let last_crawled_at = latest_success_time_by_kind(conn, &library_name, "crawl")?;
        let last_indexed_at = latest_success_time_by_kind(conn, &library_name, "index")?;
        let latest_error = latest_failed_message(conn, &library_name)?;

        println!("library_name: {library_name}");
        println!("source_url: {source_url}");
        println!("page_count: {pages}");
        println!("chunk_count: {chunks}");
        println!("embedded_chunk_count: {embedded_chunks}");
        println!("avg_chunks_per_page: {:.2}", avg_chunks);
        println!("min_chunks_per_page: {min_chunks}");
        println!("max_chunks_per_page: {max_chunks}");
        println!("pages_with_no_chunks: {empty_pages}");
        println!("content_size_chars: {content_size_chars}");
        println!("indexed_model: {:?}", DEFAULT_EMBEDDING_MODEL);
        println!("embedding_dim: 1024");
        println!("latest_job_status: {latest_status}");
        println!(
            "last_crawled_at: {}",
            last_crawled_at
                .as_deref()
                .map(human_time)
                .unwrap_or_else(|| "n/a".to_string())
        );
        println!(
            "last_indexed_at: {}",
            last_indexed_at
                .as_deref()
                .map(human_time)
                .unwrap_or_else(|| "n/a".to_string())
        );
        if let Some(err) = latest_error {
            println!("latest_error: {err}");
        }
        println!("last_refreshed_at: {}", human_time(&refreshed));
        let mut alias_stmt = conn.prepare(
            "SELECT alias FROM library_aliases WHERE library_name = ?1 ORDER BY alias ASC",
        )?;
        let alias_rows =
            alias_stmt.query_map(params![library_name], |row| row.get::<_, String>(0))?;
        let mut aliases = Vec::new();
        for row in alias_rows {
            aliases.push(row?);
        }
        if !aliases.is_empty() {
            println!("aliases: {}", aliases.join(", "));
        }
        return Ok(());
    }

    let members = group_members(conn, input_name)?;
    if members.is_empty() {
        return Err(format!("Unknown library or merged group '{}'.", input_name).into());
    }
    let (content_size_chars, pages, chunks, empty_pages, min_chunks, max_chunks) =
        aggregate_rollups_for_libraries(conn, &members)?;
    let mut embedded_chunks = 0i64;
    for member in &members {
        let (_, _, _, embedded, _, _, _) = library_rollups(conn, member)?;
        embedded_chunks += embedded;
    }
    let avg_chunks = if pages > 0 {
        chunks as f64 / pages as f64
    } else {
        0.0
    };
    println!("library_name: {input_name}");
    println!("source_url: merged group");
    println!("page_count: {pages}");
    println!("chunk_count: {chunks}");
    println!("embedded_chunk_count: {embedded_chunks}");
    println!("avg_chunks_per_page: {:.2}", avg_chunks);
    println!("min_chunks_per_page: {min_chunks}");
    println!("max_chunks_per_page: {max_chunks}");
    println!("pages_with_no_chunks: {empty_pages}");
    println!("content_size_chars: {content_size_chars}");
    println!("indexed_model: {:?}", DEFAULT_EMBEDDING_MODEL);
    println!("embedding_dim: 1024");
    println!("latest_job_status: merged");
    println!("last_crawled_at: n/a");
    println!("last_indexed_at: n/a");
    println!("last_refreshed_at: n/a");
    println!("members: {}", members.join(", "));
    Ok(())
}

fn remove_library(conn: &Connection, input_name: &str) -> Result<(), Box<dyn Error>> {
    let library_name = match resolve_library_name(conn, input_name) {
        Ok(name) => name,
        Err(_) => {
            let deleted = conn.execute(
                "DELETE FROM library_groups WHERE group_name = ?1",
                params![input_name],
            )?;
            if deleted > 0 {
                return Ok(());
            }
            return Err(format!("Unknown library or merged group '{}'.", input_name).into());
        }
    };
    conn.execute(
        "DELETE FROM library_aliases WHERE library_name = ?1",
        params![library_name],
    )?;
    conn.execute(
        "DELETE FROM chunks WHERE library_name = ?1",
        params![library_name],
    )?;
    conn.execute(
        "DELETE FROM pages WHERE library_name = ?1",
        params![library_name],
    )?;
    conn.execute(
        "DELETE FROM library_texts WHERE library_name = ?1",
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
