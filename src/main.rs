use regex::Regex;
use scraper::{Html, Selector};
use spider::configuration::Configuration;
use spider::compact_str::CompactString;
use spider::tokio;
use spider::website::Website;
use std::fs::OpenOptions;
use std::io::{stdin, stdout, Write};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use termios::{tcsetattr, Termios, ECHO, TCSANOW};
use url::Url;

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

fn normalize_seed_url(seed_url: &str) -> Result<String, String> {
    let mut parsed = Url::parse(seed_url)
        .map_err(|e| format!("Invalid seed URL '{}': {}", seed_url, e))?;
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
    let parsed = Url::parse(seed_url)
        .map_err(|e| format!("Invalid seed URL '{}': {}", seed_url, e))?;
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
    let selectors = ["main", "article", r#"[role="main"]"#];

    let mut extracted = None;
    for selector_str in selectors {
        let selector = match Selector::parse(selector_str) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if let Some(node) = document.select(&selector).next() {
            let selected_html = node.html();
            if !selected_html.trim().is_empty() {
                extracted = Some(selected_html);
                break;
            }
        }
    }

    let mut cleaned = extracted.unwrap_or_else(|| html.to_string());
    let cleanup_patterns = [
        r"(?is)<script\b[^>]*>.*?</script>",
        r"(?is)<style\b[^>]*>.*?</style>",
        r"(?is)<noscript\b[^>]*>.*?</noscript>",
        r"(?is)<nav\b[^>]*>.*?</nav>",
        r"(?is)<header\b[^>]*>.*?</header>",
        r"(?is)<footer\b[^>]*>.*?</footer>",
        r"(?is)<aside\b[^>]*>.*?</aside>",
    ];

    for pattern in cleanup_patterns {
        if let Ok(re) = Regex::new(pattern) {
            cleaned = re.replace_all(&cleaned, "").into_owned();
        }
    }

    cleaned
}

#[tokio::main]
async fn main() {
    let _echo_guard = TerminalEchoGuard::new();

    let seed_url = "https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide";
    let normalized_seed_url =
        normalize_seed_url(seed_url).expect("Failed to normalize seed URL");
    let whitelist = whitelist_for_url(&normalized_seed_url)
        .expect("Failed to build whitelist regex from seed URL");

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
        .expect("Failed to build website");

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
            println!("Done.");
            return;
        }
    };

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("output.txt")
        .expect("Failed to open output.txt");

    if let Ok(mut s) = stage.lock() {
        *s = String::from("Writing files");
    }
    for page in pages.iter() {
        let html = page.get_html();
        let extracted_html = extract_content_html(&html);
        let markdown = html2md::parse_html(&extracted_html);
        file.write_all(markdown.as_bytes())
            .expect("Couldn't write page markdown to output file.");
        file.write_all(b"\n\n")
            .expect("Couldn't write separator to output file.");
    }

    if let Ok(mut s) = stage.lock() {
        *s = String::from("Finalizing");
    }
    done.store(true, Ordering::Relaxed);
    let _ = spinner_handle.await;
    print!("\r                    \r");
    let _ = stdout().flush();
    println!("Done!");
}
