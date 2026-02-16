use spider::configuration::Configuration;
use spider::compact_str::CompactString;
use spider::tokio;
use spider::website::Website;
use url::Url;
use std::fs::File;
use std::io::Write;

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

#[tokio::main]
async fn main() {
    let seed_url = "https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference";
    let whitelist = whitelist_for_url(seed_url)
        .expect("Failed to build whitelist regex from seed URL");

    let mut config = Configuration::new();
    config
        .with_limit(5_000)
        .with_depth(25)
        .with_subdomains(false)
        .with_tld(false)
        .with_user_agent(Some("DocumentationScraper/1.0"))
        .with_whitelist_url(Some(whitelist));

    let mut website = Website::new(seed_url)
        .with_config(config)
        .build()
        .expect("Failed to build website");

    website.scrape().await;

    let pages = match website.get_pages() {
        Some(p) => p,
        None => {
            println!("No pages collected.");
            return;
        }
    };

    let mut file = File::create("output.txt").expect("Failed to create output.txt.");
    writeln!(file, "Scraped {} pages", pages.len())
        .expect("Couldn't write to output file.");
    for (i, page) in pages.iter().enumerate() {
        let url = page.get_url();
        writeln!(file, "{}: {}", i + 1, url)
            .expect("Couldn't write to output file.");
    }
}
