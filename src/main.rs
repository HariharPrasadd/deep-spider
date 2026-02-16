use spider::configuration::Configuration;
use spider::compact_str::CompactString;
use spider::tokio;
use spider::website::Website;
use std::fs::File;
use std::io::Write;

const SEED_URL: &str =
    "https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference";

fn mdn_reference_whitelist() -> Vec<CompactString> {
    vec![CompactString::new(
        r"^https://developer\.mozilla\.org/en-US/docs/Web/JavaScript/Reference(/|$)",
    )]
}

#[tokio::main]
async fn main() {
    let mut config = Configuration::new();
    config
        .with_limit(5_000)
        .with_depth(25)
        .with_subdomains(false)
        .with_tld(false)
        .with_user_agent(Some("DocumentationScraper/1.0"))
        .with_whitelist_url(Some(mdn_reference_whitelist()));

    let mut website = Website::new(SEED_URL)
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
