use crate::*;
use unicode_width::UnicodeWidthStr;

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const ORANGE: &str = "\x1b[33m";
const PURPLE: &str = "\x1b[35m";
const BLUE: &str = "\x1b[34m";
const AMBER: &str = "\x1b[93m";
const GREEN: &str = "\x1b[32m";

fn style(text: &str, codes: &[&str]) -> String {
    format!("{}{}{}", codes.join(""), text, RESET)
}

fn pad_visible(text: &str, width: usize) -> String {
    let visible = UnicodeWidthStr::width(text);
    if visible >= width {
        text.to_string()
    } else {
        format!("{}{}", text, " ".repeat(width - visible))
    }
}

fn print_command(command: &str, args: &str, description: &str, color: &str) {
    let command_col = pad_visible(command, 12);
    let args_col = pad_visible(args, 30);
    println!(
        "  {} {} {}",
        style(&command_col, &[BOLD, color]),
        style(&args_col, &[DIM]),
        description
    );
}

fn print_help_command_row() {
    let command_col = pad_visible("<command>", 12);
    let args_col = pad_visible("--help", 30);
    println!(
        "  {} {} {}",
        style(&command_col, &[GREEN]),
        style(&args_col, &[BOLD, GREEN]),
        "Print help text for command"
    );
}

pub(crate) fn print_help() {
    println!(
        "\n{}\n",
        format!(
            "{} is a local-first documentation search tool for agents and humans.",
            style("plshelp", &[BOLD, ORANGE])
        )
    );
    println!(
        "{} {}\n",
        style("Usage:", &[BOLD]),
        style("plshelp <command>", &[BOLD])
    );

    println!("{}", style("Commands:", &[BOLD]));
    print_command("add", "<library> <source>", "Crawl and index a source", PURPLE);
    print_command("crawl", "<library> <source>", "Crawl a source without indexing", PURPLE);
    print_command("query", "<library> \"<question>\"", "Search one library", PURPLE);
    print_command("trace", "<library> \"<question>\"", "Search with scoring details", PURPLE);
    // Show the optional library scope in the top-level help so the summary matches
    // the actual ask parser and command-specific usage text.
    print_command("ask", "\"<question>\" [--libraries a,b,c]", "Search across libraries", PURPLE);
    // Surface the shorthand as the normal single-library form, not just an alias-only trick.
    print_command("<library>", "\"<question>\"", "Search one library (shorthand)", PURPLE);
    println!();

    print_command("index", "<library> | --all", "Build pages from raw inputs", BLUE);
    print_command("chunk", "<library> | --all", "Split indexed pages into chunks", BLUE);
    print_command("embed", "<library> | --all", "Generate embeddings for chunks", BLUE);
    print_command("refresh", "[libraries...] | --all", "Recompute stats without crawling", BLUE);
    print_command("merge", "<group> <library...>", "Combine libraries into one view", BLUE);
    print_command("export", "<library> [path] | --all [path]", "Write stored content to disk", BLUE);
    print_command("list", "", "Show indexed libraries and groups", BLUE);
    print_command("show", "<library>", "Inspect one library", BLUE);
    print_command("open", "<chunk_id>", "Open one stored chunk", BLUE);
    println!();

    print_command("alias", "<library> <alias>", "Add a shortcut name", AMBER);
    print_command("remove", "<library> | --all", "Delete libraries", AMBER);
    print_command("config", "", "Print the active config", AMBER);
    print_command("init", "", "Write AGENTS.md or CLAUDE.md", AMBER);
    print_command("uninstall", "--all | --data | --binary", "Remove plshelp from this machine", AMBER);
    println!();

    print_help_command_row();
    println!();
}

pub(crate) fn print_command_help(command: &str) -> bool {
    let help = match command {
        "add" => Some(
            "Usage:\n  plshelp add <library_name> <source_url> [--single] [--respect-robots] [--force] [--include-artifacts[=/path]] [--json]\n\nCrawl a source and run the full ingest pipeline\n",
        ),
        "crawl" => Some(
            "Usage:\n  plshelp crawl <library_name> <source_url> [--single] [--respect-robots] [--force] [--include-artifacts[=/path]] [--json]\n\nFetch content and store crawl artifacts without indexing\n",
        ),
        "init" => Some(
            "Usage:\n  plshelp init [--agents] [--claude] [--print] [--json]\n\nWrite AGENTS.md and/or CLAUDE.md templates in the current directory\n",
        ),
        "uninstall" => Some(
            "Usage:\n  plshelp uninstall --all | --data | --binary\n\nRemove plshelp files from this machine\n",
        ),
        "index" => Some(
            "Usage:\n  plshelp index <library_name> [--file /path/to/file] [--force] [--json]\n  plshelp index --all [--force] [--json]\n\nBuild indexed pages from raw inputs\n",
        ),
        "chunk" => Some(
            "Usage:\n  plshelp chunk <library_name> [--file /path/to/file] [--force] [--json]\n  plshelp chunk --all [--force] [--json]\n\nSplit indexed pages into chunks\n",
        ),
        "embed" => Some(
            "Usage:\n  plshelp embed <library_name> [--force] [--json]\n  plshelp embed --all [--force] [--json]\n\nGenerate embeddings for stored chunks\n",
        ),
        "refresh" => Some(
            "Usage:\n  plshelp refresh [library_name ...] [--all] [--json]\n\nRecompute and backfill stats without crawling\n",
        ),
        "merge" => Some(
            "Usage:\n  plshelp merge <new_library_name> <library1> <library2> [library3 ...] [--replace] [--include-artifacts[=/path]] [--json]\n\nCombine libraries into one merged group\n",
        ),
        "export" => Some(
            "Usage:\n  plshelp export <library_name> [path] [--json]\n  plshelp export --all [path] [--json]\n\nExport stored content to disk\n",
        ),
        "query" => Some(
            "Usage:\n  plshelp query <library_name> \"<question>\" [--mode hybrid|vector|keyword] [--top-k N] [--context N] [--json]\n\nSearch one library\n",
        ),
        "trace" => Some(
            "Usage:\n  plshelp trace <library_name> \"<question>\" [--mode hybrid|vector|keyword] [--top-k N] [--context N] [--json]\n\nSearch one library with scoring details\n",
        ),
        "ask" => Some(
            "Usage:\n  plshelp ask \"<question>\" [--libraries a,b,c] [--mode hybrid|vector|keyword] [--top-k N] [--context N] [--json]\n\nSearch across libraries\n",
        ),
        "alias" => Some(
            "Usage:\n  plshelp alias <library_name> <alias> [--json]\n\nAdd a shortcut name for a library\n",
        ),
        "list" => Some(
            "Usage:\n  plshelp list [--json]\n\nShow indexed libraries and merged groups\n",
        ),
        "config" => Some(
            "Usage:\n  plshelp config [--json]\n\nPrint the active config file path and contents\n",
        ),
        "show" => Some(
            "Usage:\n  plshelp show <library_name> [--json]\n\nInspect one library or merged group\n",
        ),
        "remove" => Some(
            "Usage:\n  plshelp remove <library_name> [--json]\n  plshelp remove --all [--json]\n\nDelete one library or all libraries\n",
        ),
        "open" => Some(
            "Usage:\n  plshelp open <chunk_id> [--json]\n\nOpen one stored chunk by id\n",
        ),
        _ => None,
    };

    if let Some(help) = help {
        if let Some((usage, about)) = help.split_once("\n\n") {
            println!(
                "\n{}\n{}\n\n{}\n{}\n",
                style("Usage:", &[BOLD]),
                usage.trim_start_matches("Usage:\n"),
                style("About:", &[BOLD]),
                about.trim()
            );
        } else {
            println!("\n{}\n", help);
        }
        true
    } else {
        false
    }
}

// ============================================================================
pub(crate) fn parse_query_flags(flags: &[String]) -> Result<(SearchMode, usize, usize), Box<dyn Error>> {
    let mut mode = default_search_mode();
    let mut top_k = default_top_k();
    let mut context = default_context_window();
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

pub(crate) fn extract_json_flag(flags: &[String]) -> (bool, Vec<String>) {
    let mut output_json = false;
    let mut out = Vec::with_capacity(flags.len());
    for flag in flags {
        if flag == "--json" {
            output_json = true;
        } else {
            out.push(flag.clone());
        }
    }
    (output_json, out)
}

pub(crate) fn split_query_and_flags(args: &[String]) -> (String, Vec<String>) {
    let first_flag = args
        .iter()
        .position(|arg| arg.starts_with("--"))
        .unwrap_or(args.len());
    let query = args[..first_flag].join(" ").trim().to_string();
    let flags = args[first_flag..].to_vec();
    (query, flags)
}

pub(crate) fn print_json(value: &Value) -> Result<(), Box<dyn Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

pub(crate) fn print_command_result(
    command: &str,
    output_json: bool,
    payload: Value,
) -> Result<(), Box<dyn Error>> {
    if output_json {
        print_json(&json!({
            "command": command,
            "status": "success",
            "result": payload,
        }))
    } else {
        println!("Done.");
        Ok(())
    }
}

pub(crate) fn context_to_json(context: &[ParentRecord], active_parent_id: i64) -> Vec<Value> {
    context
        .iter()
        .filter(|parent| parent.id != active_parent_id)
        .map(|parent| {
            json!({
                "parent_id": parent.id,
                "library_name": parent.library_name,
                "source_url": parent.source_url,
                "source_page_order": parent.source_page_order,
                "parent_index_in_page": parent.parent_index_in_page,
                "global_parent_index": parent.global_parent_index,
                "content": parent.content,
            })
        })
        .collect()
}

pub(crate) fn query_hit_to_json(
    rank: usize,
    hit: &ScoredChunk,
    parent: &ParentRecord,
    context: &[ParentRecord],
) -> Value {
    json!({
        "rank": rank,
        "chunk_id": hit.chunk.id,
        "parent_id": hit.chunk.parent_id,
        "library_name": hit.chunk.library_name,
        "source_url": parent.source_url,
        "content": parent.content,
        "scores": {
            "final": hit.final_score,
            "vector": hit.vector_score,
            "bm25": hit.bm25_score,
        },
        "child_location": {
            "source_page_order": hit.chunk.source_page_order,
            "parent_index_in_page": hit.chunk.parent_index_in_page,
            "child_index_in_parent": hit.chunk.child_index_in_parent,
            "global_chunk_index": hit.chunk.global_chunk_index,
        },
        "parent_location": {
            "source_page_order": parent.source_page_order,
            "parent_index_in_page": parent.parent_index_in_page,
            "global_parent_index": parent.global_parent_index,
        },
        "context": context_to_json(context, parent.id),
    })
}

pub(crate) fn ask_flags(
    flags: &[String],
) -> Result<(SearchMode, usize, usize, Option<Vec<String>>), Box<dyn Error>> {
    let mut mode = default_search_mode();
    let mut top_k = default_top_k();
    let mut context = default_context_window();
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

pub(crate) fn parse_index_file_flag(flags: &[String]) -> Option<String> {
    let mut i = 0usize;
    while i < flags.len() {
        if flags[i] == "--file" && i + 1 < flags.len() {
            return Some(flags[i + 1].clone());
        }
        i += 1;
    }
    None
}

pub(crate) fn parse_include_artifacts_flag(flags: &[String], library_name: &str) -> Option<PathBuf> {
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

pub(crate) fn extract_single_flag(flags: &[String]) -> (bool, Vec<String>) {
    let mut single_page = false;
    let mut out = Vec::with_capacity(flags.len());
    for flag in flags {
        if flag == "--single" {
            single_page = true;
        } else {
            out.push(flag.clone());
        }
    }
    (single_page, out)
}

pub(crate) fn extract_respect_robots_flag(flags: &[String]) -> (bool, Vec<String>) {
    let mut respect_robots = false;
    let mut out = Vec::with_capacity(flags.len());
    for flag in flags {
        if flag == "--respect-robots" {
            respect_robots = true;
        } else {
            out.push(flag.clone());
        }
    }
    (respect_robots, out)
}

pub(crate) fn extract_force_flag(flags: &[String]) -> (bool, Vec<String>) {
    let mut force = false;
    let mut out = Vec::with_capacity(flags.len());
    for flag in flags {
        if flag == "--force" {
            force = true;
        } else {
            out.push(flag.clone());
        }
    }
    (force, out)
}

pub(crate) fn extract_all_flag(flags: &[String]) -> (bool, Vec<String>) {
    let mut all = false;
    let mut out = Vec::with_capacity(flags.len());
    for flag in flags {
        if flag == "--all" {
            all = true;
        } else {
            out.push(flag.clone());
        }
    }
    (all, out)
}

pub(crate) fn parse_merge_args(
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
