use std::{
    fs,
    path::{Path, PathBuf},
};

fn collect_rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[test]
fn rust_source_files_stay_under_2000_lines() {
    let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut rust_sources = Vec::new();
    collect_rust_sources(&src_dir, &mut rust_sources);
    rust_sources.sort();

    let limit = 2000usize;
    let violations = rust_sources
        .into_iter()
        .filter_map(|path| {
            let contents = fs::read_to_string(&path).ok()?;
            let lines = contents.lines().count();
            (lines > limit).then(|| format!("{} ({lines} lines)", path.display()))
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "Rust source files exceed {limit} lines:\n{}",
        violations.join("\n")
    );
}
