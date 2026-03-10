use super::*;

pub(crate) fn run_compute_metrics(req: ComputeReq) -> Result<ComputeResp, String> {
    let text = req.text;
    if text.trim().is_empty() {
        return Err("empty text for compute_metrics".to_string());
    }

    let lines_vec: Vec<&str> = text.lines().collect();
    let mut sections = 0usize;
    let mut bullets = 0usize;
    let mut cjk = 0usize;
    let mut latin = 0usize;
    let mut digits = 0usize;
    let mut reference_hits = 0usize;
    let mut note_hits = 0usize;

    for line in &lines_vec {
        let t = line.trim();
        if t.starts_with("## ") {
            sections += 1;
        }
        if t.starts_with("- ") {
            bullets += 1;
        }
        let tl = t.to_lowercase();
        if tl.contains("references")
            || tl.contains("bibliography")
            || t.contains("参考文献")
            || t.contains("引用文献")
            || t.contains("文献目录")
        {
            reference_hits += 1;
        }
        if tl.contains("acknowledg")
            || tl.contains("footnote")
            || tl.contains("appendix")
            || t.contains("注释")
            || t.contains("脚注")
            || t.contains("附录")
            || t.contains("致谢")
        {
            note_hits += 1;
        }
    }

    for ch in text.chars() {
        if ch.is_ascii_alphabetic() {
            latin += 1;
        } else if ch.is_ascii_digit() {
            digits += 1;
        } else if ('\u{4E00}'..='\u{9FFF}').contains(&ch) {
            cjk += 1;
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let sha256 = format!("{:x}", hasher.finalize());

    Ok(ComputeResp {
        ok: true,
        operator: "compute_metrics".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        metrics: ComputeMetrics {
            sections,
            bullets,
            chars: text.chars().count(),
            lines: lines_vec.len(),
            cjk,
            latin,
            digits,
            reference_hits,
            note_hits,
            sha256,
        },
    })
}
