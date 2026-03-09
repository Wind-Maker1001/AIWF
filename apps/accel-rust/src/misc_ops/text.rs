use super::*;

pub(crate) fn run_text_preprocess_v2(req: TextPreprocessReq) -> Result<TextPreprocessResp, String> {
    let mut lines: Vec<String> = req
        .text
        .replace("\r\n", "\n")
        .split('\n')
        .map(|s| s.to_string())
        .collect();
    let remove_refs = req.remove_references.unwrap_or(true);
    let remove_notes = req.remove_notes.unwrap_or(true);
    let normalize_ws = req.normalize_whitespace.unwrap_or(true);
    let mut removed_references_lines = 0usize;
    let mut removed_notes_lines = 0usize;

    if remove_refs {
        let mut cut_idx: Option<usize> = None;
        for (i, line) in lines.iter().enumerate() {
            let t = line.trim().to_lowercase();
            if t == "references" || t == "bibliography" || t == "参考文献" || t == "引用文献"
            {
                cut_idx = Some(i);
                break;
            }
        }
        if let Some(i) = cut_idx {
            removed_references_lines = lines.len().saturating_sub(i);
            lines = lines.into_iter().take(i).collect();
        }
    }

    if remove_notes {
        let mut out: Vec<String> = Vec::new();
        for line in lines {
            let t = line.trim();
            if t.starts_with('[') && t.contains(']') && t.len() < 24 {
                removed_notes_lines += 1;
                continue;
            }
            if t.to_lowercase().starts_with("footnote")
                || t.starts_with("注释")
                || t.starts_with("脚注")
            {
                removed_notes_lines += 1;
                continue;
            }
            out.push(line);
        }
        lines = out;
    }

    if normalize_ws {
        lines = lines
            .into_iter()
            .map(|x| collapse_ws(&x))
            .collect::<Vec<String>>();
    }

    if lines.is_empty() {
        return Err("text_preprocess_v2 produced empty content".to_string());
    }

    let mut markdown = String::new();
    if let Some(title) = req.title {
        let t = title.trim();
        if !t.is_empty() {
            markdown.push_str("# ");
            markdown.push_str(t);
            markdown.push_str("\n\n");
        }
    }
    markdown.push_str(lines.join("\n").trim());
    markdown.push('\n');

    let mut hasher = Sha256::new();
    hasher.update(markdown.as_bytes());
    let sha256 = format!("{:x}", hasher.finalize());
    Ok(TextPreprocessResp {
        ok: true,
        operator: "text_preprocess_v2".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        markdown,
        removed_references_lines,
        removed_notes_lines,
        sha256,
    })
}
