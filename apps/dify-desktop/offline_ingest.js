const { createOfflineIngestParsers } = require("./offline_ingest_parsers");

const { deriveLegacyRulesFromCleaningSpec } = require("./offline_cleaning_spec");

function createOfflineIngest(deps = {}) {
  const {
    normalizeCell,
    normalizeAmount,
  } = deps;

  function splitTextToChunks(text) {
    return String(text || "")
      .split(/\r?\n+/)
      .map((s) => s.trim())
      .filter((s) => s.length > 0)
      .slice(0, 5000);
  }

  function rowFromText(filePath, sourceType, txt, idx) {
    return {
      source_file: filePath,
      source_type: sourceType,
      row_no: idx + 1,
      text: txt,
      amount: "",
    };
  }

  function toRowsFromWorksheet(ws, filePath) {
    const rows = [];
    const headers = [];
    ws.getRow(1).eachCell((cell, idx) => {
      const h = normalizeCell(cell.value);
      headers[idx - 1] = h || `col_${idx}`;
    });

    for (let r = 2; r <= ws.rowCount; r += 1) {
      const row = ws.getRow(r);
      const item = { source_file: filePath, source_type: "table", row_no: r - 1 };
      let nonEmpty = 0;
      for (let c = 1; c <= headers.length; c += 1) {
        const key = headers[c - 1];
        const val = normalizeCell(row.getCell(c).value);
        if (val !== "") nonEmpty += 1;
        item[key] = val;
      }
      if (nonEmpty > 0) rows.push(item);
    }
    return rows;
  }

  const parsers = createOfflineIngestParsers({
    ...deps,
    splitTextToChunks,
    rowFromText,
    toRowsFromWorksheet,
  });

  function collectInputFiles(params = {}) {
    const files = [];
    const add = (p) => {
      let v = p;
      if (v && typeof v === "object") {
        v = v.path || v.file || v.filePath || v.value || v.source_file || "";
      }
      const s = String(v || "").trim();
      if (!s) return;
      files.push(s);
    };

    add(params.input_csv_path);
    add(params.input_path);

    if (Array.isArray(params.input_files)) {
      params.input_files.forEach(add);
    } else if (typeof params.input_files === "string") {
      const raw = String(params.input_files).trim();
      if (raw.startsWith("[") && raw.endsWith("]")) {
        try {
          const arr = JSON.parse(raw);
          if (Array.isArray(arr)) arr.forEach(add);
        } catch {
          raw.split(/\r?\n/).forEach(add);
        }
      } else {
        raw.split(/\r?\n/).forEach(add);
      }
    }

    return Array.from(new Set(files));
  }

  async function readInputRows(params, warnings, runtime = {}) {
    const files = collectInputFiles(params);
    if (files.length === 0) {
      if (String(process.env.AIWF_ALLOW_DEMO_INPUT || "").trim() !== "1") {
        throw new Error("未提供输入文件，请先选择或拖入至少一个输入文件。");
      }
      return [
        { source_file: "builtin", source_type: "demo", row_no: 1, id: "1", amount: "100.25", text: "内置示例-就业数据" },
        { source_file: "builtin", source_type: "demo", row_no: 2, id: "2", amount: "230.50", text: "内置示例-教育数据" },
        { source_file: "builtin", source_type: "demo", row_no: 3, id: "2", amount: "230.50", text: "内置示例-重复行" },
        { source_file: "builtin", source_type: "demo", row_no: 4, id: "3", amount: "-11.00", text: "内置示例-异常值" },
      ];
    }

    const all = [];
    for (const f of files) {
      // eslint-disable-next-line no-await-in-loop
      const rows = await parsers.parseOneFile(f, warnings, params, runtime);
      all.push(...rows);
    }
    return all;
  }

  function normalizeRuleMap(v) {
    return v && typeof v === "object" && !Array.isArray(v) ? v : {};
  }

  function normalizeRuleArray(v) {
    return Array.isArray(v) ? v : [];
  }

  function resolveRuleObject(params = {}) {
    if (params && typeof params.cleaning_spec_v2 === "object" && params.cleaning_spec_v2) {
      return {
        ...deriveLegacyRulesFromCleaningSpec(params.cleaning_spec_v2),
        ...(params.rules && typeof params.rules === "object" ? params.rules : {}),
      };
    }
    if (params && typeof params.rules === "object" && params.rules) return params.rules;
    return params || {};
  }

  function toBool(value, fallback = false) {
    if (typeof value === "boolean") return value;
    const text = String(value ?? "").trim().toLowerCase();
    if (!text) return fallback;
    return ["1", "true", "yes", "on"].includes(text);
  }

  function resolveCleaningStrategy(params = {}, ruleObj = {}) {
    let envMode = String(process.env.AIWF_CLEANING_RUST_V2_MODE || "off").trim().toLowerCase();
    if (!["off", "shadow", "default"].includes(envMode)) envMode = "off";
    if (Object.prototype.hasOwnProperty.call(ruleObj || {}, "use_rust_v2")) {
      const enabled = toBool(ruleObj.use_rust_v2, false);
      const decision = enabled ? "force_rust" : "force_python";
      return { decision, requested_mode: envMode, effective_mode: decision };
    }
    if (Object.prototype.hasOwnProperty.call(params || {}, "use_rust_v2")) {
      const enabled = toBool(params.use_rust_v2, false);
      const decision = enabled ? "force_rust" : "force_python";
      return { decision, requested_mode: envMode, effective_mode: decision };
    }
    return { decision: envMode, requested_mode: envMode, effective_mode: envMode };
  }

  function buildShadowCompare(status, skippedReason = "", mismatches = []) {
    return {
      status,
      matched: status === "matched",
      mismatch_count: mismatches.length,
      mismatches,
      skipped_reason: skippedReason,
      compare_fields: ["rows", "quality", "reason_counts"],
    };
  }

  function mapRowByRename(row, renameMap) {
    const out = {};
    Object.keys(row || {}).forEach((k) => {
      const kTrim = String(k).trim();
      const mapped = String(renameMap[kTrim] || kTrim).trim();
      out[mapped] = normalizeCell(row[k]);
    });
    return out;
  }

  function cleanRows(rawRows, params = {}) {
    const ruleObj = resolveRuleObject(params);
    const strategy = resolveCleaningStrategy(params, ruleObj);
    const verifyOnDefault = false;
    const renameMap = normalizeRuleMap(ruleObj.rename_map);
    const casts = normalizeRuleMap(ruleObj.casts);
    const filters = normalizeRuleArray(ruleObj.filters);
    const deduplicateBy = normalizeRuleArray(ruleObj.deduplicate_by).map((x) => String(x).trim()).filter(Boolean);
    const dedupKeep = String(ruleObj.deduplicate_keep || "first").toLowerCase();

    let filteredRows = 0;
    const normalized = rawRows
      .map((row) => mapRowByRename(row, renameMap))
      .filter((r) => {
        const nonEmpty = Object.values(r).some((v) => String(v).trim() !== "");
        if (!nonEmpty) filteredRows += 1;
        return nonEmpty;
      });

    let dedup = [];
    const seen = new Set();
    const seenText = new Set();
    normalized.forEach((r) => {
      const txt = String(r.text || "").trim();
      if (txt.length >= 18) {
        const tk = txt.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
        if (tk.length > 16) {
          if (seenText.has(tk)) return;
          seenText.add(tk);
        }
      }
      const key = JSON.stringify(r);
      if (!seen.has(key)) {
        dedup.push(r);
        seen.add(key);
      }
    });

    if (deduplicateBy.length > 0) {
      const keepMap = new Map();
      dedup.forEach((r, idx) => {
        const k = deduplicateBy.map((f) => String(r[f] ?? "")).join("|");
        if (!k) return;
        if (!keepMap.has(k) || dedupKeep === "last") keepMap.set(k, idx);
      });
      const selected = new Set(Array.from(keepMap.values()));
      dedup = dedup.filter((r, idx) => {
        const k = deduplicateBy.map((f) => String(r[f] ?? "")).join("|");
        if (!k) return true;
        return selected.has(idx);
      });
    }

    let invalidRows = 0;
    let finalRows = dedup.map((r) => {
      const out = { ...r };
      Object.keys(casts).forEach((field) => {
        if (!(field in out)) return;
        const kind = String(casts[field] || "").toLowerCase();
        const val = out[field];
        if (kind === "int") {
          const n = Number(String(val).replace(/[\s,，$¥￥]/g, ""));
          out[field] = Number.isFinite(n) ? Math.trunc(n) : "";
        } else if (kind === "float") {
          const n = Number(String(val).replace(/[\s,，$¥￥]/g, ""));
          out[field] = Number.isFinite(n) ? Math.round(n * 100) / 100 : "";
        } else if (kind === "string") {
          out[field] = String(val ?? "");
        }
      });
      const amountKey = Object.keys(out).find((k) => k.toLowerCase() === "amount");
      if (amountKey) {
        const n = normalizeAmount(out[amountKey]);
        if (n === null && out[amountKey] !== "") invalidRows += 1;
        if (n !== null) out[amountKey] = n;
      }
      return out;
    });

    if (filters.length > 0) {
      const pass = (row, f) => {
        const field = String(f.field || "").trim();
        const op = String(f.op || "").toLowerCase();
        const val = row[field];
        const cmp = f.value;
        if (op === "eq") return String(val ?? "") === String(cmp ?? "");
        if (op === "ne") return String(val ?? "") !== String(cmp ?? "");
        if (op === "contains") return String(val ?? "").includes(String(cmp ?? ""));
        const n1 = Number(val);
        const n2 = Number(cmp);
        if (!Number.isFinite(n1) || !Number.isFinite(n2)) return true;
        if (op === "gte") return n1 >= n2;
        if (op === "lte") return n1 <= n2;
        if (op === "gt") return n1 > n2;
        if (op === "lt") return n1 < n2;
        return true;
      };
      const before = finalRows.length;
      finalRows = finalRows.filter((r) => filters.every((f) => pass(r, f)));
      filteredRows += Math.max(0, before - finalRows.length);
    }

    const inputRows = rawRows.length;
    const outputRows = finalRows.length;
    const duplicateRemoved = normalized.length - dedup.length;
    const quality = {
      input_rows: inputRows,
      output_rows: outputRows,
      filtered_rows: filteredRows,
      invalid_rows: invalidRows,
      duplicate_rows_removed: duplicateRemoved,
      rust_v2_used: false,
    };
    let eligibilityReason = "eligible";
    let shadowCompare = buildShadowCompare("skipped", "mode_off");
    if (strategy.decision === "force_python") {
      eligibilityReason = "forced_python";
      shadowCompare = buildShadowCompare("skipped", "forced_python");
    } else if (strategy.decision === "force_rust") {
      eligibilityReason = "rust_v2_unavailable";
      shadowCompare = buildShadowCompare("rust_error", "", ["desktop_local_rust_v2_unavailable"]);
    } else if (strategy.decision === "off") {
      eligibilityReason = "mode_off";
      shadowCompare = buildShadowCompare("skipped", "mode_off");
    } else if (strategy.decision === "shadow") {
      eligibilityReason = "eligible";
      shadowCompare = buildShadowCompare("skipped", "desktop_local_no_compare");
    } else if (strategy.decision === "default") {
      eligibilityReason = "rust_v2_unavailable";
      shadowCompare = buildShadowCompare("skipped", "default_without_verify");
    }
    return {
      rows: finalRows,
      quality,
      execution_mode: "python_legacy",
      execution_audit: {
        schema: "desktop_cleaning.audit.v1",
        reason_counts: {
          invalid_object: 0,
          cast_failed: invalidRows,
          required_missing: 0,
          filter_rejected: filteredRows,
          duplicate_removed: duplicateRemoved,
        },
        reason_samples: {
          invalid_object: [],
          cast_failed: [],
          required_missing: [],
          filter_rejected: [],
          duplicate_removed: [],
        },
      },
      eligibility_reason: eligibilityReason,
      requested_rust_v2_mode: String(strategy.requested_mode || ""),
      effective_rust_v2_mode: String(strategy.effective_mode || ""),
      verify_on_default: verifyOnDefault,
      shadow_compare: shadowCompare,
    };
  }

  function buildFidelityRows(rawRows = []) {
    const rows = [];
    const seen = new Set();
    (Array.isArray(rawRows) ? rawRows : []).forEach((r, idx) => {
      const text = String(r?.text ?? "").replace(/\s+/g, " ").trim();
      if (!text) return;
      const out = {
        source_file: String(r?.source_file || ""),
        source_type: String(r?.source_type || "raw_text"),
        row_no: Number.isFinite(Number(r?.row_no)) ? Number(r.row_no) : idx + 1,
        text,
      };
      const key = `${out.source_file}|${out.source_type}|${out.row_no}|${out.text}`;
      if (seen.has(key)) return;
      seen.add(key);
      rows.push(out);
    });
    return rows;
  }

  function normalizeProfile(value) {
    return String(value || "").trim().toLowerCase();
  }

  function uniqueStrings(values = []) {
    return Array.from(new Set((Array.isArray(values) ? values : []).map((x) => String(x || "").trim()).filter(Boolean)));
  }

  function isTemplateDriven(params = {}) {
    const templateId = String(params.cleaning_template || "").trim().toLowerCase();
    if (templateId && templateId !== "default") return true;
    if (params.cleaning_spec_v2 && typeof params.cleaning_spec_v2 === "object") return true;
    if (normalizeProfile(params.template_expected_profile || params.canonical_profile)) return true;
    return false;
  }

  function resolveRequestedProfile(params = {}, ruleObj = {}) {
    return normalizeProfile(
      params.template_expected_profile
      || params.canonical_profile
      || params?.cleaning_spec_v2?.schema?.template_expected_profile
      || params?.cleaning_spec_v2?.schema?.canonical_profile
      || ruleObj.template_expected_profile
      || ruleObj.canonical_profile
      || params?.quality_rules?.canonical_profile
      || params?.rules?.canonical_profile
    );
  }

  function resolveBlankOutputExpected(params = {}) {
    if (params.blank_output_expected !== undefined) return toBool(params.blank_output_expected, false);
    const explicitBlank = ruleParam(params, "blank_output_expected");
    if (explicitBlank !== undefined) return toBool(explicitBlank, false);
    const allowEmpty = ruleParam(params, "allow_empty_output");
    if (allowEmpty !== undefined) return toBool(allowEmpty, true);
    return !(isTemplateDriven(params) || toBool(params.local_standalone, false));
  }

  function resolveProfileMismatchAction(params = {}) {
    const explicit = String(params.profile_mismatch_action || "").trim().toLowerCase();
    if (explicit === "warn" || explicit === "block") return explicit;
    return (isTemplateDriven(params) || toBool(params.local_standalone, false)) ? "block" : "warn";
  }

  function findRecommendedCandidate(candidateProfiles = []) {
    return (Array.isArray(candidateProfiles) ? candidateProfiles : []).find((item) => item && item.recommended && String(item.profile || "").trim()) || null;
  }

  function findCandidateByProfile(candidateProfiles = [], profile = "") {
    const target = normalizeProfile(profile);
    if (!target) return null;
    return (Array.isArray(candidateProfiles) ? candidateProfiles : []).find((item) => normalizeProfile(item?.profile) === target) || null;
  }

  function buildProfileRecommendationSummary(params = {}, candidateProfiles = []) {
    const requestedProfile = resolveRequestedProfile(params);
    const requestedCandidate = findCandidateByProfile(candidateProfiles, requestedProfile);
    const recommendedCandidate = findRecommendedCandidate(candidateProfiles);
    const recommendedProfile = normalizeProfile(recommendedCandidate?.profile);
    const recommendedTemplateId = String(recommendedCandidate?.recommended_template_id || "").trim();
    const profileConfidence = Number(recommendedCandidate?.score || 0);
    const requestedCoverage = Number(requestedCandidate?.required_coverage || 0);
    const recommendedCoverage = Number(recommendedCandidate?.required_coverage || 0);
    const mismatchAction = resolveProfileMismatchAction(params);
    const profileMismatch = !!(requestedProfile && recommendedProfile && requestedProfile !== recommendedProfile);
    const profileMismatchBlocked = !!(
      profileMismatch
      && recommendedCandidate
      && mismatchAction === "block"
      && profileConfidence >= 0.85
      && recommendedCoverage >= 0.75
      && requestedCoverage <= 0.25
    );
    const profileMismatchWarn = !!(profileMismatch && !profileMismatchBlocked);
    return {
      requested_profile: requestedProfile,
      recommended_profile: recommendedProfile,
      recommended_template_id: recommendedTemplateId,
      profile_confidence: Number(profileConfidence.toFixed(6)),
      profile_mismatch: profileMismatch,
      profile_mismatch_blocked: profileMismatchBlocked,
      profile_mismatch_warn: profileMismatchWarn,
      requested_profile_required_coverage: Number(requestedCoverage.toFixed(6)),
      recommended_profile_required_coverage: Number(recommendedCoverage.toFixed(6)),
      recommendation_signal_available: !!recommendedCandidate,
    };
  }

  function precheckRows(rawRows, params = {}, runtime = {}) {
    const ruleObj = resolveRuleObject(params);
    const renameMap = normalizeRuleMap(ruleObj.rename_map);
    const requiredFields = normalizeRuleArray(ruleObj.required_fields).map((x) => String(x).trim()).filter(Boolean);
    const mappedRows = rawRows.map((r) => mapRowByRename(r, renameMap));
    const sidecarExtracts = Array.isArray(runtime?.sidecarExtractResults) ? runtime.sidecarExtractResults : [];
    const sidecarPayload = sidecarExtracts.length > 0 && sidecarExtracts[0] && typeof sidecarExtracts[0].payload === "object"
      ? sidecarExtracts[0].payload
      : null;
    const headers = new Set();
    mappedRows.forEach((r) => Object.keys(r || {}).forEach((k) => headers.add(String(k).trim())));
    const missingRequired = requiredFields.filter((f) => !headers.has(f));

    let amountField = "";
    if (headers.has("amount")) {
      amountField = "amount";
    } else {
      const castMap = normalizeRuleMap(ruleObj.casts);
      const castFloatFields = Object.keys(castMap).filter((k) => String(castMap[k]).toLowerCase() === "float");
      amountField = castFloatFields.find((k) => headers.has(k)) || "";
    }
    if (!amountField) {
      const firstAmountLike = Array.from(headers).find((k) => /amount|amt|金额|金額/i.test(k));
      if (firstAmountLike) amountField = firstAmountLike;
    }

    let amountNonEmpty = 0;
    let amountConvertible = 0;
    let amountInvalid = 0;
    const amountInvalidSamples = [];
    if (amountField) {
      mappedRows.forEach((r) => {
        const raw = r[amountField];
        const s = String(raw ?? "").trim();
        if (!s) return;
        amountNonEmpty += 1;
        const n = normalizeAmount(raw);
        if (n === null) {
          amountInvalid += 1;
          if (amountInvalidSamples.length < 10) {
            amountInvalidSamples.push({
              row_no: r.row_no || "",
              source_file: r.source_file || "",
              raw_value: s,
            });
          }
        } else {
          amountConvertible += 1;
        }
      });
    }

    const cleaned = cleanRows(rawRows, params);
    const q = cleaned.quality || {};
    const inputRows = Number(q.input_rows || rawRows.length || 0);
    const invalidRows = Number(q.invalid_rows || 0);
    const invalidRatio = inputRows > 0 ? invalidRows / inputRows : 0;
    const amountConvertRate = amountNonEmpty > 0 ? amountConvertible / amountNonEmpty : 1;
    const minAmountConvertRate = toFloatOrNull(ruleParam(params, "precheck_amount_convert_rate_min"));
    const requiredAmountConvertRate = minAmountConvertRate === null ? 0.9 : minAmountConvertRate;
    let qualityGateOk = true;
    let qualityGateError = "";
    try {
      applyQualityGates(q, params);
    } catch (e) {
      qualityGateOk = false;
      qualityGateError = String(e && e.message ? e.message : e);
    }

    const blankOutputExpected = resolveBlankOutputExpected(params);
    const predictedZeroOutputUnexpected = Number(q.output_rows || 0) <= 0 && !blankOutputExpected;
    if (predictedZeroOutputUnexpected) {
      qualityGateOk = false;
      if (!qualityGateError) {
        qualityGateError = "quality gate failed: output_rows=0 while blank_output_expected=false";
      }
    }

    const blockingIssues = [];
    const warningIssues = [];
    const textRows = mappedRows
      .map((r) => String(r?.text || "").trim())
      .filter((t) => t.length > 0);
    const longTextRows = textRows.filter((t) => t.length >= 40);
    const coherentRows = longTextRows.filter((t) => /[。！？!?；;.]$/.test(t) || t.length >= 90);
    const contentPrecheck = {
      text_row_ratio: mappedRows.length > 0 ? Number((textRows.length / mappedRows.length).toFixed(6)) : 0,
      long_paragraph_ratio: textRows.length > 0 ? Number((longTextRows.length / textRows.length).toFixed(6)) : 0,
      coherent_paragraph_ratio: longTextRows.length > 0 ? Number((coherentRows.length / longTextRows.length).toFixed(6)) : 0,
    };
    if (missingRequired.length > 0) {
      blockingIssues.push(`缺少必填字段: ${missingRequired.join("、")}`);
    }
    if (amountNonEmpty > 0 && amountConvertRate < requiredAmountConvertRate) {
      blockingIssues.push(`金额列可转换率偏低: ${(amountConvertRate * 100).toFixed(1)}%`);
    }
    if (!qualityGateOk && qualityGateError) {
      blockingIssues.push(`质量门禁预检未通过: ${qualityGateError}`);
    }
    const sidecarBlockedReasonCodes = sidecarPayload && Array.isArray(sidecarPayload.blocked_reason_codes)
      ? sidecarPayload.blocked_reason_codes.map((x) => String(x)).filter(Boolean)
      : [];
    const candidateProfiles = sidecarPayload && Array.isArray(sidecarPayload.candidate_profiles) ? sidecarPayload.candidate_profiles : [];
    const recommendation = buildProfileRecommendationSummary(params, candidateProfiles);
    const sidecarQualityBlocked = !!(sidecarPayload && sidecarPayload.quality_blocked);
    if (sidecarQualityBlocked) {
      const sidecarBlockedText = sidecarBlockedReasonCodes.length > 0
        ? sidecarBlockedReasonCodes.join("、")
        : "输入质量门禁阻断";
      blockingIssues.unshift(`输入质量门禁阻断: ${sidecarBlockedText}`);
      qualityGateOk = false;
      if (!qualityGateError) qualityGateError = sidecarBlockedText;
    }
    if (recommendation.profile_mismatch_blocked) {
      blockingIssues.unshift(`模板画像不匹配: ${recommendation.requested_profile} -> ${recommendation.recommended_profile}`);
    } else if (recommendation.profile_mismatch_warn) {
      warningIssues.push(`模板画像可能不匹配: ${recommendation.requested_profile} -> ${recommendation.recommended_profile}`);
    } else if (recommendation.requested_profile && !recommendation.recommendation_signal_available) {
      warningIssues.push("推荐信号不足，正式运行仍可能触发运行时挡板。");
    }
    if (predictedZeroOutputUnexpected) {
      blockingIssues.push("将产生空结果，且当前模板不允许空输出。");
    }
    const contentGateEnabled = toBool(ruleParam(params, "precheck_content_gate_enabled"), false);
    if (contentGateEnabled && contentPrecheck.long_paragraph_ratio < 0.2) {
      blockingIssues.push(`正文段落比例偏低: ${(contentPrecheck.long_paragraph_ratio * 100).toFixed(1)}%`);
    }
    if (contentGateEnabled && contentPrecheck.coherent_paragraph_ratio < 0.5) {
      blockingIssues.push(`段落连贯性偏低: ${(contentPrecheck.coherent_paragraph_ratio * 100).toFixed(1)}%`);
    }

    const suggestions = [];
    if (missingRequired.length > 0) {
      suggestions.push("请补齐模板必填字段或调整 rename_map 字段映射。");
    }
    if (amountNonEmpty > 0 && amountConvertRate < requiredAmountConvertRate) {
      suggestions.push("请清理金额列中的单位、符号和非数字字符后重试。");
    }
    if (!qualityGateOk) {
      suggestions.push("请先修复预检问题，再执行正式生成。");
    }
    if (recommendation.recommended_template_id) {
      suggestions.push(`推荐模板: ${recommendation.recommended_template_id}`);
    }
    if (contentPrecheck.long_paragraph_ratio < 0.2 || contentPrecheck.coherent_paragraph_ratio < 0.5) {
      suggestions.push("建议先进行论文正文抽取（去注释/去参考文献）后再做 Office 产物。");
    }
    if (!recommendation.recommendation_signal_available && recommendation.requested_profile) {
      suggestions.push("当前缺少模板推荐信号，正式运行仍可能触发运行时挡板。");
    }
    const issues = [...blockingIssues, ...warningIssues];
    const blockingReasonCodes = uniqueStrings([
      ...sidecarBlockedReasonCodes,
      ...(recommendation.profile_mismatch_blocked ? ["profile_mismatch"] : []),
      ...(predictedZeroOutputUnexpected ? ["zero_output_unexpected"] : []),
    ]);
    const precheckAction = blockingIssues.length > 0 ? "block" : (warningIssues.length > 0 ? "warn" : "allow");
    if (issues.length === 0) {
      suggestions.push("预检通过，可以直接执行开始生成。");
    }

    return {
      source: sidecarPayload ? "glue_sidecar" : "local",
      input_rows: rawRows.length,
      headers: sidecarPayload && Array.isArray(sidecarPayload.header_mapping)
        ? sidecarPayload.header_mapping.map((item) => String(item.raw_header || item.canonical_field || "")).filter(Boolean)
        : Array.from(headers).sort(),
      required_fields: requiredFields,
      missing_required_fields: missingRequired,
      amount_field: amountField,
      amount_non_empty: amountNonEmpty,
      amount_convertible: amountConvertible,
      amount_invalid: amountInvalid,
      amount_invalid_samples: amountInvalidSamples,
      amount_convert_rate: Number(amountConvertRate.toFixed(6)),
      amount_convert_rate_required: Number(requiredAmountConvertRate.toFixed(6)),
      invalid_ratio: Number(invalidRatio.toFixed(6)),
      quality: q,
      content_precheck: contentPrecheck,
      content_gate_enabled: contentGateEnabled,
      quality_gate_ok: qualityGateOk,
      quality_gate_error: qualityGateError,
      ok: blockingIssues.length === 0,
      requested_profile: recommendation.requested_profile,
      recommended_profile: recommendation.recommended_profile,
      recommended_template_id: recommendation.recommended_template_id,
      profile_confidence: recommendation.profile_confidence,
      profile_mismatch: recommendation.profile_mismatch,
      precheck_action: precheckAction,
      predicted_zero_output_unexpected: predictedZeroOutputUnexpected,
      blank_output_expected: blankOutputExpected,
      header_mapping: sidecarPayload && Array.isArray(sidecarPayload.header_mapping) ? sidecarPayload.header_mapping : [],
      candidate_profiles: candidateProfiles,
      quality_decisions: sidecarPayload && Array.isArray(sidecarPayload.quality_decisions) ? sidecarPayload.quality_decisions : [],
      blocked_reason_codes: sidecarBlockedReasonCodes,
      blocking_reason_codes: blockingReasonCodes,
      sample_rows: sidecarPayload && Array.isArray(sidecarPayload.sample_rows) ? sidecarPayload.sample_rows : rawRows.slice(0, 5),
      issues,
      suggestions,
    };
  }

  function ruleParam(params, key) {
    const p = (params && typeof params === "object") ? params : {};
    if (p.cleaning_spec_v2 && typeof p.cleaning_spec_v2 === "object") {
      const derived = deriveLegacyRulesFromCleaningSpec(p.cleaning_spec_v2);
      if (derived[key] !== undefined) return derived[key];
    }
    if (p.quality_rules && typeof p.quality_rules === "object" && p.quality_rules[key] !== undefined) return p.quality_rules[key];
    if (p.rules && typeof p.rules === "object" && p.rules[key] !== undefined) return p.rules[key];
    return p[key];
  }

  function toIntOrNull(v) {
    if (v === null || v === undefined || v === "") return null;
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    return Math.trunc(n);
  }

  function toFloatOrNull(v) {
    if (v === null || v === undefined || v === "") return null;
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    return n;
  }

  function toBool(v, def = false) {
    if (v === null || v === undefined) return def;
    if (typeof v === "boolean") return v;
    const s = String(v).trim().toLowerCase();
    if (!s) return def;
    return ["1", "true", "yes", "y", "on"].includes(s);
  }

  function applyQualityGates(quality, params = {}) {
    const inputRows = Number(quality?.input_rows || 0);
    const outputRows = Number(quality?.output_rows || 0);
    const invalidRows = Number(quality?.invalid_rows || 0);
    const filteredRows = Number(quality?.filtered_rows || 0);

    const maxInvalidRows = toIntOrNull(ruleParam(params, "max_invalid_rows"));
    const maxFilteredRows = toIntOrNull(ruleParam(params, "max_filtered_rows"));
    const minOutputRows = toIntOrNull(ruleParam(params, "min_output_rows"));
    const maxInvalidRatio = toFloatOrNull(ruleParam(params, "max_invalid_ratio"));
    const allowEmpty = toBool(ruleParam(params, "allow_empty_output"), true);

    if (maxInvalidRows !== null && invalidRows > maxInvalidRows) {
      throw new Error(`quality gate failed: invalid_rows=${invalidRows} exceeds max_invalid_rows=${maxInvalidRows}`);
    }
    if (maxFilteredRows !== null && filteredRows > maxFilteredRows) {
      throw new Error(`quality gate failed: filtered_rows=${filteredRows} exceeds max_filtered_rows=${maxFilteredRows}`);
    }
    if (minOutputRows !== null && outputRows < minOutputRows) {
      throw new Error(`quality gate failed: output_rows=${outputRows} below min_output_rows=${minOutputRows}`);
    }
    if (maxInvalidRatio !== null) {
      const ratio = inputRows > 0 ? (invalidRows / inputRows) : 0;
      if (ratio > maxInvalidRatio) {
        throw new Error(`quality gate failed: invalid_ratio=${ratio.toFixed(6)} exceeds max_invalid_ratio=${Number(maxInvalidRatio).toFixed(6)}`);
      }
    }
    if (!allowEmpty && outputRows <= 0) {
      throw new Error("quality gate failed: output_rows=0 while allow_empty_output=false");
    }
    return {
      evaluated: true,
      max_invalid_rows: maxInvalidRows,
      max_filtered_rows: maxFilteredRows,
      min_output_rows: minOutputRows,
      max_invalid_ratio: maxInvalidRatio,
      allow_empty_output: allowEmpty,
      passed: true,
    };
  }

  return {
    readInputRows,
    cleanRows,
    buildFidelityRows,
    precheckRows,
    applyQualityGates,
  };
}

module.exports = {
  createOfflineIngest,
};
