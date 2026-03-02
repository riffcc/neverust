//! Primitive composition analysis for blockstore tuning.
//!
//! This module provides a Rust-native tradeoff matrix calculator and
//! auto-improver for primitive combinations (hashing, indexing, layout,
//! locking, commit strategy, and similar dimensions).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::Path;

const THROUGHPUT_COL: &str = "throughput_mibps";
const P99_COL: &str = "p99_ms";
const CPU_COL: &str = "cpu_pct";
const MEM_COL: &str = "mem_mb";
const WRITE_AMP_COL: &str = "write_amp";
const READ_AMP_COL: &str = "read_amp";
const DURABILITY_COL: &str = "durability_score";
const CORRECTNESS_FAIL_COL: &str = "correctness_failures";
const REORDER_FAIL_COL: &str = "reorder_violations";
const GC_FAIL_COL: &str = "gc_violations";

#[derive(Debug)]
pub enum PrimitiveLabError {
    Io(std::io::Error),
    Format(String),
}

impl Display for PrimitiveLabError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Format(msg) => write!(f, "format error: {msg}"),
        }
    }
}

impl Error for PrimitiveLabError {}

impl From<std::io::Error> for PrimitiveLabError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RunMetrics {
    pub throughput_mibps: f64,
    pub p99_ms: f64,
    pub cpu_pct: f64,
    pub mem_mb: f64,
    pub write_amp: f64,
    pub read_amp: f64,
    pub durability_score: f64,
    pub correctness_failures: u64,
    pub reorder_violations: u64,
    pub gc_violations: u64,
}

#[derive(Debug, Clone)]
pub struct RunRecord {
    pub run_id: String,
    pub primitives: BTreeMap<String, String>,
    pub metrics: RunMetrics,
}

#[derive(Debug, Clone, Copy)]
pub struct ScoreWeights {
    pub throughput: f64,
    pub durability: f64,
    pub p99: f64,
    pub cpu: f64,
    pub mem: f64,
    pub write_amp: f64,
    pub read_amp: f64,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        Self {
            throughput: 0.35,
            durability: 0.20,
            p99: 0.15,
            cpu: 0.10,
            mem: 0.10,
            write_amp: 0.05,
            read_amp: 0.05,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScoredRun {
    pub record: RunRecord,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct TradeoffCell {
    pub primitive: String,
    pub option: String,
    pub samples: usize,
    pub avg_score: f64,
    pub avg_throughput_mibps: f64,
    pub avg_p99_ms: f64,
    pub avg_cpu_pct: f64,
    pub avg_mem_mb: f64,
    pub failure_rate: f64,
}

#[derive(Debug, Clone)]
pub struct CandidateSuggestion {
    pub composition: BTreeMap<String, String>,
    pub predicted_score: f64,
    pub rationale: String,
}

#[derive(Debug, Clone)]
pub struct AnalysisReport {
    pub scored_runs: Vec<ScoredRun>,
    pub pareto_frontier: Vec<ScoredRun>,
    pub tradeoff_matrix: BTreeMap<String, Vec<TradeoffCell>>,
    pub suggestions: Vec<CandidateSuggestion>,
}

#[derive(Debug, Clone, Copy)]
struct Range {
    min: f64,
    max: f64,
}

impl Range {
    fn empty() -> Self {
        Self {
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    fn push(&mut self, value: f64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    fn normalize_maximize(&self, value: f64) -> f64 {
        let span = self.max - self.min;
        if span.abs() <= f64::EPSILON {
            1.0
        } else {
            ((value - self.min) / span).clamp(0.0, 1.0)
        }
    }

    fn normalize_minimize(&self, value: f64) -> f64 {
        let span = self.max - self.min;
        if span.abs() <= f64::EPSILON {
            1.0
        } else {
            ((self.max - value) / span).clamp(0.0, 1.0)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MetricRanges {
    throughput: Range,
    p99: Range,
    cpu: Range,
    mem: Range,
    write_amp: Range,
    read_amp: Range,
    durability: Range,
}

fn metric_ranges(records: &[RunRecord]) -> MetricRanges {
    let mut ranges = MetricRanges {
        throughput: Range::empty(),
        p99: Range::empty(),
        cpu: Range::empty(),
        mem: Range::empty(),
        write_amp: Range::empty(),
        read_amp: Range::empty(),
        durability: Range::empty(),
    };

    for r in records {
        ranges.throughput.push(r.metrics.throughput_mibps);
        ranges.p99.push(r.metrics.p99_ms);
        ranges.cpu.push(r.metrics.cpu_pct);
        ranges.mem.push(r.metrics.mem_mb);
        ranges.write_amp.push(r.metrics.write_amp);
        ranges.read_amp.push(r.metrics.read_amp);
        ranges.durability.push(r.metrics.durability_score);
    }
    ranges
}

fn is_metric_column(name: &str) -> bool {
    matches!(
        name,
        THROUGHPUT_COL
            | P99_COL
            | CPU_COL
            | MEM_COL
            | WRITE_AMP_COL
            | READ_AMP_COL
            | DURABILITY_COL
            | CORRECTNESS_FAIL_COL
            | REORDER_FAIL_COL
            | GC_FAIL_COL
    )
}

fn parse_f64(value: Option<&str>) -> f64 {
    value.unwrap_or("").trim().parse::<f64>().unwrap_or(0.0)
}

fn parse_u64(value: Option<&str>) -> u64 {
    let raw = value.unwrap_or("").trim();
    if raw.is_empty() {
        return 0;
    }
    if let Ok(v) = raw.parse::<u64>() {
        return v;
    }
    raw.parse::<f64>()
        .map(|v| if v.is_sign_negative() { 0 } else { v as u64 })
        .unwrap_or(0)
}

fn csv_split_line(line: &str) -> Result<Vec<String>, PrimitiveLabError> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes {
                    if matches!(chars.peek(), Some('"')) {
                        field.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else if field.trim().is_empty() {
                    in_quotes = true;
                    field.clear();
                } else {
                    field.push('"');
                }
            }
            ',' if !in_quotes => {
                fields.push(field.trim().to_string());
                field.clear();
            }
            _ => field.push(ch),
        }
    }
    if in_quotes {
        return Err(PrimitiveLabError::Format(
            "unterminated CSV quote".to_string(),
        ));
    }
    fields.push(field.trim().to_string());
    Ok(fields)
}

pub fn load_runs_from_csv<P: AsRef<Path>>(path: P) -> Result<Vec<RunRecord>, PrimitiveLabError> {
    let content = fs::read_to_string(path)?;
    load_runs_from_csv_str(&content)
}

pub fn load_runs_from_csv_str(content: &str) -> Result<Vec<RunRecord>, PrimitiveLabError> {
    let mut lines = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'));

    let header_line = lines
        .next()
        .ok_or_else(|| PrimitiveLabError::Format("missing CSV header".to_string()))?;
    let header = csv_split_line(header_line)?;
    if header.is_empty() {
        return Err(PrimitiveLabError::Format(
            "CSV header has zero columns".to_string(),
        ));
    }

    let mut seen = HashSet::new();
    for h in &header {
        if !seen.insert(h.clone()) {
            return Err(PrimitiveLabError::Format(format!(
                "duplicate CSV header column: {h}"
            )));
        }
    }

    let has_prefixed_primitive_columns = header.iter().any(|h| h.starts_with("p_"));
    let mut runs = Vec::new();

    for (idx, raw_line) in lines.enumerate() {
        let row = csv_split_line(raw_line)?;
        if row.len() != header.len() {
            return Err(PrimitiveLabError::Format(format!(
                "row {} has {} columns but header has {}",
                idx + 2,
                row.len(),
                header.len()
            )));
        }

        let mut map = HashMap::new();
        for (k, v) in header.iter().zip(row.iter()) {
            map.insert(k.as_str(), v.as_str());
        }

        let run_id = map
            .get("run_id")
            .or_else(|| map.get("id"))
            .copied()
            .filter(|v| !v.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("run-{:06}", idx + 1));

        let mut primitives = BTreeMap::new();
        for h in &header {
            if h == "run_id" || h == "id" || is_metric_column(h) {
                continue;
            }
            if has_prefixed_primitive_columns && !h.starts_with("p_") {
                continue;
            }
            let key = if let Some(stripped) = h.strip_prefix("p_") {
                stripped
            } else {
                h.as_str()
            };
            let value = map.get(h.as_str()).copied().unwrap_or("").to_string();
            primitives.insert(key.to_string(), value);
        }

        let metrics = RunMetrics {
            throughput_mibps: parse_f64(map.get(THROUGHPUT_COL).copied()),
            p99_ms: parse_f64(map.get(P99_COL).copied()),
            cpu_pct: parse_f64(map.get(CPU_COL).copied()),
            mem_mb: parse_f64(map.get(MEM_COL).copied()),
            write_amp: parse_f64(map.get(WRITE_AMP_COL).copied()),
            read_amp: parse_f64(map.get(READ_AMP_COL).copied()),
            durability_score: parse_f64(map.get(DURABILITY_COL).copied()),
            correctness_failures: parse_u64(map.get(CORRECTNESS_FAIL_COL).copied()),
            reorder_violations: parse_u64(map.get(REORDER_FAIL_COL).copied()),
            gc_violations: parse_u64(map.get(GC_FAIL_COL).copied()),
        };

        runs.push(RunRecord {
            run_id,
            primitives,
            metrics,
        });
    }

    if runs.is_empty() {
        return Err(PrimitiveLabError::Format(
            "CSV contains no data rows".to_string(),
        ));
    }

    Ok(runs)
}

pub fn primitive_columns(records: &[RunRecord]) -> Vec<String> {
    let mut out = BTreeSet::new();
    for record in records {
        for key in record.primitives.keys() {
            out.insert(key.clone());
        }
    }
    out.into_iter().collect()
}

pub fn score_runs(records: &[RunRecord], weights: ScoreWeights) -> Vec<ScoredRun> {
    let ranges = metric_ranges(records);

    records
        .iter()
        .cloned()
        .map(|record| {
            let m = record.metrics;
            let mut score = 0.0f64;
            score += weights.throughput * ranges.throughput.normalize_maximize(m.throughput_mibps);
            score += weights.durability * ranges.durability.normalize_maximize(m.durability_score);
            score += weights.p99 * ranges.p99.normalize_minimize(m.p99_ms);
            score += weights.cpu * ranges.cpu.normalize_minimize(m.cpu_pct);
            score += weights.mem * ranges.mem.normalize_minimize(m.mem_mb);
            score += weights.write_amp * ranges.write_amp.normalize_minimize(m.write_amp);
            score += weights.read_amp * ranges.read_amp.normalize_minimize(m.read_amp);

            let hard_failures = m.correctness_failures + m.reorder_violations + m.gc_violations;
            if hard_failures > 0 {
                score *= 0.05;
            }
            if m.durability_score <= 0.0 {
                score *= 0.80;
            }

            ScoredRun { record, score }
        })
        .collect()
}

fn dominates(a: &RunMetrics, b: &RunMetrics) -> bool {
    const EPS: f64 = 1e-12;
    let mut strictly_better = false;

    let ge = |x: f64, y: f64| x + EPS >= y;
    let le = |x: f64, y: f64| x <= y + EPS;
    let gt = |x: f64, y: f64| x > y + EPS;
    let lt = |x: f64, y: f64| x + EPS < y;

    if !ge(a.throughput_mibps, b.throughput_mibps) {
        return false;
    }
    if gt(a.throughput_mibps, b.throughput_mibps) {
        strictly_better = true;
    }

    if !ge(a.durability_score, b.durability_score) {
        return false;
    }
    if gt(a.durability_score, b.durability_score) {
        strictly_better = true;
    }

    if !le(a.p99_ms, b.p99_ms) {
        return false;
    }
    if lt(a.p99_ms, b.p99_ms) {
        strictly_better = true;
    }

    if !le(a.cpu_pct, b.cpu_pct) {
        return false;
    }
    if lt(a.cpu_pct, b.cpu_pct) {
        strictly_better = true;
    }

    if !le(a.mem_mb, b.mem_mb) {
        return false;
    }
    if lt(a.mem_mb, b.mem_mb) {
        strictly_better = true;
    }

    if !le(a.write_amp, b.write_amp) {
        return false;
    }
    if lt(a.write_amp, b.write_amp) {
        strictly_better = true;
    }

    if !le(a.read_amp, b.read_amp) {
        return false;
    }
    if lt(a.read_amp, b.read_amp) {
        strictly_better = true;
    }

    if a.correctness_failures > b.correctness_failures {
        return false;
    }
    if a.correctness_failures < b.correctness_failures {
        strictly_better = true;
    }

    if a.reorder_violations > b.reorder_violations {
        return false;
    }
    if a.reorder_violations < b.reorder_violations {
        strictly_better = true;
    }

    if a.gc_violations > b.gc_violations {
        return false;
    }
    if a.gc_violations < b.gc_violations {
        strictly_better = true;
    }

    strictly_better
}

pub fn pareto_frontier(scored_runs: &[ScoredRun]) -> Vec<ScoredRun> {
    let mut frontier = Vec::new();
    for (i, candidate) in scored_runs.iter().enumerate() {
        let dominated = scored_runs.iter().enumerate().any(|(j, other)| {
            i != j && dominates(&other.record.metrics, &candidate.record.metrics)
        });
        if !dominated {
            frontier.push(candidate.clone());
        }
    }

    frontier.sort_by(|a, b| b.score.total_cmp(&a.score));
    frontier
}

#[derive(Debug, Default)]
struct CellAccumulator {
    samples: usize,
    sum_score: f64,
    sum_throughput: f64,
    sum_p99: f64,
    sum_cpu: f64,
    sum_mem: f64,
    fail_samples: usize,
}

fn has_hard_failures(metrics: &RunMetrics) -> bool {
    (metrics.correctness_failures + metrics.reorder_violations + metrics.gc_violations) > 0
}

pub fn build_tradeoff_matrix(scored_runs: &[ScoredRun]) -> BTreeMap<String, Vec<TradeoffCell>> {
    let mut acc: BTreeMap<String, BTreeMap<String, CellAccumulator>> = BTreeMap::new();

    for scored in scored_runs {
        for (primitive, option) in &scored.record.primitives {
            let entry = acc
                .entry(primitive.clone())
                .or_default()
                .entry(option.clone())
                .or_default();
            entry.samples += 1;
            entry.sum_score += scored.score;
            entry.sum_throughput += scored.record.metrics.throughput_mibps;
            entry.sum_p99 += scored.record.metrics.p99_ms;
            entry.sum_cpu += scored.record.metrics.cpu_pct;
            entry.sum_mem += scored.record.metrics.mem_mb;
            if has_hard_failures(&scored.record.metrics) {
                entry.fail_samples += 1;
            }
        }
    }

    let mut matrix = BTreeMap::new();
    for (primitive, options) in acc {
        let mut cells = Vec::new();
        for (option, c) in options {
            let samples = c.samples.max(1) as f64;
            cells.push(TradeoffCell {
                primitive: primitive.clone(),
                option,
                samples: c.samples,
                avg_score: c.sum_score / samples,
                avg_throughput_mibps: c.sum_throughput / samples,
                avg_p99_ms: c.sum_p99 / samples,
                avg_cpu_pct: c.sum_cpu / samples,
                avg_mem_mb: c.sum_mem / samples,
                failure_rate: c.fail_samples as f64 / samples,
            });
        }
        cells.sort_by(|a, b| {
            b.avg_score
                .total_cmp(&a.avg_score)
                .then_with(|| b.avg_throughput_mibps.total_cmp(&a.avg_throughput_mibps))
        });
        matrix.insert(primitive, cells);
    }

    matrix
}

fn composition_fingerprint(composition: &BTreeMap<String, String>) -> String {
    composition
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("|")
}

fn option_score_lookup(
    matrix: &BTreeMap<String, Vec<TradeoffCell>>,
) -> HashMap<(String, String), (f64, usize)> {
    let mut lookup = HashMap::new();
    for (primitive, cells) in matrix {
        for cell in cells {
            lookup.insert(
                (primitive.clone(), cell.option.clone()),
                (cell.avg_score, cell.samples),
            );
        }
    }
    lookup
}

fn predicted_score(
    composition: &BTreeMap<String, String>,
    lookup: &HashMap<(String, String), (f64, usize)>,
) -> f64 {
    let mut sum = 0.0;
    let mut count = 0usize;
    for (primitive, option) in composition {
        if let Some((score, _)) = lookup.get(&(primitive.clone(), option.clone())) {
            sum += *score;
            count += 1;
        }
    }
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}

pub fn suggest_compositions(
    scored_runs: &[ScoredRun],
    matrix: &BTreeMap<String, Vec<TradeoffCell>>,
    max_suggestions: usize,
) -> Vec<CandidateSuggestion> {
    if scored_runs.is_empty() || matrix.is_empty() || max_suggestions == 0 {
        return Vec::new();
    }

    let mut sorted_runs = scored_runs.to_vec();
    sorted_runs.sort_by(|a, b| b.score.total_cmp(&a.score));

    let existing: HashSet<String> = scored_runs
        .iter()
        .map(|r| composition_fingerprint(&r.record.primitives))
        .collect();
    let mut emitted = HashSet::new();

    let lookup = option_score_lookup(matrix);

    let mut candidates = Vec::new();
    let mut best = BTreeMap::new();
    for (primitive, options) in matrix {
        if let Some(top) = options.first() {
            best.insert(primitive.clone(), top.option.clone());
        }
    }

    let mut add_candidate = |composition: BTreeMap<String, String>, rationale: String| {
        if composition.is_empty() {
            return;
        }
        let fp = composition_fingerprint(&composition);
        if existing.contains(&fp) || emitted.contains(&fp) {
            return;
        }
        emitted.insert(fp);
        let predicted = predicted_score(&composition, &lookup);
        candidates.push(CandidateSuggestion {
            composition,
            predicted_score: predicted,
            rationale,
        });
    };

    add_candidate(
        best.clone(),
        "baseline from top-scoring option per primitive".to_string(),
    );

    for (primitive, options) in matrix {
        if options.len() < 2 {
            continue;
        }
        let mut variant = best.clone();
        variant.insert(primitive.clone(), options[1].option.clone());
        add_candidate(
            variant,
            format!("one-axis mutation: second-best option for primitive `{primitive}`"),
        );
    }

    let elite: Vec<&ScoredRun> = sorted_runs
        .iter()
        .filter(|r| !has_hard_failures(&r.record.metrics))
        .take(4)
        .collect();
    if elite.len() >= 2 {
        let keys: Vec<String> = matrix.keys().cloned().collect();
        for i in 0..elite.len() {
            for j in (i + 1)..elite.len() {
                let mut crossed = BTreeMap::new();
                for (idx, key) in keys.iter().enumerate() {
                    let lhs = elite[i].record.primitives.get(key);
                    let rhs = elite[j].record.primitives.get(key);
                    let pick = if idx % 2 == 0 { lhs } else { rhs }
                        .or(lhs)
                        .or(rhs)
                        .cloned()
                        .unwrap_or_default();
                    crossed.insert(key.clone(), pick);
                }
                add_candidate(
                    crossed,
                    format!(
                        "crossover from elite runs `{}` and `{}`",
                        elite[i].record.run_id, elite[j].record.run_id
                    ),
                );
            }
        }
    }

    for (primitive, options) in matrix {
        if options.is_empty() {
            continue;
        }
        if let Some(least_seen) = options.iter().min_by_key(|c| c.samples) {
            let mut explore = best.clone();
            explore.insert(primitive.clone(), least_seen.option.clone());
            add_candidate(
                explore,
                format!(
                    "exploration candidate: least-tested option `{}` for primitive `{primitive}`",
                    least_seen.option
                ),
            );
        }
    }

    candidates.sort_by(|a, b| b.predicted_score.total_cmp(&a.predicted_score));
    candidates.truncate(max_suggestions);
    candidates
}

pub fn analyze_runs(
    records: &[RunRecord],
    weights: ScoreWeights,
    max_suggestions: usize,
) -> AnalysisReport {
    let mut scored_runs = score_runs(records, weights);
    scored_runs.sort_by(|a, b| b.score.total_cmp(&a.score));

    let pareto_frontier = pareto_frontier(&scored_runs);
    let tradeoff_matrix = build_tradeoff_matrix(&scored_runs);
    let suggestions = suggest_compositions(&scored_runs, &tradeoff_matrix, max_suggestions);

    AnalysisReport {
        scored_runs,
        pareto_frontier,
        tradeoff_matrix,
        suggestions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_csv() -> &'static str {
        "run_id,p_hash,p_index,p_layout,p_commit,p_lock,throughput_mibps,p99_ms,cpu_pct,mem_mb,write_amp,read_amp,durability_score,correctness_failures,reorder_violations,gc_violations\n\
r1,blake3,linear_hash,flatfs_tree,single_fsync,range_flock,930,17,68,720,1.05,1.10,0.95,0,0,0\n\
r2,blake3,linear_hash,flatfs_tree,group_commit,range_flock,1120,24,72,760,1.01,1.09,0.92,0,0,0\n\
r3,sha256,btree_index,segment_log,single_fsync,mutex,620,31,88,910,1.18,1.22,0.97,0,0,0\n\
r4,blake3,linear_hash,segment_log,async_commit,range_flock,1190,20,75,700,0.98,1.07,0.91,1,0,0\n\
r5,blake3,linear_hash,flatfs_tree,group_commit,range_flock,1080,19,70,745,1.00,1.08,0.96,0,0,0\n"
    }

    #[test]
    fn parses_prefixed_primitive_columns() {
        let rows = load_runs_from_csv_str(sample_csv()).expect("CSV should parse");
        assert_eq!(rows.len(), 5);
        let cols = primitive_columns(&rows);
        assert!(cols.contains(&"hash".to_string()));
        assert!(cols.contains(&"index".to_string()));
        assert!(cols.contains(&"layout".to_string()));
        assert_eq!(rows[0].run_id, "r1");
    }

    #[test]
    fn penalties_reduce_hard_fail_scores() {
        let rows = load_runs_from_csv_str(sample_csv()).expect("CSV should parse");
        let scored = score_runs(&rows, ScoreWeights::default());
        let good = scored
            .iter()
            .find(|x| x.record.run_id == "r2")
            .expect("missing r2");
        let bad = scored
            .iter()
            .find(|x| x.record.run_id == "r4")
            .expect("missing r4");
        assert!(good.score > bad.score);
    }

    #[test]
    fn analysis_produces_frontier_matrix_and_suggestions() {
        let rows = load_runs_from_csv_str(sample_csv()).expect("CSV should parse");
        let report = analyze_runs(&rows, ScoreWeights::default(), 8);
        assert!(!report.scored_runs.is_empty());
        assert!(!report.pareto_frontier.is_empty());
        assert!(report.tradeoff_matrix.contains_key("hash"));
        assert!(!report.suggestions.is_empty());
    }
}
