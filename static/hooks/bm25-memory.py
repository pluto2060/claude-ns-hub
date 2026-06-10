#!/usr/bin/env python3
"""
bm25-memory: BM25-based reactive memory for Claude Code.
Replaces git-memory.py (G1 proactive, recall 0.169) + g2-augment.py (G2 graph).

G1: ALL git decision commits → BM25 query-time ranking → top-7 relevant to prompt
    Keyword-identical: Structural Recall@7=1.000 (inflated, token overlap=0.476)
    Paraphrase fair eval: Structural Recall@7=0.627 (bias=0.373, 20260410 g1_fair_eval.py)
    Type2/3/4 (why/what/rationale): Structural Recall@7=0.667
    Combined fair Recall@7=0.634 (71 queries) — vs proactive 0.169 → 3.7x improvement
    Fix: CTX YYYYMMDD-prefix commits now recognized as decisions

G2a: docs/research/*.md + CLAUDE.md + MEMORY.md → BM25 → top-5 relevant chunks
     Keyword-identical eval: 10/10 (1.000) | Paraphrase eval: 7/10 (0.700)
     Note: 1.000 is inflated — paraphrase 0.700 is the honest fairness-adjusted score
G2b: codebase graph (codebase-memory-mcp SQLite) → relevant code files (project-internal only)
     Fallback: git grep -c keyword ranking when no DB available
G2b-hooks: ~/.claude/hooks/*.py BM25 search (triggered by "hook/훅/bm25-memory/auto-index/..." keywords)
     Directly indexes hook file function signatures — solves G2b external file gap

Cache: .omc/decision_corpus.json (auto-invalidated on git HEAD change)
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

try:
    from rank_bm25 import BM25Okapi
    HAS_BM25 = True
except ImportError:
    HAS_BM25 = False

RICH = "--rich" in sys.argv


# ── Tokenizer ────────────────────────────────────────────────────

_KO_PARTICLES = re.compile(
    r'(와|과|이|가|은|는|을|를|의|에서|으로|에게|부터|까지|처럼|같이|보다|이나|며|에|로|도|만|나|고)$'
)

# Conversational stopwords — filtered from QUERIES only (not the corpus).
# These appear in nearly every conversational prompt and make BM25 return
# noise matches on common words instead of real topic terms.
# Kept conservative — only words that are almost never content-bearing in
# a software-engineering commit subject.
_STOPWORDS = frozenset([
    # English function words
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
    "am", "do", "does", "did", "have", "has", "had", "will", "would",
    "could", "should", "may", "might", "can", "to", "of", "in", "on",
    "at", "by", "for", "with", "from", "as", "into", "about",
    "and", "or", "but", "if", "then", "than", "so", "not", "no",
    "i", "you", "we", "he", "she", "it", "they", "me", "my", "your",
    "our", "his", "her", "their", "this", "that", "these", "those",
    "there", "here", "what", "which", "when", "where", "why", "how",
    "who", "whom", "some", "any", "all", "each", "every", "both",
    "more", "most", "less", "few", "much", "many",
    "just", "only", "very", "too", "also", "even", "still", "yet",
    "now", "then", "up", "down", "out", "over", "again",
    # Conversational fillers
    "ok", "yeah", "yep", "pls", "please", "thanks", "thank",
    "hi", "hey", "hello", "want", "like", "think", "need",
    "make", "use", "using", "try", "trying", "get", "got",
    # Korean fillers (particles already stripped; these are standalone)
    "음", "어", "아", "그", "저", "이거", "저거", "그거",
])

# M59: Trivial hub-trigger prompts — G2-DOCS/G2-PREFETCH injection skipped.
# These words carry no semantic content and are used as hub dispatch signals
# (e.g. the user types "go" to kick off a northstar milestone execution).
# G1 (decision memory) is still processed because it uses separate relevance
# ranking and can still surface useful context even for short continuations.
_TRIVIAL_PROMPTS = frozenset([
    "go", "ㄱㄱ", "ㄱ", "ok", "okay", "네", "ㅇ", "ㄴ", "yes", "no",
    "계속", "continue", "next", "이어서", "진행", "진행해", "진행해줘",
    "go on", "carry on", "keep going", "k", "y", "n",
])

# M59: Minimum BM25 top-score for G2-DOCS injection.
# hybrid_search_docs() already gates at absolute BM25 score ≥ 1.0 internally,
# but we add a second gate HERE on the number of returned chunks as a
# proxy for confidence: if 0 chunks come back, we log a skip rather than
# silently injecting nothing.  The primary trivial-word defense is the
# _TRIVIAL_PROMPTS set + short-word-count check above.
_G2_DOCS_MIN_SCORE = 0.15  # reserved for future scored return value use


# ── Semantic rerank helpers (2026-04-24, M40 2026-05-25) ────────────────────
# Four layers (applied in priority order):
#   2. Korean-English synonym expansion (zero-cost lexical bridge)
#   3. BGE cross-encoder via bge-daemon (GPU, ~50ms, +15-25%p quality)
#   3.5. Local in-process cross-encoder (M40: ms-marco-MiniLM-L-6-v2, ~22MB CPU,
#        no daemon; fires when bge-daemon is down + sentence-transformers installed)
#   1. Bi-encoder via vec-daemon (e5-small, CPU-friendly, ~20ms/candidate)
# Layer 3 controlled by CTX_CROSS_ENCODER; Layer 3.5 by CTX_LOCAL_CE.

_VEC_SOCK = Path.home() / ".local/share/claude-vault/vec-daemon.sock"
_VEC_TIMEOUT = 0.8   # seconds — fail fast if daemon is down
_VEC_DISABLED = os.environ.get("CTX_DISABLE_SEMANTIC_RERANK") == "1"

# bge-daemon: BGE cross-encoder served over Unix socket (same pattern as vec-daemon).
# Hook stays fast because the 7s model load happens ONCE in the daemon, not per
# UserPromptSubmit. Default ON; disable via CTX_CROSS_ENCODER=0 if daemon is down
# and we don't want even the 0.8s connect-timeout cost per prompt.
_BGE_SOCK = Path.home() / ".local/share/claude-vault/bge-daemon.sock"
_BGE_TIMEOUT = 2.0   # seconds — rerank 20 cands typically <80ms, give slack
_USE_CROSS_ENCODER = os.environ.get("CTX_CROSS_ENCODER", "1") != "0"

# Layer 3.5: local in-process cross-encoder (no daemon required).
# Uses sentence-transformers CrossEncoder; loads lazily and stays cached.
# Model: cross-encoder/ms-marco-MiniLM-L-6-v2 (~22MB, ~10ms CPU for top-10).
# Applies when bge-daemon is down AND sentence-transformers is installed.
# Disable via CTX_LOCAL_CE=0.
_USE_LOCAL_CE = os.environ.get("CTX_LOCAL_CE", "1") != "0"
_LOCAL_CE_MODEL_NAME = "cross-encoder/ms-marco-MiniLM-L-6-v2"
_local_ce_model = None   # loaded lazily on first call
_local_ce_load_failed = False  # set True after first failed load attempt

# Layer 4 (M58): FlashRank reranker — ~4MB, no PyTorch, CPU <20ms.
# Model: ms-marco-MiniLM-L-12-v2 (cached in /tmp/flashrank).
# Auto-detects availability at module load; also enabled via CTX_RERANK=1.
# Applied to G2-DOCS top-10 BM25+dense RRF results before position reranking.
_FLASHRANK_AVAILABLE = False
try:
    from flashrank import Ranker as _FlashRanker, RerankRequest as _FlashRerankRequest  # noqa: F401
    _FLASHRANK_AVAILABLE = True
except ImportError:
    pass

_flashrank_ranker = None   # cached Ranker instance (lazy init)

# Small Korean-English synonym map for query expansion (Layer 2).
# Keys are case-folded. Additions expand the query token set — BM25 will match
# commits mentioning either side of each pair. Focused on CTX domain vocabulary
# that commonly appears in Korean prompts but English commits (and vice versa).
_SYNONYM_EXPANSION = {
    "cross-session":   ["long-term", "persistent", "inter-session", "장기기억"],
    "long-term":       ["cross-session", "persistent", "장기", "장기기억"],
    "memory":          ["recall", "retrieval", "기억"],
    "retrieval":       ["search", "recall", "fetch", "검색", "조회"],
    "search":          ["retrieval", "lookup", "검색"],
    "hook":            ["plugin", "extension", "훅"],
    "embed":           ["embedding", "vector", "임베딩"],
    "embedding":       ["embed", "vector", "임베딩"],
    "rerank":          ["rank", "reorder", "재정렬", "순위"],
    "semantic":        ["vector", "dense", "의미"],
    "context":         ["memory", "state", "컨텍스트"],
    "prompt":          ["query", "question", "프롬프트"],
    "improve":         ["enhance", "boost", "optimize", "개선", "향상"],
    "quality":         ["accuracy", "score", "품질"],
    "noise":           ["garbage", "irrelevant", "노이즈"],
    "cluster":         ["group", "dedup", "중복"],
    "dashboard":       ["ui", "visualization", "대시보드"],
    "bootstrap":       ["install", "setup", "부트스트랩"],
    "gpu":             ["cuda", "device", "가속"],
    "claude":          ["anthropic", "llm"],
    "korean":          ["한국어", "ko", "hangul"],
    "기억":             ["memory", "recall"],
    "검색":             ["search", "retrieval"],
    "장기기억":          ["long-term memory", "cross-session", "persistent"],
    "의사결정":          ["decision", "choice"],
    "훅":               ["hook", "plugin"],
    "임베딩":            ["embedding", "vector"],
}


def expand_query_tokens(query_tokens):
    """Layer 2: bridge Korean<->English lexical gaps via synonym map.
    Returns the original tokens + synonym expansions (capped at 2x length)."""
    out = list(query_tokens)
    for t in query_tokens:
        syns = _SYNONYM_EXPANSION.get(t.lower())
        if syns:
            out.extend(syns)
    # Dedupe while preserving order
    seen = set(); uniq = []
    for t in out:
        k = t.lower()
        if k not in seen:
            seen.add(k); uniq.append(t)
    return uniq[:len(query_tokens) * 2 + 5]   # cap growth


def _bge_rerank(query: str, docs: list):
    """Query the running bge-daemon for cross-encoder scores.

    Returns list[float] (raw logits, same length as docs) or None on failure.
    Caller applies sigmoid + filtering. Fail-fast: 2s timeout keeps the hook
    responsive if the daemon is wedged.
    """
    if not _USE_CROSS_ENCODER or not _BGE_SOCK.exists():
        return None
    try:
        import socket as _sk
        s = _sk.socket(_sk.AF_UNIX, _sk.SOCK_STREAM)
        s.settimeout(_BGE_TIMEOUT)
        s.connect(str(_BGE_SOCK))
        payload = (json.dumps({"query": query[:400],
                               "docs": [str(d)[:400] for d in docs]}) + "\n").encode("utf-8")
        s.sendall(payload)
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
        s.close()
        resp = json.loads(buf.split(b"\n")[0].decode("utf-8"))
        if resp.get("ok"):
            return resp.get("scores")
    except Exception:
        return None
    return None


def _ce_rerank_local(query: str, docs: list) -> list | None:
    """In-process cross-encoder reranker (Layer 3.5, M40).

    Returns list[float] scores (same length as docs), or None if unavailable.
    Model is loaded once and cached in _local_ce_model.
    Gracefully returns None if sentence-transformers is not installed.
    """
    global _local_ce_model, _local_ce_load_failed
    if not _USE_LOCAL_CE or _local_ce_load_failed:
        return None
    if _local_ce_model is None:
        try:
            from sentence_transformers import CrossEncoder as _CE
            _local_ce_model = _CE(_LOCAL_CE_MODEL_NAME)
        except Exception:
            _local_ce_load_failed = True
            return None
    try:
        pairs = [(query[:400], str(d)[:400]) for d in docs]
        scores = _local_ce_model.predict(pairs, show_progress_bar=False)
        return [float(s) for s in scores]
    except Exception:
        return None


def _flashrank_rerank(query: str, chunks: list, top_k: int = 10, score_threshold: float = 0.0) -> tuple:
    """Layer 4 (M58/M54.6): FlashRank reranker for G2-DOCS results.

    Reranks `chunks` (list[str]) using ms-marco-MiniLM-L-12-v2 (~4MB, no PyTorch).
    Returns (reranked_chunks, filtered_count) where filtered_count is the number of
    items removed by score_threshold filtering.
    On ImportError, model download failure, or any exception: returns original chunks
    unchanged (graceful degradation — never breaks the hook).

    Args:
        query: The user prompt used as rerank query.
        chunks: List of text chunks (BM25+dense RRF top-k results).
        top_k: Maximum number of results to return after reranking.
        score_threshold: Minimum FlashRank score to keep (0.0 = no filtering).
            Overridden by CTX_RERANK_THRESHOLD env var when > 0.

    Returns:
        Tuple (reranked_list[str], filtered_count: int).
        reranked_list length <= min(top_k, len(chunks)).
        filtered_count = number of items removed by threshold (0 when no filtering).
    """
    global _flashrank_ranker
    if not chunks:
        return chunks, 0
    # Resolve effective threshold: env var takes precedence when set
    _env_threshold = os.environ.get("CTX_RERANK_THRESHOLD", "")
    try:
        effective_threshold = float(_env_threshold) if _env_threshold else score_threshold
    except ValueError:
        effective_threshold = score_threshold
    try:
        if _flashrank_ranker is None:
            from flashrank import Ranker, RerankRequest  # noqa: F401 (already imported at module load)
            _flashrank_ranker = Ranker(
                model_name="ms-marco-MiniLM-L-12-v2",
                cache_dir="/tmp/flashrank",
            )
        from flashrank import RerankRequest
        passages = [{"id": i, "text": c} for i, c in enumerate(chunks)]
        request = RerankRequest(query=query[:500], passages=passages)
        results = _flashrank_ranker.rerank(request)
        # Apply score threshold filtering (M54.6 — Tokalator relevance gate)
        if effective_threshold > 0.0:
            before_filter = len(results)
            results = [r for r in results if r.get("score", 0.0) >= effective_threshold]
            filtered_count = before_filter - len(results)
        else:
            filtered_count = 0
        reranked = [chunks[r["id"]] for r in results[:top_k] if r["id"] < len(chunks)]
        if not reranked:
            # All items filtered out — return original top_k (safety fallback)
            return chunks[:top_k], filtered_count
        return reranked, filtered_count
    except Exception:
        return chunks[:top_k], 0


def _vec_embed(text: str):
    """Query the running vec-daemon for an embedding. Returns list[float] or None.
    Uses the same Unix socket protocol as chat-memory.py; 0 if daemon is down."""
    if _VEC_DISABLED or not _VEC_SOCK.exists():
        return None
    try:
        import socket
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(_VEC_TIMEOUT)
        s.connect(str(_VEC_SOCK))
        payload = (json.dumps({"q": text[:1000]}) + "\n").encode("utf-8")
        s.sendall(payload)
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(8192)
            if not chunk:
                break
            buf += chunk
        s.close()
        line = buf.split(b"\n")[0]
        resp = json.loads(line.decode("utf-8"))
        if resp.get("ok"):
            return resp.get("emb")
    except Exception:
        return None
    return None


def _cosine(a, b):
    if not a or not b or len(a) != len(b):
        return 0.0
    import math
    dot = sum(x * y for x, y in zip(a, b))
    # embeddings from vec-daemon are already normalized → dot = cosine
    return max(0.0, min(1.0, dot))


def semantic_rerank_filter(candidates, query, top_k, alpha_bm25=0.6,
                           cosine_min=0.55, bm25_scores=None):
    """Rerank a list of candidate items by blended BM25 + cosine semantic.

    candidates: list of dicts, each with a 'text' or 'subject' field
    query: user query string
    top_k: final count
    alpha_bm25: weight of BM25 score in blend (1-alpha = semantic weight)
    cosine_min: hard floor — items below this cosine get dropped even if BM25 is high
    bm25_scores: optional pre-computed BM25 scores (normalized 0-1); if None,
                 assume candidates are already ordered by BM25 → use rank position

    Fail-safe: if vec-daemon is down, returns candidates[:top_k] (no-op).

    Layer 3 (2026-04-24): prefer BGE cross-encoder when available — it scores
    (query, candidate) jointly instead of computing independent embeddings +
    cosine. Much stronger semantic judgement on short commit subjects.
    Falls back to bi-encoder cosine path if cross-encoder fails to load.
    """
    # ── Layer 3: bge-daemon cross-encoder path (strongest semantic signal) ───
    # Calls the resident bge-daemon over Unix socket. Daemon holds BGE weights
    # in GPU memory so hook pays ~50ms per rerank, not the 7s cold-load.
    kept = []
    doc_texts = []
    for i, c in enumerate(candidates):
        text = c.get("subject") or c.get("text") or ""
        if not text:
            continue
        kept.append((i, c))
        doc_texts.append(text[:400])
    if doc_texts:
        ce_scores = _bge_rerank(query, doc_texts)
        if ce_scores is not None and len(ce_scores) == len(kept):
            import math
            def _sig(x): return 1.0 / (1.0 + math.exp(-float(x)))
            rescored = []
            ce_min = 0.35
            for (i, c), s in zip(kept, ce_scores):
                ce_norm = _sig(s)
                if ce_norm < ce_min:
                    continue
                bm25_norm = (bm25_scores[i] if bm25_scores else (len(candidates) - i) / max(1, len(candidates)))
                blend = alpha_bm25 * bm25_norm + (1.0 - alpha_bm25) * ce_norm
                rescored.append((blend, ce_norm, c))
            rescored.sort(key=lambda x: -x[0])
            if rescored:
                return [c for _, _, c in rescored[:top_k]]
            # CE filtered everything → fall back to local CE or bi-encoder

    # ── Layer 3.5: local in-process cross-encoder (M40) ──────────────────────
    # Runs when bge-daemon is unavailable. Uses sentence-transformers
    # CrossEncoder (ms-marco-MiniLM-L-6-v2, ~22MB, ~10ms CPU for top-10).
    # Loaded lazily; gracefully skipped when sentence-transformers absent.
    if doc_texts:
        local_ce_scores = _ce_rerank_local(query, doc_texts)
        if local_ce_scores is not None and len(local_ce_scores) == len(kept):
            import math
            def _sig_local(x): return 1.0 / (1.0 + math.exp(-float(x)))
            rescored_local = []
            for (i, c), s in zip(kept, local_ce_scores):
                ce_norm = _sig_local(s)
                bm25_norm = (bm25_scores[i] if bm25_scores else (len(candidates) - i) / max(1, len(candidates)))
                blend = alpha_bm25 * bm25_norm + (1.0 - alpha_bm25) * ce_norm
                rescored_local.append((blend, ce_norm, c))
            rescored_local.sort(key=lambda x: -x[0])
            if rescored_local:
                return [c for _, _, c in rescored_local[:top_k]]

    # ── Bi-encoder fallback (original path) ───
    q_emb = _vec_embed(query)
    if not q_emb:
        return candidates[:top_k]   # daemon down → no-op

    rescored = []
    for i, c in enumerate(candidates):
        text = c.get("subject") or c.get("text") or ""
        if not text:
            continue
        c_emb = _vec_embed(text[:400])   # short for speed
        if not c_emb:
            continue
        cos = _cosine(q_emb, c_emb)
        if cos < cosine_min:
            continue   # hard drop — semantic dissimilarity overrides BM25 rank
        # Normalize BM25 to [0,1] by rank position (top = 1.0, bottom = ~0)
        bm25_norm = (bm25_scores[i] if bm25_scores else (len(candidates) - i) / max(1, len(candidates)))
        blend = alpha_bm25 * bm25_norm + (1.0 - alpha_bm25) * cos
        rescored.append((blend, cos, c))
    rescored.sort(key=lambda x: -x[0])
    return [c for _, _, c in rescored[:top_k]]


# Porter stemmer (opt-in via CTX_STEM=1, default ON 2026-04-24 after G1 regression
# showed +0.034 improvement on Recall@7 with zero losses. See
# benchmarks/results/g1_regression_ctx_v2.json).
# Rationale: collapses "logs"/"logging"/"logged" → "log" so queries match stem-
# variants in commit subjects. Especially important for conflict-resolution
# (MAB Competency-4) where reversal vocab shifts (e.g. "rerank" → "reranking").
_USE_STEMMER = os.environ.get("CTX_STEM", "1") != "0"
_STEMMER = None
if _USE_STEMMER:
    try:
        from nltk.stem.porter import PorterStemmer as _PS
        _STEMMER = _PS()
    except ImportError:
        _STEMMER = None   # stemming silently disabled if nltk not installed


def tokenize(text: str, drop_stopwords: bool = False):
    """Preserve decimal numbers (0.724) and numeric ranges (7-30) as single tokens.
    Also strips Korean particles from mixed Korean-ASCII tokens (e.g. 'BM25와' → 'bm25' + 'bm25와')
    so that Korean queries match English commit subjects correctly.

    When `drop_stopwords=True` (query-side only), conversational fillers are
    removed to prevent BM25 from matching on common words like "i", "to", "how",
    "would", etc. Corpus tokenization never drops stopwords — IDF handles those.

    Porter stemmer (2026-04-24): adds stemmed variant for each token so "logs"
    matches "logging". Preserves the original token too so exact-match precision
    is never lost (dedup handles duplicates). Opt-out via CTX_STEM=0.
    """
    raw = re.findall(r'\d+[-\u2013]\d+|\d+\.\d+|\w+', text.lower())
    result = []
    for tok in raw:
        if drop_stopwords and tok in _STOPWORDS:
            continue
        cleaned = _KO_PARTICLES.sub('', tok)
        if cleaned and cleaned != tok:
            if not (drop_stopwords and cleaned in _STOPWORDS):
                result.append(cleaned)
        result.append(tok)
        # Porter stem — adds a THIRD variant. Dedup at return preserves order
        # so original tokens remain ranked; stem is a recall-rescue fallback.
        if _STEMMER is not None and tok.isalpha() and len(tok) > 3:
            stemmed = _STEMMER.stem(tok)
            if stemmed != tok:
                result.append(stemmed)
    return list(dict.fromkeys(result))


# ── G1: Decision Corpus ──────────────────────────────────────────

_CONV_PREFIXES = (
    "feat:", "fix:", "refactor:", "perf:", "security:", "design:", "test:",
    "feat(", "fix(", "refactor(", "perf(",
)
_VERSION_RE = re.compile(r"^v\d+\.\d+")
_DECISION_KEYWORDS = (
    "pivot", "revert", "dead-end", "rejected", "chose", "switched",
    "CONVERGED", "failed", "success", "fix", "improvement",
    "benchmark", "eval", "decision", "iter",
)
_NOISE_PREFIXES = ("# ", "wip:", "merge ", 'revert "')
_STRICT_VERSION_RE = re.compile(r"^v\d+\.\d+\.\d+")
_OMC_ITER_RE = re.compile(r"^(omc-live|live-inf)\s+iter", re.IGNORECASE)
_EMBEDDED_DECISION_RE = re.compile(
    r"\s[-\u2014]\s*(feat|fix|refactor|perf|security|design|implement|add|remove|replace|switch|migrate)",
    re.IGNORECASE,
)
_YYYYMMDD_RE = re.compile(r"^\d{8}\s")  # CTX-style: "20260408 G1 temporal..."


def _is_structural_noise(subject):
    s = subject.strip()
    if _OMC_ITER_RE.match(s):
        return True
    if _STRICT_VERSION_RE.match(s):
        return not bool(_EMBEDDED_DECISION_RE.search(s))
    return False


def _is_decision(subject):
    """Detect decision commits: conventional, version-tagged, YYYYMMDD, or keyword."""
    s = subject.strip()
    if not s:
        return False
    sl = s.lower()
    if any(sl.startswith(p) for p in _NOISE_PREFIXES):
        return False
    if any(sl.startswith(p) for p in _CONV_PREFIXES):
        return True
    if _VERSION_RE.match(s):
        return True
    if _YYYYMMDD_RE.match(s):  # CTX-style date-prefixed commits
        return True
    return any(kw.lower() in sl for kw in _DECISION_KEYWORDS)


# ── query_type classification (for retrieval_event schema v1.1) ──────────────
_TEMPORAL_KW = frozenset([
    "when", "history", "timeline", "progression", "what happened", "progress",
    "previously", "before", "after", "last time", "since", "ago", "recent",
    "changed", "evolution", "how long", "session", "yesterday", "last week",
    "진행", "역사", "이전", "지난", "타임라인", "최근", "변경", "이번",
])

def _classify_query_type(prompt: str) -> str:
    """Classify prompt into TEMPORAL / KEYWORD / SEMANTIC.

    TEMPORAL  — query is about history/timeline/progression
    KEYWORD   — short technical lookup (≤60 chars) or pure symbol/identifier
    SEMANTIC  — natural language conceptual query (default)
    """
    if not prompt:
        return "KEYWORD"
    pl = prompt.lower()
    if any(kw in pl for kw in _TEMPORAL_KW):
        return "TEMPORAL"
    words = pl.split()
    if len(words) <= 6:
        return "KEYWORD"
    return "SEMANTIC"


def _process_ctx_rate(prompt: str, project_dir: str, session_id: str):
    """Handle /ctx rate command — log 👍/👎 feedback for last injection.

    Returns (rating_int, label_str, inj_preview) if command matched, else None.
    Writes to .omc/feedback_log.jsonl.
    """
    import re as _re
    stripped = prompt.strip()
    if not _re.match(r'^/ctx\s+rate\b', stripped, _re.IGNORECASE):
        return None

    tail = _re.sub(r'^/ctx\s+rate\s*', '', stripped, flags=_re.IGNORECASE).lower()
    positive = {"good", "up", "👍", "1", "+", "yes", "helpful", "great", "nice", "ok"}
    negative = {"bad", "down", "👎", "0", "-", "no", "useless", "wrong", "skip", "nope"}
    rating = -1 if any(w in tail for w in negative) else 1
    label = "👍" if rating == 1 else "👎"

    last_inj_path = Path.home() / ".claude" / "last-injection.json"
    inj_meta: dict = {}
    try:
        if last_inj_path.exists():
            inj_meta = json.loads(last_inj_path.read_text())
    except Exception:
        pass

    blocks = list({item.get("block", "") for item in inj_meta.get("items", []) if item.get("block")})
    preview = inj_meta.get("prompt_preview", "")[:60]

    import time as _t_rate
    entry = {
        "ts": _t_rate.time(),
        "session_id": session_id,
        "rating": rating,
        "injection_ts": inj_meta.get("ts"),
        "prompt_preview": preview,
        "project": inj_meta.get("project", ""),
        "blocks": blocks,
    }
    try:
        log_path = os.path.join(project_dir, ".omc", "feedback_log.jsonl")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as _f:
            _f.write(json.dumps(entry) + "\n")
    except Exception:
        pass

    return rating, label, preview


# ── M41: AST import graph ─────────────────────────────────────────────────────

def _extract_py_imports(src_file: str, project_dir: str) -> list:
    """Return (src_rel, dst_rel) edges by parsing Python import statements."""
    import ast as _ast
    try:
        src_path = Path(src_file)
        tree = _ast.parse(src_path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return []
    src_rel = os.path.relpath(src_file, project_dir)
    edges = []
    for node in _ast.walk(tree):
        if isinstance(node, _ast.Import):
            for alias in node.names:
                candidate = Path(project_dir) / (alias.name.replace(".", "/") + ".py")
                if candidate.exists():
                    edges.append((src_rel, os.path.relpath(str(candidate), project_dir)))
        elif isinstance(node, _ast.ImportFrom) and node.module:
            if node.level:
                base = Path(src_file).parent
                for _ in range(node.level - 1):
                    base = base.parent
                candidate = base / (node.module.replace(".", "/") + ".py")
            else:
                candidate = Path(project_dir) / (node.module.replace(".", "/") + ".py")
            if candidate.exists():
                edges.append((src_rel, os.path.relpath(str(candidate), project_dir)))
    return edges


_JS_IMPORT_RE = re.compile(r"""from\s+['"]([./][^'"]+)['"]""")

def _extract_js_imports(src_file: str, project_dir: str) -> list:
    """Return (src_rel, dst_rel) edges from ES module import statements."""
    try:
        text = Path(src_file).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []
    src_rel = os.path.relpath(src_file, project_dir)
    edges = []
    src_dir = Path(src_file).parent
    for m in _JS_IMPORT_RE.finditer(text):
        imp = m.group(1)
        resolved = (src_dir / imp).resolve()
        for ext in ("", ".ts", ".tsx", ".js", ".jsx"):
            candidate = Path(str(resolved) + ext)
            if candidate.exists():
                try:
                    rel = os.path.relpath(str(candidate), project_dir)
                    if not rel.startswith(".."):
                        edges.append((src_rel, rel))
                except ValueError:
                    pass
                break
    return edges


# ── M54.8: Entity KG extraction ────────────────────────────────────────────────

def _extract_entities(src_file: str, project_dir: str) -> tuple:
    """Extract function/class/method entities and their relationships from a Python file.

    Returns (entities, entity_edges) where:
    - entities: list of dicts with keys: id, file, name, kind, signature, line
    - entity_edges: list of (src_id, dst_name, kind) tuples
      kind is "inherits" (class→base) or "calls" (func→callee, best-effort)
    Only processes .py files; returns ([], []) for non-Python or parse errors.
    """
    import ast as _ast
    if not src_file.endswith(".py"):
        return [], []
    try:
        src_path = Path(src_file)
        source = src_path.read_text(encoding="utf-8", errors="ignore")
        tree = _ast.parse(source, filename=src_file)
    except Exception:
        return [], []

    file_rel = os.path.relpath(src_file, project_dir)
    entities: list = []
    entity_edges: list = []
    source_lines = source.splitlines()

    def _sig(node):
        """Return the first line of a def/class as signature."""
        try:
            return source_lines[node.lineno - 1].strip()
        except (IndexError, AttributeError):
            return ""

    def _entity_id(file_rel, name):
        return f"{file_rel}::{name}"

    # Collect method (lineno, name) pairs from all ClassDef nodes first
    # so we can skip them when ast.walk visits FunctionDef nodes independently.
    _method_lines: set = set()  # (lineno,) of nodes already captured as methods
    for node in _ast.walk(tree):
        if isinstance(node, _ast.ClassDef):
            for item in node.body:
                if isinstance(item, (_ast.FunctionDef, _ast.AsyncFunctionDef)):
                    _method_lines.add(item.lineno)

    # Walk top-level and class-level nodes
    for node in _ast.walk(tree):
        if isinstance(node, _ast.ClassDef):
            cls_id = _entity_id(file_rel, node.name)
            entities.append({
                "id": cls_id,
                "file": file_rel,
                "name": node.name,
                "kind": "class",
                "signature": _sig(node),
                "line": node.lineno,
            })
            # Base class inheritance edges (best-effort: only simple Name nodes)
            for base in node.bases:
                if isinstance(base, _ast.Name):
                    entity_edges.append((cls_id, base.id, "inherits"))
                elif isinstance(base, _ast.Attribute):
                    entity_edges.append((cls_id, base.attr, "inherits"))

            # Methods inside this class
            for item in node.body:
                if isinstance(item, (_ast.FunctionDef, _ast.AsyncFunctionDef)):
                    method_id = _entity_id(file_rel, f"{node.name}.{item.name}")
                    entities.append({
                        "id": method_id,
                        "file": file_rel,
                        "name": f"{node.name}.{item.name}",
                        "kind": "method",
                        "signature": _sig(item),
                        "line": item.lineno,
                    })
                    # Call edges from method body
                    for child in _ast.walk(item):
                        if isinstance(child, _ast.Call):
                            if isinstance(child.func, _ast.Name) and child.func.id != item.name:
                                entity_edges.append((method_id, child.func.id, "calls"))
                            elif isinstance(child.func, _ast.Attribute):
                                entity_edges.append((method_id, child.func.attr, "calls"))

        elif isinstance(node, (_ast.FunctionDef, _ast.AsyncFunctionDef)):
            # Skip nodes that were already captured as class methods
            if node.lineno in _method_lines:
                continue
            fn_id = _entity_id(file_rel, node.name)
            entities.append({
                "id": fn_id,
                "file": file_rel,
                "name": node.name,
                "kind": "function",
                "signature": _sig(node),
                "line": node.lineno,
            })
            # Best-effort call edges: extract called names from function body
            for child in _ast.walk(node):
                if isinstance(child, _ast.Call):
                    if isinstance(child.func, _ast.Name) and child.func.id != node.name:
                        entity_edges.append((fn_id, child.func.id, "calls"))
                    elif isinstance(child.func, _ast.Attribute):
                        entity_edges.append((fn_id, child.func.attr, "calls"))

    # Deduplicate edges
    entity_edges_deduped = list(dict.fromkeys(entity_edges))
    return entities, entity_edges_deduped


def build_import_graph(project_dir: str) -> "Path | None":
    """Build/cache AST import graph in .omc/import_graph.db.

    Rebuilt when > 24h old. Returns db path or None on failure.
    Layout:
      edges(src TEXT, dst TEXT, PRIMARY KEY(src, dst))
      entities(id, file, name, kind, signature, line)       — M54.8
      entity_edges(src, dst, kind, PRIMARY KEY(src,dst,kind)) — M54.8
    """
    import sqlite3 as _sql, time as _t
    omc_dir = Path(project_dir) / ".omc"
    db_path = omc_dir / "import_graph.db"
    # Use cached version if fresh
    if db_path.exists() and (_t.time() - db_path.stat().st_mtime) < 86400:
        return db_path
    try:
        omc_dir.mkdir(parents=True, exist_ok=True)
        conn = _sql.connect(str(db_path), timeout=5)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS edges (src TEXT, dst TEXT, PRIMARY KEY (src, dst))"
        )
        # M54.8: entity KG tables
        conn.execute(
            """CREATE TABLE IF NOT EXISTS entities (
                id        TEXT PRIMARY KEY,
                file      TEXT NOT NULL,
                name      TEXT NOT NULL,
                kind      TEXT NOT NULL,
                signature TEXT,
                line      INTEGER
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS entity_edges (
                src  TEXT,
                dst  TEXT,
                kind TEXT,
                PRIMARY KEY (src, dst, kind)
            )"""
        )
        conn.execute("DELETE FROM edges")
        conn.execute("DELETE FROM entities")
        conn.execute("DELETE FROM entity_edges")
        edges: list = []
        all_entities: list = []
        all_entity_edges: list = []
        skip_dirs = {".git", "__pycache__", "node_modules", ".omc", "dist", "build"}
        for root, dirs, files in os.walk(project_dir):
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for fname in files:
                fpath = os.path.join(root, fname)
                if fname.endswith(".py"):
                    edges.extend(_extract_py_imports(fpath, project_dir))
                    # M54.8: extract entity KG for Python files
                    ents, eedges = _extract_entities(fpath, project_dir)
                    all_entities.extend(ents)
                    all_entity_edges.extend(eedges)
                elif fname.endswith((".ts", ".tsx", ".js", ".jsx")):
                    edges.extend(_extract_js_imports(fpath, project_dir))
        conn.executemany("INSERT OR IGNORE INTO edges VALUES (?, ?)", edges)
        conn.executemany(
            "INSERT OR IGNORE INTO entities (id, file, name, kind, signature, line) VALUES (?,?,?,?,?,?)",
            [(e["id"], e["file"], e["name"], e["kind"], e.get("signature", ""), e.get("line")) for e in all_entities],
        )
        conn.executemany(
            "INSERT OR IGNORE INTO entity_edges (src, dst, kind) VALUES (?, ?, ?)",
            all_entity_edges,
        )
        conn.commit()
        conn.close()
        return db_path
    except Exception:
        return None


def get_graph_neighbors(db_path: "Path", file_rel: str, max_per_dir: int = 3, hops: int = 1) -> tuple:
    """Return BFS importers+importees up to `hops` levels deep.

    Returns (bfs_neighbors, dir_tier) where:
    - bfs_neighbors: list of relative paths from BFS traversal (capped max_per_dir per hop)
    - dir_tier: list of same-directory files not already in bfs_neighbors (up to max_per_dir)
    Total result is capped at 8 across both sets.
    """
    import sqlite3 as _sql
    import os as _os
    TOTAL_CAP = 8
    try:
        conn = _sql.connect(str(db_path), timeout=2)
        visited = {file_rel}
        frontier = [file_rel]
        bfs_neighbors = []

        for _hop in range(hops):
            next_frontier = []
            for seed in frontier:
                hop_results = []
                importers = [r[0] for r in conn.execute(
                    "SELECT DISTINCT src FROM edges WHERE dst=? LIMIT ?", (seed, max_per_dir)
                ).fetchall()]
                importees = [r[0] for r in conn.execute(
                    "SELECT DISTINCT dst FROM edges WHERE src=? LIMIT ?", (seed, max_per_dir)
                ).fetchall()]
                for f in importers + importees:
                    if f not in visited:
                        visited.add(f)
                        hop_results.append(f)
                        next_frontier.append(f)
                bfs_neighbors.extend(hop_results)
                if len(bfs_neighbors) >= TOTAL_CAP:
                    break
            frontier = next_frontier
            if len(bfs_neighbors) >= TOTAL_CAP or not frontier:
                break

        # Directory-scoped tier-2: same dir as seed file, not already visited
        seed_dir = _os.path.dirname(file_rel)
        dir_tier = []
        try:
            dir_rows = conn.execute(
                """SELECT DISTINCT src FROM edges WHERE src LIKE ? AND src != ?
                   UNION
                   SELECT DISTINCT dst FROM edges WHERE dst LIKE ? AND dst != ?
                   LIMIT ?""",
                (seed_dir + "/%", file_rel, seed_dir + "/%", file_rel, max_per_dir * 2)
            ).fetchall()
            for (f,) in dir_rows:
                if f not in visited and len(dir_tier) < max_per_dir:
                    dir_tier.append(f)
                    visited.add(f)
        except Exception:
            pass

        conn.close()
        # Enforce total cap
        all_results = bfs_neighbors + dir_tier
        if len(all_results) > TOTAL_CAP:
            extra = len(all_results) - TOTAL_CAP
            dir_tier = dir_tier[:-extra] if extra <= len(dir_tier) else []
        return bfs_neighbors, dir_tier
    except Exception:
        return [], []


# ── M54.8: Entity KG neighbor lookup ──────────────────────────────────────────

_CAMEL_OR_SNAKE_RE = re.compile(r'\b([A-Z][a-zA-Z0-9]{2,}|[a-z][a-z0-9]*(?:_[a-z0-9]+){1,})\b')

def get_entity_neighbors(db_path: "Path", query_term: str, max_results: int = 5) -> list:
    """Return file paths containing entities whose name or signature matches query_term.

    Searches the entities table by name (LIKE) and signature (LIKE).
    Falls back to empty list on any error (DB not built yet, missing table, etc.).
    """
    import sqlite3 as _sql
    if not db_path or not Path(db_path).exists():
        return []
    try:
        conn = _sql.connect(str(db_path), timeout=2)
        pattern = f"%{query_term}%"
        rows = conn.execute(
            """SELECT DISTINCT file, name, kind FROM entities
               WHERE name LIKE ? OR signature LIKE ?
               LIMIT ?""",
            (pattern, pattern, max_results),
        ).fetchall()
        conn.close()
        return [(row[0], row[1], row[2]) for row in rows]
    except Exception:
        return []


def _entity_query_terms(prompt: str) -> list:
    """Extract likely entity names (CamelCase or multi-word snake_case identifiers) from prompt.

    Returns up to 3 candidate terms for entity KG lookup.
    CamelCase: must start uppercase followed by at least one lowercase+uppercase transition.
    snake_case: must have at least one underscore segment (already required by regex).
    """
    matches = _CAMEL_OR_SNAKE_RE.findall(prompt)
    # Filter out common English words that happen to look like CamelCase/identifiers
    _COMMON = frozenset([
        "the", "and", "for", "that", "with", "this", "from", "have",
        "how", "also", "what", "when", "where", "which", "your", "our",
        "can", "does", "did", "not", "but", "are", "use", "see", "let",
    ])
    seen = set()
    result = []
    for m in matches:
        m_lower = m.lower()
        # Require length ≥ 4 and not a common word
        if len(m) < 4 or m_lower in _COMMON:
            continue
        # CamelCase: must contain at least one lowercase letter (avoids ALL-CAPS acronyms)
        # AND start with uppercase (true CamelCase), or be snake_case (contains _)
        is_camel = m[0].isupper() and any(c.islower() for c in m[1:])
        is_snake = "_" in m
        if not (is_camel or is_snake):
            continue
        if m not in seen:
            seen.add(m)
            result.append(m)
        if len(result) >= 3:
            break
    return result


def get_git_head(project_dir):
    try:
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project_dir, capture_output=True, text=True, timeout=3,
        )
        return r.stdout.strip() if r.returncode == 0 else None
    except Exception:
        return None


def build_decision_corpus(project_dir, n=500):
    """Extract all decision commits from git log (no cap)."""
    try:
        r = subprocess.run(
            ["git", "log", f"-{n}", "--format=%H\x1f%s\x1f%ai"],
            cwd=project_dir, capture_output=True, text=True, timeout=10,
        )
        if r.returncode != 0:
            return []
    except Exception:
        return []

    corpus = []
    seen = set()
    for line in r.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.strip().split("\x1f", 2)
        if len(parts) < 2:
            continue
        commit_hash = parts[0]
        subject = parts[1][:120]
        date = parts[2][:10] if len(parts) == 3 else ""

        if _is_structural_noise(subject):
            continue
        key = subject[:60]
        if key in seen:
            continue
        seen.add(key)

        if _is_decision(subject):
            # 고우선순위 패턴 → text 중복 삽입으로 BM25 가중치 증폭
            is_milestone = any(p in subject for p in [
                "CONVERGED", "pivot", "완성", "완료", "검증", "수렴", "FAILED", "KILL"
            ])
            text = f"{date} {subject}"
            if is_milestone:
                text = f"{text}\n{text}"  # 2배 가중치
            corpus.append({
                "hash": commit_hash,
                "subject": subject,
                "date": date,
                "text": text,
            })

    return corpus


def get_decision_corpus(project_dir):
    """Return cached corpus or rebuild if git HEAD changed.

    Extended (2026-04-26): also pre-embeds corpus items via vec-daemon and caches
    embeddings in the same file under an 'emb_head' sentinel. Embeddings allow
    dense first-stage retrieval (dense_rank_decisions) without per-query N socket
    calls. Falls back gracefully: if vec-daemon is down, items lack 'emb' field
    and dense_rank_decisions returns [].
    """
    cache_path = Path(project_dir) / ".omc" / "decision_corpus.json"
    head = get_git_head(project_dir)

    if cache_path.exists() and head:
        try:
            cached = json.loads(cache_path.read_text())
            if cached.get("head") == head:
                corpus = cached["corpus"]
                # Check if embeddings are fresh for this HEAD
                if cached.get("emb_head") != head:
                    n = embed_corpus_items(corpus)
                    if n > 0:
                        cache_path.write_text(json.dumps({
                            "head": head, "corpus": corpus, "emb_head": head
                        }))
                return corpus
        except Exception:
            pass

    corpus = build_decision_corpus(project_dir)
    if head and corpus:
        try:
            cache_path.parent.mkdir(exist_ok=True)
            embed_corpus_items(corpus)
            cache_path.write_text(json.dumps({
                "head": head, "corpus": corpus, "emb_head": head
            }))
        except Exception:
            pass
    return corpus


def embed_corpus_items(corpus):
    """Add 'emb' field to corpus items using vec-daemon. Modifies in-place.

    Only embeds items missing 'emb'. Returns count of newly embedded items.
    Fail-safe: if vec-daemon is down, items are left without 'emb' and
    dense_rank_decisions will return [] (BM25-only fallback).
    """
    embedded = 0
    for item in corpus:
        if item.get("emb"):
            continue
        text = (item.get("subject") or item.get("text") or "")[:400]
        if not text:
            continue
        emb = _vec_embed(text)
        if emb:
            item["emb"] = emb
            embedded += 1
    return embedded


def dense_rank_decisions(corpus, query, top_k=20):
    """Dense first-stage retrieval: cosine similarity between query embedding
    and pre-computed corpus embeddings (from embed_corpus_items).

    Returns top-k items by cosine, or [] if vec-daemon unavailable or corpus
    has no embeddings (BM25-only fallback).
    """
    q_emb = _vec_embed(query)
    if not q_emb:
        return []
    scored = []
    for item in corpus:
        emb = item.get("emb")
        if not emb:
            continue
        cos = _cosine(q_emb, emb)
        if cos > 0.0:
            scored.append((cos, item))
    if not scored:
        return []
    scored.sort(key=lambda x: -x[0])
    return [item for _, item in scored[:top_k]]


def rrf_merge(list_a, list_b, k_rrf=60):
    """Reciprocal Rank Fusion of two ranked lists.

    k_rrf=60: optimal constant per BEIR paper (arXiv:2104.08663) — controls
    score distribution across rank positions.

    Uses commit 'hash' as dedup key; falls back to first-20-chars of 'text'.
    Returns merged list ordered by RRF score (descending).
    """
    scores = {}
    hash_to_item = {}

    def _key(item):
        return item.get("hash") or (item.get("text") or "")[:20]

    for rank, item in enumerate(list_a, 1):
        k = _key(item)
        scores[k] = scores.get(k, 0.0) + 1.0 / (k_rrf + rank)
        hash_to_item[k] = item

    for rank, item in enumerate(list_b, 1):
        k = _key(item)
        scores[k] = scores.get(k, 0.0) + 1.0 / (k_rrf + rank)
        hash_to_item[k] = item

    merged_keys = sorted(scores.keys(), key=lambda h: (-scores[h], h))
    return [hash_to_item[h] for h in merged_keys]


def bm25_rank_decisions(corpus, query, top_k=7, min_score=0.5,
                        adaptive_floor_ratio=0.35, mmr_jaccard_threshold=0.70,
                        skip_rerank=False):
    """BM25-rank decision corpus against query, return top-k.

    Stopwords are dropped from the query (not the corpus) so conversational
    fillers like "i/to/how/would" don't dominate the ranking.

    `min_score`: if the best-matching decision scores below this, return [].
    Prevents the "no-topic-match → fallback to most-recent-7" anti-pattern
    where zero-score or near-zero queries got ranked purely by git-log order.

    `adaptive_floor_ratio` (NEW 2026-04-24): candidates below
        top_score * adaptive_floor_ratio are dropped. Eliminates the
        "surface-token match" noise where a hit scores just above min_score
        but is 3-5× worse than the actual best hit (e.g., 'iter 47/∞: token%'
        scoring 1.2 when the real match scores 4.0).

    `mmr_jaccard_threshold` (NEW 2026-04-24): if a candidate's token set has
        Jaccard similarity >= threshold with any already-selected item, skip it.
        Collapses clustered noise like multiple 'live-infinite iter N/∞' entries
        that are near-duplicates — keeps only the best of each cluster.
    """
    if not corpus:
        return []
    if not HAS_BM25 or not query.strip():
        return []

    query_tokens = tokenize(query, drop_stopwords=True)
    if not query_tokens:
        return []

    # Layer 2 (2026-04-24): synonym expansion to bridge KO↔EN + concept gaps
    # (e.g. "cross-session memory" now matches "persistent long-term 장기기억" too).
    query_tokens = expand_query_tokens(query_tokens)

    tokenized = [tokenize(c["text"]) for c in corpus]
    bm25 = BM25Okapi(tokenized)
    scores = bm25.get_scores(query_tokens)
    if len(scores) == 0 or float(max(scores)) < min_score:
        return []

    top_score = float(max(scores))
    adaptive_floor = max(min_score, top_score * adaptive_floor_ratio)

    ranked_idx = sorted(range(len(corpus)), key=lambda i: (-scores[i], i))

    # Cluster signature: normalizes "live-infinite iter N/∞: goal_vM" boilerplate
    # so different iter-numbers don't escape MMR dedup (MEMORY.md: "live-inf iter
    # N/∞ topic-dedup collapse" known issue).
    import re as _re
    def _cluster_sig(subject: str) -> str:
        s = subject.lower()
        s = _re.sub(r'\b\d{4,}\b|\b\d+/\d+\b|\b\d+/∞\b|goal_v\d+', '', s)
        s = _re.sub(r'iter\s*\d+', 'iter', s)
        s = _re.sub(r'[^a-z가-힣\s]', ' ', s)
        s = _re.sub(r'\s+', ' ', s).strip()
        # First 4 distinctive words form the cluster signature
        return ' '.join(s.split()[:4])

    selected = []
    selected_token_sets = []
    selected_cluster_sigs = set()
    for idx in ranked_idx:
        if scores[idx] < adaptive_floor:
            break
        cand_tokens = set(tokenized[idx])
        if not cand_tokens:
            continue
        cand_sig = _cluster_sig(corpus[idx].get("subject", corpus[idx].get("text", "")))
        # Cluster dedup: skip if any selected item has the same normalized sig
        if cand_sig and cand_sig in selected_cluster_sigs:
            continue
        # MMR-lite: skip if too similar to already-selected items
        is_near_dup = False
        for prev_tokens in selected_token_sets:
            union = cand_tokens | prev_tokens
            if not union:
                continue
            jaccard = len(cand_tokens & prev_tokens) / len(union)
            if jaccard >= mmr_jaccard_threshold:
                is_near_dup = True
                break
        if is_near_dup:
            continue
        selected.append(corpus[idx])
        selected_token_sets.append(cand_tokens)
        if cand_sig:
            selected_cluster_sigs.add(cand_sig)
        # Keep 2x the target so semantic rerank has room to re-order/filter
        if len(selected) >= top_k * 2:
            break
    # Layer 1 (2026-04-24): lowered gate — rerank fires for 60%+ of queries now,
    # was ~30% with `> top_k` (many queries returned exactly top_k candidates).
    if not skip_rerank and len(selected) >= top_k + 2:
        selected = semantic_rerank_filter(selected, query, top_k=top_k)
    return selected[:top_k]


def hybrid_rank_decisions(corpus, query, top_k=7):
    """Hybrid BM25+dense retrieval with RRF merge — SOTA method per MAB/LongMemEval.

    Pipeline (2026-04-26):
      1. BM25 top-(top_k*2) with MMR/cluster dedup, NO semantic rerank yet
      2. Dense top-(top_k*2) using pre-embedded corpus via vec-daemon cosine
      3. RRF merge (k=60) — union of both candidate pools
      4. Semantic rerank (BGE cross-encoder → vec-daemon bi-encoder fallback)

    Advantage over BM25-only: recovers nodes that BM25 misses entirely (zero score)
    but are semantically close to the query (e.g. synonyms, paraphrases, concept drift).

    Fail-safe: if dense_rank_decisions() returns [] (vec-daemon down or no embeddings),
    falls back to BM25-only + semantic rerank (existing behavior).
    """
    # Step 1: BM25 candidates (skip rerank here — we'll do it after RRF)
    bm25_cands = bm25_rank_decisions(
        corpus, query, top_k=top_k * 2,
        skip_rerank=True
    )
    if not bm25_cands:
        return []

    # Step 2: Dense candidates
    dense_cands = dense_rank_decisions(corpus, query, top_k=top_k * 2)

    if not dense_cands:
        # Dense unavailable — fall back to BM25 with rerank
        if len(bm25_cands) >= top_k + 2:
            bm25_cands = semantic_rerank_filter(bm25_cands, query, top_k=top_k)
        return bm25_cands[:top_k]

    # Step 3: RRF merge
    merged = rrf_merge(bm25_cands, dense_cands, k_rrf=60)

    # Step 4: Semantic rerank on merged pool
    if len(merged) >= top_k + 2:
        merged = semantic_rerank_filter(merged, query, top_k=top_k)

    return merged[:top_k]


# ── G2: Docs BM25 ────────────────────────────────────────────────

def _extra_doc_files(project_dir):
    """Return extra files to include in the docs index (project-agnostic)."""
    # MEMORY.md: ~/.claude/projects/{slug}/memory/MEMORY.md
    # Claude uses leading-dash slug: /home/foo/bar → -home-foo-bar
    slug = project_dir.replace("/", "-")
    memory_md = os.path.expanduser(f"~/.claude/projects/{slug}/memory/MEMORY.md")
    # NS-Hub north-star.md: injects active milestones as G2-DOCS context so Claude Code
    # automatically surfaces the current NS milestone when working on a hub project.
    hub_project_slug = Path(project_dir).name  # e.g. "CTX" from /home/.../Project/CTX
    north_star_md = os.path.expanduser(f"~/.hub/projects/{hub_project_slug}/north-star.md")
    candidates = [
        os.path.join(project_dir, "CLAUDE.md"),
        os.path.join(project_dir, "README.md"),
        memory_md,
        north_star_md,  # NS-Hub integration: active milestones as retrieval context
    ]
    return [p for p in candidates if os.path.exists(p)]


def chunk_document(filename, content):
    """Split by ## headers; each chunk = 'filename § header\\nbody'."""
    chunks = []
    parts = re.split(r"\n(?=## )", content)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        lines = part.split("\n", 1)
        header = re.sub(r"^#+\s*", "", lines[0].strip())
        body = lines[1].strip() if len(lines) > 1 else ""
        text = f"{filename} § {header}\n{body}"
        if len(text) > 50:
            chunks.append(text[:2500])
    return chunks


def build_docs_bm25(project_dir):
    """Build BM25 index over docs/research/*.md + CLAUDE.md + MEMORY.md.
    Strategy: full-doc (no chunking) — A/B test 2026-04-11 confirms +9.1% recall@5
    vs header-chunked approach (0.758 vs 0.667 on 33 paraphrase pairs).
    Full-doc wins on temporal/open-set/perf queries where answers span multiple sections.
    """
    # Name-keyed dict: root extras (README/CLAUDE/MEMORY) win on collision
    # Prevents duplicate entries when docs/research/ has same-name placeholders
    units_by_name = {}
    docs_dir = Path(project_dir) / "docs" / "research"
    if docs_dir.exists():
        for md_file in sorted(docs_dir.glob("*.md")):
            try:
                text = f"{md_file.name}\n{md_file.read_text()}"
                if len(text) > 50:
                    units_by_name.setdefault(md_file.name, text)
            except Exception:
                pass

    for fpath in _extra_doc_files(project_dir):
        try:
            p = Path(fpath)
            text = f"{p.name}\n{p.read_text()}"
            if len(text) > 50:
                units_by_name[p.name] = text  # root always wins
        except Exception:
            pass

    all_units = list(units_by_name.values())

    if not all_units or not HAS_BM25:
        return None, []
    tokenized = [tokenize(u) for u in all_units]
    return BM25Okapi(tokenized), all_units



# Korean→English expansion for G2-DOCS BM25 path (iter 44).
# Docs corpus is English; Korean queries must be expanded to match.
# These are CTX/ML domain terms that appear frequently in research docs.
_KO_EN_DOCS = {
    "하이브리드": "hybrid", "밀집": "dense", "검색": "search,retrieve",
    "재색인": "reindex", "인용": "citation", "거짓": "false",
    "양성": "positive", "시멘틱": "semantic", "지연": "latency",
    "시간": "time,latency", "수준": "tier,level",
    "벡터": "vector,embedding", "마이그레이션": "migration",
    "임베딩": "embedding", "벤치마크": "benchmark,eval",
    "메모리": "memory", "코드베이스": "codebase",
    "데이터베이스": "database", "오래된": "stale,staleness",
    "측정": "measure,probe", "비율": "rate,ratio",
    "성능": "performance,latency", "업그레이드": "upgrade",
    "노드": "node", "병합": "merge", "구현": "implementation",
    "분석": "analysis,evaluation", "아키텍처": "architecture",
    "평가": "eval,evaluate,benchmark", "프레임워크": "framework",
    "알고리즘": "algorithm", "최적화": "optimize,optimization",
    "자동": "auto,automatic", "색인": "index", "인덱스": "index",
}


def _expand_ko_en_docs(tokens):
    """Expand Korean tokens via _KO_EN_DOCS for G2-DOCS BM25 queries."""
    expanded = list(tokens)
    for t in tokens:
        mapping = _KO_EN_DOCS.get(t)
        if mapping:
            expanded.extend(mapping.split(","))
    return list(dict.fromkeys(expanded))


def bm25_search_docs(project_dir, query, top_k=5):
    """Return top-k docs most relevant to query (full-doc BM25, no chunking).
    Query-side stopword filter prevents conversational fillers from dominating.
    Korean queries are expanded via _KO_EN_DOCS before scoring (iter 44).
    """
    if not query.strip():
        return []
    bm25, units = build_docs_bm25(project_dir)
    if not bm25:
        return []
    query_tokens = tokenize(query, drop_stopwords=True)
    query_tokens = _expand_ko_en_docs(query_tokens)  # Korean→English expansion
    if not query_tokens:
        return []
    scores = bm25.get_scores(query_tokens)
    ranked = sorted(range(len(units)), key=lambda i: scores[i], reverse=True)
    # threshold=1.0: full-doc scores for relevant queries are 3.0-6.0; 0.0 = no overlap
    # adaptive floor (2026-04-24): also drop anything below 35% of top score
    top_score = float(max(scores)) if len(scores) else 0.0
    floor = max(1.0, top_score * 0.35)
    bm_filtered = [units[i] for i in ranked[:top_k * 2] if scores[i] >= floor]
    # Semantic rerank (2026-04-24 iter 2): dedupes BM25-surface hits from different meanings
    if len(bm_filtered) > top_k:
        # Each unit is "filename\ncontent"; wrap as dict for the reranker
        cand_dicts = [{"subject": u.split("\n", 1)[0], "text": u[:400]} for u in bm_filtered]
        reranked_dicts = semantic_rerank_filter(cand_dicts, query, top_k=top_k)
        # Map back to original units by subject (filename)
        subject_to_unit = {u.split("\n", 1)[0]: u for u in bm_filtered}
        return [subject_to_unit[d["subject"]] for d in reranked_dicts if d["subject"] in subject_to_unit]
    return bm_filtered[:top_k]


# ── G2-DOCS: Hybrid BM25+Dense Search ───────────────────────────

_docs_emb_cache_state = {}  # in-memory: {"key": str, "units_emb": [...]}


def _docs_cache_key(units):
    """Stable cache key based on doc filenames (sorted join → simple hash)."""
    filenames = sorted(u.split("\n", 1)[0] for u in units)
    key_str = "|".join(filenames)
    # stdlib-only fingerprint: sum of char ords mod 10^10
    return str(sum(ord(c) * (i + 1) for i, c in enumerate(key_str)) % (10 ** 10))


def embed_docs_units(units, cache_path):
    """Pre-embed docs corpus. Returns list of dicts:
    {"hash": filename, "text": unit_string, "emb": list_or_[]}.

    Caches to cache_path; invalidates when doc set changes.
    Fail-safe: items without embedding skip dense but still contribute via BM25.
    """
    key = _docs_cache_key(units)

    if _docs_emb_cache_state.get("key") == key:
        return _docs_emb_cache_state["units_emb"]

    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text())
            if cached.get("key") == key:
                _docs_emb_cache_state.update(cached)
                return cached["units_emb"]
        except Exception:
            pass

    # Embed each unit (filename + first 400 chars as subject)
    units_emb = []
    for u in units:
        filename = u.split("\n", 1)[0]
        preview = u[:400]
        emb = _vec_embed(preview)
        units_emb.append({"hash": filename, "text": u, "emb": emb or []})

    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps({"key": key, "units_emb": units_emb}))
    except Exception:
        pass

    _docs_emb_cache_state.update({"key": key, "units_emb": units_emb})
    return units_emb


def dense_rank_docs(units_emb, query, top_k=10):
    """Dense first-stage retrieval for docs corpus.

    units_emb: list of {"hash": filename, "text": unit_str, "emb": list}
    Returns top_k dicts ranked by cosine similarity, or [] if vec-daemon down.
    """
    q_emb = _vec_embed(query)
    if not q_emb:
        return []
    scored = []
    for item in units_emb:
        emb = item.get("emb")
        if not emb:
            continue
        cos = _cosine(q_emb, emb)
        if cos > 0.0:
            scored.append((cos, item))
    if not scored:
        return []
    scored.sort(key=lambda x: -x[0])
    return [item for _, item in scored[:top_k]]


def hybrid_search_docs(project_dir, query, top_k=5):
    """Hybrid BM25+dense RRF search over docs/research/*.md corpus.

    Same pipeline as hybrid_rank_decisions() for G1:
      1. BM25 top-(top_k*2) candidates (threshold filtered)
      2. Dense top-(top_k*2) via pre-embedded corpus (vec-daemon cosine)
      3. RRF merge (k=60)
      4. Semantic rerank (BGE/vec-daemon) on merged pool

    Fail-safe: dense unavailable → BM25+semantic rerank (existing behavior).
    Returns list of unit strings — same format as bm25_search_docs().
    """
    bm25, units = build_docs_bm25(project_dir)
    if not bm25 or not units or not query.strip():
        return []

    query_tokens = tokenize(query, drop_stopwords=True)
    query_tokens = _expand_ko_en_docs(query_tokens)  # Korean→English expansion (iter 44)
    if not query_tokens:
        return []

    # Step 1: BM25 candidates
    scores = bm25.get_scores(query_tokens)
    top_score = float(max(scores)) if len(scores) else 0.0
    if top_score < 1.0:
        return []
    floor = max(1.0, top_score * 0.35)
    ranked = sorted(range(len(units)), key=lambda i: scores[i], reverse=True)
    bm25_filtered = [units[i] for i in ranked[:top_k * 2] if scores[i] >= floor]
    if not bm25_filtered:
        return []

    bm25_dicts = [{"hash": u.split("\n", 1)[0], "text": u} for u in bm25_filtered]

    # Step 2: Dense candidates (pre-embedded corpus, 1 vec-daemon call for query)
    cache_path = Path(project_dir) / ".omc" / "docs_corpus_emb.json"
    units_emb = embed_docs_units(units, cache_path)
    dense_dicts = dense_rank_docs(units_emb, query, top_k=top_k * 2)

    if not dense_dicts:
        # Fallback: BM25 + semantic rerank
        if len(bm25_filtered) > top_k:
            cand_dicts = [{"subject": u.split("\n", 1)[0], "text": u[:400]}
                          for u in bm25_filtered]
            reranked = semantic_rerank_filter(cand_dicts, query, top_k=top_k)
            subj_map = {u.split("\n", 1)[0]: u for u in bm25_filtered}
            return [subj_map[d["subject"]] for d in reranked if d["subject"] in subj_map]
        return bm25_filtered[:top_k]

    # Step 3: RRF merge
    merged = rrf_merge(bm25_dicts, dense_dicts, k_rrf=60)

    # Step 4: Semantic rerank on merged pool
    if len(merged) >= top_k + 2:
        reranked = semantic_rerank_filter(merged, query, top_k=top_k)
        return [item.get("text", "") for item in reranked if item.get("text")]

    return [item.get("text", "") for item in merged[:top_k] if item.get("text")]


# ── G2: Code File Discovery ──────────────────────────────────────

_STOP_WORDS = {
    "the","a","an","is","are","was","were","be","been","have","has","had",
    "do","does","did","will","would","could","should","may","might","can",
    "to","of","in","for","on","with","at","by","from","as","into",
    "it","this","that","i","you","he","she","we","they","me",
    "and","or","but","not","no","if","then","else","when","where","how","what",
    "해줘","해","바람","좀","것","수","있","없","하다","되다","이","그","저","뭐","어떻게",
    "기능","작업","관련","파일","코드","문서","수정","추가","변경","확인","돌려봐",
    "올려","실행","해봐","분석","개선","확인해",
}
_KO_EN = {
    "검색": "search,retrieve,find", "엔진": "engine,retriever",
    "벤치마크": "benchmark,eval", "평가": "eval,evaluate",
    "트리거": "trigger", "분류": "classify,classifier",
    "밀도": "dense,density", "테스트": "test",
    "결과": "result", "스코어": "score",
    "그래프": "graph", "다운스트림": "downstream",
    "외부": "external,reeval", "정확도": "accuracy,precision",
    "이메일": "email,mail", "발송": "send,outreach",
    "대시보드": "dashboard,admin", "구독": "subscription,subscribe",
    "인증": "auth,authenticate", "로그인": "login,signin",
    "사용자": "user,member", "데이터베이스": "database,schema",
    "함수": "function,handler", "컴포넌트": "component",
    "페이지": "page,route", "설정": "config,settings",
    "환경": "env,environment", "서버": "server,backend",
    "실험": "experiment,trial", "배포": "deploy,deployment",
    "오류": "error,exception", "버그": "bug,error",
    "성능": "performance,latency", "최적화": "optimize,cache",
    "알림": "notification,alert", "권한": "permission,auth",
    "훅": "hook", "메모리": "memory", "인덱스": "index",
}
_CODE_EXT = {
    ".py", ".js", ".ts", ".tsx", ".jsx", ".go", ".rs", ".java",
    ".sh", ".bash", ".yaml", ".yml", ".toml", ".sql", ".css", ".html",
    ".c", ".cpp", ".h", ".rb", ".php", ".swift", ".kt",
}
_SKIP_PREFIXES = (".omc/", "docs/", "benchmarks/results/", "tests/fixtures/")


def extract_keywords(prompt):
    """Extract meaningful keywords from prompt; expand Korean→English."""
    words = re.findall(r'[a-zA-Z_][a-zA-Z0-9_]{2,}|[가-힣]{2,}', prompt)
    keywords = []
    for w in words:
        if w.lower() in _STOP_WORDS or len(w) < 2:
            continue
        if re.match(r'[가-힣]', w) and w in _KO_EN:
            keywords.extend(_KO_EN[w].split(","))
        else:
            keywords.append(w)
    return keywords[:8]


def find_db(project_dir):
    """Locate codebase-memory-mcp SQLite DB for this project."""
    cache_dir = os.path.expanduser("~/.cache/codebase-memory-mcp")
    if not os.path.isdir(cache_dir):
        return None
    slug = project_dir.replace("/", "-").lstrip("-")
    db_path = os.path.join(cache_dir, f"{slug}.db")
    if os.path.exists(db_path):
        return db_path
    for f in os.listdir(cache_dir):
        if f.endswith(".db") and os.path.basename(project_dir).lower() in f.lower():
            return os.path.join(cache_dir, f)
    return None


_REINDEX_LOCK = os.path.expanduser("~/.cache/codebase-memory-mcp/.reindex_in_progress")
_STALE_THRESHOLD_HOURS = 24

# ── Citation Probe (iter 40) ──────────────────────────────────────────────────
# Logs retrieved nodes per turn to .omc/retrieval_log.jsonl.
# A separate analysis script (benchmarks/eval/citation_probe.py) cross-references
# these logs with vault.db chat history to compute actual citation rate per node type.
# Goal: measure what fraction of retrieved G1/G2 nodes Claude actually cites in responses.

def log_retrieved_nodes(project_dir, session_id, prompt, block, items):
    """
    Append a retrieval event to .omc/retrieval_log.jsonl.

    Args:
        project_dir: project root path
        session_id: Claude session ID (from input_data)
        prompt: user prompt (first 120 chars stored)
        block: "g1_decisions" | "g2_docs" | "g2_prefetch" | "g2_hooks"
        items: list of dicts, each with at minimum {"id": str, "text": str}
               g1: {"id": hash, "text": subject, "date": date}
               g2_docs: {"id": filename, "text": unit_preview}
               g2_prefetch: {"id": fpath, "text": f"{label}:{name}"}
    """
    if not items:
        return
    try:
        import time as _t
        log_path = os.path.join(project_dir, ".omc", "retrieval_log.jsonl")
        entry = {
            "ts": _t.time(),
            "session_id": session_id,
            "prompt_prefix": prompt[:120],
            "block": block,
            "items": items[:10],  # cap at 10 per block
        }
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass  # citation probe is non-critical — never break the main hook


def check_and_trigger_reindex(project_dir, db_path):
    """
    Check if codebase-memory-mcp DB is stale (>24h). If so, spawn an incremental
    reindex in the background (non-blocking). Returns a warning string if stale,
    or None if fresh.

    Uses a lock file to prevent multiple concurrent reindex launches.
    Tool: codebase-memory-mcp cli index_repository '{"repo_path":"...", "mode":"fast"}'
    """
    try:
        import time as _t_mod
        age_hours = (_t_mod.time() - os.path.getmtime(db_path)) / 3600
    except OSError:
        return None

    if age_hours < _STALE_THRESHOLD_HOURS:
        return None  # fresh — no action needed

    age_str = f"{age_hours:.0f}h" if age_hours < 48 else f"{age_hours/24:.1f}d"

    # Check if reindex already running (lock file < 10 min old)
    if os.path.exists(_REINDEX_LOCK):
        try:
            import time as _t_mod
            lock_age = (_t_mod.time() - os.path.getmtime(_REINDEX_LOCK)) / 60
            if lock_age < 10:
                return f"⚠ G2-CODE DB stale ({age_str}) — reindex already running"
        except OSError:
            pass

    # Spawn background reindex
    try:
        import json as _json
        args = _json.dumps({"repo_path": project_dir, "mode": "fast"})
        cmd = ["codebase-memory-mcp", "cli", "index_repository", args]
        subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,  # detach from hook process group
        )
        # Touch lock file
        open(_REINDEX_LOCK, "w").close()
        return f"⚠ G2-CODE DB stale ({age_str}) — auto-reindex triggered (fast mode, background)"
    except Exception:
        return f"⚠ G2-CODE DB stale ({age_str}) — run: codebase-memory-mcp cli index_repository to reindex"


def search_graph_for_prompt(db_path, keywords, limit=5):
    """Query codebase graph nodes matching keywords."""
    if not keywords:
        return []
    try:
        import sqlite3
        db = sqlite3.connect(db_path)
        results, seen = [], set()
        for kw in keywords:
            rows = db.execute(
                "SELECT DISTINCT label, name, file_path FROM nodes "
                "WHERE name LIKE ? AND label IN ('Function','Method','Class') "
                "ORDER BY length(name) ASC LIMIT ?",
                (f"%{kw}%", 3),
            ).fetchall()
            for r in rows:
                key = (r[1], r[2])
                if key not in seen:
                    seen.add(key)
                    results.append(r)
            if len(results) < limit:
                frows = db.execute(
                    "SELECT DISTINCT label, name, file_path FROM nodes "
                    "WHERE file_path LIKE ? AND label IN ('Module','File') "
                    "ORDER BY length(file_path) ASC LIMIT ?",
                    (f"%{kw}%", 2),
                ).fetchall()
                for r in frows:
                    key = (r[1], r[2])
                    if key not in seen:
                        seen.add(key)
                        results.append(r)
        db.close()
        return results[:limit]
    except Exception:
        return []


def search_files_by_grep(project_dir, keywords, limit=5):
    """Fallback: git grep -c to rank files by keyword match count."""
    long_kws = [k for k in keywords if len(k) >= 4 and not re.match(r'[가-힣]', k)]
    if not long_kws:
        return []
    try:
        pattern = "|".join(re.escape(k) for k in long_kws[:4])
        r = subprocess.run(
            ["git", "grep", "-c", "-E", "-i", "--", pattern],
            cwd=project_dir, capture_output=True, text=True, timeout=3,
        )
        if r.returncode != 0:
            return []
        scored = []
        for line in r.stdout.strip().split("\n"):
            if not line.strip():
                continue
            try:
                fpath, count = line.rsplit(":", 1)
                scored.append((int(count), fpath.strip()))
            except ValueError:
                continue
        scored.sort(key=lambda x: -x[0])
        files = [f for _, f in scored]
        code = [
            f for f in files
            if any(f.endswith(ext) for ext in _CODE_EXT)
            and not any(f.startswith(p) for p in _SKIP_PREFIXES)
        ]
        return code[:limit]
    except Exception:
        return []


# ── G2: Hooks File Discovery ─────────────────────────────────────

_HOOKS_DIR = Path.home() / ".claude" / "hooks"
_HOOKS_TRIGGER_KWS = frozenset({
    # English
    "hook", "hooks", "bm25-memory", "bm25_memory", "git-memory", "git_memory",
    "auto-index", "auto_index", "g2-augment", "g2_augment",
    "userPromptSubmit", "sessionstart", "posttooluse",
    # Korean
    "훅", "후크",
})


def _build_hook_doc(py_path: Path) -> str:
    """Extract file name + docstring + function/class signatures from a hook file."""
    try:
        src = py_path.read_text(errors="replace")
    except Exception:
        return ""
    lines = src.split("\n")
    header_lines = []
    # Collect: module docstring (first triple-quoted block) + def/class lines
    in_docstring = False
    docstring_done = False
    for line in lines[:80]:
        stripped = line.strip()
        if not docstring_done:
            if stripped.startswith('"""') or stripped.startswith("'''"):
                in_docstring = not in_docstring
                header_lines.append(stripped[:200])
                if stripped.count('"""') >= 2 or stripped.count("'''") >= 2:
                    in_docstring = False
                    docstring_done = True
                continue
            if in_docstring:
                header_lines.append(stripped[:200])
                if '"""' in stripped or "'''" in stripped:
                    in_docstring = False
                    docstring_done = True
                continue
            else:
                docstring_done = True
        if stripped.startswith("def ") or stripped.startswith("class "):
            header_lines.append(stripped[:120])
    return f"{py_path.name}\n" + "\n".join(header_lines)


def search_hooks_files(query: str, limit: int = 3):
    """BM25-search ~/.claude/hooks/*.py for hook function/filename matches."""
    if not _HOOKS_DIR.exists() or not HAS_BM25:
        return []
    py_files = sorted(_HOOKS_DIR.glob("*.py"))
    if not py_files:
        return []
    docs = [(p, _build_hook_doc(p)) for p in py_files]
    docs = [(p, d) for p, d in docs if d]
    if not docs:
        return []
    tokenized = [tokenize(d) for _, d in docs]
    bm25 = BM25Okapi(tokenized)
    scores = bm25.get_scores(tokenize(query))
    ranked = sorted(range(len(docs)), key=lambda i: scores[i], reverse=True)
    return [(docs[i][0], scores[i]) for i in ranked[:limit] if scores[i] > 0]


def _has_hooks_keywords(prompt: str) -> bool:
    """Return True if prompt mentions hook-related terms."""
    low = prompt.lower()
    return any(kw in low for kw in _HOOKS_TRIGGER_KWS)


# ── Rich Mode: World Model ────────────────────────────────────────

def get_world_model(project_dir):
    """Load dead-ends and facts from .omc/world-model.json (--rich mode)."""
    wm_path = Path(project_dir) / ".omc" / "world-model.json"
    if not wm_path.exists():
        return [], []
    try:
        wm = json.loads(wm_path.read_text())
    except Exception:
        return [], []
    raw_de = wm.get("dead_ends", [])
    if isinstance(raw_de, dict):
        raw_de = []
    dead_ends = [
        f"  x {de.get('goal','')[:60]} -- {de.get('reason','')[:80]}"
        for de in raw_de[-5:]
    ]
    facts = []
    for fact in wm.get("known_facts", []):
        if isinstance(fact, dict):
            facts.append(f"  * {fact['fact'][:80]}")
        elif isinstance(fact, str) and not any(
            fact.startswith(p) for p in ("paper:", "README:", "uncertain:")
        ):
            facts.append(f"  * {fact[:80]}")
    return dead_ends, facts[-8:]


# ── Session Decisions ─────────────────────────────────────────────

def get_session_decisions(project_dir):
    """Read .omc/session-decisions.md for uncommitted decisions."""
    p = Path(project_dir) / ".omc" / "session-decisions.md"
    if not p.exists():
        return []
    try:
        lines = p.read_text().strip().split("\n")
        return [l.strip() for l in lines if l.strip().startswith(">")][-5:]
    except Exception:
        return []


# ── Pending Decisions (from previous session Stop hook) ──────────

def consume_pending_decisions(project_dir: str):
    """
    이전 세션 stop-decision-capture.py가 남긴 .pending-decisions.json 읽고 삭제.
    Returns list of formatted strings for additionalContext injection, or [].
    """
    slug = project_dir.replace("/", "-")
    pending_path = Path.home() / ".claude" / "projects" / slug / "memory" / ".pending-decisions.json"
    if not pending_path.exists():
        return []

    try:
        payload = json.loads(pending_path.read_text(encoding="utf-8"))
        decisions = payload.get("decisions", [])
        captured_at = payload.get("captured_at", "")[:16]  # "2026-04-14T12:34"
    except Exception:
        try:
            pending_path.unlink(missing_ok=True)
        except Exception:
            pass
        return []

    # 읽은 즉시 삭제 (1회성 전달)
    try:
        pending_path.unlink(missing_ok=True)
    except Exception:
        pass

    if not decisions:
        return []

    lines = [f"[PENDING MEMORY] 이전 세션({captured_at}) 결정 후보 — MEMORY.md 업데이트 필요 여부 판단:"]
    for d in decisions:
        tag = d.get("tag", "")
        ctx = d.get("context", "")
        lines.append(f"  [{tag}] {ctx}")
    lines.append("→ 위 항목 중 MEMORY.md에 없는 중요한 결정이 있으면 즉시 Edit tool로 기록하세요.")
    return lines


# ── Main ──────────────────────────────────────────────────────────

def _log_event(event_type, payload):
    """Opt-in telemetry wrapper — silent no-op if gate off. Never breaks hook path."""
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from _ctx_telemetry import log_event
        log_event(event_type, payload)
    except Exception:
        pass


def _count_tokens(text: str) -> int:
    """Approximate token count using whitespace + punctuation splitting.

    Uses a simple heuristic: split on whitespace, then count each non-empty
    token. This approximates GPT-style BPE tokenization (~1 token per 4 chars
    for English, ~1.5-2 chars for CJK). For CTX's purposes (relative usage
    tracking, not billing-grade precision), this is sufficient and has zero
    dependencies.
    """
    if not text:
        return 0
    # Split on whitespace and filter empty strings
    tokens = text.split()
    # Refine: split punctuation off words (e.g. "hello." → "hello" + ".")
    refined = []
    for t in tokens:
        parts = re.findall(r'[A-Za-z0-9_\-]+|[^\sA-Za-z0-9]', t)
        refined.extend(parts)
    return len(refined) if refined else len(tokens)


def _nudge_upgrade_if_old() -> None:
    """Print a one-time upgrade nudge if the installed ctx version is < 0.3.26.
    Versions before 0.3.26 had a broken .pth placement (no auto-collection).
    Shows at most once per machine; never blocks or raises."""
    _nudge_flag = Path.home() / ".claude" / "ctx-upgrade-nudge-shown"
    if _nudge_flag.exists():
        return
    try:
        import importlib.metadata as _meta
        ver = _meta.version("ctx-retriever")
        parts = ver.split(".")
        major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0
        if (major, minor, patch) < (0, 3, 26):
            import sys as _sys
            _sys.stderr.write(
                f"[CTX] You have v{ver} — auto data-collection requires v0.3.26+. "
                "Run: pip install --upgrade ctx-retriever\n"
            )
            _nudge_flag.touch()
    except Exception:
        pass


def _retry_install_pending() -> None:
    """Retry a queued install ping if present (< 7 days old). Solution 4 of
    collection-guarantee research (docs/ns-replies/20260518-M31-collection-guarantee.md).
    Handles the case where _send_install_ping() failed due to network timeout."""
    _pending = Path.home() / ".claude" / "ctx-install-pending.json"
    _revoke = Path.home() / ".claude" / "ctx-telemetry-revoke"
    if not _pending.exists() or _revoke.exists():
        return
    try:
        import time as _t, urllib.request as _ur
        data = json.loads(_pending.read_text())
        if (_t.time() - data.get("queued_at", 0)) > 7 * 86400:
            _pending.unlink(missing_ok=True)
            return
        payload = data.get("payload", {})
        if not payload:
            return
        _db = __import__("os").environ.get("HUB_CTX_TURSO_URL", "https://hub-ctx-jaytoone.aws-us-west-2.turso.io")
        _tok = __import__("os").environ.get("HUB_CTX_TURSO_TOKEN", "")
        if not _tok:
            return
        sql = ("INSERT INTO ctx_session_aggregates (schema_version,user_id,session_id_hash,"
               "ts_date,total_turns,session_outcome,ctx_version) VALUES (?,?,?,?,?,?,?)")
        args = [
            {"type": "text",    "value": payload.get("schema_version", "v1.7")},
            {"type": "text",    "value": payload.get("user_id", "unknown")},
            {"type": "text",    "value": payload.get("session_id_hash", f"retry:{int(_t.time())}")},
            {"type": "text",    "value": payload.get("ts_date", str(__import__("datetime").date.today()))},
            {"type": "integer", "value": str(payload.get("total_turns", 0))},
            {"type": "text",    "value": payload.get("session_outcome", "INSTALL_PING")},
            {"type": "text",    "value": payload.get("ctx_version", "unknown")},
        ]
        body = json.dumps({"requests": [{"type": "execute", "stmt": {"sql": sql, "args": args}}]}).encode()
        req = _ur.Request(f"{_db}/v2/pipeline", data=body,
            headers={"Authorization": f"Bearer {_tok}", "Content-Type": "application/json"}, method="POST")
        with _ur.urlopen(req, timeout=5) as r:
            json.load(r)
        _pending.unlink(missing_ok=True)
    except Exception:
        pass  # silent — never block the main hook


def main():
    import time as _time
    _t_start = _time.perf_counter()
    # Opportunistic retry of any queued install ping (non-blocking, silent on failure)
    try:
        _retry_install_pending()
    except Exception:
        pass
    # Nudge old-version users to upgrade (one-time, silent on failure)
    try:
        _nudge_upgrade_if_old()
    except Exception:
        pass
    try:
        input_data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    project_dir = os.environ.get("CLAUDE_PROJECT_DIR", os.getcwd())
    prompt = input_data.get("prompt", "")
    _session_id = input_data.get("session_id", "")
    _prompt_tokens = _count_tokens(prompt)

    # A/B scaffold: control arm skips injection entirely (CTX_AB_DISABLE=1).
    # Dashboard uses the logged ab_skipped events to count control-arm sessions.
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from _ctx_telemetry import ab_disabled, log_event
        if ab_disabled():
            log_event("ab_skipped", {"hook": "bm25-memory", "reason": "CTX_AB_DISABLE"})
            sys.exit(0)
    except Exception:
        pass
    # /ctx rate command: short-circuit before BM25 retrieval
    _rate_result = _process_ctx_rate(prompt, project_dir, _session_id)
    if _rate_result:
        _r_rating, _r_label, _r_preview = _rate_result
        _r_ctx = f"[CTX-RATE] {_r_label} Feedback logged"
        if _r_preview:
            _r_ctx += f' for: "{_r_preview}…"'
        _r_ctx += ". Calibration data saved to .omc/feedback_log.jsonl."
        json.dump({"hookSpecificOutput": {"hookEventName": "UserPromptSubmit",
                                          "additionalContext": _r_ctx}}, sys.stdout)
        sys.stdout.flush()
        return

    # M59: Trivial-prompt / hub-trigger gate.
    # Short hub dispatch words like "go" / "ㄱㄱ" carry no semantic content.
    # Injecting G2-DOCS/G2-PREFETCH for these produces irrelevant context and
    # wastes tokens — the hub's northstar-execute-inject.py already provides
    # proper context for these continuation signals.
    # Rule: skip G2 if (a) prompt is in _TRIVIAL_PROMPTS  OR
    #                    (b) stripped word-count ≤ 2 AND not a /ctx command
    # G1 (decision memory) is always processed — it's cheap and still relevant.
    _prompt_stripped = prompt.strip()
    _prompt_lower = _prompt_stripped.lower()
    # M59: skip G2 only for known trivial hub-trigger words (exact set match).
    # M1181: also treat hub wake-message patterns as trivial so stone text is used as query instead.
    _HUB_WAKE_PAT = re.compile(
        r'(tasks?\s+ready|get_pending_task|mcp__ns.hub__get_pending_task)', re.I
    )
    _skip_g2 = _prompt_lower in _TRIVIAL_PROMPTS or bool(_HUB_WAKE_PAT.search(_prompt_stripped))

    # M59-HUB: when prompt is trivial, fetch current queued hub stone text and use
    # THAT as the G2 query instead of skipping entirely. This makes CTX work correctly
    # in hub-ctx sessions where "go" is the dispatch signal and the real intent is
    # described in the stone text being worked on.
    _hub_stone_query = None
    if _skip_g2:
        try:
            import urllib.request as _url_req, re as _re_hub
            # Try to detect hub project from project_dir name
            _proj_name = os.path.basename(os.path.abspath(project_dir))
            _hub_resp = _url_req.urlopen(
                f"http://127.0.0.1:9000/api/northstar/{_proj_name}/milestones",
                timeout=0.5
            ).read().decode()
            _hub_data = json.loads(_hub_resp)
            _hub_ms = _hub_data.get("milestones", _hub_data) if isinstance(_hub_data, dict) else _hub_data
            _queued = [m for m in _hub_ms
                       if not m.get("done") and m.get("status") not in ("done", "pending_confirmation")
                       and m.get("text", "").strip()
                       and not m.get("text", "").startswith("[검수]")]
            if _queued:
                _stone = _queued[0]
                _stone_text = _stone.get("text", "")[:200].strip()
                # Also append latest user message from stone conversation for richer query
                _stone_conv = _stone.get("conversation", [])
                _last_user = next(
                    (c.get("text", "") for c in reversed(_stone_conv)
                     if c.get("role") == "user" and c.get("text", "").strip()),
                    ""
                )
                if _last_user and _last_user.strip().lower() not in _TRIVIAL_PROMPTS:
                    _stone_text = _stone_text + " " + _last_user[:100].strip()
                _hub_stone_query = _stone_text
                _hub_stone_mid = _stone.get("id", "?")  # track stone ID for display
        except Exception:
            pass
    if _hub_stone_query:
        _skip_g2 = False  # don't skip — use stone text as query
    else:
        _hub_stone_mid = None

    lines = []
    _blocks_fired = []  # for final hook_invoked telemetry summary
    _retrieval_meta = {"ts": _time.time(), "blocks": {}}  # retrieval_event telemetry
    _block_tokens = {}  # per-block injected token counts → token_usage telemetry
    _injected_files = []  # M61: track injected file paths for retrieval-feedback hook

    # 0a. Pending decisions from previous session (Stop hook → queue file)
    pending = consume_pending_decisions(project_dir)
    if pending:
        lines.extend(pending)

    # 0b. Session decisions (uncommitted notes)
    session_notes = get_session_decisions(project_dir)
    if session_notes:
        lines.append("[SESSION NOTES (미커밋 판단)]")
        lines.extend(session_notes)

    # 1. G1: Hybrid BM25+dense RRF over decision corpus (2026-04-26)
    # Uses hybrid_rank_decisions() when vec-daemon is up (BM25+dense→RRF→rerank).
    # Falls back to bm25_rank_decisions() if dense unavailable — explicit coverage.
    # Eval: BM25=0.966, Hybrid=0.983 (+1.7pp) on 59-query G1 bench (172 commits).
    _t_g1 = _time.perf_counter()
    corpus = get_decision_corpus(project_dir)
    g1_header = ""
    if corpus:
        relevant = hybrid_rank_decisions(corpus, prompt, top_k=7)
        if relevant:
            # Build forced display header (mechanically injected, not advisory)
            first_subj = relevant[0]["subject"][:70]
            rest_count = len(relevant) - 1
            g1_header = f'> **G1** (time memory): "{first_subj}" and {rest_count} more'

            _g1_start = len(lines)
            lines.append(
                f"[RECENT DECISIONS] (BM25: top {len(relevant)} of {len(corpus)})"
            )
            for c in relevant:
                date = c.get("date", "")
                subj = c["subject"]
                prefix = f"  > [{date}] " if date else "  > "
                lines.append(f"{prefix}{subj}")
            _block_tokens["g1_decisions"] = _count_tokens("\n".join(lines[_g1_start:]))
            _log_event("block_fired", {
                "hook": "bm25-memory", "block": "g1_decisions",
                "count": len(relevant),
                "tokens": _block_tokens["g1_decisions"],
                "duration_ms": int((_time.perf_counter() - _t_g1) * 1000),
            })
            _blocks_fired.append("g1")
            _retrieval_meta["blocks"]["g1_decisions"] = {
                "candidates": len(corpus),
                "returned": len(relevant),
                "retrieval_method": "HYBRID" if (_VEC_SOCK.exists() and not _VEC_DISABLED) else "BM25",
                "duration_ms": int((_time.perf_counter() - _t_g1) * 1000),
                "query_type": _classify_query_type(prompt),
            }
            # Citation probe: log G1 retrieved nodes
            log_retrieved_nodes(project_dir, _session_id, prompt, "g1_decisions", [
                {"id": c.get("hash", c["subject"][:20]), "text": c["subject"], "date": c.get("date", "")}
                for c in relevant
            ])

    # 2. G2: BM25 over project docs
    # M59: skip entirely when trivial hub-trigger word detected (_skip_g2=True)
    g2_files = []
    g2_keywords = []
    if _skip_g2:
        _log_event("g2_skipped", {
            "hook": "bm25-memory", "reason": "trivial_prompt",
            "prompt_preview": _prompt_stripped[:40],
        })
    if prompt and not _skip_g2:
        _t_g2d = _time.perf_counter()
        # M59-HUB: use hub stone text as query when prompt was trivial
        _g2_query = _hub_stone_query if _hub_stone_query else prompt
        _g2_qt = _classify_query_type(_g2_query)
        doc_chunks = hybrid_search_docs(project_dir, _g2_query, top_k=10)
        if not doc_chunks:
            # Corpus quality warning: no docs indexed yet
            _docs_dir = Path(project_dir) / "docs" / "research"
            _n_docs = len(list(_docs_dir.glob("*.md"))) if _docs_dir.exists() else 0
            if _n_docs < 5:
                lines.append(f"⚠ [G2-DOCS] thin corpus ({_n_docs} docs in docs/research/); retrieval quality low.")
        # FlashRank reranking + relevance gate (M58/M54.6): apply after BM25+dense RRF,
        # before L-RAG skip. Fires when flashrank is installed OR CTX_RERANK=1 is set.
        # Gracefully degrades: any failure returns original order unchanged.
        # M54.6 (Tokalator): score_threshold=0.3 filters low-relevance items to reduce
        # injected tokens 20-30% (context engineering principle arXiv:2604.08290).
        _flashrank_applied = False
        _fr_filtered_count = 0
        if doc_chunks and (_FLASHRANK_AVAILABLE or os.environ.get("CTX_RERANK") == "1"):
            _fr_before_count = len(doc_chunks)
            _fr_fallback = doc_chunks[:3]  # safety fallback if all items filtered
            _fr_threshold = float(os.environ.get("CTX_RERANK_THRESHOLD", "0.3"))
            doc_chunks, _fr_filtered_count = _flashrank_rerank(
                prompt, doc_chunks, top_k=len(doc_chunks), score_threshold=_fr_threshold
            )
            if not doc_chunks:
                # All items scored below threshold — fall back to original top-3
                doc_chunks = _fr_fallback
                _fr_filtered_count = _fr_before_count - len(doc_chunks)
            _flashrank_applied = True
        # L-RAG entropy skip (M42): if 2+ of top-3 doc filenames already appear
        # in prompt, injecting them would over-anchor — skip to reduce redundancy
        if doc_chunks:
            _prompt_lower = prompt.lower()
            _top_fnames = []
            for _c in doc_chunks[:3]:
                _fn = _c.strip().split("\n", 1)[0].split(" §")[0].strip().lower()
                if _fn:
                    _top_fnames.append(_fn)
            _already_known = sum(1 for _fn in _top_fnames if _fn in _prompt_lower)
            if _already_known >= 2:
                lines.append(
                    f"[CTX-SKIP] G2-DOCS skipped: {_already_known}/3 top results already in prompt "
                    f"(L-RAG entropy skip — reduces over-anchoring)"
                )
                doc_chunks = []
        if doc_chunks:
            _displayed = doc_chunks[:5]
            _hidden = len(doc_chunks) - len(_displayed)
            _g2d_start = len(lines)
            if _flashrank_applied and _fr_filtered_count > 0:
                _fr_threshold_display = float(os.environ.get("CTX_RERANK_THRESHOLD", "0.3"))
                _retrieval_label = (
                    f"BM25+dense RRF + FlashRank rerank "
                    f"(filtered: {_fr_before_count}→{len(doc_chunks)} items, threshold={_fr_threshold_display})"
                )
            elif _flashrank_applied:
                _retrieval_label = "BM25+dense RRF + FlashRank rerank"
            else:
                _retrieval_label = "BM25+dense RRF"
            # M59-HUB: show query source so dashboard and user know what was used
            if _hub_stone_query and _hub_stone_mid:
                _query_source = f"hub stone [{_hub_stone_mid}]: \"{_hub_stone_query[:50]}…\""
            else:
                _query_source = f"user prompt: \"{prompt[:40]}\"" if len(prompt) > 40 else f"user prompt: \"{prompt}\""
            lines.append(f"[G2-DOCS] [{_g2_qt}] ({_retrieval_label} relevant research docs, top-{len(_displayed)}, best-last)"
                         f"\n  ↳ query source: {_query_source}")
            # Position reranking: reverse display so highest-relevance result is last
            # (Liu et al. 2023 "Lost in the Middle" — LLMs recall end-of-context best)
            for rank, chunk in enumerate(reversed(_displayed), 1):
                actual_rank = len(_displayed) - rank + 1
                chunk_lines = chunk.strip().split("\n")
                header = chunk_lines[0]  # "filename § section"
                fname = header.split(" §")[0].strip()
                if fname and fname not in g2_files:
                    g2_files.append(fname)
                    if fname not in _injected_files:
                        _injected_files.append(fname)
                snippet = ""
                if len(chunk_lines) > 1:
                    # Find first non-empty content line
                    for cl in chunk_lines[1:]:
                        cl = cl.strip()
                        if cl and not cl.startswith("#"):
                            snippet = cl[:120]
                            break
                lines.append(f"  > [#{actual_rank}] {header}")
                if snippet:
                    lines.append(f"    {snippet}")
            if _hidden > 0:
                lines.append(f"  … and {_hidden} more results (BM25 threshold passed)")
            _block_tokens["g2_docs"] = _count_tokens("\n".join(lines[_g2d_start:]))
            _log_event("block_fired", {
                "hook": "bm25-memory", "block": "g2_docs",
                "count": len(_displayed),
                "tokens": _block_tokens["g2_docs"],
                "duration_ms": int((_time.perf_counter() - _t_g2d) * 1000),
            })
            _blocks_fired.append("g2_docs")
            _retrieval_meta["blocks"]["g2_docs"] = {
                "candidates": len(doc_chunks),
                "returned": len(_displayed),
                "retrieval_method": "HYBRID" if (_VEC_SOCK.exists() and not _VEC_DISABLED) else "BM25",
                "duration_ms": int((_time.perf_counter() - _t_g2d) * 1000),
                "query_type": _g2_qt,
            }
            # Citation probe: log G2-DOCS retrieved nodes
            log_retrieved_nodes(project_dir, _session_id, prompt, "g2_docs", [
                {"id": chunk.strip().split("\n")[0].split(" §")[0].strip(), "text": chunk.strip().split("\n")[0][:80]}
                for chunk in doc_chunks
            ])

    # 3. G2: Code file discovery (graph → grep fallback)
    # M59: also skipped for trivial prompts (same _skip_g2 flag as G2-DOCS)
    if prompt and not _skip_g2:
        # M59-HUB: use hub stone text as keyword source when prompt was trivial
        _g2p_query = _hub_stone_query if _hub_stone_query else prompt
        keywords = extract_keywords(_g2p_query)
        g2_keywords = keywords[:3]
        if keywords:
            _t_g2p = _time.perf_counter()
            db_path = find_db(project_dir)
            if db_path:
                # Staleness check: auto-reindex if DB > 24h old
                stale_warn = check_and_trigger_reindex(project_dir, db_path)
                if stale_warn:
                    lines.append(stale_warn)
                graph_results = search_graph_for_prompt(db_path, keywords)
                if graph_results:
                    _g2p_start = len(lines)
                    _prefetch_src = f"hub:{_hub_stone_mid}" if _hub_stone_query and _hub_stone_mid else "prompt"
                    lines.append(f"[G2-PREFETCH] Related code for '{' '.join(keywords[:3])}' (src: {_prefetch_src}):")
                    seen_files = set()
                    for label, name, fpath in graph_results:
                        lines.append(f"  {label}: {name} @ {fpath}")
                        seen_files.add(fpath)
                        if fpath and fpath not in _injected_files:
                            _injected_files.append(fpath)
                    if seen_files:
                        lines.append(f"  Start with: {', '.join(sorted(seen_files)[:3])}")
                    # M56: expand with 2-hop import neighbors + directory-tier candidates
                    _ig_db = None
                    try:
                        _ig_db = build_import_graph(project_dir)
                        if _ig_db:
                            _bfs_all = set()
                            _dir_all = set()
                            _seen_rel = {os.path.relpath(f, project_dir) for f in seen_files}
                            for _fp in list(seen_files)[:3]:
                                _rel = os.path.relpath(_fp, project_dir)
                                _bfs, _dir = get_graph_neighbors(_ig_db, _rel, hops=2)
                                _bfs_all.update(_bfs)
                                _dir_all.update(_dir)
                            _bfs_all -= _seen_rel
                            _dir_all -= _seen_rel
                            _dir_all -= _bfs_all
                            if _bfs_all:
                                lines.append(
                                    f"  [import-graph] 2-hop: {', '.join(sorted(_bfs_all)[:4])}"
                                )
                            if _dir_all:
                                lines.append(
                                    f"  [dir-tier]: {', '.join(sorted(_dir_all)[:3])}"
                                )
                    except Exception:
                        pass
                    # M54.8: Entity KG lookup — find files containing matching functions/classes
                    try:
                        _eq_terms = _entity_query_terms(prompt or "")
                        if _eq_terms and _ig_db:
                            _entity_hits = []
                            _entity_seen = set()
                            for _eterm in _eq_terms:
                                for _efile, _ename, _ekind in get_entity_neighbors(_ig_db, _eterm, max_results=3):
                                    if _efile not in _entity_seen:
                                        _entity_seen.add(_efile)
                                        _entity_hits.append((_eterm, _ename, _ekind, _efile))
                            if _entity_hits:
                                for _eterm, _ename, _ekind, _efile in _entity_hits[:4]:
                                    lines.append(
                                        f"  [entity-graph] {_ekind} '{_ename}' in {_efile}"
                                    )
                    except Exception:
                        pass
                    _block_tokens["g2_prefetch"] = _count_tokens("\n".join(lines[_g2p_start:]))
                    _log_event("block_fired", {
                        "hook": "bm25-memory", "block": "g2_prefetch",
                        "count": len(graph_results),
                        "tokens": _block_tokens["g2_prefetch"],
                        "duration_ms": int((_time.perf_counter() - _t_g2p) * 1000),
                    })
                    _blocks_fired.append("g2_prefetch")
            else:
                # Fallback: git grep
                files = search_files_by_grep(project_dir, keywords)
                if files:
                    _g2g_start = len(lines)
                    lines.append(f"[G2-GREP] Files matching '{' '.join(keywords[:3])}' (grep):")
                    for f in files:
                        lines.append(f"  {f}")
                    lines.append(f"  Start with: {', '.join(files[:3])}")
                    _block_tokens["g2_grep"] = _count_tokens("\n".join(lines[_g2g_start:]))
                    _log_event("block_fired", {
                        "hook": "bm25-memory", "block": "g2_grep",
                        "count": len(files),
                        "tokens": _block_tokens["g2_grep"],
                        "duration_ms": int((_time.perf_counter() - _t_g2p) * 1000),
                    })
                    _blocks_fired.append("g2_grep")

    # 3b. G2: Hooks file discovery (when hook-related terms in prompt)
    # M59: also skipped for trivial prompts
    if prompt and not _skip_g2 and _has_hooks_keywords(prompt):
        _t_g2h = _time.perf_counter()
        hook_results = search_hooks_files(prompt)
        if hook_results:
            _g2h_start = len(lines)
            lines.append(f"[G2-HOOKS] Hook files matching '{prompt[:40]}':")
            for hp, score in hook_results:
                lines.append(f"  {hp}  (score={score:.1f})")
            _block_tokens["g2_hooks"] = _count_tokens("\n".join(lines[_g2h_start:]))
            _log_event("block_fired", {
                "hook": "bm25-memory", "block": "g2_hooks",
                "count": len(hook_results),
                "tokens": _block_tokens["g2_hooks"],
                "duration_ms": int((_time.perf_counter() - _t_g2h) * 1000),
            })
            _blocks_fired.append("g2_hooks")

    # 4. World model (--rich)
    if RICH:
        dead_ends, facts = get_world_model(project_dir)
        if dead_ends:
            lines.append("[DEAD-ENDS -- do not retry]")
            lines.extend(dead_ends)
        if facts:
            lines.append("[KNOWN FACTS]")
            lines.extend(facts)

    if lines:
        # Prepend forced display header (mechanically enforced, replaces CLAUDE.md advisory)
        header_lines = []
        if g1_header:
            header_lines.append(g1_header)
        if g2_files or g2_keywords:
            files_str = ", ".join(f"`{f}`" for f in g2_files[:3]) if g2_files else "(docs BM25)"
            kw_str = " ".join(g2_keywords[:3]) if g2_keywords else ""
            via_str = f' — found via "{kw_str}"' if kw_str else ""
            header_lines.append(f"> **G2** (space search): {files_str}{via_str}")
        # Daemon degradation warnings — shown only when socket is absent
        _daemon_warns = []
        if not _VEC_DISABLED and not _VEC_SOCK.exists():
            _daemon_warns.append("vec-daemon down — BM25-only mode (semantic rerank disabled)")
        if _USE_CROSS_ENCODER and not _BGE_SOCK.exists():
            _daemon_warns.append("bge-daemon down — cross-encoder rerank disabled")
        if _daemon_warns:
            header_lines.append("> **⚠ Semantic layer**: " + " | ".join(_daemon_warns))
        if header_lines:
            lines = header_lines + [""] + lines

        output = {
            "hookSpecificOutput": {
                "hookEventName": "UserPromptSubmit",
                "additionalContext": "\n".join(lines),
            }
        }
        json.dump(output, sys.stdout)
        sys.stdout.flush()
        if header_lines:
            print("\n".join(header_lines), file=sys.stderr)
            sys.stderr.flush()

    # Final summary event: one record per hook invocation (outside `if lines:`)
    _total_injected_tokens = sum(_block_tokens.values())
    _log_event("hook_invoked", {
        "hook": "bm25-memory",
        "duration_ms": int((_time.perf_counter() - _t_start) * 1000),
        "prompt_len": len(prompt) if prompt else 0,
        "prompt_tokens": _prompt_tokens,
        "injected_tokens": _total_injected_tokens,
        "block_tokens": _block_tokens,
    })
    # Token usage event: standalone record for usage-based tracking
    if _total_injected_tokens > 0:
        _log_event("token_usage", {
            "hook": "bm25-memory",
            "prompt_tokens": _prompt_tokens,
            "injected_tokens": _total_injected_tokens,
            "block_tokens": _block_tokens,
            "blocks_fired": _blocks_fired,
        })

    # ── P1: record what we injected for utility-rate measurement ─────
    # Stop hook reads this + the latest assistant turn + substring-matches
    # each item's distinctive tokens. Not stored when dashboard-internal.
    if os.environ.get("CTX_DASHBOARD_INTERNAL") != "1":
        try:
            # Preview = first 120 chars, newlines stripped (same privacy surface
            # as vault.db which already stores full prompts; this just makes the
            # dashboard see new prompts *before* vault.db incremental fires on Stop).
            preview = (prompt or "")[:120].replace("\n", " ").replace("\r", " ")
            # Full prompt stored too so the dashboard's node-details pane can
            # show the whole message before vault.db catches up.
            prompt_full_str = (prompt or "").replace("\r", "")
            # Derive the project basename from CLAUDE_PROJECT_DIR (fallback to cwd)
            try:
                _proj = os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()
                _project_name = os.path.basename(_proj.rstrip("/")) if _proj else None
            except Exception:
                _project_name = None
            injection = {
                "ts": _time.time(),
                "session_id": _session_id or "",
                "prompt_len": len(prompt) if prompt else 0,
                "prompt_preview": preview,
                "prompt_full": prompt_full_str,
                "project": _project_name,
                "files": list(_injected_files),  # M61: for retrieval-feedback hit/miss
                "items": [],
            }
            # Collect distinctive substrings from emitted blocks.
            # Each item is (block, signature) — signature is a 4-20 char
            # distinctive substring the assistant's response can echo.
            # Meta/filler words from commit subjects that never represent a topic.
            # Drops CTX's internal taxonomy (live-infinite, iter, goal_vN) + conventional
            # commit prefixes + common English verbs — anything that would generate
            # false-positive matches against unrelated responses.
            _META_WORDS = frozenset([
                "live-infinite", "live-inf", "omc-live", "iter", "live",
                "goal_v1", "goal_v2", "goal_v3", "goal",
                "feat", "fix", "refactor", "perf", "docs", "test", "chore",
                "success", "section", "update", "add", "remove", "change",
                "fixed", "added", "removed", "completed",
            ])
            # Header-row detector for "> **G1/G2**" and similar markdown headers
            _is_header_line = lambda st: st.startswith("> **") and "** (" in st

            def _extract_content_tokens(subject: str, n: int = 5) -> list:
                """Pick up to N distinctive content tokens from a commit subject.
                Filters meta words, pure digits, punctuation-only fragments.
                Prefers longer words (more specific = better substring hit rate)."""
                candidates = []
                for w in subject.split():
                    w_clean = w.strip(".,()[]{}:;!?\"'").lower()
                    if len(w_clean) < 4:
                        continue
                    if w_clean in _META_WORDS:
                        continue
                    if w_clean.replace("/", "").replace(".", "").replace("-", "").isdigit():
                        continue   # 20260402, 58/∞, etc.
                    # Keep case of original for better citation-style match
                    candidates.append(w.strip(".,()[]{}:;!?\"'"))
                # Dedup preserving order, sort by length desc for specificity
                seen = set()
                uniq = [t for t in candidates if not (t.lower() in seen or seen.add(t.lower()))]
                uniq.sort(key=lambda t: -len(t))
                return uniq[:n]

            for line in lines:
                s = line.strip()
                # Skip markdown headers like "> **G1** (time memory): ..." — they are
                # not items, they're section labels that would leak into signatures.
                if _is_header_line(s):
                    continue
                # G1 decisions: "> [YYYY-MM-DD] subject" — capture date for age-based wow trigger
                if s.startswith("> [") and "]" in s:
                    close_idx = s.index("]")
                    date_str = s[3:close_idx]
                    subj = s[close_idx + 1:].strip()
                    tokens = _extract_content_tokens(subj, n=5)
                    if tokens:
                        item = {
                            "block": "g1",
                            "tokens": tokens,
                            "subject": subj[:200],  # preserved for semantic scoring
                        }
                        if len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-":
                            item["date"] = date_str
                        injection["items"].append(item)
                # G2-DOCS entries: "  > filename.md" → filename AS signature AND
                # also extract date-token + topic words from filename for more hit
                # surface (e.g. "20260411-g1-generalization-validation.md" also
                # matches on "generalization" / "validation").
                elif s.startswith("> ") and (".md" in s or s.endswith(".py")):
                    fname = s.lstrip("> ").strip().split(" §")[0].split()[0]
                    if fname:
                        # filename + its stem words as tokens
                        stem = fname.rsplit(".", 1)[0]
                        parts = [p for p in stem.replace("-", " ").replace("_", " ").split()
                                 if len(p) >= 4 and not p.isdigit()]
                        tokens = [fname] + parts[:4]
                        # Subject for semantic: the filename's natural-language form
                        subject = " ".join(parts) if parts else fname
                        injection["items"].append({
                            "block": "g2_docs", "tokens": tokens, "subject": subject[:200]
                        })
                # G2-PREFETCH: symbol names (function/class) + their path
                elif ": " in s and "@" in s and any(k in s for k in ("Function:", "Class:", "Method:", "Module:", "File:")):
                    try:
                        name = s.split(":", 1)[1].split("@")[0].strip()
                        path = s.split("@", 1)[1].strip() if "@" in s else ""
                        path_base = path.rsplit("/", 1)[-1] if path else ""
                        tokens = [t for t in [name, path_base] if t and len(t) >= 4]
                        if tokens:
                            injection["items"].append({
                                "block": "g2_prefetch",
                                "tokens": tokens,
                                "subject": f"{name} in {path}"[:200],
                            })
                    except Exception:
                        pass
            Path(os.path.expanduser("~/.claude/last-injection.json")).write_text(
                json.dumps(injection)
            )
            # Write retrieval metadata for utility-rate.py → retrieval_event schema
            _retrieval_meta["vec_daemon_up"] = _VEC_SOCK.exists() and not _VEC_DISABLED
            _retrieval_meta["bge_daemon_up"] = _BGE_SOCK.exists() and bool(
                os.environ.get("CTX_CROSS_ENCODER", "1") != "0"
            )
            _retrieval_meta["query_char_count"] = len(prompt) if prompt else 0
            _retrieval_meta["session_id"] = _session_id or ""
            Path(os.path.expanduser("~/.claude/last-retrieval-meta.json")).write_text(
                json.dumps(_retrieval_meta)
            )
        except Exception:
            pass


if __name__ == "__main__":
    main()
