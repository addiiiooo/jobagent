#!/usr/bin/env python3
"""
backends.py — three ways to score a job. Pick one in jobagent.py: BACKEND = ...

  "claude_code"  Shells out to your installed Claude Code in headless mode
                 (claude -p --output-format json). Costs no API money: it runs
                 on the Claude subscription you already pay for. Quality is the
                 same as the API. The real cost is QUOTA — this is the same
                 allowance you use for coding, so a 40-job batch eats into it.
                 Slowest of the three (a few seconds per job).

  "offline"      No network, no model, no cost, no quota. Pure Python keyword
                 matching between the JD and your fact bank. Noticeably dumber:
                 it cannot tell that "columnar warehouse" means BigQuery. It
                 will under-match rather than over-match, which is the safe
                 direction — you lose some good jobs, you never send a bad
                 application. Runs instantly on the M1 Air.

  "gemini"       Google AI Studio free tier. No card, no cost, and it does NOT
                 touch your Claude quota. Its API enforces a JSON schema
                 server-side, so the reply cannot come back malformed — the
                 same guarantee the Anthropic tool-use path gives. Free tier is
                 Flash-class only (Pro left the free tier in April 2026), which
                 is the right class for extract-and-classify anyway.
                 Caveat: free-tier prompts may be used by Google to improve
                 their products. Your fact bank is resume content you publish
                 and JDs are public, so this is low-stakes — but it is true.

  "api"          The metered Anthropic API. Best quality, costs ~$0.005/job.

Everything downstream — evidence verification, scoring, resume selection,
urgency, digests — is identical whichever you pick. Only this file changes.
"""

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

# ------------------------------- config ----------------------------------

CLAUDE_BIN = os.path.expanduser("~/.local/bin/claude")   # not on your PATH
CC_TIMEOUT = 240          # seconds per job
CC_SLEEP = 1.5            # pause between calls, to be gentle on your quota

# Gemini. Key from https://aistudio.google.com/apikey (no card).
GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-flash-latest")
GEMINI_RPM = 9            # stay under the free-tier per-minute limit
GEMINI_HOST = "https://generativelanguage.googleapis.com/v1beta"
_last_calls = []

# Terms the offline scorer knows how to look for in a JD.
VOCAB = [
    # languages
    "python", "sql", "go", "golang", "java", "javascript", "typescript",
    "bash", "shell scripting", "powershell", "scala", "c++", "c#", "ruby",
    # backend
    "fastapi", "flask", "django", "rest api", "graphql", "grpc",
    "microservices", "event-driven", "message queue", "pub/sub", "kafka",
    "celery", "websocket", "api design", "schema design", "migrations",
    "authentication", "oauth", "jwt", "rate limiting", "caching", "redis",
    # data
    "etl", "elt", "data pipeline", "data warehouse", "data modeling",
    "bigquery", "snowflake", "redshift", "databricks", "delta lake",
    "apache beam", "dataflow", "spark", "pyspark", "airflow",
    "cloud composer", "dbt", "partitioning", "clustering", "streaming",
    "batch processing", "data quality", "sql optimization",
    # databases
    "postgresql", "postgres", "mysql", "mongodb", "cosmos db", "dynamodb",
    "nosql", "indexing", "query optimization", "relational",
    # cloud
    "gcp", "google cloud", "aws", "azure", "lambda", "cloud functions",
    "cloud run", "cloud storage", "s3", "ec2", "eks", "gke", "aks", "ecs",
    "serverless", "iam", "vpc",
    # platform
    "docker", "kubernetes", "terraform", "helm", "ansible", "linux",
    "ci/cd", "github actions", "jenkins", "gitlab ci", "cloud build",
    "git", "version control", "infrastructure as code",
    # ops
    "monitoring", "observability", "logging", "alerting", "prometheus",
    "grafana", "datadog", "cloudwatch", "incident response", "on-call",
    "sre", "reliability", "distributed tracing",
    # practice
    "unit testing", "pytest", "integration testing", "tdd", "code review",
    "agile", "scrum", "documentation", "pair programming",
    # ai
    "llm", "rag", "prompt engineering", "machine learning", "mcp",
    # ecosystems you do NOT have — listed so that if one slips past the gates
    # it scores as an explicit gap instead of being invisible
    "spring boot", "spring", "hibernate", "junit", "maven", "gradle", "j2ee",
    "ruby on rails", "rails", "ruby", "rspec", ".net", "asp.net",
    "react", "angular", "vue", "next.js", "php", "laravel",
    "selenium", "cypress", "playwright", "rust", "kotlin", "swift",
    "power bi", "tableau", "alteryx", "sap", "salesforce",
]

# When the JD term is not literally in the fact bank, these count as partial.
SIMILAR = {
    "snowflake": ["bigquery", "data warehouse"],
    "redshift": ["bigquery", "data warehouse"],
    "databricks": ["databricks", "dataflow", "apache beam"],
    "spark": ["pyspark", "apache beam"],
    "dbt": ["etl", "sql"],
    "kafka": ["pub/sub", "event-driven"],
    "message queue": ["pub/sub"],
    "celery": ["pub/sub", "event-driven"],
    "postgresql": ["relational", "sql"],
    "postgres": ["relational", "sql"],
    "mysql": ["relational", "sql"],
    "dynamodb": ["cosmos db", "nosql"],
    "mongodb": ["cosmos db", "nosql"],
    "s3": ["cloud storage"],
    "eks": ["kubernetes"], "gke": ["kubernetes"], "aks": ["kubernetes"],
    "ecs": ["docker", "kubernetes"],
    "cloud functions": ["lambda", "azure covers functions"],
    "flask": ["fastapi"], "django": ["fastapi"],
    "graphql": ["rest api"], "grpc": ["rest api"],
    "gitlab ci": ["github actions", "ci/cd"],
    "jenkins": ["jenkins"],
    "grafana": ["cloud monitoring", "prometheus"],
    "datadog": ["cloud monitoring", "prometheus"],
    "cloudwatch": ["aws", "cloud monitoring"],
    "distributed tracing": ["observability"],
    "sre": ["reliability", "incident response"],
    "tdd": ["pytest", "unit testing"],
    "scrum": ["iterative", "code review"],
    "agile": ["iterative", "code review"],
    "machine learning": ["random forest", "model"],
    "rag": ["claude api"], "llm": ["claude api"], "mcp": ["claude api"],
    "redis": ["caching"],
    "integration testing": ["integration tests"],
    "infrastructure as code": ["terraform"],
    "version control": ["git"],
}


# ---------------------------- claude code ---------------------------------

FENCE = re.compile(r"```(?:json)?\s*(.*?)```", re.S)


def _json_from(text):
    m = FENCE.search(text)
    if m:
        text = m.group(1)
    text = text.strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1:
        return None
    try:
        return json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return None


def claude_code_available():
    return os.path.exists(CLAUDE_BIN) or shutil.which("claude")


def _bin():
    return CLAUDE_BIN if os.path.exists(CLAUDE_BIN) else shutil.which("claude")


def claude_code_call(system, prompt, schema_hint):
    """Run one headless Claude Code turn and return parsed JSON, or None."""
    exe = _bin()
    if not exe:
        raise RuntimeError(f"claude not found at {CLAUDE_BIN}")
    full = (f"{prompt}\n\nReturn ONLY a JSON object with this exact shape, "
            f"no prose, no markdown fences:\n{schema_hint}")
    cmd = [exe, "-p", full,
           "--output-format", "json",
           "--append-system-prompt", system,
           "--disallowedTools", "Write,Edit,Bash,WebFetch,WebSearch,Task",
           "--max-turns", "1"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=CC_TIMEOUT)
    except subprocess.TimeoutExpired:
        raise RuntimeError("claude -p timed out")
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout or "claude failed")[:300])
    try:
        env = json.loads(r.stdout)
    except json.JSONDecodeError:
        raise RuntimeError("claude returned non-JSON envelope: "
                           + r.stdout[:200])
    if env.get("is_error"):
        raise RuntimeError(str(env.get("result"))[:300])
    return _json_from(env.get("result") or "")


TRIAGE_SCHEMA_HINT = """{
  "min_years": number or null,
  "work_mode": "onsite" | "hybrid" | "remote" | "unstated",
  "posting_ok": true | false,
  "posting_issue": "one sentence naming the contradiction, or empty",
  "must_have": ["short requirement phrase", ...],
  "keywords": ["BigQuery", "CI/CD", ...]  (8-20 atomic 1-3 word skills, exactly as the JD spells them),
  "assessments": [
    {"requirement": "...",
     "status": "met" | "partial" | "gap",
     "evidence_quote": "span copied VERBATIM from the fact bank, or \\"\\" if gap"}
  ],
  "red_flags": ["..."],
  "summary": "one sentence, max 22 words"
}"""

EDIT_SCHEMA_HINT = """{
  "headline": "replacement headline line",
  "edits": [{"old": "exact line from the resume", "new": "replacement",
             "grounds": "VERBATIM span from the fact bank",
             "jd_terms": ["..."]}],
  "refused": ["JD terms you could not cover honestly"]
}"""


# ------------------------------- offline ----------------------------------

def _norm(s):
    s = (s or "").lower()
    s = s.replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"')
    s = re.sub(r"[\u2010-\u2015]", "-", s)
    return re.sub(r"\s+", " ", s).strip()


def _has(term, text):
    t = _norm(term)
    if re.fullmatch(r"[a-z0-9 ]+", t):
        return re.search(rf"(?<![a-z0-9]){re.escape(t)}(?![a-z0-9])", text) is not None
    return t in text


def _fact_lines(facts):
    out = []
    for line in facts.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("---"):
            continue
        if line.startswith("- ") or line.startswith("Rules"):
            continue
        if len(line) < 25:
            continue
        out.append(line)
    return out


# Generic practice words carry far less signal than a named technology, so they
# lose ties for the limited requirement slots.
GENERIC = {"monitoring", "logging", "documentation", "code review", "agile",
           "scrum", "git", "version control", "linux", "relational",
           "streaming", "caching", "indexing", "authentication", "migrations",
           "reliability", "batch processing", "pair programming"}


def _best_line(term, lines, jd_terms):
    """Of the fact lines naming this term, cite the one that covers the most
    of what the JD is actually asking for."""
    cands = [l for l in lines if _has(term, _norm(l))]
    if not cands:
        return None
    return max(cands, key=lambda l: (
        sum(1 for t in jd_terms if _has(t, _norm(l))), -len(l)))


NEGATION = re.compile(r"^no\b|\bno production\b|\bconceptual\b|\blight exposure\b|"
                      r"\bcoursework\b|\bnot production\b|\bis a plan\b", re.I)


def offline_triage(title, company, jd_text, facts, max_reqs=16):
    """Keyword scorer. No network, no model, no cost."""
    jd = _norm(jd_text)
    lines = _fact_lines(facts)
    pos = [l for l in lines if not NEGATION.search(l)]
    neg = [l for l in lines if NEGATION.search(l)]
    pos_blob = _norm(" || ".join(pos))

    # Requirements = vocabulary terms the JD actually names, most frequent first
    hits = []
    for t in VOCAB:
        n = len(re.findall(rf"(?<![a-z0-9]){re.escape(_norm(t))}(?![a-z0-9])", jd)) \
            if re.fullmatch(r"[a-z0-9 ]+", _norm(t)) else jd.count(_norm(t))
        if n:
            weight = n * (0.4 if _norm(t) in GENERIC else 1.0)
            hits.append((weight, len(t), t))
    hits.sort(reverse=True)
    must = [t for _, _, t in hits[:max_reqs]]

    assessments = []
    for t in must:
        # An explicit "No X" line in the fact bank is an immediate gap.
        if any(_has(t, _norm(l)) for l in neg):
            assessments.append({"requirement": t, "status": "gap",
                                "evidence_quote": ""})
            continue
        line = _best_line(t, pos, must)
        if line:
            assessments.append({"requirement": t, "status": "met",
                                "evidence_quote": line})
            continue
        alt = next((a for a in SIMILAR.get(_norm(t), [])
                    if _has(a, pos_blob)), None)
        if alt:
            line = _best_line(alt, pos, must)
            assessments.append({"requirement": t, "status": "partial",
                                "evidence_quote": line or ""})
        else:
            assessments.append({"requirement": t, "status": "gap",
                                "evidence_quote": ""})

    yrs = None
    for m in re.finditer(r"(\d{1,2})\s*\+?\s*(?:-|to)?\s*\d{0,2}\s*\+?\s*(?:years?|yrs?)", jd):
        w = jd[max(0, m.start() - 60): m.end() + 60]
        if "experience" in w or "exp" in w:
            v = int(m.group(1))
            if 0 <= v <= 25 and (yrs is None or v < yrs):
                yrs = v

    mode = ("remote" if "remote" in jd else "hybrid" if "hybrid" in jd
            else "onsite" if "on-site" in jd or "onsite" in jd else "unstated")
    met = sum(1 for a in assessments if a["status"] == "met")
    gaps = [a["requirement"] for a in assessments if a["status"] == "gap"]
    return {
        "min_years": yrs, "work_mode": mode, "must_have": must,
        "assessments": assessments,
        "red_flags": ([f"no evidence for: {', '.join(gaps[:4])}"] if gaps else []),
        "summary": f"Keyword match: {met}/{len(assessments)} named technologies "
                   f"are in your fact bank.",
    }


def offline_edit_plan(must_have, resume_text, facts):
    """No generation offline — report what to add by hand, grounded in facts."""
    rt = _norm(resume_text)
    pos = [l for l in _fact_lines(facts) if not NEGATION.search(l)]
    edits, refused = [], []
    for t in must_have:
        if _has(t, rt):
            continue
        line = _best_line(t, pos, must_have)
        if line:
            edits.append({"old": "", "new": f"ADD BY HAND — '{t}': {line}",
                          "grounds": line, "jd_terms": [t]})
        else:
            refused.append(t)
    return {"headline": "", "edits": edits, "refused": refused}


# ------------------------------- gemini -----------------------------------

GEMINI_TRIAGE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "min_years": {"type": "NUMBER", "nullable": True},
        "work_mode": {"type": "STRING",
                      "enum": ["onsite", "hybrid", "remote", "unstated"]},
        "posting_ok": {"type": "BOOLEAN"},
        "posting_issue": {"type": "STRING"},
        "keywords": {"type": "ARRAY", "items": {"type": "STRING"}},
        "must_have": {"type": "ARRAY", "items": {"type": "STRING"}},
        "assessments": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "requirement": {"type": "STRING"},
                    "status": {"type": "STRING",
                               "enum": ["met", "partial", "gap"]},
                    "evidence_quote": {"type": "STRING"},
                },
                "required": ["requirement", "status", "evidence_quote"],
            },
        },
        "red_flags": {"type": "ARRAY", "items": {"type": "STRING"}},
        "summary": {"type": "STRING"},
    },
    "required": ["min_years", "work_mode", "posting_ok", "posting_issue",
                 "must_have", "keywords", "assessments", "red_flags",
                 "summary"],
}

GEMINI_EDIT_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "headline": {"type": "STRING"},
        "edits": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "old": {"type": "STRING"},
                    "new": {"type": "STRING"},
                    "grounds": {"type": "STRING"},
                    "jd_terms": {"type": "ARRAY", "items": {"type": "STRING"}},
                },
                "required": ["old", "new", "grounds", "jd_terms"],
            },
        },
        "refused": {"type": "ARRAY", "items": {"type": "STRING"}},
    },
    "required": ["headline", "edits", "refused"],
}


def gemini_available():
    return bool(GEMINI_KEY)


def _throttle():
    """Keep under GEMINI_RPM without needing a scheduler."""
    now = time.time()
    _last_calls[:] = [t for t in _last_calls if now - t < 60]
    if len(_last_calls) >= GEMINI_RPM:
        wait = 60 - (now - _last_calls[0]) + 0.5
        if wait > 0:
            time.sleep(wait)
        now = time.time()
        _last_calls[:] = [t for t in _last_calls if now - t < 60]
    _last_calls.append(time.time())


# Models tried in order when the primary is overloaded. Free-tier quota is
# per model, so falling back also spreads your daily request budget.
GEMINI_FALLBACKS = [m.strip() for m in os.environ.get(
    "GEMINI_FALLBACKS",
    "gemini-3.7-flash,gemini-3.6-flash,gemini-3.1-flash-lite,gemini-2.5-flash"
    ).split(",")
    if m.strip()]
COOLDOWN_S = 300           # a model that just failed is skipped for this long
_cooldown = {}
_announced = set()


class GeminiHTTP(RuntimeError):
    def __init__(self, code, detail):
        super().__init__(f"HTTP {code}: {detail}")
        self.code = code


def _gemini_once(model, body):
    _throttle()
    req = urllib.request.Request(f"{GEMINI_HOST}/models/{model}:generateContent",
                                 data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("x-goog-api-key", GEMINI_KEY)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        raise GeminiHTTP(e.code, e.read().decode("utf-8", "replace")[:200])
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise GeminiHTTP(0, f"network: {e}")


def gemini_call(system, prompt, schema, model=None, max_tokens=8192):
    """One Gemini call with a server-enforced schema. If the model is overloaded
    (503) or rate limited (429) it is benched for COOLDOWN_S and the next model
    in the chain answers, so one busy model never stalls the pipeline.

    max_tokens is generous on purpose: thinking-capable Flash models spend part
    of this budget on reasoning, and a tight cap returns an empty reply."""
    if not GEMINI_KEY:
        raise RuntimeError("GEMINI_API_KEY not set — get one free at "
                           "https://aistudio.google.com/apikey")
    primary = model or GEMINI_MODEL
    chain = [primary] + [m for m in GEMINI_FALLBACKS if m != primary]
    now = time.time()
    live = [m for m in chain if _cooldown.get(m, 0) <= now]
    order = live or sorted(chain, key=lambda m: _cooldown.get(m, 0))[:1]

    body = json.dumps({
        "system_instruction": {"parts": [{"text": system}]},
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": schema,
            "temperature": 0,
            "maxOutputTokens": max_tokens,
        },
    }).encode("utf-8")

    last = None
    for m in order:
        for attempt in range(2):
            try:
                d = _gemini_once(m, body)
                if m != primary and m not in _announced:
                    print(f"[gemini] using fallback '{m}' while '{primary}' recovers")
                    _announced.add(m)
                return _gemini_parse(d)
            except GeminiHTTP as e:
                last = e
                if e.code == 404:
                    _cooldown[m] = time.time() + 86400
                    print(f"[gemini] '{m}' returned 404 — skipping it. Google said: "
                          f"{str(e)[:160]}")
                    break
                if e.code in (0, 429, 500, 502, 503, 504):
                    if attempt == 0:
                        time.sleep(3)
                        continue
                    _cooldown[m] = time.time() + COOLDOWN_S
                    print(f"[gemini] '{m}' busy (HTTP {e.code}) — benched "
                          f"{COOLDOWN_S // 60} min")
                    break
                raise
    raise RuntimeError(f"every gemini model failed; last error: {last}")


def _gemini_parse(d):
    cands = d.get("candidates") or []
    if not cands:
        fb = (d.get("promptFeedback") or {}).get("blockReason")
        raise RuntimeError(f"no candidate returned (blockReason={fb})")
    c = cands[0]
    if c.get("finishReason") not in (None, "STOP"):
        raise RuntimeError(f"finishReason={c.get('finishReason')}")
    parts = (c.get("content") or {}).get("parts") or []
    text = "".join(p.get("text", "") for p in parts)
    if not text.strip():
        raise RuntimeError("empty response body")
    return _json_from(text)


def gemini_models():
    """List models this key can actually call, so you never guess a name."""
    if not GEMINI_KEY:
        sys.exit("GEMINI_API_KEY not set")
    req = urllib.request.Request(f"{GEMINI_HOST}/models")
    req.add_header("x-goog-api-key", GEMINI_KEY)
    with urllib.request.urlopen(req, timeout=60) as r:
        d = json.loads(r.read().decode("utf-8", "replace"))
    rows = []
    for m in d.get("models", []):
        if "generateContent" not in (m.get("supportedGenerationMethods") or []):
            continue
        name = m["name"].replace("models/", "")
        rows.append((name, m.get("inputTokenLimit", 0)))
    print(f"{len(rows)} model(s) callable with this key:\n")
    flash = [r for r in rows if "flash" in r[0]]
    for name, lim in sorted(flash) or sorted(rows):
        print(f"  {name:<44}{lim:>10,} input tokens")
    print("\nPut one in GEMINI_MODEL. Free tier is Flash-class only — "
          "Pro models left the free tier in April 2026.")


def gemini_test(names):
    """Fire one tiny real request per model and show exactly what came back.
    Being LISTED by `models` does not mean a model will answer for your key."""
    if not GEMINI_KEY:
        sys.exit("GEMINI_API_KEY not set")
    schema = {"type": "OBJECT", "properties": {"ok": {"type": "BOOLEAN"}},
              "required": ["ok"]}
    body = json.dumps({
        "contents": [{"role": "user", "parts": [{"text": 'Return {"ok": true}'}]}],
        "generationConfig": {"responseMimeType": "application/json",
                             "responseSchema": schema, "temperature": 0,
                             "maxOutputTokens": 1024},
    }).encode("utf-8")
    names = names or [GEMINI_MODEL] + [m for m in GEMINI_FALLBACKS if m != GEMINI_MODEL]
    winners = []
    print(f"{'MODEL':<30}{'RESULT':<10}DETAIL")
    for m in names:
        t0 = time.time()
        try:
            d = _gemini_once(m, body)
            out = _gemini_parse(d)
            ok = isinstance(out, dict) and out.get("ok") is True
            print(f"{m:<30}{'OK' if ok else 'ODD':<10}{time.time() - t0:.1f}s  {out}")
            if ok:
                winners.append(m)
        except GeminiHTTP as e:
            print(f"{m:<30}{'HTTP ' + str(e.code):<10}{str(e)[:150]}")
        except Exception as e:  # noqa: BLE001
            print(f"{m:<30}{'ERROR':<10}{str(e)[:150]}")
        time.sleep(1.5)
    print()
    if winners:
        print(f"First model that answered: {winners[0]}")
        print(f"  sed -i '' 's/^export GEMINI_MODEL=.*/export GEMINI_MODEL=\"{winners[0]}\"/' ~/.bash_profile")
        print("  source ~/.bash_profile")
    else:
        print("No model answered. Read the DETAIL column: 429 = quota, 403 = key "
              "or region, 404 = model retired for your project.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "models":
        gemini_models()
    elif len(sys.argv) > 1 and sys.argv[1] == "test":
        gemini_test(sys.argv[2:])
    else:
        print("usage: python3 backends.py models | test [model ...]")
