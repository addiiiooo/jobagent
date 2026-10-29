#!/usr/bin/env python3
"""
sources.py — pull jobs from every board that exposes a usable feed.

Run via `python3 jobagent.py fetch`, or standalone for probing.

WHAT IS AND IS NOT REACHABLE
  Working public APIs (no key, no login, intended for consumption):
    greenhouse, lever, ashby, smartrecruiters, workable, recruitee
    -> these are the ATSes behind most Indian product companies and startups.
       One entry per company in companies.txt. This is the deepest source:
       you get the full JD text, and you see the req the moment it is posted,
       before it propagates to any aggregator.
    remoteok, remotive, weworkremotely, himalayas, arbeitnow, hn
    -> remote-first boards, global, free.
  Needs a free key:
    adzuna -> genuine India coverage, aggregates many boards. 
  NOT reachable by script:
    naukri, indeed, wellfound, instahyre, cutshort, hirist
    -> bot-protected and/or login-walled, and scraping them breaks their ToS.
       Use their own email alerts instead and turn on the `email` source
       below, which reads them over IMAP. Those rows arrive as title +
       company + link with no JD text, so they are NOT scored by the model;
       they land in `queue --manual` for you to open yourself.
"""

import html
import json
import os
import re
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import jobagent as ja

HERE = os.path.dirname(os.path.abspath(__file__))
COMPANIES = os.path.join(HERE, "companies.txt")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) jobagent/1.0"
TIMEOUT = 25
MAX_BYTES = 8_000_000
POLITE_SLEEP = 0.6           # seconds between requests to one host

# A row is kept only if it looks India-based or remote.
INDIA_OR_REMOTE = [
    "india", "bengaluru", "bangalore", "hyderabad", "chennai", "pune", "mumbai",
    "gurgaon", "gurugram", "noida", "delhi", "kochi", "cochin", "kolkata",
    "ahmedabad", "jaipur", "trivandrum", "thiruvananthapuram", "coimbatore",
    "remote", "anywhere", "worldwide", "work from home", "distributed",
]

ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "")
ADZUNA_QUERIES = ["software engineer", "backend developer", "data engineer",
                  "python developer", "devops engineer", "platform engineer"]

# IMAP ingestion of Naukri / Indeed / Wellfound alert mails (optional).
IMAP_HOST = "imap.gmail.com"
IMAP_USER = os.environ.get("JOBAGENT_IMAP_USER", "")
IMAP_PASS = os.environ.get("JOBAGENT_IMAP_PASS", "")   # Gmail app password
IMAP_FOLDER = os.environ.get("JOBAGENT_IMAP_FOLDER", "JobAlerts")


# ----------------------------- plumbing ----------------------------------

def get(url, headers=None, data=None, timeout=TIMEOUT):
    req = urllib.request.Request(url, data=data, method="POST" if data else "GET")
    req.add_header("User-Agent", UA)
    req.add_header("Accept", "application/json, text/xml, */*")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        if r.geturl().split(":")[0] != "https":
            raise ValueError("refused non-https redirect")
        return r.read(MAX_BYTES)


def get_json(url, headers=None, data=None):
    return json.loads(get(url, headers, data).decode("utf-8", "replace"))


TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"[ \t]*\n[ \t]*")


def detag(s):
    if not s:
        return ""
    if not isinstance(s, str):
        s = str(s)
    if "&lt;" in s or "&amp;lt;" in s:      # escaped markup (Greenhouse does this)
        s = html.unescape(s)
    s = re.sub(r"<\s*(br|/p|/div|/li|/h\d)\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"<\s*li[^>]*>", "\n- ", s, flags=re.I)
    s = TAG_RE.sub(" ", s)
    s = html.unescape(s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = WS_RE.sub("\n", s)
    return re.sub(r"\n{3,}", "\n\n", s).strip()


def flat(v):
    """Locations arrive as str, None, dict or list depending on the ATS."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return ", ".join(flat(x) for x in
                         (v.get("city"), v.get("region"), v.get("state"),
                          v.get("country"), v.get("name"), v.get("location"))
                         if x)
    if isinstance(v, (list, tuple)):
        return ", ".join(flat(x) for x in v if x)
    return str(v)


def india_or_remote(*fields):
    blob = ja.norm(" ".join(flat(f) for f in fields))
    return any(t in blob for t in INDIA_OR_REMOTE)


def row(source, jid, title, company, location, url, jd, require_location=True):
    location = flat(location)
    if require_location and not india_or_remote(location, title):
        return None
    return dict(job_id=f"{source[:3]}-{jid}", title=flat(title).strip(),
                company=flat(company).strip(), location=location.strip(),
                url=url, jd_text=(jd or "")[:ja.MAX_JD_CHARS], source=source)


def insert(conn, rows, status="new"):
    added = 0
    for r in rows:
        if not r or not r["title"]:
            continue
        try:
            conn.execute(
                "INSERT INTO jobs (job_id,title,company,location,url,jd_text,"
                "status,source,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (r["job_id"], r["title"], r["company"], r["location"], r["url"],
                 r["jd_text"], status, r["source"], ja.now(), ja.now()))
            added += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return added


# ------------------------- ATS company boards -----------------------------
# All six are documented public endpoints returning JSON with no auth.

def greenhouse(token):
    d = get_json(f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true")
    out = []
    for j in d.get("jobs", []):
        out.append(row("greenhouse", j["id"], j.get("title"), token,
                       (j.get("location") or {}).get("name"),
                       j.get("absolute_url"), detag(j.get("content"))))
    return out


def lever(token):
    d = get_json(f"https://api.lever.co/v0/postings/{token}?mode=json")
    out = []
    for j in d:
        cat = j.get("categories") or {}
        jd = j.get("descriptionPlain") or detag(j.get("description"))
        for lst in (j.get("lists") or []):
            jd += "\n\n" + (lst.get("text") or "") + "\n" + detag(lst.get("content"))
        out.append(row("lever", j["id"], j.get("text"), token,
                       cat.get("location"), j.get("hostedUrl"), jd))
    return out


def ashby(token):
    d = get_json("https://api.ashbyhq.com/posting-api/job-board/"
                 f"{token}?includeCompensation=true")
    out = []
    for j in d.get("jobs", []):
        out.append(row("ashby", j.get("id"), j.get("title"), token,
                       j.get("location"), j.get("jobUrl"),
                       j.get("descriptionPlain") or detag(j.get("descriptionHtml"))))
    return out


def smartrecruiters(token):
    d = get_json(f"https://api.smartrecruiters.com/v1/companies/{token}/postings?limit=100")
    out = []
    for j in d.get("content", []):
        loc = j.get("location") or {}
        loc_s = ", ".join(x for x in (loc.get("city"), loc.get("country")) if x)
        detail_url = f"https://api.smartrecruiters.com/v1/companies/{token}/postings/{j['id']}"
        jd = ""
        try:
            time.sleep(POLITE_SLEEP)
            dd = get_json(detail_url)
            secs = ((dd.get("jobAd") or {}).get("sections") or {})
            jd = "\n\n".join(detag((secs.get(k) or {}).get("text"))
                             for k in ("companyDescription", "jobDescription",
                                       "qualifications", "additionalInformation"))
        except Exception:  # noqa: BLE001
            pass
        out.append(row("smartrecruiters", j["id"], j.get("name"), token, loc_s,
                       j.get("ref") or f"https://jobs.smartrecruiters.com/{token}/{j['id']}", jd))
    return out


def workable(token):
    d = get_json("https://apply.workable.com/api/v1/widget/accounts/"
                 f"{token}?details=true")
    out = []
    for j in d.get("jobs", []):
        jd = "\n\n".join(detag(j.get(k)) for k in
                         ("description", "requirements", "benefits"))
        out.append(row("workable", j.get("shortcode"), j.get("title"),
                       d.get("name") or token,
                       j.get("location") or j.get("city"), j.get("url"), jd))  # dict ok
    return out


def recruitee(token):
    d = get_json(f"https://{token}.recruitee.com/api/offers/")
    out = []
    for j in d.get("offers", []):
        jd = detag(j.get("description")) + "\n\n" + detag(j.get("requirements"))
        out.append(row("recruitee", j.get("id"), j.get("title"), token,
                       j.get("location") or j.get("city"),
                       j.get("careers_url") or j.get("careers_apply_url"), jd))
    return out


ATS = {"greenhouse": greenhouse, "lever": lever, "ashby": ashby,
       "smartrecruiters": smartrecruiters, "workable": workable,
       "recruitee": recruitee}


def load_companies():
    """companies.txt: one `ats:token` per line, # comments allowed."""
    if not os.path.exists(COMPANIES):
        return []
    out = []
    for line in open(COMPANIES, encoding="utf-8"):
        line = line.split("#")[0].strip()
        if not line or ":" not in line:
            continue
        ats, token = line.split(":", 1)
        ats, token = ats.strip().lower(), token.strip()
        if ats in ATS and token:
            out.append((ats, token))
    return out


def fetch_ats(verbose=True):
    out, seen = [], load_companies()
    for ats, token in seen:
        try:
            got = [r for r in ATS[ats](token) if r]
            out += got
            if verbose:
                print(f"  {ats}:{token:<24} {len(got)} in-scope")
        except urllib.error.HTTPError as e:
            print(f"  {ats}:{token:<24} HTTP {e.code} — wrong token? remove it")
        except Exception as e:  # noqa: BLE001
            print(f"  {ats}:{token:<24} {type(e).__name__}: {e}")
        time.sleep(POLITE_SLEEP)
    return out


# --------------------------- remote boards --------------------------------

def remoteok():
    d = get_json("https://remoteok.com/api")
    out = []
    for j in d:
        if not isinstance(j, dict) or not j.get("position"):
            continue
        loc = j.get("location") or "Remote"
        jd = detag(j.get("description")) + "\n" + " ".join(j.get("tags") or [])
        out.append(row("remoteok", j.get("id"), j.get("position"),
                       j.get("company"), loc, j.get("url"), jd))
    return out


def remotive():
    d = get_json("https://remotive.com/api/remote-jobs?category=software-dev&limit=200")
    return [row("remotive", j.get("id"), j.get("title"), j.get("company_name"),
                j.get("candidate_required_location"), j.get("url"),
                detag(j.get("description"))) for j in d.get("jobs", [])]


ITEM_RE = re.compile(r"<item>(.*?)</item>", re.S | re.I)


def rss_field(item, tag):
    m = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", item, re.S | re.I)
    if not m:
        return ""
    v = m.group(1).strip()
    v = re.sub(r"^<!\[CDATA\[(.*?)\]\]>$", r"\1", v, flags=re.S)
    return html.unescape(v).strip()


def weworkremotely():
    out = []
    for feed in ("remote-programming-jobs", "remote-devops-sysadmin-jobs",
                 "remote-back-end-programming-jobs"):
        try:
            xml = get(f"https://weworkremotely.com/categories/{feed}.rss").decode(
                "utf-8", "replace")
        except Exception as e:  # noqa: BLE001
            print(f"  wwr:{feed} {type(e).__name__}")
            continue
        for item in ITEM_RE.findall(xml):
            title = rss_field(item, "title")
            company, _, role = title.partition(":")
            link = rss_field(item, "link")
            jid = re.sub(r"\W+", "", link)[-24:]
            out.append(row("wwr", jid, (role or title).strip(), company.strip(),
                           rss_field(item, "region") or "Remote", link,
                           detag(rss_field(item, "description"))))
        time.sleep(POLITE_SLEEP)
    return out


def himalayas():
    d = get_json("https://himalayas.app/jobs/api?limit=200")
    return [row("himalayas", j.get("guid") or j.get("id"), j.get("title"),
                j.get("companyName"), ", ".join(j.get("locationRestrictions") or
                                                ["Remote"]),
                j.get("applicationLink") or j.get("url"),
                detag(j.get("description"))) for j in d.get("jobs", [])]


def arbeitnow():
    d = get_json("https://www.arbeitnow.com/api/job-board-api")
    return [row("arbeitnow", j.get("slug"), j.get("title"), j.get("company_name"),
                j.get("location") + (" remote" if j.get("remote") else ""),
                j.get("url"), detag(j.get("description")))
            for j in d.get("data", [])]


def hn_hiring():
    """Latest 'Ask HN: Who is hiring?' comments via the free Algolia API."""
    s = get_json("https://hn.algolia.com/api/v1/search_by_date?"
                 "tags=story&query=%22Ask%20HN%3A%20Who%20is%20hiring%22&hitsPerPage=2")
    out = []
    for story in s.get("hits", [])[:2]:
        sid = story.get("objectID")
        time.sleep(POLITE_SLEEP)
        c = get_json(f"https://hn.algolia.com/api/v1/items/{sid}")
        for kid in (c.get("children") or []):
            txt = detag(kid.get("text"))
            if len(txt) < 120:
                continue
            first = txt.split("\n")[0][:140]
            out.append(row("hn", kid.get("id"), first, "via HN who-is-hiring",
                           txt[:400], f"https://news.ycombinator.com/item?id={kid.get('id')}",
                           txt))
    return out


def adzuna():
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        print("  adzuna: no keys set (ADZUNA_APP_ID / ADZUNA_APP_KEY) — skipped")
        return []
    out = []
    for q in ADZUNA_QUERIES:
        for page in (1, 2):
            params = urllib.parse.urlencode({
                "app_id": ADZUNA_APP_ID, "app_key": ADZUNA_APP_KEY,
                "results_per_page": 50, "what": q, "max_days_old": 3,
                "content-type": "application/json"})
            try:
                d = get_json(f"https://api.adzuna.com/v1/api/jobs/in/search/{page}?{params}")
            except Exception as e:  # noqa: BLE001
                print(f"  adzuna '{q}' p{page}: {type(e).__name__}: {e}")
                break
            for j in d.get("results", []):
                out.append(row("adzuna", j.get("id"), j.get("title"),
                               (j.get("company") or {}).get("display_name"),
                               (j.get("location") or {}).get("display_name"),
                               j.get("redirect_url"), detag(j.get("description"))))
            time.sleep(POLITE_SLEEP)
    return out


FEEDS = {"remoteok": remoteok, "remotive": remotive,
         "weworkremotely": weworkremotely, "himalayas": himalayas,
         "arbeitnow": arbeitnow, "hn": hn_hiring, "adzuna": adzuna}


# ------------------- naukri / indeed / wellfound via email ------------------

LINK_RE = re.compile(r'https?://[^\s"\'<>)]+', re.I)
ALERT_HOSTS = ("naukri.com", "indeed.com", "wellfound.com", "angel.co",
               "instahyre.com", "cutshort.io", "hirist.com", "linkedin.com")


def email_alerts():
    """Read job-alert mails over IMAP. Gives title + link only, no JD text,
    so these rows are stored as 'manual' and never sent to the model."""
    if not (IMAP_USER and IMAP_PASS):
        print("  email: JOBAGENT_IMAP_USER / _PASS not set — skipped")
        return []
    import email as emaillib
    import imaplib
    from email.header import decode_header, make_header

    out = []
    M = imaplib.IMAP4_SSL(IMAP_HOST)
    try:
        M.login(IMAP_USER, IMAP_PASS)
        M.select(IMAP_FOLDER, readonly=True)
        typ, data = M.search(None, "UNSEEN")
        ids = (data[0].split() if typ == "OK" else [])[-40:]
        for i in ids:
            typ, msg_data = M.fetch(i, "(BODY.PEEK[])")
            if typ != "OK":
                continue
            msg = emaillib.message_from_bytes(msg_data[0][1])
            subj = str(make_header(decode_header(msg.get("Subject") or "")))
            body = ""
            for part in msg.walk():
                if part.get_content_type() in ("text/plain", "text/html"):
                    try:
                        body += part.get_payload(decode=True).decode(
                            part.get_content_charset() or "utf-8", "replace")
                    except Exception:  # noqa: BLE001
                        pass
            links = [u for u in LINK_RE.findall(body)
                     if any(h in u for h in ALERT_HOSTS)]
            seen = set()
            for u in links[:25]:
                key = re.sub(r"\W+", "", u)[-28:]
                if key in seen:
                    continue
                seen.add(key)
                host = urllib.parse.urlparse(u).netloc.replace("www.", "")
                out.append(row("email", key, subj[:120], host, "India / alert",
                               u, "", require_location=False))
    finally:
        try:
            M.logout()
        except Exception:  # noqa: BLE001
            pass
    return out


# ------------------------------- driver -----------------------------------

def run(conn, only=None, verbose=True):
    total = 0

    if not only or only == "ats":
        if verbose:
            print("ATS company boards:")
        total += insert(conn, fetch_ats(verbose))

    for name, fn in FEEDS.items():
        if only and only != name:
            continue
        try:
            rows = [r for r in fn() if r]
            n = insert(conn, rows)
            if verbose:
                print(f"  {name:<16} {len(rows)} in-scope, {n} new")
            total += n
        except Exception as e:  # noqa: BLE001
            print(f"  {name:<16} {type(e).__name__}: {e}")
        time.sleep(POLITE_SLEEP)

    if not only or only == "email":
        try:
            rows = [r for r in email_alerts() if r]
            n = insert(conn, rows, status="manual")
            if verbose and rows:
                print(f"  {'email':<16} {len(rows)} link(s), {n} new (manual)")
            total += n
        except Exception as e:  # noqa: BLE001
            print(f"  {'email':<16} {type(e).__name__}: {e}")

    return total


ATS_PATTERNS = [
    ("greenhouse", r"(?:boards|job-boards)\.greenhouse\.io/(?:embed/job_board\?for=)?([a-z0-9_-]+)"),
    ("greenhouse", r"boards-api\.greenhouse\.io/v1/boards/([a-z0-9_-]+)"),
    ("lever", r"jobs\.(?:eu\.)?lever\.co/([a-z0-9_-]+)"),
    ("ashby", r"jobs\.ashbyhq\.com/([a-z0-9_.-]+)"),
    ("smartrecruiters", r"(?:jobs|careers)\.smartrecruiters\.com/([A-Za-z0-9_-]+)"),
    ("workable", r"apply\.workable\.com/(?:api/v1/widget/accounts/)?([a-z0-9_-]+)"),
    ("recruitee", r"([a-z0-9_-]+)\.recruitee\.com"),
]


def discover(url):
    """`sources.py discover https://acme.com/careers` -> which ATS and token."""
    if not url.startswith("http"):
        url = "https://" + url
    try:
        page = get(url).decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        sys.exit(f"could not read {url}: {type(e).__name__}: {e}")
    found, skip = [], {"www", "api", "jobs", "careers", "embed", "boards"}
    for ats, pat in ATS_PATTERNS:
        for m in re.finditer(pat, page, re.I):
            tok = m.group(1)
            if tok.lower() in skip or (ats, tok) in found:
                continue
            found.append((ats, tok))
    if not found:
        print(f"{url}: no supported ATS found in the page source.")
        print("  Open the 'Apply' link and look at the domain it lands on.")
        return
    print(f"{url}:")
    for ats, tok in found:
        print(f"  {ats}:{tok}        <- add this line to companies.txt")


def probe(spec):
    """`sources.py probe greenhouse:acme` — is this token real?"""
    ats, _, token = spec.partition(":")
    if ats not in ATS:
        sys.exit(f"unknown ats '{ats}'. one of: {', '.join(ATS)}")
    try:
        rows = ATS[ats](token)
    except urllib.error.HTTPError as e:
        print(f"{spec}: HTTP {e.code} — token is wrong")
        return
    except Exception as e:  # noqa: BLE001
        print(f"{spec}: {type(e).__name__}: {e}")
        return
    kept = [r for r in rows if r]
    print(f"{spec}: OK — {len(kept)} India/remote role(s)")
    for r in kept[:8]:
        print(f"   {r['title'][:52]:<54}{r['location'][:26]}")


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "probe":
        probe(sys.argv[2])
    elif len(sys.argv) > 2 and sys.argv[1] == "discover":
        discover(sys.argv[2])
    else:
        c = ja.db()
        print(f"\nfetched {run(c, sys.argv[1] if len(sys.argv) > 1 else None)} new row(s)")
