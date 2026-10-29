# jobagent

Job-alert triage that costs as few tokens as possible. It pulls fresh postings
straight from company job boards, throws away the ones you'd never apply to
using free Python checks, has a small model score the rest against a fact bank
you wrote, and pushes only the good matches to your phone with
[ntfy](https://ntfy.sh).

Standard library Python only. The metered Anthropic backend is the one
optional dependency (`pip install anthropic`).

## Pipeline

```
fetch / import      public ATS feeds and remote boards, or any CSV       free
  -> gates          YOE, title, location, stack, duplicates              free
  -> triage         ONE model call per job: extract + score + evidence   cheap
  -> verify         every evidence quote must exist in facts.md          free
  -> coverage       JD keyword coverage per resume variant               free
  -> notify         ntfy push: urgent now, the rest in a digest          free
  -> tailor         rewrite for one job, only when you type YES          on demand
```

Two rules hold throughout:

- **Free checks run before any token is spent.** Seniority, location, years of
  experience, stack mismatch and duplicates are all rejected in pure Python.
- **The model cannot make up your experience.** Each requirement it marks as
  met must quote a line from `facts.md` verbatim. Code checks the quote exists,
  and a requirement whose quote is missing is graded as a gap. Nothing escalates
  to the expensive tailoring step on its own.

## Sources

Configured in `sources.py`. All are public, documented feeds meant for this
kind of use; nothing logs in or scrapes a bot-protected site.

| Kind | Sources |
|---|---|
| Company ATS feeds (full JD text, one line per company in `companies.txt`) | Greenhouse, Lever, Ashby, SmartRecruiters, Workable, Recruitee |
| Remote boards | RemoteOK, Remotive, We Work Remotely, Himalayas, Arbeitnow, HN "Who is hiring" |
| Needs a free key | Adzuna (`ADZUNA_APP_ID`, `ADZUNA_APP_KEY`) |
| Optional | Your own job-alert emails over IMAP (`JOBAGENT_IMAP_USER`, `JOBAGENT_IMAP_PASS`) — title and link only, so these are listed for you to open rather than scored |

Sites that are login-walled or bot-protected are deliberately not scraped; use
their own email alerts with the IMAP source instead.

## Model backends

Chosen in `jobagent.py` with `TRIAGE_BACKEND` and `TAILOR_BACKEND`. Trade-offs
are written up at the top of `backends.py`.

| Backend | Cost | Notes |
|---|---|---|
| `gemini` | free tier | Schema enforced server-side. Needs `GEMINI_API_KEY`. |
| `claude_code` | your Claude Code subscription | Runs `claude -p` headless; uses quota, not API money. |
| `api` | ~$0.005 per job | Metered Anthropic API with prompt caching. Needs `ANTHROPIC_API_KEY`. |
| `offline` | none | Keyword matching only, no network. Under-matches rather than over-matches. |

`triage --dry-run` prices a batch with the free token-counting endpoint before
anything is spent, and `stats` shows tokens and cost by model and day.

## Setup

```bash
cp facts.example.md facts.md              # your fact bank — be honest, it is checked
cp companies.example.txt companies.txt    # the company boards you want to watch
python3 jobagent.py init
python3 jobagent.py topic --test          # prints your private ntfy topic and sends a test push
```

Install the ntfy app, subscribe to the topic it printed, and you should see the
test push. The topic is a random 192-bit token stored with `0600` permissions in
`ntfy_topic.txt`; treat it like a password, since anyone who knows it can read
your alerts.

Put one `.txt` per resume variant in `resumes/` if you want coverage scores.

## Daily use

```bash
python3 jobagent.py fetch                 # pull new postings
python3 jobagent.py gates                 # free filters
python3 jobagent.py triage --limit 20     # score and push the good ones
python3 jobagent.py queue                 # what is worth applying to
python3 jobagent.py show JOB_ID           # full assessment with evidence
python3 jobagent.py tailor JOB_ID         # rewrite for one job, asks first
python3 jobagent.py apply JOB_ID          # or: skip JOB_ID [reason]
```

Or leave it running in focus mode: it polls every few minutes, pushes only
urgent matches immediately, and sends the rest as a digest at each break.

```bash
python3 jobagent.py focus --poll 10
```

## Configuration

Everything you are expected to edit is in the `CONFIG — EDIT ME` block at the
top of `jobagent.py`: title and stack reject lists, target locations, the
maximum years of experience, score thresholds, and the focus and break lengths.
The shipped values are an example profile; replace them with your own.

## Privacy

`facts.md`, `companies.txt`, `resumes/*.txt`, `jobs.csv`, the SQLite database
and your ntfy topic are all git-ignored. Job descriptions and your fact bank go
to whichever model backend you choose; with `offline` nothing leaves your
machine.
