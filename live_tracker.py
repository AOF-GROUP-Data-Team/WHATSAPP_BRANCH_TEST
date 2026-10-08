"""
Branch Live Tracker - WhatsApp alerts per branch, near real time
One pass per run. Triggered every 5 minutes (github alarm Apps Script via
workflow_dispatch, with an Actions cron as fallback). Set RUN_MINUTES > 0 to
instead keep polling in a loop inside one job.

Live alerts (sent once per item, as soon as it appears):
  cctv          new CCTV violation (template 821185) + its photos
  service_time  new service-time check (820340, 821186, 821187)
  gas           new gas box check (401648, 671643, 472189) + its photos
  missing_task  a task whose due time passed with no submission

Daily digest (once a day after DAILY_HOUR KSA):
  yesterday's service time checks + last area-manager visit, per branch

State (what was already sent) is kept in state/seen.json and committed back
to the repo, so a new run never re-sends old items.

Env: ZENPUT_API_KEY, WA_TOKEN, WA_PHONE_NUMBER_ID, WA_TEST_PHONE
     MODE auto|dry_run   RUN_MINUTES (0 = single pass)   POLL_SECONDS (60, loop only)
     BACKFILL_HOURS (3)  DAILY_HOUR (9)
"""

import os, re, io, sys, json, time, subprocess, requests
from datetime import datetime, timedelta, date
from pathlib import Path
import pytz

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ZENPUT_API_KEY = os.environ.get("ZENPUT_API_KEY", "")
WA_TOKEN       = os.environ.get("WA_TOKEN", "")
WA_PHONE_ID    = os.environ.get("WA_PHONE_NUMBER_ID", "")
TEST_PHONE     = re.sub(r"\D", "", os.environ.get("WA_TEST_PHONE", ""))
API_VERSION    = os.environ.get("WA_API_VERSION", "v25.0")

MODE           = (os.environ.get("MODE", "auto").strip().lower() or "auto")
DRY_RUN        = MODE == "dry_run"
POLL_SECONDS   = max(5, int(os.environ.get("POLL_SECONDS", "") or 60))
TASK_SECONDS   = max(POLL_SECONDS, int(os.environ.get("TASK_POLL_SECONDS", "") or 300))
RUN_MINUTES    = int(os.environ.get("RUN_MINUTES", "") or 0)
SINGLE_PASS    = RUN_MINUTES <= 0
BACKFILL_HOURS = float(os.environ.get("BACKFILL_HOURS", "") or 3)
DAILY_HOUR     = int(os.environ.get("DAILY_HOUR", "") or 9)
SAVE_EVERY_MIN = 10

TZ    = pytz.timezone("Asia/Riyadh")
ZHOST = "https://www.zenput.com"
ZHDR  = {"X-API-TOKEN": ZENPUT_API_KEY}

# 10 pilot branches (Zenput location id). All go to the test phone for now.
BRANCHES = {
    2164025: {"label": "RBRUH B14",  "phone": TEST_PHONE},
    2164032: {"label": "KRRUH B21",  "phone": TEST_PHONE},
    2260889: {"label": "UHDMM B38",  "phone": TEST_PHONE},
    2155654: {"label": "AQRUH B13",  "phone": TEST_PHONE},
    2239240: {"label": "FYJED B32",  "phone": TEST_PHONE},
    2242934: {"label": "HIRJED B33", "phone": TEST_PHONE},
    2263062: {"label": "HSRUH B39",  "phone": TEST_PHONE},
    2250799: {"label": "IRRUH B35",  "phone": TEST_PHONE},
    2164013: {"label": "KHRUH B02",  "phone": TEST_PHONE},
    2243963: {"label": "URRUH B34",  "phone": TEST_PHONE},
}

LIVE_SOURCES = {
    "cctv":         {"name": "مخالفة كاميرات CCTV", "templates": [821185]},
    "service_time": {"name": "قياس وقت الخدمة",     "templates": [820340, 821186, 821187]},
    "gas":          {"name": "فحص بوكس الغاز",      "templates": [401648, 671643, 472189]},
}
SERVICE_TIME_TEMPLATES = LIVE_SOURCES["service_time"]["templates"]
VISIT_TEMPLATES = {663690: "Area Manager MIX visits", 595910: "Lubda - Area Managers Visit",
                   625395: "Area Manager Visual Visit", 868585: "Garatis area manager visit",
                   602710: "Accommodation visit"}
MAX_PHOTOS = 5
PAGE_LIMIT = 50

STATE_FILE = Path("state/seen.json")
START = time.time()
END   = START + RUN_MINUTES * 60


def now_ksa():
    return datetime.now(TZ)


def log(msg):
    print(f"[{now_ksa().strftime('%H:%M:%S')}] {msg}", flush=True)


# ─── STATE ────────────────────────────────────────────────────────────────────
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8")), False
        except Exception:
            pass
    return {"subs": {}, "tasks": {}, "daily": ""}, True


def prune(state):
    cutoff = (now_ksa() - timedelta(days=4)).isoformat()
    for k in ("subs", "tasks"):
        state[k] = {i: t for i, t in state[k].items() if t >= cutoff}


def save_state(state, commit):
    prune(state)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    if not commit or DRY_RUN or not os.environ.get("GITHUB_ACTIONS"):
        return
    cmds = [
        ["git", "config", "user.name", "branch-live-tracker"],
        ["git", "config", "user.email", "actions@users.noreply.github.com"],
        ["git", "add", str(STATE_FILE)],
    ]
    for c in cmds:
        subprocess.run(c, check=False, capture_output=True)
    if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode == 0:
        return
    subprocess.run(["git", "commit", "-m", "tracker state [skip ci]"], check=False, capture_output=True)
    for _ in range(3):
        subprocess.run(["git", "pull", "--rebase", "-X", "theirs"], check=False, capture_output=True)
        if subprocess.run(["git", "push"], capture_output=True).returncode == 0:
            log("state saved to repo")
            return
        time.sleep(3)
    log("WARNING: could not push state")


# ─── ZENPUT ───────────────────────────────────────────────────────────────────
_backoff = 0


def zget(url, params=None):
    """GET with backoff on 429 / 5xx."""
    global _backoff
    for attempt in range(4):
        try:
            r = requests.get(url, headers=ZHDR, params=params, timeout=60)
        except Exception as e:
            log(f"zenput error {e}"); time.sleep(5); continue
        if r.status_code == 429 or r.status_code >= 500:
            _backoff = min(max(_backoff * 2, 30), 600)
            log(f"zenput HTTP {r.status_code} - backing off {_backoff}s")
            time.sleep(_backoff)
            continue
        _backoff = 0
        return r
    return None


def sub_location_id(s):
    loc = (s.get("smetadata") or {}).get("location") or {}
    if loc.get("id"):
        return int(loc["id"])
    for a in s.get("answers", []):
        if a.get("field_type") == "account" and str(a.get("value") or "").isdigit():
            return int(a["value"])
    return None


def sub_time(s):
    raw = (s.get("smetadata") or {}).get("date_submitted_local") or ""
    try:
        d = datetime.fromisoformat(raw)
        return d if d.tzinfo else TZ.localize(d)
    except Exception:
        return None


def latest_submissions(tid, pages=1):
    out = []
    for p in range(pages):
        r = zget(f"{ZHOST}/api/v3/submissions/",
                 {"form_template_id": tid, "limit": PAGE_LIMIT, "start": p * PAGE_LIMIT})
        if not r or r.status_code != 200:
            break
        batch = r.json().get("data", [])
        out.extend(batch)
        if len(batch) < PAGE_LIMIT:
            break
    return out


def submissions_since(tid, oldest_day):
    out = []
    for p in range(60):
        r = zget(f"{ZHOST}/api/v3/submissions/",
                 {"form_template_id": tid, "limit": 100, "start": p * 100})
        if not r or r.status_code != 200:
            break
        batch = r.json().get("data", [])
        if not batch:
            break
        out.extend(batch)
        days = [((s.get("smetadata") or {}).get("date_submitted_local") or "")[:10] for s in batch]
        days = [d for d in days if d]
        if days and max(days) < oldest_day:
            break
        if len(batch) < 100:
            break
    return out


def get_photo(s3_key):
    r = zget(f"{ZHOST}/api/v2/users/current/storage/", {"path": s3_key})
    if not r or r.status_code != 200:
        return None
    url = (r.json().get("data") or {}).get("location")
    if not url:
        return None
    img = requests.get(url, timeout=60)
    if img.status_code != 200:
        return None
    data = img.content
    if len(data) > 4_800_000:
        from PIL import Image
        im = Image.open(io.BytesIO(data)).convert("RGB")
        im.thumbnail((2000, 2000))
        buf = io.BytesIO(); im.save(buf, "JPEG", quality=80); data = buf.getvalue()
    return data


def short_title(title):
    t = str(title or "").strip()
    parts = re.split(r"\s+-\s+|-\s(?=[؀-ۿ])", t)
    ar = [p for p in parts if re.search(r"[؀-ۿ]", p)]
    t = (ar[-1] if ar else t).strip()
    return t if len(t) <= 60 else t[:58] + ".."


def fmt_value(title, val):
    if val is None or val == "" or val == [] or isinstance(val, dict):
        return None
    if isinstance(val, bool):
        return "نعم" if val else "لا"
    if isinstance(val, list):
        val = "، ".join(str(v) for v in val if str(v).strip())
    v = str(val).strip()
    if v.lower() in ("true", "yes"):  return "نعم"
    if v.lower() in ("false", "no"):  return "لا"
    if "service time" in str(title).lower():
        try:
            return str(timedelta(milliseconds=int(float(v)))).split(".")[0]
        except Exception:
            pass
    return v if len(v) <= 150 else v[:148] + ".."


def describe(s):
    lines, photos = [], []
    for a in s.get("answers", []):
        title, ftype, val = a.get("title"), a.get("field_type"), a.get("value")
        if ftype == "account":
            continue
        if ftype == "image":
            items = val if isinstance(val, list) else ([val] if val else [])
            for p in items:
                if isinstance(p, dict) and p.get("s3_key") and len(photos) < MAX_PHOTOS:
                    photos.append((p["s3_key"], short_title(title)))
            continue
        fv = fmt_value(title, val)
        if fv:
            lines.append(f"{short_title(title)}: {fv}")
    return lines, photos


# ─── WHATSAPP ─────────────────────────────────────────────────────────────────
GRAPH = f"https://graph.facebook.com/{API_VERSION}"
WHDR  = {"Authorization": f"Bearer {WA_TOKEN.strip()}"}
BASE  = f"{GRAPH}/{WA_PHONE_ID}"
STATS = {"sent": 0, "failed": 0}


def wa_text(to, body, label):
    if DRY_RUN:
        log(f"DRY {label}\n{body}\n"); return True
    try:
        r = requests.post(f"{BASE}/messages", headers={**WHDR, "Content-Type": "application/json"},
                          json={"messaging_product": "whatsapp", "to": to, "type": "text",
                                "text": {"body": body[:4000]}}, timeout=30)
    except Exception as e:
        log(f"WA error {label}: {e}"); STATS["failed"] += 1; return False
    return _wa_result(r, label)


def wa_photo(to, data, name, caption, label):
    if DRY_RUN:
        log(f"DRY {label} ({len(data)} bytes)"); return True
    try:
        up = requests.post(f"{BASE}/media", headers=WHDR,
                           files={"file": (name, data, "image/jpeg")},
                           data={"messaging_product": "whatsapp", "type": "image/jpeg"}, timeout=60)
        if up.status_code != 200 or "id" not in up.json():
            log(f"WA upload failed {label}: {up.text[:200]}"); STATS["failed"] += 1; return False
        r = requests.post(f"{BASE}/messages", headers={**WHDR, "Content-Type": "application/json"},
                          json={"messaging_product": "whatsapp", "to": to, "type": "image",
                                "image": {"id": up.json()["id"], "caption": caption[:1000]}}, timeout=30)
    except Exception as e:
        log(f"WA error {label}: {e}"); STATS["failed"] += 1; return False
    return _wa_result(r, label)


def _wa_result(r, label):
    if r.status_code == 200:
        STATS["sent"] += 1
        log(f"sent  {label}")
        return True
    STATS["failed"] += 1
    try:
        e = r.json().get("error", {})
        hint = " (24h window closed - send 'hi' from the phone)" if e.get("code") == 131047 else ""
        log(f"FAIL  {label}: code {e.get('code')} {e.get('message')}{hint}")
    except Exception:
        log(f"FAIL  {label}: HTTP {r.status_code}")
    return False


# ─── LIVE CHECKS ──────────────────────────────────────────────────────────────
def check_live(state, first_run):
    backfill_from = now_ksa() - timedelta(hours=BACKFILL_HOURS)
    for key, cfg in LIVE_SOURCES.items():
        for tid in cfg["templates"]:
            for s in latest_submissions(tid):
                sid = str(s.get("id"))
                lid = sub_location_id(s)
                if sid in state["subs"] or lid not in BRANCHES:
                    continue
                st = sub_time(s)
                state["subs"][sid] = now_ksa().isoformat()
                if first_run and (st is None or st < backfill_from):
                    continue                      # baseline: old items are not sent
                send_submission(key, cfg["name"], s, lid, st)


def send_submission(key, name, s, lid, st):
    br = BRANCHES[lid]
    who = ((s.get("smetadata") or {}).get("created_by") or {}).get("display_name", "")
    when = st.strftime("%d-%m-%Y %I:%M %p") if st else ""
    lines, photos = describe(s)
    body = "\n".join([f"تنبيه - {name}", f"فرع {br['label']}", f"الوقت: {when}"]
                     + ([f"بواسطة: {who}"] if who else []) + [""] + lines
                     + ([f"\nعدد الصور: {len(photos)}"] if photos else []))
    wa_text(br["phone"], body, f"{br['label']} {key} text")
    for i, (s3_key, qtitle) in enumerate(photos, start=1):
        data = get_photo(s3_key)
        if data:
            wa_photo(br["phone"], data, Path(s3_key).name,
                     f"فرع {br['label']} - {name}\n{qtitle} - {when}",
                     f"{br['label']} {key} photo {i}")


_projects = {}


def project_titles():
    if _projects:
        return _projects
    nxt = f"{ZHOST}/api/v3/projects/one_off_and_recurring_projects/"
    for _ in range(30):
        r = zget(nxt)
        if not r or r.status_code != 200:
            break
        js = r.json()
        for p in js.get("data", []):
            _projects[p.get("id")] = p.get("title")
        nxt = (js.get("meta") or {}).get("next")
        if not nxt:
            break
    return _projects


def task_loc(t):
    for k in ("location", "account", "store"):
        v = t.get(k)
        if isinstance(v, dict) and v.get("id"):
            return int(v["id"])
        if isinstance(v, (int, str)) and str(v).isdigit():
            return int(v)
    return None


def task_due(t):
    raw = t.get("date_due")
    if raw:
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(TZ)
        except Exception:
            pass
    raw = t.get("date_due_local")
    if raw:
        try:
            d = datetime.fromisoformat(str(raw))
            return d if d.tzinfo else TZ.localize(d)
        except Exception:
            pass
    return None


_task_debug_done = False


def check_tasks(state, first_run):
    global _task_debug_done
    now = now_ksa()
    backfill_from = now - timedelta(hours=BACKFILL_HOURS)
    titles = project_titles()
    for page in range(4):
        r = zget(f"{ZHOST}/api/v3/tasks/",
                 {"limit": 100, "offset": page * 100, "order_by": "-date_start"})
        if not r or r.status_code != 200:
            break
        batch = r.json().get("data", [])
        if not batch:
            break
        if not _task_debug_done:
            _task_debug_done = True
            log(f"task keys: {sorted(batch[0].keys())}")
        for t in batch:
            tid, lid, due = str(t.get("id")), task_loc(t), task_due(t)
            if lid not in BRANCHES or not due or due > now or tid in state["tasks"]:
                continue
            if t.get("date_submitted") or t.get("submission_id"):
                continue
            if due < now - timedelta(days=2):
                continue
            state["tasks"][tid] = now.isoformat()
            if first_run and due < backfill_from:
                continue
            br = BRANCHES[lid]
            proj = t.get("project")
            pid = proj.get("id") if isinstance(proj, dict) else proj
            title = (proj.get("title") if isinstance(proj, dict) else None) or titles.get(pid) or f"Project {pid}"
            assignee = t.get("assignee")
            who = assignee.get("display_name") if isinstance(assignee, dict) else ""
            body = "\n".join([f"تنبيه - مهمة لم تنجز", f"فرع {br['label']}", f"المهمة: {title}",
                              f"موعد التسليم: {due.strftime('%d-%m-%Y %I:%M %p')}"]
                             + ([f"المسؤول: {who}"] if who else [])
                             + ["", "يرجى إنجاز المهمة في أقرب وقت."])
            wa_text(br["phone"], body, f"{br['label']} missing task")


# ─── DAILY DIGEST ─────────────────────────────────────────────────────────────
def daily_digest(state):
    today = now_ksa().date()
    if state.get("daily") == today.isoformat() or now_ksa().hour < DAILY_HOUR:
        return
    y = today - timedelta(days=1)
    y_iso, y_disp = y.isoformat(), y.strftime("%d-%m-%Y")
    log(f"daily digest for {y_iso}")

    checks = {lid: [] for lid in BRANCHES}
    for tid in SERVICE_TIME_TEMPLATES:
        for s in submissions_since(tid, y_iso):
            lid = sub_location_id(s)
            st = sub_time(s)
            if lid not in BRANCHES or not st or st.date() != y:
                continue
            ms = None
            for a in s.get("answers", []):
                if "service time" in str(a.get("title", "")).lower():
                    try: ms = int(float(a.get("value")))
                    except Exception: pass
            checks[lid].append((st, ms))

    oldest = (y - timedelta(days=14)).isoformat()
    last = {}
    for tid, name in VISIT_TEMPLATES.items():
        for s in submissions_since(tid, oldest):
            lid, st = sub_location_id(s), sub_time(s)
            if lid not in BRANCHES or not st or st.date() > y:
                continue
            who = ((s.get("smetadata") or {}).get("created_by") or {}).get("display_name", "")
            if lid not in last or st > last[lid][0]:
                last[lid] = (st, who)

    for lid, br in BRANCHES.items():
        lines = [f"الملخص اليومي - فرع {br['label']}", f"ليوم {y_disp}", "", "*وقت الخدمة*"]
        rows = sorted(checks[lid])
        if rows:
            for st, ms in rows:
                val = str(timedelta(milliseconds=ms)).split(".")[0] if ms is not None else "-"
                lines.append(f"- {st.strftime('%I:%M %p')}: {val}")
            vals = [ms for _, ms in rows if ms is not None]
            if vals:
                avg = str(timedelta(milliseconds=sum(vals) // len(vals))).split(".")[0]
                lines.append(f"المتوسط: {avg}")
        else:
            lines.append("- لا توجد قياسات")
        lines += ["", "*زيارة مدير المنطقة*"]
        if lid in last:
            st, who = last[lid]
            lines.append(f"- آخر زيارة {st.strftime('%d-%m-%Y')} ({(y - st.date()).days} يوم) - {who}")
        else:
            lines.append("- لا توجد زيارة خلال آخر 14 يوم")
        lines += ["", "هذه رسالة آلية، يرجى عدم الرد عليها."]
        wa_text(br["phone"], "\n".join(lines), f"{br['label']} daily digest")
    state["daily"] = today.isoformat()


# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    if not ZENPUT_API_KEY:
        sys.exit("ZENPUT_API_KEY missing")
    if not DRY_RUN and not (WA_TOKEN and WA_PHONE_ID and TEST_PHONE):
        sys.exit("WA_TOKEN / WA_PHONE_NUMBER_ID / WA_TEST_PHONE secret missing (or run with MODE=dry_run)")

    state, first_run = load_state()
    log(f"start | mode {'DRY RUN' if DRY_RUN else 'SEND'} | "
        f"{'single pass' if SINGLE_PASS else f'loop {RUN_MINUTES} min every {POLL_SECONDS}s'} "
        f"| first run {first_run} | branches {len(BRANCHES)}")
    if first_run:
        log(f"first run: items older than {BACKFILL_HOURS}h are recorded but not sent")

    if SINGLE_PASS:
        try:
            check_live(state, first_run)
            check_tasks(state, first_run)
            daily_digest(state)
        except Exception as e:
            log(f"run error: {e}")
        finally:
            save_state(state, commit=True)
            log(f"done | sent {STATS['sent']} | failed {STATS['failed']} "
                f"| tracking {len(state['subs'])} subs, {len(state['tasks'])} tasks")
        return

    last_task = last_save = 0
    cycle = 0
    try:
        while time.time() < END:
            t0 = time.time()
            cycle += 1
            try:
                check_live(state, first_run)
                if time.time() - last_task >= TASK_SECONDS:
                    check_tasks(state, first_run)
                    last_task = time.time()
                daily_digest(state)
            except Exception as e:
                log(f"cycle error: {e}")
            first_run = False
            if time.time() - last_save >= SAVE_EVERY_MIN * 60:
                save_state(state, commit=True)
                last_save = time.time()
            if cycle % 30 == 1:
                log(f"cycle {cycle} | sent {STATS['sent']} | failed {STATS['failed']} "
                    f"| tracking {len(state['subs'])} subs, {len(state['tasks'])} tasks")
            time.sleep(max(1, POLL_SECONDS - (time.time() - t0)))
    finally:
        save_state(state, commit=True)
        log(f"stop | cycles {cycle} | sent {STATS['sent']} | failed {STATS['failed']}")

if __name__ == "__main__":
    main()
