import os, re, hashlib, json, time, datetime, random
import requests
import logging, sys
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------
# Logging setup
# -----------------------
LOG_LEVEL = "DEBUG"
LOG_JSON = "0"

class _KVFormatter(logging.Formatter):
    def format(self, record):
        base = {
            "ts": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        # merge any extra dict attached via record.__dict__["_extra"]
        extra = getattr(record, "_extra", None)
        if extra:
            base.update(extra)
        if LOG_JSON:
            return json.dumps(base, ensure_ascii=False)
        # key=value format
        kv = " ".join(f"{k}={json.dumps(v, ensure_ascii=False)}" for k, v in base.items())
        return kv

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_KVFormatter())
logger = logging.getLogger("elysee-monitor")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
logger.handlers = [ _handler ]
logger.propagate = False

def log(level, msg, **kw):
    rec = logger.makeRecord(logger.name, level, fn="", lno=0, msg=msg, args=(), exc_info=None)
    setattr(rec, "_extra", kw)
    logger.handle(rec)

# -----------------------
# HTTP session + retries
# -----------------------
SESSION = requests.Session()
retry_cfg = Retry(
    total=0,   # manual retry logic below
    connect=3,
    read=3,
    backoff_factor=0,
    status_forcelist=[],
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
    respect_retry_after_header=True,
)
SESSION.mount("https://", HTTPAdapter(max_retries=retry_cfg))
SESSION.mount("http://", HTTPAdapter(max_retries=retry_cfg))

URLS = [
    "https://www.elysee.fr/actualites",
    "https://www.elysee.fr/toutes-les-actualites",
    "https://www.elysee.fr/emmanuel-macron/2024/09/16/les-journees-europeennes-du-patrimoine-2024-au-palais-de-lelysee",
    "https://www.elysee.fr/emmanuel-macron/2023/09/07/journees-du-patrimoine-2023",
    "https://www.elysee.fr/emmanuel-macron/2022/09/11/journees-du-patrimoine-2022",
]

KEYWORDS = {
    r"patrimoine\s*2025": "Patrimoine 2025",
    r"journées?\s+du\s+patrimoine": "Journées du Patrimoine",
    r"journées?\s+europ[eé]enne[s]?\s+du\s+patrimoine": "Journées Européennes du Patrimoine",
    r"\bbilletterie\b": "Billetterie",
    r"\breservation[s]?\b": "Réservation",
    r"r[ée]serv(er|ation|ations|ez)": "Réserver",
    r"\binscription[s]?\b": "Inscriptions",
    r"\bticket[s]?\b": "Tickets",
    r"\bcr[eé]neau[x]?\b": "Créneau",
    r"\bvisite[s]?\b": "Visite",
    r"\bentr[eé]e[s]?\b": "Entrée",
}

STATE_FILE = "state.json"
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID")

def generate_candidate_urls(year=2025):
    base = "https://www.elysee.fr/emmanuel-macron/"
    slugs = [
        f"les-journees-europeennes-du-patrimoine-{year}-au-palais-de-lelysee",
        f"journees-du-patrimoine-{year}",
        f"les-journees-du-patrimoine-{year}",
        f"journees-europeennes-du-patrimoine-{year}",
        f"journees-du-patrimoine-au-palais-de-lelysee-{year}",
    ]
    start = datetime.date(year, 9, 1)
    end   = datetime.date(year, 9, 22)
    candidates = []
    d = start
    while d <= end:
        for slug in slugs:
            url = urljoin(base, f"{d.year:04d}/{d.month:02d}/{d.day:02d}/{slug}")
            candidates.append(url)
        d += datetime.timedelta(days=1)
    candidates.append(urljoin(base, f"{year}/09/les-journees-europeennes-du-patrimoine-{year}-au-palais-de-lelysee"))
    candidates.append(urljoin(base, f"{year}/09/journees-du-patrimoine-{year}"))
    return candidates

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            s = json.load(f)
            log(logging.INFO, "state_loaded", file=STATE_FILE, urls=len(s))
            return s
    log(logging.INFO, "state_missing_starting_fresh", file=STATE_FILE)
    return {}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    log(logging.INFO, "state_saved", file=STATE_FILE, urls=len(state))

def _parse_retry_after(value):
    if not value:
        return None
    try:
        return max(0, int(value))
    except ValueError:
        try:
            dt = parsedate_to_datetime(value)
            return max(0, int((dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()))
        except Exception:
            return None

def fetch(url, timeout=20, max_retries=6, base_backoff=1.0, max_backoff=30.0):
    headers = {"User-Agent": "Mozilla/5.0 (monitor-bot)"}
    attempt = 0
    backoff = base_backoff

    while True:
        attempt += 1
        t0 = time.time()
        try:
            r = SESSION.get(url, headers=headers, timeout=timeout)
            dt = round((time.time() - t0) * 1000)
            if 200 <= r.status_code < 300:
                log(logging.DEBUG, "fetch_ok", url=url, status=r.status_code, ms=dt, size=len(r.text))
                return r.text

            if r.status_code == 429:
                ra = _parse_retry_after(r.headers.get("Retry-After"))
                wait = ra if ra is not None else min(backoff, max_backoff)
                jitter = random.uniform(0, 0.5)
                wait_j = wait + jitter
                log(logging.WARNING, "fetch_rate_limited",
                    url=url, status=r.status_code, attempt=attempt, retry_after=ra, wait=round(wait_j, 3))
            elif 500 <= r.status_code < 600:
                wait = min(backoff, max_backoff) + random.uniform(0, 0.5)
                log(logging.WARNING, "fetch_server_error",
                    url=url, status=r.status_code, attempt=attempt, wait=round(wait, 3))
            else:
                log(logging.ERROR, "fetch_non_retryable_http_error",
                    url=url, status=r.status_code, body_snippet=r.text[:200])
                r.raise_for_status()

            if attempt >= max_retries:
                msg = f"HTTP {r.status_code} after {attempt} attempt(s) for {url}"
                log(logging.ERROR, "fetch_give_up", url=url, attempts=attempt, last_status=r.status_code,
                    body_snippet=r.text[:300])
                raise requests.HTTPError(msg, response=r)

            time.sleep(wait)
            backoff = min(max_backoff, backoff * 2)

        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt >= max_retries:
                log(logging.ERROR, "fetch_network_give_up", url=url, attempts=attempt, error=str(e))
                raise
            wait = min(backoff, max_backoff) + random.uniform(0, 0.5)
            log(logging.WARNING, "fetch_network_error_retry",
                url=url, attempt=attempt, error=str(e), wait=round(wait, 3))
            time.sleep(wait)
            backoff = min(max_backoff, backoff * 2)
        except requests.HTTPError as e:
            # already logged above
            raise

def textify(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ").lower()

def find_keywords(text):
    found = []
    for pattern, label in KEYWORDS.items():
        if re.search(pattern, text, flags=re.IGNORECASE):
            found.append(label)
    return found

def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def _send_telegram(text, token, chat_id, max_retries=5):
    if not (token and chat_id):
        log(logging.WARNING, "telegram_not_configured")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}

    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, data=payload, timeout=20)
            if resp.status_code == 200:
                log(logging.INFO, "telegram_sent", bytes=len(text), chunks=1, attempt=attempt)
                return True

            if resp.status_code == 429:
                try:
                    data = resp.json()
                    retry_after = data.get("parameters", {}).get("retry_after", 1)
                except Exception:
                    retry_after = 1
                wait = retry_after + 0.5
                log(logging.WARNING, "telegram_rate_limited", retry_after=retry_after, wait=wait, attempt=attempt)
                time.sleep(wait)
                continue

            if 500 <= resp.status_code < 600:
                log(logging.WARNING, "telegram_server_error_retry", status=resp.status_code,
                    attempt=attempt, delay=round(delay, 3))
                time.sleep(delay)
                delay *= 2
                continue

            log(logging.ERROR, "telegram_http_error", status=resp.status_code,
                body_snippet=resp.text[:200])
            return False

        except Exception as e:
            log(logging.WARNING, "telegram_network_error_retry", error=str(e), attempt=attempt, delay=round(delay, 3))
            time.sleep(delay)
            delay *= 2
    log(logging.ERROR, "telegram_give_up")
    return False

def notify_telegram(msg, token=None, chat_id=None):
    token = token or TG_BOT_TOKEN
    chat_id = chat_id or TG_CHAT_ID

    limit = 3500
    chunks = []
    original_len = len(msg)
    while len(msg) > limit:
        cut = msg.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        chunks.append(msg[:cut])
        msg = msg[cut:].lstrip("\n")
    chunks.append(msg)

    log(logging.INFO, "telegram_prepare_send", original_len=original_len, chunks=len(chunks), chunk_limit=limit)

    ok = True
    for i, part in enumerate(chunks, 1):
        part_ok = _send_telegram(part, token, chat_id)
        ok = part_ok and ok
        log(logging.DEBUG, "telegram_chunk_result", chunk=i, ok=part_ok, size=len(part))
        time.sleep(1.0)
    log(logging.INFO, "telegram_send_complete", ok=ok)
    return ok

def main():
    log(logging.INFO, "run_start", log_level=LOG_LEVEL, json=LOG_JSON)
    state = load_state()

    urls_to_check = URLS + generate_candidate_urls(2025)
    log(logging.INFO, "planning_urls", total=len(urls_to_check), seeds=len(URLS))

    alerts = []
    scanned = 0

    for url in urls_to_check:
        scanned += 1
        try:
            html = fetch(url)
            txt = textify(html)
            found = find_keywords(txt)
            content_hash = sha256(txt)

            prev = state.get(url, {})
            prev_hash = prev.get("hash")
            prev_found = prev.get("found", [])

            trigger = (not prev_found and bool(found)) or ((content_hash != prev_hash) and bool(found))

            # granular logging per URL
            log(logging.DEBUG, "scan_result",
                url=url,
                found_labels=found,
                n_labels=len(found),
                new_hash=content_hash,
                prev_hash=prev_hash,
                prev_found=prev_found,
                hash_changed=(content_hash != prev_hash),
                trigger=trigger)

            if trigger:
                alerts.append((url, found))
                log(logging.INFO, "trigger_alert", url=url, labels=found)

            state[url] = {
                "hash": content_hash,
                "found": found,
                "last_check": int(time.time())
            }

        except Exception as e:
            state[url] = {"error": str(e), "last_check": int(time.time())}
            log(logging.ERROR, "scan_error", url=url, error=str(e))

    if alerts:
        MAX_LINES = 20
        lines = [f"🔔 {len(alerts)} page(s) avec signaux JEP 2025 :"]
        for i, (u, labels) in enumerate(alerts[:MAX_LINES], 1):
            lines.append(f"• {u}\n   → {', '.join(labels)}")
        if len(alerts) > MAX_LINES:
            lines.append(f"… et {len(alerts) - MAX_LINES} de plus.")

        log(logging.INFO, "sending_alerts", count=len(alerts), capped=min(len(alerts), MAX_LINES))
        notify_telegram("\n".join(lines))
    else:
        log(logging.INFO, "no_alerts_no_send", scanned=scanned)

    save_state(state)
    log(logging.INFO, "run_end", alerts=len(alerts), scanned=scanned)

if __name__ == "__main__":
    main()
