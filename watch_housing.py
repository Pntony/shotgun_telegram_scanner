import datetime
import json
import logging
import os
import random
import re
import sys
import time
import unicodedata
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_JSON = os.getenv("LOG_JSON", "0") == "1"
DATABASE_FILE = "housing_db.json"
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
LOCAL_TIMEZONE_NAME = os.getenv("LOCAL_TIMEZONE", "Europe/Paris")
DAILY_RECAP_HOUR = int(os.getenv("DAILY_RECAP_HOUR", "9"))

MIN_AREA_M2 = 20.0
MAX_PRICE_EUR = 850.0
TARGET_CITIES = {
    "nice": {"postal_codes": {"06000", "06100", "06200", "06300"}},
    "cagnes-sur-mer": {"postal_codes": {"06800"}},
}
POSTAL_CODE_TO_CITY = {
    "06000": "nice",
    "06100": "nice",
    "06200": "nice",
    "06300": "nice",
    "06800": "cagnes-sur-mer",
}
BLOCKED_KEYWORDS = (
    "colocation",
    "chambre",
    "parking",
    "garage",
    "box",
    "etudiant",
    "bail-mobilite",
    "bail mobilite",
    "saisonnier",
    "vacances",
)

SOURCES = [
    {
        "name": "Orpi Nice",
        "url": "https://www.orpi.com/location-immobiliere-nice/louer-appartement/",
        "city_slug": "nice",
    },
    {
        "name": "Orpi Cagnes-sur-Mer",
        "url": "https://www.orpi.com/location-immobiliere-cagnes-sur-mer/louer-appartement/",
        "city_slug": "cagnes-sur-mer",
    },
    {
        "name": "Laforet Nice",
        "url": "https://www.laforet.com/ville/location-appartement-nice-06000",
        "city_slug": "nice",
    },
    {
        "name": "Laforet Nice 06100",
        "url": "https://www.laforet.com/ville/location-appartement-nice-06100",
        "city_slug": "nice",
    },
    {
        "name": "Laforet Nice 06200",
        "url": "https://www.laforet.com/ville/location-appartement-nice-06200",
        "city_slug": "nice",
    },
    {
        "name": "Laforet Nice 06300",
        "url": "https://www.laforet.com/ville/location-appartement-nice-06300",
        "city_slug": "nice",
    },
    {
        "name": "Laforet Cagnes-sur-Mer",
        "url": "https://www.laforet.com/ville/location-appartement-cagnes-sur-mer-06800",
        "city_slug": "cagnes-sur-mer",
    },
    {
        "name": "SeLoger Nice",
        "url": "https://www.seloger.com/immobilier/locations/immo-nice-06/bien-appartement/",
        "city_slug": "nice",
    },
    {
        "name": "SeLoger Cagnes-sur-Mer",
        "url": "https://www.seloger.com/immobilier/locations/immo-cagnes-sur-mer-06/bien-appartement/",
        "city_slug": "cagnes-sur-mer",
    },
    {
        "name": "Century 21 Nice",
        "url": "https://www.century21.fr/annonces/location-appartement/v-nice/",
        "city_slug": "nice",
    },
    {
        "name": "Century 21 Cagnes-sur-Mer",
        "url": "https://www.century21.fr/annonces/location-appartement/v-cagnes%2Bsur%2Bmer/",
        "city_slug": "cagnes-sur-mer",
    },
    {
        "name": "Century 21 Nice 06000",
        "url": "https://www.century21.fr/annonces/location-appartement/cpv-06000_nice/",
        "city_slug": "nice",
    },
    {
        "name": "Century 21 Nice 06100",
        "url": "https://www.century21.fr/annonces/location-appartement/cpv-06100_nice/",
        "city_slug": "nice",
    },
    {
        "name": "Century 21 Nice 06200",
        "url": "https://www.century21.fr/annonces/location-appartement/cpv-06200_nice/",
        "city_slug": "nice",
    },
]


class _KVFormatter(logging.Formatter):
    def format(self, record):
        base = {
            "ts": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "_extra", None)
        if extra:
            base.update(extra)
        if LOG_JSON:
            return json.dumps(base, ensure_ascii=False)
        return " ".join(f"{k}={json.dumps(v, ensure_ascii=False)}" for k, v in base.items())


_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_KVFormatter())
logger = logging.getLogger("housing-monitor")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
logger.handlers = [_handler]
logger.propagate = False


def resolve_local_timezone():
    try:
        return ZoneInfo(LOCAL_TIMEZONE_NAME)
    except ZoneInfoNotFoundError:
        return datetime.timezone(datetime.timedelta(hours=1), name="UTC+01:00")


LOCAL_TIMEZONE = resolve_local_timezone()


def log(level, msg, **kw):
    rec = logger.makeRecord(logger.name, level, fn="", lno=0, msg=msg, args=(), exc_info=None)
    setattr(rec, "_extra", kw)
    logger.handle(rec)


if getattr(LOCAL_TIMEZONE, "key", None) != LOCAL_TIMEZONE_NAME:
    log(
        logging.WARNING,
        "timezone_fallback_in_use",
        timezone=LOCAL_TIMEZONE_NAME,
        fallback="UTC+01:00",
    )


SESSION = requests.Session()
retry_cfg = Retry(
    total=0,
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


def fetch(url, timeout=25, max_retries=5, base_backoff=1.0, max_backoff=20.0):
    headers = {
        "User-Agent": "Mozilla/5.0 (housing-monitor)",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }
    attempt = 0
    backoff = base_backoff

    while True:
        attempt += 1
        t0 = time.time()
        try:
            response = SESSION.get(url, headers=headers, timeout=timeout)
            ms = round((time.time() - t0) * 1000)
            if 200 <= response.status_code < 300:
                log(logging.DEBUG, "fetch_ok", url=url, status=response.status_code, ms=ms, size=len(response.text))
                return response.text

            if response.status_code == 429:
                retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                wait = retry_after if retry_after is not None else min(backoff, max_backoff)
                wait += random.uniform(0, 0.5)
                log(logging.WARNING, "fetch_rate_limited", url=url, status=response.status_code, attempt=attempt, wait=round(wait, 3))
            elif 500 <= response.status_code < 600:
                wait = min(backoff, max_backoff) + random.uniform(0, 0.5)
                log(logging.WARNING, "fetch_server_error", url=url, status=response.status_code, attempt=attempt, wait=round(wait, 3))
            else:
                log(logging.ERROR, "fetch_non_retryable_http_error", url=url, status=response.status_code, body_snippet=response.text[:200])
                response.raise_for_status()

            if attempt >= max_retries:
                raise requests.HTTPError(f"HTTP {response.status_code} after {attempt} attempts for {url}", response=response)

            time.sleep(wait)
            backoff = min(max_backoff, backoff * 2)
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt >= max_retries:
                log(logging.ERROR, "fetch_network_give_up", url=url, attempts=attempt, error=str(exc))
                raise
            wait = min(backoff, max_backoff) + random.uniform(0, 0.5)
            log(logging.WARNING, "fetch_network_error_retry", url=url, attempt=attempt, error=str(exc), wait=round(wait, 3))
            time.sleep(wait)
            backoff = min(max_backoff, backoff * 2)


def load_database():
    if os.path.exists(DATABASE_FILE):
        with open(DATABASE_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        log(logging.INFO, "database_loaded", file=DATABASE_FILE, listings=len(data.get("listings", [])))
        return data
    log(logging.INFO, "database_missing_starting_fresh", file=DATABASE_FILE)
    return {}


def save_database(data):
    with open(DATABASE_FILE, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    log(logging.INFO, "database_saved", file=DATABASE_FILE, listings=len(data.get("listings", [])))


def slugify(value):
    normalized = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii").lower()
    normalized = normalized.replace("'", "-")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    return normalized.strip("-")


def canonical_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")


def normalize_whitespace(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def is_listing_url(url):
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"}


def is_generic_cta(text):
    normalized = slugify(text or "")
    return normalized in {"appeler", "message", "contacter", "voir-plus", "details"}


def normalize_numeric_text(text):
    if not text:
        return ""
    normalized = str(text)
    normalized = normalized.replace("\xa0", " ").replace("\u202f", " ").replace("\u2009", " ")
    return normalize_whitespace(normalized)


def parse_number_token(token):
    if token is None:
        return None
    compact = normalize_numeric_text(token).strip()
    compact = re.sub(r"(?<=\d)\s+(?=\d{3}\b)", "", compact)
    compact = re.sub(r"\s+", "", compact)
    if compact.count(".") > 1:
        parts = compact.split(".")
        compact = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(compact)
    except ValueError:
        return None


def parse_price(text):
    if not text:
        return None
    cleaned = normalize_numeric_text(text).replace(",", ".")
    scored_candidates = []
    patterns = [
        r"(?P<amount>\d{1,5}(?:[\s.]\d{3})*(?:\.\d+)?)\s*\u20ac(?:\s*/\s*(?:mois|month))?",
        r"\u20ac\s*(?P<amount>\d{1,5}(?:[\s.]\d{3})*(?:\.\d+)?)",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, cleaned, flags=re.IGNORECASE):
            amount = parse_number_token(match.group("amount"))
            if amount is None:
                continue
            context = cleaned[max(0, match.start() - 24): min(len(cleaned), match.end() + 24)].lower()
            score = 0
            if "mois" in context or "month" in context:
                score += 4
            if "charges comprises" in context or "cc" in context:
                score += 2
            if "appartement" in context or "studio" in context or "maison" in context:
                score += 1
            scored_candidates.append((score, amount))
    if scored_candidates:
        scored_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return scored_candidates[0][1]
    raw_number = cleaned.strip()
    if re.fullmatch(r"\d{2,5}(?:\.\d+)?", raw_number):
        return parse_number_token(raw_number)
    return None


def parse_area(text):
    if not text:
        return None
    cleaned = text.replace("\xa0", " ").replace(",", ".")
    match = re.search(r"(\d{1,4}(?:\.\d+)?)\s*m(?:\u00b2|2)", cleaned, flags=re.IGNORECASE)
    if match:
        return parse_number_token(match.group(1))
    raw_number = cleaned.strip()
    if re.fullmatch(r"\d{1,4}(?:\.\d+)?", raw_number):
        return parse_number_token(raw_number)
    return None


def extract_postal_code(text):
    if not text:
        return None
    match = re.search(r"\b(0\d{4})\b", text)
    return match.group(1) if match else None


def detect_city(text, fallback_slug=None):
    haystack = slugify(text or "")
    postal_code = extract_postal_code(text or "")
    if postal_code in POSTAL_CODE_TO_CITY:
        return POSTAL_CODE_TO_CITY[postal_code]
    for city_slug in TARGET_CITIES:
        if city_slug in haystack:
            return city_slug
    if fallback_slug and not postal_code:
        return fallback_slug
    return None


def is_rental_context(text):
    haystack = slugify(text or "")
    return any(token in haystack for token in ("louer", "location", "mois"))


def should_exclude_listing(text):
    haystack = slugify(text or "")
    return any(keyword in haystack for keyword in BLOCKED_KEYWORDS)


def listing_id_from_url(source_name, listing_url):
    return f"{slugify(source_name)}::{canonical_url(listing_url)}"


def extract_json_ld_listings(html, base_url, source):
    soup = BeautifulSoup(html, "html.parser")
    listings = []

    def visit(node):
        if isinstance(node, list):
            for item in node:
                visit(item)
            return
        if not isinstance(node, dict):
            return

        node_type = node.get("@type")
        if isinstance(node_type, list):
            node_type = " ".join(node_type)
        node_type = (node_type or "").lower()

        if node_type == "itemlist":
            visit(node.get("itemListElement", []))

        if node_type == "listitem":
            visit(node.get("item"))
            if node.get("url"):
                listings.append(
                    build_listing(
                        source=source,
                        url=urljoin(base_url, node.get("url")),
                        title=node.get("name"),
                        card_text=node.get("name") or "",
                    )
                )

        candidate_types = ("product", "offer", "apartment", "house", "singlefamilyresidence", "place", "accommodation")
        if any(t in node_type for t in candidate_types):
            offers = node.get("offers") or {}
            address = node.get("address") or {}
            floor_size = node.get("floorSize") or {}
            description = normalize_whitespace(node.get("description") or "")
            title = normalize_whitespace(node.get("name") or "")
            city = detect_city(" ".join(filter(None, [address.get("addressLocality"), title, description])), source.get("city_slug"))
            listing = build_listing(
                source=source,
                url=urljoin(base_url, node.get("url") or node.get("@id") or base_url),
                title=title,
                card_text=" ".join(
                    filter(
                        None,
                        [
                            title,
                            description,
                            address.get("addressLocality"),
                            address.get("postalCode"),
                            str(offers.get("price") or ""),
                            str(floor_size.get("value") or ""),
                        ],
                    )
                ),
                city_slug=city,
                postal_code=address.get("postalCode"),
                price_eur=parse_price(str(offers.get("price") or "")) or parse_price(description),
                area_m2=parse_area(str(floor_size.get("value") or "")) or parse_area(description),
                description=description,
            )
            listings.append(listing)

        for value in node.values():
            visit(value)

    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        visit(payload)

    return listings


def build_listing(source, url, title, card_text, city_slug=None, postal_code=None, price_eur=None, area_m2=None, description=None):
    normalized_url = canonical_url(urljoin(source["url"], url))
    normalized_title = normalize_whitespace(title)
    normalized_description = normalize_whitespace(description or "")
    normalized_card_text = normalize_whitespace(card_text)
    combined_text = normalize_whitespace(" ".join(filter(None, [normalized_title, normalized_description, normalized_card_text])))

    postal_code = (
        postal_code
        or extract_postal_code(normalized_title)
        or extract_postal_code(normalized_description)
        or extract_postal_code(normalized_card_text)
    )
    city_slug = (
        city_slug
        or POSTAL_CODE_TO_CITY.get(postal_code)
        or detect_city(normalized_title)
        or detect_city(normalized_description)
        or detect_city(normalized_card_text, source.get("city_slug"))
    )
    price_eur = (
        price_eur
        if price_eur is not None
        else parse_price(normalized_title)
        or parse_price(normalized_description)
        or parse_price(normalized_card_text)
    )
    area_m2 = (
        area_m2
        if area_m2 is not None
        else parse_area(normalized_title)
        or parse_area(normalized_description)
        or parse_area(normalized_card_text)
    )

    return {
        "id": listing_id_from_url(source["name"], normalized_url),
        "source": source["name"],
        "source_url": source["url"],
        "url": normalized_url,
        "title": normalized_title or normalized_description or normalized_url.rsplit("/", 1)[-1],
        "description": normalized_description,
        "city_slug": city_slug,
        "postal_code": postal_code,
        "price_eur": price_eur,
        "area_m2": area_m2,
        "raw_text": combined_text[:1000],
    }


def extract_card_listings(html, source):
    soup = BeautifulSoup(html, "html.parser")
    listings = []
    seen_urls = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        absolute_url = canonical_url(urljoin(source["url"], href))
        if not is_listing_url(absolute_url):
            continue
        if absolute_url in seen_urls:
            continue
        if absolute_url == canonical_url(source["url"]):
            continue

        card = anchor
        for _ in range(4):
            if not card.parent:
                break
            candidate = card.parent
            if candidate.name in {"article", "li", "div"}:
                card = candidate
            else:
                break

        card_text = normalize_whitespace(card.get_text(" ", strip=True))
        if len(card_text) < 20:
            continue
        if len(card_text) > 450:
            continue
        if not is_rental_context(card_text):
            continue
        if "\u20ac" not in card_text and "\u00b2" not in card_text and "m2" not in card_text.lower():
            continue

        title = normalize_whitespace(anchor.get_text(" ", strip=True))
        if not title or is_generic_cta(title):
            title = card_text[:140]
        listing = build_listing(source=source, url=absolute_url, title=title, card_text=card_text)
        listings.append(listing)
        seen_urls.add(absolute_url)

    return listings


def dedupe_listings(listings):
    deduped = {}
    for listing in listings:
        existing = deduped.get(listing["id"])
        if not existing:
            deduped[listing["id"]] = listing
            continue
        score_existing = int(existing.get("price_eur") is not None) + int(existing.get("area_m2") is not None)
        score_new = int(listing.get("price_eur") is not None) + int(listing.get("area_m2") is not None)
        if score_new > score_existing:
            deduped[listing["id"]] = listing
    return list(deduped.values())


def validate_listing_location(listing):
    postal_code = listing.get("postal_code")
    city_slug = listing.get("city_slug")
    if postal_code in POSTAL_CODE_TO_CITY:
        expected_city = POSTAL_CODE_TO_CITY[postal_code]
        if city_slug != expected_city:
            listing["city_slug"] = expected_city
    elif postal_code and city_slug in TARGET_CITIES:
        allowed = TARGET_CITIES[city_slug]["postal_codes"]
        if postal_code not in allowed:
            listing["city_slug"] = None
    return listing


def evaluate_listing(listing):
    text = " ".join(filter(None, [listing.get("title"), listing.get("description")]))
    city_slug = listing.get("city_slug")
    postal_code = listing.get("postal_code")
    price_eur = listing.get("price_eur")
    area_m2 = listing.get("area_m2")

    if should_exclude_listing(text):
        return False, "excluded_type"
    if city_slug not in TARGET_CITIES:
        return False, "outside_city"
    allowed_postal_codes = TARGET_CITIES[city_slug]["postal_codes"]
    if postal_code and postal_code not in allowed_postal_codes:
        return False, "outside_postal_code"
    if area_m2 is None:
        return False, "missing_area"
    if area_m2 < MIN_AREA_M2:
        return False, "area_too_small"
    if price_eur is None:
        return False, "missing_price"
    if price_eur > MAX_PRICE_EUR:
        return False, "price_too_high"
    return True, "match"


def scan_source(source):
    html = fetch(source["url"])
    candidates = []
    candidates.extend(extract_json_ld_listings(html, source["url"], source))
    candidates.extend(extract_card_listings(html, source))
    listings = [validate_listing_location(listing) for listing in dedupe_listings(candidates)]

    matched = 0
    for listing in listings:
        listing["matches_filters"], listing["match_reason"] = evaluate_listing(listing)
        if listing["matches_filters"]:
            matched += 1

    log(logging.INFO, "source_scanned", source=source["name"], listings=len(listings), matches=matched)
    return listings


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
                log(logging.INFO, "telegram_sent", bytes=len(text), attempt=attempt)
                return True
            if resp.status_code == 429:
                try:
                    retry_after = resp.json().get("parameters", {}).get("retry_after", 1)
                except Exception:
                    retry_after = 1
                wait = retry_after + 0.5
                log(logging.WARNING, "telegram_rate_limited", retry_after=retry_after, wait=wait, attempt=attempt)
                time.sleep(wait)
                continue
            if 500 <= resp.status_code < 600:
                log(logging.WARNING, "telegram_server_error_retry", status=resp.status_code, attempt=attempt, delay=round(delay, 3))
                time.sleep(delay)
                delay *= 2
                continue
            log(logging.ERROR, "telegram_http_error", status=resp.status_code, body_snippet=resp.text[:200])
            return False
        except Exception as exc:
            log(logging.WARNING, "telegram_network_error_retry", error=str(exc), attempt=attempt, delay=round(delay, 3))
            time.sleep(delay)
            delay *= 2

    log(logging.ERROR, "telegram_give_up")
    return False


def notify_telegram(message):
    limit = 3500
    chunks = []
    while len(message) > limit:
        cut = message.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        chunks.append(message[:cut])
        message = message[cut:].lstrip("\n")
    chunks.append(message)

    ok = True
    for part in chunks:
        ok = _send_telegram(part, TG_BOT_TOKEN, TG_CHAT_ID) and ok
        time.sleep(1.0)
    return ok


def format_listing_line(listing):
    price = "?" if listing.get("price_eur") is None else int(round(listing["price_eur"]))
    area = "?" if listing.get("area_m2") is None else round(listing["area_m2"], 1)
    city = (listing.get("city_slug") or "unknown city").replace("-", " ").title()
    postal_code = listing.get("postal_code")
    location = city if not postal_code else f"{city} ({postal_code})"
    first_seen = listing.get("first_seen_at")
    if first_seen:
        try:
            dt = datetime.datetime.fromisoformat(first_seen.replace("Z", "+00:00")).astimezone(LOCAL_TIMEZONE)
            first_seen = dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            pass
    return [
        f"- {listing['title']}",
        f"  Rent: {price} EUR/month | Area: {area} m2 | {location}",
        f"  Link: {listing['url']}",
        f"  First seen: {first_seen or '?'}",
    ]


def build_alert_message(new_matches, total_matches):
    lines = [
        f"Housing monitor: {len(new_matches)} new listing(s) match your filters.",
        f"Filters: Nice/Cagnes-sur-Mer, >= {int(MIN_AREA_M2)} m2, <= {int(MAX_PRICE_EUR)} EUR/month.",
        f"Current matching listings in database: {total_matches}.",
        "",
    ]
    for listing in new_matches[:10]:
        lines.extend(format_listing_line(listing))
        lines.append("")
    if len(new_matches) > 10:
        lines.append(f"... and {len(new_matches) - 10} more.")
    return "\n".join(lines).strip()


def build_daily_recap_message(matching_listings, generated_at_local):
    ranked = sorted(
        matching_listings,
        key=lambda listing: (
            listing.get("price_eur") if listing.get("price_eur") is not None else float("inf"),
            -(listing.get("area_m2") or 0),
            listing.get("first_seen_at") or "",
        ),
    )
    lines = [
        f"Daily housing recap for {generated_at_local:%Y-%m-%d}.",
        f"{len(matching_listings)} listing(s) currently match your filters.",
        f"Top offers sorted by lowest rent then biggest surface:",
        "",
    ]
    for listing in ranked[:8]:
        lines.extend(format_listing_line(listing))
        lines.append("")
    return "\n".join(lines).strip()


def should_send_daily_recap(previous_db, now_local):
    last_recap_date = previous_db.get("last_daily_recap_date")
    today = now_local.date().isoformat()
    return now_local.hour >= DAILY_RECAP_HOUR and last_recap_date != today


def main():
    log(logging.INFO, "run_start", source_count=len(SOURCES), db_file=DATABASE_FILE)
    previous_db = load_database()
    previous_matching_ids = set(previous_db.get("matching_ids", []))
    previous_listings_by_id = {listing["id"]: listing for listing in previous_db.get("listings", []) if listing.get("id")}
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_local = now_utc.astimezone(LOCAL_TIMEZONE)
    now_utc_iso = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")

    all_listings = []
    scan_errors = []

    for source in SOURCES:
        try:
            all_listings.extend(scan_source(source))
        except Exception as exc:
            scan_errors.append({"source": source["name"], "error": str(exc)})
            log(logging.ERROR, "source_scan_error", source=source["name"], error=str(exc))

    listings_by_id = {}
    for listing in all_listings:
        existing = listings_by_id.get(listing["id"])
        if not existing:
            listings_by_id[listing["id"]] = listing
            continue
        score_existing = int(existing.get("matches_filters")) + int(existing.get("price_eur") is not None) + int(existing.get("area_m2") is not None)
        score_new = int(listing.get("matches_filters")) + int(listing.get("price_eur") is not None) + int(listing.get("area_m2") is not None)
        if score_new > score_existing:
            listings_by_id[listing["id"]] = listing

    listings = sorted(listings_by_id.values(), key=lambda item: (item["source"], item["url"]))
    for listing in listings:
        previous_listing = previous_listings_by_id.get(listing["id"], {})
        listing["first_seen_at"] = previous_listing.get("first_seen_at", now_utc_iso)
        listing["last_seen_at"] = now_utc_iso
        listing["times_seen"] = int(previous_listing.get("times_seen", 0)) + 1

    matching_listings = [listing for listing in listings if listing.get("matches_filters")]
    current_matching_ids = {listing["id"] for listing in matching_listings}
    new_matches = [listing for listing in matching_listings if listing["id"] not in previous_matching_ids]
    send_daily_recap = should_send_daily_recap(previous_db, now_local)

    database = {
        "generated_at": now_utc_iso,
        "filters": {
            "cities": {
                "nice": ["06000", "06100", "06200", "06300"],
                "cagnes-sur-mer": ["06800"],
            },
            "min_area_m2": MIN_AREA_M2,
            "max_price_eur": MAX_PRICE_EUR,
        },
        "sources": [{"name": source["name"], "url": source["url"]} for source in SOURCES],
        "stats": {
            "scanned_sources": len(SOURCES),
            "scan_errors": len(scan_errors),
            "total_listings": len(listings),
            "matching_listings": len(matching_listings),
            "new_matching_listings": len(new_matches),
        },
        "scan_errors": scan_errors,
        "last_daily_recap_date": previous_db.get("last_daily_recap_date"),
        "matching_ids": sorted(current_matching_ids),
        "listings": listings,
    }
    save_database(database)

    if new_matches:
        log(logging.INFO, "sending_alerts", count=len(new_matches))
        notify_telegram(build_alert_message(new_matches, len(matching_listings)))
    else:
        log(logging.INFO, "no_new_matches", matching=len(matching_listings))

    if send_daily_recap and matching_listings:
        log(logging.INFO, "sending_daily_recap", count=len(matching_listings), local_date=now_local.date().isoformat())
        recap_ok = notify_telegram(build_daily_recap_message(matching_listings, now_local))
        if recap_ok:
            database["last_daily_recap_date"] = now_local.date().isoformat()
            save_database(database)
    elif send_daily_recap:
        log(logging.INFO, "skipping_daily_recap_no_matches", local_date=now_local.date().isoformat())
        database["last_daily_recap_date"] = now_local.date().isoformat()
        save_database(database)

    log(
        logging.INFO,
        "run_end",
        total_listings=len(listings),
        matching=len(matching_listings),
        new_matches=len(new_matches),
        errors=len(scan_errors),
    )


if __name__ == "__main__":
    main()
