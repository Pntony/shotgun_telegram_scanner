import os, re, hashlib, json, time, datetime
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

URLS = [
    "https://www.elysee.fr/actualites", 
    "https://www.elysee.fr/toutes-les-actualites",  

    "https://www.elysee.fr/emmanuel-macron/2024/09/16/les-journees-europeennes-du-patrimoine-2024-au-palais-de-lelysee",
    "https://www.elysee.fr/emmanuel-macron/2023/09/07/journees-du-patrimoine-2023",
    "https://www.elysee.fr/emmanuel-macron/2022/09/11/journees-du-patrimoine-2022",
]

# --- MODIF: regex -> libellé humain ---
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
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def fetch(url):
    headers = {"User-Agent": "Mozilla/5.0 (monitor-bot)"}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text

def textify(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ").lower()

def find_keywords(text):
    """Retourne la liste des libellés humains trouvés dans le texte."""
    found = []
    for pattern, label in KEYWORDS.items():
        if re.search(pattern, text, flags=re.IGNORECASE):
            found.append(label)
    return found

def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def notify_telegram(msg):
    if not (TG_BOT_TOKEN and TG_CHAT_ID):
        print("[WARN] Telegram non configuré, message aurait été :", msg)
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": msg, "disable_web_page_preview": True}
    try:
        requests.post(url, data=payload, timeout=15)
    except Exception as e:
        print("[ERR] Envoi Telegram:", e)

def main():
    state = load_state()
    changed_any = False

    urls_to_check = URLS + generate_candidate_urls(2025)

    for url in urls_to_check:
        try:
            html = fetch(url)
            txt = textify(html)
            found = find_keywords(txt)
            content_hash = sha256(txt)

            prev = state.get(url, {})
            prev_hash = prev.get("hash")
            prev_found = prev.get("found", [])

            trigger = (not prev_found and found) or (content_hash != prev_hash and found)

            if trigger:
                keywords_str = ", ".join(found)
                notify_telegram(f"🔔 Élysée : signaux repérés sur {url}\n👉 Mots-clés trouvés : {keywords_str}")
                changed_any = True

            state[url] = {"hash": content_hash, "found": found, "last_check": int(time.time())}

        except Exception as e:
            state[url] = {"error": str(e), "last_check": int(time.time())}

    if changed_any:
        notify_telegram("💡 Vérifie rapidement, il pourrait s’agir de la billetterie ou des infos JEP 2025.")

    save_state(state)

if __name__ == "__main__":
    main()
