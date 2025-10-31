import os, csv, logging, re
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd, requests
from tenacity import retry, stop_after_attempt, wait_fixed

TARGET_DATE_STR = "TODAY"
ALSO_FETCH_PREV = True

try:
    PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    PROJECT_DIR = os.getcwd()

DATA_DIR = os.path.join(PROJECT_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def parse_target_date(s):
    return datetime.today() if str(s).upper()=="TODAY" else datetime.strptime(s, "%Y-%m-%d")

def previous_calendar_day(dt):
    return dt - timedelta(days=1)

REQUEST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/all-reports",
}

def mto_urls_for_date(dt):
    ddmmyyyy = dt.strftime("%d%m%Y")
    f = f"MTO_{ddmmyyyy}.DAT"
    return [
        f"https://archives.nseindia.com/archives/equities/mto/{f}",
        f"https://nsearchives.nseindia.com/archives/equities/mto/{f}",
        f"https://www1.nseindia.com/archives/equities/mto/{f}",
    ]

def looks_like_html(text):
    t = text.lstrip()
    return t.startswith("<") or "<!DOCTYPE" in t or "Access Denied" in t or "Denied" in t

def shorten(text, n=300):
    s = text.replace("\r", " ")[:n]
    return re.sub(r"\s+", " ", s)

@retry(stop=stop_after_attempt(4), wait=wait_fixed(1))
def download_mto_text(dt):
    s = requests.Session()
    s.headers.update(REQUEST_HEADERS)
    try: s.get("https://www.nseindia.com", timeout=15)
    except Exception: pass
    last_error = None
    for url in mto_urls_for_date(dt):
        try:
            logging.info(f"Fetching {url}")
            r = s.get(url, timeout=25)
            if r.status_code != 200:
                last_error = Exception(f"HTTP {r.status_code} from {url}")
                continue
            text = r.text
            if looks_like_html(text):
                logging.warning(f"HTML/error from {url}: {shorten(text)}")
                last_error = Exception("Received HTML/error instead of .DAT")
                continue
            return text
        except Exception as e:
            logging.warning(f"Fetch failed from {url}: {e}")
            last_error = e
    if last_error: raise last_error
    raise RuntimeError("All archive mirrors failed.")

def parse_mto_text_to_frame(raw_text):
    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    header_index = -1
    for i, ln in enumerate(lines):
        low = ln.lower()
        if ("symbol" in low and "series" in low and "," in ln) or ("record type" in low and "name of security" in low and "," in ln):
            header_index = i
            break
    if header_index == -1:
        raise ValueError("Could not find header. " + shorten(raw_text, 500))
    table_lines = [lines[header_index]]
    for ln in lines[header_index + 1:]:
        low = ln.lower()
        if low.startswith("total") or low.startswith("grand total"): break
        if "," in ln: table_lines.append(ln)
    rows = list(csv.reader(StringIO("\n".join(table_lines))))
    if not rows or len(rows) < 2: raise ValueError("No data rows detected.")
    header_raw, data_rows = rows[0], rows[1:]
    header_norm = [h.strip().upper().replace(" ", "_").replace("%", "PCT") for h in header_raw]
    if ("NAME_OF_SECURITY" in header_norm) and ("SERIES" not in header_norm):
        first_len = len(data_rows[0]); header_len = len(header_norm)
        if first_len == header_len + 1:
            try: insert_at = header_norm.index("NAME_OF_SECURITY") + 1
            except ValueError: insert_at = 3
            header_norm = header_norm[:insert_at] + ["SERIES"] + header_norm[insert_at:]
            fixed = []
            for r in data_rows:
                if len(r) == header_len: r = r[:insert_at] + [""] + r[insert_at:]
                fixed.append(r)
            data_rows = fixed
    df = pd.DataFrame(data_rows, columns=header_norm)
    def pick(cands):
        for c in cands:
            if c in df.columns: return c
        raise KeyError(f"Missing {cands}. Got {list(df.columns)}")
    col_symbol = "SYMBOL" if "SYMBOL" in df.columns else pick(["NAME_OF_SECURITY"])
    col_series = pick(["SERIES"])
    col_qty = pick(["QTY_TRADED","QTY_TRADED_(NOS)","QTY_TRADED_NOS","QUANTITY_TRADED"])
    col_deliv = pick(["DELIVERABLE_QTY","DELIVERABLE_QTY_(NOS)","DELIVERABLE_QTY_NOS",
                      "DELIVERABLE_QUANTITY(GROSS_ACROSS_CLIENT_LEVEL)".upper(),
                      "DELIVERABLE_QUANTITY(GROSS_ACROSS_CLIENT_LEVEL)".replace(" ","_").upper()])
    col_deliv_pct = None
    for c in df.columns:
        if "DELIVERABLE" in c and "TRADED" in c and "PCT" in c: col_deliv_pct = c; break
    if not col_deliv_pct:
        for c in df.columns:
            if "DLY" in c and "PCT" in c: col_deliv_pct = c; break
    if not col_deliv_pct:
        for c in df.columns:
            if c.endswith("PCT"): col_deliv_pct = c; break
    if not col_deliv_pct:
        for c in df.columns:
            if "DELIVERABLE" in c and "TRADED" in c and ("TO_TRADED_QUANTITY" in c or "TO_TRADED_QTY" in c): col_deliv_pct = c; break
    if not col_deliv_pct: raise KeyError("Deliverable % column not found.")
    out = df[[col_symbol, col_series, col_qty, col_deliv, col_deliv_pct]].copy()
    out.columns = ["SYMBOL", "SERIES", "QTY_TRADED", "DELIV_QTY", "DELIV_PCT"]
    def to_num(x):
        x = (x or "").replace(",", "").replace("%", "").strip()
        if x in ("", "-", "NA"): return None
        try: return float(x)
        except: return None
    out["QTY_TRADED"] = out["QTY_TRADED"].map(to_num)
    out["DELIV_QTY"] = out["DELIV_QTY"].map(to_num)
    out["DELIV_PCT"] = out["DELIV_PCT"].map(to_num)
    out["SYMBOL"] = out["SYMBOL"].astype(str).str.strip()
    out["SERIES"] = out["SERIES"].astype(str).str.strip()
    out = out[out["SYMBOL"].str.len() > 0].reset_index(drop=True)
    return out

def write_raw(dt, text):
    p = os.path.join(RAW_DIR, f"MTO_{dt.strftime('%Y-%m-%d')}.DAT")
    open(p, "w", encoding="utf-8").write(text)
    return p

def write_clean_csv(dt, df):
    p = os.path.join(CLEAN_DIR, f"delivery_{dt.strftime('%Y-%m-%d')}.csv")
    df.to_csv(p, index=False)
    return p

def fetch_and_clean_for_date(dt):
    text = download_mto_text(dt)
    write_raw(dt, text)
    df = parse_mto_text_to_frame(text)
    path = write_clean_csv(dt, df)
    logging.info(f"Saved clean CSV: {path}")
    return path

target_dt = parse_target_date(TARGET_DATE_STR)
fetch_and_clean_for_date(target_dt)
if ALSO_FETCH_PREV:
    fetch_and_clean_for_date(previous_calendar_day(target_dt))
