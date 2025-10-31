import os, logging
from datetime import datetime, timedelta
import pandas as pd

TARGET_DATE_STR = "TODAY"

try:
    PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    PROJECT_DIR = os.getcwd()

CLEAN_DIR = os.path.join(PROJECT_DIR, "data", "clean")
OUT_DIR = os.path.join(PROJECT_DIR, "data", "output")
os.makedirs(OUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def parse_target_date(s):
    return datetime.today() if str(s).upper()=="TODAY" else datetime.strptime(s, "%Y-%m-%d")

def previous_calendar_day(dt):
    return dt - timedelta(days=1)

def path_for(date_obj):
    return os.path.join(CLEAN_DIR, f"delivery_{date_obj.strftime('%Y-%m-%d')}.csv")

def main():
    dt_today = parse_target_date(TARGET_DATE_STR)
    dt_prev = previous_calendar_day(dt_today)
    p_today = path_for(dt_today)
    p_prev = path_for(dt_prev)
    if not os.path.exists(p_today): raise FileNotFoundError(f"Missing {p_today}")
    if not os.path.exists(p_prev): raise FileNotFoundError(f"Missing {p_prev}")
    df_today = pd.read_csv(p_today)
    df_prev = pd.read_csv(p_prev)
    df_today = df_today[df_today["SERIES"]=="EQ"][["SYMBOL","DELIV_PCT"]].rename(columns={"DELIV_PCT":"today_deliv_pct"})
    df_prev = df_prev[df_prev["SERIES"]=="EQ"][["SYMBOL","DELIV_PCT"]].rename(columns={"DELIV_PCT":"prev_deliv_pct"})
    merged = df_today.merge(df_prev, on="SYMBOL", how="inner")
    merged["change_deliv_pct"] = merged["today_deliv_pct"] - merged["prev_deliv_pct"]
    top10 = merged.sort_values("change_deliv_pct", ascending=False).head(10).copy()
    top10["date_today"] = dt_today.strftime("%Y-%m-%d")
    top10["date_prev"] = dt_prev.strftime("%Y-%m-%d")
    cols = ["SYMBOL","today_deliv_pct","prev_deliv_pct","change_deliv_pct","date_today","date_prev"]
    top10 = top10[cols]
    out_path = os.path.join(OUT_DIR, f"top10_{dt_today.strftime('%Y-%m-%d')}.csv")
    top10.to_csv(out_path, index=False)
    disp = top10.copy()
    for c in ["today_deliv_pct","prev_deliv_pct","change_deliv_pct"]:
        disp[c] = disp[c].map(lambda x: f"{x:.2f}" if pd.notnull(x) else "")
    print("Top 10 increase in delivery percentage (largest first)")
    print("Date:", dt_today.strftime("%Y-%m-%d"))
    print(disp.to_string(index=False))
    print("\nSaved CSV:", out_path)

if __name__ == "__main__":
    main()
