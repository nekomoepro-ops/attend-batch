from __future__ import annotations

import re
import json
from pathlib import Path
import time
from datetime import datetime, date, time as dtime, timedelta
from typing import List
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# ===============================
# 設定（このフォルダの config.json）
# ===============================
from config import (
    SERVICE_ACCOUNT_JSON,
    SPREADSHEET_ID,
    SHEET_NAME,
    DB_SPREADSHEET_ID,
    DB_SHEET_NAME,
    ATTEND_URL_TEMPLATE,
    DAYS_AHEAD,
    CUTOFF_HOUR,
    REQUEST_SLEEP,
    TIMEOUT_SEC,
)

JST = ZoneInfo("Asia/Tokyo")


def business_date(now: datetime | None = None) -> date:
    if now is None:
        now = datetime.now(JST)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=JST)

    cutoff = datetime.combine(now.date(), dtime(CUTOFF_HOUR, 0), tzinfo=JST)
    return now.date() - timedelta(days=1) if now < cutoff else now.date()


def target_dates() -> List[str]:
    base = business_date()
    return [(base + timedelta(days=i)).strftime("%Y%m%d") for i in range(DAYS_AHEAD + 1)]

def normalize_gengou_name(name: str) -> str:
    if not name:
        return ""

    name = name.strip()
    cut_chars = r"\(\（\【\『\[\「"
    name = re.split(f"[{cut_chars}]", name, maxsplit=1)[0].strip()

    return name

def _decode_best(raw: bytes) -> tuple[str, str]:
    candidates = ["utf-8", "shift_jis", "cp932", "euc_jp"]
    keywords = ["出勤", "次回", "初出勤", "本日", "受付", "一覧"]

    best_text = raw.decode("utf-8", errors="replace")
    best_enc = "utf-8"
    best_score = -1

    for enc in candidates:
        text = raw.decode(enc, errors="replace")
        score = sum(text.count(k) for k in keywords)
        if score > best_score or (score == best_score and text.count("\uFFFD") < best_text.count("\uFFFD")):
            best_score = score
            best_text = text
            best_enc = enc

    return best_text, best_enc


def fetch_html(url: str) -> str | None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120 Safari/537.36"
        ),
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Referer": "https://www.cityheaven.net/",
    }

    r = requests.get(url, headers=headers, timeout=TIMEOUT_SEC, allow_redirects=True)

    if r.status_code == 404:
        return None
    if r.status_code != 200:
        print(f"SKIP: HTTP {r.status_code} url={url}")
        return None

    html, enc = _decode_best(r.content)
    print(f"DECODED AS: {enc} | len={len(html)}")
    return html


def _extract_girlid_from_block(block) -> str:
    a = block.select_one('a[href*="girlid-"]')
    href = a.get("href", "") if a else ""

    m = re.search(r"girlid-(\d+)", href)
    if m:
        return m.group(1)

    m2 = re.search(r"girlid-(\d+)", str(block))
    return m2.group(1) if m2 else ""


def parse_attend(html: str, yyyymmdd: str) -> List[List[str]]:
    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.select("div.list.attend-list")

    rows: List[List[str]] = []

    for b in blocks:
        name_el = b.select_one("p.name span.link-color") or b.select_one("p.name")
        name = name_el.get_text(strip=True) if name_el else ""
        name = normalize_gengou_name(name)

        clock_el = b.select_one("p.clock")
        clock = clock_el.get_text(strip=True) if clock_el else ""

        if not re.search(r"\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}", clock):
            continue

        clock = re.sub(r"\s*出勤\s*$", "", clock).strip()

        girlid = _extract_girlid_from_block(b)

        if name:
            rows.append([yyyymmdd, girlid, name, clock])

    return rows

def _last_filled_row_in_colA(service, spreadsheet_id: str, sheet_name: str) -> int:
    """
    A列を見て、最後に値が入っている行番号（1始まり）を返す。
    空なら0。
    """
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A:A",
        majorDimension="COLUMNS",
    ).execute()

    col = (resp.get("values") or [[]])[0]
    # col は A1.. の値配列。末尾の空は入ってこないことが多いが、念のため右側もstrip
    last = 0
    for i, v in enumerate(col, start=1):
        if str(v).strip() != "":
            last = i
    return last

def _clear_tail_rows(service, spreadsheet_id: str, sheet_name: str, start_row: int, end_row: int) -> None:
    """
    start_row〜end_row をクリア（A:D想定）。
    """
    if end_row < start_row:
        return
    # A〜D を消す。列数を増やしたいなら D を増やす
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A{start_row}:D{end_row}",
        body={},
    ).execute()

def write_to_target(service, spreadsheet_id: str, sheet_name: str, values: List[List[str]]) -> None:
    # A1から上書き
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()

    # 下に残る古いデータを掃除
    written_rows = len(values)
    last_row = _last_filled_row_in_colA(service, spreadsheet_id, sheet_name)
    _clear_tail_rows(service, spreadsheet_id, sheet_name, written_rows + 1, last_row)

def load_service_account_creds(value: str, scopes: list[str]) -> Credentials:
    v = (value or "").strip()
    if not v:
        raise ValueError("SERVICE_ACCOUNT_JSON が空です")

    # JSON本文ならこっち
    if v.startswith("{"):
        info = json.loads(v)
        return Credentials.from_service_account_info(info, scopes=scopes)

    # ファイルパスならこっち（ローカル互換）
    p = Path(v)
    if p.exists():
        return Credentials.from_service_account_file(str(p), scopes=scopes)

    raise ValueError("SERVICE_ACCOUNT_JSON は JSON本文 か ファイルパスにしてください")

def main():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = load_service_account_creds(SERVICE_ACCOUNT_JSON, scopes=scopes)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    dates = target_dates()
    header = ["日付", "id", "源氏名", "時間"]
    all_rows: List[List[str]] = []

    for d in dates:
        url = ATTEND_URL_TEMPLATE.format(DATE=d)
        print("FETCH:", url)

        html = fetch_html(url)
        if html is None:
            time.sleep(REQUEST_SLEEP)
            continue

        all_rows.extend(parse_attend(html, d))
        time.sleep(REQUEST_SLEEP)

    values = [header] + all_rows
    write_to_target(service, SPREADSHEET_ID, SHEET_NAME, values)
    if str(DB_SPREADSHEET_ID).strip() and str(DB_SHEET_NAME).strip():
        write_to_target(service, DB_SPREADSHEET_ID, DB_SHEET_NAME, values)

    print(f"OK: days={len(dates)} total_rows={len(all_rows)}")
    print("BUSINESS_DATE:", business_date().isoformat(), "CUTOFF_HOUR:", CUTOFF_HOUR)

if __name__ == "__main__":
    main()




