from __future__ import annotations

import re
import json
import time
from pathlib import Path
from datetime import datetime, date, time as dtime, timedelta
from typing import List, Tuple
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from config import (
    SERVICE_ACCOUNT_JSON,
    SPREADSHEET_ID,
    SHEET_NAME,
    ATTEND_URL_TEMPLATE,
    DAYS_AHEAD,
    CUTOFF_HOUR,
    REQUEST_SLEEP,
    TIMEOUT_SEC,
)

JST = ZoneInfo("Asia/Tokyo")


# ===============================
# 日付まわり
# ===============================
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


def _min_target_date_str(dates: List[str]) -> str:
    return min(dates) if dates else business_date().strftime("%Y%m%d")


# ===============================
# HTML 取得/解析
# ===============================
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


def fetch_html(url: str) -> tuple[str | None, int]:
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
        return None, 404

    if r.status_code != 200:
        print(f"SKIP: HTTP {r.status_code} url={url}")
        return None, r.status_code

    html, enc = _decode_best(r.content)
    print(f"DECODED AS: {enc} | len={len(html)}")
    return html, 200


def _extract_girlid_from_block(block) -> str:
    a = block.select_one('a[href*="girlid-"]')
    href = a.get("href", "") if a else ""

    m = re.search(r"girlid-(\d+)", href)
    if m:
        return m.group(1)

    m2 = re.search(r"girlid-(\d+)", str(block))
    return m2.group(1) if m2 else ""


def _normalize_schedule(clock: str) -> str:
    """
    例: '14:00 - 2:00' / '14:00-02:00' などを '14:00 - 2:00' っぽく整形
    """
    s = (clock or "").strip()
    s = re.sub(r"\s*出勤\s*$", "", s).strip()
    s = re.sub(r"\s*-\s*", " - ", s)
    return s


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

        girlid = _extract_girlid_from_block(b)
        schedule = _normalize_schedule(clock)

        if name:
            rows.append([yyyymmdd, girlid, name, schedule])

    return rows


# ===============================
# Sheets（DB追記 + 重複排除）
# ===============================
def load_service_account_creds(value: str, scopes: list[str]) -> Credentials:
    v = (value or "").strip()
    if not v:
        raise ValueError("SERVICE_ACCOUNT_JSON が空です")

    # JSON本文
    if v.startswith("{"):
        info = json.loads(v)
        return Credentials.from_service_account_info(info, scopes=scopes)

    # ファイルパス（ローカル互換）
    p = Path(v)
    if p.exists():
        return Credentials.from_service_account_file(str(p), scopes=scopes)

    raise ValueError("SERVICE_ACCOUNT_JSON は JSON本文 か ファイルパスにしてください")


def ensure_header(service, spreadsheet_id: str, sheet_name: str, header: List[str]) -> None:
    """
    A1 が空ならヘッダーを入れる。既に何か入ってたら触らない。
    """
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1:D1",
    ).execute()
    row = (resp.get("values") or [[]])[0]
    if any(str(x).strip() for x in row):
        return

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": [header]},
    ).execute()


def load_existing_keys_for_window(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    min_date_str: str,
) -> set[Tuple[str, str, str]]:
    """
    DBシートの A2:D を読み、min_date_str 以上の行だけキー化して返す
    キー: (business_date, girl_id, schedule)
    """
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A2:D",
    ).execute()

    values = resp.get("values") or []
    keys: set[Tuple[str, str, str]] = set()

    for r in values:
        if len(r) < 4:
            continue
        d, gid, _name, sched = (r[0], r[1], r[2], r[3])
        d = str(d).strip()
        if not d:
            continue
        # 直近期間だけを見る（YYYYMMDD前提）
        if d >= min_date_str:
            keys.add((d, str(gid).strip(), str(sched).strip()))

    return keys


def append_rows(service, spreadsheet_id: str, sheet_name: str, rows: List[List[str]]) -> None:
    """
    末尾に追記。ヘッダーは含めない想定（データ行だけ）
    """
    if not rows:
        return

    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A:D",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def main():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = load_service_account_creds(SERVICE_ACCOUNT_JSON, scopes=scopes)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    dates = target_dates()
    min_date_str = _min_target_date_str(dates)

    header = ["business_date", "girl_id", "girl_name", "schedule"]

    # DBシート準備
    ensure_header(service, SPREADSHEET_ID, SHEET_NAME, header)

    # 直近期間の既存キーをロード（重複排除用）
    existing_keys = load_existing_keys_for_window(service, SPREADSHEET_ID, SHEET_NAME, min_date_str)

    all_rows: List[List[str]] = []

    for d in dates:
    url = ATTEND_URL_TEMPLATE.format(DATE=d)
    print("FETCH:", url)

    html, status = fetch_html(url)

    if status == 403:
        print("403 detected. Stop further requests.")
        break  # ← これ重要

    if html is None:
        time.sleep(REQUEST_SLEEP)
        continue

    all_rows.extend(parse_attend(html, d))
    time.sleep(REQUEST_SLEEP)
    
    # 重複排除（同じ business_date + girl_id + schedule は追加しない）
    new_rows: List[List[str]] = []
    for r in all_rows:
        if len(r) < 4:
            continue
        key = (str(r[0]).strip(), str(r[1]).strip(), str(r[3]).strip())
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_rows.append(r)

    # new_rows が0なら書き込みしない（何も起きてない日）
    if not new_rows:
        print("No new rows. Skip append.")
        return

    append_rows(service, SPREADSHEET_ID, SHEET_NAME, new_rows)

    print(f"OK: days={len(dates)} fetched_rows={len(all_rows)} appended_rows={len(new_rows)}")
    print("BUSINESS_DATE:", business_date().isoformat(), "CUTOFF_HOUR:", CUTOFF_HOUR)


if __name__ == "__main__":
    main()

