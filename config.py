# -*- coding: utf-8 -*-
"""
出勤情報（attend_sync）用の設定読み込み。
このフォルダ内の config.json を参照します。

使い方:
  1. config.example.json を config.json にコピー
  2. config.json を編集してあなたの環境の値を入れる
  3. python attend_sync.py を実行（または run_attend_sync.bat）

GitHub Actions:
  - Secrets に APP_CONFIG_JSON を入れる（JSON文字列）
  - サービスアカウントは GOOGLE_SERVICE_ACCOUNT_JSON を優先（入れ子事故防止）
"""

from __future__ import annotations

import json
import os
from pathlib import Path

_CONFIG_DIR = Path(__file__).resolve().parent
_CONFIG_PATH = _CONFIG_DIR / "config.json"
_EXAMPLE_PATH = _CONFIG_DIR / "config.example.json"

_REQUIRED_KEYS = (
    "service_account_json",
    "spreadsheet_id",
    "sheet_name",
    "attend_url_template",
)

_OPTIONAL_DEFAULTS = {
    "days_ahead": 14,
    "cutoff_hour": 3,
    "request_sleep": 1.0,
    "timeout_sec": 30,
}


def _load_config() -> dict:
    # 1) GitHub Actions用：Secrets（環境変数）から読む
    raw = os.environ.get("APP_CONFIG_JSON", "").strip()
    if raw:
        data = json.loads(raw)
    else:
        # 2) ローカル用：config.json
        if not _CONFIG_PATH.exists():
            hint = ""
            if _EXAMPLE_PATH.exists():
                hint = f"\n  {_EXAMPLE_PATH.name} を config.json にコピーして編集してください。"
            raise FileNotFoundError(
                f"設定ファイルが見つかりません: {_CONFIG_PATH}{hint}"
            )

        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

    # サービスアカウントは別Secretを優先（入れ子JSON事故防止）
    raw_sa = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw_sa:
        data["service_account_json"] = raw_sa

    # 必須チェック
    for key in _REQUIRED_KEYS:
        if key not in data or not str(data[key]).strip():
            raise ValueError(f"config に必須キー '{key}' を設定してください。")

    # デフォルト補完
    for key, default in _OPTIONAL_DEFAULTS.items():
        if key not in data:
            data[key] = default

    return data


def _get_config() -> dict:
    if not hasattr(_get_config, "_cache"):
        _get_config._cache = _load_config()  # type: ignore[attr-defined]
    return _get_config._cache  # type: ignore[attr-defined]


def __getattr__(name: str):
    key_map = {
        "SERVICE_ACCOUNT_JSON": "service_account_json",
        "SPREADSHEET_ID": "spreadsheet_id",
        "SHEET_NAME": "sheet_name",
        "ATTEND_URL_TEMPLATE": "attend_url_template",
        "DAYS_AHEAD": "days_ahead",
        "CUTOFF_HOUR": "cutoff_hour",
        "REQUEST_SLEEP": "request_sleep",
        "TIMEOUT_SEC": "timeout_sec",
    }
    if name not in key_map:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return _get_config()[key_map[name]]


__all__ = [
    "SERVICE_ACCOUNT_JSON",
    "SPREADSHEET_ID",
    "SHEET_NAME",
    "ATTEND_URL_TEMPLATE",
    "DAYS_AHEAD",
    "CUTOFF_HOUR",
    "REQUEST_SLEEP",
    "TIMEOUT_SEC",
]
