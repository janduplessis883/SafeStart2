from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import pandas as pd


def sanitize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    sanitized = df.copy()
    sanitized.columns = [
        str(column).replace("\ufeff", "").strip()
        if column is not None
        else column
        for column in sanitized.columns
    ]
    return sanitized


def parse_date(value: object) -> Optional[date]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.date()
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()

    raw = str(value).strip()
    if not raw or raw.lower() in {"nan", "nat"}:
        return None

    for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue

    parsed = pd.to_datetime(raw, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.date()


def clean_nhs_number(value: object) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) > 10 and digits.endswith("0") and "." in str(value):
        digits = digits[:-1]
    return digits[:10]


def normalize_phone(value: object) -> Optional[str]:
    raw = str(value).strip()
    if not raw or raw.lower() == "nan":
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits or None


def normalize_email(value: object) -> Optional[str]:
    raw = str(value).strip()
    if not raw or raw.lower() == "nan":
        return None
    if " (" in raw:
        raw = raw.split(" (", 1)[0].strip()
    return raw.lower()


def load_dataframe(uploaded_file) -> pd.DataFrame:
    if uploaded_file.name.lower().endswith(".csv"):
        try:
            return sanitize_dataframe_columns(pd.read_csv(uploaded_file))
        except UnicodeDecodeError:
            uploaded_file.seek(0)
            return sanitize_dataframe_columns(pd.read_csv(uploaded_file, encoding="latin-1"))
    return sanitize_dataframe_columns(pd.read_excel(uploaded_file))
