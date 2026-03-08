from __future__ import annotations

import json
import time
from typing import Dict, List, Optional
from urllib import error, request


GSM_7BIT_CHARS = (
    "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ"
    ' !"#¤%&\'()*+,-./0123456789:;<=>?'
    "ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿"
    "abcdefghijklmnopqrstuvwxyzäöñüà"
)
GSM_EXTENDED_CHARS = "^{}\\[~]|€"
DEFAULT_SEND_DELAY_SECONDS = 0.5


def normalize_smsworks_destination(phone: Optional[str]) -> Optional[str]:
    raw = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if not raw:
        return None
    if raw.startswith("00"):
        return raw[2:]
    if raw.startswith("44"):
        return raw
    if raw.startswith("0"):
        return f"44{raw[1:]}"
    return raw


def _gsm_units(text: str) -> Optional[int]:
    units = 0
    for char in text:
        if char in GSM_7BIT_CHARS:
            units += 1
        elif char in GSM_EXTENDED_CHARS:
            units += 2
        else:
            return None
    return units


def analyze_sms_text(text: Optional[str]) -> Dict[str, object]:
    content = str(text or "")
    gsm_units = _gsm_units(content)
    if gsm_units is None:
        chars = len(content)
        single_limit = 70
        multi_limit = 67
        segments = 0 if chars == 0 else 1 if chars <= single_limit else ((chars + multi_limit - 1) // multi_limit)
        return {
            "encoding": "unicode",
            "length": chars,
            "segments": segments,
        }

    single_limit = 160
    multi_limit = 153
    segments = 0 if gsm_units == 0 else 1 if gsm_units <= single_limit else ((gsm_units + multi_limit - 1) // multi_limit)
    return {
        "encoding": "gsm",
        "length": gsm_units,
        "segments": segments,
    }


def build_smsworks_request_rows(rows: List[dict], sender: Optional[str]) -> List[dict]:
    requests: List[dict] = []
    for row in rows:
        destination = normalize_smsworks_destination(row.get("Phone"))
        message = str(row.get("Message") or "")
        analysis = analyze_sms_text(message)
        requests.append(
            {
                "endpoint": "/message/send",
                "method": "POST",
                "body": {
                    "sender": sender or "",
                    "destination": destination or "",
                    "content": message,
                },
                "metadata": {
                    "patient": row.get("Patient") or "",
                    "nhs_number": row.get("NHS Number") or "",
                    "group_id": row.get("Group ID") or "",
                    "recommendation_ids": list(row.get("Recommendation IDs") or []),
                    "encoding": analysis["encoding"],
                    "length": analysis["length"],
                    "segments": analysis["segments"],
                    "valid_destination": bool(destination),
                },
            }
        )
    return requests


def build_smsworks_dry_run_payload(rows: List[dict], sender: Optional[str]) -> dict:
    request_rows = build_smsworks_request_rows(rows, sender=sender)
    return {
        "provider": "the-sms-works",
        "mode": "dry_run",
        "base_url": "https://api.thesmsworks.co.uk/v1",
        "authorization_header_required": True,
        "requests": request_rows,
        "summary": {
            "messages": len(request_rows),
            "valid_destinations": sum(1 for row in request_rows if row["metadata"]["valid_destination"]),
            "total_segments": sum(int(row["metadata"]["segments"]) for row in request_rows),
            "sender": sender or "",
        },
    }


def send_smsworks_requests(
    request_rows: List[dict],
    jwt: str,
    timeout_seconds: int = 30,
    delay_seconds: float = DEFAULT_SEND_DELAY_SECONDS,
) -> dict:
    results: List[dict] = []
    for index, row in enumerate(request_rows):
        body = dict(row.get("body") or {})
        metadata = dict(row.get("metadata") or {})
        destination = str(body.get("destination") or "").strip()
        sender = str(body.get("sender") or "").strip()
        content = str(body.get("content") or "").strip()

        if not destination or not sender or not content:
            results.append(
                {
                    "success": False,
                    "status_code": None,
                    "response": {},
                    "error": "Missing sender, destination, or content.",
                    "body": body,
                    "metadata": metadata,
                }
            )
            continue

        payload = json.dumps(body).encode("utf-8")
        req = request.Request(
            url=f"https://api.thesmsworks.co.uk/v1{row.get('endpoint', '/message/send')}",
            data=payload,
            method=str(row.get("method") or "POST"),
            headers={
                "Content-Type": "application/json",
                "Authorization": jwt,
            },
        )
        try:
            with request.urlopen(req, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
                parsed = json.loads(raw) if raw else {}
                results.append(
                    {
                        "success": True,
                        "status_code": response.status,
                        "response": parsed,
                        "error": None,
                        "body": body,
                        "metadata": metadata,
                    }
                )
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw) if raw else {}
            except Exception:
                parsed = {"raw": raw}
            results.append(
                {
                    "success": False,
                    "status_code": exc.code,
                    "response": parsed,
                    "error": str(exc),
                    "body": body,
                    "metadata": metadata,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "success": False,
                    "status_code": None,
                    "response": {},
                    "error": str(exc),
                    "body": body,
                    "metadata": metadata,
                }
            )
        if delay_seconds > 0 and index < len(request_rows) - 1 and destination and sender and content:
            time.sleep(delay_seconds)

    return {
        "results": results,
        "sent": sum(1 for row in results if row["success"]),
        "failed": sum(1 for row in results if not row["success"]),
    }
