from __future__ import annotations

from html import escape
import time
from typing import Dict, List, Optional


SENDER_ADDRESS = "hello@attribut.me"
DEFAULT_SEND_DELAY_SECONDS = 0.5


def _sender_identity(sender_name: Optional[str]) -> str:
    display_name = str(sender_name or "sender_id").strip() or "sender_id"
    return f"{display_name} <{SENDER_ADDRESS}>"


def build_resend_requests(
    email_rows: List[dict],
    sender_name: Optional[str] = None,
) -> dict:
    sender_identity = _sender_identity(sender_name)
    requests: List[dict] = []
    for row in email_rows:
        recipient = str(row.get("Email") or "").strip()
        subject = str(row.get("Subject") or "").strip()
        message = str(row.get("Message") or "").strip()
        reply_to = str(row.get("Reply To") or "").strip()
        valid = bool(recipient and subject and message and reply_to)
        body = {
            "from": sender_identity,
            "to": [recipient] if recipient else [],
            "subject": subject,
            "text": message,
            "html": f"<p>{escape(message).replace(chr(10), '<br>')}</p>",
        }
        if reply_to:
            body["reply_to"] = [reply_to]
        requests.append(
            {
                "body": body,
                "metadata": {
                    "patient": row.get("Patient") or "—",
                    "email": recipient,
                    "reply_to": reply_to or "",
                    "subject": subject,
                    "recommendation_ids": list(row.get("Recommendation IDs") or []),
                    "valid": valid,
                },
            }
        )

    return {
        "summary": {
            "messages": len(requests),
            "valid_destinations": sum(1 for request in requests if request["metadata"]["valid"]),
            "sender": sender_identity,
        },
        "requests": requests,
    }


def send_resend_requests(
    requests: List[dict],
    *,
    api_key: str,
    delay_seconds: float = DEFAULT_SEND_DELAY_SECONDS,
) -> dict:
    import resend

    resend.api_key = api_key
    results: List[dict] = []
    for index, request in enumerate(requests):
        result = {
            "body": request["body"],
            "metadata": request["metadata"],
            "success": False,
            "response": None,
            "error": None,
        }
        if not request["metadata"].get("valid"):
            result["error"] = "Missing recipient, subject, message, or reply-to email."
            results.append(result)
            continue
        try:
            response = resend.Emails.send(request["body"])
            result["success"] = True
            result["response"] = response
        except Exception as exc:
            result["error"] = str(exc)
        results.append(result)
        if delay_seconds > 0 and index < len(requests) - 1:
            time.sleep(delay_seconds)
    return {"results": results}
