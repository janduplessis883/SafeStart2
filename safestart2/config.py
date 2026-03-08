from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional

import streamlit as st


@dataclass
class SupabaseSettings:
    url: str
    anon_key: str
    service_role_key: Optional[str] = None

    @property
    def key(self) -> str:
        # Backward-compatible alias for older callers that still expect `key`.
        return self.anon_key


@dataclass
class SMSWorksSettings:
    jwt: str


@dataclass
class ResendSettings:
    api_key: str


def _secret_value(section: Mapping[str, object], *names: str) -> Optional[str]:
    for name in names:
        value = section.get(name)
        if value:
            return str(value)
    return None


def get_supabase_settings() -> Optional[SupabaseSettings]:
    """Return Supabase settings from Streamlit secrets if available."""
    try:
        if "supabase" in st.secrets:
            section = st.secrets["supabase"]
            url = _secret_value(section, "url", "SUPABASE_URL")
            anon_key = _secret_value(
                section,
                "anon_key",
                "key",
                "SUPABASE_ANON_KEY",
                "SUPABASE_KEY",
            )
            service_role_key = _secret_value(
                section,
                "service_role_key",
                "SUPABASE_SERVICE_ROLE_KEY",
            )
        else:
            url = _secret_value(st.secrets, "SUPABASE_URL")
            anon_key = _secret_value(st.secrets, "SUPABASE_ANON_KEY", "SUPABASE_KEY")
            service_role_key = _secret_value(st.secrets, "SUPABASE_SERVICE_ROLE_KEY")

        if not url or not anon_key:
            return None

        return SupabaseSettings(
            url=str(url),
            anon_key=str(anon_key),
            service_role_key=str(service_role_key) if service_role_key else None,
        )
    except Exception:
        return None


def get_smsworks_settings() -> Optional[SMSWorksSettings]:
    """Return The SMS Works settings from Streamlit secrets if available."""
    try:
        if "smsworks" in st.secrets:
            section = st.secrets["smsworks"]
            jwt = _secret_value(section, "jwt", "token", "SMSWORKS_JWT", "SMSWORKS_TOKEN")
        else:
            jwt = _secret_value(st.secrets, "SMSWORKS_JWT", "SMSWORKS_TOKEN")

        if not jwt:
            return None

        return SMSWorksSettings(
            jwt=str(jwt),
        )
    except Exception:
        return None


def get_resend_settings() -> Optional[ResendSettings]:
    """Return Resend settings from Streamlit secrets if available."""
    try:
        if "resend" in st.secrets:
            section = st.secrets["resend"]
            api_key = _secret_value(section, "api_key", "RESEND_API_KEY")
        else:
            api_key = _secret_value(st.secrets, "RESEND_API_KEY")

        if not api_key:
            return None

        return ResendSettings(api_key=str(api_key))
    except Exception:
        return None
