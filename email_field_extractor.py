"""
Email Field Extractor - direct extraction, no LLM.
Extracts broker/underwriter info from email metadata using simple parsing.
"""

import re


def extract_email_fields(email_metadata: dict) -> dict:
    """
    Extract structured fields from email metadata without any LLM calls.
    Uses direct field access and simple regex for name parsing.
    """
    if not email_metadata:
        return _empty_result()

    # --- Sender ---
    from_field = email_metadata.get("from", "")
    sender_email, sender_name = _parse_email_field(from_field)

    # --- Receiver ---
    to_field = email_metadata.get("toRecipients", "") or email_metadata.get("userEmail", "")
    if isinstance(to_field, list):
        to_field = to_field[0] if to_field else ""
    receiver_email, receiver_name = _parse_email_field(to_field)

    # Fallback: use userEmail if receiver_email still empty
    if not receiver_email:
        receiver_email = email_metadata.get("userEmail", "")
        receiver_name = receiver_email.split("@")[0].replace(".", " ").title() if receiver_email else ""

    # --- Policy number from subject ---
    subject = email_metadata.get("subject", "")
    policy_number = _extract_policy_number(subject)

    # --- Agency from sender email domain ---
    agency_name = ""
    if sender_email and "@" in sender_email:
        domain = sender_email.split("@")[1]
        agency_name = domain.split(".")[0].replace("-", " ").title()

    result = {
        "sender_email": sender_email or "Not Found",
        "sender_name": sender_name or "Not Found",
        "receiver_email": receiver_email or "Not Found",
        "receiver_name": receiver_name or "Not Found",
        "policy_number": policy_number or "Not Found",
        "agency_name": agency_name or "Not Found",
        "agency_id": "Not Found",
        "email_summary": subject or "Not Found",
        # Backward compatibility aliases
        "broker_email": sender_email or "Not Found",
        "broker_name": sender_name or "Not Found",
        "underwriter_email": receiver_email or "Not Found",
        "underwriter_name": receiver_name or "Not Found",
        "broker_agency_name": agency_name or "Not Found",
        "broker_agency_id": "Not Found",
        "comments": "",
        "timestamp": email_metadata.get("receivedDateTime", "")
    }

    print(f"[EMAIL] Extracted fields (no LLM):")
    print(f"  Sender: {result['sender_name']} ({result['sender_email']})")
    print(f"  Receiver: {result['receiver_name']} ({result['receiver_email']})")
    print(f"  Policy: {result['policy_number']}")

    return result


def _parse_email_field(field: str):
    """Parse 'Name <email>' or plain email string. Returns (email, name)."""
    if not field:
        return "", ""

    field = field.strip()

    # Format: "Display Name <email@domain.com>"
    match = re.match(r'^(.+?)\s*<([^>]+)>$', field)
    if match:
        name = match.group(1).strip().strip('"')
        email = match.group(2).strip()
        return email, name

    # Plain email
    if "@" in field:
        local = field.split("@")[0]
        name = local.replace(".", " ").replace("_", " ").title()
        return field, name

    return "", field


def _extract_policy_number(subject: str) -> str:
    """Try to extract a policy number from the email subject."""
    if not subject:
        return ""
    # Match patterns like PN-123456, Policy 123456, #123456, or standalone long numbers
    patterns = [
        r'PN[-_]?\s*(\d{6,})',
        r'[Pp]olicy\s*[#:]?\s*(\d{6,})',
        r'#\s*(\d{6,})',
        r'\b(\d{8,})\b',
    ]
    for pattern in patterns:
        match = re.search(pattern, subject)
        if match:
            return match.group(1)
    return ""


def _empty_result() -> dict:
    return {
        "sender_email": "Not Found",
        "sender_name": "Not Found",
        "receiver_email": "Not Found",
        "receiver_name": "Not Found",
        "policy_number": "Not Found",
        "agency_name": "Not Found",
        "agency_id": "Not Found",
        "email_summary": "Not Found",
        "broker_email": "Not Found",
        "broker_name": "Not Found",
        "underwriter_email": "Not Found",
        "underwriter_name": "Not Found",
        "broker_agency_name": "Not Found",
        "broker_agency_id": "Not Found",
        "comments": "",
        "timestamp": ""
    }
