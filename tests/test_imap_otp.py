from email.message import EmailMessage

from integrations.imap_otp import OTPExtractionRule, extract_otp_from_message


def test_extract_otp_from_message_matches_filters_and_regex():
    msg = EmailMessage()
    msg["From"] = "no-reply@target.com"
    msg["Subject"] = "Your verification code"
    msg.set_content("Use code 654321 to continue checkout")

    payload = extract_otp_from_message(
        msg,
        OTPExtractionRule(
            otp_pattern=r"\b(\d{6})\b",
            allowed_senders=("target.com",),
            subject_keywords=("verification",),
        ),
    )

    assert payload is not None
    assert payload["code"] == "654321"


def test_extract_otp_from_message_returns_none_on_sender_filter_miss():
    msg = EmailMessage()
    msg["From"] = "alerts@other.com"
    msg["Subject"] = "Code"
    msg.set_content("Use code 111222")

    payload = extract_otp_from_message(
        msg,
        OTPExtractionRule(
            otp_pattern=r"\b(\d{6})\b",
            allowed_senders=("target.com",),
            subject_keywords=(),
        ),
    )

    assert payload is None
