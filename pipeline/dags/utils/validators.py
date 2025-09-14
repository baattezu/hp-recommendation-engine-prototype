def red_policy_ok(text: str):
    if text.upper() == text and any(c.isalpha() for c in text):
        return False, "ALL_CAPS"
    if text.count('!') > 1:
        return False, "TOO_MANY_EXCLAMATIONS"
    if len(text) > 220:
        return False, "TOO_LONG"
    return True, ""
