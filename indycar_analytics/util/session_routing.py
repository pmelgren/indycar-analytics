import re


def get_session_prefix(file_name, session_token_index, fallback_session_token_index=None):
    tokens = [p.upper() for p in file_name.split(';') if p]

    session_token = ''
    if len(tokens) > session_token_index:
        session_token = tokens[session_token_index]
    elif fallback_session_token_index is not None and len(tokens) > fallback_session_token_index:
        session_token = tokens[fallback_session_token_index]

    normalized = re.sub(r'[^A-Z0-9]', '_', session_token)

    if (
        'QUAL' in normalized
        or 'FAST_12' in normalized
        or 'FAST12' in normalized
        or 'FAST_6' in normalized
        or 'FAST6' in normalized
        or 'POLE' in normalized
    ):
        return 'Qualifying'

    if (
        'PRACTICE' in normalized
        or 'WARMUP' in normalized
        or 'WARM_UP' in normalized
        or 'ORIENTATION' in normalized
        or 'TEST' in normalized
    ):
        return 'Practice'

    return 'Race'
