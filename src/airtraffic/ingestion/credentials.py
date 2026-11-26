"""Load OpenSky OAuth credentials without logging their contents."""

import json
import os
from pathlib import Path


def load_credentials(path=None):
    if path:
        try:
            document = json.loads(Path(path).read_text())
        except (OSError, ValueError):
            raise ValueError('Cannot read OpenSky credentials JSON') from None
        if not isinstance(document, dict):
            raise ValueError('OpenSky credentials must be a JSON object')
        client_id, secret = document.get('clientId'), document.get('clientSecret')
    else:
        client_id = os.getenv('AIRTRAFFIC_OPENSKY_CLIENT_ID')
        secret = os.getenv('AIRTRAFFIC_OPENSKY_CLIENT_SECRET')
    if not all(isinstance(value, str) and value.strip() for value in (client_id, secret)):
        raise ValueError('Provide clientId/clientSecret in --credentials-file or set OpenSky environment variables')
    return client_id, secret
