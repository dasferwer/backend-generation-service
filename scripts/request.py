"""Отправка подписанного JSON теми же байтами, по которым вычислена подпись."""

import hashlib
import hmac
import json
import os
import sys
import urllib.error
import urllib.request


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("Использование: python scripts/request.py URL JSON")
    body = json.dumps(json.loads(sys.argv[2]), separators=(",", ":"), ensure_ascii=False).encode()
    signature = hmac.new(
        os.getenv("WEBHOOK_SECRET", "demo-secret-change-me").encode(), body, hashlib.sha256
    ).hexdigest()
    request = urllib.request.Request(
        sys.argv[1],
        data=body,
        headers={"Content-Type": "application/json", "X-Signature": signature},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            print(response.read().decode())
    except urllib.error.HTTPError as error:
        print(error.read().decode(), file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
