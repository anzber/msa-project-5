#!/usr/bin/env python3
"""Простой клиент, который вызывает API запуска ETL-задачи.

Клиент отправляет W3C-заголовок traceparent, поэтому сервер продолжает трейс
клиента: traceId, напечатанный здесь, виден в логах приложения в Kibana.
"""

import argparse
import json
import secrets
import urllib.error
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser(description="Запуск ETL-задачи через API")
    parser.add_argument("--url", default="http://localhost:8082",
                        help="базовый URL приложения (по умолчанию http://localhost:8082)")
    parser.add_argument("--source", default="python-client",
                        help="идентификатор источника запроса")
    args = parser.parse_args()

    trace_id = secrets.token_hex(16)
    span_id = secrets.token_hex(8)
    endpoint = f"{args.url}/api/jobs/import-products?source={args.source}"

    request = urllib.request.Request(
        endpoint,
        data=b"",
        method="POST",
        headers={
            "traceparent": f"00-{trace_id}-{span_id}-01",
            "Content-Type": "application/json",
        },
    )

    print(f"POST {endpoint}")
    print(f"client traceId: {trace_id}")
    print(f"client spanId : {span_id}")

    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            payload = response.read().decode("utf-8")
            print(f"HTTP {response.status}")
            print(json.dumps(json.loads(payload), indent=2, ensure_ascii=False))
    except urllib.error.HTTPError as error:
        print(f"HTTP {error.code}")
        print(error.read().decode("utf-8"))


if __name__ == "__main__":
    main()
