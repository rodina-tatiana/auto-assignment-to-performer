# index.py — Yandex Cloud Functions entrypoint: index.handler
import os
import json
import base64
import sys
import requests
from datetime import datetime, timezone

# --- Env ---
TRACKER_TOKEN = os.environ["TRACKER_TOKEN"]
ORG_ID = os.environ["TRACKER_ORG_ID"]
ORG_HEADER = os.getenv("TRACKER_ORG_HEADER", "X-Org-ID")  # or "X-Cloud-Org-ID"
TRACKER_QUEUE = os.getenv("TRACKER_QUEUE", "PM")
STATUS_TYPES = [x.strip() for x in os.getenv("STATUS_TYPES", "").split(",") if x.strip()]
DEBUG = os.getenv("DEBUG", "0") in ("1", "true", "True", "yes", "on")

BASE = "https://api.tracker.yandex.net"

HEADERS = {
    "Authorization": f"OAuth {TRACKER_TOKEN}",
    ORG_HEADER: ORG_ID,
    "Content-Type": "application/json",
}


def log(message: str):
    # В Cloud Logging время и так отображается отдельно — печатаем только сообщение
    print(message, file=sys.stderr, flush=True)


def log_debug(message: str, **fields):
    if not DEBUG:
        return
    ts = datetime.now(timezone.utc).isoformat()
    line = f"[DEBUG {ts}] {message}"
    if fields:
        line += " | " + json.dumps(fields, ensure_ascii=False)
    print(line, file=sys.stderr, flush=True)


def _resp(status: int, body: dict):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "isBase64Encoded": False,
        "body": json.dumps(body, ensure_ascii=False),
    }


def _parse_body(event: dict) -> dict:
    body = event.get("body")
    if body is None:
        return {}
    if isinstance(body, dict):
        return body
    if isinstance(body, str):
        raw = body
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(body).decode("utf-8", errors="replace")
        raw = raw.strip()
        return json.loads(raw) if raw else {}
    return {}


def get_queue_team_user_ids(queue_key: str) -> list[str]:
    r = requests.get(f"{BASE}/v2/queues/{queue_key}?expand=all", headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()

    team_users = data.get("teamUsers") or []
    ids: list[str] = []
    for u in team_users:
        if not isinstance(u, dict):
            continue
        if u.get("id") is not None:
            ids.append(str(u["id"]))
        elif u.get("uid") is not None:
            ids.append(str(u["uid"]))

    # unique preserving order
    seen = set()
    out = []
    for x in ids:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def build_search_query(user_id: str) -> str:
    base = f"assignee: {user_id}"

    if not STATUS_TYPES:
        return base

    if len(STATUS_TYPES) == 1:
        return f'{base} "Status type": {STATUS_TYPES[0]}'

    st = " OR ".join([f"\"Status type\": {t}" for t in STATUS_TYPES])
    return f"{base} ({st})"


def _get_total_count_header(r: requests.Response) -> str | None:
    return r.headers.get("X-Total-Count") or r.headers.get("x-total-count")


def count_issues_for_assignee_via_search(user_id: str) -> tuple[int, str | None]:
    """
    Возвращает (count, display).
    display берём из assignee.display первой найденной задачи (perPage=1).
    """
    query = build_search_query(user_id)

    url = f"{BASE}/v2/issues/_search"
    params = {"page": 1, "perPage": 1}
    payload = {"query": query}

    r = requests.post(url, headers=HEADERS, params=params, json=payload, timeout=30)
    log_debug("SEARCH", userId=user_id, status=r.status_code, x_total_count=_get_total_count_header(r), query=query)

    r.raise_for_status()

    total = _get_total_count_header(r)
    if total is None:
        raise RuntimeError("X-Total-Count header is missing in Tracker response")

    count = int(total)

    display = None
    try:
        issues = r.json()
        if isinstance(issues, list) and issues:
            assignee = issues[0].get("assignee") or {}
            if isinstance(assignee, dict):
                display = assignee.get("display")
    except Exception:
        display = None

    return count, display


def set_issue_assignee(issue_key: str, user_id: str) -> None:
    payload = {"assignee": {"id": user_id}}
    r = requests.patch(f"{BASE}/v2/issues/{issue_key}", headers=HEADERS, json=payload, timeout=20)
    r.raise_for_status()


def handler(event, context):
    try:
        body = _parse_body(event)
        issue_key = body.get("issueKey") or body.get("key")
        if not issue_key:
            log("Ошибка: в запросе не передан ключ задачи (issueKey)")
            return _resp(400, {"error": "issueKey (or key) is required"})

        log(f"Функция отработала для задачи {issue_key}")

        team_ids = get_queue_team_user_ids(TRACKER_QUEUE)
        if not team_ids:
            log(f"Ошибка: в очереди {TRACKER_QUEUE} не найдено ни одного участника команды")
            return _resp(500, {"error": "No teamUsers with ids found", "queue": TRACKER_QUEUE})

        loads: list[tuple[int, str, str | None]] = []  # (count, id, display)
        for uid in team_ids:
            cnt, display = count_issues_for_assignee_via_search(uid)
            loads.append((cnt, uid, display))

        # выбираем наименее загруженного
        loads.sort(key=lambda x: x[0])
        least_count, least_uid, least_display = loads[0]

        # лог по каждому пользователю — один раз
        for cnt, uid, display in loads:
            marker = " ← выбран" if uid == least_uid else ""
            log(f"У пользователя {display or 'Без имени'} (id: {uid}) — {cnt} задач{marker}")

        # назначаем
        set_issue_assignee(issue_key, least_uid)

        # финальная строка как вы просили: id + count внутри скобок
        log(
            f'Задача "{issue_key}" назначена пользователю {least_display or "Без имени"} '
            f'(id: {least_uid} - {least_count} задач)'
        )

        return _resp(200, {
            "issueKey": issue_key,
            "assignedTo": {"id": least_uid},
            "leastLoad": least_count,
            "allLoads": [{"userId": uid, "count": cnt, "display": (display or None)} for cnt, uid, display in loads],
        })

    except requests.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else "unknown"
        log(f"Ошибка Tracker API: статус {status_code}")
        if DEBUG and e.response is not None:
            log_debug("Tracker API error body", body=e.response.text)
        return _resp(502, {"error": "Tracker API error", "status_code": status_code})

    except Exception as e:
        log(f"Неожиданная ошибка: {e}")
        return _resp(500, {"error": "Unhandled error", "details": str(e)})
