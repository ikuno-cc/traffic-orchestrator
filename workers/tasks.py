import json
import os
import re
import time
import base64
from datetime import datetime
from typing import Any, Optional

import redis
import requests as http_requests

from app.supabase_sync import is_supabase_enabled, sync_request_to_supabase
from workers.celery_app import celery_app

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
)

SERVICES_KEY = "orchestrator:services"
REQUESTS_KEY = "orchestrator:requests"
PAUSED_KEY = "orchestrator:paused_services"
WORKER_LIMIT_KEY = "orchestrator:worker_limit"
RUNNING_SLOTS_KEY = "orchestrator:running_slots"


class NonRetryableDispatchError(Exception):
    """Raised for request failures that should not be retried."""


def _resolve_url(url: Optional[str]) -> Optional[str]:
    """Map localhost URLs to host.docker.internal for container-to-host calls."""
    if not url:
        return url
    fixed = re.sub(
        r"(https?://)(?:localhost|127\.0\.0\.1)",
        r"\1host.docker.internal",
        url,
        flags=re.IGNORECASE,
    )
    if fixed != url:
        print(f"[URL-FIX] {url} -> {fixed}")
    return fixed


def _update_request(request_id: str, updates: dict[str, Any]) -> None:
    raw = redis_client.hget(REQUESTS_KEY, request_id)
    if not raw:
        return
    record = json.loads(raw)
    record.update(updates)
    record["updated_at"] = datetime.utcnow().isoformat()
    redis_client.hset(REQUESTS_KEY, request_id, json.dumps(record))
    if is_supabase_enabled():
        sync_request_to_supabase(record)


def _truncate(value: Any, limit: int = 500) -> str:
    return str(value)[:limit]


def _get_parallel_limit() -> int:
    raw = redis_client.get(WORKER_LIMIT_KEY)
    try:
        val = int(raw) if raw else 0
    except ValueError:
        val = 0
    return max(0, val)


def _try_acquire_slot(limit: int) -> bool:
    if limit <= 0:
        return True

    for _ in range(10):
        pipe = redis_client.pipeline()
        try:
            pipe.watch(RUNNING_SLOTS_KEY)
            current = int(pipe.get(RUNNING_SLOTS_KEY) or 0)
            if current >= limit:
                return False
            pipe.multi()
            pipe.incr(RUNNING_SLOTS_KEY)
            pipe.execute()
            return True
        except redis.WatchError:
            continue
        finally:
            pipe.reset()
    return False


def _release_slot() -> None:
    for _ in range(10):
        pipe = redis_client.pipeline()
        try:
            pipe.watch(RUNNING_SLOTS_KEY)
            current = int(pipe.get(RUNNING_SLOTS_KEY) or 0)
            new_val = max(0, current - 1)
            pipe.multi()
            pipe.set(RUNNING_SLOTS_KEY, new_val)
            pipe.execute()
            return
        except redis.WatchError:
            continue
        finally:
            pipe.reset()


@celery_app.task(bind=True, name="workers.tasks.dispatch_task", max_retries=3)
def dispatch_task(self, request_id: str, service_id: str, payload: Any, webhook_url: Optional[str] = None):
    """Main dispatch task that routes payload to the target service."""

    if redis_client.sismember(PAUSED_KEY, service_id):
        _update_request(request_id, {"status": "paused"})
        return {"status": "paused", "request_id": request_id}

    raw = redis_client.hget(SERVICES_KEY, service_id)
    if not raw:
        _update_request(request_id, {"status": "failed", "error": "Service not found"})
        return {"status": "failed", "request_id": request_id, "error": "Service not found"}

    service = json.loads(raw)
    service_url = _resolve_url(service.get("url"))
    webhook_url = _resolve_url(webhook_url)

    slot_acquired = False
    while not slot_acquired:
        limit = _get_parallel_limit()
        if _try_acquire_slot(limit):
            slot_acquired = True
            break
        _update_request(
            request_id,
            {
                "status": "queued",
                "error": f"Waiting for worker slot ({limit} max parallel)",
            },
        )
        time.sleep(0.3)

    _update_request(request_id, {"status": "running", "error": None})

    try:
        service_type = service.get("type", "custom")
        headers = service.get("headers", {})
        timeout = int(service.get("timeout", 120))

        if service_type == "comfyui":
            response = _send_to_comfyui(service_url, payload, headers, timeout)
        elif service_type == "n8n":
            response = _send_to_n8n(service_url, payload, headers, timeout)
        else:
            response = _send_generic(service_url, payload, headers, timeout)

        _update_request(
            request_id,
            {
                "status": "success",
                "response_summary": str(response)[:500],
                "error": None,
            },
        )

        if webhook_url:
            webhook_result = _fire_webhook(
                webhook_url,
                {
                    "request_id": request_id,
                    "status": "success",
                    "service_id": service_id,
                    "response": response,
                },
            )
            if webhook_result:
                _update_request(
                    request_id,
                    {
                        "webhook_status": webhook_result.get("status"),
                        "webhook_response_summary": _truncate(webhook_result.get("body")),
                        "webhook_error": webhook_result.get("error"),
                    },
                )

        return {"status": "success", "request_id": request_id, "response": response}

    except Exception as exc:
        error_msg = str(exc)
        attempt = self.request.retries
        max_ret = self.max_retries

        if isinstance(exc, NonRetryableDispatchError):
            _update_request(
                request_id,
                {
                    "status": "failed",
                    "error": error_msg,
                    "retry_count": attempt + 1,
                },
            )
            webhook_result = _fire_webhook(
                webhook_url,
                {
                    "request_id": request_id,
                    "status": "failed",
                    "service_id": service_id,
                    "error": error_msg,
                },
            )
            if webhook_result:
                _update_request(
                    request_id,
                    {
                        "webhook_status": webhook_result.get("status"),
                        "webhook_response_summary": _truncate(webhook_result.get("body")),
                        "webhook_error": webhook_result.get("error"),
                    },
                )
            return {"status": "failed", "request_id": request_id, "error": error_msg}

        if attempt < max_ret:
            countdown = 5 * (2 ** attempt)
            _update_request(
                request_id,
                {
                    "status": "retrying",
                    "error": f"Attempt {attempt + 1}/{max_ret + 1} failed: {error_msg}. Retrying in {countdown}s...",
                    "retry_count": attempt + 1,
                },
            )
            raise self.retry(exc=exc, countdown=countdown)

        _update_request(
            request_id,
            {
                "status": "failed",
                "error": error_msg,
                "retry_count": attempt + 1,
            },
        )
        webhook_result = _fire_webhook(
            webhook_url,
            {
                "request_id": request_id,
                "status": "failed",
                "service_id": service_id,
                "error": error_msg,
            },
        )
        if webhook_result:
            _update_request(
                request_id,
                {
                    "webhook_status": webhook_result.get("status"),
                    "webhook_response_summary": _truncate(webhook_result.get("body")),
                    "webhook_error": webhook_result.get("error"),
                },
            )
        return {"status": "failed", "request_id": request_id, "error": error_msg}
    finally:
        if slot_acquired:
            _release_slot()


def _fire_webhook(webhook_url: Optional[str], body: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not webhook_url:
        return None
    try:
        resp = http_requests.post(
            webhook_url,
            json=body,
            headers={"Content-Type": "application/json", "Accept": "application/json, text/plain, */*"},
            timeout=10,
        )
        parsed = _safe_json_or_text(resp)
        error = None if resp.status_code < 400 else _truncate(parsed, 1000)
        return {"status": resp.status_code, "body": parsed, "error": error}
    except Exception as exc:
        print(f"[WEBHOOK] Failed to call {webhook_url}: {exc}")
        return {"status": None, "body": None, "error": str(exc)}


def _safe_json_or_text(resp: http_requests.Response) -> dict[str, Any]:
    content_type = (resp.headers.get("Content-Type") or "").lower()
    content_bytes = resp.content or b""
    try:
        return resp.json()
    except Exception:
        pass

    # Preserve non-text payloads (images/files/audio/etc.) via base64 envelope.
    # Also detect binary by bytes because some services return wrong/missing content-type.
    is_declared_textual = (
        content_type.startswith("text/")
        or "json" in content_type
        or "xml" in content_type
        or "javascript" in content_type
        or "x-www-form-urlencoded" in content_type
    )
    looks_binary = (
        b"\x00" in content_bytes
        or any(
            (b < 9) or (13 < b < 32)
            for b in content_bytes[:2048]
        )
    )
    if (not is_declared_textual) or looks_binary:
        return {
            "__binary__": True,
            "status_code": resp.status_code,
            "content_type": resp.headers.get("Content-Type") or "application/octet-stream",
            "content_disposition": resp.headers.get("Content-Disposition"),
            "body_base64": base64.b64encode(content_bytes).decode("ascii"),
            "size_bytes": len(content_bytes),
        }

    text = (resp.text or "").strip()
    return {"raw": text[:1000], "status": resp.status_code}


def _send_to_comfyui(url: str, payload: Any, headers: dict[str, Any], timeout: int):
    base_url = url.rstrip("/")
    endpoint = base_url + "/prompt"
    body = _normalize_comfyui_payload(payload)
    resp = http_requests.post(
        endpoint,
        json=body,
        headers={"Content-Type": "application/json", **headers},
        timeout=timeout,
    )
    if resp.status_code >= 400:
        detail = _safe_json_or_text(resp)
        message = f"ComfyUI {resp.status_code} at {endpoint}: {detail}"
        if 400 <= resp.status_code < 500:
            raise NonRetryableDispatchError(message)
        raise RuntimeError(message)
    queued = _safe_json_or_text(resp)
    prompt_id = _extract_prompt_id(queued)
    if not prompt_id:
        raise RuntimeError(f"ComfyUI enqueue response missing prompt_id: {queued}")

    history_entry = _wait_for_comfyui_completion(base_url, prompt_id, headers, timeout)
    status_str, error_msg = _extract_comfyui_terminal_status(history_entry)
    if status_str not in {"success", "succeeded"}:
        detail = error_msg or f"ComfyUI execution ended with status '{status_str}'"
        raise NonRetryableDispatchError(f"ComfyUI prompt {prompt_id} failed: {detail}")

    return {
        "prompt_id": prompt_id,
        "queue_response": queued,
        "history": history_entry,
    }


def _normalize_comfyui_payload(payload: Any) -> dict[str, Any]:
    """Accept raw graph payloads or n8n-wrapped payloads for ComfyUI."""
    if isinstance(payload, dict):
        if isinstance(payload.get("prompt"), dict):
            return payload
        if isinstance(payload.get("workflow"), dict):
            body = {"prompt": payload["workflow"]}
            client_id = payload.get("client_id")
            if client_id is not None:
                body["client_id"] = client_id
            extra_data = payload.get("extra_data")
            if isinstance(extra_data, dict):
                body["extra_data"] = extra_data
            return body
    return {"prompt": payload}


def _extract_prompt_id(response_body: Any) -> Optional[str]:
    if isinstance(response_body, dict):
        prompt_id = response_body.get("prompt_id")
        if isinstance(prompt_id, str) and prompt_id:
            return prompt_id
    return None


def _wait_for_comfyui_completion(
    base_url: str, prompt_id: str, headers: dict[str, Any], timeout: int
) -> dict[str, Any]:
    endpoint = f"{base_url}/history/{prompt_id}"
    deadline = time.time() + max(1, timeout)
    poll_interval_seconds = 2.0
    request_headers = dict(headers or {})

    while time.time() < deadline:
        resp = http_requests.get(endpoint, headers=request_headers, timeout=15)
        if resp.status_code >= 400:
            detail = _safe_json_or_text(resp)
            raise RuntimeError(f"ComfyUI history poll failed {resp.status_code} at {endpoint}: {detail}")

        body = _safe_json_or_text(resp)
        entry = _history_entry_for_prompt(body, prompt_id)
        if entry is not None:
            status_str, _ = _extract_comfyui_terminal_status(entry)
            if status_str is not None:
                return entry
        time.sleep(poll_interval_seconds)

    raise RuntimeError(f"Timed out waiting for ComfyUI prompt {prompt_id} completion after {timeout}s")


def _history_entry_for_prompt(history_body: Any, prompt_id: str) -> Optional[dict[str, Any]]:
    if not isinstance(history_body, dict):
        return None

    direct = history_body.get(prompt_id)
    if isinstance(direct, dict):
        return direct

    if history_body.get("prompt_id") == prompt_id:
        return history_body

    return None


def _extract_comfyui_terminal_status(history_entry: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    status = history_entry.get("status")
    if isinstance(status, dict):
        status_str = status.get("status_str")
        completed = status.get("completed")
        if isinstance(status_str, str):
            error_msg = _extract_comfyui_error_from_messages(status.get("messages"))
            return status_str.lower(), error_msg
        if completed is True:
            return "success", None
    return None, None


def _extract_comfyui_error_from_messages(messages: Any) -> Optional[str]:
    if not isinstance(messages, list):
        return None
    for message in messages:
        if not (isinstance(message, list) and len(message) >= 2 and isinstance(message[1], dict)):
            continue
        payload = message[1]
        if payload.get("exception_message"):
            return str(payload.get("exception_message"))
        if payload.get("error"):
            return str(payload.get("error"))
    return None


def _send_to_n8n(url: str, payload: Any, headers: dict[str, Any], timeout: int):
    resp = http_requests.post(
        url,
        json=payload,
        headers={"Content-Type": "application/json", **headers},
        timeout=timeout,
    )
    resp.raise_for_status()
    return _safe_json_or_text(resp)


def _send_generic(url: str, payload: Any, headers: dict[str, Any], timeout: int):
    resp = http_requests.post(
        url,
        json=payload,
        headers={"Content-Type": "application/json", **headers},
        timeout=timeout,
    )
    resp.raise_for_status()
    return _safe_json_or_text(resp)
