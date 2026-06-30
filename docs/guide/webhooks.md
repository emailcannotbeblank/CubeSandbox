# Webhook Event Notifications

CubeAPI can asynchronously POST sandbox lifecycle events to user-configured HTTP
endpoints. Delivery runs in background Tokio workers and does not block sandbox
create, delete, pause or resume APIs.

## Configuration

CubeAPI reads Webhook settings from environment variables:

```bash
export CUBE_API_WEBHOOK_ENABLED=true
export CUBE_API_WEBHOOK_ENDPOINTS_JSON='[
  {
    "url": "http://127.0.0.1:9000/webhook",
    "events": [
      "sandbox.created",
      "sandbox.deleted",
      "sandbox.paused",
      "sandbox.resumed"
    ],
    "secret": "change-me"
  }
]'
export CUBE_API_WEBHOOK_QUEUE_SIZE=1024
export CUBE_API_WEBHOOK_WORKERS=4
export CUBE_API_WEBHOOK_TIMEOUT_SECS=3
export CUBE_API_WEBHOOK_MAX_RETRIES=3
```

Endpoint fields:

| Field | Description |
|---|---|
| `url` | Target HTTP URL. |
| `events` | Subscribed event names. Use `["*"]` or an empty array for all events. |
| `secret` | Optional HMAC-SHA256 signing secret. |
| `name` | Optional diagnostic name used in CubeAPI logs. |

## Events

Supported events:

- `sandbox.created`
- `sandbox.deleted`
- `sandbox.paused`
- `sandbox.resumed`

Each lifecycle event is emitted twice with the same event name:

- `phase=start`: the operation has started.
- `phase=end`: the operation finished. The payload includes `end_time`,
  `duration_ms` and `success`.

Both messages include the same `operation_id`. For `sandbox.created`, the start
message is emitted before CubeMaster returns the new sandbox ID, so receivers can
join the start and end messages by `operation_id` and read `sandbox_id` from the
end message.

Example start payload:

```json
{
  "timestamp": "2026-07-01T00:00:00Z",
  "level": "info",
  "event": "sandbox.created",
  "phase": "start",
  "operation_id": "6cb9705b-cfdc-44a2-bcbf-9f4b0a7d51dd",
  "start_time": "2026-07-01T00:00:00Z",
  "template_id": "tpl-xxx",
  "timeout": 15
}
```

Example end payload:

```json
{
  "timestamp": "2026-07-01T00:00:03Z",
  "level": "info",
  "event": "sandbox.created",
  "phase": "end",
  "operation_id": "6cb9705b-cfdc-44a2-bcbf-9f4b0a7d51dd",
  "start_time": "2026-07-01T00:00:00Z",
  "end_time": "2026-07-01T00:00:03Z",
  "duration_ms": 3120,
  "success": true,
  "sandbox_id": "sbx-xxx",
  "template_id": "tpl-xxx"
}
```

If the lifecycle operation fails, the end payload still gets delivered with
`success=false` and an `error` field.

## Headers

CubeAPI sends:

```text
Content-Type: application/json
X-CubeSandbox-Event: sandbox.created
X-CubeSandbox-Timestamp: 2026-07-01T00:00:03Z
X-CubeSandbox-Delivery: <uuid>
X-CubeSandbox-Signature: sha256=<hex>
```

`X-CubeSandbox-Signature` is present only when `secret` is configured.

## Signature Verification

The signature input is:

```text
timestamp + "." + raw_request_body
```

Python example:

```python
import hashlib
import hmac

def verify(secret, timestamp, body, signature):
    expected = hmac.new(
        secret.encode("utf-8"),
        timestamp.encode("utf-8") + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature, f"sha256={expected}")
```

## Failure Handling

HTTP 2xx responses are treated as success. Network errors, timeouts and non-2xx
responses are retried with exponential backoff. If all attempts fail, CubeAPI
logs the failure and drops that delivery.

If the in-memory queue is full, CubeAPI drops new Webhook events and logs a
warning. Sandbox API requests still continue normally.

## WeCom and Alerting Integration

Enterprise WeChat robots usually expect their own message format, so point
CubeAPI at a small receiver or relay service. The relay can verify the
CubeSandbox signature, transform the event into a WeCom `markdown` or `text`
message, then POST it to the robot URL.

See `examples/webhook-receiver/` for a runnable receiver.
