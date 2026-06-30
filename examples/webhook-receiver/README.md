# CubeSandbox Webhook Receiver Example

This example starts a small HTTP server that accepts CubeAPI Webhook events and
optionally verifies `X-CubeSandbox-Signature`.

## Run

```bash
cd examples/webhook-receiver
export CUBE_WEBHOOK_SECRET=change-me
python3 receiver.py
```

Configure CubeAPI:

```bash
export CUBE_API_WEBHOOK_ENABLED=true
export CUBE_API_WEBHOOK_ENDPOINTS_JSON='[{"url":"http://127.0.0.1:9000/webhook","events":["sandbox.created","sandbox.deleted","sandbox.paused","sandbox.resumed"],"secret":"change-me"}]'
```

Then restart CubeAPI and create, pause, resume or delete a sandbox. Each
lifecycle event is delivered twice:

- `phase=start`: operation start time
- `phase=end`: operation end time, duration and success flag

## Signature

When `secret` is configured, CubeAPI signs:

```text
timestamp + "." + raw_request_body
```

with HMAC-SHA256 and sends:

```text
X-CubeSandbox-Signature: sha256=<hex>
```
