# Venice Provider Plugin

This plugin adds Venice web-chat upstream support to CLIProxyAPI through the native plugin ABI.

## Capabilities

- Parses `type: "venice"` auth files.
- Supports command-line account import with `--venice-login` and `--venice-cookie`.
- Uses the durable Clerk `__client` cookie from `clerk.venice.ai` to mint fresh Venice bearer tokens.
- Executes OpenAI `chat.completions` requests against `https://outerface.venice.ai/api/inference/chat`.
- Converts Venice newline-delimited response chunks into OpenAI-compatible non-streaming responses.
- Passes Venice streaming chunks through the host stream path.
- Exposes Venice account status and quota metadata through CLIProxyAPI plugin management routes.

## Command-Line Flags

- `--venice-login`: opens Venice and prompts for a `__client` cookie or Cookie header.
- `--venice-cookie`: creates auth from a pasted `__client=...`, Cookie header, Cookie Editor JSON, or copied cURL containing `__client`.

Use the host `--no-browser` flag to skip opening the browser automatically.

Example:

```powershell
cliproxyapi --venice-cookie "__client=..."
```

Interactive:

```powershell
cliproxyapi --venice-login
```

## Auth Storage

The plugin stores provider-owned auth JSON:

```json
{
  "type": "venice",
  "email": "user@example.com",
  "cookie": "__client=...",
  "authorization": "Bearer ...",
  "authorization_expires_at": "2026-06-27T18:30:00Z",
  "user_id": "user_...",
  "account_plan": "pro",
  "quota_checked_at": "2026-06-28T00:00:00Z",
  "quota": {
    "balance": {
      "creditsRemaining": 42
    }
  }
}
```

Only `cookie` with a valid `__client` is required. The authorization fields are cached and refreshed automatically.
Quota fields are collected from Venice's user session response when present, with auth tokens and cookies filtered out.

## Account Management

The plugin registers:

- `GET /v0/management/plugins/venice/accounts`
- `GET /v0/management/plugins/venice/accounts.json`
- Resource menu page: `/v0/resource/plugins/venice/accounts`

The response includes safe account metadata only: email, status, request counts, token expiry, plan, quota, and the time quota was checked.

## Build

Unit tests:

```powershell
$env:GOCACHE=(Resolve-Path .).Path + '\.gocache'
go test ./...
```

Native plugin DLL:

```powershell
$env:CGO_ENABLED='1'
go build -buildmode=c-shared -o dist\cpa-plugin-venice.dll ./cmd/venice
```

On Windows, `-buildmode=c-shared` requires a C toolchain such as MinGW-w64 with `gcc` on `PATH`.
