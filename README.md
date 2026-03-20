# AnchorDB

AnchorDB is a lightweight Go service for automated database backups.

## D1 backups

AnchorDB supports Cloudflare D1 as a connection type (`type: d1`).

- `host` maps to `CLOUDFLARE_ACCOUNT_ID`
- `database` maps to `CLOUDFLARE_DATABASE_ID`
- `password` maps to `CLOUDFLARE_API_KEY`

Optional global fallbacks can be configured with environment variables:

- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_DATABASE_ID`
- `CLOUDFLARE_API_KEY`
- `D1_EXPORT_LIMIT` (default `1000`)
- `D1_API_BASE_URL` (default `https://api.cloudflare.com/client/v4`)

> [!WARNING]
> AnchorDB is still in an active development phase.

## Testing

Run the repository's integration-style test suite (self-contained, sqlite-backed):

```bash
go test ./...
```

Run optional live-stack integration tests against a running AnchorDB instance:

```bash
go test -tags=integration ./integration/...
```

By default, live-stack tests target `http://localhost:8080`. Override with:

```bash
ANCHORDB_URL=http://your-host:8080 go test -tags=integration ./integration/...
```
