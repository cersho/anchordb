# AnchorDB

AnchorDB is a lightweight Go service for automated database backups.

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
