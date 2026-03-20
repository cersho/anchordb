package scheduler

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"anchordb/internal/config"
	"anchordb/internal/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Executor struct {
	cfg config.Config
}

func NewExecutor(cfg config.Config) *Executor {
	return &Executor{cfg: cfg}
}

func (e *Executor) Run(ctx context.Context, b models.Backup) (string, error) {
	runCtx, cancel := context.WithTimeout(ctx, e.cfg.BackupCommandTimeout)
	defer cancel()
	if strings.EqualFold(b.Connection.Type, "convex") {
		return e.runConvexExport(runCtx, b)
	}
	if strings.EqualFold(b.Connection.Type, "d1") {
		return e.runD1Export(runCtx, b)
	}

	cmd, stdout, err := buildDumpCommand(runCtx, b.Connection)
	if err != nil {
		return "", err
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("start dump command: %w", err)
	}

	reader, closeReader, err := wrapCompression(stdout, b.Compression)
	if err != nil {
		_ = cmd.Process.Kill()
		return "", err
	}
	defer closeReader()

	key := buildObjectKey(b)

	switch b.TargetType {
	case "local":
		err = e.saveLocal(b.LocalPath, key, reader)
	case "s3":
		if b.Remote == nil {
			err = fmt.Errorf("backup target s3 requires remote")
		} else {
			err = e.saveS3(runCtx, *b.Remote, key, reader)
		}
	default:
		err = fmt.Errorf("unsupported target_type: %s", b.TargetType)
	}

	waitErr := cmd.Wait()
	if err != nil {
		if waitErr != nil {
			return "", fmt.Errorf("store backup: %w (dump error: %v, stderr: %s)", err, waitErr, stderr.String())
		}
		return "", fmt.Errorf("store backup: %w", err)
	}
	if waitErr != nil {
		return "", fmt.Errorf("dump command failed: %w; stderr: %s", waitErr, stderr.String())
	}

	return key, nil
}

func (e *Executor) runConvexExport(ctx context.Context, b models.Backup) (string, error) {
	tempDir, err := os.MkdirTemp("", "anchordb-convex-export-*")
	if err != nil {
		return "", fmt.Errorf("create temporary export directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	packageJSONPath := filepath.Join(tempDir, "package.json")
	packageJSON := []byte("{\"name\":\"anchordb-convex-export\",\"private\":true,\"dependencies\":{\"convex\":\"^1.0.0\"}}\n")
	if err := os.WriteFile(packageJSONPath, packageJSON, 0o644); err != nil {
		return "", fmt.Errorf("create temporary package.json: %w", err)
	}

	exportPath := filepath.Join(tempDir, "convex-export.zip")
	args := []string{"--yes", "convex", "export", "--path", exportPath}
	if b.IncludeFileStorage {
		args = append(args, "--include-file-storage")
	}

	cmd := exec.CommandContext(ctx, "npx", args...)
	cmd.Dir = tempDir
	env := os.Environ()
	convexURL, err := normalizeConvexURL(b.Connection.Host)
	if err != nil {
		return "", fmt.Errorf("invalid convex url %q: %w", strings.TrimSpace(b.Connection.Host), err)
	}
	env = append(env,
		"CONVEX_URL="+convexURL,
		"CONVEX_SELF_HOSTED_URL="+convexURL,
		"CONVEX_SELF_HOSTED_ADMIN_KEY="+b.Connection.Password,
	)
	cmd.Env = env

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("convex export failed: %w; stderr: %s", err, strings.TrimSpace(stderr.String()))
	}

	f, err := os.Open(exportPath)
	if err != nil {
		return "", fmt.Errorf("open convex export artifact: %w", err)
	}
	defer func() { _ = f.Close() }()

	key := buildObjectKey(b)
	switch b.TargetType {
	case "local":
		err = e.saveLocal(b.LocalPath, key, f)
	case "s3":
		if b.Remote == nil {
			err = fmt.Errorf("backup target s3 requires remote")
		} else {
			err = e.saveS3(ctx, *b.Remote, key, f)
		}
	default:
		err = fmt.Errorf("unsupported target_type: %s", b.TargetType)
	}
	if err != nil {
		return "", fmt.Errorf("store backup: %w", err)
	}

	return key, nil
}

func (e *Executor) runD1Export(ctx context.Context, b models.Backup) (string, error) {
	accountID := strings.TrimSpace(b.Connection.Host)
	if accountID == "" {
		accountID = strings.TrimSpace(e.cfg.CloudflareAccountID)
	}
	databaseID := strings.TrimSpace(b.Connection.Database)
	if databaseID == "" {
		databaseID = strings.TrimSpace(e.cfg.CloudflareDatabaseID)
	}
	apiKey := strings.TrimSpace(b.Connection.Password)
	if apiKey == "" {
		apiKey = strings.TrimSpace(e.cfg.CloudflareAPIKey)
	}

	if accountID == "" || databaseID == "" || apiKey == "" {
		return "", fmt.Errorf("d1 export requires account id, database id, and api key")
	}

	limit := e.cfg.D1ExportLimit
	if limit <= 0 {
		limit = 1000
	}

	sqlBackup, err := e.buildD1SQLBackup(ctx, accountID, databaseID, apiKey, limit)
	if err != nil {
		return "", err
	}

	reader, closeReader, err := wrapCompression(io.NopCloser(strings.NewReader(sqlBackup)), b.Compression)
	if err != nil {
		return "", err
	}
	defer closeReader()

	key := buildObjectKey(b)
	switch b.TargetType {
	case "local":
		err = e.saveLocal(b.LocalPath, key, reader)
	case "s3":
		if b.Remote == nil {
			err = fmt.Errorf("backup target s3 requires remote")
		} else {
			err = e.saveS3(ctx, *b.Remote, key, reader)
		}
	default:
		err = fmt.Errorf("unsupported target_type: %s", b.TargetType)
	}
	if err != nil {
		return "", fmt.Errorf("store backup: %w", err)
	}

	return key, nil
}

func (e *Executor) buildD1SQLBackup(ctx context.Context, accountID, databaseID, apiKey string, limit int) (string, error) {
	if limit <= 0 {
		limit = 1000
	}

	lines := make([]string, 0, 128)
	appendLine := func(command string) {
		lines = append(lines, command)
	}

	writableSchema := false

	tables, err := e.d1QueryRows(ctx, accountID, databaseID, apiKey,
		"SELECT name, type, sql FROM sqlite_master WHERE sql IS NOT NULL AND type = 'table' ORDER BY rootpage DESC")
	if err != nil {
		return "", err
	}

	for _, table := range tables {
		tableName, _ := table["name"].(string)
		tableSQL, _ := table["sql"].(string)
		if tableName == "" {
			continue
		}

		switch {
		case strings.HasPrefix(tableName, "_cf_"):
			continue
		case tableName == "sqlite_sequence":
			appendLine("DELETE FROM sqlite_sequence;")
			continue
		case tableName == "sqlite_stat1":
			appendLine("ANALYZE sqlite_master;")
			continue
		case strings.HasPrefix(tableName, "sqlite_"):
			continue
		}

		upperSQL := strings.ToUpper(tableSQL)
		switch {
		case strings.HasPrefix(upperSQL, "CREATE VIRTUAL TABLE"):
			if !writableSchema {
				appendLine("PRAGMA writable_schema=ON;")
				writableSchema = true
			}
			escapedName := strings.ReplaceAll(tableName, "'", "''")
			escapedSQL := strings.ReplaceAll(tableSQL, "'", "''")
			appendLine(fmt.Sprintf("INSERT INTO sqlite_master (type, name, tbl_name, rootpage, sql) VALUES ('table', '%s', '%s', 0, '%s');", escapedName, escapedName, escapedSQL))
		case strings.HasPrefix(upperSQL, "CREATE TABLE "):
			appendLine("CREATE TABLE IF NOT EXISTS " + tableSQL[len("CREATE TABLE "):] + ";")
		default:
			appendLine(tableSQL + ";")
		}

		quotedTableName := d1QuoteIdentifier(tableName)
		sampleRows, sampleErr := e.d1QueryRows(ctx, accountID, databaseID, apiKey, fmt.Sprintf("SELECT * FROM %s LIMIT 1", quotedTableName))
		if sampleErr != nil {
			return "", sampleErr
		}
		if len(sampleRows) == 0 {
			continue
		}

		columnNames := make([]string, 0, len(sampleRows[0]))
		for name := range sampleRows[0] {
			columnNames = append(columnNames, name)
		}
		sort.Strings(columnNames)

		countRows, countErr := e.d1QueryRows(ctx, accountID, databaseID, apiKey, fmt.Sprintf("SELECT COUNT(*) AS count FROM %s", quotedTableName))
		if countErr != nil {
			return "", countErr
		}
		if len(countRows) == 0 {
			continue
		}
		count, convErr := d1ToInt(countRows[0]["count"])
		if convErr != nil {
			return "", fmt.Errorf("parse table row count for %s: %w", tableName, convErr)
		}

		for offset := 0; offset <= count; offset += limit {
			queries := make([]string, 0, (len(columnNames)+8)/9)
			for idx := 0; idx < len(columnNames); idx += 9 {
				current := columnNames[idx:min(idx+9, len(columnNames))]
				exprParts := make([]string, 0, len(current))
				for _, colName := range current {
					exprParts = append(exprParts, "'||quote("+d1QuoteIdentifier(colName)+")||'")
				}
				expr := "'" + strings.Join(exprParts, ", ") + "'"
				queries = append(queries, fmt.Sprintf("SELECT %s AS partial_command FROM %s LIMIT %d OFFSET %d", expr, quotedTableName, limit, offset))
			}

			batch, batchErr := e.d1Query(ctx, accountID, databaseID, apiKey, strings.Join(queries, ";\n"))
			if batchErr != nil {
				return "", batchErr
			}
			if len(batch) == 0 || len(batch[0].Results) == 0 {
				continue
			}

			baseRows := len(batch[0].Results)
			for i := 1; i < len(batch); i++ {
				if len(batch[i].Results) != baseRows {
					return "", fmt.Errorf("d1 multi-query split mismatch for table %s", tableName)
				}
			}

			quotedColumns := make([]string, 0, len(columnNames))
			for _, colName := range columnNames {
				quotedColumns = append(quotedColumns, d1QuoteIdentifier(colName))
			}

			for rowIdx := 0; rowIdx < baseRows; rowIdx++ {
				parts := make([]string, 0, len(batch))
				for resultIdx := range batch {
					part, _ := batch[resultIdx].Results[rowIdx]["partial_command"].(string)
					parts = append(parts, strings.ReplaceAll(part, "\n", "\\n"))
				}
				appendLine(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", quotedTableName, strings.Join(quotedColumns, ", "), strings.Join(parts, ", ")))
			}
		}
	}

	schemas, err := e.d1QueryRows(ctx, accountID, databaseID, apiKey,
		"SELECT name, type, sql FROM sqlite_master WHERE sql IS NOT NULL AND type IN ('index', 'trigger', 'view')")
	if err != nil {
		return "", err
	}
	for _, schema := range schemas {
		schemaSQL, _ := schema["sql"].(string)
		if strings.TrimSpace(schemaSQL) == "" {
			continue
		}
		appendLine(schemaSQL + ";")
	}

	if writableSchema {
		appendLine("PRAGMA writable_schema=OFF;")
	}

	return strings.Join(lines, "\n"), nil
}

type d1QueryEnvelope struct {
	Success bool            `json:"success"`
	Errors  []d1ErrorObject `json:"errors"`
	Result  []d1QueryResult `json:"result"`
}

type d1ErrorObject struct {
	Message string `json:"message"`
}

type d1QueryResult struct {
	Success bool             `json:"success"`
	Results []map[string]any `json:"results"`
}

func (e *Executor) d1Query(ctx context.Context, accountID, databaseID, apiKey, sql string) ([]d1QueryResult, error) {
	endpoint := strings.TrimSuffix(strings.TrimSpace(e.cfg.D1APIBaseURL), "/")
	if endpoint == "" {
		endpoint = "https://api.cloudflare.com/client/v4"
	}
	endpoint = endpoint + "/accounts/" + accountID + "/d1/database/" + databaseID + "/query"

	payload, err := json.Marshal(map[string]any{"sql": sql})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var envelope d1QueryEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		if res.StatusCode >= http.StatusBadRequest {
			return nil, fmt.Errorf("d1 query failed (status %d): %s", res.StatusCode, strings.TrimSpace(string(body)))
		}
		return nil, fmt.Errorf("decode d1 response: %w", err)
	}

	if res.StatusCode >= http.StatusBadRequest {
		if len(envelope.Errors) > 0 {
			messages := make([]string, 0, len(envelope.Errors))
			for _, item := range envelope.Errors {
				if strings.TrimSpace(item.Message) != "" {
					messages = append(messages, item.Message)
				}
			}
			return nil, fmt.Errorf("d1 query failed: %s", strings.Join(messages, "; "))
		}
		return nil, fmt.Errorf("d1 query failed (status %d)", res.StatusCode)
	}

	if len(envelope.Errors) > 0 {
		messages := make([]string, 0, len(envelope.Errors))
		for _, item := range envelope.Errors {
			if strings.TrimSpace(item.Message) != "" {
				messages = append(messages, item.Message)
			}
		}
		return nil, fmt.Errorf("d1 query error: %s", strings.Join(messages, "; "))
	}

	for _, result := range envelope.Result {
		if !result.Success {
			return nil, fmt.Errorf("d1 query returned unsuccessful result")
		}
	}

	return envelope.Result, nil
}

func (e *Executor) d1QueryRows(ctx context.Context, accountID, databaseID, apiKey, sql string) ([]map[string]any, error) {
	results, err := e.d1Query(ctx, accountID, databaseID, apiKey, sql)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0].Results, nil
}

func d1QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func d1ToInt(value any) (int, error) {
	switch v := value.(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0, err
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unexpected type %T", value)
	}
}

func (e *Executor) CleanupRetention(ctx context.Context, b models.Backup) error {
	if b.RetentionDays <= 0 {
		return nil
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -b.RetentionDays)
	prefix := sanitizePath(b.Connection.Name) + "/"

	switch b.TargetType {
	case "local":
		return e.cleanupLocal(b.LocalPath, prefix, cutoff)
	case "s3":
		if b.Remote == nil {
			return nil
		}
		return e.cleanupS3(ctx, *b.Remote, prefix, cutoff)
	default:
		return nil
	}
}

func buildDumpCommand(ctx context.Context, c models.Connection) (*exec.Cmd, io.ReadCloser, error) {
	switch strings.ToLower(c.Type) {
	case "postgres", "postgresql":
		args := []string{
			"--host", c.Host,
			"--port", fmt.Sprintf("%d", c.Port),
			"--username", c.Username,
			"--dbname", c.Database,
			"--no-owner",
			"--no-privileges",
		}
		cmd := exec.CommandContext(ctx, "pg_dump", args...)
		env := os.Environ()
		env = append(env, "PGPASSWORD="+c.Password)
		if c.SSLMode != "" {
			env = append(env, "PGSSLMODE="+c.SSLMode)
		}
		cmd.Env = env
		stdout, err := cmd.StdoutPipe()
		return cmd, stdout, err
	case "mysql":
		args := []string{
			"--host", c.Host,
			"--port", fmt.Sprintf("%d", c.Port),
			"--user", c.Username,
			"--single-transaction",
			"--quick",
			c.Database,
		}
		cmd := exec.CommandContext(ctx, "mysqldump", args...)
		env := os.Environ()
		env = append(env, "MYSQL_PWD="+c.Password)
		cmd.Env = env
		stdout, err := cmd.StdoutPipe()
		return cmd, stdout, err
	default:
		return nil, nil, fmt.Errorf("unsupported connection type: %s", c.Type)
	}
}

func wrapCompression(input io.ReadCloser, compression string) (io.Reader, func(), error) {
	if compression != "gzip" {
		return input, func() { _ = input.Close() }, nil
	}

	pr, pw := io.Pipe()
	go func() {
		defer func() {
			_ = input.Close()
		}()
		gw := gzip.NewWriter(pw)
		_, err := io.Copy(gw, input)
		closeErr := gw.Close()
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if closeErr != nil {
			_ = pw.CloseWithError(closeErr)
			return
		}
		_ = pw.Close()
	}()

	return pr, func() { _ = pr.Close() }, nil
}

func buildObjectKey(b models.Backup) string {
	now := time.Now().UTC()
	ext := ".sql"
	if strings.EqualFold(b.Connection.Type, "convex") {
		ext = ".zip"
	}
	base := fmt.Sprintf("%s/%04d/%02d/%02d/%s_%s%s", sanitizePath(b.Connection.Name), now.Year(), now.Month(), now.Day(), b.ID, now.Format("20060102_150405"), ext)
	if ext == ".sql" && b.Compression == "gzip" {
		base += ".gz"
	}
	if b.TargetType == "s3" && b.Remote != nil && b.Remote.PathPrefix != "" {
		return strings.TrimSuffix(b.Remote.PathPrefix, "/") + "/" + base
	}
	return base
}

func sanitizePath(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	name = strings.ReplaceAll(name, " ", "-")
	name = strings.ReplaceAll(name, "/", "-")
	if name == "" {
		return "connection"
	}
	return name
}

func normalizeConvexURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("empty url")
	}
	if !strings.Contains(trimmed, "://") {
		trimmed = "http://" + trimmed
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("missing scheme or host")
	}
	return parsed.String(), nil
}

func (e *Executor) saveLocal(basePath, key string, src io.Reader) error {
	root := basePath
	if root == "" {
		root = e.cfg.DefaultLocalBasePath
	}
	full := filepath.Join(root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}

	f, err := os.Create(full)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	_, err = io.Copy(f, src)
	return err
}

func (e *Executor) cleanupLocal(basePath, connectionPrefix string, cutoff time.Time) error {
	root := basePath
	if root == "" {
		root = e.cfg.DefaultLocalBasePath
	}
	connectionRoot := filepath.Join(root, filepath.FromSlash(connectionPrefix))
	if _, err := os.Stat(connectionRoot); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return filepath.WalkDir(connectionRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}
		if info.ModTime().UTC().Before(cutoff) {
			return os.Remove(path)
		}
		return nil
	})
}

func (e *Executor) saveS3(ctx context.Context, rem models.Remote, key string, src io.Reader) error {
	client, err := s3ClientFromRemote(ctx, rem)
	if err != nil {
		return err
	}

	uploader := manager.NewUploader(client)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &rem.Bucket,
		Key:    &key,
		Body:   src,
	})
	return err
}

func (e *Executor) cleanupS3(ctx context.Context, rem models.Remote, connectionPrefix string, cutoff time.Time) error {
	client, err := s3ClientFromRemote(ctx, rem)
	if err != nil {
		return err
	}

	prefix := connectionPrefix
	if rem.PathPrefix != "" {
		prefix = strings.TrimSuffix(rem.PathPrefix, "/") + "/" + connectionPrefix
	}

	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &rem.Bucket,
		Prefix: &prefix,
	})

	for pager.HasMorePages() {
		page, pageErr := pager.NextPage(ctx)
		if pageErr != nil {
			return pageErr
		}
		for _, item := range page.Contents {
			if item.LastModified != nil && item.LastModified.UTC().Before(cutoff) {
				_, delErr := client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: &rem.Bucket,
					Key:    item.Key,
				})
				if delErr != nil {
					return delErr
				}
			}
		}
	}

	return nil
}

func s3ClientFromRemote(ctx context.Context, rem models.Remote) (*s3.Client, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(rem.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(rem.AccessKey, rem.SecretKey, "")),
	}

	usePathStyle := false
	if rem.Endpoint != "" {
		usePathStyle = true
		resolver := s3.EndpointResolverFromURL(rem.Endpoint)
		loadOpts = append(loadOpts, awsconfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				if service == s3.ServiceID {
					return resolver.ResolveEndpoint(region, s3.EndpointResolverOptions{})
				}
				return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
			},
		)))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
	}), nil
}
