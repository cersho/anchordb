package scheduler

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
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
	base := fmt.Sprintf("%s/%04d/%02d/%02d/%s_%s.sql", sanitizePath(b.Connection.Name), now.Year(), now.Month(), now.Day(), b.ID, now.Format("20060102_150405"))
	if b.Compression == "gzip" {
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
