package s3

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.opentelemetry.io/collector/config/configmapprovider"
)

type provider struct {
	downloader *s3manager.Downloader
	bucket     string
	key        string
	versionID  string
	mu         sync.Mutex
	closed     bool
}

var vhostBucketRE = regexp.MustCompile(`^(.+)\.s3\.([^\.]+)\.amazonaws.com`)

const pathSep = "/"

func NewFromURL(rawURL string) (configmapprovider.Provider, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse S3 URL: %w", err)
	}

	var (
		region    string
		bucket    string
		key       string
		versionID string
	)

	if parsedURL.Scheme == "s3" {
		bucket = parsedURL.Host
		key = strings.TrimPrefix(parsedURL.Path, pathSep)
	} else if matches := vhostBucketRE.FindStringSubmatch(parsedURL.Host); len(matches) > 0 {
		bucket = matches[1]
		region = matches[2]
		key = strings.TrimPrefix(parsedURL.Path, pathSep)
	} else {
		// path-style URL
		splitHost := strings.Split(parsedURL.Host, ".")
		if len(splitHost) != 4 {
			return nil, fmt.Errorf("invalid path-style URL: expected 4 hostname elements, found %d)", len(splitHost))
		}
		region = splitHost[1]
		bucket, key, err = bucketAndKeyFromPath(parsedURL)
		if err != nil {
			return nil, err
		}
	}

	if parsedURL.Query().Has("version") {
		versionID = parsedURL.Query().Get("version")
	}

	cfg := &aws.Config{}
	if region != "" {
		cfg.Region = aws.String(region)
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create AWS session: %w", err)
	}

	return &provider{
		downloader: s3manager.NewDownloader(sess),
		bucket:     bucket,
		key:        key,
		versionID:  versionID,
	}, nil
}

func bucketAndKeyFromPath(parsedURL *url.URL) (string, string, error) {
	splitPath := strings.SplitN(strings.TrimPrefix(parsedURL.Path, pathSep), pathSep, 2)
	if len(splitPath) != 2 {
		return "", "", fmt.Errorf("invalid path-style URL: expected bucket and key name to be separated by '/'")
	}
	return splitPath[0], splitPath[1], nil
}

func (p *provider) Retrieve(ctx context.Context, onChange func(*configmapprovider.ChangeEvent)) (configmapprovider.Retrieved, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("Retrieve() called on already closed Provider")
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.key),
	}

	if p.versionID != "" {
		input.VersionId = aws.String(p.versionID)
	}

	buf := &aws.WriteAtBuffer{}
	_, err := p.downloader.DownloadWithContext(ctx, buf, input) // number of bytes downloaded is not useful here
	if err != nil {
		return nil, fmt.Errorf("unable to download s3 object: %w", err)
	}
	return configmapprovider.NewInMemory(bytes.NewReader(buf.Bytes())).Retrieve(ctx, onChange)
}

func (p *provider) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}
