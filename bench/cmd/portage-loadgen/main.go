// Command portage-loadgen writes synthetic blobs into an Azure Blob container
// at a target daily volume, for soak-testing a Portage pipeline. Every blob it
// writes is recorded in a JSONL manifest (key, size, sha256, written_at) so a
// verifier can later check the destination. See bench/README.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "portage-loadgen:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("portage-loadgen", flag.ContinueOnError)
	var (
		connStr     = fs.String("connection-string", os.Getenv("AZURE_STORAGE_CONNECTION_STRING"), "Azure Storage connection string (default $AZURE_STORAGE_CONNECTION_STRING)")
		accountURL  = fs.String("account-url", "", "blob service URL, e.g. https://<account>.blob.core.windows.net; uses DefaultAzureCredential")
		containerNm = fs.String("container", "", "container to write into (required)")
		create      = fs.Bool("create-container", false, "create the container if it does not exist")
		prefix      = fs.String("prefix", "loadgen/", "key prefix")
		rateStr     = fs.String("rate", "500GB/day", "target volume, e.g. 500GB/day, 20MB/s; 0 = unlimited")
		burstStr    = fs.String("burst", "", "write this much as fast as possible, then stop (e.g. 2TB); overrides -rate")
		duration    = fs.Duration("duration", 0, "stop after this long (0 = until -count/-burst or Ctrl-C)")
		count       = fs.Int64("count", 0, "stop after this many blobs (0 = no limit)")
		mixStr      = fs.String("mix", defaultMix, "size mix MIN-MAX:WEIGHT,... (sizes drawn log-uniformly within each range)")
		concurrency = fs.Int("concurrency", 16, "blobs uploaded in parallel")
		blockStr    = fs.String("block-size", "8MiB", "block size for staged uploads (memory ~ concurrency x block-concurrency x block-size)")
		blockConc   = fs.Int("block-concurrency", 4, "concurrent block uploads per blob")
		seed        = fs.Uint64("seed", uint64(time.Now().UnixNano()), "seed for sizes and content (reuse to reproduce a run)")
		manifest    = fs.String("manifest", "loadgen-manifest.jsonl", "JSONL manifest to append to ('-' = stdout)")
		progress    = fs.Duration("progress", 30*time.Second, "progress log interval (0 = off)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *containerNm == "" {
		return errors.New("-container is required")
	}

	mix, err := parseMix(*mixStr)
	if err != nil {
		return err
	}
	rate, err := parseRate(*rateStr)
	if err != nil {
		return err
	}
	var total int64
	if *burstStr != "" {
		if total, err = parseSize(*burstStr); err != nil {
			return fmt.Errorf("-burst: %w", err)
		}
		rate = 0
	}
	blockSize, err := parseSize(*blockStr)
	if err != nil {
		return fmt.Errorf("-block-size: %w", err)
	}

	c, err := newContainerClient(*connStr, *accountURL, *containerNm)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *create {
		if _, err := c.Create(ctx, nil); err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
			return fmt.Errorf("create container: %w", err)
		}
	}

	mf, err := openManifest(*manifest)
	if err != nil {
		return err
	}
	defer func() { _ = mf.Close() }()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	log.Info("starting",
		"container", *containerNm, "prefix", *prefix, "seed", *seed,
		"rate_per_day", rateLabel(rate), "burst", *burstStr,
		"mean_blob", formatBytes(mix.Mean()), "concurrency", *concurrency)

	stats, err := generate(ctx, c, genConfig{
		Prefix:      *prefix,
		Rate:        rate,
		TotalBytes:  total,
		MaxBlobs:    *count,
		Duration:    *duration,
		Concurrency: *concurrency,
		BlockSize:   blockSize,
		BlockConc:   *blockConc,
		Seed:        *seed,
		Mix:         mix,
		Manifest:    mf,
		Progress:    *progress,
		Logger:      log,
	})
	log.Info("done", "blobs", stats.Blobs, "bytes", formatBytes(float64(stats.Bytes)), "errors", stats.Errors)
	if errors.Is(err, context.Canceled) && stats.Errors == 0 {
		return nil // interrupted by the user; manifest is complete for what was written
	}
	return err
}

func rateLabel(bytesPerSec float64) string {
	if bytesPerSec == 0 {
		return "unlimited"
	}
	return formatBytes(bytesPerSec * 86400)
}

func newContainerClient(connStr, accountURL, name string) (*container.Client, error) {
	switch {
	case connStr != "":
		return container.NewClientFromConnectionString(connStr, name, nil)
	case accountURL != "":
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("default azure credential: %w", err)
		}
		u := accountURL
		if u[len(u)-1] != '/' {
			u += "/"
		}
		return container.NewClient(u+name, cred, nil)
	default:
		return nil, errors.New("set -connection-string (or $AZURE_STORAGE_CONNECTION_STRING) or -account-url")
	}
}
