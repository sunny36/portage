package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/sunny36/portage/internal/connector"
)

// upload is an S3 multipart upload. Metadata and content type are fixed at
// CreateMultipartUpload; S3 cannot change them at completion.
type upload struct {
	c    *Connector
	key  string // relative key, for errors
	id   string
	opts connector.WriteOptions
}

var _ connector.Upload = (*upload)(nil)

// BeginUpload implements connector.Connector.
func (c *Connector) BeginUpload(ctx context.Context, key string, opts connector.WriteOptions) (connector.Upload, error) {
	in := &s3sdk.CreateMultipartUploadInput{
		Bucket:   aws.String(c.bucket),
		Key:      aws.String(c.full(key)),
		Metadata: opts.Metadata,
	}
	if opts.ContentType != "" {
		in.ContentType = aws.String(opts.ContentType)
	}
	out, err := c.client.CreateMultipartUpload(ctx, in)
	if err != nil {
		return nil, mapError("BeginUpload", key, err)
	}
	id := aws.ToString(out.UploadId)
	if id == "" {
		return nil, &connector.ProviderError{Op: "BeginUpload", Key: key, Err: errors.New("provider returned an empty upload ID")}
	}
	return &upload{c: c, key: key, id: id, opts: opts}, nil
}

// ResumeUpload implements connector.Connector. It checks that the session
// still exists (ListParts), returning ErrNotFound if it expired or was
// completed/aborted.
func (c *Connector) ResumeUpload(ctx context.Context, key, uploadID string, opts connector.WriteOptions) (connector.Upload, error) {
	if uploadID == "" {
		return nil, &connector.ProviderError{Op: "ResumeUpload", Key: key, Sentinel: connector.ErrNotFound, Err: errors.New("empty upload ID")}
	}
	_, err := c.client.ListParts(ctx, &s3sdk.ListPartsInput{
		Bucket:   aws.String(c.bucket),
		Key:      aws.String(c.full(key)),
		UploadId: aws.String(uploadID),
		MaxParts: aws.Int32(1),
	})
	if err != nil {
		return nil, mapError("ResumeUpload", key, err)
	}
	return &upload{c: c, key: key, id: uploadID, opts: opts}, nil
}

func (u *upload) ID() string { return u.id }

func (u *upload) UploadPart(ctx context.Context, number int, data []byte) (connector.Part, error) {
	if number < 1 || number > limits.MaxParts {
		return connector.Part{}, fmt.Errorf("s3: UploadPart %q: part number %d out of range 1..%d", u.key, number, limits.MaxParts)
	}
	out, err := u.c.client.UploadPart(ctx, &s3sdk.UploadPartInput{
		Bucket:        aws.String(u.c.bucket),
		Key:           aws.String(u.c.full(u.key)),
		UploadId:      aws.String(u.id),
		PartNumber:    aws.Int32(int32(number)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return connector.Part{}, mapError("UploadPart", u.key, err)
	}
	return connector.Part{Number: number, Size: int64(len(data)), Token: normalizeETag(aws.ToString(out.ETag))}, nil
}

func (u *upload) ListParts(ctx context.Context) ([]connector.Part, error) {
	var parts []connector.Part
	var marker *string
	for {
		out, err := u.c.client.ListParts(ctx, &s3sdk.ListPartsInput{
			Bucket:           aws.String(u.c.bucket),
			Key:              aws.String(u.c.full(u.key)),
			UploadId:         aws.String(u.id),
			PartNumberMarker: marker,
		})
		if err != nil {
			return nil, mapError("ListParts", u.key, err)
		}
		for _, p := range out.Parts {
			parts = append(parts, connector.Part{
				Number: int(aws.ToInt32(p.PartNumber)),
				Size:   aws.ToInt64(p.Size),
				Token:  normalizeETag(aws.ToString(p.ETag)),
			})
		}
		next := aws.ToString(out.NextPartNumberMarker)
		if !aws.ToBool(out.IsTruncated) || next == "" || next == aws.ToString(marker) {
			break
		}
		marker = aws.String(next)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	return parts, nil
}

func (u *upload) Complete(ctx context.Context, parts []connector.Part) (connector.WriteResult, error) {
	if len(parts) == 0 {
		return connector.WriteResult{}, fmt.Errorf("s3: Complete %q: no parts", u.key)
	}
	sorted := append([]connector.Part(nil), parts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Number < sorted[j].Number })
	completed := make([]types.CompletedPart, len(sorted))
	for i, p := range sorted {
		if i > 0 && p.Number == sorted[i-1].Number {
			return connector.WriteResult{}, fmt.Errorf("s3: Complete %q: duplicate part %d", u.key, p.Number)
		}
		completed[i] = types.CompletedPart{
			PartNumber: aws.Int32(int32(p.Number)),
			ETag:       aws.String(quoteETag(p.Token)),
		}
	}
	in := &s3sdk.CompleteMultipartUploadInput{
		Bucket:          aws.String(u.c.bucket),
		Key:             aws.String(u.c.full(u.key)),
		UploadId:        aws.String(u.id),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	}
	in.IfMatch, in.IfNoneMatch = conditions(u.opts)
	out, err := u.c.client.CompleteMultipartUpload(ctx, in)
	if err != nil {
		return connector.WriteResult{}, mapError("Complete", u.key, err)
	}
	// Multipart ETags are never content MD5s; leave Checksums empty.
	return connector.WriteResult{
		Version: normalizeETag(aws.ToString(out.ETag)),
	}, nil
}

// Abort discards the session. Aborting one that no longer exists succeeds.
func (u *upload) Abort(ctx context.Context) error {
	_, err := u.c.client.AbortMultipartUpload(ctx, &s3sdk.AbortMultipartUploadInput{
		Bucket:   aws.String(u.c.bucket),
		Key:      aws.String(u.c.full(u.key)),
		UploadId: aws.String(u.id),
	})
	if err != nil {
		err = mapError("Abort", u.key, err)
		if errors.Is(err, connector.ErrNotFound) {
			return nil
		}
		return err
	}
	return nil
}
