package azure

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/sunny36/portage/internal/connector"
)

// Block IDs within one blob must all have the same length. The raw ID is
// "<16 hex session>-<7 digit part>" = 24 bytes, which base64-encodes to 32
// characters without padding.
const (
	sessionHexLen = 16
	partDigits    = 7
	rawBlockIDLen = sessionHexLen + 1 + partDigits
)

func newSessionID() (string, error) {
	b := make([]byte, sessionHexLen/2)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func validSessionID(s string) bool {
	if len(s) != sessionHexLen {
		return false
	}
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// blockID returns the base64 block ID for part number n of a session.
func blockID(session string, n int) string {
	raw := fmt.Sprintf("%s-%0*d", session, partDigits, n)
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// parseBlockID decodes a block ID produced by blockID. ok is false for IDs
// from other writers or sessions.
func parseBlockID(id string) (session string, n int, ok bool) {
	raw, err := base64.StdEncoding.DecodeString(id)
	if err != nil || len(raw) != rawBlockIDLen || raw[sessionHexLen] != '-' {
		return "", 0, false
	}
	session = string(raw[:sessionHexLen])
	if !validSessionID(session) {
		return "", 0, false
	}
	n, err = strconv.Atoi(string(raw[sessionHexLen+1:]))
	if err != nil || n < 1 {
		return "", 0, false
	}
	return session, n, true
}

type upload struct {
	c       *Connector
	key     string
	session string
	opts    connector.WriteOptions
}

// BeginUpload implements connector.Connector. Nothing is sent to Azure until
// the first part: a block blob session exists only through its staged blocks.
func (c *Connector) BeginUpload(ctx context.Context, key string, opts connector.WriteOptions) (connector.Upload, error) {
	s, err := newSessionID()
	if err != nil {
		return nil, fmt.Errorf("azure: BeginUpload %q: %w", key, err)
	}
	return &upload{c: c, key: key, session: s, opts: opts}, nil
}

// ResumeUpload implements connector.Connector. It returns ErrNotFound when
// the ID is malformed or the session was already committed (its blocks are
// in the committed list). Uncommitted blocks that Azure has garbage
// collected (after 7 days) simply show up as no parts in ListParts.
func (c *Connector) ResumeUpload(ctx context.Context, key, uploadID string, opts connector.WriteOptions) (connector.Upload, error) {
	if !validSessionID(uploadID) {
		return nil, &connector.ProviderError{Op: "ResumeUpload", Key: key, Sentinel: connector.ErrNotFound,
			Err: fmt.Errorf("malformed azure upload id %q", uploadID)}
	}
	resp, err := c.blockBlob(key).GetBlockList(ctx, blockblob.BlockListTypeCommitted, nil)
	if err != nil {
		werr := wrap("ResumeUpload", key, err)
		if !errors.Is(werr, connector.ErrNotFound) {
			return nil, werr
		}
	} else {
		for _, b := range resp.CommittedBlocks {
			if b == nil || b.Name == nil {
				continue
			}
			if s, _, ok := parseBlockID(*b.Name); ok && s == uploadID {
				return nil, &connector.ProviderError{Op: "ResumeUpload", Key: key, Sentinel: connector.ErrNotFound,
					Err: errors.New("upload session already completed")}
			}
		}
	}
	return &upload{c: c, key: key, session: uploadID, opts: opts}, nil
}

func (u *upload) ID() string { return u.session }

func (u *upload) UploadPart(ctx context.Context, number int, data []byte) (connector.Part, error) {
	if number < 1 || number > maxParts {
		return connector.Part{}, fmt.Errorf("azure: UploadPart %q: part number %d out of range [1,%d]", u.key, number, maxParts)
	}
	if int64(len(data)) > maxPartSize {
		return connector.Part{}, fmt.Errorf("azure: UploadPart %q: part %d is %d bytes, max %d", u.key, number, len(data), int64(maxPartSize))
	}
	id := blockID(u.session, number)
	sum := md5.Sum(data)
	_, err := u.c.blockBlob(u.key).StageBlock(ctx, id, streaming.NopCloser(bytes.NewReader(data)), &blockblob.StageBlockOptions{
		TransactionalValidation: blob.TransferValidationTypeMD5(sum[:]),
	})
	if err != nil {
		return connector.Part{}, wrap("UploadPart", u.key, err)
	}
	return connector.Part{Number: number, Size: int64(len(data)), Token: id}, nil
}

func (u *upload) ListParts(ctx context.Context) ([]connector.Part, error) {
	resp, err := u.c.blockBlob(u.key).GetBlockList(ctx, blockblob.BlockListTypeUncommitted, nil)
	if err != nil {
		werr := wrap("ListParts", u.key, err)
		if errors.Is(werr, connector.ErrNotFound) {
			return nil, nil // no block staged yet
		}
		return nil, werr
	}
	var parts []connector.Part
	for _, b := range resp.UncommittedBlocks {
		if b == nil || b.Name == nil {
			continue
		}
		s, n, ok := parseBlockID(*b.Name)
		if !ok || s != u.session {
			continue
		}
		parts = append(parts, connector.Part{Number: n, Size: deref(b.Size), Token: *b.Name})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	return parts, nil
}

func (u *upload) Complete(ctx context.Context, parts []connector.Part) (connector.WriteResult, error) {
	ids, err := u.commitList(parts)
	if err != nil {
		return connector.WriteResult{}, fmt.Errorf("azure: Complete %q: %w", u.key, err)
	}
	resp, err := u.c.blockBlob(u.key).CommitBlockList(ctx, ids, &blockblob.CommitBlockListOptions{
		Metadata:         toAzMeta(u.opts.Metadata),
		HTTPHeaders:      httpHeaders(u.opts),
		AccessConditions: accessConditions(u.opts),
	})
	if err != nil {
		return connector.WriteResult{}, wrap("Complete", u.key, err)
	}
	return connector.WriteResult{
		Version:   etag(resp.ETag),
		Checksums: checksums(nil, u.opts.Metadata),
	}, nil
}

// commitList orders parts by number and returns their block IDs, rejecting
// duplicates and tokens that do not belong to this session.
func (u *upload) commitList(parts []connector.Part) ([]string, error) {
	sorted := append([]connector.Part(nil), parts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Number < sorted[j].Number })
	ids := make([]string, 0, len(sorted))
	for i, p := range sorted {
		if p.Number < 1 || p.Number > maxParts {
			return nil, fmt.Errorf("part number %d out of range", p.Number)
		}
		if i > 0 && sorted[i-1].Number == p.Number {
			return nil, fmt.Errorf("duplicate part number %d", p.Number)
		}
		id := blockID(u.session, p.Number)
		if p.Token != "" && p.Token != id {
			return nil, fmt.Errorf("part %d token %q does not belong to session %s", p.Number, p.Token, u.session)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// Abort is a best-effort no-op. Azure has no API to delete uncommitted
// blocks; they never become visible and are garbage collected after 7 days,
// or discarded by the next Put Blob / Put Block List on the same blob.
func (u *upload) Abort(ctx context.Context) error { return nil }
