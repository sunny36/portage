// Package memconn is an in-memory connector.Connector for transfer tests,
// with hooks to inject failures.
package memconn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/sunny36/portage/internal/connector"
)

type object struct {
	data    []byte
	version string
	meta    map[string]string
	ctype   string
}

type session struct {
	key   string
	opts  connector.WriteOptions
	parts map[int][]byte
}

// Conn is a thread-safe in-memory connector. Hook fields must be set before
// use; hooks are called without the lock held.
type Conn struct {
	Lim connector.Limits

	// ReadHook runs before every OpenRange; a non-nil error is returned.
	ReadHook func(key string, offset, length int64) error
	// UploadPartHook runs before every UploadPart; a non-nil error is
	// returned instead of storing the part.
	UploadPartHook func(number int) error
	// PutHook runs before every PutObject.
	PutHook func() error
	// CorruptAt, if >= 0, flips the byte at that offset of every object
	// written (simulates silent corruption at rest).
	CorruptAt int64

	mu       sync.Mutex
	objects  map[string]*object
	uploads  map[string]*session
	nextID   int
	nextVer  int
	aborted  map[string]bool
	inflight atomic.Int64
	peak     atomic.Int64

	// Counters.
	UploadPartCalls atomic.Int64
	PartsStored     atomic.Int64
	Puts            atomic.Int64
}

// New returns an empty connector with the given limits.
func New(lim connector.Limits) *Conn {
	return &Conn{
		Lim:       lim,
		CorruptAt: -1,
		objects:   map[string]*object{},
		uploads:   map[string]*session{},
		aborted:   map[string]bool{},
	}
}

func notFound(op, key string) error {
	return &connector.ProviderError{Op: op, Key: key, Status: 404, Sentinel: connector.ErrNotFound, Err: fmt.Errorf("no such key")}
}

// Set stores data at key with a new version and returns that version.
func (c *Conn) Set(key string, data []byte, meta map[string]string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setLocked(key, data, meta, "")
}

func (c *Conn) setLocked(key string, data []byte, meta map[string]string, ctype string) string {
	c.nextVer++
	v := "v" + strconv.Itoa(c.nextVer)
	d := bytes.Clone(data)
	if c.CorruptAt >= 0 && c.CorruptAt < int64(len(d)) {
		d[c.CorruptAt] ^= 0xff
	}
	c.objects[key] = &object{data: d, version: v, meta: maps.Clone(meta), ctype: ctype}
	return v
}

// Get returns the stored bytes and metadata of key.
func (c *Conn) Get(key string) ([]byte, map[string]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o, ok := c.objects[key]
	if !ok {
		return nil, nil, false
	}
	return bytes.Clone(o.data), maps.Clone(o.meta), true
}

// Aborted reports whether upload id was aborted.
func (c *Conn) Aborted(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.aborted[id]
}

// OpenUploads returns the number of sessions not completed or aborted.
func (c *Conn) OpenUploads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.uploads)
}

// PeakConcurrentParts is the highest number of UploadPart calls in flight.
func (c *Conn) PeakConcurrentParts() int64 { return c.peak.Load() }

func (c *Conn) Name() string             { return "mem" }
func (c *Conn) Limits() connector.Limits { return c.Lim }

func (c *Conn) List(ctx context.Context, prefix, cursor string, limit int) (connector.ListPage, error) {
	return connector.ListPage{}, fmt.Errorf("memconn: List not implemented")
}

func (c *Conn) Stat(ctx context.Context, key string) (connector.ObjectInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o, ok := c.objects[key]
	if !ok {
		return connector.ObjectInfo{}, notFound("Stat", key)
	}
	info := connector.ObjectInfo{Key: key, Size: int64(len(o.data)), Version: o.version, Metadata: maps.Clone(o.meta)}
	if b, err := hex.DecodeString(o.meta[connector.MetaSHA256]); err == nil && len(b) == sha256.Size {
		info.Checksums.SHA256 = b
	}
	return info, nil
}

func (c *Conn) OpenRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, error) {
	if c.ReadHook != nil {
		if err := c.ReadHook(key, offset, length); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	o, ok := c.objects[key]
	if !ok {
		return nil, notFound("OpenRange", key)
	}
	if version != "" && version != o.version {
		return nil, &connector.ProviderError{Op: "OpenRange", Key: key, Status: 412, Sentinel: connector.ErrVersionChanged, Err: fmt.Errorf("version is %s", o.version)}
	}
	size := int64(len(o.data))
	if offset > size {
		offset = size
	}
	end := size
	if length >= 0 && offset+length < size {
		end = offset + length
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(o.data[offset:end]))), nil
}

func (c *Conn) PutObject(ctx context.Context, key string, data io.Reader, size int64, opts connector.WriteOptions) (connector.WriteResult, error) {
	if c.PutHook != nil {
		if err := c.PutHook(); err != nil {
			return connector.WriteResult{}, err
		}
	}
	c.Puts.Add(1)
	if data == nil {
		data = bytes.NewReader(nil)
	}
	b, err := io.ReadAll(data)
	if err != nil {
		return connector.WriteResult{}, err
	}
	if int64(len(b)) != size {
		return connector.WriteResult{}, fmt.Errorf("memconn: PutObject got %d bytes, want %d", len(b), size)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return connector.WriteResult{Version: c.setLocked(key, b, opts.Metadata, opts.ContentType)}, nil
}

func (c *Conn) BeginUpload(ctx context.Context, key string, opts connector.WriteOptions) (connector.Upload, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	id := "up" + strconv.Itoa(c.nextID)
	c.uploads[id] = &session{key: key, opts: opts, parts: map[int][]byte{}}
	return &upload{c: c, id: id, key: key}, nil
}

func (c *Conn) ResumeUpload(ctx context.Context, key, uploadID string, opts connector.WriteOptions) (connector.Upload, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.uploads[uploadID]
	if !ok || s.key != key {
		return nil, notFound("ResumeUpload", key)
	}
	return &upload{c: c, id: uploadID, key: key}, nil
}

func (c *Conn) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.objects, key)
	return nil
}

type upload struct {
	c   *Conn
	id  string
	key string
}

func (u *upload) ID() string { return u.id }

func (u *upload) UploadPart(ctx context.Context, number int, data []byte) (connector.Part, error) {
	c := u.c
	c.UploadPartCalls.Add(1)
	cur := c.inflight.Add(1)
	defer c.inflight.Add(-1)
	for {
		p := c.peak.Load()
		if cur <= p || c.peak.CompareAndSwap(p, cur) {
			break
		}
	}
	if c.UploadPartHook != nil {
		if err := c.UploadPartHook(number); err != nil {
			return connector.Part{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return connector.Part{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.uploads[u.id]
	if !ok {
		return connector.Part{}, notFound("UploadPart", u.key)
	}
	s.parts[number] = bytes.Clone(data)
	c.PartsStored.Add(1)
	return connector.Part{Number: number, Size: int64(len(data)), Token: "t" + strconv.Itoa(number)}, nil
}

func (u *upload) ListParts(ctx context.Context) ([]connector.Part, error) {
	u.c.mu.Lock()
	defer u.c.mu.Unlock()
	s, ok := u.c.uploads[u.id]
	if !ok {
		return nil, notFound("ListParts", u.key)
	}
	var parts []connector.Part
	for n, d := range s.parts {
		parts = append(parts, connector.Part{Number: n, Size: int64(len(d)), Token: "t" + strconv.Itoa(n)})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	return parts, nil
}

func (u *upload) Complete(ctx context.Context, parts []connector.Part) (connector.WriteResult, error) {
	c := u.c
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.uploads[u.id]
	if !ok {
		return connector.WriteResult{}, notFound("Complete", u.key)
	}
	var buf bytes.Buffer
	for i, p := range parts {
		if p.Number != i+1 {
			return connector.WriteResult{}, fmt.Errorf("memconn: Complete: part %d at position %d", p.Number, i)
		}
		d, ok := s.parts[p.Number]
		if !ok {
			return connector.WriteResult{}, fmt.Errorf("memconn: Complete: part %d not uploaded", p.Number)
		}
		if p.Token != "t"+strconv.Itoa(p.Number) {
			return connector.WriteResult{}, fmt.Errorf("memconn: Complete: part %d bad token %q", p.Number, p.Token)
		}
		buf.Write(d)
	}
	delete(c.uploads, u.id)
	return connector.WriteResult{Version: c.setLocked(s.key, buf.Bytes(), s.opts.Metadata, s.opts.ContentType)}, nil
}

func (u *upload) Abort(ctx context.Context) error {
	u.c.mu.Lock()
	defer u.c.mu.Unlock()
	delete(u.c.uploads, u.id)
	u.c.aborted[u.id] = true
	return nil
}
