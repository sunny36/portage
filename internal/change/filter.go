package change

import (
	"fmt"
	"path"
	"strings"

	"github.com/sunny36/portage/internal/config"
)

// Filter decides which keys a pipeline syncs. Keys are relative to the
// pipeline's source prefix (the same Key carried by ObjectChanged).
//
// Pattern syntax is path.Match, matched against the WHOLE key, where `*`
// and `?` never cross a `/`. Two extensions handle directory depth:
//
//   - A leading "**/" means "at any directory depth, including the top":
//     "**/*.tmp" matches "a.tmp", "x/a.tmp" and "x/y/a.tmp". Formally the
//     rest of the pattern must path.Match the key, or some suffix of the key
//     that starts right after a "/".
//   - A trailing "/**" means "everything below this directory, at any
//     depth": "logs/**" matches "logs/a" and "logs/2026/a" but not "logs"
//     itself or "logsX/a". Formally the rest of the pattern must path.Match
//     a proper directory prefix of the key (the part before some "/").
//
// Both may be combined ("**/cache/**"). "**" anywhere else is rejected,
// because path.Match would silently treat it as a single `*`.
//
// An empty include list includes every key. A key matching any exclude
// pattern is skipped even if it also matches an include pattern.
type Filter struct {
	include []pattern
	exclude []pattern
}

type pattern struct {
	anyDepth bool   // leading "**/"
	subtree  bool   // trailing "/**"
	glob     string // the remaining path.Match pattern
}

// NewFilter validates and compiles f.
func NewFilter(f config.Filters) (*Filter, error) {
	out := &Filter{}
	for _, list := range []struct {
		name string
		in   []string
		dst  *[]pattern
	}{
		{"include", f.Include, &out.include},
		{"exclude", f.Exclude, &out.exclude},
	} {
		for i, raw := range list.in {
			p, err := compile(raw)
			if err != nil {
				return nil, fmt.Errorf("filters.%s[%d] %q: %w", list.name, i, raw, err)
			}
			*list.dst = append(*list.dst, p)
		}
	}
	return out, nil
}

func compile(raw string) (pattern, error) {
	var p pattern
	s := raw
	if strings.HasPrefix(s, "**/") {
		p.anyDepth = true
		s = s[len("**/"):]
	}
	if strings.HasSuffix(s, "/**") {
		p.subtree = true
		s = s[:len(s)-len("/**")]
	}
	if s == "" {
		return pattern{}, fmt.Errorf("empty pattern")
	}
	if strings.Contains(s, "**") {
		return pattern{}, fmt.Errorf(`"**" is only supported as a leading "**/" or trailing "/**"`)
	}
	if _, err := path.Match(s, ""); err != nil {
		return pattern{}, err
	}
	p.glob = s
	return p, nil
}

// match reports whether key matches p. The glob was validated by compile, so
// path.Match cannot fail here.
func (p pattern) match(key string) bool {
	if !p.subtree {
		return p.matchDepth(key)
	}
	// Try every directory prefix of key: "a/b/c" → "a", "a/b".
	for i := 0; i < len(key); i++ {
		if key[i] == '/' && p.matchDepth(key[:i]) {
			return true
		}
	}
	return false
}

func (p pattern) matchDepth(s string) bool {
	if ok, _ := path.Match(p.glob, s); ok {
		return true
	}
	if !p.anyDepth {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			if ok, _ := path.Match(p.glob, s[i+1:]); ok {
				return true
			}
		}
	}
	return false
}

// Match reports whether key (relative to the source prefix) is synced. A nil
// Filter matches everything.
func (f *Filter) Match(key string) bool {
	if f == nil {
		return true
	}
	for _, p := range f.exclude {
		if p.match(key) {
			return false
		}
	}
	if len(f.include) == 0 {
		return true
	}
	for _, p := range f.include {
		if p.match(key) {
			return true
		}
	}
	return false
}
