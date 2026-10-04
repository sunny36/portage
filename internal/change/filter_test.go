package change

import (
	"testing"

	"github.com/sunny36/portage/internal/config"
)

func TestFilterMatch(t *testing.T) {
	tests := []struct {
		name    string
		f       config.Filters
		match   []string
		noMatch []string
	}{
		{
			name:  "empty includes all",
			match: []string{"a", "a/b/c.txt", ""},
		},
		{
			name:    "include star does not cross slash",
			f:       config.Filters{Include: []string{"*.csv"}},
			match:   []string{"a.csv"},
			noMatch: []string{"dir/a.csv", "a.txt"},
		},
		{
			name:    "leading ** any depth",
			f:       config.Filters{Include: []string{"**/*.csv"}},
			match:   []string{"a.csv", "x/a.csv", "x/y/z/a.csv"},
			noMatch: []string{"a.csv.bak", "x/a.txt"},
		},
		{
			name:    "leading ** with directory in rest",
			f:       config.Filters{Include: []string{"**/tmp/*"}},
			match:   []string{"tmp/a", "x/tmp/a", "x/y/tmp/a"},
			noMatch: []string{"tmp/a/b", "xtmp/a"},
		},
		{
			name:    "trailing ** subtree",
			f:       config.Filters{Include: []string{"logs/**"}},
			match:   []string{"logs/a", "logs/2026/10/a.log"},
			noMatch: []string{"logs", "logsX/a", "other/logs/a"},
		},
		{
			name:    "both",
			f:       config.Filters{Exclude: []string{"**/.cache/**"}},
			match:   []string{"a", "x/cache/a", ".cache"},
			noMatch: []string{".cache/a", "x/.cache/a", "x/y/.cache/z/a"},
		},
		{
			name: "exclude wins",
			f: config.Filters{
				Include: []string{"data/*"},
				Exclude: []string{"data/*.tmp"},
			},
			match:   []string{"data/a.csv"},
			noMatch: []string{"data/a.tmp", "other/a.csv"},
		},
		{
			name:    "character class and question mark",
			f:       config.Filters{Include: []string{"r[0-9]?.bin"}},
			match:   []string{"r1a.bin"},
			noMatch: []string{"rxa.bin", "r1.bin"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewFilter(tt.f)
			if err != nil {
				t.Fatal(err)
			}
			for _, k := range tt.match {
				if !f.Match(k) {
					t.Errorf("Match(%q) = false, want true", k)
				}
			}
			for _, k := range tt.noMatch {
				if f.Match(k) {
					t.Errorf("Match(%q) = true, want false", k)
				}
			}
		})
	}
}

func TestFilterInvalid(t *testing.T) {
	for _, p := range []string{"[", "a/**/b", "**", "**/", "x**"} {
		if _, err := NewFilter(config.Filters{Include: []string{p}}); err == nil {
			t.Errorf("NewFilter(%q): want error", p)
		}
		if _, err := NewFilter(config.Filters{Exclude: []string{p}}); err == nil {
			t.Errorf("NewFilter(exclude %q): want error", p)
		}
	}
}

func TestNilFilterMatchesAll(t *testing.T) {
	var f *Filter
	if !f.Match("anything") {
		t.Fatal("nil filter must match")
	}
}
