package textproc

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Entry is one pronunciation fix. Term is matched literally in the text.
// Pinyin (tone numbers, e.g. "xiang1 yang2") records the intended reading and
// is the model-independent source of truth; Say is the spoken substitute
// written into the text for models that take plain text only. Models limits
// the entry to the listed model names (empty = every model), because a
// substitute that fixes one model's misreading can hurt another.
type Entry struct {
	Term   string   `yaml:"term"`
	Pinyin string   `yaml:"pinyin"`
	Say    string   `yaml:"say"`
	Models []string `yaml:"models"`
	Test   string   `yaml:"test"` // sentence for offline regression; unused at runtime
	Note   string   `yaml:"note"`
}

type lexiconFile struct {
	Entries []Entry `yaml:"entries"`
}

// reloadEvery bounds how often Apply stats the lexicon file for changes.
const reloadEvery = 30 * time.Second

// Lexicon applies Entry substitutions and reloads its YAML file when it
// changes on disk, so edits take effect without a restart. Safe for
// concurrent use.
type Lexicon struct {
	path string

	mu        sync.Mutex
	modTime   time.Time
	lastCheck time.Time
	entries   []Entry
	byModel   map[string]*strings.Replacer
}

// LoadLexicon reads path. An empty path returns nil (no lexicon); a nil
// *Lexicon is valid and applies nothing.
func LoadLexicon(path string) (*Lexicon, error) {
	if path == "" {
		return nil, nil
	}
	l := &Lexicon{path: path}
	if err := l.reload(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Lexicon) reload() error {
	st, err := os.Stat(l.path)
	if err != nil {
		return fmt.Errorf("tts lexicon: %w", err)
	}
	raw, err := os.ReadFile(l.path)
	if err != nil {
		return fmt.Errorf("tts lexicon: %w", err)
	}
	var f lexiconFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return fmt.Errorf("tts lexicon %s: %w", l.path, err)
	}
	var keep []Entry
	for _, e := range f.Entries {
		if e.Term != "" && e.Say != "" && e.Say != e.Term {
			keep = append(keep, e)
		}
	}
	// Longest term first, so "襄阳路" wins over "襄阳" where both match.
	sort.SliceStable(keep, func(i, j int) bool { return len(keep[i].Term) > len(keep[j].Term) })
	l.entries, l.byModel, l.modTime = keep, map[string]*strings.Replacer{}, st.ModTime()
	return nil
}

// maybeReload re-reads the file when its mtime changed. A broken edit is
// logged and the previous entries stay in effect. Caller holds l.mu.
func (l *Lexicon) maybeReload(now time.Time) {
	if now.Sub(l.lastCheck) < reloadEvery {
		return
	}
	l.lastCheck = now
	st, err := os.Stat(l.path)
	if err != nil || st.ModTime().Equal(l.modTime) {
		return
	}
	if err := l.reload(); err != nil {
		slog.Default().Warn("tts lexicon: reload failed, keeping previous entries", "err", err)
		return
	}
	slog.Default().Info("tts lexicon: reloaded", "path", l.path, "entries", len(l.entries))
}

// Len reports how many entries are loaded; a nil *Lexicon has none.
func (l *Lexicon) Len() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// Apply rewrites every applicable Term in text to its Say for model.
func (l *Lexicon) Apply(text, model string) string {
	if l == nil {
		return text
	}
	l.mu.Lock()
	l.maybeReload(time.Now())
	r, ok := l.byModel[model]
	if !ok {
		var pairs []string
		for _, e := range l.entries {
			if len(e.Models) == 0 || contains(e.Models, model) {
				pairs = append(pairs, e.Term, e.Say)
			}
		}
		r = strings.NewReplacer(pairs...)
		l.byModel[model] = r
	}
	l.mu.Unlock()
	return r.Replace(text)
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
