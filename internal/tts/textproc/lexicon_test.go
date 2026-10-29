package textproc

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeLexicon(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLexiconApply(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lexicon.yaml")
	writeLexicon(t, path, `
entries:
  - term: 襄阳
    pinyin: xiang1 yang2
    say: 香阳
    models: [cosyvoice2]
  - term: 襄阳路
    say: 香阳路
    models: [cosyvoice2]
  - term: 延续
    say: 延续上
  - term: 只有拼音
    pinyin: zhi3 you3
`)
	l, err := LoadLexicon(path)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct{ model, in, want string }{
		{"cosyvoice2", "襄阳路营业厅，襄阳市", "香阳路营业厅，香阳市"}, // longest term wins
		{"aliyun", "襄阳路营业厅", "襄阳路营业厅"},             // entry limited to cosyvoice2
		{"aliyun", "给您延续", "给您延续上"},                // no models = every model
		{"cosyvoice2", "只有拼音", "只有拼音"},             // no say → nothing to substitute
	}
	for _, c := range cases {
		if got := l.Apply(c.in, c.model); got != c.want {
			t.Errorf("Apply(%q, %s) = %q, want %q", c.in, c.model, got, c.want)
		}
	}
}

func TestLexiconNilAndEmptyPath(t *testing.T) {
	l, err := LoadLexicon("")
	if err != nil || l != nil {
		t.Fatalf("LoadLexicon(\"\") = %v, %v", l, err)
	}
	if got := l.Apply("襄阳", "x"); got != "襄阳" {
		t.Errorf("nil lexicon changed text: %q", got)
	}
}

func TestLexiconReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lexicon.yaml")
	writeLexicon(t, path, "entries:\n  - {term: 甲, say: 乙}\n")
	l, err := LoadLexicon(path)
	if err != nil {
		t.Fatal(err)
	}
	writeLexicon(t, path, "entries:\n  - {term: 甲, say: 丙}\n")
	future := time.Now().Add(time.Minute)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	l.lastCheck = time.Time{} // let the next Apply stat the file
	l.mu.Unlock()
	if got := l.Apply("甲", "m"); got != "丙" {
		t.Errorf("after edit Apply = %q, want 丙", got)
	}

	writeLexicon(t, path, "entries: [broken")
	later := future.Add(time.Minute)
	_ = os.Chtimes(path, later, later)
	l.mu.Lock()
	l.lastCheck = time.Time{}
	l.mu.Unlock()
	if got := l.Apply("甲", "m"); got != "丙" {
		t.Errorf("broken edit should keep previous entries, got %q", got)
	}
}
