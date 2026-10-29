// Command ttstext prints the text Flowkit would send to a TTS model, so offline
// evaluation scripts synthesize exactly what production synthesizes.
//
// It reads lines from stdin and writes the processed lines to stdout. A line
// of the form "id<TAB>text" keeps its id and processes only the text.
//
//	ttstext -lexicon tts_lexicon.yaml -model cosyvoice2 < texts.tsv > processed.tsv
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/DeepFlowAI/flowkit/internal/tts/textproc"
)

func main() {
	lexPath := flag.String("lexicon", os.Getenv("TTS_LEXICON_PATH"), "pronunciation lexicon YAML (optional)")
	model := flag.String("model", os.Getenv("TTS_MODEL"), "model name matched against lexicon entries' models")
	flag.Parse()

	lex, err := textproc.LoadLexicon(*lexPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	proc := &textproc.Processor{Lexicon: lex, Model: *model}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<20)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	for in.Scan() {
		line := in.Text()
		if id, text, ok := strings.Cut(line, "\t"); ok {
			fmt.Fprintf(out, "%s\t%s\n", id, proc.Process(text))
		} else {
			fmt.Fprintln(out, proc.Process(line))
		}
	}
	if err := in.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
