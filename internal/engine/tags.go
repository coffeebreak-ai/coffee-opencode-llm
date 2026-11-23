package engine

import (
	"regexp"
	"strings"
)

// Upstream reasoning control markers belong to the inference protocol, not to
// the answer. A backend may emit a stray closing marker inside delta.content;
// the unbalanced tag is a command for the client UI and breaks the client
// session. The gateway removes the whole family, both complete blocks and
// single markers, before any text crosses a boundary (contract.md §9.2.3).
//
// The pattern is deliberately narrow. Client system prompts and tool results
// carry legitimate angle-bracket tags (<user_query>, </shell>, ...) that must
// survive; only a tag whose name starts with think is a command tag.
var (
	commandBlock = regexp.MustCompile(`(?is)\x3cthink[^\x3c\x3e]*\x3e.*?\x3c/think[^\x3c\x3e]*\x3e`)
	commandOpen  = regexp.MustCompile(`(?i)\x3cthink[^\x3c\x3e]*\x3e`)
	commandTag   = regexp.MustCompile(`(?i)\x3c/?think[^\x3c\x3e]*\x3e`)
)

// HasCommandTag reports whether text carries an upstream reasoning marker.
func HasCommandTag(text string) bool {
	return commandTag.MatchString(text)
}

// StripCommandTags removes reasoning blocks and stray markers, including an unclosed block's tail.
func StripCommandTags(text string) string {
	if !HasCommandTag(text) {
		return text
	}
	text = commandBlock.ReplaceAllString(text, "")
	if open := commandOpen.FindStringIndex(text); open != nil {
		text = text[:open[0]]
	}
	return commandTag.ReplaceAllString(text, "")
}

// SanitizeModelText removes gateway service reports and upstream command tags.
func SanitizeModelText(text string) string {
	return StripCommandTags(StripServiceReports(text))
}

type reasoningStreamSanitizer struct {
	pending    string
	discarding bool
	changed    bool
}

func (s *reasoningStreamSanitizer) Changed() bool {
	return s.changed
}

func (s *reasoningStreamSanitizer) writeSafe(output *strings.Builder, text string) {
	cleaned := SanitizeModelText(text)
	s.changed = s.changed || cleaned != text
	output.WriteString(cleaned)
}

func (s *reasoningStreamSanitizer) Push(text string) string {
	s.pending += text
	var output strings.Builder
	for s.pending != "" {
		lower := strings.ToLower(s.pending)
		if s.discarding {
			start := strings.Index(lower, "</think")
			if start < 0 {
				s.keepClosingPrefix()
				return SanitizeModelText(output.String())
			}
			end, ok := commandTagEnd(s.pending, start)
			if !ok {
				s.pending = s.pending[start:]
				return output.String()
			}
			s.pending = s.pending[end:]
			s.discarding = false
			continue
		}

		open := strings.Index(lower, "<think")
		closing := strings.Index(lower, "</think")
		start, opening := firstCommandTag(open, closing)
		if start < 0 {
			keep := max(partialMarkerSuffix(lower, "<think"), partialMarkerSuffix(lower, "</think"))
			safeEnd := len(s.pending) - keep
			s.writeSafe(&output, s.pending[:safeEnd])
			s.pending = s.pending[safeEnd:]
			return output.String()
		}

		s.writeSafe(&output, s.pending[:start])
		end, ok := commandTagEnd(s.pending, start)
		if !ok {
			s.pending = s.pending[start:]
			return output.String()
		}
		s.changed = true
		s.pending = s.pending[end:]
		if opening {
			s.discarding = true
		}
	}
	return output.String()
}

func (s *reasoningStreamSanitizer) Flush() string {
	defer func() {
		s.pending = ""
		s.discarding = false
	}()
	if s.discarding {
		s.changed = true
		return ""
	}
	lower := strings.ToLower(s.pending)
	open, closing := strings.Index(lower, "<think"), strings.Index(lower, "</think")
	start, _ := firstCommandTag(open, closing)
	if start >= 0 {
		s.pending = s.pending[:start]
		s.changed = true
	}
	lower = strings.ToLower(s.pending)
	keep := max(partialMarkerSuffix(lower, "<think"), partialMarkerSuffix(lower, "</think"))
	if keep > 0 {
		s.pending = s.pending[:len(s.pending)-keep]
		s.changed = true
	}
	var output strings.Builder
	s.writeSafe(&output, s.pending)
	return output.String()
}

func (s *reasoningStreamSanitizer) keepClosingPrefix() {
	lower := strings.ToLower(s.pending)
	keep := partialMarkerSuffix(lower, "</think")
	if keep == 0 {
		s.pending = ""
		return
	}
	s.pending = s.pending[len(s.pending)-keep:]
}

func commandTagEnd(text string, start int) (int, bool) {
	relativeEnd := strings.IndexByte(text[start:], '>')
	if relativeEnd < 0 {
		return 0, false
	}
	end := start + relativeEnd
	if strings.Contains(text[start+1:end], "<") {
		return 0, false
	}
	return end + 1, true
}

func firstCommandTag(open, closing int) (int, bool) {
	switch {
	case open < 0 && closing < 0:
		return -1, false
	case closing >= 0 && (open < 0 || closing < open):
		return closing, false
	default:
		return open, true
	}
}

func partialMarkerSuffix(text, marker string) int {
	for size := min(len(text), len(marker)-1); size > 0; size-- {
		if strings.HasSuffix(text, marker[:size]) {
			return size
		}
	}
	return 0
}
