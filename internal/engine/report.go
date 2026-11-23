package engine

import "strings"

// Synthetic service reports are presentation artifacts: they are emitted once as
// stream chunks, but a client stores them as part of the assistant message and
// sends them back with the next request. They must not reach the model prompt,
// the append-only history or the delivered answer (contract.md §9.7 rule 4,
// §9.8 rule 5).
//
// The visible form is a fenced code block, so the client renders pipeline
// information as code with a language label instead of prose. The bracket-line
// forms are kept for history written before the fenced block.
const (
	ProgressBlockLanguage     = "coffee-progress"
	ParticipantsBlockLanguage = "coffee-participants"
	UsageBlockLanguage        = "coffee-usage"

	progressReportPrefix     = "[coffee-progress] "
	participantsReportPrefix = "[coffee-opencode-llm] "
)

// StripServiceReports removes complete service blocks and legacy report lines
// from a text fragment.
func StripServiceReports(text string) string {
	if !containsServiceReport(text) {
		return text
	}
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for i := 0; i < len(lines); i++ {
		if isServiceBlockFence(strings.TrimSpace(lines[i])) {
			if end := closingFence(lines, i+1); end >= 0 {
				i = end
				continue
			}
			continue
		}
		if isParticipantsReportLine(lines[i]) || isProgressReportLine(lines[i]) {
			continue
		}
		out = append(out, lines[i])
	}
	return strings.Join(out, "\n")
}

func containsServiceReport(text string) bool {
	return strings.Contains(text, "```"+ProgressBlockLanguage) ||
		strings.Contains(text, "```"+ParticipantsBlockLanguage) ||
		strings.Contains(text, "```"+UsageBlockLanguage) ||
		strings.Contains(text, progressReportPrefix) ||
		strings.Contains(text, participantsReportPrefix)
}

func isServiceBlockFence(line string) bool {
	if !strings.HasPrefix(line, "```") {
		return false
	}
	language := strings.TrimSpace(strings.TrimPrefix(line, "```"))
	return language == ProgressBlockLanguage || language == ParticipantsBlockLanguage ||
		language == UsageBlockLanguage
}

func closingFence(lines []string, start int) int {
	for i := start; i < len(lines); i++ {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), "```") {
			return i
		}
	}
	return -1
}

func isParticipantsReportLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	body := strings.TrimPrefix(trimmed, participantsReportPrefix)
	if body == trimmed || body == "" {
		return false
	}
	for _, part := range strings.Split(body, "; ") {
		name, _, ok := strings.Cut(part, ":")
		if !ok {
			return false
		}
		switch name {
		case "expert", "assistant", "deflator":
		default:
			return false
		}
	}
	return true
}

func isProgressReportLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	body := strings.TrimPrefix(trimmed, progressReportPrefix)
	return body != trimmed && body != ""
}
