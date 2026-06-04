package policy

import "strings"

type Mode string

const (
	ModeLinearizable Mode = "linearizable"
	ModeBoundedStale Mode = "bounded_stale"
	ModeEventual     Mode = "eventual"
)

type Rule struct {
	Prefix string
	Mode   Mode
}

type Engine struct {
	DefaultMode Mode
	Rules       []Rule
}

func (e Engine) ModeForKey(key string) Mode {
	longest := -1
	mode := e.DefaultMode
	for _, rule := range e.Rules {
		if strings.HasPrefix(key, rule.Prefix) && len(rule.Prefix) > longest {
			longest = len(rule.Prefix)
			mode = rule.Mode
		}
	}
	return mode
}
