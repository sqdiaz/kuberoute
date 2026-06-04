package policy

import "testing"

func TestModeForKeyLongestPrefixWins(t *testing.T) {
	engine := Engine{
		DefaultMode: ModeLinearizable,
		Rules: []Rule{
			{Prefix: "/eventual/", Mode: ModeEventual},
			{Prefix: "/eventual/team-a/", Mode: ModeBoundedStale},
		},
	}

	mode := engine.ModeForKey("/eventual/team-a/config")
	if mode != ModeBoundedStale {
		t.Fatalf("expected %s, got %s", ModeBoundedStale, mode)
	}
}
