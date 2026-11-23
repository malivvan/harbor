package harbor

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type sessionState struct {
	User  string   `json:"user"`
	Roles []string `json:"roles"`
}

// TestFSSessionSurvivesReopen documents the application-session pattern: a
// named file system holds JSON files, and the store reopens with the same
// contents.
func TestFSSessionSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "app.db")

	s := mustOpen(t, path, "session-password")
	fsys, err := s.File().Open("app")
	require.NoError(t, err)
	require.NoError(t, fsys.MkdirAll("/sessions", 0o755))

	state := sessionState{User: "alice", Roles: []string{"admin", "editor"}}
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	f, err := fsys.Create("/sessions/alice.json")
	require.NoError(t, err)
	_, err = f.Write(raw)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	mustClose(t, s)

	reopened := mustOpen(t, path, "session-password")
	defer mustClose(t, reopened)

	app, err := reopened.File().Open("app")
	require.NoError(t, err)
	body, err := app.ReadFile("/sessions/alice.json")
	require.NoError(t, err)

	var got sessionState
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, state.User, got.User)
	require.Equal(t, state.Roles, got.Roles)

	// The session file is part of the encrypted at-rest store.
	_ = ctx
}
