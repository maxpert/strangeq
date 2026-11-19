package clusterraft

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type immutableCommandFixture struct {
	testMachine
	optIn    bool
	commands [][]byte
	sources  []io.ReaderAt
	prefix   int
	capacity int
}

func (m *immutableCommandFixture) AcceptsImmutableJournalCommands() bool { return m.optIn }

func (m *immutableCommandFixture) ApplyJournaled(data []byte, wantReply bool, source io.ReaderAt) ([]byte, error) {
	m.commands = append(m.commands, data)
	m.sources = append(m.sources, source)
	if owned, ok := source.(interface{ ImmutableJournalCommandBacking() (int, int) }); ok {
		m.prefix, m.capacity = owned.ImmutableJournalCommandBacking()
	}
	reply := m.Apply(data)
	if !wantReply {
		reply = nil
	}
	return reply, nil
}

func TestImmutableCommandCapabilityRequiresOptInAndIsolatedProcessOwner(t *testing.T) {
	for _, tc := range []struct {
		name           string
		process, optIn bool
	}{
		{name: "certified", process: true, optIn: true},
		{name: "full-durability", process: false, optIn: true},
		{name: "ordinary-machine", process: true, optIn: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			node, err := OpenNodeJournalWithOptions(dir, true, JournalOptions{ProcessDurability: tc.process})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, node.Close()) })
			machine := &immutableCommandFixture{optIn: tc.optIn}
			group, err := node.OpenGroup(Config{
				ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"), Peers: []uint64{1},
				Machine: machine, TickInterval: -1, ProcessDurability: tc.process,
				AssignmentReplicaBodies: true,
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, group.Close()) })
			require.NoError(t, group.Campaign(t.Context()))
			body := bytes.Repeat([]byte{0xae}, 64<<10)
			_, err = group.Propose(t.Context(), body)
			require.NoError(t, err)
			require.Len(t, machine.commands, 1)
			command := machine.commands[0]
			require.NotSame(t, &body[0], &command[0], "Ready must own bytes independent of the proposal caller")
			clear(body)
			require.Equal(t, bytes.Repeat([]byte{0xae}, 64<<10), command)
			requireJournaledCommand(t, machine.sources[0], command)
			if tc.process && tc.optIn {
				require.Equal(t, len(key{}), machine.prefix)
				require.Equal(t, cap(command)+len(key{}), machine.capacity)
				// Forwarding provenance through assignment sources must also
				// preserve real segment ownership, not turn Retain into a no-op.
				retainer := machine.sources[0].(interface {
					Retain() (io.ReaderAt, func(), error)
				})
				retained, release, err := retainer.Retain()
				require.NoError(t, err)
				require.IsType(t, &journalRetainedSource{}, retained)
				requireJournaledCommand(t, retained, command)
				release()
				require.NoError(t, node.RequireFullDurability())
				_, err = group.Propose(t.Context(), []byte("after peer activation fence"))
				require.NoError(t, err)
				owned := machine.sources[1].(interface{ ImmutableJournalCommandBacking() (int, int) })
				prefix, capacity := owned.ImmutableJournalCommandBacking()
				require.Zero(t, prefix)
				require.Zero(t, capacity)
			} else {
				require.Zero(t, machine.prefix)
				require.Zero(t, machine.capacity)
			}
		})
	}
}
