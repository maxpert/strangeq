package clusterqueue

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueueSnapshotFitsAtLegalCapacity(t *testing.T) {
	machine := &Machine{}
	machine.state.Queues = make(map[string]*queueState, maxQueues)
	machine.state.LastQueue = maxQueues
	namePrefix := strings.Repeat("\x01", 1018)
	owner := strings.Repeat("\x01", 128)
	arguments := make([]byte, maxArgumentsBytes)
	body := make([]byte, maxBodyBytes/3)
	body[0] = 1
	var firstQueue string
	for i := range maxQueues {
		name := fmt.Sprintf("%s%06d", namePrefix, i)
		q := &queueState{
			ID: QueueID(i + 1), Spec: QueueSpec{Arguments: arguments}, LastSession: 2,
			Sessions: map[SessionID]*sessionState{
				1: {Owner: owner, Deadline: 1, Prefetch: maxMessages, Consumer: true},
				2: {Owner: owner, Deadline: 1},
			},
		}
		if i == 0 {
			firstQueue = name
			q.LastMessage = maxMessages
			q.LastAttempt = maxMessages
			first := q.Sessions[1]
			first.Sequence = maxMessages
			first.LastKey = commandKey{Kind: ClaimMessage}
			first.Last = outcome{Attempt: maxMessages}
			first.Flights = make(map[AttemptID]messageState, maxMessages)
			for j := range maxMessages {
				message := messageState{ID: MessageID(j + 1)}
				if j < 3 {
					message.Body = body
				}
				first.Flights[AttemptID(j+1)] = message
			}
		}
		machine.state.Queues[name] = q
	}
	require.NoError(t, machine.state.validate(machine.assignments))
	data, err := machine.Snapshot()
	require.NoError(t, err)
	require.Less(t, len(data), maxSnapshotBytes)

	restored := &Machine{}
	require.NoError(t, restored.Restore(data))
	require.Equal(t, machine.state.bodies, restored.state.bodies)
	require.Equal(t, machine.state.messages, restored.state.messages)
	require.Equal(t, machine.state.sessions, restored.state.sessions)
	require.Equal(t, body, restored.state.Queues[firstQueue].Sessions[1].Flights[1].Body)
}

func TestQueueSnapshotRestoresLegacyJSON(t *testing.T) {
	machine := &Machine{}
	require.NoError(t, apply(t, machine, QueueCommand{Kind: DeclareQueue, Queue: queueName}).Err())
	require.NoError(t, apply(t, machine, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: []byte("legacy payload")}).Err())
	data, err := json.Marshal(legacySnapshot{Version: legacySnapshotVersion, State: &machine.state})
	require.NoError(t, err)
	restored := &Machine{}
	require.NoError(t, restored.Restore(data))
	require.Equal(t, machine.state, restored.state)
}
