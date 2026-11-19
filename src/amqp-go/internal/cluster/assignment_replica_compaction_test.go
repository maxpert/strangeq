//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package cluster

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/internal/clusterwire"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
)

type replicaBodyCompactionPeer struct {
	routingPeer
	read func(context.Context, []byte) ([]byte, error)
}

func (p replicaBodyCompactionPeer) ReadCommand(ctx context.Context, data []byte) ([]byte, error) {
	return p.read(ctx, data)
}

func TestAssignmentReplicaRetriesInlineAtSameCursorAfterIngressCompaction(t *testing.T) {
	checkAssignmentReplicaRetry(t, false)
}

func TestAssignmentReplicaUsesAuthoritativePageAfterLocalBodyMismatch(t *testing.T) {
	checkAssignmentReplicaRetry(t, true)
}

func checkAssignmentReplicaRetry(t *testing.T, localMismatch bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	const queue = "replica-body-compaction"
	const groupID = uint64(1)
	first := []byte("already handed off")
	body := bytes.Repeat([]byte{0x8b}, 64<<10)
	tail := []byte("inline from the mixed page")
	openReplica := func(id uint64, coldBody []byte) (*clusterraft.NodeJournal, *clusterraft.Group, clusterqueue.QueueID) {
		t.Helper()
		root := t.TempDir()
		journal, err := clusterraft.OpenNodeJournal(filepath.Join(root, "journal"), true)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, journal.Close()) })

		machine := clusterqueue.NewAssignmentMachine()
		group, err := journal.OpenGroup(clusterraft.Config{
			ID: id, GroupID: groupID, Dir: filepath.Join(root, "group"), Peers: []uint64{id},
			Machine: &machine, TickInterval: -1, AssignmentReplicaBodies: true,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			err := group.Close()
			if !localMismatch || id == 2 {
				require.NoError(t, err)
			}
		})
		require.NoError(t, group.Campaign(ctx))
		apply := func(command clusterqueue.QueueCommand) clusterqueue.QueueResponse {
			t.Helper()
			packet, err := clusterqueue.EncodeCommand(command)
			require.NoError(t, err)
			reply, err := group.Propose(ctx, packet)
			require.NoError(t, err)
			result, err := clusterqueue.DecodeResponse(reply)
			require.NoError(t, err)
			require.NoError(t, result.Err())
			return result
		}
		declared := apply(clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: queue})
		incarnation := declared.Queue.Incarnation
		apply(clusterqueue.QueueCommand{
			Kind: clusterqueue.OpenQueueSession, Queue: queue, Incarnation: incarnation,
			Session: 1, Owner: "consumer", Consumer: true, AutoAssign: true, Prefetch: 3, Deadline: 100,
		})
		for _, payload := range [][]byte{first, coldBody, tail} {
			apply(clusterqueue.QueueCommand{Kind: clusterqueue.PublishMessage, Queue: queue, Incarnation: incarnation, Body: payload})
		}
		require.Equal(t, id, group.Leader())
		return journal, group, incarnation
	}
	ingressBody := body
	if localMismatch {
		// Independent singleton fixtures allow the local logical coordinate
		// to hold different bytes and exercise ingress integrity checking.
		ingressBody = bytes.Repeat([]byte{0x23}, len(body))
	}
	ingressJournal, ingressGroup, incarnation := openReplica(1, ingressBody)
	leaderJournal, leaderGroup, leaderIncarnation := openReplica(2, body)
	require.Equal(t, incarnation, leaderIncarnation)
	request := clusterqueue.AssignmentRequest{
		Queue: queue, Incarnation: incarnation, Session: 1, Owner: "consumer",
		After: 1, Limit: 2, Bytes: 128 << 10,
	}
	full, err := clusterqueue.EncodeAssignmentRequest(request)
	require.NoError(t, err)
	term, firstIndex, lastIndex := ingressGroup.AssignmentJournalCoverage()
	coverage := clusterqueue.AssignmentCoverage{Term: term, First: firstIndex, Last: lastIndex}
	require.NotZero(t, coverage.Term)

	newNode := func(id uint64, journal *clusterraft.NodeJournal, group *clusterraft.Group) *Node {
		return &Node{
			cfg: Config{Peer: Peer{ID: id}, Shards: 1, ExperimentalAssignments: true, ExperimentalReplicaBodies: true},
			ctx: ctx, journal: journal, groups: map[uint64]*clusterraft.Group{groupID: group},
			routes:                 map[uint64]uint64{groupID: 2},
			assignmentReplicaSlots: make(chan struct{}, maxAssignmentReplicaRequests),
		}
	}
	leader := newNode(2, leaderJournal, leaderGroup)
	ingress := newNode(1, ingressJournal, ingressGroup)
	config := clusterwire.Config{
		ClusterID: "assignment-replica-bodies-compaction", Listen: "127.0.0.1:0", Timeout: requestTimeout,
		Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), AllowInsecure: true,
		ProtocolVersion: assignmentPeerProtocol,
	}

	var calls atomic.Uint32
	var gateDone atomic.Bool
	var gateFailure atomic.Pointer[error]
	read := func(work context.Context, frame []byte) (_ []byte, err error) {
		defer func() {
			if err != nil {
				gateFailure.Store(&err)
			}
		}()
		call := calls.Add(1)
		if len(frame) < 9 || binary.BigEndian.Uint64(frame[:8]) != groupID {
			return nil, errors.New("unexpected authenticated group frame")
		}
		data := frame[8:]
		switch call {
		case 1:
			if !bytes.Equal(data, clusterqueue.EncodeAssignmentReplicaRequest(full, coverage)) {
				return nil, errors.New("first read did not advertise exact retained coverage and cursor")
			}
			response, err := leader.ReadCommand(work, frame)
			if err != nil {
				return nil, err
			}
			if len(response) < 10 || response[0] != committed || !clusterqueue.IsAssignmentReplica(response[9:]) {
				return nil, errors.New("leader did not capture a mixed assignment page")
			}
			page, err := clusterqueue.DecodeAssignmentReplicaPage(response[9:], request, coverage)
			if err != nil {
				return nil, err
			}
			refs := page.References()
			if len(refs) != 1 || refs[0].Length != uint32(len(body)) || !bytes.Contains(response[9:], tail) {
				return nil, errors.New("leader did not omit exactly the cold body")
			}
			ref := refs[0]
			spans := []clusterraft.AssignmentJournalSpan{{Index: ref.Index, Term: ref.Term, Offset: ref.Offset, Length: ref.Length}}
			read, release, err := ingressGroup.TryPinAssignmentJournal(spans)
			if err != nil {
				return nil, fmt.Errorf("reference was unavailable before the response: %w", err)
			}
			if localMismatch {
				actual := make([]byte, len(body))
				err := read(0, actual)
				release()
				if err != nil || !bytes.Equal(actual, ingressBody) {
					return nil, fmt.Errorf("fixture failed to expose different local bytes: %v", err)
				}
				gateDone.Store(true)
				return response, nil
			}
			release()
			// The server signs and sends only after this handler returns.
			if err := ingressGroup.Snapshot(work); err != nil {
				return nil, err
			}
			_, release, err = ingressGroup.TryPinAssignmentJournal(spans)
			if release != nil {
				release()
			}
			if !errors.Is(err, raft.ErrCompacted) {
				return nil, fmt.Errorf("reference did not compact before the reply: %v", err)
			}
			gateDone.Store(true)
			return response, nil
		case 2:
			if !gateDone.Load() || !bytes.Equal(data, full) {
				return nil, errors.New("fallback did not retry the exact original inline request")
			}
			return leader.ReadCommand(work, frame)
		case 3:
			if localMismatch && bytes.Equal(data, full) {
				return leader.ReadCommand(work, frame)
			}
			return nil, errors.New("the next request attempted local bodies after a mismatch")
		default:
			return nil, errors.New("more than one inline retry")
		}
	}
	server, err := clusterwire.NewServer(config, replicaBodyCompactionPeer{
		routingPeer: func() ([]byte, error) { return nil, errors.New("assignment retry must not write") }, read: read,
	})
	require.NoError(t, err)
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		closeCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		require.NoError(t, server.Close(closeCtx))
	})

	ingress.cfg.Peer.Address = "127.0.0.1:1"
	leader.cfg.Peer.Address = server.Addr().String()
	placement := &view{
		Nodes:      []Peer{ingress.cfg.Peer, leader.cfg.Peer},
		Placements: []placement{{Group: groupID, Voters: []uint64{1, 2}}},
	}
	ingress.current.Store(placement)
	leader.current.Store(placement)
	ingress.client, err = clusterwire.NewClient(config)
	require.NoError(t, err)
	t.Cleanup(ingress.client.Close)

	deliveries, through, err := ingress.ReadAssignmentPage(ctx, request)
	if failure := gateFailure.Load(); failure != nil {
		require.NoError(t, *failure)
	}
	require.NoError(t, err)
	require.True(t, gateDone.Load())
	require.EqualValues(t, 2, calls.Load())
	want := []clusterqueue.Delivery{
		{MessageID: 2, Attempt: 2, Body: body},
		{MessageID: 3, Attempt: 3, Body: tail},
	}
	require.Equal(t, want, deliveries)
	require.EqualValues(t, 3, through)
	if localMismatch {
		require.Zero(t, ingressGroup.Leader(), "the damaged local group cannot serve as the authority")
		rejected := ingress.executeRead(ctx, groupID, full)
		require.Equal(t, redirect, rejected.code)
		require.Empty(t, rejected.body)
		deliveries, through, err := ingress.ReadAssignmentPage(ctx, request)
		require.NoError(t, err)
		require.Equal(t, want, deliveries)
		require.EqualValues(t, 3, through)
		require.EqualValues(t, 3, calls.Load(), "a damaged local path stays disabled on the next read")
		require.ErrorIs(t, ingressGroup.Close(), clusterqueue.ErrAssignmentBodyDigest)
	}
}
