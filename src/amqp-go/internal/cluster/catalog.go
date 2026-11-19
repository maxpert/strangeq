package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
)

const catalogID = ^uint64(0)
const replicaCount = 3

type Peer struct {
	ID      uint64
	Address string
}

type placement struct {
	Group, Index     uint64
	Voters, Learners []uint64
}

type view struct {
	Cluster       string
	Shards, Epoch uint64
	Nodes         []Peer
	Placements    []placement
	Catalog       placement
	CatalogTarget []uint64
	Nonces        map[uint64]string
}

type catalogCommand struct {
	Join         *Peer
	Nonce        string
	Report       *placement
	ActivateSeed *Peer `json:",omitempty"`
}

type catalogReply struct {
	View  view
	Error string
}

type catalog struct {
	initial view
	current atomic.Pointer[view]
}

func newCatalog(id string, shards uint64, seeds []Peer) (*catalog, error) {
	if id == "" || len(id) > 128 || shards == 0 || shards > 1024 || (len(seeds) == 0 || len(seeds) > replicaCount) {
		return nil, errors.New("cluster requires an ID, one to three seed peers and 1–1024 shards")
	}
	nodes := slices.Clone(seeds)
	slices.SortFunc(nodes, comparePeer)
	if err := checkPeers(nodes); err != nil {
		return nil, err
	}
	voters := make([]uint64, 0, len(nodes))
	for _, node := range nodes {
		voters = append(voters, node.ID)
	}
	initial := view{Cluster: id, Shards: shards, Epoch: 1, Nodes: nodes, Placements: make([]placement, shards), Catalog: placement{Group: catalogID, Voters: voters}, CatalogTarget: voters}
	for i := range initial.Placements {
		initial.Placements[i] = placement{Group: uint64(i + 1), Voters: voters}
	}
	c := &catalog{initial: initial}
	c.current.Store(&initial)
	return c, nil
}

func compareID(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func comparePeer(a, b Peer) int           { return compareID(a.ID, b.ID) }
func (c *catalog) view() view             { return *c.current.Load() }
func contains(a []uint64, id uint64) bool { return slices.Contains(a, id) }
func lookupPeer(v view, id uint64) (Peer, bool) {
	i, ok := slices.BinarySearchFunc(v.Nodes, id, func(p Peer, id uint64) int { return compareID(p.ID, id) })
	if !ok {
		return Peer{}, false
	}
	return v.Nodes[i], true
}
func address(v view, id uint64) string {
	peer, _ := lookupPeer(v, id)
	return peer.Address
}
func equalMembers(a, b []uint64) bool {
	return len(a) == len(b) && slices.Equal(a, b)
}
func validAddress(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	n, numberErr := strconv.ParseUint(port, 10, 16)
	return err == nil && numberErr == nil && n != 0 && host != "" && !strings.ContainsAny(host, " \t\r\n/?#@")
}
func checkPeers(peers []Peer) error {
	addresses := make(map[string]bool, len(peers))
	var prior uint64
	for _, peer := range peers {
		localOnly := len(peers) == 1 && peer.Address == ""
		if !clusterraft.ValidMemberID(peer.ID) || peer.ID <= prior || (!localOnly && !validAddress(peer.Address)) || addresses[peer.Address] {
			return errors.New("peer IDs and advertised addresses must be valid and unique")
		}
		prior, addresses[peer.Address] = peer.ID, true
	}
	return nil
}

func groupFor(queue string, shards uint64) uint64 {
	digest := sha256.Sum256([]byte(queue))
	return binary.BigEndian.Uint64(digest[:8])%shards + 1
}

// rank returns the highest scoring peers first, with preferred leader first.
func rank(v view, group uint64) []uint64 {
	type candidate struct{ id, score uint64 }
	var top [replicaCount]candidate
	var key [16]byte
	binary.BigEndian.PutUint64(key[:8], group)
	for _, peer := range v.Nodes {
		binary.BigEndian.PutUint64(key[8:], peer.ID)
		sum := sha256.Sum256(key[:])
		c := candidate{peer.ID, binary.BigEndian.Uint64(sum[:8])}
		for i := range top {
			if top[i].id == 0 || c.score > top[i].score {
				top[i], c = c, top[i]
				if c.id == 0 {
					break
				}
			}
		}
	}
	result := make([]uint64, 0, replicaCount)
	for _, peer := range top {
		if peer.id != 0 {
			result = append(result, peer.id)
		}
	}
	return result
}
func encode(value any) []byte { data, _ := json.Marshal(value); return data }

func validNonce(s string) bool {
	if len(s) != 32 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func (c *catalog) Apply(raw []byte) []byte {
	v := c.view()
	reply := catalogReply{View: v}
	var cmd catalogCommand
	if json.Unmarshal(raw, &cmd) != nil {
		reply.Error = "invalid catalog command"
		return encode(reply)
	}
	changed := false
	switch {
	case cmd.ActivateSeed != nil && cmd.Join == nil && cmd.Report == nil && cmd.Nonce == "":
		p := *cmd.ActivateSeed
		current, known := lookupPeer(v, p.ID)
		switch {
		case len(c.initial.Nodes) != 1 || p.ID != c.initial.Nodes[0].ID || !known || !validAddress(p.Address):
			reply.Error = "activation requires the sole original voter and a valid peer address"
		case current.Address == p.Address:
			// A retry can arrive after the first replicas have already joined.
		case current.Address != "" || len(v.Nodes) != 1:
			reply.Error = "seed peer address already established"
		default:
			v.Nodes = []Peer{p}
			changed = true
		}
	case cmd.Join != nil && cmd.ActivateSeed == nil && cmd.Report == nil:
		p := *cmd.Join
		known, enrolled := lookupPeer(v, p.ID)
		switch {
		case len(v.Nodes) == 1 && v.Nodes[0].Address == "":
			reply.Error = "activate the seed peer address before enrollment"
		case !validNonce(cmd.Nonce):
			reply.Error = "invalid enrollment identity"
		case enrolled && known.Address == p.Address && v.Nonces[p.ID] == cmd.Nonce:
		case enrolled:
			reply.Error = "node identity already enrolled"
		default:
			nodes := append(slices.Clone(v.Nodes), p)
			slices.SortFunc(nodes, comparePeer)
			if err := checkPeers(nodes); err != nil {
				reply.Error = err.Error()
				break
			}
			v.Nodes = nodes
			v.Nonces = maps.Clone(v.Nonces)
			if v.Nonces == nil {
				v.Nonces = make(map[uint64]string)
			}
			v.Nonces[p.ID] = cmd.Nonce
			if len(v.CatalogTarget) < replicaCount {
				v.CatalogTarget = append(slices.Clone(v.CatalogTarget), p.ID)
			}
			changed = true
		}
	case cmd.Report != nil && cmd.Join == nil && cmd.ActivateSeed == nil && cmd.Nonce == "":
		p := *cmd.Report
		if !checkPlacement(v, p) {
			reply.Error = "invalid committed placement"
			break
		}
		current := groupPlacement(v, p.Group)
		if p.Index == current.Index && (!equalMembers(p.Voters, current.Voters) || !equalMembers(p.Learners, current.Learners)) {
			reply.Error = "conflicting placement at one index"
		} else if p.Index > current.Index {
			if p.Group == catalogID {
				v.Catalog = p
			} else {
				v.Placements = slices.Clone(v.Placements)
				v.Placements[p.Group-1] = p
			}
			changed = true
		}
	default:
		reply.Error = "invalid catalog command"
	}
	if changed {
		v.Epoch++
		c.current.Store(&v)
		reply.View = v
	}
	return encode(reply)
}

func checkPlacement(v view, p placement) bool {
	if p.Group == 0 || (p.Group != catalogID && p.Group > v.Shards) || len(p.Voters) < 1 || len(p.Voters) > replicaCount+1 || len(p.Learners) > 1 {
		return false
	}
	for _, list := range [][]uint64{p.Voters, p.Learners} {
		var prior uint64
		for _, id := range list {
			if _, known := lookupPeer(v, id); id <= prior || !known {
				return false
			}
			prior = id
		}
	}
	return len(p.Learners) == 0 || !contains(p.Voters, p.Learners[0])
}

func groupPlacement(v view, id uint64) placement {
	if id == catalogID {
		return v.Catalog
	}
	return v.Placements[id-1]
}

func desiredMembers(v view, id uint64) []uint64 {
	if id == catalogID {
		return v.CatalogTarget
	}
	return rank(v, id)
}

type catalogSnapshot struct {
	Version uint16
	View    view
}

func (c *catalog) Snapshot() ([]byte, error) { return json.Marshal(catalogSnapshot{1, c.view()}) }
func (c *catalog) Restore(data []byte) error {
	var snap catalogSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Version != 1 || !c.valid(snap.View) {
		return errors.New("invalid catalog snapshot")
	}
	c.current.Store(&snap.View)
	return nil
}
func (c *catalog) valid(v view) bool {
	if v.Cluster != c.initial.Cluster || v.Shards != c.initial.Shards || v.Epoch < 1 ||
		len(v.Placements) != int(v.Shards) || checkPeers(v.Nodes) != nil {
		return false
	}
	if len(v.Nonces) != len(v.Nodes)-len(c.initial.Nodes) {
		return false
	}
	for _, p := range c.initial.Nodes {
		peer, known := lookupPeer(v, p.ID)
		// A singleton's endpoint is established by the replicated activation.
		// A joiner's discovery address need not be its authoritative endpoint.
		if !known || (len(c.initial.Nodes) > 1 && peer.Address != p.Address) || v.Nonces[p.ID] != "" {
			return false
		}
	}
	for _, p := range v.Nodes {
		if _, original := lookupPeer(c.initial, p.ID); !original && !validNonce(v.Nonces[p.ID]) {
			return false
		}
	}
	if v.Catalog.Group != catalogID || !checkPlacement(v, v.Catalog) ||
		len(v.CatalogTarget) != min(replicaCount, len(v.Nodes)) {
		return false
	}
	targets := make(map[uint64]bool, len(v.CatalogTarget))
	for _, id := range v.CatalogTarget {
		if _, known := lookupPeer(v, id); !known || targets[id] {
			return false
		}
		targets[id] = true
	}
	for _, peer := range c.initial.Nodes {
		if !targets[peer.ID] {
			return false
		}
	}
	for _, id := range append(slices.Clone(v.Catalog.Voters), v.Catalog.Learners...) {
		if !targets[id] {
			return false
		}
	}
	for i, p := range v.Placements {
		if p.Group != uint64(i+1) || !checkPlacement(v, p) {
			return false
		}
	}
	return true
}
