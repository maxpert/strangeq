// Package clusteradmin describes the cluster observations exposed by management.
package clusteradmin

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNodeNotFound = errors.New("unknown cluster node")
	ErrConflict     = errors.New("node action conflicts with its current state")
	ErrStopBlocked  = errors.New("stopping the node would risk losing quorum")
	ErrUnavailable  = errors.New("cluster observation or control unavailable")
)

type Observer interface {
	Observe(context.Context) (Status, error)
}

// Controller is an optional process supervisor. It does not change membership.
type Controller interface {
	SetRunning(context.Context, string, bool) error
}

type Status struct {
	ClusterID  string    `json:"cluster_id"`
	ObservedAt time.Time `json:"observed_at"`
	Observer   string    `json:"observer,omitempty"`
	CanControl bool      `json:"can_control"`
	Nodes      []Node    `json:"nodes"`
	Groups     []Group   `json:"groups"`
}

type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	State   string `json:"state"`
	Ready   *bool  `json:"ready,omitempty"`
	Managed bool   `json:"managed,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

type Group struct {
	ID           string   `json:"id"`
	Catalog      bool     `json:"catalog,omitempty"`
	Observer     string   `json:"observer,omitempty"`
	Leader       string   `json:"leader,omitempty"`
	Term         string   `json:"term"`
	Voters       []string `json:"voters"`
	Learners     []string `json:"learners"`
	Commit       string   `json:"commit"`
	Applied      string   `json:"applied"`
	ConfigIndex  string   `json:"config_index"`
	ActiveVoters *int     `json:"active_voters,omitempty"`
	Quorum       int      `json:"quorum,omitempty"`
}
