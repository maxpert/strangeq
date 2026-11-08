package management

import (
	"encoding/json"
	"os"
	"strings"
	"sync"

	"github.com/maxpert/amqp-go/interfaces"
)

// userRecord is the read-only projection of one auth.json user (SEC-070: no
// hashes, no metadata, no groups).
type userRecord struct {
	Name             string
	Tags             []string
	VHostPermissions []interfaces.VHostPermission
}

// permissionRecord is one {user,vhost,configure,write,read} triple.
type permissionRecord struct {
	User      string
	VHost     string
	Configure string
	Write     string
	Read      string
}

// userSource reads auth.json directly (SEC-091) as the read-only source of the
// user and permission lists. Writes are 501 (BOB-6); this type never mutates
// the file.
type userSource struct {
	path string

	mu     sync.RWMutex
	users  []userRecord
	perms  []permissionRecord
	loaded bool
}

// loadUserSource parses the auth file if it exists. A missing or unreadable
// file yields an empty source with loaded=false: the management API reports
// `200 []`, never an error (FR-API-111).
func loadUserSource(path string) *userSource {
	u := &userSource{path: path}
	u.reload()
	return u
}

type rawAuthFile struct {
	Users []struct {
		Username         string                       `json:"username"`
		VHostPermissions []interfaces.VHostPermission `json:"vhost_permissions"`
		Tags             []string                     `json:"tags"`
	} `json:"users"`
}

// reload re-reads the file. It returns the list of usernames that changed or
// were removed so a caller (SIGHUP) can invalidate their sessions (AUTH-051).
func (u *userSource) reload() []string {
	u.mu.Lock()
	defer u.mu.Unlock()

	prev := make(map[string][]string, len(u.users))
	for _, r := range u.users {
		prev[r.Name] = r.Tags
	}

	if u.path == "" {
		u.users, u.perms, u.loaded = nil, nil, false
		return changedNames(prev, nil)
	}
	data, err := os.ReadFile(u.path)
	if err != nil {
		u.users, u.perms, u.loaded = nil, nil, false
		return changedNames(prev, nil)
	}
	var raw rawAuthFile
	if err := json.Unmarshal(data, &raw); err != nil {
		u.users, u.perms, u.loaded = nil, nil, false
		return changedNames(prev, nil)
	}

	users := make([]userRecord, 0, len(raw.Users))
	var perms []permissionRecord
	now := make(map[string][]string, len(raw.Users))
	for _, ru := range raw.Users {
		users = append(users, userRecord{
			Name:             ru.Username,
			Tags:             append([]string(nil), ru.Tags...),
			VHostPermissions: append([]interfaces.VHostPermission(nil), ru.VHostPermissions...),
		})
		now[ru.Username] = ru.Tags
		for _, vp := range ru.VHostPermissions {
			perms = append(perms, permissionRecord{
				User:      ru.Username,
				VHost:     vp.VHost,
				Configure: vp.Permission.Configure,
				Write:     vp.Permission.Write,
				Read:      vp.Permission.Read,
			})
		}
	}
	u.users, u.perms, u.loaded = users, perms, true
	return changedNames(prev, now)
}

func changedNames(prev, now map[string][]string) []string {
	var out []string
	for name, tags := range prev {
		cur, ok := now[name]
		if !ok || !sameTags(tags, cur) {
			out = append(out, name)
		}
	}
	return out
}

func sameTags(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (u *userSource) list() []userRecord {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return append([]userRecord(nil), u.users...)
}

func (u *userSource) get(name string) (userRecord, bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	for _, r := range u.users {
		if r.Name == name {
			return r, true
		}
	}
	return userRecord{}, false
}

func (u *userSource) permissions() []permissionRecord {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return append([]permissionRecord(nil), u.perms...)
}

func (u *userSource) permissionsFor(user string) []permissionRecord {
	u.mu.RLock()
	defer u.mu.RUnlock()
	var out []permissionRecord
	for _, p := range u.perms {
		if p.User == user {
			out = append(out, p)
		}
	}
	return out
}

func (u *userSource) permission(vhost, user string) (permissionRecord, bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	for _, p := range u.perms {
		if p.User == user && p.VHost == vhost {
			return p, true
		}
	}
	return permissionRecord{}, false
}

// names returns the set of usernames, used by the SIGHUP invalidation
// predicate.
func (u *userSource) names() map[string]struct{} {
	u.mu.RLock()
	defer u.mu.RUnlock()
	out := make(map[string]struct{}, len(u.users))
	for _, r := range u.users {
		out[r.Name] = struct{}{}
	}
	return out
}

var _ = strings.TrimSpace
