package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// Cluster observations and process controls extend the same embedded
// management UI and ordinary broker API. One shard omits voter liveness so
// the Nodes screen must preserve the distinction from configured membership.
type clusterUIFixture struct {
	mu           sync.Mutex
	stopped      bool
	failRead     bool
	canControl   bool
	standard     *stubAPI
	actions      []string
	csrf         string
	versionDelay time.Duration
}

func (f *clusterUIFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/api/whoami":
		http.SetCookie(w, &http.Cookie{Name: "SQ_CSRF", Value: "cluster-token", Path: "/"})
		writeJSON(w, 200, map[string]any{"name": "demo-admin"})
	case "/api/strangeq/version":
		time.Sleep(f.versionDelay)
		writeJSON(w, 200, map[string]any{"version": "cluster-demo", "capabilities": map[string]any{"cluster": true, "cluster_node_control": true}})
	case "/api/strangeq/cluster":
		if f.failRead {
			writeJSON(w, 503, map[string]any{"error": "observer unavailable"})
			return
		}
		state, leader := "running", "n1"
		if f.stopped {
			state, leader = "stopped", "n2"
		}
		writeJSON(w, 200, map[string]any{
			"cluster_id": "demo", "observed_at": "2026-11-19T12:00:00Z", "observer": "n2", "can_control": f.canControl,
			"nodes": []map[string]any{
				{"id": "n1", "address": "127.0.0.1:6001", "state": state, "ready": !f.stopped, "managed": true},
				{"id": "n2", "address": "127.0.0.1:6002", "state": "running", "ready": true, "managed": false},
				{"id": "n3", "address": "127.0.0.1:6003", "state": "stopped", "ready": false, "managed": true},
			},
			"groups": []map[string]any{
				{"id": "catalog", "catalog": true, "leader": leader, "voters": []string{"n1", "n2", "n3"}, "active_voters": 2, "quorum": 2, "commit": "9007199254740993", "applied": "9007199254740992"},
				{"id": "shard-1", "voters": []string{"n1", "n2", "n3"}},
			},
		})
	case "/api/strangeq/cluster/nodes/n1/start", "/api/strangeq/cluster/nodes/n1/stop":
		if r.Method != http.MethodPost || !f.canControl {
			w.WriteHeader(403)
			return
		}
		f.csrf = r.Header.Get("X-CSRF-Token")
		f.actions = append(f.actions, r.URL.Path)
		f.stopped = strings.HasSuffix(r.URL.Path, "/stop")
		w.WriteHeader(204)
	default:
		f.standard.ServeHTTP(w, r)
	}
}

func clusterUIServer(t *testing.T, f *clusterUIFixture) *httptest.Server {
	t.Helper()
	f.standard = newStub()
	mux := http.NewServeMux()
	mux.Handle("/api/", f)
	mux.Handle("/", Handler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestUI_ClusterObservationAndControls(t *testing.T) {
	f := &clusterUIFixture{canControl: true}
	srv := clusterUIServer(t, f)
	ctx, cancel := newTab(t)
	defer cancel()
	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/"), chromedp.WaitVisible(chromedp.CSS(".screen-overview")))
	if got := evalStr(t, ctx, `location.pathname`); got != "/" {
		t.Fatalf("cluster-capable home route = %q, want /", got)
	}
	if !evalBool(t, ctx, `Array.from(document.querySelectorAll('#nav a')).every(a => !a.hidden) && !document.getElementById('global-search').hidden`) {
		t.Fatal("cluster capability hid existing navigation or search")
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#nav a[data-nav=queues]")), chromedp.WaitVisible(chromedp.CSS(".screen-queues")))
	if !evalBool(t, ctx, `location.pathname === '/queues' && document.querySelector('.screen-queues').textContent.includes('orders')`) {
		t.Fatal("ordinary queue management did not coexist with cluster capability")
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#nav a[data-nav=nodes]")), chromedp.WaitVisible(chromedp.CSS("#cluster-nodes")))
	if got := evalStr(t, ctx, `document.querySelector('#cluster-groups tr[data-key="shard-1"] [data-col="active_voters"]').textContent`); got != "unknown" {
		t.Fatalf("missing observed liveness = %q, want unknown", got)
	}
	if got := evalStr(t, ctx, `document.querySelector('#cluster-groups [data-col="commit"]').textContent`); got != "Commit index" {
		t.Fatalf("unexpected commit column %q", got)
	}
	if !evalBool(t, ctx, `document.querySelector('#cluster-groups tbody [data-col="commit"]').textContent === '9007199254740993' && document.querySelectorAll('#cluster-nodes button').length === 2`) {
		t.Fatal("indexes lost precision or an unmanaged node has controls")
	}
	mustRun(t, ctx, refreshAwait(`(async function(){ document.getElementById('cluster-action-n1').focus(); await SQ.refreshNow(); return 'ok'; })()`))
	if got := evalStr(t, ctx, `document.activeElement.id`); got != "cluster-action-n1" {
		t.Fatalf("refresh lost meaningful focus: %q", got)
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#cluster-action-n1")), chromedp.WaitVisible(chromedp.CSS("#modal-confirm")), chromedp.Click(chromedp.CSS("#modal-confirm")))
	mustDo(t, ctx, chromedp.WaitVisible(chromedp.CSS(`#cluster-action-n1[aria-label="Start node n1"]`)))
	if got := evalStr(t, ctx, `document.querySelector('#cluster-groups tbody [data-col="leader"]').textContent`); got != "n2" {
		t.Fatalf("post-stop observed leader = %q, want n2", got)
	}
	f.mu.Lock()
	f.failRead = true
	f.mu.Unlock()
	mustRun(t, ctx, refreshAwait(`(async function(){ await SQ.refreshNow(); return 'ok'; })()`))
	if !evalBool(t, ctx, `!!document.querySelector('#cluster-read-error [role="alert"]') && !!document.getElementById('cluster-groups') && document.getElementById('cluster-action-n1').disabled`) {
		t.Fatal("read outage did not preserve stale observation with disabled controls and retry")
	}
	f.mu.Lock()
	f.failRead = false
	f.mu.Unlock()
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#cluster-retry")), chromedp.WaitEnabled(chromedp.CSS("#cluster-action-n1")))
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#cluster-action-n1")), chromedp.Click(chromedp.CSS("#modal-confirm")))
	mustDo(t, ctx, chromedp.WaitVisible(chromedp.CSS(`#cluster-action-n1[aria-label="Stop node n1"]`)))
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.actions) != 2 || f.csrf != "cluster-token" {
		t.Fatalf("actions=%v csrf=%q", f.actions, f.csrf)
	}
	if !f.standard.pathSeen("overview", "/api/overview") || !f.standard.pathSeen("queues", "/api/queues") || !f.standard.pathSeen("health", "/api/health") {
		t.Fatal("cluster-capable UI stopped using ordinary overview, queues, or health routes")
	}
}

func TestUI_ClusterReadOnly(t *testing.T) {
	f := &clusterUIFixture{versionDelay: 200 * time.Millisecond}
	srv := clusterUIServer(t, f)
	ctx, cancel := newTab(t)
	defer cancel()
	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/nodes"), chromedp.WaitVisible(chromedp.CSS("#cluster-nodes")))
	if !evalBool(t, ctx, `document.querySelectorAll('#cluster-nodes button').length === 0 && document.getElementById('cluster-screen').textContent.includes('read only')`) {
		t.Fatal("principal without can_control saw actionable node controls")
	}
	if f.standard.pathSeen("nodes", "/api/nodes") {
		t.Fatal("initial Nodes link fetched standalone data before cluster capability arrived")
	}
}

// A passive notification used to cover the last node's control in a small
// viewport. It must never intercept pointer input; persistent job links must.
func TestUI_ClusterPassiveToastDoesNotBlockLastNode(t *testing.T) {
	srv := clusterUIServer(t, &clusterUIFixture{canControl: true})
	ctx, cancel := newTab(t)
	defer cancel()
	mustDo(t, ctx, chromedp.EmulateViewport(756, 413), chromedp.Navigate(srv.URL+"/nodes"), chromedp.WaitVisible(chromedp.CSS("#cluster-action-n3")))
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
		var button = document.getElementById('cluster-action-n3');
		button.scrollIntoView({block:'center', inline:'nearest'});
		var r = button.getBoundingClientRect();
		var root = document.getElementById('toast-root');
		root.style.left = (r.left - 20) + 'px';
		root.style.top = (r.top - 10) + 'px';
		root.style.right = 'auto'; root.style.bottom = 'auto';
		var toast = document.createElement('div');
		toast.className = 'toast ok'; toast.textContent = 'Stop requested';
		root.appendChild(toast);
		return 'ok';
	})()`))
	if !evalBool(t, ctx, `(function(){ var b = document.getElementById('cluster-action-n3'); var r = b.getBoundingClientRect(); return document.elementFromPoint(r.left+r.width/2,r.top+r.height/2) === b; })()`) {
		t.Fatal("passive notification intercepted the last node's Start control")
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#cluster-action-n3")), chromedp.WaitVisible(chromedp.CSS("#modal-confirm")))
	if got := evalStr(t, ctx, `document.getElementById('modal-title').textContent`); got != "Start node n3?" {
		t.Fatalf("last node opened the wrong dialog: %q", got)
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-cancel")))
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
		var root = document.getElementById('toast-root'); root.replaceChildren();
		root.style.left = '32px'; root.style.top = '80px';
		var notice = document.createElement('div'); notice.className = 'notice job-accepted';
		var link = document.createElement('a'); link.href = '/api/jobs/1'; link.textContent = 'View job status';
		notice.appendChild(link); root.appendChild(notice);
		return 'ok';
	})()`))
	if !evalBool(t, ctx, `(function(){ var a = document.querySelector('#toast-root .job-accepted a'); var r = a.getBoundingClientRect(); return document.elementFromPoint(r.left+r.width/2,r.top+r.height/2) === a; })()`) {
		t.Fatal("interactive background-job link lost pointer input")
	}
}
