package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/clusteradmin"
	"github.com/maxpert/amqp-go/server"
)

type clusterObserverFunc func(context.Context) (clusteradmin.Status, error)

func (f clusterObserverFunc) Observe(ctx context.Context) (clusteradmin.Status, error) { return f(ctx) }

type clusterControllerFunc func(context.Context, string, bool) error

func (f clusterControllerFunc) SetRunning(ctx context.Context, id string, running bool) error {
	return f(ctx, id, running)
}

func clusterTestOptions(t *testing.T) Options {
	t.Helper()
	cfg := baseEnvConfig(t, func(cfg *config.AMQPConfig) {
		cfg.Management.Authentication = config.ManagementAuthenticationBasic
		cfg.Management.Basic.Username = "admin"
		cfg.Management.Basic.PasswordHash = secretHash(t)
		cfg.Management.Basic.Admin = true
	})
	return Options{
		Config: cfg,
		Cluster: clusterObserverFunc(func(ctx context.Context) (clusteradmin.Status, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("observation must receive the request deadline")
			}
			return clusteradmin.Status{ClusterID: "three-node-demo", CanControl: true,
				Nodes: []clusteradmin.Node{{ID: "1", State: "running"}}}, nil
		}),
	}
}

func newClusterTestEnv(t *testing.T, o Options) *env {
	t.Helper()
	m, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return &env{t: t, cfg: o.Config, mgmt: m}
}

func TestClusterInspectionRoutesAndCapabilities(t *testing.T) {
	o := clusterTestOptions(t)
	e := newClusterTestEnv(t, o)
	rec := e.do(http.MethodGet, "/api/strangeq/cluster", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	var status clusteradmin.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.ClusterID != "three-node-demo" || len(status.Nodes) != 1 || status.CanControl {
		t.Fatalf("unexpected status: %+v", status)
	}
	for path, code := range map[string]int{
		"/api/overview": http.StatusOK, "/api/health": http.StatusServiceUnavailable,
		"/api/nodes": http.StatusOK, "/api/queues": http.StatusOK,
		"/api/exchanges": http.StatusOK, "/api/connections": http.StatusOK,
		"/api/channels": http.StatusOK, "/api/metrics": http.StatusNotImplemented,
	} {
		wantStatus(t, e.do(http.MethodGet, path, nil, basicAuthHeader()), code)
	}
	wantStatus(t, e.do(http.MethodPost, "/api/strangeq/cluster/nodes/1/stop", nil, basicAuthHeader()), http.StatusNotFound)
	wantStatus(t, e.do(http.MethodGet, "/api/strangeq/cluster", nil, nil), http.StatusUnauthorized)
	wantStatus(t, e.do(http.MethodGet, "/api/whoami", nil, basicAuthHeader()), http.StatusOK)
	var version versionResponse
	rec = e.do(http.MethodGet, "/api/strangeq/version", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	if err := json.Unmarshal(rec.Body.Bytes(), &version); err != nil {
		t.Fatal(err)
	}
	if !version.Capabilities.Cluster || version.Capabilities.ClusterNodeControl || !version.Capabilities.CloseConnection {
		t.Fatalf("unexpected cluster capabilities: %+v", version.Capabilities)
	}
	if len(version.Endpoints) < 20 {
		t.Fatalf("broker endpoints missing alongside cluster inspection: %v", version.Endpoints)
	}
	// Existing standalone reports retain their exact key set, including no
	// newly added false cluster capabilities.
	o.Cluster = nil
	e = newClusterTestEnv(t, o)
	rec = e.do(http.MethodGet, "/api/strangeq/version", nil, basicAuthHeader())
	if bytes.Contains(rec.Body.Bytes(), []byte(`"cluster`)) {
		t.Fatalf("standalone version gained cluster keys: %s", rec.Body.String())
	}
	wantStatus(t, e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader()), http.StatusOK)
}

func TestClusterControlRequiresLocalExplicitController(t *testing.T) {
	control := clusterControllerFunc(func(context.Context, string, bool) error { return nil })
	tests := []struct {
		name string
		edit func(*Options)
	}{
		{"missing observer", func(o *Options) { o.Cluster = nil }},
		{"public bind", func(o *Options) { o.Config.Management.Address = "0.0.0.0" }},
		{"empty bind", func(o *Options) { o.Config.Management.Address = "" }},
		{"hostname bind", func(o *Options) { o.Config.Management.Address = "localhost" }},
		{"trusted proxy", func(o *Options) { o.Config.Management.TrustedProxies = []string{"127.0.0.1"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := clusterTestOptions(t)
			o.ClusterControl = control
			tt.edit(&o)
			m, err := New(o)
			if m != nil {
				_ = m.Close()
			}
			if err == nil {
				t.Fatal("unsafe cluster control configuration accepted")
			}
		})
	}
}

func TestClusterControlAuthorizationAndCanonicalIDs(t *testing.T) {
	o := clusterTestOptions(t)
	var calls []string
	o.ClusterControl = clusterControllerFunc(func(ctx context.Context, id string, running bool) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("control must receive the request deadline")
		}
		calls = append(calls, fmt.Sprintf("%s:%t", id, running))
		return nil
	})
	e := newClusterTestEnv(t, o)
	sink := &captureSink{}
	e.mgmt.auditSink = sink
	for _, id := range []string{"0", "01", "-1", "+1", "1e2", "1%0A", "18446744073709551616", "999999999999999999999"} {
		wantStatus(t, e.do(http.MethodPost, "/api/strangeq/cluster/nodes/"+id+"/stop", nil, basicAuthHeader()), http.StatusBadRequest)
	}
	if len(calls) != 0 {
		t.Fatal("invalid IDs reached controller")
	}
	wantStatus(t, e.do(http.MethodPost, "/api/strangeq/cluster/nodes/1/stop", nil, basicAuthHeader()), http.StatusNoContent)
	wantStatus(t, e.do(http.MethodPost, "/api/strangeq/cluster/nodes/18446744073709551615/start", nil, basicAuthHeader()), http.StatusNoContent)
	if fmt.Sprint(calls) != "[1:false 18446744073709551615:true]" {
		t.Fatalf("control calls = %v", calls)
	}
	var status clusteradmin.Status
	rec := e.do(http.MethodGet, "/api/strangeq/cluster", nil, basicAuthHeader())
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil || !status.CanControl {
		t.Fatalf("admin control missing: %s (%v)", rec.Body.String(), err)
	}
	recs := sink.records()
	if last := recs[len(recs)-1]; last.Entity != "cluster_node:18446744073709551615" || last.Principal != "admin" || last.Action != "start" || last.Status != http.StatusNoContent {
		t.Fatalf("completion audit = %+v", last)
	}
	e.mgmt.auditSink = failingSink{}
	wantStatus(t, e.do(http.MethodPost, "/api/strangeq/cluster/nodes/1/stop", nil, basicAuthHeader()), http.StatusServiceUnavailable)
	if len(calls) != 2 {
		t.Fatal("controller ran without durable audit")
	}
	e.mgmt.auditSink = sink
	// The listener restriction also applies when mounted via Handler.
	req := httptest.NewRequest(http.MethodPost, "/api/strangeq/cluster/nodes/1/stop", nil)
	req.RemoteAddr = "192.0.2.10:1234"
	req.SetBasicAuth("admin", "secret")
	rec = httptest.NewRecorder()
	e.mgmt.Handler().ServeHTTP(rec, req)
	wantStatus(t, rec, http.StatusForbidden)
	if len(calls) != 2 {
		t.Fatal("remote request reached controller")
	}
	for _, mode := range []string{"reader", "guest"} {
		t.Run(mode, func(t *testing.T) {
			x := clusterTestOptions(t)
			x.ClusterControl = o.ClusterControl
			x.Config.Management.Basic.Admin = false
			if mode == "guest" {
				x.Config.Management.Authentication = config.ManagementAuthenticationNone
				x.Config.Management.AllowUnauthenticatedMutations = true
			}
			xe := newClusterTestEnv(t, x)
			wantStatus(t, xe.do(http.MethodPost, "/api/strangeq/cluster/nodes/1/stop", nil, basicAuthHeader()), http.StatusForbidden)
		})
	}
}

func TestClusterControlSessionCSRF(t *testing.T) {
	o := clusterTestOptions(t)
	calls := 0
	o.ClusterControl = clusterControllerFunc(func(context.Context, string, bool) error { calls++; return nil })
	e := newClusterTestEnv(t, o)
	login := e.do(http.MethodPost, "/api/strangeq/session", []byte(`{"username":"admin","password":"secret"}`), nil)
	wantStatus(t, login, http.StatusOK)
	var sess sessionResponse
	if err := json.Unmarshal(login.Body.Bytes(), &sess); err != nil {
		t.Fatal(err)
	}
	request := func(token, origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/api/strangeq/cluster/nodes/1/stop", nil)
		r.RemoteAddr = "127.0.0.1:1234"
		for _, cookie := range login.Result().Cookies() {
			r.AddCookie(cookie)
		}
		r.Header.Set("X-CSRF-Token", token)
		r.Header.Set("Origin", origin)
		w := httptest.NewRecorder()
		e.mgmt.Handler().ServeHTTP(w, r)
		return w
	}
	wantStatus(t, request("", ""), http.StatusForbidden)
	wantStatus(t, request(sess.CSRFToken, "http://elsewhere.example"), http.StatusForbidden)
	wantStatus(t, request(sess.CSRFToken, ""), http.StatusNoContent)
	if calls != 1 {
		t.Fatalf("CSRF gate allowed %d controller calls", calls)
	}
}

func TestClusterErrorMapping(t *testing.T) {
	for _, tt := range []struct {
		err  error
		code int
	}{
		{clusteradmin.ErrNodeNotFound, http.StatusNotFound},
		{clusteradmin.ErrUseAnotherBroker, http.StatusConflict},
		{fmt.Errorf("wrapped: %w", clusteradmin.ErrConflict), http.StatusConflict},
		{clusteradmin.ErrUnavailable, http.StatusServiceUnavailable},
		{context.DeadlineExceeded, http.StatusServiceUnavailable},
		{errors.New("private implementation detail"), http.StatusInternalServerError},
	} {
		t.Run(tt.err.Error(), func(t *testing.T) {
			o := clusterTestOptions(t)
			o.Cluster = clusterObserverFunc(func(context.Context) (clusteradmin.Status, error) { return clusteradmin.Status{}, tt.err })
			o.ClusterControl = clusterControllerFunc(func(context.Context, string, bool) error { return tt.err })
			e := newClusterTestEnv(t, o)
			for method, path := range map[string]string{http.MethodGet: "/api/strangeq/cluster", http.MethodPost: "/api/strangeq/cluster/nodes/1/stop"} {
				rec := e.do(method, path, nil, basicAuthHeader())
				wantStatus(t, rec, tt.code)
				if bytes.Contains(rec.Body.Bytes(), []byte("private implementation detail")) {
					t.Fatalf("private error leaked: %s", rec.Body.String())
				}
			}
		})
	}
}

// An observer exposing other methods does not grant process-control authority.
type dualClusterObserver struct {
	calls int
}

func (*dualClusterObserver) Observe(context.Context) (clusteradmin.Status, error) {
	return clusteradmin.Status{ClusterID: "local"}, nil
}
func (o *dualClusterObserver) SetRunning(context.Context, string, bool) error {
	o.calls++
	return nil
}

func TestClusterViewSelectsAggregateOrLocalWithoutGrantingControl(t *testing.T) {
	o := clusterTestOptions(t)
	local := &dualClusterObserver{}
	o.Cluster = local
	viewCalls := 0
	o.ClusterView = clusterObserverFunc(func(context.Context) (clusteradmin.Status, error) {
		viewCalls++
		return clusteradmin.Status{ClusterID: "aggregate", CanControl: true}, nil
	})
	e := newClusterTestEnv(t, o)
	for path, want := range map[string]string{
		"/api/strangeq/cluster":            "aggregate",
		"/api/strangeq/cluster?view=local": "local",
	} {
		rec := e.do(http.MethodGet, path, nil, basicAuthHeader())
		wantStatus(t, rec, http.StatusOK)
		var status clusteradmin.Status
		if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil || status.ClusterID != want || status.CanControl {
			t.Fatalf("%s = %s (%v)", path, rec.Body.String(), err)
		}
	}
	if viewCalls != 1 {
		t.Fatalf("local view called aggregate observer: %d calls", viewCalls)
	}
	for _, action := range []string{"start", "stop"} {
		wantStatus(t, e.do(http.MethodPost, "/api/strangeq/cluster/nodes/1/"+action, nil, basicAuthHeader()), http.StatusNotFound)
	}
	if local.calls != 0 {
		t.Fatal("observer inferred lifecycle control")
	}
}

func TestClusterViewRequiresLocalSupervisorScope(t *testing.T) {
	for _, change := range []func(*Options){
		func(o *Options) { o.Cluster = nil },
		func(o *Options) { o.Config.Management.Address = "0.0.0.0" },
		func(o *Options) { o.Config.Management.Address = "" },
		func(o *Options) { o.Config.Management.TrustedProxies = []string{"127.0.0.1"} },
	} {
		o := clusterTestOptions(t)
		o.ClusterView = o.Cluster
		change(&o)
		m, err := New(o)
		if m != nil {
			_ = m.Close()
		}
		if err == nil {
			t.Fatal("aggregate view accepted outside explicit local supervisor scope")
		}
	}
}

type panelClusterMachine struct{ machine server.ClusterMachine }

func (m *panelClusterMachine) Submit(_ context.Context, _ string, data []byte) ([]byte, error) {
	return m.machine.Apply(data), nil
}
func (m *panelClusterMachine) SubmitEachShard(_ context.Context, data []byte) ([][]byte, error) {
	return [][]byte{m.machine.Apply(data)}, nil
}

func TestClusterPanelUsesUnifiedBrokerAlongsideClusterView(t *testing.T) {
	o := clusterTestOptions(t)
	o.Server = &server.Server{Broker: server.NewClusterBroker(&panelClusterMachine{})}
	e := newClusterTestEnv(t, o)
	wantStatus(t, e.do(http.MethodPut, "/api/queues/%2F/panel-jobs", []byte(`{"durable":true}`), basicAuthHeader()), http.StatusCreated)
	wantStatus(t, e.do(http.MethodPut, "/api/exchanges/%2F/panel-events", []byte(`{"type":"direct","durable":true}`), basicAuthHeader()), http.StatusCreated)
	for path, name := range map[string]string{
		"/api/queues": "panel-jobs", "/api/exchanges": "panel-events",
	} {
		rec := e.do(http.MethodGet, path, nil, basicAuthHeader())
		wantStatus(t, rec, http.StatusOK)
		if !bytes.Contains(rec.Body.Bytes(), []byte(name)) {
			t.Fatalf("%s did not read replicated broker entities: %s", path, rec.Body.String())
		}
	}
	rec := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	var overview overviewResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &overview); err != nil || overview.ObjectTotals.Queues != 1 || overview.ObjectTotals.Exchanges != 6 || overview.SnapshotCoherent {
		t.Fatalf("live cluster totals = %s (%v)", rec.Body.String(), err)
	}
	wantStatus(t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()), http.StatusNotImplemented)
	wantStatus(t, e.do(http.MethodGet, "/api/strangeq/cluster", nil, basicAuthHeader()), http.StatusOK)
	if e.mgmt.panicCount.Load() != 0 {
		t.Fatal("embedded cluster panel handlers panicked")
	}
}
