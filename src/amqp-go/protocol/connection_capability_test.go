package protocol

import "testing"

// TestClientSupportsBlocked covers the client-capability query SQ-12 uses to
// decide whether to emit connection.blocked/unblocked to a given connection.
func TestClientSupportsBlocked(t *testing.T) {
	cases := []struct {
		name  string
		props map[string]interface{}
		want  bool
	}{
		{
			name:  "nil properties",
			props: nil,
			want:  false,
		},
		{
			name:  "no capabilities key",
			props: map[string]interface{}{"product": "x"},
			want:  false,
		},
		{
			name: "capability true",
			props: map[string]interface{}{
				"capabilities": map[string]interface{}{
					"connection.blocked": true,
				},
			},
			want: true,
		},
		{
			name: "capability false",
			props: map[string]interface{}{
				"capabilities": map[string]interface{}{
					"connection.blocked": false,
				},
			},
			want: false,
		},
		{
			name: "capability absent from table",
			props: map[string]interface{}{
				"capabilities": map[string]interface{}{
					"publisher_confirms": true,
				},
			},
			want: false,
		},
		{
			name: "capabilities wrong type",
			props: map[string]interface{}{
				"capabilities": "not-a-table",
			},
			want: false,
		},
		{
			name: "capability wrong type",
			props: map[string]interface{}{
				"capabilities": map[string]interface{}{
					"connection.blocked": "yes",
				},
			},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Connection{ClientProperties: tc.props}
			if got := c.ClientSupportsBlocked(); got != tc.want {
				t.Errorf("ClientSupportsBlocked() = %v, want %v", got, tc.want)
			}
		})
	}
}
