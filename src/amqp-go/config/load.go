package config

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// LoadReport describes where the effective configuration came from. It never
// contains values, only names and paths: the startup line is safe to log.
type LoadReport struct {
	File        string   // absolute path, "" when no file
	EnvKeys     []string // AMQP_* variable NAMES that were applied (never values)
	IgnoredEnv  []string // AMQP_* names that matched no known section (e.g. AMQP_URL)
	UnknownKeys []string // normalized dotted keys present in file/env but bound to no field
}

// knownEnvSections maps the AMQP_<SECTION>_ prefix a variable may use to the
// canonical (normalized) config section. Longest prefix wins, which is why
// "resource_alarms" is listed before any shorter name.
var knownEnvSections = []string{
	"resource_alarms",
	"resourcealarms", // concatenated alias (spec 1.3 step 4)
	"network",
	"storage",
	"security",
	"server",
	"engine",
	"telemetry",
}

// normalizeSection strips underscores from a section name.
func normalizeSection(s string) string {
	return strings.ReplaceAll(strings.ToLower(s), "_", "")
}

// normalizeKey lower-cases every segment and removes underscores, so both
// canonical concatenated keys (authenticationenabled) and snake_case spellings
// (authentication_enabled) bind to the same field. The free-form
// security.authenticationconfig map is exempt: its child keys are preserved
// so map lookups such as AuthenticationConfig["user_file"] keep working.
func normalizeKey(key string) string {
	raw := strings.Split(key, ".")
	parts := make([]string, len(raw))
	for i, p := range raw {
		parts[i] = strings.ReplaceAll(strings.ToLower(p), "_", "")
	}
	// The free-form security.authenticationconfig map keeps its child keys
	// verbatim, so map lookups such as AuthenticationConfig["user_file"]
	// keep working. The exemption is decided on the NORMALIZED two-segment
	// prefix: the snake_case spelling (authentication_config) must exempt
	// the tail exactly like the canonical one.
	if len(parts) > 2 && parts[0] == "security" && parts[1] == "authenticationconfig" {
		copy(parts[2:], raw[2:])
	}
	return strings.Join(parts, ".")
}

// normalizedKoanf copies raw into a new koanf instance with every key
// normalized. Two raw keys that normalize to the same key are ambiguous and
// rejected: silently picking one was the old behavior that made snake_case
// keys look like they worked.
func normalizedKoanf(raw *koanf.Koanf) (*koanf.Koanf, error) {
	k := koanf.New(".")
	all := raw.All()
	keys := make([]string, 0, len(all))
	for key := range all {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	seen := make(map[string]string, len(keys))
	for _, key := range keys {
		norm := normalizeKey(key)
		if prev, ok := seen[norm]; ok && prev != key {
			return nil, fmt.Errorf("duplicate config key %q (also given as %q)", key, prev)
		}
		seen[norm] = key
		if err := k.Set(norm, all[key]); err != nil {
			return nil, fmt.Errorf("failed to normalize config key %q: %w", key, err)
		}
	}
	return k, nil
}

// envTransform maps AMQP_<SECTION>_<KEY> to <section>.<normalizedkey>. A name
// whose first segment is not a known config section is ignored (key ""),
// which keeps AMQP_URL and other unrelated variables from clobbering a whole
// section map.
func envTransform(report *LoadReport) func(string, string) (string, any) {
	return func(name, value string) (string, any) {
		if !strings.HasPrefix(name, "AMQP_") {
			return "", value
		}
		lower := strings.ToLower(strings.TrimPrefix(name, "AMQP_"))
		for _, section := range knownEnvSections {
			prefix := section + "_"
			if !strings.HasPrefix(lower, prefix) {
				continue
			}
			rest := strings.ReplaceAll(lower[len(prefix):], "_", "")
			if report != nil {
				report.EnvKeys = append(report.EnvKeys, name)
			}
			return normalizeSection(section) + "." + rest, value
		}
		if report != nil {
			report.IgnoredEnv = append(report.IgnoredEnv, name)
		}
		return "", value
	}
}

// loadEnv applies AMQP_* variables onto k.
func loadEnv(k *koanf.Koanf, report *LoadReport) error {
	if err := k.Load(env.Provider(".", env.Opt{
		Prefix:        "AMQP_",
		TransformFunc: envTransform(report),
	}), nil); err != nil {
		return fmt.Errorf("failed to load environment variables: %w", err)
	}
	return nil
}

// applyAddressPortReconciliation makes network.address (the only value the
// listener reads) and network.port agree. It runs only in LoadConfig: Validate
// must stay free of this check because test helpers set Address without Port.
func applyAddressPortReconciliation(cfg *AMQPConfig, k *koanf.Koanf) error {
	addressSet := k.Exists("network.address")
	portSet := k.Exists("network.port")
	switch {
	case portSet && !addressSet:
		host, _, err := net.SplitHostPort(DefaultConfig().Network.Address)
		if err != nil {
			host = DefaultConfig().Network.Address
		}
		cfg.Network.Address = net.JoinHostPort(host, strconv.Itoa(cfg.Network.Port))
	case addressSet && !portSet:
		host, portStr, err := net.SplitHostPort(cfg.Network.Address)
		if err != nil {
			// No port in the address (for example "0.0.0.0" or "::1").
			cfg.Network.Address = net.JoinHostPort(cfg.Network.Address, strconv.Itoa(cfg.Network.Port))
			return nil
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("network.address has a non-numeric port %q", portStr)
		}
		_ = host
		cfg.Network.Port = port
	case addressSet && portSet:
		_, portStr, err := net.SplitHostPort(cfg.Network.Address)
		if err != nil {
			// Address has no port: append the configured one.
			cfg.Network.Address = net.JoinHostPort(cfg.Network.Address, strconv.Itoa(cfg.Network.Port))
			return nil
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("network.address has a non-numeric port %q", portStr)
		}
		if port != cfg.Network.Port {
			return fmt.Errorf("network.address port %s conflicts with network.port %d", portStr, cfg.Network.Port)
		}
	}
	return nil
}

// decodeInto decodes k onto cfg and records keys that matched no field.
func decodeInto(k *koanf.Koanf, cfg *AMQPConfig, report *LoadReport) error {
	var md mapstructure.Metadata
	err := k.UnmarshalWithConf("", cfg, koanf.UnmarshalConf{DecoderConfig: &mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.TextUnmarshallerHookFunc()),
		WeaklyTypedInput: true,
		Metadata:         &md,
		Result:           cfg,
		TagName:          "koanf",
	}})
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if report != nil {
		unknown := make([]string, 0, len(md.Unused))
		for _, key := range md.Unused {
			unknown = append(unknown, strings.ToLower(key))
		}
		sort.Strings(unknown)
		report.UnknownKeys = unknown
	}
	return nil
}

// LoadConfig builds the effective config: DefaultConfig() → file (optional) →
// AMQP_* env. It does NOT call Validate: main validates once, after CLI flags.
// path=="" skips the file.
func LoadConfig(path string) (*AMQPConfig, *LoadReport, error) {
	cfg := DefaultConfig()
	report := &LoadReport{}

	raw := koanf.New(".")
	if path != "" {
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to resolve config file path %s: %w", path, err)
		}
		report.File = abs
		if err := raw.Load(file.Provider(abs), yaml.Parser()); err != nil {
			return nil, nil, fmt.Errorf("failed to load config file %s: %w", abs, err)
		}
	}

	k, err := normalizedKoanf(raw)
	if err != nil {
		return nil, nil, err
	}
	if err := loadEnv(k, report); err != nil {
		return nil, nil, err
	}
	if err := decodeInto(k, cfg, report); err != nil {
		return nil, nil, err
	}
	// Reconciliation runs after decode (it needs the decoded port), but before
	// return so the listener sees an address consistent with the configured port.
	if err := applyAddressPortReconciliation(cfg, k); err != nil {
		return nil, nil, err
	}
	return cfg, report, nil
}

// PrintLoadReport writes the one-line source summary plus one warning per
// unknown key and per ignored AMQP_* variable. Names only, never values.
func PrintLoadReport(w io.Writer, report *LoadReport) {
	if report == nil {
		fmt.Fprintln(w, "Config sources: defaults")
		return
	}
	sources := []string{"defaults"}
	if report.File != "" {
		sources = append(sources, "file="+report.File)
	}
	if len(report.EnvKeys) > 0 {
		keys := append([]string(nil), report.EnvKeys...)
		sort.Strings(keys)
		sources = append(sources, "env=["+strings.Join(keys, " ")+"]")
	}
	fmt.Fprintf(w, "Config sources: %s\n", strings.Join(sources, ", "))

	for _, key := range report.UnknownKeys {
		fmt.Fprintf(w, "unknown config key %q ignored (check spelling; see config.sample.yaml)\n", key)
	}
	for _, name := range report.IgnoredEnv {
		fmt.Fprintf(w, "ignored environment variable %s (no matching config section)\n", name)
	}
}
