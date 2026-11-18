package main

import (
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := controllerConfig{addr: ":8080", dbPath: "data/meta.db", gcInterval: 30 * time.Second, stagedTimeout: 5 * time.Minute, orphanGrace: 15 * time.Minute, webPort: 8081, dockerSocket: "/var/run/docker.sock", webDir: ""}
	if cfg != want {
		t.Fatalf("got %+v want %+v", cfg, want)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"CONTROLLER_PORT": "9090", "DB_PATH": "/x.db", "GC_INTERVAL_S": "1", "STAGED_TIMEOUT_MIN": "2", "ORPHAN_GRACE_S": "0", "WEBUI_PORT": "9000", "DEMO_MODE": "true", "DOCKER_SOCKET": "/tmp/d.sock", "WEBUI_DIR": "/ui"}))
	if err != nil {
		t.Fatal(err)
	}
	want := controllerConfig{addr: ":9090", dbPath: "/x.db", gcInterval: time.Second, stagedTimeout: 2 * time.Minute, orphanGrace: 0, webPort: 9000, demoMode: true, dockerSocket: "/tmp/d.sock", webDir: "/ui"}
	if cfg != want {
		t.Fatalf("got %+v want %+v", cfg, want)
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	for _, tc := range []map[string]string{
		{"GC_INTERVAL_S": "0"},
		{"GC_INTERVAL_S": "-5"},
		{"GC_INTERVAL_S": "30s"},
		{"GC_INTERVAL_S": "abc"},
		{"STAGED_TIMEOUT_MIN": "0"},
		{"STAGED_TIMEOUT_MIN": "-1"},
		{"STAGED_TIMEOUT_MIN": "5m"},
		{"ORPHAN_GRACE_S": "-1"},
		{"ORPHAN_GRACE_S": "10s"},
		{"WEBUI_PORT": "-1"},
		{"WEBUI_PORT": "70000"},
		{"WEBUI_PORT": "abc"},
		{"DEMO_MODE": "maybe"},
		{"DEMO_MODE": "2"},
		{"CONTROLLER_PORT": "0"},
		{"CONTROLLER_PORT": "70000"},
	} {
		if _, err := loadConfig(env(tc)); err == nil {
			t.Errorf("%v: expected an error", tc)
		}
	}
}

func TestWebUIPortZeroTurnsTheUIOff(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"WEBUI_PORT": "0"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.webPort != 0 {
		t.Fatalf("webPort %d", cfg.webPort)
	}
}

func TestDemoModeIsOffByDefaultAndAcceptsCommonSpellings(t *testing.T) {
	cfg, _ := loadConfig(env(nil))
	if cfg.demoMode {
		t.Fatal("demo mode must be opt-in")
	}
	for _, v := range []string{"true", "TRUE", "1", "yes", "on"} {
		cfg, err := loadConfig(env(map[string]string{"DEMO_MODE": v}))
		if err != nil || !cfg.demoMode {
			t.Errorf("%q: %v %v", v, cfg.demoMode, err)
		}
	}
}
