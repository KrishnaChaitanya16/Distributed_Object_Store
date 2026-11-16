package config

import (
	"strings"
	"testing"
)

func fakeEnv(m map[string]string) Getenv {
	return func(k string) string { return m[k] }
}

func TestStr(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"unset uses default", nil, "def"},
		{"empty uses default", map[string]string{"K": ""}, "def"},
		{"whitespace uses default", map[string]string{"K": "   "}, "def"},
		{"value wins", map[string]string{"K": "/data"}, "/data"},
		{"value is trimmed", map[string]string{"K": "  /data \n"}, "/data"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Str(fakeEnv(c.env), "K", "def"); got != c.want {
				t.Fatalf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestRequired(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		want    string
		wantErr bool
	}{
		{"unset errors", nil, "", true},
		{"empty errors", map[string]string{"K": ""}, "", true},
		{"whitespace errors", map[string]string{"K": " \t "}, "", true},
		{"value ok", map[string]string{"K": "worker1"}, "worker1", false},
		{"value trimmed", map[string]string{"K": " worker1 "}, "worker1", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Required(fakeEnv(c.env), "K")
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if got != c.want {
				t.Fatalf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestInt(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    int
		wantErr bool
	}{
		{"unset uses default", "", 7, false},
		{"whitespace uses default", "  ", 7, false},
		{"positive", "42", 42, false},
		{"zero", "0", 0, false},
		{"negative", "-5", -5, false},
		{"padded", " 9 ", 9, false},
		{"plus sign", "+3", 3, false},
		{"letters", "abc", 0, true},
		{"float", "1.5", 0, true},
		{"trailing junk", "8080x", 0, true},
		{"hex", "0x10", 0, true},
		{"overflow", "99999999999999999999", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Int(fakeEnv(map[string]string{"K": c.val}), "K", 7)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Fatalf("got %d want %d", got, c.want)
			}
		})
	}
}

func TestPositiveInt(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    int
		wantErr bool
	}{
		{"default", "", 50, false},
		{"one", "1", 1, false},
		{"large", "1000", 1000, false},
		{"zero rejected", "0", 0, true},
		{"negative rejected", "-1", 0, true},
		{"invalid rejected", "many", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := PositiveInt(fakeEnv(map[string]string{"K": c.val}), "K", 50)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Fatalf("got %d want %d", got, c.want)
			}
		})
	}
	if _, err := PositiveInt(fakeEnv(nil), "K", 0); err == nil {
		t.Fatal("invalid default must also be rejected")
	}
}

func TestNonNegativeInt(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    int
		wantErr bool
	}{
		{"default", "", 10, false},
		{"zero allowed", "0", 0, false},
		{"zero with spaces", " 0 ", 0, false},
		{"positive", "250", 250, false},
		{"negative rejected", "-1", 0, true},
		{"float rejected", "1.5", 0, true},
		{"unit suffix rejected", "10ms", 0, true},
		{"invalid rejected", "none", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := NonNegativeInt(fakeEnv(map[string]string{"K": c.val}), "K", 10)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Fatalf("got %d want %d", got, c.want)
			}
		})
	}
	if _, err := NonNegativeInt(fakeEnv(nil), "K", -1); err == nil {
		t.Fatal("invalid default must also be rejected")
	}
	if _, err := NonNegativeInt(fakeEnv(map[string]string{"SCRUB_PAUSE_MS": "-5"}), "SCRUB_PAUSE_MS", 0); err == nil || !strings.Contains(err.Error(), "SCRUB_PAUSE_MS") {
		t.Fatalf("error must name the key, got %v", err)
	}
}

func TestPort(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    int
		wantErr bool
	}{
		{"default", "", 8080, false},
		{"lowest", "1", 1, false},
		{"highest", "65535", 65535, false},
		{"zero", "0", 0, true},
		{"negative", "-80", 0, true},
		{"too high", "65536", 0, true},
		{"not a number", "http", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Port(fakeEnv(map[string]string{"K": c.val}), "K", 8080)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Fatalf("got %d want %d", got, c.want)
			}
		})
	}
}

func TestHostPort(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    string
		wantErr bool
	}{
		{"default", "", "controller:8080", false},
		{"hostname", "ctrl:9000", "ctrl:9000", false},
		{"ipv4", "10.0.0.1:8080", "10.0.0.1:8080", false},
		{"ipv6", "[::1]:8080", "[::1]:8080", false},
		{"trimmed", " ctrl:9000 ", "ctrl:9000", false},
		{"missing port", "controller", "", true},
		{"empty port", "controller:", "", true},
		{"missing host", ":8080", "", true},
		{"port zero", "controller:0", "", true},
		{"port too high", "controller:70000", "", true},
		{"port not numeric", "controller:http", "", true},
		{"scheme included", "http://controller:8080", "", true},
		{"bare ipv6", "::1:8080", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := HostPort(fakeEnv(map[string]string{"K": c.val}), "K", "controller:8080")
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if got != c.want {
				t.Fatalf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestErrorMentionsKey(t *testing.T) {
	_, err := Port(fakeEnv(map[string]string{"WORKER_PORT": "x"}), "WORKER_PORT", 1)
	if err == nil || !strings.Contains(err.Error(), "WORKER_PORT") {
		t.Fatalf("error should name the env var, got %v", err)
	}
}
