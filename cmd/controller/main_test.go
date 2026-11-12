package main

import "testing"

func TestLoadConfig(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		want    controllerConfig
		wantErr bool
	}{
		{
			name: "all defaults",
			env:  nil,
			want: controllerConfig{port: 8080, dbPath: "/data/meta.db", vnodeCount: 50},
		},
		{
			name: "all overridden",
			env:  map[string]string{"CONTROLLER_PORT": "9000", "DB_PATH": "/tmp/x.db", "VNODE_COUNT": "100"},
			want: controllerConfig{port: 9000, dbPath: "/tmp/x.db", vnodeCount: 100},
		},
		{
			name: "empty values fall back to defaults",
			env:  map[string]string{"CONTROLLER_PORT": "", "DB_PATH": "", "VNODE_COUNT": ""},
			want: controllerConfig{port: 8080, dbPath: "/data/meta.db", vnodeCount: 50},
		},
		{name: "non-numeric port", env: map[string]string{"CONTROLLER_PORT": "abc"}, wantErr: true},
		{name: "port zero", env: map[string]string{"CONTROLLER_PORT": "0"}, wantErr: true},
		{name: "port too high", env: map[string]string{"CONTROLLER_PORT": "65536"}, wantErr: true},
		{name: "zero vnodes", env: map[string]string{"VNODE_COUNT": "0"}, wantErr: true},
		{name: "negative vnodes", env: map[string]string{"VNODE_COUNT": "-50"}, wantErr: true},
		{name: "non-numeric vnodes", env: map[string]string{"VNODE_COUNT": "fifty"}, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := loadConfig(func(k string) string { return c.env[k] })
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Fatalf("got %+v want %+v", got, c.want)
			}
		})
	}
}
