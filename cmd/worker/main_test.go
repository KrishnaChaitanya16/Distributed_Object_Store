package main

import "testing"

func TestLoadConfig(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		want    workerConfig
		wantErr bool
	}{
		{
			name: "only required id set",
			env:  map[string]string{"WORKER_ID": "worker1"},
			want: workerConfig{workerID: "worker1", port: 8001, dataDir: "/data", controllerAddr: "controller:8080"},
		},
		{
			name: "all overridden",
			env: map[string]string{
				"WORKER_ID": "worker3", "WORKER_PORT": "8003",
				"DATA_DIR": "/mnt/chunks", "CONTROLLER_ADDRESS": "10.0.0.5:9090",
			},
			want: workerConfig{workerID: "worker3", port: 8003, dataDir: "/mnt/chunks", controllerAddr: "10.0.0.5:9090"},
		},
		{
			name: "id is trimmed",
			env:  map[string]string{"WORKER_ID": "  worker2 "},
			want: workerConfig{workerID: "worker2", port: 8001, dataDir: "/data", controllerAddr: "controller:8080"},
		},
		{name: "missing id", env: nil, wantErr: true},
		{name: "blank id", env: map[string]string{"WORKER_ID": "   "}, wantErr: true},
		{name: "invalid port", env: map[string]string{"WORKER_ID": "w", "WORKER_PORT": "80a"}, wantErr: true},
		{name: "port out of range", env: map[string]string{"WORKER_ID": "w", "WORKER_PORT": "100000"}, wantErr: true},
		{name: "controller without port", env: map[string]string{"WORKER_ID": "w", "CONTROLLER_ADDRESS": "controller"}, wantErr: true},
		{name: "controller with scheme", env: map[string]string{"WORKER_ID": "w", "CONTROLLER_ADDRESS": "http://controller:8080"}, wantErr: true},
		{name: "controller bad port", env: map[string]string{"WORKER_ID": "w", "CONTROLLER_ADDRESS": "controller:0"}, wantErr: true},
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
