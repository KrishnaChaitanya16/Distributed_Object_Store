package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Getenv matches os.Getenv so tests can inject a fake environment.
type Getenv func(key string) string

func Str(getenv Getenv, key, def string) string {
	if v := strings.TrimSpace(getenv(key)); v != "" {
		return v
	}
	return def
}

func Required(getenv Getenv, key string) (string, error) {
	v := strings.TrimSpace(getenv(key))
	if v == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return v, nil
}

func Int(getenv Getenv, key string, def int) (int, error) {
	v := strings.TrimSpace(getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not an integer", key, v)
	}
	return n, nil
}

func PositiveInt(getenv Getenv, key string, def int) (int, error) {
	n, err := Int(getenv, key, def)
	if err != nil {
		return 0, err
	}
	if n < 1 {
		return 0, fmt.Errorf("%s=%d must be >= 1", key, n)
	}
	return n, nil
}

func NonNegativeInt(getenv Getenv, key string, def int) (int, error) {
	n, err := Int(getenv, key, def)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("%s=%d must be >= 0", key, n)
	}
	return n, nil
}

// Bool reads true/false, 1/0, yes/no or on/off in any case; an unset or blank value gives def.
func Bool(getenv Getenv, key string, def bool) (bool, error) {
	v := strings.ToLower(strings.TrimSpace(getenv(key)))
	switch v {
	case "":
		return def, nil
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("%s=%q is not a boolean (use true or false)", key, v)
}

// OptionalPort is like Port but 0 is allowed and means "off".
func OptionalPort(getenv Getenv, key string, def int) (int, error) {
	n, err := Int(getenv, key, def)
	if err != nil {
		return 0, err
	}
	if n < 0 || n > 65535 {
		return 0, fmt.Errorf("%s=%d is outside 0-65535 (0 turns it off)", key, n)
	}
	return n, nil
}

func Port(getenv Getenv, key string, def int) (int, error) {
	n, err := Int(getenv, key, def)
	if err != nil {
		return 0, err
	}
	if n < 1 || n > 65535 {
		return 0, fmt.Errorf("%s=%d is outside 1-65535", key, n)
	}
	return n, nil
}

// HostPort returns the value only if it is a well-formed "host:port" with a valid port.
func HostPort(getenv Getenv, key, def string) (string, error) {
	v := Str(getenv, key, def)
	host, portStr, err := net.SplitHostPort(v)
	if err != nil || host == "" {
		return "", fmt.Errorf("%s=%q must be host:port", key, v)
	}
	p, err := strconv.Atoi(portStr)
	if err != nil || p < 1 || p > 65535 {
		return "", fmt.Errorf("%s=%q has an invalid port", key, v)
	}
	return v, nil
}
