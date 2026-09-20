package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
)

type DeviceConfig struct {
	ID           string   `json:"id"`
	Format       string   `json:"format"`
	Capabilities []string `json:"capabilities"`
}
type ConnectionConfig struct {
	ID          string         `json:"id"`
	Host        string         `json:"host"`
	Port        int            `json:"port"`
	UsernameEnv string         `json:"usernameEnv"`
	PasswordEnv string         `json:"passwordEnv"`
	Devices     []DeviceConfig `json:"devices"`
}
type Config struct {
	Connections               []ConnectionConfig `json:"connections"`
	RPCTimeoutMs              int                `json:"rpcTimeoutMs"`
	NotificationMaxAgeSeconds int64              `json:"notificationMaxAgeSeconds"`
	MaxInboxRows              int                `json:"maxInboxRows"`
	RetentionDays             int                `json:"retentionDays"`
	MaxEvidenceBytes          int64              `json:"maxEvidenceBytes"`
}

func DefaultConfig() Config {
	return Config{RPCTimeoutMs: 10000, NotificationMaxAgeSeconds: 300, MaxInboxRows: 10000, RetentionDays: 7, MaxEvidenceBytes: 268435456}
}
func LoadConfig(path string) (Config, error) {
	c := DefaultConfig()
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return c, errors.New("config must contain one JSON object")
	}
	return c, c.Validate()
}

var validID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var validEnv = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (c Config) Validate() error {
	if c.RPCTimeoutMs < 100 || c.RPCTimeoutMs > 60000 || c.MaxInboxRows < 1 || c.RetentionDays < 1 || c.MaxEvidenceBytes < 1 || c.NotificationMaxAgeSeconds < 0 {
		return errors.New("invalid service bounds")
	}
	seen := map[string]bool{}
	for _, conn := range c.Connections {
		if !validID.MatchString(conn.ID) || seen[conn.ID] || conn.Host == "" || conn.Port < 1 || conn.Port > 65535 {
			return errors.New("invalid or duplicate connection")
		}
		seen[conn.ID] = true
		for _, env := range []string{conn.UsernameEnv, conn.PasswordEnv} {
			if env != "" && !validEnv.MatchString(env) {
				return errors.New("invalid credential environment variable")
			}
		}
		devices := map[string]bool{}
		for _, d := range conn.Devices {
			if !validID.MatchString(d.ID) || devices[d.ID] || (d.Format != "json" && d.Format != "protobuf") {
				return fmt.Errorf("invalid or duplicate device in %s", conn.ID)
			}
			devices[d.ID] = true
		}
	}
	return nil
}
func ValidateBind(host, token string) error {
	ip := net.ParseIP(host)
	if token == "" && (ip == nil || !ip.IsLoopback()) {
		return errors.New("non-loopback bind requires VDM_API_TOKEN; use literal 127.0.0.1 or ::1 for unauthenticated local access")
	}
	return nil
}
