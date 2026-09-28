package gyro

import "time"

// ConnectionConfig contains protocol-neutral connection limits and timeouts.
// Protocol adapters interpret these values when constructing native clients.
type ConnectionConfig struct {
	MaxIdleConns   int           `json:"max_idle_conns"`
	MaxActiveConns int           `json:"max_active_conns"`
	IdleTimeout    time.Duration `json:"idle_timeout"`
	ConnectTimeout time.Duration `json:"connect_timeout"`
	ReadTimeout    time.Duration `json:"read_timeout"`
	WriteTimeout   time.Duration `json:"write_timeout"`
}
