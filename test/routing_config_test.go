package test

import (
	"encoding/json"
	"reflect"
	"testing"

	"gyro"
	grpcadapter "gyro/adapters/grpc"
	redisadapter "gyro/adapters/redis"
	clientpkg "gyro/client"
)

func TestProtocolClientsShareRoutingDefaults(t *testing.T) {
	want := gyro.DefaultRoutingConfig()
	configs := []struct {
		name string
		got  gyro.RoutingConfig
	}{
		{name: "redis", got: redisadapter.DefaultClientConfig().RoutingConfig},
		{name: "grpc", got: grpcadapter.DefaultClientConfig().RoutingConfig},
		{name: "dynamic client", got: clientpkg.DefaultConfig().RoutingConfig},
	}
	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			if !reflect.DeepEqual(config.got, want) {
				t.Fatalf("routing config = %#v, want %#v", config.got, want)
			}
		})
	}
}

func TestRoutingConfigEmbeddingPreservesConfigurationShape(t *testing.T) {
	data, err := json.Marshal(redisadapter.DefaultClientConfig())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"locator", "health_checker", "connection"} {
		if _, ok := fields[field]; !ok {
			t.Fatalf("serialized config missing %q: %s", field, data)
		}
	}
}
