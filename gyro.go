// Package gyro routes keys to stable members using consistent hashing.
// NewRouter is the direct entry point when callers need a member without
// protocol connections or health checks.
//
// With context, fmt, and gyro imported:
//
//	router, err := gyro.NewRouter([]gyro.Member{{ID: "node-a"}, {ID: "node-b"}}, gyro.DefaultLocatorConfig())
//	if err != nil {
//		panic(err)
//	}
//	member, err := router.Route(context.Background(), "user:123")
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println(member.ID)
//
// Fixed-address Redis and gRPC clients live in adapters/redis and
// adapters/grpc. Applications with dynamic discovery use the client package.
package gyro
