# Internal Routing

This package owns node membership and combines the pure selector, failure
policy, and health observations into route decisions. It acquires resource
leases while the selected locator is stable. `internal/health` owns health
state, and `internal/resource` is the only owner of node closure.

`internal/routed` builds this runtime for protocol adapters. The dynamic
client uses the same routing components directly.
