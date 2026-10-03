package test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"gyro"
)

func routerMembers() []gyro.Member {
	return []gyro.Member{
		{ID: "node-c", Endpoints: []gyro.Endpoint{{Address: "c:8080", Attributes: map[string]string{"zone": "c"}}}, Attributes: map[string]string{"role": "worker"}},
		{ID: "node-a", Endpoints: []gyro.Endpoint{{Address: "a:8080"}}},
		{ID: "node-b", Endpoints: []gyro.Endpoint{{Address: "b:8080"}}},
	}
}

func TestRouterRoutesAndOrdersCandidates(t *testing.T) {
	ctx := context.Background()
	router, err := gyro.NewRouter(routerMembers(), gyro.DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	const key = "routing-regression-key"
	primary, err := router.Route(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if primary.ID != "node-b" {
		t.Fatalf("primary = %s, want node-b", primary.ID)
	}
	candidates, err := router.Candidates(ctx, key, 10)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{candidates[0].ID, candidates[1].ID, candidates[2].ID}
	if want := []string{"node-b", "node-c", "node-a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("candidates = %v, want %v", got, want)
	}
	if candidates[0].ID != primary.ID {
		t.Fatal("first candidate differs from Route")
	}

	reordered := routerMembers()
	reordered[0], reordered[2] = reordered[2], reordered[0]
	if err := router.ReplaceMembers(ctx, reordered); err != nil {
		t.Fatal(err)
	}
	again, err := router.Route(ctx, key)
	if err != nil || again.ID != primary.ID {
		t.Fatalf("route after input reorder = %v, %v; want %s", again, err, primary.ID)
	}
}

func TestRouterValidatesInputAndPreservesLastSnapshot(t *testing.T) {
	ctx := context.Background()
	if _, err := gyro.NewRouter(nil, gyro.DefaultLocatorConfig()); !errors.Is(err, gyro.ErrNoMembers) {
		t.Fatalf("empty constructor error = %v", err)
	}
	if _, err := gyro.NewRouter([]gyro.Member{{ID: "a"}, {ID: "a"}}, gyro.DefaultLocatorConfig()); !errors.Is(err, gyro.ErrInvalidSnapshot) {
		t.Fatalf("duplicate constructor error = %v", err)
	}
	config := gyro.DefaultLocatorConfig()
	config.HashFunction = "unknown"
	if _, err := gyro.NewRouter(routerMembers(), config); err == nil {
		t.Fatal("invalid hash function accepted")
	}
	router, err := gyro.NewRouter(routerMembers(), gyro.DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct {
		ctx   context.Context
		key   string
		count int
		want  error
	}{
		{nil, "key", 1, gyro.ErrNilContext},
		{ctx, "", 1, gyro.ErrInvalidRequest},
		{ctx, "key", 0, gyro.ErrInvalidRequest},
	} {
		if _, err := router.Candidates(input.ctx, input.key, input.count); !errors.Is(err, input.want) {
			t.Fatalf("Candidates(%v, %q, %d) error = %v, want %v", input.ctx, input.key, input.count, err, input.want)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := router.Route(canceled, "key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Route error = %v", err)
	}
	if err := router.ReplaceMembers(ctx, []gyro.Member{{ID: "a"}, {ID: "a"}}); !errors.Is(err, gyro.ErrInvalidSnapshot) {
		t.Fatalf("duplicate replacement error = %v", err)
	}
	if err := router.ReplaceMembers(canceled, []gyro.Member{{ID: "new"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled replacement error = %v", err)
	}
	if member, err := router.Route(ctx, "key"); err != nil || member.ID == "new" {
		t.Fatalf("failed replacement changed route: %v, %v", member, err)
	}
	if err := router.ReplaceMembers(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Route(ctx, "key"); !errors.Is(err, gyro.ErrNoMembers) {
		t.Fatalf("empty replacement route error = %v", err)
	}
}

func TestRouterDetachesMemberDataAndPublishesWholeSnapshots(t *testing.T) {
	ctx := context.Background()
	members := routerMembers()
	router, err := gyro.NewRouter(members, gyro.DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	members[0].ID = "changed"
	members[0].Attributes["role"] = "changed"
	members[0].Endpoints[0].Address = "changed"
	members[0].Endpoints[0].Attributes["zone"] = "changed"
	candidates, err := router.Candidates(ctx, "routing-regression-key", 3)
	if err != nil {
		t.Fatal(err)
	}
	if candidates[1].ID != "node-c" || candidates[1].Attributes["role"] != "worker" || candidates[1].Endpoints[0].Address != "c:8080" || candidates[1].Endpoints[0].Attributes["zone"] != "c" {
		t.Fatalf("router retained mutable input: %#v", candidates[1])
	}
	candidates[1].Attributes["role"] = "changed"
	candidates[1].Endpoints[0].Attributes["zone"] = "changed"
	candidates[1].Endpoints[0].Address = "changed"
	again, err := router.Candidates(ctx, "routing-regression-key", 3)
	if err != nil {
		t.Fatal(err)
	}
	if again[1].Attributes["role"] != "worker" || again[1].Endpoints[0].Attributes["zone"] != "c" || again[1].Endpoints[0].Address != "c:8080" {
		t.Fatalf("router exposed mutable output: %#v", again[1])
	}
	updated := routerMembers()
	updated[0].Endpoints[0].Address = "new-c:8080"
	if err := router.ReplaceMembers(ctx, updated); err != nil {
		t.Fatal(err)
	}
	updated[0].Endpoints[0].Address = "changed-again"
	again, err = router.Candidates(ctx, "routing-regression-key", 3)
	if err != nil || again[0].ID != "node-b" || again[1].Endpoints[0].Address != "new-c:8080" {
		t.Fatalf("metadata replacement changed placement or retained input: %v, %v", again, err)
	}

	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 50; j++ {
				selection, err := router.Candidates(ctx, "routing-regression-key", 3)
				if err != nil {
					t.Errorf("concurrent Candidates: %v", err)
					continue
				}
				if len(selection) == 1 && selection[0].ID == "new" {
					continue
				}
				if len(selection) != 3 || selection[0].ID != "node-b" || selection[1].ID != "node-c" || selection[2].ID != "node-a" {
					t.Errorf("partial membership snapshot: %v", selection)
				}
			}
		}()
	}
	if err := router.ReplaceMembers(ctx, []gyro.Member{{ID: "new", Endpoints: []gyro.Endpoint{{Address: "new:8080"}}}}); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	member, err := router.Route(ctx, "routing-regression-key")
	if err != nil || member.ID != "new" {
		t.Fatalf("replacement route = %v, %v", member, err)
	}
}
