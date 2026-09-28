package test

import (
	"context"
	"errors"
	"testing"

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/internal/policy"
)

type testHealthView map[string]gyro.HealthStatus

func (h testHealthView) Status(memberID string) gyro.HealthStatus {
	status, ok := h[memberID]
	if !ok {
		return gyro.Unknown
	}
	return status
}

func (h testHealthView) Snapshot() map[string]gyro.HealthStatus {
	copy := make(map[string]gyro.HealthStatus, len(h))
	for id, status := range h {
		copy[id] = status
	}
	return copy
}

func policySnapshot() gyro.TopologySnapshot {
	return gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "policy", Generation: 2, Token: "2"},
		Members: []gyro.Member{
			{ID: "a", Endpoints: []gyro.Endpoint{{Address: "a"}}},
			{ID: "b", Endpoints: []gyro.Endpoint{{Address: "b"}}},
			{ID: "c", Endpoints: []gyro.Endpoint{{Address: "c"}}},
		},
	}
}

func policySelection(snapshot gyro.TopologySnapshot) gyro.CandidateSet {
	return gyro.CandidateSet{
		Revision: snapshot.Revision,
		Candidates: []gyro.Candidate{
			{MemberID: "a"}, {MemberID: "b"}, {MemberID: "c"},
		},
	}
}

func TestPrimaryOnlyDoesNotFailOverAndPreservesCandidates(t *testing.T) {
	snapshot := policySnapshot()
	selection := policySelection(snapshot)
	decision, err := (policy.PrimaryOnly{}).Decide(
		context.Background(), gyro.RouteRequest{Key: "key"}, snapshot, selection,
		testHealthView{"a": gyro.Healthy, "b": gyro.Unhealthy, "c": gyro.Healthy},
	)
	if err != nil {
		t.Fatalf("PrimaryOnly failed: %v", err)
	}
	if decision.Primary.ID != "a" || len(decision.Candidates) != 2 || decision.Candidates[0].ID != "b" {
		t.Fatalf("unexpected decision: %#v", decision)
	}
	if decision.Policy != "primary-only" || decision.Revision != snapshot.Revision {
		t.Fatalf("decision metadata = %#v", decision)
	}
}

func TestPrimaryOnlyRejectsUnhealthyOrUnknownPrimary(t *testing.T) {
	snapshot := policySnapshot()
	selection := policySelection(snapshot)
	for _, status := range []gyro.HealthStatus{gyro.Unknown, gyro.Unhealthy} {
		_, err := (policy.PrimaryOnly{}).Decide(context.Background(), gyro.RouteRequest{Key: "key"}, snapshot, selection, testHealthView{"a": status})
		if !errors.Is(err, gyro.ErrFailoverNotAllowed) {
			t.Fatalf("status %v error = %v, want ErrFailoverNotAllowed", status, err)
		}
	}
}

func TestHealthyCandidateSelectsFirstEligibleAndControlsUnknown(t *testing.T) {
	snapshot := policySnapshot()
	selection := policySelection(snapshot)
	health := testHealthView{"a": gyro.Unhealthy, "b": gyro.Healthy, "c": gyro.Healthy}
	decision, err := (policy.HealthyCandidate{}).Decide(context.Background(), gyro.RouteRequest{Key: "key"}, snapshot, selection, health)
	if err != nil {
		t.Fatalf("HealthyCandidate failed: %v", err)
	}
	if decision.Primary.ID != "b" || len(decision.Candidates) != 2 || decision.Candidates[0].ID != "a" || decision.Candidates[1].ID != "c" {
		t.Fatalf("unexpected failover decision: %#v", decision)
	}
	if _, err := (policy.HealthyCandidate{}).Decide(context.Background(), gyro.RouteRequest{Key: "key"}, snapshot, selection, testHealthView{}); !errors.Is(err, gyro.ErrNoEligibleCandidate) {
		t.Fatalf("unknown-only error = %v, want ErrNoEligibleCandidate", err)
	}
	decision, err = (policy.HealthyCandidate{AllowUnknown: true}).Decide(context.Background(), gyro.RouteRequest{Key: "key"}, snapshot, selection, testHealthView{})
	if err != nil || decision.Primary.ID != "a" {
		t.Fatalf("AllowUnknown decision = %#v, error = %v", decision, err)
	}
}

func TestFailurePolicyRejectsMismatchedSelectionAndCanceledRequest(t *testing.T) {
	snapshot := policySnapshot()
	selection := policySelection(snapshot)
	selection.Revision.Generation++
	_, err := (policy.PrimaryOnly{}).Decide(context.Background(), gyro.RouteRequest{Key: "key"}, snapshot, selection, testHealthView{"a": gyro.Healthy})
	if !errors.Is(err, gyro.ErrSelectionMismatch) {
		t.Fatalf("mismatched selection error = %v, want ErrSelectionMismatch", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	selection = policySelection(snapshot)
	_, err = (policy.HealthyCandidate{}).Decide(canceled, gyro.RouteRequest{Key: "key"}, snapshot, selection, testHealthView{"a": gyro.Healthy})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v, want context.Canceled", err)
	}
}
