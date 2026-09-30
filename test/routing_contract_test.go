package test

import (
	"context"
	"testing"

	"gyro/gyro"
)

type contractSelector struct{}

func (contractSelector) Select(_ context.Context, request gyro.RouteRequest, snapshot gyro.TopologySnapshot) (gyro.CandidateSet, error) {
	if len(snapshot.Members) == 0 {
		return gyro.CandidateSet{}, gyro.ErrNoMembers
	}
	if request.Key == "" && len(request.Attributes) == 0 {
		return gyro.CandidateSet{}, gyro.ErrInvalidRequest
	}
	return gyro.CandidateSet{
		Revision: snapshot.Revision,
		Candidates: []gyro.Candidate{{
			MemberID: snapshot.Members[0].ID,
		}},
	}, nil
}

var _ gyro.Selector = contractSelector{}

func TestSelectorContractCarriesTopologyRevision(t *testing.T) {
	snapshot := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source", Generation: 7, Token: "token"},
		Members:  []gyro.Member{{ID: "member-a"}},
	}
	selection, err := (contractSelector{}).Select(context.Background(), gyro.RouteRequest{Key: "key"}, snapshot)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if selection.Revision != snapshot.Revision {
		t.Fatalf("selection revision = %#v, want %#v", selection.Revision, snapshot.Revision)
	}
	if len(selection.Candidates) != 1 || selection.Candidates[0].MemberID != "member-a" {
		t.Fatalf("unexpected candidates: %#v", selection.Candidates)
	}
}

func TestSelectorContractRejectsEmptyTopologyAndMissingRequest(t *testing.T) {
	selector := contractSelector{}
	if _, err := selector.Select(context.Background(), gyro.RouteRequest{Key: "key"}, gyro.TopologySnapshot{}); err != gyro.ErrNoMembers {
		t.Fatalf("empty topology error = %v, want ErrNoMembers", err)
	}
	snapshot := gyro.TopologySnapshot{Members: []gyro.Member{{ID: "member-a"}}}
	if _, err := selector.Select(context.Background(), gyro.RouteRequest{}, snapshot); err != gyro.ErrInvalidRequest {
		t.Fatalf("missing request error = %v, want ErrInvalidRequest", err)
	}
}
