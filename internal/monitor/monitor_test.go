package monitor

import "testing"

func TestSnapshotAggregatesRealtimeRows(t *testing.T) {
	ResetForTest()
	span := Start(RequestInfo{
		Source:      "user@example.com",
		Model:       "zai-org-glm-5.2",
		Effort:      "high",
		Tier:        "default",
		InputTokens: 12,
	})
	span.MarkTTFT()
	span.Finish(Result{Success: true, OutputTokens: 8, TotalTokens: 20})

	snapshot := SnapshotRows(500, false, true)
	if snapshot.Summary.Rows != 1 || snapshot.Summary.Accounts != 1 {
		t.Fatalf("summary = %#v", snapshot.Summary)
	}
	if len(snapshot.Rows) != 1 {
		t.Fatalf("rows len = %d", len(snapshot.Rows))
	}
	row := snapshot.Rows[0]
	if row.Source != "use***@example.com" {
		t.Fatalf("masked source = %q", row.Source)
	}
	if row.Calls != 1 || row.Success != 1 || row.Failed != 0 {
		t.Fatalf("counts = %#v", row)
	}
	if row.Usage.TotalTokens != 20 || row.Usage.InputTokens != 12 || row.Usage.OutputTokens != 8 {
		t.Fatalf("usage = %#v", row.Usage)
	}
	if row.SuccessRate != 100 {
		t.Fatalf("success rate = %f", row.SuccessRate)
	}
}
