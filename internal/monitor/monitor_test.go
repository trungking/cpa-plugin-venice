package monitor

import "testing"

func TestSnapshotReturnsOneRowPerRequest(t *testing.T) {
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
	span = Start(RequestInfo{
		Source:      "user@example.com",
		Model:       "zai-org-glm-5.2",
		Effort:      "high",
		Tier:        "default",
		InputTokens: 4,
	})
	span.Finish(Result{Success: false, Error: "boom", TotalTokens: 4})

	snapshot := SnapshotRows(500, false, true)
	if snapshot.Summary.Rows != 2 || snapshot.Summary.Accounts != 1 || snapshot.Summary.Failures != 1 {
		t.Fatalf("summary = %#v", snapshot.Summary)
	}
	if len(snapshot.Rows) != 2 {
		t.Fatalf("rows len = %d", len(snapshot.Rows))
	}
	var row Row
	for _, candidate := range snapshot.Rows {
		if candidate.Status == "Success" {
			row = candidate
			break
		}
	}
	if row.Source != "use***@example.com" || row.ID == "" {
		t.Fatalf("success row = %#v", row)
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
	if snapshot.Rows[0].Status != "Failed" || snapshot.Rows[0].Error != "boom" {
		t.Fatalf("newest row = %#v", snapshot.Rows[0])
	}
}
