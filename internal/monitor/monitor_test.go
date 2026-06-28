package monitor

import "testing"

func TestSnapshotReturnsOneRowPerRequest(t *testing.T) {
	ResetForTest()
	span := Start(RequestInfo{
		Source:      "user@example.com",
		ClientKey:   "primary-key",
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
	if row.ClientKey != "prim***-key" {
		t.Fatalf("client key = %q", row.ClientKey)
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

func TestSnapshotPagePaginatesNewestFirst(t *testing.T) {
	ResetForTest()
	for i := 0; i < 5; i++ {
		span := Start(RequestInfo{
			Source:      "user@example.com",
			ClientKey:   "key-alias",
			Model:       "model",
			InputTokens: int64(i + 1),
		})
		span.Finish(Result{Success: true, OutputTokens: 1})
	}

	snapshot := SnapshotPage(2, 2, false, false)
	if snapshot.Summary.Total != 5 || snapshot.Summary.Page != 2 || snapshot.Summary.PageSize != 2 || snapshot.Summary.Pages != 3 {
		t.Fatalf("summary = %#v", snapshot.Summary)
	}
	if len(snapshot.Rows) != 2 {
		t.Fatalf("rows len = %d", len(snapshot.Rows))
	}
	if snapshot.Rows[0].Usage.InputTokens != 3 || snapshot.Rows[1].Usage.InputTokens != 2 {
		t.Fatalf("page rows = %#v", snapshot.Rows)
	}
	if snapshot.Rows[0].ClientKey != "key-alias" {
		t.Fatalf("client key = %q", snapshot.Rows[0].ClientKey)
	}
}
