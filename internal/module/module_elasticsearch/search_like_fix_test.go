package module_elasticsearch

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildLikeWildcardQuery_CaseInsensitive(t *testing.T) {
	q := buildLikeWildcardQuery("mem_name", "CQTany", likeModeContains, true)
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	expected := `{"wildcard":{"mem_name":{"case_insensitive":true,"value":"*CQTany*"}}}`
	if got != expected {
		t.Fatalf("expected %s, got %s", expected, got)
	}
}

func TestBuildLikeWildcardQuery_FallbackVariants(t *testing.T) {
	q := buildLikeWildcardQuery("mem_name", "CQTany", likeModeContains, false)
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if strings.Contains(got, "case_insensitive") {
		t.Fatalf("did not expect case_insensitive in %s", got)
	}
	for _, want := range []string{`"*CQTany*"`, `"*cqtany*"`, `"*CQTANY*"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %s in %s", want, got)
		}
	}
}
