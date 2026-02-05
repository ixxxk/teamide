package module_elasticsearch

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildLikeWildcardQuery_CaseInsensitive(t *testing.T) {
	q := buildLikeQuery("mem_name", "CQTany", likeModeContains, true)
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	for _, want := range []string{
		`"wildcard":{"mem_name":{"case_insensitive":true,"value":"*CQTany*"}}`,
		`"wildcard":{"mem_name.keyword":{"case_insensitive":true,"value":"*CQTany*"}}`,
		`"match_phrase":{"mem_name":{"query":"CQTany"}}`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %s in %s", want, got)
		}
	}
}

func TestBuildLikeWildcardQuery_FallbackVariants(t *testing.T) {
	q := buildLikeQuery("mem_name", "CQTany", likeModeContains, false)
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
	if !strings.Contains(got, "mem_name.keyword") {
		t.Fatalf("expected mem_name.keyword in %s", got)
	}
	for _, want := range []string{`"*CQTany*"`, `"*cqtany*"`, `"*CQTANY*"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %s in %s", want, got)
		}
	}
}
