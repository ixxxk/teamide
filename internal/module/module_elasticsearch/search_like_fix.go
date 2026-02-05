package module_elasticsearch

import (
	"context"
	"strings"

	"github.com/olivere/elastic/v7"
	"github.com/team-ide/go-tool/elasticsearch"
	"github.com/team-ide/go-tool/util"
)

func searchWithLikeCaseFix(service elasticsearch.IService, indexName string, pageIndex int, pageSize int, whereList []*elasticsearch.Where, orderList []*elasticsearch.Order) (res *elasticsearch.SearchResult, err error) {
	v7, ok := service.(*elasticsearch.V7Service)
	if !ok {
		return service.Search(indexName, pageIndex, pageSize, whereList, orderList)
	}

	res, err = searchV7WithLikeFix(v7, indexName, pageIndex, pageSize, whereList, orderList, true)
	if err != nil && isWildcardCaseInsensitiveUnsupported(err) {
		res, err = searchV7WithLikeFix(v7, indexName, pageIndex, pageSize, whereList, orderList, false)
	}
	return
}

func isWildcardCaseInsensitiveUnsupported(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if !strings.Contains(msg, "case_insensitive") {
		return false
	}
	return strings.Contains(msg, "unknown field") || strings.Contains(msg, "parsing_exception") || strings.Contains(msg, "x_content_parse_exception")
}

func searchV7WithLikeFix(service *elasticsearch.V7Service, indexName string, pageIndex int, pageSize int, whereList []*elasticsearch.Where, orderList []*elasticsearch.Order, useCaseInsensitive bool) (res *elasticsearch.SearchResult, err error) {
	client, err := service.GetClient()
	if err != nil {
		return
	}

	if pageIndex < 1 {
		pageIndex = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}

	doer := client.Search(indexName)
	query := elastic.NewBoolQuery()

	for _, where := range whereList {
		if where == nil || strings.TrimSpace(where.Name) == "" {
			continue
		}
		op := strings.ToLower(strings.TrimSpace(where.SqlConditionalOperation))
		var q elastic.Query
		var isNot bool

		switch op {
		case "like":
			q = buildLikeWildcardQuery(where.Name, where.Value, likeModeContains, useCaseInsensitive)
		case "not like":
			q = buildLikeWildcardQuery(where.Name, where.Value, likeModeContains, useCaseInsensitive)
			isNot = true
		case "like start":
			q = buildLikeWildcardQuery(where.Name, where.Value, likeModeStart, useCaseInsensitive)
		case "not like start":
			q = buildLikeWildcardQuery(where.Name, where.Value, likeModeStart, useCaseInsensitive)
			isNot = true
		case "like end":
			q = buildLikeWildcardQuery(where.Name, where.Value, likeModeEnd, useCaseInsensitive)
		case "not like end":
			q = buildLikeWildcardQuery(where.Name, where.Value, likeModeEnd, useCaseInsensitive)
			isNot = true
		case "between":
			q = elastic.NewRangeQuery(where.Name).Gte(where.Before).Lte(where.After)
		case "not between":
			q = elastic.NewRangeQuery(where.Name).Gte(where.Before).Lte(where.After)
			isNot = true
		case "in":
			q = elastic.NewTermsQuery(where.Name, splitTerms(where.Value)...)
		case "not in":
			q = elastic.NewTermsQuery(where.Name, splitTerms(where.Value)...)
			isNot = true
		default:
			q = elastic.NewTermQuery(where.Name, where.Value)
		}

		var addQ elastic.Query
		if strings.Contains(where.Name, ".") {
			addQ = elastic.NewNestedQuery(where.Name[0:strings.LastIndex(where.Name, ".")], q)
		} else {
			addQ = q
		}

		if isNot {
			query.MustNot(addQ)
		} else {
			query.Must(addQ)
		}
	}

	doer.Query(query)

	for _, one := range orderList {
		if one == nil || strings.TrimSpace(one.Name) == "" {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(one.AscDesc)) {
		case "ASC":
			doer.Sort(one.Name, true)
		default:
			doer.Sort(one.Name, false)
		}
	}

	doer.TrackTotalHits(true)

	searchResult, err := doer.Size(pageSize).From((pageIndex - 1) * pageSize).Do(context.Background())
	if err != nil {
		return
	}

	res = &elasticsearch.SearchResult{}
	if searchResult.Hits == nil {
		return
	}

	res.TotalHits = searchResult.Hits.TotalHits
	res.MaxScore = searchResult.Hits.MaxScore
	for _, one := range searchResult.Hits.Hits {
		data := &elasticsearch.HitData{
			Id:      one.Id,
			Type:    one.Type,
			Index:   one.Index,
			Uid:     one.Uid,
			Version: one.Version,
		}
		if one.Source != nil {
			data.Source, _ = util.ObjToJson(one.Source)
		}
		res.Hits = append(res.Hits, data)
	}

	return
}

type likeMode int

const (
	likeModeContains likeMode = iota
	likeModeStart
	likeModeEnd
)

func buildLikeWildcardQuery(field string, value string, mode likeMode, useCaseInsensitive bool) elastic.Query {
	if useCaseInsensitive {
		return elastic.NewWildcardQuery(field, buildLikeWildcardPattern(value, mode)).CaseInsensitive(true)
	}

	patterns := uniqueStrings([]string{
		buildLikeWildcardPattern(value, mode),
		buildLikeWildcardPattern(strings.ToLower(value), mode),
		buildLikeWildcardPattern(strings.ToUpper(value), mode),
	})

	if len(patterns) == 1 {
		return elastic.NewWildcardQuery(field, patterns[0])
	}

	b := elastic.NewBoolQuery().MinimumNumberShouldMatch(1)
	for _, p := range patterns {
		b.Should(elastic.NewWildcardQuery(field, p))
	}
	return b
}

func buildLikeWildcardPattern(value string, mode likeMode) string {
	switch mode {
	case likeModeStart:
		return value + "*"
	case likeModeEnd:
		return "*" + value
	default:
		return "*" + value + "*"
	}
}

func uniqueStrings(in []string) (out []string) {
	seen := map[string]struct{}{}
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return
}

func splitTerms(value string) (res []interface{}) {
	for _, one := range strings.Split(value, ",") {
		one = strings.TrimSpace(one)
		if one == "" {
			continue
		}
		res = append(res, one)
	}
	return
}
