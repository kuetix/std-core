package transitions

import (
	"math"
	"strconv"

	"github.com/kuetix/engine/engine/domain"
	"github.com/kuetix/engine/engine/domain/interfaces"
	"github.com/kuetix/engine/engine/workflow"
	"github.com/kuetix/helpers"
)

type paginationTransitions struct {
	workflow.BaseServiceTransition
}

func NewPaginationTransitions() interfaces.ServiceTransitions {
	return &paginationTransitions{}
}

//goland:noinspection GoUnusedParameter
func (pt *paginationTransitions) PaginationTotal(pagination *domain.Pagination, total int) (r domain.FlowStepResult) {
	if pagination == nil {
		pagination = &domain.Pagination{
			Page:             1,
			PageSize:         999999999,
			TotalPage:        0,
			TotalRecords:     0,
			CurrentPageTotal: 0,
		}
	}
	pagination.TotalRecords = total
	pagination.TotalPage = int(math.Ceil(float64(total) / float64(pagination.PageSize)))

	r.Success = true
	return
}

//goland:noinspection GoUnusedParameter
func (pt *paginationTransitions) CurrentPageTotal(pagination *domain.Pagination, entities []interface{}) (r domain.FlowStepResult) {
	pagination.CurrentPageTotal = helpers.Len(entities)

	r.Success = true
	return
}

// ResolvePageParams parses raw offset/limit query-string values (always
// strings coming off $qs.offset/$qs.limit - WSL action arguments only bind
// bare $dotted.path references, with no confirmed string->int coercion
// inline, so every list workflow needs this Go-side parse rather than
// doing it in WSL) into usable ints, with sane defaults/clamps: an empty
// or invalid offset becomes 0, an empty/invalid/non-positive limit becomes
// 20 (this project's standard page size), and limit is capped at 100 so a
// caller can't force an unbounded Redis range read. stop is precomputed
// (offset+limit-1) since WSL has no arithmetic operators either - it's
// meant to be fed directly into redis/list.LRange's stop argument.
func (pt *paginationTransitions) ResolvePageParams(offsetParam, limitParam string) (r domain.FlowStepResult) {
	offset, err := strconv.Atoi(offsetParam)
	if err != nil || offset < 0 {
		offset = 0
	}
	limit, err := strconv.Atoi(limitParam)
	if err != nil || limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	r.Success = true
	r.Response = map[string]interface{}{
		"offset": offset,
		"limit":  limit,
		"stop":   offset + limit - 1,
	}
	return
}

// BuildPaginationMeta builds the {pagination: {...}} object every
// paginated list endpoint's response nests under its "meta" key. hasMore
// is computed here rather than in WSL for the same "no arithmetic/
// comparison operators" reason as ResolvePageParams' stop field.
//
// total is interface{}, and deliberately meant to be passed the WHOLE
// alias from a count action (e.g. `total: $total` after
// `action redis/list.LLen(...) as total`), not a dotted sub-field
// reference like `$total.length` - confirmed by testing that dotted-path
// field access silently fails to resolve when used as an action argument
// (unlike inside a response.Response(value: {...}) literal, where it
// works fine): passing `total: $total.length` bound the ENTIRE
// {"key":...,"length":35} map to this parameter instead of just 35. So
// this accepts either a bare number (or numeric string) or a map
// containing a "length"/"total"/"count" key (matching every count-style
// action's response shape used so far - redis/list.LLen's "length", this
// project's SearchTotal's "total") and unwraps whichever it finds.
func (pt *paginationTransitions) BuildPaginationMeta(offset, limit int, total interface{}) (r domain.FlowStepResult) {
	totalInt := toInt(total)
	r.Success = true
	r.Response = map[string]interface{}{
		"pagination": map[string]interface{}{
			"offset":  offset,
			"limit":   limit,
			"total":   totalInt,
			"hasMore": offset+limit < totalInt,
		},
	}
	return
}

func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case int32:
		return int(n)
	case float64:
		return int(n)
	case float32:
		return int(n)
	case string:
		parsed, err := strconv.Atoi(n)
		if err != nil {
			return 0
		}
		return parsed
	case map[string]interface{}:
		for _, key := range []string{"length", "total", "count"} {
			if inner, ok := n[key]; ok {
				return toInt(inner)
			}
		}
		return 0
	default:
		return 0
	}
}
