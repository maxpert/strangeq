package management

import (
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// listQuery is the parsed, validated §6.5 list-query surface.
type listQuery struct {
	presentPage bool
	page        int
	pageSize    int

	name     string
	nameSet  bool
	useRegex bool

	sortField   string
	sortReverse bool

	columns []string
}

// parseListQuery validates the shared pagination/filter/sort/projection
// parameters. Unknown parameters are ignored (FR-CONV-051). `page` present
// selects the envelope form; absent selects the bare-array form (FR-CONV-052).
func parseListQuery(r *http.Request, maxPageSize int) (*listQuery, *apiError) {
	q := r.URL.Query()
	out := &listQuery{pageSize: 100}

	if raw, ok := firstPresent(q, "page"); ok {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return nil, errBad("page must be a positive integer")
		}
		out.presentPage = true
		out.page = n
	}

	if raw, ok := firstPresent(q, "page_size"); ok {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return nil, errBad("page_size must be a positive integer")
		}
		if n > maxPageSize {
			return nil, errBad("page_size exceeds management.limits.max_page_size (" + strconv.Itoa(maxPageSize) + ")")
		}
		out.pageSize = n
	}

	raw, hasName := firstPresent(q, "name")
	if !hasName {
		raw, hasName = firstPresent(q, "filter")
	}
	if hasName {
		out.name = raw
		out.nameSet = true
	}
	if raw, ok := firstPresent(q, "use_regex"); ok {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errBad("use_regex must be true or false")
		}
		out.useRegex = b
		if b && out.nameSet {
			if _, err := regexp.Compile(out.name); err != nil {
				return nil, errBad("invalid name regexp: " + err.Error())
			}
		}
	}

	if raw, ok := firstPresent(q, "sort"); ok && raw != "" {
		out.sortField = raw
	}
	if raw, ok := firstPresent(q, "sort_reverse"); ok {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errBad("sort_reverse must be true or false")
		}
		out.sortReverse = b
	}

	if raw, ok := firstPresent(q, "columns"); ok && raw != "" {
		for _, c := range strings.Split(raw, ",") {
			c = strings.TrimSpace(c)
			if c != "" {
				out.columns = append(out.columns, c)
			}
		}
	}
	return out, nil
}

func firstPresent(q url.Values, key string) (string, bool) {
	v, ok := q[key]
	if !ok || len(v) == 0 {
		return "", false
	}
	return v[0], true
}

// nameMatcher builds the case-insensitive substring / regexp filter.
func (q *listQuery) nameMatcher() func(string) bool {
	if !q.nameSet {
		return func(string) bool { return true }
	}
	if q.useRegex {
		re := regexp.MustCompile(q.name)
		return func(s string) bool { return re.MatchString(s) }
	}
	needle := strings.ToLower(q.name)
	return func(s string) bool { return strings.Contains(strings.ToLower(s), needle) }
}

// applyList filters by name, sorts (with a stable name tie-break), and projects
// the requested columns. items are the pre-filtered endpoint items; nameOf
// yields each item's filterable name.
//
// sortable is asked whether a sort field is valid for this endpoint; it must
// reject the special `message_stats` field with a 400 (BOB-2 unresolved,
// FR-CONV-054) and any unknown field.
func applyList(items []any, q *listQuery, nameOf func(any) string, sortable func(string) bool) ([]any, *apiError) {
	matcher := q.nameMatcher()
	filtered := make([]any, 0, len(items))
	for _, it := range items {
		if matcher(nameOf(it)) {
			filtered = append(filtered, it)
		}
	}

	if q.sortField != "" {
		if !sortable(q.sortField) {
			if q.sortField == "message_stats" || strings.HasPrefix(q.sortField, "message_stats.") {
				return nil, errBad("sorting by message_stats is not available: per-queue message rates are not implemented")
			}
			return nil, errBad("unknown sort field " + q.sortField)
		}
		var sortErr *apiError
		sort.SliceStable(filtered, func(i, j int) bool {
			ai, ok1 := resolvePath(filtered[i], q.sortField)
			bj, ok2 := resolvePath(filtered[j], q.sortField)
			if !ok1 || !ok2 {
				sortErr = errBad("unknown sort field " + q.sortField)
				return false
			}
			c := compareValues(ai, bj)
			if c == 0 {
				c = compareValues(nameOf(filtered[i]), nameOf(filtered[j]))
			}
			if q.sortReverse {
				return c > 0
			}
			return c < 0
		})
		if sortErr != nil {
			return nil, sortErr
		}
	}

	out := filtered
	if len(q.columns) > 0 {
		projected := make([]any, 0, len(filtered))
		for _, it := range filtered {
			pv, err := projectValue(it, q.columns)
			if err != nil {
				return nil, errBad("unknown column in columns=: " + err.Error())
			}
			projected = append(projected, pv)
		}
		out = projected
	}
	return out, nil
}

// projectionFieldsValid rejects a columns= selection naming a field that does
// not exist on the item type. It is a best-effort check against the concrete
// item type.
func projectionFieldsValid(item any, cols []string) *apiError {
	for _, c := range cols {
		if _, ok := resolvePath(item, c); !ok {
			return errBad("unknown column " + c)
		}
	}
	return nil
}

// pageEnvelope is the paginated list response shape (FR-CONV-052).
type pageEnvelope struct {
	Items         any `json:"items"`
	TotalCount    int `json:"total_count"`
	ItemCount     int `json:"item_count"`
	FilteredCount int `json:"filtered_count"`
	Page          int `json:"page"`
	PageSize      int `json:"page_size"`
	PageCount     int `json:"page_count"`
}

// pageWindow returns the items for the requested page and the page count.
func pageWindow(items []any, q *listQuery) ([]any, int) {
	total := len(items)
	pageSize := q.pageSize
	pageCount := (total + pageSize - 1) / pageSize
	if pageCount == 0 {
		pageCount = 1
	}
	start := (q.page - 1) * pageSize
	if start >= total {
		return []any{}, pageCount
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return items[start:end], pageCount
}

// linkHeader builds the RFC-5988 Link header for a paginated response
// (FR-CONV-053). page_count is clamped to at least 1.
func linkHeader(r *http.Request, q *listQuery, pageCount int) string {
	var parts []string
	base := r.URL.Path
	build := func(p int) string {
		u := url.URL{Path: base}
		vals := r.URL.Query()
		vals.Set("page", strconv.Itoa(p))
		u.RawQuery = vals.Encode()
		return "<" + u.String() + ">"
	}
	if q.page > 1 {
		parts = append(parts, build(q.page-1)+`; rel="prev"`)
	}
	if q.page < pageCount {
		parts = append(parts, build(q.page+1)+`; rel="next"`)
	}
	return strings.Join(parts, ", ")
}

// listEnvelopeWithTotals adds the optional top-level queue_totals to a
// paginated envelope (FR-CONV-056, enable_queue_totals=true).
type listEnvelopeWithTotals struct {
	pageEnvelope
	QueueTotals queueTotalsJSON `json:"queue_totals"`
}

type bareListWithTotals struct {
	Items       any             `json:"items"`
	QueueTotals queueTotalsJSON `json:"queue_totals"`
}

// writeList renders a list response: `page` present ⇒ the §6.5 envelope,
// absent ⇒ a bare array (FR-CONV-052). It validates projection columns and the
// sort field, returning 400 on an unknown one.
func (s *Server) writeList(w http.ResponseWriter, r *http.Request, items []any, sample any, q *listQuery, nameOf func(any) string, sortable func(string) bool) {
	s.writeListWithTotals(w, r, items, sample, q, nameOf, sortable, false)
}

func (s *Server) writeListWithTotals(w http.ResponseWriter, r *http.Request, items []any, sample any, q *listQuery, nameOf func(any) string, sortable func(string) bool, includeQueueTotals bool) {
	if len(q.columns) > 0 {
		if ae := projectionFieldsValid(sample, q.columns); ae != nil {
			s.writeAPIError(w, r, ae)
			return
		}
	}
	filtered, aerr := applyList(items, q, nameOf, sortable)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}

	if !q.presentPage {
		if includeQueueTotals {
			qt, _ := s.queueTotals()
			s.writeJSON(w, r, http.StatusOK, bareListWithTotals{
				Items:       filtered,
				QueueTotals: toQueueTotalsJSON(qt),
			})
			return
		}
		s.writeJSON(w, r, http.StatusOK, filtered)
		return
	}

	pageItems, pageCount := pageWindow(filtered, q)
	if link := linkHeader(r, q, pageCount); link != "" {
		w.Header().Set("Link", link)
	}
	env := pageEnvelope{
		Items:         pageItems,
		TotalCount:    len(items),
		ItemCount:     len(pageItems),
		FilteredCount: len(filtered),
		Page:          q.page,
		PageSize:      q.pageSize,
		PageCount:     pageCount,
	}
	if includeQueueTotals {
		qt, _ := s.queueTotals()
		s.writeJSON(w, r, http.StatusOK, listEnvelopeWithTotals{pageEnvelope: env, QueueTotals: toQueueTotalsJSON(qt)})
		return
	}
	s.writeJSON(w, r, http.StatusOK, env)
}

func toQueueTotalsJSON(qt queueTotalsResult) queueTotalsJSON {
	return queueTotalsJSON{
		MessagesReady:      qt.messagesReady,
		MessagesUnacked:    qt.messagesUnacked,
		Messages:           qt.messages,
		QueuesWithMessages: qt.queuesWithMsgs,
	}
}
