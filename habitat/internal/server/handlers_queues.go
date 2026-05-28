package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// ============================================================================
// Helpers
// ============================================================================

func isContextQueryFailure(err error, ctx context.Context) bool {
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return true
		}
	}
	ctxErr := ctx.Err()
	return errors.Is(ctxErr, context.DeadlineExceeded) || errors.Is(ctxErr, context.Canceled)
}

func writeTaskQueryContextError(w http.ResponseWriter, err error, ctx context.Context) {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		http.Error(w, "task query timed out", http.StatusGatewayTimeout)
		return
	}
	http.Error(w, "task query canceled", http.StatusServiceUnavailable)
}

// ============================================================================
// Queue Operations
// ============================================================================

const recentTaskNamesCacheTTL = time.Minute

func (s *Server) listRecentTaskNames(ctx context.Context, queueNames []string, recentRunLimit int) ([]string, error) {
	if len(queueNames) == 0 {
		return []string{}, nil
	}
	if recentRunLimit <= 0 {
		return []string{}, nil
	}

	namesByValue := make(map[string]struct{})
	for _, name := range queueNames {
		namesByValue[name] = struct{}{}
	}

	if cached, ok := s.recentTaskNamesCache.Load("recent_names"); ok {
		if cached, ok := cached.(recentTaskNamesCacheEntry); ok {
			if time.Since(cached.cachedAt) < recentTaskNamesCacheTTL {
				return cached.names, nil
			}
		}
	}

	batch := make([]string, 0, len(queueNames))
	rest := make([]string, 0)
	copy(batch, queueNames)
	if len(queueNames) > 1 {
		rest = queueNames[1:]
	}
	batchNames, err := s.getRecentQueueTaskNamesCached(ctx, batch[0], recentRunLimit)
	if err == nil {
		for _, n := range batchNames {
			namesByValue[n] = struct{}{}
		}
	}
	for _, q := range rest {
		names, err := s.getRecentQueueTaskNamesCached(ctx, q, recentRunLimit)
		if err != nil {
			log.Printf("handleTasks: failed to list recent names for queue %s: %v", q, err)
			continue
		}
		for _, n := range names {
			namesByValue[n] = struct{}{}
		}
	}

	result := make([]string, 0, len(namesByValue))
	for name := range namesByValue {
		result = append(result, name)
	}
	sort.Strings(result)

	s.recentTaskNamesCache.Store("recent_names", recentTaskNamesCacheEntry{
		names:    result,
		cachedAt: time.Now(),
	})
	return result, nil
}

func (s *Server) getRecentQueueTaskNamesCached(ctx context.Context, queueName string, recentRunLimit int) ([]string, error) {
	if recentRunLimit <= 0 {
		return []string{}, nil
	}

	cacheKey := fmt.Sprintf("%s:%d", queueName, recentRunLimit)
	if cached, ok := s.queueTaskNamesCache.Load(cacheKey); ok {
		if cached, ok := cached.(cachedQueueTaskNamesEntry); ok {
			if time.Since(cached.cachedAt) < recentTaskNamesCacheTTL {
				return cached.names, nil
			}
		}
	}

	names, err := s.fetchRecentQueueTaskNames(ctx, queueName, recentRunLimit)
	if err != nil {
		return nil, err
	}

	s.queueTaskNamesCache.Store(cacheKey, cachedQueueTaskNamesEntry{
		names:    names,
		cachedAt: time.Now(),
	})
	return names, nil
}

func (s *Server) fetchRecentQueueTaskNames(ctx context.Context, queueName string, recentRunLimit int) ([]string, error) {
	ttable := queueTableIdentifier("t", queueName)
	rtable := queueTableIdentifier("r", queueName)

	query := fmt.Sprintf(`
		SELECT DISTINCT t.task_name
		FROM absurd.%s r
		JOIN absurd.%s t ON t.task_id = r.task_id
		WHERE t.task_name IS NOT NULL
		ORDER BY MAX(r.created_at) DESC
		LIMIT $1
	`, rtable, ttable)

	rows, err := s.db.QueryContext(ctx, query, recentRunLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil && name != "" {
			names = append(names, name)
		}
	}
	return names, rows.Err()
}

func (s *Server) fetchQueueTaskCandidates(
	ctx context.Context,
	queueName string,
	statusFilter string,
	taskNameFilter string,
	taskIDFilter string,
	limit int,
	includeParams bool,
	afterTime *time.Time,
	beforeTime *time.Time,
) ([]TaskSummary, bool, error) {
	ttable := queueTableIdentifier("t", queueName)
	rtable := queueTableIdentifier("r", queueName)
	queueLiteral := pq.QuoteLiteral(queueName)
	paramsSelect := "NULL::jsonb"
	if includeParams {
		paramsSelect = "t.params"
	}

	query := fmt.Sprintf(`
		SELECT
			t.task_id,
			r.run_id,
			%s AS queue_name,
			t.task_name,
			r.state,
			r.attempt,
			t.max_attempts,
			r.created_at,
			t.scheduled_at,
			r.completed_at,
			r.claimed_by,
			%s AS params
		FROM absurd.%s r
		JOIN absurd.%s t ON t.task_id = r.task_id
	`, queueLiteral, paramsSelect, rtable, ttable)

	var (
		clauses []string
		params  []any
	)

	if statusFilter != "" {
		params = append(params, statusFilter)
		clauses = append(clauses, fmt.Sprintf("r.state = $%d", len(params)))
	}
	if taskNameFilter != "" {
		params = append(params, taskNameFilter)
		clauses = append(clauses, fmt.Sprintf("t.task_name = $%d", len(params)))
	}
	if taskIDFilter != "" {
		params = append(params, taskIDFilter)
		clauses = append(clauses, fmt.Sprintf("t.task_id = $%d", len(params)))
	}
	if afterTime != nil {
		params = append(params, *afterTime)
		clauses = append(clauses, fmt.Sprintf("r.created_at >= $%d", len(params)))
	}
	if beforeTime != nil {
		params = append(params, *beforeTime)
		clauses = append(clauses, fmt.Sprintf("r.created_at <= $%d", len(params)))
	}

	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	query += " ORDER BY r.run_id DESC"

	queryLimit := limit
	if queryLimit > 0 {
		queryLimit += 1
		params = append(params, queryLimit)
		query += fmt.Sprintf(" LIMIT $%d", len(params))
	}

	rows, err := s.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var tasks []TaskSummary
	hasMore := false
	count := 0
	for rows.Next() {
		count++
		if count > limit {
			hasMore = true
			break
		}

		var (
			taskID     string
			runID      string
			queueName2 string
			taskName   string
			status     string
			attempt    int
			maxAttempts sql.NullInt64
			createdAt  time.Time
			scheduledAt sql.NullTime
			completedAt sql.NullTime
			claimedBy   sql.NullString
			params      []byte
		)
		if err := rows.Scan(
			&taskID, &runID, &queueName2, &taskName, &status,
			&attempt, &maxAttempts, &createdAt, &scheduledAt,
			&completedAt, &claimedBy, &params,
		); err != nil {
			log.Printf("fetchQueueTaskCandidates: scan error: %v", err)
			continue
		}

		tasks = append(tasks, TaskSummary{
			TaskID:     taskID,
			RunID:      runID,
			QueueName:  queueName2,
			TaskName:   taskName,
			Status:     status,
			Attempt:    attempt,
			ClaimedBy:  nullableString(claimedBy),
			CreatedAt:  &createdAt,
			ScheduledAt: nullableTime(scheduledAt),
			CheckpointNum: nullableInt(maxAttempts),
			Params:    nullableBytes(params),
		})
	}

	return tasks, hasMore, rows.Err()
}

func (s *Server) getTaskAttempts(ctx context.Context, queueName, taskID string) (int, error) {
	table := queueTableIdentifier("t", queueName)
	// SAFE: table name is quoted via pq.QuoteIdentifier; value uses $1 parameter.
	query := `SELECT attempts FROM absurd.` + table + ` WHERE task_id = $1 LIMIT 1`

	var attempts int
	err := s.db.QueryRowContext(ctx, query, taskID).Scan(&attempts)
	if err != nil {
		return 0, err
	}
	return attempts, nil
}

func (s *Server) fetchQueueEvents(ctx context.Context, queueName string, limit int, eventName string, afterTime *time.Time, beforeTime *time.Time) ([]QueueEvent, error) {
	rtable := queueTableIdentifier("r", queueName)
	ttable := queueTableIdentifier("t", queueName)

	query := fmt.Sprintf(`
		SELECT
			e.event_name,
			e.emitted_at,
			e.task_id,
			e.run_id,
			%s AS queue_name,
			e.payload,
			t.task_name
		FROM absurd.%s e
		LEFT JOIN absurd.%s t ON t.task_id = e.task_id
	`, pq.QuoteLiteral(queueName), rtable, ttable)

	var (
		clauses []string
		params  []any
	)

	if eventName != "" {
		params = append(params, eventName)
		clauses = append(clauses, fmt.Sprintf("event_name = $%d", len(params)))
	}
	if afterTime != nil {
		params = append(params, *afterTime)
		clauses = append(clauses, fmt.Sprintf("emitted_at >= $%d", len(params)))
	}
	if beforeTime != nil {
		params = append(params, *beforeTime)
		clauses = append(clauses, fmt.Sprintf("emitted_at <= $%d", len(params)))
	}

	whereClause := ""
	if len(clauses) > 0 {
		whereClause = "WHERE " + strings.Join(clauses, " AND ")
	}

	query += " " + whereClause + fmt.Sprintf(" ORDER BY e.emitted_at DESC LIMIT $%d", len(params)+1)
	params = append(params, limit+1)

	rows, err := s.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []QueueEvent
	for rows.Next() {
		var ev QueueEvent
		var taskName sql.NullString
		if err := rows.Scan(&ev.EventName, &ev.EmittedAt, &ev.TaskID, &ev.RunID, &ev.QueueName, &ev.Payload, &taskName); err != nil {
			continue
		}
		if taskName.Valid {
			ev.TaskName = &taskName.String
		}
		events = append(events, ev)
	}

	return events, rows.Err()
}

func (s *Server) listQueueNames(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT queue_name FROM absurd.queues ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			names = append(names, name)
		}
	}
	return names, rows.Err()
}

func (s *Server) findQueueForRun(ctx context.Context, runID string) (string, error) {
	queueNames, err := s.listQueueNames(ctx)
	if err != nil {
		return "", err
	}

	for _, queueName := range queueNames {
		rtable := queueTableIdentifier("r", queueName)
		// SAFE: table name quoted via pq.QuoteIdentifier; runID uses $1 parameter.
		query := `SELECT 1 FROM absurd.` + rtable + ` WHERE run_id = $1 LIMIT 1`
		var dummy int
		err = s.db.QueryRowContext(ctx, query, runID).Scan(&dummy)
		switch {
		case err == nil:
			return queueName, nil
		case err == sql.ErrNoRows:
			continue
		default:
			log.Printf("findQueueForRun: query failed for queue %s: %v", queueName, err)
			continue
		}
	}
	return "", sql.ErrNoRows
}

func (s *Server) ensureQueueExists(ctx context.Context, queueName string) error {
	var name string
	return s.db.QueryRowContext(ctx, `SELECT queue_name FROM absurd.queues WHERE queue_name = $1`, queueName).Scan(&name)
}

func queueTableIdentifier(prefix, queueName string) string {
	return pq.QuoteIdentifier(prefix + "_" + queueName)
}

// ============================================================================
// Record Types (for DB scanning)
// ============================================================================

type taskSummaryRecord struct {
	TaskID      string
	RunID       string
	QueueName   string
	TaskName    string
	Status      string
	Attempt     int
	MaxAttempts sql.NullInt64
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt sql.NullTime
	ClaimedBy   sql.NullString
	Params      []byte
}

type taskDetailRecord struct {
	taskSummaryRecord
	Params        []byte
	RetryStrategy []byte
	Headers       []byte
	State         []byte
	Checkpoints   []checkpointStateRecord
	WaitStates    []waitStateRecord
}

type checkpointStateRecord struct {
	CheckpointName string
	State          string
	Status         string
	OwnerRunID     string
	ExpiresAt      time.Time
	UpdatedAt      time.Time
}

type waitStateRecord struct {
	WaitDuration string
	PollInterval string
	RunID        string
}

// AsAPI converts taskDetailRecord to TaskDetail API type.
func (r *taskDetailRecord) AsAPI() TaskDetail {
	td := TaskDetail{
		TaskID:      r.TaskID,
		RunID:       r.RunID,
		TaskName:    r.TaskName,
		Status:      r.Status,
		Attempt:     r.Attempt,
		CreatedAt:   &r.CreatedAt,
		ClaimedBy:   nullableString(r.ClaimedBy),
		Params:      nullableBytes(r.Params),
	}
	for _, cp := range r.Checkpoints {
		td.Checkpoints = append(td.Checkpoints, CheckpointState{
			CheckpointName: cp.CheckpointName,
			State:          cp.State,
			Status:         cp.Status,
			OwnerRunID:     cp.OwnerRunID,
			ExpiresAt:      &cp.ExpiresAt,
			UpdatedAt:      &cp.UpdatedAt,
		})
	}
	for _, ws := range r.WaitStates {
		td.Wait = &WaitState{
			WaitDuration: ws.WaitDuration,
			PollInterval: ws.PollInterval,
			RunID:        ws.RunID,
		}
	}
	return td
}