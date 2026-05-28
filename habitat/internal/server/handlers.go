package server

// Security notes on SQL query construction:
// - Table names (queue identifiers) are ALWAYS quoted via pq.QuoteIdentifier
//   (see queueTableIdentifier in handlers_queues.go).
// - User-provided values are ALWAYS passed as $N parameters (QueryContext/ExecContext),
//   never interpolated into SQL strings.
// - When building dynamic WHERE clauses, only the parameter index ($N) is concatenated
//   via fmt.Sprintf — this is safe because it only contains digits.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.runtimeConfig(r))
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	runtimeCfg := s.runtimeConfig(r)
	runtimeCfg.Title = "Habitat"
	runtimeCfg.ShowCheckpoints = true
	document := s.renderIndexHTML(runtimeCfg)
	if len(document) == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(document)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	queueNames, err := s.listQueueNames(ctx)
	if err != nil {
		http.Error(w, "failed to list queues", http.StatusInternalServerError)
		return
	}

	var metrics []interface{}
	for _, queueName := range queueNames {
		m := s.fetchQueueMetricsForQueue(ctx, queueName)
		if m != nil {
			metrics = append(metrics, m)
		}
	}

	response := map[string]interface{}{
		"queues":     metrics,
		"scrapeTime": time.Now(),
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) fetchQueueMetricsForQueue(ctx context.Context, queueName string) interface{} {
	return nil
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	queryValues := r.URL.Query()
	search := strings.TrimSpace(queryValues.Get("search"))
	statusFilter := ""
	statusValid := false
	if queryValues.Get("status") != "" {
		statusFilter, statusValid = normalizeTaskStatusFilter(queryValues.Get("status"))
		if !statusValid {
			http.Error(w, "invalid status filter", http.StatusBadRequest)
			return
		}
	}
	afterTime := parseOptionalTime(strings.TrimSpace(queryValues.Get("after")))
	beforeTime := parseOptionalTime(strings.TrimSpace(queryValues.Get("before")))
	page := parsePositiveInt(queryValues.Get("page"), 1)
	perPage := parsePositiveInt(queryValues.Get("perPage"), 25)

	queueNames, err := s.listQueueNames(ctx)
	if err != nil {
		http.Error(w, "failed to list queues", http.StatusInternalServerError)
		return
	}

	if len(queueNames) == 0 {
		writeJSON(w, http.StatusOK, emptyTaskListResponse(page, perPage, queueNames))
		return
	}

	allTasks := make([]TaskSummary, 0)
	for _, queueName := range queueNames {
		tasks, _, err := s.fetchQueueTaskCandidates(ctx, queueName, statusFilter, "", "", perPage, true, afterTime, beforeTime)
		if err != nil {
			if isContextQueryFailure(err, ctx) {
				writeTaskQueryContextError(w, err, ctx)
				return
			}
			log.Printf("handleTasks: failed to fetch queue %s: %v", queueName, err)
			continue
		}
		for _, task := range tasks {
			if matchesTaskFilters(task, search, "", "", "", "") {
				allTasks = append(allTasks, task)
			}
		}
	}

	recentNames, err := s.listRecentTaskNames(ctx, queueNames, 20)
	if err != nil {
		log.Printf("handleTasks: failed to list recent names: %v", err)
	}
	recentSet := make(map[string]struct{})
	for _, n := range recentNames {
		recentSet[n] = struct{}{}
	}

	filteredTasks := make([]TaskSummary, 0)
	for _, t := range allTasks {
		_, inRecent := recentSet[t.TaskName]
		_, hasStatus := map[string]struct{}{
			"waiting": {}, "scheduled": {}, "running": {},
		}[t.Status]
		if !hasStatus || (search == "" && inRecent) {
			filteredTasks = append(filteredTasks, t)
		}
	}

	if page < 1 {
		page = 1
	}
	start := (page - 1) * perPage
	end := start + perPage
	if start > len(filteredTasks) {
		filteredTasks = []TaskSummary{}
	} else if end > len(filteredTasks) {
		filteredTasks = filteredTasks[start:]
	} else {
		filteredTasks = filteredTasks[start:end]
	}

	response := TaskListResponse{
		Tasks:              filteredTasks,
		Count:              len(filteredTasks),
		TotalCount:         len(allTasks),
		Page:               page,
		PerPage:            perPage,
		AvailableStatuses:  allTaskStatuses(),
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleTaskDetail(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	taskID := r.URL.Query().Get("taskId")
	queueName := r.URL.Query().Get("queue")
	if taskID == "" || queueName == "" {
		http.Error(w, "taskId and queue are required", http.StatusBadRequest)
		return
	}

	if err := s.ensureQueueExists(ctx, queueName); err != nil {
		http.Error(w, "queue not found", http.StatusNotFound)
		return
	}

	var task taskDetailRecord
	query := fmt.Sprintf(`
		SELECT t.task_id, r.run_id, t.task_name, r.state, r.attempt,
		       t.params, t.retry_strategy, t.headers, r.state,
		       r.created_at, r.completed_at, r.claimed_by
		FROM absurd.%s r
		JOIN absurd.%s t ON t.task_id = r.task_id
		WHERE t.task_id = $1
	`, queueTableIdentifier("r", queueName), queueTableIdentifier("t", queueName))

	err := s.db.QueryRowContext(ctx, query, taskID).Scan(
		&task.TaskID, &task.RunID, &task.TaskName, &task.Status, &task.Attempt,
		&task.Params, &task.RetryStrategy, &task.Headers, &task.State,
		&task.CreatedAt, &task.CompletedAt, &task.ClaimedBy,
	)
	if err != nil {
		if isContextQueryFailure(err, ctx) {
			writeTaskQueryContextError(w, err, ctx)
			return
		}
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	ctable := queueTableIdentifier("c", queueName)
	wtable := queueTableIdentifier("w", queueName)

	checkpointsQuery := fmt.Sprintf(`
		SELECT checkpoint_name, state, status, owner_run_id, expires_at, updated_at
		FROM absurd.%s
		WHERE owner_run_id = $1
		ORDER BY updated_at ASC
	`, ctable)
	crows, err := s.db.QueryContext(ctx, checkpointsQuery, task.RunID)
	if err != nil {
		log.Printf("handleTaskDetail: failed to fetch checkpoints: %v", err)
	} else {
		defer crows.Close()
		for crows.Next() {
			var cp checkpointStateRecord
			if err := crows.Scan(&cp.CheckpointName, &cp.State, &cp.Status, &cp.OwnerRunID, &cp.ExpiresAt, &cp.UpdatedAt); err == nil {
				task.Checkpoints = append(task.Checkpoints, cp)
			}
		}
	}

	waitQuery := fmt.Sprintf(`
		SELECT wait_duration, poll_interval, run_id
		FROM absurd.%s
		WHERE run_id = $1
	`, wtable)
	wrows, err := s.db.QueryContext(ctx, waitQuery, task.RunID)
	if err != nil {
		log.Printf("handleTaskDetail: failed to fetch wait states: %v", err)
	} else {
		defer wrows.Close()
		for wrows.Next() {
			var wt waitStateRecord
			if err := wrows.Scan(&wt.WaitDuration, &wt.PollInterval, &wt.RunID); err == nil {
				task.WaitStates = append(task.WaitStates, wt)
			}
		}
	}

	runtimeCfg := s.runtimeConfig(r)
	runtimeCfg.Title = "Task: " + task.TaskName
	runtimeCfg.QueueName = queueName
	runtimeCfg.IsTaskDetail = true
	runtimeCfg.ShowCheckpoints = true
	runtimeCfg.ShowTaskWaitInfo = len(task.WaitStates) > 0
	document := s.renderIndexHTML(runtimeCfg)

	if len(document) == 0 {
		writeJSON(w, http.StatusOK, task.AsAPI())
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(document)
}

func (s *Server) handleRetryTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var request struct {
		TaskID         string   `json:"taskId"`
		QueueName      string   `json:"queueName"`
		MaxAttempts    *int     `json:"maxAttempts,omitempty"`
		ExtraAttempts  *int     `json:"extraAttempts,omitempty"`
		SpawnNewTask   bool     `json:"spawnNewTask"`
		AdditionalArgs []string `json:"additionalArgs,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if request.TaskID == "" || request.QueueName == "" {
		http.Error(w, "taskId and queueName are required", http.StatusBadRequest)
		return
	}

	parsedTaskID, err := uuid.Parse(request.TaskID)
	if err != nil {
		http.Error(w, "taskId must be a valid UUID", http.StatusBadRequest)
		return
	}
	_ = parsedTaskID

	if request.MaxAttempts != nil && *request.MaxAttempts < 1 {
		http.Error(w, "maxAttempts must be >= 1", http.StatusBadRequest)
		return
	}
	if request.ExtraAttempts != nil && *request.ExtraAttempts < 1 {
		http.Error(w, "extraAttempts must be >= 1", http.StatusBadRequest)
		return
	}
	if request.SpawnNewTask && request.ExtraAttempts != nil {
		if err := s.db.QueryRowContext(ctx, `
			UPDATE absurd.`+queueTableIdentifier("t", request.QueueName)+`
			SET max_attempts = max_attempts + $1
			WHERE task_id = $2
			RETURNING task_id
		`, *request.ExtraAttempts, request.TaskID).Scan(&parsedTaskID); err != nil {
			http.Error(w, "failed to retry task", http.StatusInternalServerError)
			return
		}
	}

	response := map[string]string{
		"queueName": request.QueueName,
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	queueNames, err := s.listQueueNames(ctx)
	if err != nil {
		http.Error(w, "failed to list queues", http.StatusInternalServerError)
		return
	}

	type queueInfo struct {
		QueueName  string `json:"queueName"`
		QueueLen   int64  `json:"queueLength"`
		OldestTask string `json:"oldestTask"`
		NewestTask string `json:"newestTask"`
		CreatedAt  string `json:"createdAt"`
	}

	var queues []queueInfo
	for _, name := range queueNames {
		var createdAt time.Time
		if err := s.db.QueryRowContext(ctx, `SELECT created_at FROM absurd.queues WHERE queue_name = $1`, name).Scan(&createdAt); err != nil {
			continue
		}
		queues = append(queues, queueInfo{
			QueueName: name,
			CreatedAt: createdAt.Format(time.RFC3339),
		})
	}

	type response struct {
		Queues    []queueInfo `json:"queues"`
		Count     int         `json:"count"`
		CreatedAt string      `json:"createdAt"`
	}

	writeJSON(w, http.StatusOK, response{
		Queues:    queues,
		Count:     len(queues),
		CreatedAt: time.Now().Format(time.RFC3339),
	})
}

func (s *Server) handleQueueResource(w http.ResponseWriter, r *http.Request) {
	queueName := strings.TrimPrefix(r.URL.Path, "/api/queues/")
	queueName = strings.Split(queueName, "/")[0]

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := s.ensureQueueExists(ctx, queueName); err != nil {
		http.Error(w, "queue not found", http.StatusNotFound)
		return
	}

	switch r.URL.Path {
	case "/api/queues/" + queueName + "/tasks":
		s.handleQueueTasks(w, r, queueName)
	case "/api/queues/" + queueName + "/events":
		s.handleQueueEvents(w, r, queueName)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleQueueTasks(w http.ResponseWriter, r *http.Request, queueName string) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	queryValues := r.URL.Query()
	statusFilter := ""
	statusValid := false
	if queryValues.Get("status") != "" {
		statusFilter, statusValid = normalizeTaskStatusFilter(queryValues.Get("status"))
		if !statusValid {
			http.Error(w, "invalid status filter", http.StatusBadRequest)
			return
		}
	}
	taskNameFilter := queryValues.Get("taskName")
	taskIDFilter := queryValues.Get("taskId")
	afterTime := parseOptionalTime(queryValues.Get("after"))
	beforeTime := parseOptionalTime(queryValues.Get("before"))
	page := parsePositiveInt(queryValues.Get("page"), 1)
	perPage := parsePositiveInt(queryValues.Get("perPage"), 25)
	includeParams := queryValues.Get("includeParams") == "1"

	if err := s.ensureQueueExists(ctx, queueName); err != nil {
		http.Error(w, "queue not found", http.StatusNotFound)
		return
	}

	tasks, hasMore, err := s.fetchQueueTaskCandidates(ctx, queueName, statusFilter, taskNameFilter, taskIDFilter, perPage, includeParams, afterTime, beforeTime)
	if err != nil {
		if isContextQueryFailure(err, ctx) {
			writeTaskQueryContextError(w, err, ctx)
			return
		}
		http.Error(w, "failed to query tasks", http.StatusInternalServerError)
		return
	}

	if page < 1 {
		page = 1
	}
	start := (page - 1) * perPage
	end := start + perPage
	if start > len(tasks) {
		tasks = []TaskSummary{}
	} else if end > len(tasks) {
		tasks = tasks[start:]
	} else {
		tasks = tasks[start:end]
	}

	response := TaskListResponse{
		Tasks:              tasks,
		Count:              len(tasks),
		TotalCount:         len(tasks) + (1 if hasMore else 0),
		Page:               page,
		PerPage:            perPage,
		AvailableStatuses:  allTaskStatuses(),
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleQueueEvents(w http.ResponseWriter, r *http.Request, queueName string) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	queryValues := r.URL.Query()
	limit := parsePositiveInt(queryValues.Get("limit"), 100)
	eventName := queryValues.Get("eventName")
	afterTime := parseOptionalTime(queryValues.Get("after"))
	beforeTime := parseOptionalTime(queryValues.Get("before"))

	if err := s.ensureQueueExists(ctx, queueName); err != nil {
		http.Error(w, "queue not found", http.StatusNotFound)
		return
	}

	events, err := s.fetchQueueEvents(ctx, queueName, limit, eventName, afterTime, beforeTime)
	if err != nil {
		if isContextQueryFailure(err, ctx) {
			writeTaskQueryContextError(w, err, ctx)
			return
		}
		http.Error(w, "failed to query events", http.StatusInternalServerError)
		return
	}

	type response struct {
		Events    []QueueEvent `json:"events"`
		Count     int          `json:"count"`
		QueueName string       `json:"queueName"`
	}

	writeJSON(w, http.StatusOK, response{
		Events:    events,
		Count:     len(events),
		QueueName: queueName,
	})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	queryValues := r.URL.Query()
	limit := parsePositiveInt(queryValues.Get("limit"), 100)
	eventName := queryValues.Get("eventName")
	afterTime := parseOptionalTime(queryValues.Get("after"))
	beforeTime := parseOptionalTime(queryValues.Get("before"))

	queueNames, err := s.listQueueNames(ctx)
	if err != nil {
		http.Error(w, "failed to list queues", http.StatusInternalServerError)
		return
	}

	var allEvents []QueueEvent
	for _, queueName := range queueNames {
		events, err := s.fetchQueueEvents(ctx, queueName, limit, eventName, afterTime, beforeTime)
		if err != nil {
			log.Printf("handleEvents: failed to fetch events for queue %s: %v", queueName, err)
			continue
		}
		allEvents = append(allEvents, events...)
	}

	type response struct {
		Events []QueueEvent `json:"events"`
		Count  int          `json:"count"`
	}

	writeJSON(w, http.StatusOK, response{
		Events: allEvents,
		Count:  len(allEvents),
	})
}

func matchesTaskFilters(task TaskSummary, search string, status string, queue string, taskName string, taskID string) bool {
	if status != "" && task.Status != status {
		return false
	}
	if queue != "" && task.QueueName != queue {
		return false
	}
	if taskName != "" && !strings.Contains(strings.ToLower(task.TaskName), strings.ToLower(taskName)) {
		return false
	}
	if taskID != "" && !strings.Contains(strings.ToLower(task.TaskID), strings.ToLower(taskID)) {
		return false
	}
	if search != "" {
		lsearch := strings.ToLower(search)
		match := strings.Contains(strings.ToLower(task.TaskID), lsearch) ||
			strings.Contains(strings.ToLower(task.TaskName), lsearch) ||
			strings.Contains(strings.ToLower(task.QueueName), lsearch)
		if !match {
			return false
		}
	}
	return true
}

func parseOptionalTime(value string) *time.Time {
	if value == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil
	}
	return &t
}

func allTaskStatuses() []string {
	return []string{
		"waiting",
		"scheduled",
		"running",
		"completed",
		"failed",
	}
}

func normalizeTaskStatusFilter(value string) (string, bool) {
	if value == "" {
		return "", true
	}
	for _, s := range allTaskStatuses() {
		if strings.EqualFold(s, value) {
			return s, true
		}
	}
	return value, false
}

func emptyTaskListResponse(page, perPage int, queueNames []string) TaskListResponse {
	return TaskListResponse{
		Tasks:      []TaskSummary{},
		Count:      0,
		TotalCount: 0,
		Page:       page,
		PerPage:    perPage,
	}
}

func parsePositiveInt(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil && n > 0 {
		return int(n)
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload != nil {
		enc := json.NewEncoder(w)
		enc.Encode(payload)
	}
}