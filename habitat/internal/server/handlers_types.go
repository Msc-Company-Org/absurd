package server

import (
	"database/sql"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// ============================================================================
// Types
// ============================================================================

type QueueMetrics struct {
	QueueName          string     `json:"queueName"`
	QueueLength        int64      `json:"queueLength"`
	NewestMsgAt        *time.Time `json:"newestMsgAt,omitempty"`
	OldestMsgAt        *time.Time `json:"oldestMsgAt,omitempty"`
	TotalMessages      int64      `json:"totalMessages"`
	ScrapeTime         time.Time  `json:"scrapeTime"`
	QueueVisibleLength int64      `json:"queueVisibleLength"`
}

type queueMetricsRecord struct {
	QueueName          string
	QueueLength        int64
	NewestMsgAt        *time.Time
	OldestMsgAt        *time.Time
	TotalMessages      int64
	ScrapeTime         time.Time
	QueueVisibleLength int64
}

func (r queueMetricsRecord) AsAPI() QueueMetrics {
	return QueueMetrics{
		QueueName:          r.QueueName,
		QueueLength:        r.QueueLength,
		NewestMsgAt:        r.NewestMsgAt,
		OldestMsgAt:        r.OldestMsgAt,
		TotalMessages:      r.TotalMessages,
		ScrapeTime:         r.ScrapeTime,
		QueueVisibleLength: r.QueueVisibleLength,
	}
}

type TaskSummary struct {
	TaskID        string        `json:"taskId"`
	RunID         string        `json:"runId,omitempty"`
	QueueName     string        `json:"queueName"`
	TaskName      string        `json:"taskName"`
	Status        string        `json:"status"`
	Attempt       int           `json:"attempt,omitempty"`
	ClaimedBy     *string       `json:"claimedBy,omitempty"`
	CreatedAt     *time.Time    `json:"createdAt,omitempty"`
	ScheduledAt   *time.Time    `json:"scheduledAt,omitempty"`
	CheckpointNum *int          `json:"checkpointNum,omitempty"`
	WorkerID      *string       `json:"workerId,omitempty"`
	Params        *json.RawMessage `json:"params,omitempty"`
}

type TaskDetail struct {
	TaskID      string        `json:"taskId"`
	RunID       string        `json:"runId"`
	QueueName   string        `json:"queueName"`
	TaskName    string        `json:"taskName"`
	Status      string        `json:"status"`
	Params      *json.RawMessage `json:"params,omitempty"`
	Checkpoints []CheckpointState `json:"checkpoints,omitempty"`
	Wait        *WaitState    `json:"wait,omitempty"`
	CreatedAt   *time.Time    `json:"createdAt,omitempty"`
	ScheduledAt *time.Time    `json:"scheduledAt,omitempty"`
	ClaimedBy   *string       `json:"claimedBy,omitempty"`
	Attempt     int           `json:"attempt,omitempty"`
}

type CheckpointState struct {
	CheckpointName string     `json:"checkpointName"`
	State          string     `json:"state"`
	Status         string     `json:"status"`
	OwnerRunID     string     `json:"ownerRunId"`
	ExpiresAt      *time.Time `json:"expiresAt,omitempty"`
	UpdatedAt      *time.Time `json:"updatedAt,omitempty"`
}

type WaitState struct {
	WaitDuration string `json:"waitDuration"`
	PollInterval string `json:"pollInterval"`
	RunID        string `json:"runId"`
}

type QueueSummary struct {
	QueueName  string `json:"queueName"`
	QueueLen   int64  `json:"queueLength"`
	OldestTask string `json:"oldestTask"`
	NewestTask string `json:"newestTask"`
	CreatedAt  string `json:"createdAt"`
}

type TaskListResponse struct {
	Tasks      []TaskSummary `json:"tasks"`
	Count      int           `json:"count"`
	TotalCount int           `json:"totalCount"`
	Page       int           `json:"page"`
	PerPage    int           `json:"perPage"`
}

type QueueMessage struct {
	QueueName string `json:"queueName"`
	Message  string `json:"message"`
	TaskID    string `json:"taskId"`
	RunID     string `json:"runId"`
}

type QueueEvent struct {
	EventName  string     `json:"eventName"`
	EmittedAt  time.Time  `json:"emittedAt"`
	TaskID     string     `json:"taskId"`
	RunID      string     `json:"runId"`
	QueueName  string     `json:"queueName"`
	Payload    *string    `json:"payload,omitempty"`
	TaskName   *string    `json:"taskName,omitempty"`
}

// ============================================================================
// Helpers
// ============================================================================

func nullableInt(v sql.NullInt64) *int {
	if !v.Valid {
		return nil
	}
	i := int(v.Int64)
	return &i
}

func nullableTime(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	t := v.Time
	return &t
}

func nullableString(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	s := v.String
	return &s
}

func nullableBytes(v []byte) json.RawMessage {
	if len(v) == 0 {
		return nil
	}
	return json.RawMessage(v)
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

func sortedKeys(set map[string]struct{}) []string {
	result := make([]string, 0, len(set))
	for k := range set {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
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

func parseInt(s string, base int, bitSize int) (int64, error) {
