package server

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"strings"
	"time"
)

// ============================================================================
// UI / Runtime Config
// ============================================================================

type uiRuntimeConfig struct {
	BasePath         string `json:"basePath"`
	StaticBasePath   string `json:"staticBasePath"`
	APIWebSocketURL  string `json:"apiWebSocketUrl"`
	QueueName        string `json:"queueName,omitempty"`
	Title            string `json:"title"`
	IsTaskDetail     bool   `json:"isTaskDetail"`
	ShowCheckpoints  bool   `json:"showCheckpoints"`
	ShowTaskWaitInfo bool   `json:"showTaskWaitInfo"`
	DebugMode        bool   `json:"debugMode"`
}

func (s *Server) runtimeConfig(r *http.Request) uiRuntimeConfig {
	base := s.publicBasePath(r)
	return uiRuntimeConfig{
		BasePath:       base,
		StaticBasePath: base,
		APIWebSocketURL: base + "/ws",
		DebugMode:      r.URL.Query().Get("debug") == "1",
	}
}

func (s *Server) publicBasePath(r *http.Request) string {
	prefix := r.URL.Path
	prefix = extractForwardedPrefix(r)
	if prefix == "" {
		prefix = "/"
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix
}

func extractForwardedPrefix(r *http.Request) string {
	if proto := r.Header.Get("X-Forwarded-Prefix"); proto != "" {
		return proto
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		host := r.Host
		if strings.Contains(host, ":") {
			if (proto == "http" && strings.HasSuffix(host, ":80")) ||
				(proto == "https" && strings.HasSuffix(host, ":443")) {
				host = strings.TrimSuffix(host, ":80")
				host = strings.TrimSuffix(host, ":443")
			}
		}
		return proto + "://" + host
	}
	return ""
}

func normalizePathPrefix(value string) string {
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	value = "/" + strings.TrimLeft(value, "/")
	value = strings.TrimRight(value, "/")
	if value == "/" {
		return ""
	}
	return value
}

func joinPathPrefixes(parts ...string) string {
	result := ""
	for _, part := range parts {
		normalized := normalizePathPrefix(part)
		if normalized == "" {
			continue
		}
		if result == "" {
			result = normalized
			continue
		}
		result += normalized
	}
	if result == "" {
		return ""
	}
	return result
}

// ============================================================================
// Index HTML Rendering
// ============================================================================

func (s *Server) renderIndexHTML(runtimeCfg uiRuntimeConfig) []byte {
	if len(s.indexHTML) == 0 {
		return nil
	}

	baseHref := runtimeCfg.BasePath
	if baseHref == "" {
		baseHref = "/"
	} else {
		baseHref += "/"
	}

	payload, err := json.Marshal(runtimeCfg)
	if err != nil {
		payload = []byte("{}")
	}

	injection := fmt.Sprintf("<base href=\"%s\"><script>window.__HABITAT_RUNTIME_CONFIG__=%s;</script>", html.EscapeString(baseHref), payload)
	document := string(s.indexHTML)
	staticPrefix := runtimeCfg.StaticBasePath + "/"
	document = strings.ReplaceAll(document, "\"/_static/", staticPrefix+'"')
	document = strings.ReplaceAll(document, "'/_static/", staticPrefix+"'")

	if idx := strings.Index(document, "</head>"); idx >= 0 {
		document = document[:idx] + injection + document[idx:]
	} else {
		document = injection + document
	}

	return []byte(document)
}
