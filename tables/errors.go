package tables

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

// AzureError represents a structured error from Azure Table Storage
type AzureError struct {
	StatusCode int
	RequestID  string
	Message    string
	Code       string
}

func (e *AzureError) Error() string {
	return fmt.Sprintf("azure table error: status=%d code=%s request_id=%s message=%s",
		e.StatusCode, e.Code, e.RequestID, e.Message)
}

// IsTransient returns true if the error is likely transient and retryable
func (e *AzureError) IsTransient() bool {
	return isTransientStatus(e.StatusCode)
}

func isTransientStatus(code int) bool {
	return code == http.StatusTooManyRequests ||
		code == http.StatusServiceUnavailable ||
		code == http.StatusRequestTimeout
}

// ErrorResponse represents an Azure error response body.
type ErrorResponse struct {
	ODataError struct {
		Code    string `json:"code"`
		Message struct {
			Lang  string `json:"lang"`
			Value string `json:"value"`
		} `json:"message"`
	} `json:"odata.error"`
}

// ParseAzureError extracts structured error information from an Azure response.
func ParseAzureError(resp *http.Response, body []byte) *AzureError {
	azErr := &AzureError{
		StatusCode: resp.StatusCode,
		RequestID:  resp.Header.Get("x-ms-request-id"),
		Message:    string(body),
	}

	var errResp ErrorResponse
	if err := json.Unmarshal(body, &errResp); err == nil {
		if errResp.ODataError.Code != "" {
			azErr.Code = errResp.ODataError.Code
			azErr.Message = errResp.ODataError.Message.Value
			return azErr
		}
	}

	bodyStr := string(body)
	if strings.Contains(bodyStr, "<m:code>") {
		if start := strings.Index(bodyStr, "<m:code>"); start != -1 {
			start += len("<m:code>")
			if end := strings.Index(bodyStr[start:], "</m:code>"); end != -1 {
				azErr.Code = bodyStr[start : start+end]
			}
		}
		if start := strings.Index(bodyStr, "<m:message"); start != -1 {
			if tagEnd := strings.Index(bodyStr[start:], ">"); tagEnd != -1 {
				msgStart := start + tagEnd + 1
				if end := strings.Index(bodyStr[msgStart:], "</m:message>"); end != -1 {
					azErr.Message = bodyStr[msgStart : msgStart+end]
				}
			}
		}
	}

	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
		diagHeaders := make(map[string]string)
		for _, key := range []string{
			"x-ms-error-code",
			"x-ms-request-id",
			"WWW-Authenticate",
			"x-ms-version",
			"Server",
			"Date",
		} {
			if v := resp.Header.Get(key); v != "" {
				diagHeaders[key] = v
			}
		}
		slog.Error("azure auth failure diagnostic",
			"status", resp.StatusCode,
			"code", azErr.Code,
			"request_id", azErr.RequestID,
			"response_headers", diagHeaders,
			"message", azErr.Message,
		)
	}

	return azErr
}

// parseBatchResponse parses the multipart/mixed body of an Azure Table Storage
// batch response to detect per-entity failures.
func parseBatchResponse(respBody []byte) error {
	lines := strings.Split(string(respBody), "\n")
	var entityErrors []string
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "HTTP/1.1 ") {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 {
			continue
		}
		statusCode, err := strconv.Atoi(parts[1])
		if err != nil || statusCode < 400 {
			continue
		}
		detail := ""
		for j := i + 1; j < len(lines); j++ {
			trimmed := strings.TrimSpace(lines[j])
			if strings.HasPrefix(trimmed, "--") {
				break
			}
			if strings.HasPrefix(trimmed, "{") {
				detail = trimmed
				break
			}
		}
		if detail != "" {
			entityErrors = append(entityErrors, fmt.Sprintf("status %d: %s", statusCode, detail))
		} else {
			entityErrors = append(entityErrors, line)
		}
	}
	if len(entityErrors) > 0 {
		return fmt.Errorf("batch entity failures: %s", strings.Join(entityErrors, "; "))
	}
	return nil
}
