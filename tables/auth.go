package tables

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/fgrzl/azkit/internal/httputil"
	"github.com/fgrzl/azkit/internal/jwt"
)

// signRequest signs an HTTP request with either Bearer token or SharedKeyLite authentication.
func (c *HTTPTableClient) signRequest(req *http.Request, method string, _ []byte, resourcePath string) error {
	req.Header.Set("x-ms-version", AzureAPIVersion)
	date := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Set("x-ms-date", date)

	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
		req.Header.Set("Content-Type", contentType)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json;odata=nometadata")
	}

	if c.useBearerToken && c.managedCred != nil {
		token, err := c.managedCred.GetToken(req.Context())
		if err != nil {
			slog.Error("failed to acquire managed identity token",
				"error", err,
				"method", method,
				"path", resourcePath)
			return fmt.Errorf("managed identity token: %w", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

		tokenPrefix := token
		if len(tokenPrefix) > 20 {
			tokenPrefix = tokenPrefix[:20] + "..."
		}
		slog.Debug("azure bearer auth: request signed",
			"method", method,
			"url", req.URL.String(),
			"resource_path", resourcePath,
			"x-ms-version", req.Header.Get("x-ms-version"),
			"x-ms-date", req.Header.Get("x-ms-date"),
			"token_prefix", tokenPrefix,
			"account", c.accountName,
		)
		return nil
	}

	if c.useSAS {
		return nil
	}

	canonicalResource := fmt.Sprintf("/%s%s", c.accountName, resourcePath)

	if req.URL != nil && req.URL.RawQuery != "" {
		params, err := url.ParseQuery(req.URL.RawQuery)
		if err != nil {
			return fmt.Errorf("parse request query: %w", err)
		}
		if compVal, ok := params["comp"]; ok && len(compVal) > 0 {
			canonicalResource += "?comp=" + compVal[0]
		}
	}

	stringToSign := date + "\n" + canonicalResource

	decodedKey, err := base64.StdEncoding.DecodeString(c.accountKey)
	if err != nil {
		slog.Error("account key failed to decode despite factory validation",
			"error", err,
			"method", method,
			"path", resourcePath,
			"account", c.accountName)
		return fmt.Errorf("shared key decode: %w", err)
	}

	h := hmac.New(sha256.New, decodedKey)
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	authHeader := fmt.Sprintf("SharedKeyLite %s:%s", c.accountName, signature)
	req.Header.Set("Authorization", authHeader)
	return nil
}

// AuthDiagnostic contains the results of an auth diagnostic check
type AuthDiagnostic struct {
	AuthMode        string            `json:"auth_mode"`
	Endpoint        string            `json:"endpoint"`
	AccountName     string            `json:"account_name"`
	TableName       string            `json:"table_name"`
	TokenAcquired   bool              `json:"token_acquired,omitempty"`
	TokenError      string            `json:"token_error,omitempty"`
	TokenAudience   string            `json:"token_audience,omitempty"`
	TokenIssuer     string            `json:"token_issuer,omitempty"`
	TokenTenantID   string            `json:"token_tenant_id,omitempty"`
	TokenObjectID   string            `json:"token_object_id,omitempty"`
	RequestURL      string            `json:"request_url"`
	ResponseStatus  int               `json:"response_status"`
	ResponseHeaders map[string]string `json:"response_headers,omitempty"`
	ResponseBody    string            `json:"response_body,omitempty"`
	ErrorCode       string            `json:"error_code,omitempty"`
	ErrorMessage    string            `json:"error_message,omitempty"`
	Suggestion      string            `json:"suggestion,omitempty"`
}

// DiagnoseAuth performs a lightweight diagnostic request to help identify authentication issues.
func (c *HTTPTableClient) DiagnoseAuth(ctx context.Context) *AuthDiagnostic {
	diag := &AuthDiagnostic{
		Endpoint:    c.endpoint,
		AccountName: c.accountName,
		TableName:   c.tableName,
	}

	switch {
	case c.useBearerToken:
		diag.AuthMode = "ManagedIdentity (Bearer)"
	case c.useSAS:
		diag.AuthMode = "SAS"
	default:
		diag.AuthMode = "SharedKey"
	}

	if c.useBearerToken && c.managedCred != nil {
		token, err := c.managedCred.GetToken(ctx)
		if err != nil {
			diag.TokenAcquired = false
			diag.TokenError = err.Error()
			diag.Suggestion = "Token acquisition failed. Check: (1) IDENTITY_ENDPOINT and IDENTITY_HEADER env vars are set, " +
				"(2) Managed identity is assigned to the Container App, " +
				"(3) If using user-assigned identity, ManagedIdentityID matches the client ID"
			return diag
		}
		diag.TokenAcquired = true

		claims, err := jwt.DecodeClaims(token)
		if err == nil {
			diag.TokenAudience = jwt.ClaimString(claims, "aud")
			diag.TokenIssuer = jwt.ClaimString(claims, "iss")
			diag.TokenTenantID = jwt.ClaimString(claims, "tid")
			diag.TokenObjectID = jwt.ClaimString(claims, "oid")
		}
	}

	reqURL := c.endpoint + "/Tables?$top=1"
	if c.useSAS {
		reqURL += "&" + c.sasToken
	}
	diag.RequestURL = reqURL

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		diag.ErrorMessage = fmt.Sprintf("create request: %v", err)
		return diag
	}

	if err := c.signRequest(req, "GET", nil, "/Tables"); err != nil {
		diag.ErrorMessage = err.Error()
		return diag
	}
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		diag.ErrorMessage = fmt.Sprintf("request failed (network): %v", err)
		diag.Suggestion = "Network error. Check: (1) Storage account firewall allows Container App outbound IPs or VNet, " +
			"(2) Private endpoint DNS is configured correctly, " +
			"(3) Storage account endpoint is reachable"
		return diag
	}
	defer closeResponseBody(resp)

	diag.ResponseStatus = resp.StatusCode

	diag.ResponseHeaders = make(map[string]string)
	for _, key := range []string{
		"x-ms-error-code", "x-ms-request-id", "WWW-Authenticate",
		"x-ms-version", "Server", "Date", "Content-Type",
	} {
		if v := resp.Header.Get(key); v != "" {
			diag.ResponseHeaders[key] = v
		}
	}

	body, readErr := httputil.ReadResponseBody(resp)
	if readErr != nil {
		diag.ErrorMessage = readErr.Error()
		return diag
	}
	if len(body) > 2048 {
		diag.ResponseBody = string(body[:2048]) + "...(truncated)"
	} else {
		diag.ResponseBody = string(body)
	}

	if resp.StatusCode == http.StatusOK {
		diag.Suggestion = "Authentication is working correctly."
		return diag
	}

	azErr := ParseAzureError(resp, body)
	diag.ErrorCode = azErr.Code
	diag.ErrorMessage = azErr.Message

	switch {
	case resp.StatusCode == http.StatusForbidden && (diag.ErrorCode == "AuthenticationFailed" || strings.Contains(diag.ErrorMessage, "AuthenticationFailed")):
		diag.Suggestion = "Azure rejected the authentication token. Check: " +
			"(1) Token audience should be 'https://storage.azure.com' (got: '" + diag.TokenAudience + "'), " +
			"(2) Token tenant ID should match the storage account's tenant, " +
			"(3) The storage account exists and the endpoint URL is correct"

	case resp.StatusCode == http.StatusForbidden && (diag.ErrorCode == "AuthorizationPermissionMismatch" || strings.Contains(diag.ErrorMessage, "AuthorizationPermissionMismatch")):
		diag.Suggestion = "Token is valid but identity lacks permissions. Assign 'Storage Table Data Contributor' role to the managed identity " +
			"(Object ID: " + diag.TokenObjectID + ") on the storage account. " +
			"Note: 'Contributor' and 'Owner' roles do NOT grant data-plane access."

	case resp.StatusCode == http.StatusForbidden && (diag.ErrorCode == "AuthorizationFailure" || strings.Contains(diag.ErrorMessage, "AuthorizationFailure")):
		diag.Suggestion = "Authorization failed. This usually means no RBAC role is assigned at all. " +
			"Assign 'Storage Table Data Contributor' to the managed identity on the storage account."

	case resp.StatusCode == http.StatusUnauthorized:
		diag.Suggestion = "Request was not authenticated. Check: (1) Bearer token is present in Authorization header, " +
			"(2) Token has not expired, (3) Storage account allows AAD authentication"

	default:
		diag.Suggestion = fmt.Sprintf("Unexpected status %d. Check Azure Storage account health and network connectivity.", resp.StatusCode)
	}

	return diag
}
