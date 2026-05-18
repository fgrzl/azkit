package tables

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/fgrzl/azkit/credentials"
	"github.com/fgrzl/azkit/internal/validate"
)

const (
	HTTPRequestTimeout      = 30 * time.Second
	HTTPConnectTimeout      = 5 * time.Second
	HTTPKeepAlive           = 30 * time.Second
	HTTPIdleConnTimeout     = 90 * time.Second
	HTTPTLSHandshakeTimeout = 10 * time.Second
	HTTPMaxIdleConns        = 100
	HTTPMaxIdleConnsPerHost = 100

	AzureBatchMaxEntities = 100
	AzureDefaultPageSize  = 1000
	AzureAPIVersion       = "2021-06-08"

	MaxRetryAttempts  = 3
	InitialRetryDelay = 100 * time.Millisecond
	MaxRetryDelay     = 10 * time.Second

	BufferPoolDefaultSize = 32 * 1024
)

// HTTPTableClient provides a lightweight native Go HTTP client for Azure Table Storage REST API
type HTTPTableClient struct {
	endpoint       string
	tableName      string
	accountName    string
	accountKey     string
	sasToken       string
	managedCred    *credentials.ManagedIdentityCredential
	httpClient     *http.Client
	allowInsecure  bool
	useBearerToken bool
	useSAS         bool
}

func resolveEndpoint(accountName, customEndpoint string, allowInsecure bool) string {
	if customEndpoint != "" {
		return customEndpoint
	}
	scheme := "https"
	if allowInsecure {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s.table.core.windows.net", scheme, accountName)
}

// NewHTTPTableClient creates a new HTTP-based Table Storage client with SharedKey authentication
func NewHTTPTableClient(accountName, accountKey, tableName string, allowInsecure bool, customEndpoint string) (*HTTPTableClient, error) {
	if accountName == "" || accountKey == "" {
		return nil, fmt.Errorf("account name and key are required")
	}
	if err := validate.AccountKeyBase64(accountKey); err != nil {
		return nil, err
	}

	return &HTTPTableClient{
		endpoint:       resolveEndpoint(accountName, customEndpoint, allowInsecure),
		tableName:      tableName,
		accountName:    accountName,
		accountKey:     accountKey,
		httpClient:     newOptimizedHTTPClient(),
		allowInsecure:  allowInsecure,
		useBearerToken: false,
		useSAS:         false,
	}, nil
}

// NewHTTPTableClientWithSAS creates a client with SAS token authentication
func NewHTTPTableClientWithSAS(accountName, sasToken, tableName string, allowInsecure bool, customEndpoint string) (*HTTPTableClient, error) {
	if accountName == "" || sasToken == "" {
		return nil, fmt.Errorf("account name and SAS token are required")
	}

	return &HTTPTableClient{
		endpoint:       resolveEndpoint(accountName, customEndpoint, allowInsecure),
		tableName:      tableName,
		accountName:    accountName,
		sasToken:       sasToken,
		httpClient:     newOptimizedHTTPClient(),
		allowInsecure:  allowInsecure,
		useBearerToken: false,
		useSAS:         true,
	}, nil
}

// NewHTTPTableClientWithManagedIdentity creates a client with Managed Identity (Bearer token) authentication
func NewHTTPTableClientWithManagedIdentity(accountName string, managedCred *credentials.ManagedIdentityCredential, tableName string, allowInsecure bool, customEndpoint string) (*HTTPTableClient, error) {
	if accountName == "" {
		return nil, fmt.Errorf("account name is required")
	}
	if managedCred == nil {
		return nil, fmt.Errorf("managed identity credential is required")
	}

	return &HTTPTableClient{
		endpoint:       resolveEndpoint(accountName, customEndpoint, allowInsecure),
		tableName:      tableName,
		accountName:    accountName,
		managedCred:    managedCred,
		httpClient:     newOptimizedHTTPClient(),
		allowInsecure:  allowInsecure,
		useBearerToken: true,
		useSAS:         false,
	}, nil
}

// SetHTTPClient sets the HTTP client used for requests (e.g. for tests or custom transport).
func (c *HTTPTableClient) SetHTTPClient(client *http.Client) {
	if client != nil {
		c.httpClient = client
	}
}

// AccountName returns the storage account name.
func (c *HTTPTableClient) AccountName() string { return c.accountName }

// Endpoint returns the table endpoint URL.
func (c *HTTPTableClient) Endpoint() string { return c.endpoint }

// TableName returns the table name.
func (c *HTTPTableClient) TableName() string { return c.tableName }

// UseBearerToken returns true if the client uses Managed Identity (Bearer) auth.
func (c *HTTPTableClient) UseBearerToken() bool { return c.useBearerToken }

func newOptimizedHTTPClient() *http.Client {
	return &http.Client{
		Timeout: HTTPRequestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        HTTPMaxIdleConns,
			MaxIdleConnsPerHost: HTTPMaxIdleConnsPerHost,
			IdleConnTimeout:     HTTPIdleConnTimeout,
			DisableCompression:  true,
			DialContext: (&net.Dialer{
				Timeout:   HTTPConnectTimeout,
				KeepAlive: HTTPKeepAlive,
			}).DialContext,
			TLSHandshakeTimeout:   HTTPTLSHandshakeTimeout,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		},
	}
}

// buildTableURL returns endpoint/tableName with optional SAS query string.
func (c *HTTPTableClient) buildTableURL() string {
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	expectedLen := len(c.endpoint) + 1 + len(c.tableName)
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	return b.String()
}

// buildBatchURL returns endpoint/$batch with optional SAS query string.
func (c *HTTPTableClient) buildBatchURL() string {
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	expectedLen := len(c.endpoint) + 7
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteString("/$batch")
	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	return b.String()
}

// buildEntityURL returns endpoint/table(PartitionKey='...',RowKey='...') with an optional query suffix.
func (c *HTTPTableClient) buildEntityURL(pk, rk, querySuffix string) string {
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	expectedLen := len(c.endpoint) + 1 + len(c.tableName) + 16 +
		len(pk)*3 + 11 + len(rk)*3 + 2 + len(querySuffix)
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	b.WriteString("(PartitionKey='")
	b.WriteString(url.QueryEscape(pk))
	b.WriteString("',RowKey='")
	b.WriteString(url.QueryEscape(rk))
	b.WriteString("')")
	b.WriteString(querySuffix)
	return b.String()
}

func (c *HTTPTableClient) entityQuerySuffix(formatJSON bool) string {
	if c.useSAS {
		return "?" + c.sasToken
	}
	if formatJSON {
		return "?$format=json"
	}
	return ""
}
