package plugin

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/sqlds/v4"
)

func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	clickhousePlugin := Clickhouse{}
	ds := sqlds.NewDatasource(&clickhousePlugin)
	pluginSettings := clickhousePlugin.Settings(ctx, settings)
	if pluginSettings.ForwardHeaders {
		ds.EnableMultipleConnections = true
	}

	// Load settings so we can build the GreptimeDB health check URL
	loadedSettings, err := LoadSettings(ctx, settings)
	if err != nil {
		return nil, err
	}

	// Override the health check to use GreptimeDB's native /v1/sql endpoint
	// instead of the ClickHouse driver's PingContext(), which sends ClickHouse
	// wire protocol that GreptimeDB doesn't understand.
	ds.PreCheckHealth = func(ctx context.Context, req *backend.CheckHealthRequest) *backend.CheckHealthResult {
		result, err := greptimeHealthCheck(ctx, loadedSettings)
		if err != nil {
			return &backend.CheckHealthResult{
				Status:  backend.HealthStatusError,
				Message: fmt.Sprintf("Health check failed: %s", err),
			}
		}
		return result
	}

	return ds.NewDatasource(ctx, settings)
}

// greptimeHealthCheck performs a direct HTTP health check against GreptimeDB's /v1/sql endpoint
func greptimeHealthCheck(ctx context.Context, settings Settings) (*backend.CheckHealthResult, error) {
	// Build the GreptimeDB /v1/sql URL from the configured host
	host := strings.TrimRight(settings.Host, "/")
	// If host doesn't include a scheme, assume http
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = fmt.Sprintf("http://%s:%d", host, settings.Port)
	}
	sqlURL := fmt.Sprintf("%s/v1/sql", host)

	db := settings.DefaultDatabase
	if db == "" {
		db = "public"
	}

	// POST a simple SELECT 1 as form-encoded data
	form := url.Values{}
	form.Set("sql", "SELECT 1")

	httpReq, err := http.NewRequestWithContext(ctx, "POST", sqlURL+"?db="+url.QueryEscape(db), strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("Connection failed: %s", err),
		}, nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("GreptimeDB returned HTTP %d: %s", resp.StatusCode, string(body)),
		}, nil
	}

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Database Connection OK",
	}, nil
}
