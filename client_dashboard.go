package sls

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
)

type ChartSearch struct {
	Logstore string `json:"logstore"`
	Topic    string `json:"topic"`
	Query    string `json:"query"`
	Start    string `json:"start"`
	End      string `json:"end"`
}

type ChartDisplay struct {
	XAxisKeys   []string `json:"xAxis,omitempty"`
	YAxisKeys   []string `json:"yAxis,omitempty"`
	XPosition   float64  `json:"xPos"`
	YPosition   float64  `json:"yPos"`
	Width       float64  `json:"width"`
	Height      float64  `json:"height"`
	DisplayName string   `json:"displayName"`
}

type Chart struct {
	Title   string       `json:"title"`
	Type    string       `json:"type"`
	Search  ChartSearch  `json:"search"`
	Display ChartDisplay `json:"display"`
}

type Dashboard struct {
	DashboardName string  `json:"dashboardName"`
	Description   string  `json:"description"`
	ChartList     []Chart `json:"charts"`
	DisplayName   string  `json:"displayName"`
}

type ResponseDashboardItem struct {
	DashboardName string `json:"dashboardName"`
	DisplayName   string `json:"displayName"`
}

func (c *Client) CreateChart(project, dashboardName string, chart Chart) error {
	body, err := json.Marshal(chart)
	if err != nil {
		return NewClientError(err)
	}

	h := map[string]string{
		"x-log-bodyrawsize": fmt.Sprintf("%v", len(body)),
		"Content-Type":      "application/json",
	}

	uri := "/dashboards/" + dashboardName + "/charts"
	r, err := c.request(project, "POST", uri, h, body)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) UpdateChart(project, dashboardName string, chart Chart) error {
	body, err := json.Marshal(chart)
	if err != nil {
		return NewClientError(err)
	}

	h := map[string]string{
		"x-log-bodyrawsize": fmt.Sprintf("%v", len(body)),
		"Content-Type":      "application/json",
	}

	uri := "/dashboards/" + dashboardName + "/charts"
	r, err := c.request(project, "PUT", uri, h, body)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) DeleteChart(project, dashboardName, chartName string) error {

	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}

	uri := "/dashboards/" + dashboardName + "/charts/" + chartName
	r, err := c.request(project, "DELETE", uri, h, nil)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) GetChart(project, dashboardName, chartName string) (chart *Chart, err error) {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}
	uri := "/dashboards/" + dashboardName + "/charts/" + chartName
	r, err := c.request(project, "GET", uri, h, nil)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, readResponseError(err)
	}
	chart = &Chart{}
	if err = json.Unmarshal(buf, chart); err != nil {
		err = NewClientError(err)
	}
	return chart, err
}

func (c *Client) CreateDashboardString(project string, dashboardStr string) error {
	body := []byte(dashboardStr)

	h := map[string]string{
		"x-log-bodyrawsize": fmt.Sprintf("%v", len(body)),
		"Content-Type":      "application/json",
	}

	uri := "/dashboards"
	r, err := c.request(project, "POST", uri, h, body)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) CreateDashboard(project string, dashboard Dashboard) error {
	body, err := json.Marshal(dashboard)
	if err != nil {
		return NewClientError(err)
	}

	h := map[string]string{
		"x-log-bodyrawsize": fmt.Sprintf("%v", len(body)),
		"Content-Type":      "application/json",
	}

	uri := "/dashboards"
	r, err := c.request(project, "POST", uri, h, body)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) UpdateDashboard(project string, dashboard Dashboard) error {
	body, err := json.Marshal(dashboard)
	if err != nil {
		return NewClientError(err)
	}

	h := map[string]string{
		"x-log-bodyrawsize": fmt.Sprintf("%v", len(body)),
		"Content-Type":      "application/json",
	}

	uri := "/dashboards/" + dashboard.DashboardName
	r, err := c.request(project, "PUT", uri, h, body)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) UpdateDashboardString(project string, dashboardName, dashboardStr string) error {
	body := []byte(dashboardStr)

	h := map[string]string{
		"x-log-bodyrawsize": fmt.Sprintf("%v", len(body)),
		"Content-Type":      "application/json",
	}

	uri := "/dashboards/" + dashboardName
	r, err := c.request(project, "PUT", uri, h, body)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) DeleteDashboard(project, name string) error {

	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}

	uri := "/dashboards/" + name
	r, err := c.request(project, "DELETE", uri, h, nil)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) GetDashboard(project, name string) (dashboard *Dashboard, err error) {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}
	uri := "/dashboards/" + name
	r, err := c.request(project, "GET", uri, h, nil)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, readResponseError(err)
	}
	dashboard = &Dashboard{}
	if err = json.Unmarshal(buf, dashboard); err != nil {
		err = NewClientError(err)
	}
	return dashboard, err
}

func (c *Client) GetDashboardString(project, name string) (dashboard string, err error) {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}
	uri := "/dashboards/" + name
	r, err := c.request(project, "GET", uri, h, nil)
	if err != nil {
		return "", err
	}
	defer r.Body.Close()
	buf, err := ioutil.ReadAll(r.Body)
	return string(buf), err
}

func (c *Client) ListDashboard(project string, dashboardName string, offset, size int) (dashboardList []string, count, total int, err error) {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
		"dashboardName":     dashboardName,
		"offset":            strconv.Itoa(offset),
		"size":              strconv.Itoa(size),
	}
	uri := "/dashboards"
	r, err := c.request(project, "GET", uri, h, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	defer r.Body.Close()
	type ListDashboardResponse struct {
		DashboardList []string `json:"dashboards"`
		Total         int      `json:"total"`
		Count         int      `json:"count"`
	}

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, 0, 0, readResponseError(err)
	}
	dashboards := &ListDashboardResponse{}
	if err = json.Unmarshal(buf, dashboards); err != nil {
		err = NewClientError(err)
	}
	return dashboards.DashboardList, dashboards.Count, dashboards.Total, err
}

func (c *Client) ListDashboardV2(project string, dashboardName string, offset, size int) (dashboardList []string, dashboardItems []ResponseDashboardItem, count, total int, err error) {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
		"dashboardName":     dashboardName,
		"offset":            strconv.Itoa(offset),
		"size":              strconv.Itoa(size),
	}
	uri := "/dashboards"
	r, err := c.request(project, "GET", uri, h, nil)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	defer r.Body.Close()

	type ListDashboardResponse struct {
		DashboardList  []string                `json:"dashboards"`
		Total          int                     `json:"total"`
		Count          int                     `json:"count"`
		DashboardItems []ResponseDashboardItem `json:"dashboardItems"`
	}

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, nil, 0, 0, readResponseError(err)
	}
	dashboards := &ListDashboardResponse{}
	if err = json.Unmarshal(buf, dashboards); err != nil {
		err = NewClientError(err)
	}
	return dashboards.DashboardList, dashboards.DashboardItems, dashboards.Count, dashboards.Total, err
}
