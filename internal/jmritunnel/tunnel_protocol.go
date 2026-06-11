// Package jmritunnel defines the multiplexing protocol used between the
// layout agent and the MavSphere backend for JMRI panel proxying.
//
// The agent opens a single outbound WebSocket to the backend
// (/api/ws/jmri-tunnel). Over this connection, both HTTP request/response
// pairs and WebSocket streams are multiplexed using JSON frames tagged with
// a unique connection ID.
//
// Frame types (agent→backend and backend→agent):
//
//	HTTP_REQ    backend→agent: proxy an HTTP request to local JMRI
//	HTTP_RESP   agent→backend: HTTP response from JMRI
//	WS_OPEN     backend→agent: open a WebSocket to local JMRI
//	WS_FRAME    bidirectional: a WebSocket message frame
//	WS_CLOSE    bidirectional: close a WebSocket connection
//	PANEL_LIST  backend→agent: request list of open JMRI panels
//	PANEL_LIST_RESP agent→backend: panel list response
//	ERROR       agent→backend: error for a specific connection ID
package jmritunnel

// MsgType identifies the tunnel frame type.
type MsgType string

const (
	MsgHTTPReq       MsgType = "HTTP_REQ"
	MsgHTTPResp      MsgType = "HTTP_RESP"
	MsgWSOpen        MsgType = "WS_OPEN"
	MsgWSFrame       MsgType = "WS_FRAME"
	MsgWSClose       MsgType = "WS_CLOSE"
	MsgPanelList     MsgType = "PANEL_LIST"
	MsgPanelListResp MsgType = "PANEL_LIST_RESP"
	MsgError         MsgType = "ERROR"
)

// Frame is the envelope for all tunnel messages.
// ConnID groups related HTTP or WebSocket exchanges.
type Frame struct {
	Type   MsgType `json:"type"`
	ConnID string  `json:"connId"`

	// HTTP_REQ fields (backend→agent)
	Method  string            `json:"method,omitempty"`
	Path    string            `json:"path,omitempty"`  // e.g. "/panel/Layout/Main"
	Query   string            `json:"query,omitempty"` // raw query string
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"body,omitempty"` // base64-encoded by JSON marshaller

	// HTTP_RESP fields (agent→backend)
	Status      int               `json:"status,omitempty"`
	RespHeaders map[string]string `json:"respHeaders,omitempty"`
	RespBody    []byte            `json:"respBody,omitempty"` // base64-encoded

	// WS_FRAME fields (bidirectional)
	WSData   []byte `json:"wsData,omitempty"` // base64-encoded
	WSBinary bool   `json:"wsBinary,omitempty"`

	// PANEL_LIST_RESP / ERROR
	Panels []PanelInfo `json:"panels,omitempty"`
	ErrMsg string      `json:"errMsg"` // no omitempty — always serialise so Java never gets null
}

// PanelInfo describes a single open JMRI panel.
type PanelInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`     // "Layout", "ControlPanel", "Panel", "Switchboard"
	JmriPath string `json:"jmriPath"` // e.g. "/panel/Layout/Main"
}

// AllowedPathPrefixes is the set of JMRI URL paths the agent will proxy.
// Anything not matching is rejected by the agent before hitting JMRI.
var AllowedPathPrefixes = []string{
	// JMRI top-level pages. Include both the exact path and subpaths because
	// JMRI menu links commonly use /panel as well as /panel/....
	"/panel",
	"/tables",
	"/json", // JMRI JSON API + WebSocket
	"/roster",
	"/operations",
	"/help",
	"/permission",
	"/config",
	"/about",
	"/prefs",
	"/program",
	"/profile",

	// Static assets and icon libraries used by JMRI panels.
	"/web/",
	"/css/",
	"/js/",
	"/images/",
	"/icons/",
	"/resources/",
	"/xml/",
	"/dist/",
	"/webjars/",
	"/fonts/",
	"/font/",

	"/api/jmri/", // agent's own panel-list endpoint (served locally)
}

// BlockedPathPrefixes are never proxied regardless of allow-list.
var BlockedPathPrefixes = []string{
	"/preferences",
	"/shutdown",
	"/setTurnoutState", // raw DCC command endpoints
}
