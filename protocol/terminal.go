package protocol

// TerminalOpenReq asks a site to start a new interactive shell session (a
// pty running locally on the site's own machine — not a separate SSH hop).
// Sent as a Req; the site's Resp is a StatusResp. Once open, I/O flows both
// ways as TerminalDataNotify messages tagged with the same TermId.
type TerminalOpenReq struct {
	TermId string `json:"term_id"`
	Cols   int    `json:"cols"`
	Rows   int    `json:"rows"`
}

// TerminalDataNotify carries one chunk of terminal I/O. Server→site is
// keyboard input, site→server is shell output. Sent as a Notify: chunks are
// frequent enough that a Req/Resp round trip per chunk would add needless
// latency and overhead.
type TerminalDataNotify struct {
	TermId  string `json:"term_id"`
	Payload string `json:"payload"`
}

// TerminalResizeNotify tells the site's pty to resize.
type TerminalResizeNotify struct {
	TermId string `json:"term_id"`
	Cols   int    `json:"cols"`
	Rows   int    `json:"rows"`
}

// TerminalCloseNotify signals that one side is done with the terminal
// session (browser tab closed, shell process exited, ...). The receiver
// should tear down its half if it hasn't already.
type TerminalCloseNotify struct {
	TermId string `json:"term_id"`
	Reason string `json:"reason,omitempty"`
}
