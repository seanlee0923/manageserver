package protocol

type LoginResp struct {
	Result       bool   `json:"result"`
	NextPassword string `json:"next_password"`
}

type StatusResp struct {
	Ok      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

type HeartBeatResp struct {
	Ok bool `json:"ok"`
}

// FileUploadReq asks a site to download a file to local disk. This is
// transfer-only — it carries no deploy instructions (no service name,
// version, or file type/target selector). What a site does with a
// transferred file is a separate, not-yet-introduced command's concern.
type FileUploadReq struct {
	CommandId string `json:"command_id"`
	FileId    int    `json:"file_id"`
	FileName  string `json:"file_name"`
	FileSize  int64  `json:"file_size"`
	AllowKey  string `json:"allow_key"`
}

type TickerConfigReq struct {
	CpDuration     int `json:"cp_duration"`
	PcDuration     int `json:"pc_duration"`
	SyncDuration   int `json:"sync_duration"`
	RecordDuration int `json:"record_duration"`
}

type MessageTriggerReq struct {
	MsgType  string `json:"msg_type"`
	MinAfter int    `json:"min_after"`
}
