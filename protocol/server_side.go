package protocol

type LoginResp struct {
	Result       bool   `json:"result"`
	NextPassword string `json:"next_password"`
}

type StatusResp struct {
	Ok bool `json:"ok"`
}

type HeartBeatResp struct {
	Ok bool `json:"ok"`
}

type FileUploadReq struct {
	FileId      int    `json:"file_id"`
	FileName    string `json:"file_name"`
	FileSize    int64  `json:"file_size"`
	AllowKey    string `json:"allow_key"`
	FileType    string `json:"file_type"`
	ServiceName string `json:"service_name"`
	Version     string `json:"version"`
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

type BootNotificationCntReq struct {
	ChargePointId string `json:"charge_point_id"`
	SearchStDate  string `json:"search_st_date"`
	SearchEndDate string `json:"search_end_date"`
}
