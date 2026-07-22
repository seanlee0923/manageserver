package protocol

// CommandResultNotify reports the final outcome of a previously dispatched
// command, correlated by CommandId. The dispatch side (e.g. a Req like
// FileUploadReq) already returns an immediate synchronous ack/nack for
// "did the site start working on this" — this is the async follow-up for
// "did it actually finish", sent as a Notify since the sender doesn't need
// (or want to block on) a reply.
type CommandResultNotify struct {
	CommandId string `json:"command_id"`
	Success   bool   `json:"success"`
	Message   string `json:"message"`
}

// FirmwareDeployReq asks a site to expose an already-transferred file on its
// local firmware HTTP server and request OCPP UpdateFirmware for the selected
// charge points. ChargePointIds is deliberately a list: one CSMS action can
// target multiple charge points at once.
//
// The request does not mean that firmware installation completed. The caller
// must confirm completion later from the firmware version reported after the
// charge point's BootNotification.
type FirmwareDeployReq struct {
	CommandId      string   `json:"command_id"`
	FileId         int      `json:"file_id"`
	FileName       string   `json:"file_name"`
	TargetVersion  string   `json:"target_version"`
	ChargePointIds []string `json:"charge_point_ids"`
	Retries        int      `json:"retries,omitempty"`
	RetryInterval  int      `json:"retry_interval,omitempty"`
}

// FirmwareDeploySkip describes a selected charge point that the site did not
// include in the UpdateFirmware request, for example because it was charging.
type FirmwareDeploySkip struct {
	ChargePointId string `json:"charge_point_id"`
	Reason        string `json:"reason"`
}

// FirmwareDeployResp is only the immediate dispatch result. Requested charge
// points have entered the "waiting for BootNotification" phase; they have not
// necessarily downloaded or installed the firmware yet.
type FirmwareDeployResp struct {
	Status                  string               `json:"status"`
	RequestedChargePointIds []string             `json:"requested_charge_point_ids"`
	Skipped                 []FirmwareDeploySkip `json:"skipped"`
	Message                 string               `json:"message,omitempty"`
}
