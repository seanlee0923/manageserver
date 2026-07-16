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
