package protocol

import (
	"encoding/json"
	"testing"
)

func TestMessageTypeValuesAreStable(t *testing.T) {
	// These integers are persisted on the wire. Reordering the iota block is
	// a breaking protocol change even though the Go code would still compile.
	want := map[MessageType]int{Req: 0, Resp: 1, Err: 2, Notify: 3}
	for messageType, value := range want {
		if int(messageType) != value {
			t.Fatalf("message type %v = %d, want %d", messageType, messageType, value)
		}
	}
}

func TestWireDTOContracts(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{
			name:  "file transfer",
			value: FileUploadReq{CommandId: "cmd-1", FileId: 7, FileName: "image.tar", FileSize: 1024, Checksum: "abc123", AllowKey: "grant"},
			want:  `{"command_id":"cmd-1","file_id":7,"file_name":"image.tar","file_size":1024,"checksum":"abc123","allow_key":"grant"}`,
		},
		{
			name:  "command result",
			value: CommandResultNotify{CommandId: "cmd-1", Success: true, Message: "done"},
			want:  `{"command_id":"cmd-1","success":true,"message":"done"}`,
		},
		{
			name: "firmware deployment request",
			value: FirmwareDeployReq{
				CommandId: "cmd-2", FileId: 8, FileName: "charger.bin", TargetVersion: "1.2.3",
				ChargePointIds: []string{"CP-1", "CP-2"}, Retries: 2, RetryInterval: 60,
			},
			want: `{"command_id":"cmd-2","file_id":8,"file_name":"charger.bin","target_version":"1.2.3","charge_point_ids":["CP-1","CP-2"],"retries":2,"retry_interval":60}`,
		},
		{
			name: "firmware deployment response",
			value: FirmwareDeployResp{
				Status: "OK", RequestedChargePointIds: []string{"CP-1"},
				Skipped: []FirmwareDeploySkip{{ChargePointId: "CP-2", Reason: "charging"}},
			},
			want: `{"status":"OK","requested_charge_point_ids":["CP-1"],"skipped":[{"charge_point_id":"CP-2","reason":"charging"}]}`,
		},
		{
			name:  "ticker config",
			value: TickerConfigReq{CpDuration: 1, PcDuration: 2, SyncDuration: 3, RecordDuration: 4},
			want:  `{"cp_duration":1,"pc_duration":2,"sync_duration":3,"record_duration":4}`,
		},
		{
			name: "charge point status with daily reboot count",
			value: ChargePointStat{
				Serial: "CP-1", LastStatus: "Available", LastStatusDetail: "NoError",
				LastStatusTimestamp: "2026-07-20 12:00:00", LastDisconnectedTime: "-", DailyRebootCount: 2,
			},
			want: `{"s":"CP-1","ls":"Available","lsd":"NoError","lst":"2026-07-20 12:00:00","ldct":"-","daily_reboot_count":2}`,
		},
		{
			name:  "trigger",
			value: MessageTriggerReq{CommandId: "cmd-1", MsgType: "PcStatus", MinAfter: 5},
			want:  `{"command_id":"cmd-1","msg_type":"PcStatus","min_after":5}`,
		},
		{
			name:  "terminal open",
			value: TerminalOpenReq{TermId: "term-1", Cols: 80, Rows: 24},
			want:  `{"term_id":"term-1","cols":80,"rows":24}`,
		},
		{
			name:  "terminal data",
			value: TerminalDataNotify{TermId: "term-1", Payload: "aGVsbG8="},
			want:  `{"term_id":"term-1","payload":"aGVsbG8="}`,
		},
		{
			name:  "terminal resize",
			value: TerminalResizeNotify{TermId: "term-1", Cols: 100, Rows: 40},
			want:  `{"term_id":"term-1","cols":100,"rows":40}`,
		},
		{
			name:  "terminal close without optional reason",
			value: TerminalCloseNotify{TermId: "term-1"},
			want:  `{"term_id":"term-1"}`,
		},
		{
			name:  "protocol error",
			value: ErrorResp{Code: "unknown_action", Message: "unknown action"},
			want:  `{"code":"unknown_action","message":"unknown action"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != test.want {
				t.Fatalf("wire JSON = %s, want %s", got, test.want)
			}
		})
	}
}

func TestMessageEnvelopeContract(t *testing.T) {
	message := Message{
		Id: "msg-1", Type: Notify, Action: "CommandResult",
		Data: json.RawMessage(`{"success":true}`),
	}
	got, err := message.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"msg-1","type":3,"action":"CommandResult","data":{"success":true}}`
	if string(got) != want {
		t.Fatalf("message JSON = %s, want %s", got, want)
	}
}
