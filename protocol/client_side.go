package protocol

type LoginReq struct {
	Id       string `json:"id"`
	Password string `json:"password"`
}

type HeartBeatReq struct {
}

type PcStatusReq struct {
	CpuUsage      float64         `json:"cpu_usage"`
	CpuCores      int             `json:"cpu_cores"`
	MemTotal      float64         `json:"mem_total"`
	MemUsage      float64         `json:"mem_usage"`
	MemPercent    float64         `json:"mem_percent"`
	DiskTotal     float64         `json:"disk_total"`
	DiskUsage     float64         `json:"disk_usage"`
	DiskPercent   float64         `json:"disk_percent"`
	PacketIn      uint64          `json:"packet_in"`
	PacketOut     uint64          `json:"packet_out"`
	DropIn        uint64          `json:"drop_in"`
	DropOut       uint64          `json:"drop_out"`
	ErrIn         uint64          `json:"err_in"`
	ErrOut        uint64          `json:"err_out"`
	ContainerList []ContainerInfo `json:"container_list"`
	ServerTime    string          `json:"st"`
}

type ServerInfoReq struct {
	AnydeskId      string            `json:"anydesk_id"`
	ServiceVersion map[string]string `json:"svc_version"`
}

type ContainerInfo struct {
	Service string `json:"Service"`
	State   string `json:"State"`
	Status  string `json:"Status"`
}

type ChargePointSyncReq struct {
	ChargePointList []ChargePointDetail `json:"cp_list"`
	ServerTime      string              `json:"st"`
}

type ChargePointStatusReq struct {
	ChargePointList []ChargePointStat `json:"cp_list"`
	ServerTime      string            `json:"st"`
}

type ChargePointBootSummaryReq struct {
	ChargePointList []ChargePointBootCnt `json:"cp_list"`
	ServerTime      string               `json:"st"`
}

type ChargePointDetail struct {
	Serial          string `json:"s"`
	FirmwareVersion string `json:"fver"`
	Kwh             string `json:"kwh"`
	IpAddr          string `json:"ip_addr"`
	ChargeAreaName  string `json:"ca_name"`
	ChargePointName string `json:"cp_name"`
}

type ChargePointStat struct {
	Serial               string `json:"s"`
	LastStatus           string `json:"ls"`
	LastStatusDetail     string `json:"lsd"`
	LastStatusTimestamp  string `json:"lst"`
	LastDisconnectedTime string `json:"ldct"`
	DailyRebootCount     int    `json:"daily_reboot_count"`
}

type ChargePointBootCnt struct {
	Serial string `json:"s"`
	Count  int    `json:"c"`
}

type ChargeRecordStatusReq struct {
	NegativeCnt        int              `json:"ne_cnt"`
	HighUsageCnt       int              `json:"high_cnt"`
	AbnormalRecordList []AbnormalRecord `json:"ab_record"`
}

type AbnormalRecord struct {
	TransactionId     int    `json:"trId"`
	ChargeAreaName    string `json:"caname"`
	ChargePointName   string `json:"cpname"`
	ChargePointSerial string `json:"cpid"`
	ChargeStDate      string `json:"stdate"`
	ChargeEndDate     string `json:"endate"`
	Usage             string `json:"usage"`
}

// RecordAlertReq carries only newly-detected abnormal charge records for the
// fast, event-driven "RecordAlert" action. Unlike ChargeRecordStatusReq (the
// long-duration full sync, which also reports site-wide totals), this is
// alert-only: a non-empty list means "something new just happened", so it
// carries no NegativeCnt/HighUsageCnt totals — those would be misleading
// here since this payload is a delta, not the full current count.
type RecordAlertReq struct {
	AbnormalRecordList []AbnormalRecord `json:"ab_record"`
}

// BootAlertReq carries only the charge points that newly crossed the reboot
// alert threshold, for the fast, event-driven "BootAlert" action. Unlike
// ChargePointBootSummaryReq (the long-duration full sync), this is
// alert-only and carries no ServerTime/full-list semantics.
type BootAlertReq struct {
	ChargePointList []ChargePointBootCnt `json:"cp_list"`
}

type FileUploadResp struct {
	Status string `json:"st"`
}
