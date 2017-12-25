package essentials

type ChangeStatePayload struct {
	MessageID  string            `json:"message_id"`
	NewState   string            `json:"state"`
	ClientTag  string            `json:"tag"`
	Remark     string            `json:"remark"`
	Extensions map[string]string `json:"exts"`
}
