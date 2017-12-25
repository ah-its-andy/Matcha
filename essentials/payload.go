package essentials

type Payload struct {
	Env           string                 `json:"env"`
	ClientTag     string                 `json:"client_tag"`
	MessageType   string                 `json:"type"`
	Content       string                 `json:"content"`
	Subscriptions []*SubscriptionPayload `json:"subs"`
	Extensions    map[string]string      `json:"exts"`
	Headers       map[string]interface{} `json:"headers"`
}

type SubscriptionPayload struct {
	Tag      string `json:"tag"`
	Exchange string `json:"exchange"`
	RouteKey string `json:"key"`
}

type ConfirmPayload struct {
	Tag         string `json:"tag"`
	MessageID   string `json:"message_id"`
	ConfirmTime string `json:"confirm_time"`
}

type ExtensionsEventArgs struct {
	Extensions map[string]string
	MessageID  string
}
