package essentials

type JSONClaimValueType struct {
	IsComplexType bool   `json:"complex_type"`
	IsBase64      bool   `json:"base64"`
	FullName      string `json:"name"`
}
