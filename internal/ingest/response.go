package ingest

type Response struct {
	Accepted int `json:"accepted"`
	Deduped  int `json:"deduped,omitempty"`
	Invalid  int `json:"invalid"`
	Rejected int `json:"rejected"`
}
