package ingest

type Response struct {
	Accepted int `json:"accepted"`
	Invalid  int `json:"invalid"`
	Rejected int `json:"rejected"`
}
