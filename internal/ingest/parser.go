package ingest

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
)

type RequestEnvelope struct {
	APIVersion string `json:"api_version"`
	Source     struct {
		SDK     string `json:"sdk"`
		Version string `json:"version"`
		Service string `json:"service"`
	} `json:"source"`
	Events []json.RawMessage `json:"events"`
}

func ParseEvents(r *http.Request, maxBodyBytes int64) ([][]byte, error) {
	defer r.Body.Close()

	reader := io.Reader(r.Body)
	if strings.Contains(strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))), "gzip") {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		reader = gz
	}

	payload, err := io.ReadAll(io.LimitReader(reader, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > maxBodyBytes {
		return nil, fmt.Errorf("payload exceeds max body bytes")
	}
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return nil, nil
	}

	switch payload[0] {
	case '[':
		var arr []json.RawMessage
		if err := json.Unmarshal(payload, &arr); err != nil {
			return nil, err
		}
		out := make([][]byte, 0, len(arr))
		for _, item := range arr {
			out = append(out, bytes.TrimSpace([]byte(item)))
		}
		return out, nil
	case '{':
		if bytes.Contains(payload, []byte{'\n'}) {
			ndjson := ParseNDJSON(payload)
			if len(ndjson) > 1 {
				return ndjson, nil
			}
		}
		var envelope RequestEnvelope
		if err := json.Unmarshal(payload, &envelope); err == nil && len(envelope.Events) > 0 {
			out := make([][]byte, 0, len(envelope.Events))
			for _, item := range envelope.Events {
				out = append(out, bytes.TrimSpace([]byte(item)))
			}
			return out, nil
		}
		return [][]byte{payload}, nil
	default:
		return ParseNDJSON(payload), nil
	}
}

func ParseNDJSON(payload []byte) [][]byte {
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	buffer := make([]byte, 0, 1024*1024)
	scanner.Buffer(buffer, math.MaxInt32)

	out := make([][]byte, 0, 64)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		copyLine := make([]byte, len(line))
		copy(copyLine, line)
		out = append(out, copyLine)
	}
	return out
}
