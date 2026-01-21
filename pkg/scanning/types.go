package scanning

import (
	"encoding/json"
)

type Version int

const (
	V1 Version = iota + 1
	V2
)

type Scan struct {
	Ip          string      `json:"ip"`
	Port        uint32      `json:"port"`
	Service     string      `json:"service"`
	Timestamp   int64       `json:"timestamp"`
	DataVersion Version     `json:"data_version"`
	Data        interface{} `json:"data"`
}

func (s *Scan) UnmarshalJSON(b []byte) error {
	type Alias Scan
	aux := &struct {
		Data json.RawMessage `json:"data"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	switch s.DataVersion {
	case V1:
		var v1 V1Data
		if err := json.Unmarshal(aux.Data, &v1); err != nil {
			return err
		}
		s.Data = &v1
	case V2:
		var v2 V2Data
		if err := json.Unmarshal(aux.Data, &v2); err != nil {
			return err
		}
		s.Data = &v2
	}
	return nil
}

type V1Data struct {
	ResponseBytesUtf8 []byte `json:"response_bytes_utf8"`
}

type V2Data struct {
	ResponseStr string `json:"response_str"`
}
