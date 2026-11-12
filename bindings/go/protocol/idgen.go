package protocol

import "github.com/xconnio/xconn/bindings/go/internal/bridge"

func GenerateID() uint64 { return bridge.Uint(bridge.Must("generate_id")) }
