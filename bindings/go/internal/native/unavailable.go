//go:build !cgo || (!linux && !darwin)

package native

import "fmt"

func Call(uint32, uint64, any) (any, error) {
	return nil, fmt.Errorf("xconn requires cgo on Linux or macOS")
}
