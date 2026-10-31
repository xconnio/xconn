//go:build !cgo || (!linux && !darwin && (!windows || !amd64))

package native

import "fmt"

func Call(uint32, uint64, any) (any, error) {
	return nil, fmt.Errorf("xconn requires cgo on Linux, macOS, or Windows amd64")
}
