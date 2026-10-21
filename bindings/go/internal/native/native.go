//go:build cgo && (linux || darwin)

package native

/*
#cgo !xconn_static pkg-config: xconn
#cgo xconn_static pkg-config: xconn-static
#include "xconn.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/xconnio/xconn-core/bindings/go/internal/valuebridge"
)

const expectedABIVersion = 1

func Call(operation uint32, handle uint64, value any) (any, error) {
	if C.XC_ABI_VERSION != expectedABIVersion || C.xc_abi_version() != expectedABIVersion {
		return nil, fmt.Errorf("incompatible XConn ABI version")
	}

	var input []byte
	var err error

	if operation == 1 || operation == 5 {
		input, err = valuebridge.Encode(value)

		if err != nil {
			return nil, err
		}
	}

	if len(input) > 64*1024*1024 {
		return nil, fmt.Errorf("binding input exceeds 64 MiB")
	}

	var pointer *C.uint8_t

	if len(input) > 0 {
		pointer = (*C.uint8_t)(unsafe.Pointer(&input[0]))
	}

	output := C.xc_process(C.uint32_t(operation), C.uint64_t(handle), pointer, C.size_t(len(input)))
	if output == nil {
		return nil, fmt.Errorf("native allocation failed")
	}

	defer C.xc_result_free(output)

	if output.len > 64*1024*1024 {
		return nil, fmt.Errorf("binding result exceeds 64 MiB")
	}

	data := C.GoBytes(unsafe.Pointer(output.data), C.int(output.len))
	if output.status != 0 {
		return nil, fmt.Errorf("xconn: %s", data)
	}

	return valuebridge.Decode(data)
}
