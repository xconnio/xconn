package xconn

func (r *Result) ArgString(index int) (string, error) { return NewList(r.Args).String(index) }

func (r *Result) ArgStringOr(index int, fallback string) string {
	return NewList(r.Args).StringOr(index, fallback)
}

func (r *Result) ArgBool(index int) (bool, error) { return NewList(r.Args).Bool(index) }

func (r *Result) ArgBoolOr(index int, fallback bool) bool {
	return NewList(r.Args).BoolOr(index, fallback)
}

func (r *Result) ArgInt64(index int) (int64, error) { return NewList(r.Args).Int64(index) }

func (r *Result) ArgInt64Or(index int, fallback int64) int64 {
	return NewList(r.Args).Int64Or(index, fallback)
}

func (r *Result) ArgUInt64(index int) (uint64, error) { return NewList(r.Args).UInt64(index) }

func (r *Result) ArgUInt64Or(index int, fallback uint64) uint64 {
	return NewList(r.Args).UInt64Or(index, fallback)
}

func (r *Result) ArgFloat64(index int) (float64, error) { return NewList(r.Args).Float64(index) }

func (r *Result) ArgFloat64Or(index int, fallback float64) float64 {
	return NewList(r.Args).Float64Or(index, fallback)
}

func (r *Result) ArgBytes(index int) ([]byte, error) { return NewList(r.Args).Bytes(index) }

func (r *Result) ArgBytesOr(index int, fallback []byte) []byte {
	return NewList(r.Args).BytesOr(index, fallback)
}

func (r *Result) ArgList(index int) (List, error) { return NewList(r.Args).List(index) }

func (r *Result) ArgListOr(index int, fallback List) List {
	return NewList(r.Args).ListOr(index, fallback)
}

func (r *Result) ArgDict(index int) (Dict, error) { return NewList(r.Args).Dict(index) }

func (r *Result) ArgDictOr(index int, fallback Dict) Dict {
	return NewList(r.Args).DictOr(index, fallback)
}

func (r *Result) KwargString(key string) (string, error) { return NewDict(r.Kwargs).String(key) }

func (r *Result) KwargStringOr(key string, fallback string) string {
	return NewDict(r.Kwargs).StringOr(key, fallback)
}

func (r *Result) KwargBool(key string) (bool, error) { return NewDict(r.Kwargs).Bool(key) }

func (r *Result) KwargBoolOr(key string, fallback bool) bool {
	return NewDict(r.Kwargs).BoolOr(key, fallback)
}

func (r *Result) KwargInt64(key string) (int64, error) { return NewDict(r.Kwargs).Int64(key) }

func (r *Result) KwargInt64Or(key string, fallback int64) int64 {
	return NewDict(r.Kwargs).Int64Or(key, fallback)
}

func (r *Result) KwargUInt64(key string) (uint64, error) { return NewDict(r.Kwargs).UInt64(key) }

func (r *Result) KwargUInt64Or(key string, fallback uint64) uint64 {
	return NewDict(r.Kwargs).UInt64Or(key, fallback)
}

func (r *Result) KwargFloat64(key string) (float64, error) { return NewDict(r.Kwargs).Float64(key) }

func (r *Result) KwargFloat64Or(key string, fallback float64) float64 {
	return NewDict(r.Kwargs).Float64Or(key, fallback)
}

func (r *Result) KwargBytes(key string) ([]byte, error) { return NewDict(r.Kwargs).Bytes(key) }

func (r *Result) KwargBytesOr(key string, fallback []byte) []byte {
	return NewDict(r.Kwargs).BytesOr(key, fallback)
}

func (r *Result) KwargList(key string) (List, error) { return NewDict(r.Kwargs).List(key) }

func (r *Result) KwargListOr(key string, fallback List) List {
	return NewDict(r.Kwargs).ListOr(key, fallback)
}

func (r *Result) KwargDict(key string) (Dict, error) { return NewDict(r.Kwargs).Dict(key) }

func (r *Result) KwargDictOr(key string, fallback Dict) Dict {
	return NewDict(r.Kwargs).DictOr(key, fallback)
}
