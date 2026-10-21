package native

const (
	Submit uint32 = iota + 1
	WaitReady
	Take
	Cancel
	Execute
	NextCompletion
)
