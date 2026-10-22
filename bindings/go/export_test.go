package xconn

// RealmPolicyDone exposes bridge lifetime solely to the external regression suite.
func RealmPolicyDone(r *Router, name string) <-chan struct{} {
	r.policyMu.Lock()
	defer r.policyMu.Unlock()
	return r.policies[name].callbacks.done
}
