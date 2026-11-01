package xconn

// RealmPolicyDone exposes bridge lifetime solely to the external regression suite.
func RealmPolicyDone(r *Router, name string) <-chan struct{} {
	r.policyMu.Lock()
	defer r.policyMu.Unlock()
	return r.policies[name].callbacks.done
}

// CachedPeerSessions observes adapter allocation without opening a direct view.
func CachedPeerSessions(s *Session) int {
	if s.peer == nil {
		return 0
	}
	s.peer.mu.Lock()
	defer s.peer.mu.Unlock()
	return len(s.peer.sessions)
}
