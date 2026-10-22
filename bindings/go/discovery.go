package xconn

import (
	"context"
	"fmt"
	"sync"
)

// Endpoint advertises connection hints and capabilities, never secret credentials.
type Endpoint struct {
	ID           string         `json:"id"`
	Addresses    []string       `json:"addresses"`
	Capabilities []string       `json:"capabilities"`
	Metadata     map[string]any `json:"metadata"`
}

type Discovery struct {
	session *Session
	prefix  string
}

func (s *Session) Discovery(prefix string) *Discovery { return &Discovery{session: s, prefix: prefix} }

func endpointFields(endpoint Endpoint) map[string]any {
	addresses, capabilities, metadata := endpoint.Addresses, endpoint.Capabilities, endpoint.Metadata
	if addresses == nil {
		addresses = []string{}
	}
	if capabilities == nil {
		capabilities = []string{}
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	return map[string]any{"id": endpoint.ID, "addresses": addresses, "capabilities": capabilities, "metadata": metadata}
}

func decodeEndpoint(value any) Endpoint {
	fields := nativeFields(value)
	endpoint := Endpoint{ID: stringField(fields, "id"), Metadata: nativeFields(fields["metadata"])}
	for _, value := range fields["addresses"].([]any) {
		endpoint.Addresses = append(endpoint.Addresses, value.(string))
	}
	for _, value := range fields["capabilities"].([]any) {
		endpoint.Capabilities = append(endpoint.Capabilities, value.(string))
	}
	return endpoint
}

func (d *Discovery) request(ctx context.Context, action string, args ...any) (any, error) {
	if args == nil {
		args = []any{}
	}
	return invoke(
		ctx,
		"service_discovery_request",
		map[string]any{"session": d.session.handle, "prefix": d.prefix, "action": action, "args": args},
	)
}

func (d *Discovery) Advertise(ctx context.Context, endpoint Endpoint) error {
	_, err := d.request(ctx, "advertise", endpointFields(endpoint))
	return err
}

func (d *Discovery) Withdraw(ctx context.Context, id string) error {
	_, err := d.request(ctx, "withdraw", id)
	return err
}

func (d *Discovery) Get(ctx context.Context, id string) (Endpoint, error) {
	value, err := d.request(ctx, "get", id)
	if err != nil {
		return Endpoint{}, err
	}
	return decodeEndpoint(value), nil
}

func (d *Discovery) List(ctx context.Context) ([]Endpoint, error) {
	value, err := d.request(ctx, "list")
	if err != nil {
		return nil, err
	}
	values := value.([]any)
	endpoints := make([]Endpoint, len(values))
	for index, value := range values {
		endpoints[index] = decodeEndpoint(value)
	}
	return endpoints, nil
}

type DiscoveryAction string

const (
	DiscoveryAdvertise DiscoveryAction = "advertise"
	DiscoveryDiscover  DiscoveryAction = "discover"
	DiscoveryWithdraw  DiscoveryAction = "withdraw"
)

// DiscoveryService owns directory registrations. Applications must explicitly
// authorize every operation, including which entries each caller can discover.
type DiscoveryService struct {
	handle    uint64
	callbacks *foreignCallbacks
	once      sync.Once
	err       error
}

func (s *Session) ServeDiscovery(ctx context.Context, prefix string,
	authorize func(CallerIdentity, DiscoveryAction, Endpoint) bool) (*DiscoveryService, error) {
	if authorize == nil {
		return nil, fmt.Errorf("discovery authorization policy is required")
	}
	callbacks, err := newForeignCallbacks(ctx, func(method string, value any) (any, error) {
		if method != "discovery_authorize" {
			return nil, unexpectedCallback(method)
		}
		fields := nativeFields(value)
		return authorize(
			CallerIdentity{
				SessionID: nativeUint64(fields["session_id"]),
				AuthID:    stringField(fields, "authid"),
				AuthRole:  stringField(fields, "authrole"),
			},
			DiscoveryAction(stringField(fields, "action")),
			decodeEndpoint(fields["endpoint"]),
		), nil
	})
	if err != nil {
		return nil, err
	}
	value, err := invoke(
		ctx,
		"service_discovery_start",
		map[string]any{"session": s.handle, "prefix": prefix, "callback": callbacks.handle},
	)
	if err != nil {
		callbacks.close()
		return nil, err
	}
	service := &DiscoveryService{handle: nativeUint64(nativeFields(value)["handle"]), callbacks: callbacks}
	go func() {
		select {
		case <-ctx.Done():
		case <-s.Done():
		case <-callbacks.done:
		}
		_ = service.CloseContext(context.WithoutCancel(ctx))
	}()
	return service, nil
}

func (s *DiscoveryService) Close() error { return s.CloseContext(context.Background()) }

func (s *DiscoveryService) CloseContext(ctx context.Context) error {
	s.once.Do(func() {
		s.callbacks.close()
		_, s.err = invoke(ctx, "service_discovery_close", map[string]any{"handle": s.handle})
		closeHandle(s.handle)
	})
	return s.err
}
