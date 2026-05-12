package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	cfg   GRPCConfig
	state State
	ready atomic.Bool
	mu    sync.Mutex
	lis   net.Listener
	srv   *grpc.Server
}

func NewGRPCServer(cfg GRPCConfig, state State) *GRPCServer {
	return &GRPCServer{
		cfg:   cfg,
		state: state,
	}
}

func (s *GRPCServer) Name() string { return "grpc" }

func (s *GRPCServer) Addr() string { return s.cfg.Port }

func (s *GRPCServer) IsReady() bool { return s.ready.Load() }

func (s *GRPCServer) Start(ctx context.Context) error {
	if !s.cfg.Enabled {
		return nil
	}

	var opts []grpc.ServerOption

	opts = append(opts, grpc.MaxConcurrentStreams(uint32(s.cfg.MaxConcurrentStreams)))
	opts = append(opts, grpc.MaxRecvMsgSize(s.cfg.MaxRecvMsgSize))
	opts = append(opts, grpc.MaxSendMsgSize(s.cfg.MaxSendMsgSize))

	if s.cfg.MaxConnectionAge.d > 0 || s.cfg.KeepaliveTime.d > 0 {
		kep := keepalive.EnforcementPolicy{
			MinTime: s.cfg.KeepaliveTime.d,
		}
		opts = append(opts, grpc.KeepaliveEnforcementPolicy(kep))
	}

	kvp := keepalive.ServerParameters{
		MaxConnectionAge:      s.cfg.MaxConnectionAge.d,
		MaxConnectionAgeGrace: s.cfg.MaxConnectionAgeGrace.d,
		Time:                  s.cfg.KeepaliveTime.d,
		Timeout:               s.cfg.KeepaliveTimeout.d,
	}
	opts = append(opts, grpc.KeepaliveParams(kvp))

	if s.cfg.TLSEnabled {
		creds, err := credentials.NewServerTLSFromFile(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("failed to load TLS certs: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	s.srv = grpc.NewServer(opts...)

	RegisterCollectorServiceServer(s.srv, &collectorSvcServer{state: s.state})
	RegisterLogIngestServiceServer(s.srv, &logIngestSvcServer{state: s.state})

	var err error
	s.lis, err = net.Listen("tcp", s.cfg.Port)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.cfg.Port, err)
	}

	go func() {
		<-ctx.Done()
		s.srv.GracefulStop()
	}()

	s.ready.Store(true)
	return s.srv.Serve(s.lis)
}

func (s *GRPCServer) Stop(ctx context.Context) error {
	s.ready.Store(false)
	if s.srv == nil {
		return nil
	}
	s.srv.GracefulStop()
	return nil
}

type collectorSvcServer struct {
	UnimplementedCollectorServiceServer
	state State
}

func (s *collectorSvcServer) Health(ctx context.Context, req *CollectorStatusRequest) (*CollectorStatusResponse, error) {
	if s.state.IsHealthy() {
		return &CollectorStatusResponse{Status: "ok"}, nil
	}
	return &CollectorStatusResponse{Status: "unhealthy"}, nil
}

func (s *collectorSvcServer) Ready(ctx context.Context, req *CollectorStatusRequest) (*CollectorStatusResponse, error) {
	if s.state.IsReady() {
		return &CollectorStatusResponse{Status: "ready"}, nil
	}
	return &CollectorStatusResponse{Status: "not_ready"}, nil
}

type logIngestSvcServer struct {
	UnimplementedLogIngestServiceServer
	state State
}

func (s *logIngestSvcServer) Push(ctx context.Context, batch *EventBatch) (*PushResponse, error) {
	if batch == nil || batch.Events == nil {
		return &PushResponse{Accepted: 0}, nil
	}

	events := make([][]byte, 0, len(batch.Events))
	for _, event := range batch.Events {
		if event != nil && event.RawJson != "" {
			events = append(events, []byte(event.RawJson))
		}
	}

	if len(events) == 0 {
		return &PushResponse{Accepted: 0}, nil
	}

	accepted, err := s.state.Ingest(ctx, events)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ingest failed: %v", err)
	}

	return &PushResponse{Accepted: int64(accepted)}, nil
}

type CollectorStatusRequest struct{}

type CollectorStatusResponse struct {
	Status string `protobuf:"bytes,1,opt,name=status" json:"status,omitempty"`
}

func (x *CollectorStatusResponse) Reset()         { *x = CollectorStatusResponse{} }
func (x *CollectorStatusResponse) String() string   { return x.Status }
func (x *CollectorStatusResponse) GetStatus() string { return x.Status }

type EventBatch struct {
	Events []*Event `protobuf:"bytes,1,rep,name=events" json:"events,omitempty"`
}

func (x *EventBatch) Reset()         { *x = EventBatch{} }
func (x *EventBatch) String() string { return fmt.Sprintf("%v", x.Events) }

type Event struct {
	RawJson string `protobuf:"bytes,1,opt,name=raw_json,json=rawJson" json:"raw_json,omitempty"`
}

func (x *Event) Reset()         { *x = Event{} }
func (x *Event) String() string { return x.RawJson }

type PushResponse struct {
	Accepted int64 `protobuf:"varint,1,opt,name=accepted" json:"accepted,omitempty"`
}

func (x *PushResponse) Reset()         { *x = PushResponse{} }
func (x *PushResponse) String() string { return fmt.Sprintf("accepted=%d", x.Accepted) }

type CollectorServiceServer interface {
	Health(context.Context, *CollectorStatusRequest) (*CollectorStatusResponse, error)
	Ready(context.Context, *CollectorStatusRequest) (*CollectorStatusResponse, error)
}

type UnimplementedCollectorServiceServer struct{}

func (UnimplementedCollectorServiceServer) Health(context.Context, *CollectorStatusRequest) (*CollectorStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Health not implemented")
}
func (UnimplementedCollectorServiceServer) Ready(context.Context, *CollectorStatusRequest) (*CollectorStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ready not implemented")
}

type LogIngestServiceServer interface {
	Push(context.Context, *EventBatch) (*PushResponse, error)
}

type UnimplementedLogIngestServiceServer struct{}

func (UnimplementedLogIngestServiceServer) Push(context.Context, *EventBatch) (*PushResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Push not implemented")
}

func RegisterCollectorServiceServer(s *grpc.Server, srv CollectorServiceServer) {
	desc := grpc.ServiceDesc{
		ServiceName: "loxav1.CollectorService",
		HandlerType: (*CollectorServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Health",
				Handler:    _CollectorService_Health_Handler,
			},
			{
				MethodName: "Ready",
				Handler:    _CollectorService_Ready_Handler,
			},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "collector.proto",
	}
	s.RegisterService(&desc, srv)
}

func RegisterLogIngestServiceServer(s *grpc.Server, srv LogIngestServiceServer) {
	desc := grpc.ServiceDesc{
		ServiceName: "loxav1.LogIngest",
		HandlerType: (*LogIngestServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Push",
				Handler:    _LogIngest_Push_Handler,
			},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "ingest.proto",
	}
	s.RegisterService(&desc, srv)
}

func _CollectorService_Health_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CollectorStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CollectorServiceServer).Health(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/loxav1.CollectorService/Health",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CollectorServiceServer).Health(ctx, req.(*CollectorStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CollectorService_Ready_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CollectorStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CollectorServiceServer).Ready(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/loxav1.CollectorService/Ready",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CollectorServiceServer).Ready(ctx, req.(*CollectorStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LogIngest_Push_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EventBatch)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LogIngestServiceServer).Push(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/loxav1.LogIngest/Push",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LogIngestServiceServer).Push(ctx, req.(*EventBatch))
	}
	return interceptor(ctx, in, info, handler)
}