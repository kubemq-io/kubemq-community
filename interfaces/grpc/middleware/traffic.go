package middleware

import (
	"context"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func UnaryTrafficServerInterceptor(acceptTraffic *atomic.Bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !acceptTraffic.Load() {
			return nil, status.Error(codes.Unavailable, "grpc server is not ready to accept requests")
		}
		resp, err := handler(ctx, req)
		return resp, err
	}
}

func StreamTrafficServerInterceptor(acceptTraffic *atomic.Bool) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !acceptTraffic.Load() {
			return status.Error(codes.Unavailable, "grpc server is not ready to accept requests")
		}
		return handler(srv, stream)
	}
}
