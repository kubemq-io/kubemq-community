package middleware

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"google.golang.org/grpc"
)

func UnaryLoggerServerInterceptor(l *logging.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

		l.Debugw("start rpc call", "call", info.FullMethod)
		resp, err := handler(ctx, req)
		if err != nil {
			l.Debugw("rpc call ends with error", "call", info.FullMethod, "error", err.Error())
		} else {
			l.Debugw("rpc call ended successfully", "call", info.FullMethod)
		}
		return resp, err
	}
}

func StreamLoggerServerInterceptor(l *logging.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		l.Debugw("start rpc call", "call", info.FullMethod)
		err := handler(srv, stream)
		if err != nil {
			l.Debugw("rpc call ends with error", "call", info.FullMethod, "error", err.Error())
		} else {
			l.Debugw("rpc call ended successfully", "call", info.FullMethod)
		}
		return err
	}
}
