package middleware

import (
	"context"
	"github.com/kubemq-io/kubemq-community/services/authentication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"strings"
)

func UnaryAuthServerInterceptor(auth *authentication.Service) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

		if auth != nil && !strings.Contains(info.FullMethod, "Ping") {
			md, ok := metadata.FromIncomingContext(ctx)
			if ok {
				authHeader, ok := md["authorization"]
				if !ok {
					return nil, status.Errorf(codes.Unauthenticated, "Authorization token is not supplied")
				}

				_, err := auth.Authenticate(authHeader[0])
				if err != nil {
					return nil, status.Errorf(codes.Unauthenticated, err.Error())
				}
			} else {
				return nil, status.Errorf(codes.InvalidArgument, "Retrieving metadata is failed")
			}
		}
		resp, err := handler(ctx, req)
		return resp, err
	}
}

func StreamAuthServerInterceptor(auth *authentication.Service) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if auth != nil {
			md, ok := metadata.FromIncomingContext(stream.Context())
			if ok {
				authHeader, ok := md["authorization"]
				if !ok {
					return status.Errorf(codes.Unauthenticated, "Authorization token is not supplied")
				}
				_, err := auth.Authenticate(authHeader[0])
				if err != nil {

					return status.Errorf(codes.Unauthenticated, err.Error())
				}
			} else {
				return status.Errorf(codes.InvalidArgument, "Retrieving metadata is failed")
			}
		}
		return handler(srv, stream)
	}
}
