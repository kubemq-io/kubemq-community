package grpc

import (
	"context"
	"fmt"

	pb "github.com/kubemq-io/protobuf/go"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/interfaces/grpc/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type grpcClient struct {
	pb.KubemqClient
	*grpc.ClientConn
}
type ClientOptions struct {
	Address  string
	isSecure bool

	certFile       string
	certDomain     string
	keepAliveParam *keepalive.ClientParameters
}

func NewClientOptions(addr string) *ClientOptions {
	return &ClientOptions{
		Address: addr,
	}
}

//TODO - Add test for secure gRPC
func (co *ClientOptions) SetSecureConnection(certFile, certDomain string) *ClientOptions {
	co.certFile = certFile
	co.certDomain = certDomain
	co.isSecure = true
	return co
}

func GetClientConn(ctx context.Context, co *ClientOptions) (conn *grpc.ClientConn, err error) {
	var connOptions []grpc.DialOption
	if co.isSecure {
		creds, err := credentials.NewClientTLSFromFile(co.certFile, co.certDomain)
		if err != nil {
			return nil, fmt.Errorf("could not load tls cert: %s", err)
		}
		connOptions = append(connOptions, grpc.WithTransportCredentials(creds))
	} else {
		connOptions = append(connOptions, grpc.WithInsecure())
	}
	if co.keepAliveParam != nil {
		connOptions = append(connOptions, grpc.WithKeepaliveParams(*co.keepAliveParam))
	}
	connOptions = append(connOptions, grpc.WithStreamInterceptor(middleware.ChainStreamClient(
	//		grpc_prometheus.StreamClientInterceptor,
	//		ot.StreamClientInterceptor(ot.WithTracer(tracing.GetTracer())),
	//grpc_zap.StreamClientInterceptor(co.logger.Desugar()),
	)), grpc.WithUnaryInterceptor(middleware.ChainUnaryClient(
	//grpc_prometheus.UnaryClientInterceptor,
	//ot.UnaryClientInterceptor(ot.WithTracer(tracing.GetTracer())),
	//grpc_zap.UnaryClientInterceptor(co.logger.Desugar()),
	)),
	)
	conn, err = grpc.DialContext(ctx, co.Address, connOptions...)
	if err != nil {
		return nil, err
	}
	go func() {

		<-ctx.Done()
		if conn != nil {
			_ = conn.Close()
		}

	}()
	return conn, nil
}

func SendRequest(ctx context.Context, appConfig *config.Config, req *pb.Request) (*pb.Response, error) {
	conn, err := GetClientConn(ctx, &ClientOptions{
		Address: fmt.Sprintf(":%d", appConfig.Grpc.Port),
	})
	if err != nil {
		return nil, err
	}
	client := pb.NewKubemqClient(conn)
	res, err := client.SendRequest(ctx, req)
	return res, err
}

func SendResponse(ctx context.Context, appConfig *config.Config, res *pb.Response) error {

	conn, err := GetClientConn(ctx, &ClientOptions{
		Address: fmt.Sprintf(":%d", appConfig.Grpc.Port),
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()
	client := pb.NewKubemqClient(conn)
	_, err = client.SendResponse(ctx, res)
	return err
}

func SubscribeToRequests(ctx context.Context, appConfig *config.Config, subReq *pb.Subscribe, reqCh chan *pb.Request, errCh chan error) error {
	conn, err := GetClientConn(ctx, &ClientOptions{
		Address: fmt.Sprintf(":%d", appConfig.Grpc.Port),
	})
	if err != nil {
		return err
	}

	client := pb.NewKubemqClient(conn)

	sub, err := client.SubscribeToRequests(ctx, subReq)
	if err != nil {
		return err
	}

	go func() {
		for {
			req, err := sub.Recv()
			if err != nil {
				errCh <- err
				_ = conn.Close()
				return
			}
			reqCh <- req
		}
	}()

	go func() {
		for {
			<-ctx.Done()
			_ = conn.Close()
			return
		}
	}()

	return nil
}

func SendEvents(ctx context.Context, appConfig *config.Config, msg *pb.Event) (*pb.Result, error) {
	conn, err := GetClientConn(ctx, &ClientOptions{
		Address: fmt.Sprintf(":%d", appConfig.Grpc.Port),
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()
	client := pb.NewKubemqClient(conn)
	md, err := client.SendEvent(ctx, msg)
	return md, err
}

func EventsStreamMessage(ctx context.Context, appConfig *config.Config, msgCh chan *pb.Event, msgDlvCh chan *pb.Result) error {
	conn, err := GetClientConn(ctx, &ClientOptions{
		Address: fmt.Sprintf(":%d", appConfig.Grpc.Port),
	})
	if err != nil {
		return err
	}
	client := pb.NewKubemqClient(conn)

	sender, err := client.SendEventsStream(ctx)
	if err != nil {
		return err
	}
	go func() {
		for {
			msgDlv, err := sender.Recv()
			if err != nil {
				_ = conn.Close()
				return
			}
			msgDlvCh <- msgDlv

		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				_ = conn.Close()
				return
			case msg := <-msgCh:
				if err := sender.Send(msg); err != nil {
					return
				}
			}
		}
	}()

	return nil
}

func SubscribeToEvents(ctx context.Context, appConfig *config.Config, subReq *pb.Subscribe, msgCh chan *pb.EventReceive, errCh chan error) error {
	conn, err := GetClientConn(ctx, &ClientOptions{
		Address: fmt.Sprintf(":%d", appConfig.Grpc.Port),
	})
	if err != nil {
		return err
	}
	client := pb.NewKubemqClient(conn)
	sub, err := client.SubscribeToEvents(ctx, subReq)
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			msg, err := sub.Recv()
			if err != nil {
				errCh <- err
				_ = sub.CloseSend()
				_ = conn.Close()
				return
			}
			msgCh <- msg
		}
	}()

	go func() {
		<-ctx.Done()
		_ = sub.CloseSend()
		_ = conn.Close()
	}()
	return nil
}
