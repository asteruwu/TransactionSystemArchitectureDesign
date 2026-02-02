package rpc

import (
	"context"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/userservice/genproto"
	"github.com/GoogleCloudPlatform/microservices-demo/src/userservice/pkg/service"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UserService struct {
	pb.UnimplementedUserServiceServer
	Logic service.UserServiceLogic
}

func (s *UserService) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	userID, err := s.Logic.Register(ctx, req.Username, req.Password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register: %v", err)
	}
	return &pb.RegisterResponse{UserId: userID}, nil
}

func (s *UserService) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	userID, token, err := s.Logic.Login(ctx, req.Username, req.Password)
	if err != nil {
		if err.Error() == "invalid credentials" {
			return nil, status.Error(codes.Unauthenticated, "invalid username or password")
		}
		return nil, status.Errorf(codes.Internal, "failed to login: %v", err)
	}
	return &pb.LoginResponse{
		UserId: userID,
		Token:  token,
	}, nil
}

func (s *UserService) VerifyToken(ctx context.Context, req *pb.VerifyTokenRequest) (*pb.VerifyTokenResponse, error) {
	uid, valid := s.Logic.VerifyToken(ctx, req.Token)
	if !valid {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	return &pb.VerifyTokenResponse{
		IsValid: valid,
		UserId:  uid,
	}, nil
}
