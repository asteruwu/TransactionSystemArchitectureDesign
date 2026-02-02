package service

import (
	"context"
	"errors"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/userservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/userservice/pkg/repo"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// 定义 JWT 密钥 (生产环境应从 Config/Env 读取)
var jwtSecret = []byte("my_super_secret_key_for_demo")

type UserServiceLogic interface {
	Register(ctx context.Context, username, password string) (string, error)
	Login(ctx context.Context, username, password string) (string, string, error)
	VerifyToken(ctx context.Context, tokenStr string) (string, bool)
	RefreshToken(ctx context.Context, tokenStr string) (string, int64, error)
}

type userServiceLogic struct {
	repo repo.UserRepository
}

func NewUserServiceLogic(repo repo.UserRepository) UserServiceLogic {
	return &userServiceLogic{repo: repo}
}

func (s *userServiceLogic) Register(ctx context.Context, username, password string) (string, error) {
	// 1. 检查用户是否存在 (略，DB Unique Index 会处理)

	// 2. 密码加密
	hashedPwd, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}

	// 3. 生成 UUID
	uid := uuid.New().String()

	// 4. 落库
	user := &model.User{
		UserID:   uid,
		Username: username,
		Password: string(hashedPwd),
	}

	if err := s.repo.CreateUser(ctx, user); err != nil {
		return "", err
	}

	return uid, nil
}

func (s *userServiceLogic) Login(ctx context.Context, username, password string) (string, string, error) {
	// 1. 查询用户
	user, err := s.repo.GetUserByUsername(ctx, username)
	if err != nil {
		return "", "", errors.New("invalid credentials")
	}

	// 2. 校验密码
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)); err != nil {
		return "", "", errors.New("invalid credentials")
	}

	// 3. 生成 JWT
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": user.UserID,
		"exp":     time.Now().Add(time.Hour * 24).Unix(), // 24小时过期
	})

	tokenString, err := token.SignedString(jwtSecret)
	if err != nil {
		return "", "", err
	}

	return user.UserID, tokenString, nil
}

func (s *userServiceLogic) VerifyToken(ctx context.Context, tokenStr string) (string, bool) {
	token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
		return jwtSecret, nil
	})

	if err != nil || !token.Valid {
		return "", false
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", false
	}

	return claims["user_id"].(string), true
}

func (s *userServiceLogic) RefreshToken(ctx context.Context, tokenStr string) (string, int64, error) {
	// 1. 验证现有 token
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		return jwtSecret, nil
	})
	if err != nil || !token.Valid {
		return "", 0, errors.New("invalid token")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", 0, errors.New("invalid token claims")
	}
	userID, ok := claims["user_id"].(string)
	if !ok {
		return "", 0, errors.New("invalid user_id in token")
	}

	// 2. 生成新 token（24小时）
	newExp := time.Now().Add(24 * time.Hour)
	newToken := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": userID,
		"exp":     newExp.Unix(),
	})

	tokenString, err := newToken.SignedString(jwtSecret)
	if err != nil {
		return "", 0, err
	}

	return tokenString, newExp.Unix(), nil
}
