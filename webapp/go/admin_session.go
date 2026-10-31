package isureal

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

type AdminSession struct {
	Token           string    `db:"-" json:"-"`
	AdministratorID string    `db:"administrator_id"`
	TokenHash       string    `db:"token_hash"`
	CreatedAt       time.Time `db:"created_at"`
}

func GetAdminSession(c *echo.Context) *AdminSession {
	session, _ := c.Get("admin_session").(*AdminSession)
	return session
}

func getAdminSessionFromHeader(c *echo.Context, db *sqlx.DB) (*AdminSession, error) {
	authHeader := c.Request().Header.Get(echo.HeaderAuthorization)
	if authHeader == "" {
		return nil, nil
	}
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, NewAPIError(http.StatusUnauthorized, "invalid_administrator_session")
	}
	token := strings.TrimPrefix(authHeader, "Bearer ")
	hash := sha256.Sum256([]byte(token))
	var session AdminSession
	err := db.GetContext(c.Request().Context(), &session, `
  SELECT administrator_id, token_hash, created_at
  FROM administrator_sessions
  WHERE token_hash = $1 AND active = true
 `, hex.EncodeToString(hash[:]))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get admin session: %w", err)
	}
	return &session, nil
}

func AdminSessionMiddleware(h *HandlerContext, authRequired bool) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			session, err := getAdminSessionFromHeader(c, h.DB)
			if err != nil {
				return err
			}
			if session == nil && authRequired {
				return NewAPIError(http.StatusUnauthorized, "invalid_administrator_session")
			}
			c.Set("admin_session", session)
			return next(c)
		}
	}
}

func CreateAdminSession(db *sqlx.DB, administratorID string) (*AdminSession, error) {
	ctx := context.Background()
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	sessionID, token := uuid.NewString(), uuid.NewString()
	hash := sha256.Sum256([]byte(token))
	if _, err := tx.ExecContext(ctx, `
  INSERT INTO administrator_sessions (id, administrator_id, token_hash)
  VALUES ($1, $2, $3)
 `, sessionID, administratorID, hex.EncodeToString(hash[:])); err != nil {
		return nil, fmt.Errorf("failed to insert admin session: %w", err)
	}
	session, err := getAdminSession(ctx, tx, sessionID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	session.Token = token
	return session, nil
}

func getAdminSession(ctx context.Context, db sqlx.QueryerContext, sessionID string) (*AdminSession, error) {
	var session AdminSession
	if err := sqlx.GetContext(ctx, db, &session, `SELECT administrator_id, token_hash, created_at FROM administrator_sessions WHERE id = $1`, sessionID); err != nil {
		return nil, fmt.Errorf("failed to get admin session: %w", err)
	}
	return &session, nil
}

func InvalidateAdminSession(db *sqlx.DB, administratorID, tokenHash string) error {
	if _, err := db.Exec(`
  UPDATE administrator_sessions
  SET active = false, invalidated_at = clock_timestamp()
  WHERE administrator_id = $1 AND token_hash = $2 AND active = true
 `, administratorID, tokenHash); err != nil {
		return fmt.Errorf("failed to invalidate admin session: %w", err)
	}
	return nil
}
