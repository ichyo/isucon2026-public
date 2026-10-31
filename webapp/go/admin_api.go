package isureal

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

type adminLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type adminLoginResponse struct {
	Token           string `json:"token"`
	AdministratorID string `json:"administratorId"`
}

func adminLoginHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var input adminLoginRequest
		if err := c.Bind(&input); err != nil {
			return err
		}
		if !usernamePattern.MatchString(input.Username) {
			return NewAPIError(http.StatusUnauthorized, "invalid_credentials")
		}
		var administrator struct {
			ID           string `db:"id"`
			PasswordHash string `db:"password_hash"`
		}
		err := h.DB.GetContext(c.Request().Context(), &administrator,
			`SELECT id, password_hash FROM administrators WHERE username = $1`, input.Username)
		if errors.Is(err, sql.ErrNoRows) {
			return NewAPIError(http.StatusUnauthorized, "invalid_credentials")
		}
		if err != nil {
			return err
		}
		hash := sha256.Sum256([]byte(input.Password))
		if administrator.PasswordHash != hex.EncodeToString(hash[:]) {
			return NewAPIError(http.StatusUnauthorized, "invalid_credentials")
		}
		session, err := CreateAdminSession(h.DB, administrator.ID)
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, adminLoginResponse{
			Token: session.Token, AdministratorID: session.AdministratorID,
		})
	}
}

func adminLogoutHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		session := GetAdminSession(c)
		if session == nil {
			return NewAPIError(http.StatusUnauthorized, "invalid_administrator_session")
		}
		if err := InvalidateAdminSession(h.DB, session.AdministratorID, session.TokenHash); err != nil {
			return err
		}
		return c.NoContent(http.StatusNoContent)
	}
}
