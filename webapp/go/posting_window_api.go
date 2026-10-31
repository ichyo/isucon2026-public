package isureal

import (
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

type currentPostingWindowResponse struct {
	ID             string    `json:"id"`
	Region         string    `json:"region"`
	LocalDate      string    `json:"localDate"`
	StartsAt       time.Time `json:"startsAt"`
	OnTimeDeadline time.Time `json:"onTimeDeadline"`
}

type getCurrentPostingWindowResponse struct {
	Window        *currentPostingWindowResponse `json:"window"`
	HasPosted     bool                          `json:"hasPosted"`
	CanPost       bool                          `json:"canPost"`
	DeletionCount int                           `json:"deletionCount"`
}

func getCurrentPostingWindowHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var state struct {
			PostingWindow
			Active  int `db:"active"`
			Deleted int `db:"deleted"`
		}
		err := h.DB.GetContext(c.Request().Context(), &state, `
   SELECT w.id, w.region, w.local_date, w.starts_at, w.on_time_deadline, w.ended_at,
          count(p.id) FILTER (WHERE p.deleted_at IS NULL) AS active,
          count(p.id) FILTER (WHERE p.deleted_at IS NOT NULL) AS deleted
   FROM users u JOIN posting_windows w ON w.region = u.region AND w.ended_at IS NULL
   LEFT JOIN posting_windows posted_window ON posted_window.local_date = w.local_date
   LEFT JOIN posts p ON p.author_id = u.id AND p.posting_window_id = posted_window.id
   WHERE u.id = $1
   GROUP BY w.id
  `, GetSession(c).UserID)
		if errors.Is(err, sql.ErrNoRows) {
			return c.JSON(http.StatusOK, getCurrentPostingWindowResponse{})
		}
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, getCurrentPostingWindowResponse{
			Window: &currentPostingWindowResponse{
				ID: state.ID, Region: state.Region, LocalDate: state.LocalDate.Format(time.DateOnly),
				StartsAt: state.StartsAt, OnTimeDeadline: state.OnTimeDeadline,
			},
			HasPosted:     state.Active > 0,
			CanPost:       state.Active == 0 && state.Deleted < 2 && !time.Now().Before(state.StartsAt),
			DeletionCount: state.Deleted,
		})
	}
}
