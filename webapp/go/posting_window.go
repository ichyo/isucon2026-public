package isureal

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

type PostingWindow struct {
	ID             string     `db:"id"`
	Region         string     `db:"region"`
	LocalDate      time.Time  `db:"local_date"`
	StartsAt       time.Time  `db:"starts_at"`
	OnTimeDeadline time.Time  `db:"on_time_deadline"`
	EndedAt        *time.Time `db:"ended_at"`
}

func getPostingWindow(ctx context.Context, db sqlx.QueryerContext, windowID string) (PostingWindow, error) {
	var window PostingWindow
	err := sqlx.GetContext(ctx, db, &window, `SELECT id, region, local_date, starts_at, on_time_deadline, ended_at FROM posting_windows WHERE id = $1`, windowID)
	return window, err
}
