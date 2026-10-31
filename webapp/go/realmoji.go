package isureal

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

type RealMoji struct {
	ID           string     `db:"id"`
	PostID       string     `db:"post_id"`
	ReactorID    string     `db:"reactor_id"`
	ImageID      string     `db:"image_id"`
	ReactionType string     `db:"reaction_type"`
	CreatedAt    time.Time  `db:"created_at"`
	ReplacedAt   *time.Time `db:"replaced_at"`
	DeletedAt    *time.Time `db:"deleted_at"`
}

func getRealMoji(ctx context.Context, db sqlx.QueryerContext, id string) (RealMoji, error) {
	var reaction RealMoji
	err := sqlx.GetContext(ctx, db, &reaction, `SELECT id, post_id, reactor_id, image_id, reaction_type, created_at, replaced_at, deleted_at FROM realmojis WHERE id=$1`, id)
	return reaction, err
}

type RealMojiWithReactor struct {
	RealMoji
	Reactor User `db:"reactor"`
}

func selectCurrentRealMojis(ctx context.Context, db sqlx.QueryerContext, conditions string, args ...any) ([]RealMojiWithReactor, error) {
	var reactions []RealMojiWithReactor
	if err := sqlx.SelectContext(ctx, db, &reactions, `
  SELECT r.id, r.post_id, r.reactor_id, r.image_id, r.reaction_type, r.created_at, r.replaced_at, r.deleted_at,
         `+userColumnsAs("u", "reactor")+`
  FROM realmojis r JOIN users u ON u.id = r.reactor_id AND u.status = 'active'
  WHERE r.replaced_at IS NULL AND r.deleted_at IS NULL `+conditions, args...); err != nil {
		return nil, err
	}
	for i := range reactions {
		if err := reactions[i].Reactor.setImageKey(); err != nil {
			return nil, err
		}
	}
	return reactions, nil
}
