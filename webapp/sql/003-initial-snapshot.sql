DROP TABLE IF EXISTS
  initial_data_boundary,
  initial_users,
  initial_friend_requests,
  initial_friendships,
  initial_follows,
  initial_block_relations;

CREATE TABLE initial_data_boundary AS
SELECT greatest(
  (SELECT max(created_at) FROM users),
  (SELECT max(created_at) FROM image_objects),
  (SELECT max(created_at) FROM posting_windows),
  (SELECT max(created_at) FROM posts),
  (SELECT max(created_at) FROM realmojis),
  (SELECT max(created_at) FROM comments),
  (SELECT max(created_at) FROM notifications)
) AS created_at;

CREATE TABLE initial_users AS SELECT * FROM users;
CREATE TABLE initial_friend_requests AS SELECT * FROM friend_requests;
CREATE TABLE initial_friendships AS SELECT * FROM friendships;
CREATE TABLE initial_follows AS SELECT * FROM follows;
CREATE TABLE initial_block_relations AS SELECT * FROM block_relations;
