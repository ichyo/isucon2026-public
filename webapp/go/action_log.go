package isureal

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/labstack/echo/v5"
)

var actionNames = map[string]string{
	"POST /api/initialize":                          "initialize",
	"POST /api/auth/register":                       "register",
	"POST /api/auth/login":                          "login",
	"POST /api/auth/logout":                         "logout",
	"GET /api/users":                                "search_users",
	"GET /api/users/:userId":                        "get_user",
	"GET /api/me/export":                            "export_posts",
	"GET /api/me":                                   "get_me",
	"PATCH /api/me":                                 "update_me",
	"PUT /api/me/profile-image":                     "put_profile_image",
	"DELETE /api/me/profile-image":                  "delete_profile_image",
	"POST /api/me/account-deletion":                 "delete_account",
	"POST /api/posts":                               "create_post",
	"GET /api/posting-window/current":               "get_current_posting_window",
	"GET /api/posts/me/current":                     "get_my_current_post",
	"GET /api/posts/:postId":                        "get_post",
	"PATCH /api/posts/:postId":                      "update_post",
	"DELETE /api/posts/:postId":                     "delete_post",
	"GET /api/feeds/:feed":                          "get_feed",
	"GET /api/memories":                             "get_memories",
	"GET /api/memories/:date":                       "get_memories_by_date",
	"POST /api/posts/:postId/comments":              "create_comment",
	"GET /api/posts/:postId/comments":               "get_comments",
	"DELETE /api/comments/:commentId":               "delete_comment",
	"POST /api/posts/:postId/realmojis":             "replace_real_moji",
	"GET /api/posts/:postId/realmojis":              "get_real_mojis",
	"DELETE /api/realmojis/:realMojiId":             "delete_real_moji",
	"GET /api/images/:imageId":                      "get_image",
	"GET /api/notifications":                        "get_notifications",
	"POST /api/friend-requests":                     "create_friend_request",
	"POST /api/friend-requests/:requestId/:action":  "transition_friend_request",
	"GET /api/relationships/:kind":                  "get_relationships",
	"DELETE /api/friendships/:userId":               "delete_friendship",
	"PUT /api/follows/:userId":                      "follow",
	"DELETE /api/follows/:userId":                   "unfollow",
	"PUT /api/blocks/:userId":                       "block",
	"DELETE /api/blocks/:userId":                    "unblock",
	"POST /api/admin/auth/login":                    "admin_login",
	"POST /api/admin/auth/logout":                   "admin_logout",
	"PUT /api/admin/users/:userId/certification":    "certify_user",
	"DELETE /api/admin/users/:userId/certification": "delete_user_certification",
	"POST /api/admin/posting-window":                "open_posting_window",
}

var logLevelRanks = map[string]int{"debug": 0, "info": 1, "error": 2}

func ActionLogMiddleware(minLevel string, output io.Writer) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			err := next(c)
			if err != nil {
				c.Echo().HTTPErrorHandler(c, err)
			}
			_, status := echo.ResolveResponseStatus(c.Response(), err)

			level := "info"
			switch {
			case status >= http.StatusInternalServerError:
				level = "error"
			case c.Request().Method == http.MethodGet && !(status == http.StatusForbidden && c.Get("abuse") == "invalid_signature"):
				level = "debug"
			}
			if logLevelRanks[level] < logLevelRanks[minLevel] {
				return nil
			}

			route := c.RouteInfo()
			entry := map[string]any{
				"level":   level,
				"action":  nil,
				"status":  status,
				"ip":      c.RealIP(),
				"user_id": nil,
			}
			action, ok := actionNames[route.Method+" "+route.Path]
			if ok {
				entry["action"] = action
			}
			if session := GetSession(c); session != nil {
				entry["user_id"] = session.UserID
			}
			if action == "login" || action == "register" {
				entry["username"] = nil
				if username, _ := c.Get("username").(string); usernamePattern.MatchString(username) {
					entry["username"] = username
				}
			}
			abuse, _ := c.Get("abuse").(string)
			if abuse != "" {
				entry["abuse"] = abuse
			}
			line, marshalErr := json.Marshal(entry)
			if marshalErr != nil {
				return marshalErr
			}
			_, writeErr := output.Write(append(line, '\n'))
			return writeErr
		}
	}
}
