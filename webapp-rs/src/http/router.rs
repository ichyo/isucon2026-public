//! Routing table for every API endpoint (§6) with Echo-like 404/405 semantics (§2.1).

use std::future::Future;
use std::pin::Pin;

use http::Method;
use smallvec::SmallVec;

use super::req::Req;
use super::resp::HResult;
use crate::api;
use crate::app::Ctx;

pub type BoxFut = Pin<Box<dyn Future<Output = HResult> + Send>>;
pub type HandlerFn = fn(Ctx, Req) -> BoxFut;

macro_rules! h {
    ($f:path) => {
        (|c: Ctx, r: Req| -> BoxFut { Box::pin($f(c, r)) }) as HandlerFn
    };
}

#[derive(Clone, Copy, Debug)]
enum Seg {
    Lit(&'static str),
    Param,
}

pub struct Route {
    pub method: Method,
    pub pattern: &'static str,
    /// action name (spec §8.3)
    pub name: &'static str,
    pub handler: HandlerFn,
    /// position in the routing table (metrics action id = metrics::SPECIAL.len() + idx)
    pub idx: usize,
    segs: Vec<Seg>,
    lits: usize,
}

pub enum Match<'a> {
    Found(&'a Route, SmallVec<[String; 2]>),
    /// known path, wrong method: value of the `Allow` header
    MethodNotAllowed(String),
    /// OPTIONS on a known path
    Options(String),
    NotFound,
}

pub struct Router {
    routes: Vec<Route>,
}

fn route(method: Method, pattern: &'static str, name: &'static str, handler: HandlerFn) -> Route {
    let segs: Vec<Seg> = pattern.trim_start_matches('/').split('/').map(|s| if s.starts_with(':') { Seg::Param } else { Seg::Lit(s) }).collect();
    let lits = segs.iter().filter(|s| matches!(s, Seg::Lit(_))).count();
    Route { method, pattern, name, handler, idx: 0, segs, lits }
}

const METHOD_ORDER: [Method; 6] = [Method::DELETE, Method::GET, Method::HEAD, Method::PATCH, Method::POST, Method::PUT];

impl Router {
    pub fn new() -> Router {
        use Method as M;
        let routes = vec![
            route(M::POST, "/api/initialize", "initialize", h!(api::initialize::initialize)),
            // ---- group A ----
            route(M::POST, "/api/auth/register", "register", h!(api::auth::register)),
            route(M::POST, "/api/auth/login", "login", h!(api::auth::login)),
            route(M::POST, "/api/auth/logout", "logout", h!(api::auth::logout)),
            route(M::GET, "/api/me", "get_me", h!(api::me::get_me)),
            route(M::PATCH, "/api/me", "update_me", h!(api::me::update_me)),
            route(M::PUT, "/api/me/profile-image", "put_profile_image", h!(api::me::put_profile_image)),
            route(M::DELETE, "/api/me/profile-image", "delete_profile_image", h!(api::me::delete_profile_image)),
            route(M::POST, "/api/me/account-deletion", "delete_account", h!(api::me::delete_account)),
            route(M::GET, "/api/users", "search_users", h!(api::users::search_users)),
            route(M::GET, "/api/users/:userId", "get_user", h!(api::users::get_user)),
            route(M::POST, "/api/friend-requests", "create_friend_request", h!(api::relationships::create_friend_request)),
            route(M::POST, "/api/friend-requests/:requestId/:action", "transition_friend_request", h!(api::relationships::transition_friend_request)),
            route(M::GET, "/api/relationships/:kind", "get_relationships", h!(api::relationships::get_relationships)),
            route(M::DELETE, "/api/friendships/:userId", "delete_friendship", h!(api::relationships::delete_friendship)),
            route(M::PUT, "/api/follows/:userId", "follow", h!(api::relationships::follow)),
            route(M::DELETE, "/api/follows/:userId", "unfollow", h!(api::relationships::unfollow)),
            route(M::PUT, "/api/blocks/:userId", "block", h!(api::relationships::block)),
            route(M::DELETE, "/api/blocks/:userId", "unblock", h!(api::relationships::unblock)),
            route(M::POST, "/api/admin/auth/login", "admin_login", h!(api::admin::admin_login)),
            route(M::POST, "/api/admin/auth/logout", "admin_logout", h!(api::admin::admin_logout)),
            route(M::PUT, "/api/admin/users/:userId/certification", "certify_user", h!(api::admin::certify_user)),
            route(M::DELETE, "/api/admin/users/:userId/certification", "delete_user_certification", h!(api::admin::delete_user_certification)),
            // ---- group B ----
            route(M::GET, "/api/posting-window/current", "get_current_posting_window", h!(api::windows::get_current_posting_window)),
            route(M::POST, "/api/admin/posting-window", "open_posting_window", h!(api::windows::open_posting_window)),
            route(M::POST, "/api/posts", "create_post", h!(api::posts::create_post)),
            route(M::GET, "/api/posts/me/current", "get_my_current_post", h!(api::posts::get_my_current_post)),
            route(M::GET, "/api/posts/:postId", "get_post", h!(api::posts::get_post)),
            route(M::PATCH, "/api/posts/:postId", "update_post", h!(api::posts::update_post)),
            route(M::DELETE, "/api/posts/:postId", "delete_post", h!(api::posts::delete_post)),
            route(M::GET, "/api/feeds/:feed", "get_feed", h!(api::feeds::get_feed)),
            route(M::GET, "/api/memories", "get_memories", h!(api::memories::get_memories)),
            route(M::GET, "/api/memories/:date", "get_memories_by_date", h!(api::memories::get_memories_by_date)),
            route(M::GET, "/api/me/export", "export_posts", h!(api::export::export_posts)),
            // ---- group C ----
            route(M::POST, "/api/posts/:postId/comments", "create_comment", h!(api::comments::create_comment)),
            route(M::GET, "/api/posts/:postId/comments", "get_comments", h!(api::comments::get_comments)),
            route(M::DELETE, "/api/comments/:commentId", "delete_comment", h!(api::comments::delete_comment)),
            route(M::POST, "/api/posts/:postId/realmojis", "replace_real_moji", h!(api::realmojis::replace_real_moji)),
            route(M::GET, "/api/posts/:postId/realmojis", "get_real_mojis", h!(api::realmojis::get_real_mojis)),
            route(M::DELETE, "/api/realmojis/:realMojiId", "delete_real_moji", h!(api::realmojis::delete_real_moji)),
            route(M::GET, "/api/images/:imageId", "get_image", h!(api::images::get_image)),
            route(M::GET, "/api/notifications", "get_notifications", h!(api::notifications::get_notifications)),
        ];
        let mut routes = routes;
        for (i, r) in routes.iter_mut().enumerate() {
            r.idx = i;
        }
        Router { routes }
    }

    /// Action names in routing-table order (for metrics).
    pub fn action_names(&self) -> Vec<&'static str> {
        self.routes.iter().map(|r| r.name).collect()
    }

    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    pub fn find(&self, method: &Method, path: &str) -> Match<'_> {
        let segs: SmallVec<[&str; 8]> = path.trim_start_matches('/').split('/').collect();
        let mut best: Option<(&Route, usize)> = None;
        let mut methods: SmallVec<[Method; 4]> = SmallVec::new();
        for r in &self.routes {
            if r.segs.len() != segs.len() {
                continue;
            }
            let ok = r.segs.iter().zip(segs.iter()).all(|(p, s)| match p {
                Seg::Lit(l) => l == s,
                Seg::Param => true,
            });
            if !ok {
                continue;
            }
            if !methods.contains(&r.method) {
                methods.push(r.method.clone());
            }
            if r.method == method && best.is_none_or(|(_, l)| r.lits > l) {
                best = Some((r, r.lits));
            }
        }
        if let Some((r, _)) = best {
            let params = r
                .segs
                .iter()
                .zip(segs.iter())
                .filter(|(p, _)| matches!(p, Seg::Param))
                .map(|(_, s)| percent_encoding::percent_decode_str(s).decode_utf8_lossy().into_owned())
                .collect();
            return Match::Found(r, params);
        }
        if methods.is_empty() {
            return Match::NotFound;
        }
        let mut allow = String::from("OPTIONS");
        for m in METHOD_ORDER.iter() {
            if methods.contains(m) {
                allow.push_str(", ");
                allow.push_str(m.as_str());
            }
        }
        if method == Method::OPTIONS {
            Match::Options(allow)
        } else {
            Match::MethodNotAllowed(allow)
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Router::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching() {
        let r = Router::new();
        assert!(matches!(r.find(&Method::GET, "/api/posts/me/current"), Match::Found(rt, _) if rt.name == "get_my_current_post"));
        assert!(matches!(r.find(&Method::GET, "/api/posts/abc"), Match::Found(rt, p) if rt.name == "get_post" && p[0] == "abc"));
        assert!(matches!(r.find(&Method::GET, "/api/users/"), Match::Found(rt, p) if rt.name == "get_user" && p[0].is_empty()));
        match r.find(&Method::PUT, "/api/me") {
            Match::MethodNotAllowed(a) => assert_eq!(a, "OPTIONS, GET, PATCH"),
            _ => panic!(),
        }
        match r.find(&Method::PATCH, "/api/posts/me/current") {
            Match::MethodNotAllowed(a) => assert_eq!(a, "OPTIONS, GET"),
            _ => panic!(),
        }
        assert!(matches!(r.find(&Method::GET, "/api/nope"), Match::NotFound));
        assert!(matches!(r.find(&Method::GET, "/api/me/"), Match::NotFound));
        assert!(matches!(r.find(&Method::POST, "/api/friend-requests/x/accept"), Match::Found(rt, p) if rt.name == "transition_friend_request" && p[1] == "accept"));
    }
}
