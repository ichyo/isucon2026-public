//! POST /api/initialize (§6.1, §9) — core.

use super::prelude::*;

pub const INITIALIZE_RESPONSE: &str = "{\"language\":\"rust\",\"agent\":\"claude-code\",\"harness\":\"claude-code\",\"model\":\"claude-opus-5-5\"}\n";

pub async fn initialize(ctx: Ctx, _req: Req) -> HResult {
    ctx.write(Command::Initialize).await?;
    ctx.sigwarm.initialize(ctx).await;
    Ok(Resp::json(200, INITIALIZE_RESPONSE))
}
