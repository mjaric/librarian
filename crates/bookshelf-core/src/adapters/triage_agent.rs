//! LLM triage agent (feature `agent`, config `triage = "agent"`).
//!
//! Consulted ONLY for ambiguous cases the deterministic table handles poorly:
//! 3+ consecutive same-URL ladder failures, a 200 carrying `text/html` on a
//! file download, unexpected bodies, or unclassifiable rsync stderr. The
//! rules engine remains the fallback for every malformed or failed model
//! response, so the whole path stays correct keyless.
//!
//! rig 0.41's `CompletionModel` is not dyn-compatible (it is `Clone` +
//! RPITIT), so `AgentTriage` is generic over the concrete model handle and
//! the provider factories below return the concrete instances.

use std::time::Duration;

use async_trait::async_trait;

use crate::domain::{Triage, TriageContext, TriageDecision};

use super::triage_rules;

const SYSTEM_PROMPT: &str = "You are an SRE triage assistant for a polite, rate-limited Project Gutenberg\narchiver. Given a failed fetch or rsync run, classify the recovery action. Respond with\nONLY a JSON object, no other text.";

const ACTIONS_SUFFIX: &str = "\nActions: \"skip\" (item is permanently unavailable or\nblocked — do not retry), \"retry\" (transient — include retry_after_sec), \"defer\"\n(wait for the next sync run). Choose conservatively; the archiver must never hammer\nthe server.";

#[derive(Debug, serde::Deserialize)]
struct AgentReply {
    action: String,
    #[serde(default)]
    retry_after_sec: Option<u64>,
    #[serde(default)]
    #[allow(dead_code)] // logged by callers; kept for prompt/response parity
    reason: String,
}

pub struct AgentTriage<M: rig_core::completion::CompletionModel> {
    model: M,
    provider_name: String,
    model_name: String,
}

#[cfg(feature = "agent")]
impl<M: rig_core::completion::CompletionModel> AgentTriage<M> {
    pub fn new(model: M, provider_name: &str, model_name: &str) -> Self {
        Self {
            model,
            provider_name: provider_name.to_string(),
            model_name: model_name.to_string(),
        }
    }

    pub fn describe(&self) -> String {
        format!("{}/{}", self.provider_name, self.model_name)
    }

    async fn ask(&self, ctx: &TriageContext) -> anyhow::Result<TriageDecision> {
        use rig_core::completion::Message;
        let user_payload = serde_json::json!({
            "tool": ctx.tool,
            "url_or_dest": ctx.url_or_dest,
            "status": ctx.status,
            "headers": ctx.headers,
            "body_head_500_chars": ctx.body_head,
            "attempts": ctx.attempts,
            "error": ctx.error,
        });
        let request = self
            .model
            .completion_request(Message::user(format!("{user_payload}{ACTIONS_SUFFIX}")))
            .preamble(SYSTEM_PROMPT.to_string())
            .build();
        let response = self.model.completion(request).await?;
        let text = extract_text(&response);
        parse_reply(&text)
    }
}

#[cfg(feature = "agent")]
mod factories {
    use rig_core::client::{CompletionClient, ProviderClient};
    use rig_core::completion::CompletionModel;

    use super::AgentTriage;

    /// zai via `ZAI_API_KEY` (OpenAI-compatible GLM endpoint).
    pub fn zai(model_name: String) -> anyhow::Result<AgentTriage<impl CompletionModel>> {
        let client = rig_core::providers::zai::Client::from_env()
            .map_err(|e| anyhow::anyhow!("ZAI_API_KEY missing/invalid: {e}"))?;
        Ok(AgentTriage::new(
            client.completion_model(model_name.clone()),
            "zai",
            &model_name,
        ))
    }

    /// openai via `OPENAI_API_KEY`.
    pub fn openai(model_name: String) -> anyhow::Result<AgentTriage<impl CompletionModel>> {
        let client = rig_core::providers::openai::Client::from_env()
            .map_err(|e| anyhow::anyhow!("OPENAI_API_KEY missing/invalid: {e}"))?;
        Ok(AgentTriage::new(
            client.completion_model(model_name.clone()),
            "openai",
            &model_name,
        ))
    }

    /// ollama (local; `OLLAMA_API_BASE_URL` optional).
    pub fn ollama(model_name: String) -> anyhow::Result<AgentTriage<impl CompletionModel>> {
        let client = rig_core::providers::ollama::Client::from_env()
            .map_err(|e| anyhow::anyhow!("ollama client from env: {e}"))?;
        Ok(AgentTriage::new(
            client.completion_model(model_name.clone()),
            "ollama",
            &model_name,
        ))
    }
}

#[cfg(feature = "agent")]
pub use factories::{ollama, openai, zai};

/// Build the agent triage by provider key: "zai" | "openai" | "ollama".
#[cfg(feature = "agent")]
pub fn by_provider(provider: &str, model_name: &str) -> anyhow::Result<Box<dyn Triage>> {
    match provider {
        "zai" => Ok(Box::new(zai(model_name.to_string())?)),
        "openai" => Ok(Box::new(openai(model_name.to_string())?)),
        "ollama" => Ok(Box::new(ollama(model_name.to_string())?)),
        other => anyhow::bail!("unknown agent provider {other:?} (want zai|openai|ollama)"),
    }
}

/// Pull the assistant text out of a completion response.
#[cfg(feature = "agent")]
fn extract_text(
    response: &rig_core::completion::CompletionResponse<impl serde::Serialize>,
) -> String {
    use rig_core::message::AssistantContent;
    response
        .choice
        .iter()
        .map(|c| match c {
            AssistantContent::Text(t) => t.text.clone(),
            _ => String::new(),
        })
        .collect()
}

/// Both variants share this parser so the no-agent build can still be tested.
fn parse_reply(text: &str) -> anyhow::Result<TriageDecision> {
    let start = text
        .find('{')
        .ok_or_else(|| anyhow::anyhow!("no JSON object in model output"))?;
    let end = text
        .rfind('}')
        .ok_or_else(|| anyhow::anyhow!("unterminated JSON in model output"))?;
    let reply: AgentReply = serde_json::from_str(&text[start..=end])
        .map_err(|e| anyhow::anyhow!("malformed triage JSON: {e}"))?;
    match reply.action.as_str() {
        "skip" => Ok(TriageDecision::Skip),
        "defer" => Ok(TriageDecision::Defer),
        "retry" => {
            let secs = reply.retry_after_sec.unwrap_or(300).clamp(60, 7200);
            Ok(TriageDecision::RetryAfter(Duration::from_secs(secs)))
        }
        other => Err(anyhow::anyhow!("unknown action {other:?}")),
    }
}

#[cfg(feature = "agent")]
#[async_trait]
impl<M: CompletionModelSync> Triage for AgentTriage<M> {
    async fn decide(&self, ctx: &TriageContext) -> TriageDecision {
        match self.ask(ctx).await {
            Ok(d) => {
                tracing::info!(agent = %self.describe(), ?d, "agent triage decision");
                d
            }
            Err(e) => {
                tracing::warn!(agent = %self.describe(), error = %e, "agent triage failed — falling back to rules");
                fallback_rules(ctx)
            }
        }
    }
}

/// `CompletionModel` with a Send+Sync future bound so `AgentTriage<M>` can
/// sit behind `Arc<dyn Triage>` in a multi-threaded runtime.
#[cfg(feature = "agent")]
pub trait CompletionModelSync: rig_core::completion::CompletionModel + Send + Sync {}

#[cfg(feature = "agent")]
impl<T> CompletionModelSync for T where T: rig_core::completion::CompletionModel + Send + Sync {}

/// Without the `agent` feature there is nothing to consult: rules only.
fn fallback_rules(ctx: &TriageContext) -> TriageDecision {
    let err = crate::adapters::http::FetchError {
        url: ctx.url_or_dest.clone(),
        kind: match ctx.tool {
            "http" => crate::adapters::http::FetchErrorKind::Status,
            _ => crate::adapters::http::FetchErrorKind::Connect,
        },
        status: ctx.status,
        retry_after: None,
        body_head: ctx.body_head.clone(),
        headers: serde_json::Value::Null,
    };
    triage_rules::decide(&err, ctx.attempts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_wellformed_agent_replies() {
        assert_eq!(
            parse_reply(r#"{"action":"skip","reason":"gone"}"#).unwrap(),
            TriageDecision::Skip
        );
        assert_eq!(
            parse_reply(r#"Here you go: {"action":"defer"}"#).unwrap(),
            TriageDecision::Defer
        );
        // retry clamps into 60..7200
        match parse_reply(r#"{"action":"retry","retry_after_sec":5}"#).unwrap() {
            TriageDecision::RetryAfter(d) => assert_eq!(d, Duration::from_secs(60)),
            other => panic!("{other:?}"),
        }
        match parse_reply(r#"{"action":"retry","retry_after_sec":99999}"#).unwrap() {
            TriageDecision::RetryAfter(d) => assert_eq!(d, Duration::from_secs(7200)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn malformed_replies_are_errors() {
        assert!(parse_reply("no json here").is_err());
        assert!(parse_reply(r#"{"action":"explode"}"#).is_err());
    }
}
