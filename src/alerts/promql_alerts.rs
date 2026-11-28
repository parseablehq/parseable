//! Persisted, per-series threshold state for native PromQL alerts.
use crate::{
    alerts::{
        AlertConfig, AlertError, AlertQueryType, AlertState, NotificationState,
        alert_structs::DeploymentInfo,
        alert_types::ThresholdAlert,
        alerts_utils::evaluate_condition,
        target::{TARGETS, safe_runtime_error},
    },
    handlers::http::promql::AlertQueryError,
    parseable::{DEFAULT_TENANT, PARSEABLE},
    promql::Labels,
    rbac::{
        Response, Users,
        map::{SessionKey, Sessions, users},
        role::Action,
        roles_to_permission,
    },
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use ulid::Ulid;

/// One lock per alert, serializing changes to that alert. Each lock counts the
/// changes made under it, so a PromQL evaluation that ran its query or its
/// deliveries without the lock can tell that the alert changed meanwhile and
/// must not write back stale state. Evaluations never hold a lock across a
/// query or a delivery, so a slow rule cannot stall other alerts or tenants.
static LIFECYCLES: Lazy<DashMap<Ulid, Arc<Mutex<u64>>>> = Lazy::new(DashMap::new);

fn lifecycle(id: Ulid) -> Arc<Mutex<u64>> {
    LIFECYCLES.entry(id).or_default().clone()
}

/// Lock an alert for a change. Hold the guard until the change is persisted.
pub async fn lock_lifecycle(id: Ulid) -> OwnedMutexGuard<u64> {
    let mut guard = lifecycle(id).lock_owned().await;
    *guard += 1;
    guard
}

/// Drop a deleted alert's lock. Call this while still holding its guard.
pub fn forget_lifecycle(id: Ulid) {
    LIFECYCLES.remove(&id);
}

const MAX_INSTANCES: usize = 1000;
const MAX_DELIVERY_ATTEMPTS: u32 = 3;
/// Retryable deliveries kept in the outbox; the oldest are dropped on overflow.
const MAX_QUEUED_DELIVERIES: usize = 1000;
/// Exhausted deliveries kept only so the latest failures stay inspectable.
const MAX_EXHAUSTED_DELIVERIES: usize = 50;
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Runtime {
    pub health: String,
    pub error: Option<String>,
    pub last_evaluated_at: Option<DateTime<Utc>>,
    pub instances: BTreeMap<String, Instance>,
    #[serde(default)]
    pub deliveries: Vec<Delivery>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delivery {
    labels: Labels,
    value: f64,
    firing: bool,
    target: ulid::Ulid,
    attempts: u32,
    error: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Instance {
    pub labels: Labels,
    pub state: String,
    pub pending_since: Option<DateTime<Utc>>,
    pub last_seen: DateTime<Utc>,
    pub value: f64,
}

pub fn hold_duration(config: &AlertConfig) -> Result<std::time::Duration, AlertError> {
    if let Some(value) = config
        .other_fields
        .as_ref()
        .and_then(|fields| fields.get("promqlConfig"))
        && (!value.is_object()
            || value
                .get("holdDuration")
                .is_some_and(|duration| !duration.is_string()))
    {
        return Err(AlertError::ValidationFailure(
            "promqlConfig must contain a string holdDuration".into(),
        ));
    }
    let duration = config
        .other_fields
        .as_ref()
        .and_then(|fields| fields.get("promqlConfig"))
        .and_then(|v| v.get("holdDuration"))
        .and_then(|v| v.as_str())
        .unwrap_or("0s");
    let duration = humantime::parse_duration(duration).map_err(|_| {
        AlertError::ValidationFailure(
            "promqlConfig.holdDuration must be a duration such as 5m".into(),
        )
    })?;
    if duration.as_secs() > 30 * 86400 {
        return Err(AlertError::ValidationFailure(
            "Hold duration cannot exceed 30 days".into(),
        ));
    }
    Ok(duration)
}

/// Capture a stable user ID, never a password or expiring browser session.
pub fn capture_identity(config: &mut AlertConfig, session: &SessionKey) -> Result<(), AlertError> {
    let fields = config.other_fields.get_or_insert_with(Default::default);
    fields.remove("promqlRuntime");
    fields.remove("executionIdentity");
    if config.query_type == AlertQueryType::Promql {
        let (user, tenant) = Users.get_userid_from_session(session).ok_or_else(|| {
            AlertError::ValidationFailure("Unable to identify alert owner".into())
        })?;
        if tenant != config.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT) {
            return Err(AlertError::ValidationFailure(
                "Alert owner tenant mismatch".into(),
            ));
        }
        fields.insert(
            "executionIdentity".into(),
            serde_json::json!({"userId":user,"tenantId":tenant}),
        );
    }
    Ok(())
}

pub(super) fn authorize_execution(config: &AlertConfig) -> Result<(), String> {
    let identity = config
        .other_fields
        .as_ref()
        .and_then(|f| f.get("executionIdentity"))
        .ok_or("Alert execution identity is missing; edit the rule to set its owner")?;
    let owner = identity
        .get("userId")
        .and_then(|v| v.as_str())
        .ok_or("Invalid alert owner")?;
    let tenant = config.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    if identity.get("tenantId").and_then(|v| v.as_str()) != Some(tenant) {
        return Err("Alert owner tenant mismatch".into());
    }
    let user_roles = users()
        .get(tenant)
        .and_then(|u| u.get(owner))
        .map(|u| u.roles())
        .ok_or("Alert owner no longer exists")?;
    let permissions = roles_to_permission(user_roles, tenant);
    let key = SessionKey::SessionId(ulid::Ulid::new());
    let mut auth = Sessions::default();
    auth.track_new(
        owner.to_owned(),
        key.clone(),
        DateTime::<Utc>::MAX_UTC,
        permissions,
        &config.tenant_id,
    );
    let dataset = config.datasets.first().ok_or("Missing alert dataset")?;
    if auth.check_auth(&key, Action::Query, Some(dataset), None) != Some(Response::Authorized) {
        return Err("Alert owner's dataset permission has been revoked".into());
    }
    Ok(())
}

impl Runtime {
    /// Scrub diagnostics restored from storage, which may predate URL redaction.
    pub fn sanitize_errors(&mut self) {
        let errors = self
            .deliveries
            .iter_mut()
            .map(|delivery| &mut delivery.error);
        for error in std::iter::once(&mut self.error).chain(errors).flatten() {
            *error = safe_runtime_error(error);
        }
    }
    pub fn discontinuity(&mut self, health: &str, error: Option<String>, now: DateTime<Utc>) {
        self.health = health.into();
        self.error = error;
        self.last_evaluated_at = Some(now);
        for instance in self.instances.values_mut() {
            if instance.state == "pending" {
                instance.pending_since = None;
            }
        }
    }
    /// Record an evaluation that lacked capacity rather than data. Pending
    /// holds survive it, unless their series has not been seen for the
    /// staleness bound, so a busy incident does not restart every hold.
    pub fn interrupted(&mut self, error: String, config: &AlertConfig, now: DateTime<Utc>) {
        self.health = "error".into();
        self.error = Some(error);
        self.last_evaluated_at = Some(now);
        let stale_after = staleness_bound(config);
        for instance in self.instances.values_mut() {
            if instance.state == "pending" && now - instance.last_seen > stale_after {
                instance.pending_since = None;
            }
        }
    }
    pub fn observe_transactionally(
        &mut self,
        samples: Vec<(Labels, f64)>,
        config: &AlertConfig,
        now: DateTime<Utc>,
    ) -> Result<Vec<(Labels, f64, bool)>, String> {
        let mut next = self.clone();
        let transitions = next.observe(samples, config, now)?;
        // A newer transition supersedes every older opposite-state notification
        // for that instance, including failed deliveries paused while muted.
        for (labels, _, firing) in &transitions {
            next.deliveries
                .retain(|delivery| delivery.labels != *labels || delivery.firing == *firing);
        }
        *self = next;
        Ok(transitions)
    }
    /// Queue one delivery per transition and target, returning how many queued
    /// deliveries were dropped to keep the outbox bounded.
    pub fn enqueue(
        &mut self,
        transitions: &[(Labels, f64, bool)],
        targets: &[ulid::Ulid],
    ) -> usize {
        for (labels, value, firing) in transitions {
            for target in targets {
                self.deliveries.push(Delivery {
                    labels: labels.clone(),
                    value: *value,
                    firing: *firing,
                    target: *target,
                    attempts: 0,
                    error: None,
                });
            }
        }
        self.trim_deliveries()
    }
    /// After a delivery round: forget deliveries that succeeded, keep failed
    /// and untried ones, and bound the outbox.
    fn settle_deliveries(&mut self) {
        self.deliveries
            .retain(|delivery| delivery.error.is_some() || delivery.attempts == 0);
        self.trim_deliveries();
    }
    /// Bound the outbox without ever blocking state evaluation: keep the newest
    /// retryable deliveries, and only the most recent exhausted ones. Returns
    /// the number of retryable deliveries dropped.
    fn trim_deliveries(&mut self) -> usize {
        let mut excess_queued = self
            .deliveries
            .iter()
            .filter(|delivery| delivery.attempts < MAX_DELIVERY_ATTEMPTS)
            .count()
            .saturating_sub(MAX_QUEUED_DELIVERIES);
        let dropped = excess_queued;
        let mut excess_exhausted = self
            .deliveries
            .iter()
            .filter(|delivery| delivery.attempts >= MAX_DELIVERY_ATTEMPTS)
            .count()
            .saturating_sub(MAX_EXHAUSTED_DELIVERIES);
        // Deliveries are appended in order, so this drops the oldest first.
        self.deliveries.retain(|delivery| {
            let excess = if delivery.attempts < MAX_DELIVERY_ATTEMPTS {
                &mut excess_queued
            } else {
                &mut excess_exhausted
            };
            if *excess > 0 {
                *excess -= 1;
                false
            } else {
                true
            }
        });
        dropped
    }
    pub fn observe(
        &mut self,
        samples: Vec<(Labels, f64)>,
        config: &AlertConfig,
        now: DateTime<Utc>,
    ) -> Result<Vec<(Labels, f64, bool)>, String> {
        if samples.len() > MAX_INSTANCES || samples.iter().any(|(_, v)| !v.is_finite()) {
            return Err(
                "PromQL alert result exceeds instance limit or contains non-finite values".into(),
            );
        }
        let hold = hold_duration(config)
            .map_err(|e| e.to_string())?
            .as_millis() as i64;
        let interval = config.get_eval_frequency().saturating_mul(60) as i64;
        let gap = self.last_evaluated_at.is_none_or(|previous| {
            (now - previous).num_seconds() > interval + interval / 2 || now < previous
        });
        let mut seen = BTreeSet::new();
        let mut transitions = vec![];
        for (labels, value) in samples {
            let key = serde_json::to_string(&labels).map_err(|e| e.to_string())?;
            if !seen.insert(key.clone()) {
                return Err("Duplicate PromQL label set".into());
            }
            let entry = self.instances.entry(key).or_insert_with(|| Instance {
                labels: labels.clone(),
                state: "resolved".into(),
                pending_since: None,
                last_seen: now,
                value,
            });
            entry.value = value;
            entry.last_seen = now;
            if evaluate_condition(
                &config.threshold_config.operator,
                value,
                config.threshold_config.value,
            ) {
                if entry.state != "firing" {
                    if gap || entry.pending_since.is_none() {
                        entry.pending_since = Some(now);
                    }
                    entry.state = "pending".into();
                    if (now - entry.pending_since.unwrap()).num_milliseconds() >= hold {
                        entry.state = "firing".into();
                        transitions.push((labels, value, true));
                    }
                }
            } else {
                if entry.state == "firing" {
                    transitions.push((labels, value, false));
                }
                entry.state = "resolved".into();
                entry.pending_since = None;
            }
        }
        // A firing instance survives short data gaps, but one whose series has
        // been absent for longer than the staleness bound resolves, as the
        // series is gone. That also frees its slot under the instance limit.
        let stale_after = staleness_bound(config);
        let mut missing = false;
        self.instances.retain(|key, instance| {
            if seen.contains(key) {
                return true;
            }
            if instance.state == "firing" && now - instance.last_seen > stale_after {
                transitions.push((instance.labels.clone(), instance.value, false));
                return false;
            }
            missing |= instance.state != "resolved";
            instance.pending_since = None;
            instance.state == "firing"
        });
        if self.instances.len() > MAX_INSTANCES {
            return Err("PromQL alert instance limit reached".into());
        }
        self.health = if seen.is_empty() || missing {
            "noData"
        } else {
            "ok"
        }
        .into();
        self.error = None;
        self.last_evaluated_at = Some(now);
        Ok(transitions)
    }
}

pub(super) fn reset_runtime_for_disable(
    fields: &mut Option<serde_json::Map<String, serde_json::Value>>,
) {
    if let Some(fields) = fields {
        fields.remove("promqlRuntime");
    }
}

/// The human-readable notification for one instance, also used as the
/// Alertmanager `message` annotation.
fn instance_message(config: &AlertConfig, labels: &Labels, value: f64, firing: bool) -> String {
    let identity = labels
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{} [{}]: {}\nValue: {}\nThreshold: {} {}\nQuery: {}",
        config.title,
        identity,
        if firing { "firing" } else { "resolved" },
        value,
        config.threshold_config.operator,
        config.threshold_config.value,
        config.query
    )
}

/// Alertmanager identifies an alert by its label set, so the series labels
/// are sent as labels for grouping, inhibition and routing. As in Prometheus,
/// `alertname` overrides a series label of the same name and `__name__` is
/// dropped. A firing and a resolved alert for one instance carry the same
/// labels, so the resolution matches the alert it ends.
fn alertmanager_alert(
    title: &str,
    deployment: &DeploymentInfo,
    labels: &Labels,
    message: String,
    ends_at: DateTime<Utc>,
) -> serde_json::Value {
    let mut alert_labels: serde_json::Map<_, _> = labels
        .iter()
        .filter(|(name, _)| name.as_str() != "__name__")
        .map(|(name, value)| (name.clone(), value.clone().into()))
        .collect();
    alert_labels.insert(
        "deployment_instance".into(),
        deployment.deployment_instance.clone().into(),
    );
    alert_labels.insert(
        "deployment_id".into(),
        deployment.deployment_id.to_string().into(),
    );
    alert_labels.insert(
        "deployment_mode".into(),
        deployment.deployment_mode.clone().into(),
    );
    alert_labels.insert("alertname".into(), title.into());
    serde_json::json!({
        "labels": alert_labels,
        "annotations": {"message": message},
        "endsAt": ends_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    })
}

/// How long a firing instance stays firing without a sample: four evaluation
/// intervals, as with Prometheus' Alertmanager `endsAt`.
fn staleness_bound(config: &AlertConfig) -> chrono::Duration {
    chrono::Duration::minutes(4 * config.get_eval_frequency().max(1) as i64)
}

/// Alertmanager resolves an alert once its `endsAt` passes. Firing alerts stay
/// valid for the staleness bound, so a few missed re-sends do not resolve them.
fn firing_valid_until(config: &AlertConfig, now: DateTime<Utc>) -> DateTime<Utc> {
    now + staleness_bound(config)
}

fn notifications_allowed(state: &NotificationState, now: DateTime<Utc>) -> bool {
    match state {
        NotificationState::Notify => true,
        NotificationState::Mute(till) => {
            till != "indefinite" && till.parse::<DateTime<Utc>>().is_ok_and(|time| now > time)
        }
    }
}

/// The runtime to store after a delivery round, or `None` when the round
/// changed nothing (no delivery was attempted or retired), so the runtime
/// `committed` before it is still the stored one.
fn delivery_round_result(
    runtime: &Runtime,
    committed: &serde_json::Value,
) -> Result<Option<serde_json::Value>, serde_json::Error> {
    let settled = serde_json::to_value(runtime)?;
    Ok((settled != *committed).then_some(settled))
}

pub async fn evaluate(id: Ulid, tenant_id: &Option<String>) -> Result<(), AlertError> {
    let lifecycle = lifecycle(id);
    let manager = super::get_alert_manager().await;
    let (config, generation) = {
        let generation = lifecycle.lock().await;
        let config = manager
            .get_alert_by_id(id, tenant_id)
            .await?
            .to_alert_config();
        (config, *generation)
    };
    if config.state == AlertState::Disabled {
        return Ok(());
    }
    let now = Utc::now();
    let mut runtime: Runtime = config
        .other_fields
        .as_ref()
        .and_then(|f| f.get("promqlRuntime"))
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?
        .unwrap_or_default();
    runtime.sanitize_errors();
    let authorized = authorize_execution(&config);
    let may_deliver = authorized.is_ok();
    let result = match authorized {
        Ok(()) => {
            crate::handlers::http::promql::execute_alert_instant(
                &config.datasets[0],
                &config.query,
                &config.tenant_id,
            )
            .await
        }
        Err(error) => Err(AlertQueryError::Failed(error)),
    };
    let result = match result {
        Ok(samples) => runtime.observe_transactionally(samples, &config, now),
        Err(AlertQueryError::Busy(error)) => {
            runtime.interrupted(safe_runtime_error(&error), &config, now);
            Ok(vec![])
        }
        Err(AlertQueryError::Failed(error)) => Err(error),
    };
    let transitions = match result {
        Ok(transitions) => transitions,
        Err(error) => {
            runtime.discontinuity("error", Some(safe_runtime_error(&error)), now);
            vec![]
        }
    };
    let mut updated = config;
    let previous_state = updated.state;
    updated.state = if runtime.instances.values().any(|i| i.state == "firing") {
        AlertState::Triggered
    } else {
        AlertState::NotTriggered
    };
    if transitions.iter().any(|(_, _, firing)| *firing) {
        updated.last_triggered_at = Some(now);
    }
    let notify = notifications_allowed(&updated.notification_state, now);
    if notify {
        let dropped = runtime.enqueue(&transitions, &updated.targets);
        if dropped > 0 {
            tracing::warn!(
                "PromQL alert {} dropped {dropped} oldest queued notifications because its outbox is full",
                updated.id
            );
        }
    }
    let committed = serde_json::to_value(&runtime)?;
    updated
        .other_fields
        .get_or_insert_with(Default::default)
        .insert("promqlRuntime".into(), committed.clone());
    {
        let mut guard = lifecycle.lock().await;
        if *guard != generation {
            // The alert was changed, disabled or deleted while the query ran.
            return Ok(());
        }
        *guard += 1;
        // Written here rather than through `ThresholdAlert`, whose record of
        // what it last wrote would otherwise go stale.
        super::alert_types::forget_persisted_alert(updated.id, &updated.tenant_id);
        PARSEABLE
            .metastore
            .put_alert(&updated, &updated.tenant_id)
            .await?;
        manager.update(&ThresholdAlert::from(updated.clone())).await;
        if previous_state != updated.state {
            PARSEABLE
                .metastore
                .put_alert_state(
                    &super::AlertStateEntry::new(
                        updated.id,
                        updated.state,
                        updated.tenant_id.clone(),
                    ),
                    &updated.tenant_id,
                )
                .await?;
        }
    }
    if notify && may_deliver {
        // Each evaluation makes at most one delivery attempt per queued transition.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let deployment = updated.get_context().deployment_info;
        let valid_until = firing_valid_until(&updated, now);
        for delivery in runtime
            .deliveries
            .iter_mut()
            .filter(|delivery| delivery.attempts < MAX_DELIVERY_ATTEMPTS)
            .take(10)
        {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            let message =
                instance_message(&updated, &delivery.labels, delivery.value, delivery.firing);
            let alert = alertmanager_alert(
                &updated.title,
                &deployment,
                &delivery.labels,
                message.clone(),
                if delivery.firing { valid_until } else { now },
            );
            delivery.attempts += 1;
            let target_result = TARGETS
                .get_target_by_id(&delivery.target, &updated.tenant_id)
                .await
                .map_err(|error| safe_runtime_error(&error.to_string()));
            let result = match target_result {
                Ok(target) => match tokio::time::timeout_at(
                    deadline.min(tokio::time::Instant::now() + std::time::Duration::from_secs(5)),
                    target.deliver_promql(&updated.tenant_id, &message, &[alert]),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err("Notification delivery timed out".into()),
                },
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => {
                    delivery.attempts = u32::MAX;
                    delivery.error = None;
                }
                Err(error) => delivery.error = Some(safe_runtime_error(&error)),
            }
        }
        // Alertmanager expects active alerts to be re-sent, so every
        // evaluation posts all firing instances to Alertmanager targets. It
        // deduplicates them by label set. Slack and webhook targets only get
        // the transitions queued above.
        let firing = runtime
            .instances
            .values()
            .filter(|instance| instance.state == "firing")
            .map(|instance| {
                alertmanager_alert(
                    &updated.title,
                    &deployment,
                    &instance.labels,
                    instance_message(&updated, &instance.labels, instance.value, true),
                    valid_until,
                )
            })
            .collect::<Vec<_>>();
        if !firing.is_empty() {
            for target_id in &updated.targets {
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                let Ok(target) = TARGETS
                    .get_target_by_id(target_id, &updated.tenant_id)
                    .await
                else {
                    continue;
                };
                let Some(alertmanager) = target.alertmanager() else {
                    continue;
                };
                let result = tokio::time::timeout_at(
                    deadline.min(tokio::time::Instant::now() + std::time::Duration::from_secs(5)),
                    alertmanager.post_promql(&updated.tenant_id, &firing),
                )
                .await
                .unwrap_or_else(|_| Err("Notification delivery timed out".into()));
                if let Err(error) = result {
                    tracing::warn!(
                        "PromQL alert {} could not re-send firing alerts to Alertmanager target {target_id}: {}",
                        updated.id,
                        safe_runtime_error(&error)
                    );
                }
            }
        }
        runtime.settle_deliveries();
        // Without a delivery attempted or retired, the runtime committed
        // above is still current and is already stored, so the alert is not
        // re-read and rewritten. Changes made meanwhile by others were
        // persisted by whoever made them.
        let Some(settled) = delivery_round_result(&runtime, &committed)? else {
            return Ok(());
        };
        // Re-read the alert so that changes made during delivery, such as a
        // new notification state, are kept. Delivery results are only
        // recorded if nothing replaced or reset the runtime meanwhile.
        let mut guard = lifecycle.lock().await;
        let Ok(current) = manager.get_alert_by_id(id, tenant_id).await else {
            return Ok(());
        };
        let mut current = current.to_alert_config();
        let Some(fields) = current.other_fields.as_mut() else {
            return Ok(());
        };
        if fields.get("promqlRuntime") != Some(&committed) {
            return Ok(());
        }
        fields.insert("promqlRuntime".into(), settled);
        *guard += 1;
        super::alert_types::forget_persisted_alert(current.id, &current.tenant_id);
        PARSEABLE
            .metastore
            .put_alert(&current, &current.tenant_id)
            .await?;
        manager.update(&ThresholdAlert::from(current)).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> AlertConfig {
        serde_json::from_value(serde_json::json!({
            "version":"v2","id":ulid::Ulid::new(),"severity":"high","title":"Host load",
            "queryType":"promql","query":"load","datasets":["metrics"],"alertType":"threshold",
            "thresholdConfig":{"operator":">","value":8.0},
            "evalConfig":{"rollingWindow":{"evalStart":"5m","evalEnd":"now","evalFrequency":1}},
            "targets":[],"notificationState":"notify","notificationConfig":{"interval":1},
            "created":Utc::now(),"promqlConfig":{"holdDuration":"5m"}
        }))
        .unwrap()
    }
    fn sample(host: &str, value: f64) -> (Labels, f64) {
        (BTreeMap::from([("host".into(), host.into())]), value)
    }
    #[test]
    fn independent_hosts_hold_and_recover() {
        let config = config();
        let now = Utc::now();
        let mut runtime = Runtime::default();
        assert!(
            runtime
                .observe(vec![sample("a", 9.0), sample("b", 3.0)], &config, now)
                .unwrap()
                .is_empty()
        );
        for minute in 1..5 {
            assert!(
                runtime
                    .observe(
                        vec![sample("a", 9.0), sample("b", 9.0)],
                        &config,
                        now + chrono::Duration::minutes(minute)
                    )
                    .unwrap()
                    .is_empty()
            );
        }
        let fired = runtime
            .observe(
                vec![sample("a", 9.0), sample("b", 9.0)],
                &config,
                now + chrono::Duration::minutes(5),
            )
            .unwrap();
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].0["host"], "a");
        assert!(fired[0].2);
        let next = runtime
            .observe(
                vec![sample("a", 2.0), sample("b", 9.0)],
                &config,
                now + chrono::Duration::minutes(6),
            )
            .unwrap();
        assert_eq!(next.len(), 2);
        assert!(!next[0].2);
        assert!(next[1].2);
    }
    #[test]
    fn gaps_and_errors_do_not_recover_firing_or_advance_pending() {
        let mut config = config();
        config.other_fields.as_mut().unwrap().insert(
            "promqlConfig".into(),
            serde_json::json!({"holdDuration":"0s"}),
        );
        let now = Utc::now();
        let mut runtime = Runtime::default();
        assert_eq!(
            runtime
                .observe(vec![sample("a", 9.0)], &config, now)
                .unwrap()
                .len(),
            1
        );
        assert!(
            runtime
                .observe(vec![], &config, now + chrono::Duration::minutes(1))
                .unwrap()
                .is_empty()
        );
        assert_eq!(runtime.health, "noData");
        assert_eq!(runtime.instances.values().next().unwrap().state, "firing");
        runtime.discontinuity(
            "error",
            Some("timeout".into()),
            now + chrono::Duration::minutes(2),
        );
        assert_eq!(runtime.instances.values().next().unwrap().state, "firing");
        let persisted = serde_json::to_string(&runtime).unwrap();
        let restored: Runtime = serde_json::from_str(&persisted).unwrap();
        assert_eq!(restored.health, "error");
        assert_eq!(restored.instances.values().next().unwrap().state, "firing");
    }
    #[test]
    fn vanished_firing_series_resolve_after_staleness_bound() {
        let mut config = config();
        config.other_fields.as_mut().unwrap().insert(
            "promqlConfig".into(),
            serde_json::json!({"holdDuration":"0s"}),
        );
        let now = Utc::now();
        let minutes = |m| now + chrono::Duration::minutes(m);
        let mut runtime = Runtime::default();
        runtime
            .observe(vec![sample("a", 9.0), sample("b", 9.0)], &config, now)
            .unwrap();
        // Within four intervals, "a" keeps firing through the gap.
        for minute in 1..=4 {
            assert!(
                runtime
                    .observe(vec![sample("b", 9.0)], &config, minutes(minute))
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(runtime.health, "noData");
        }
        let resolved = runtime
            .observe(vec![sample("b", 9.0)], &config, minutes(5))
            .unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].0["host"], "a");
        assert_eq!(resolved[0].1, 9.0);
        assert!(!resolved[0].2);
        assert_eq!(runtime.health, "ok");
        assert_eq!(runtime.instances.len(), 1);
        assert_eq!(
            runtime.instances.values().next().unwrap().labels["host"],
            "b"
        );
    }
    #[test]
    fn churned_firing_series_do_not_exhaust_instance_limit() {
        let mut config = config();
        config.other_fields.as_mut().unwrap().insert(
            "promqlConfig".into(),
            serde_json::json!({"holdDuration":"0s"}),
        );
        let now = Utc::now();
        let batch = |generation: usize| {
            (0..MAX_INSTANCES)
                .map(|pod| sample(&format!("{generation}-{pod}"), 9.0))
                .collect::<Vec<_>>()
        };
        let mut runtime = Runtime::default();
        runtime
            .observe_transactionally(batch(0), &config, now)
            .unwrap();
        // Replacing every series while the old ones are still fresh exceeds the limit.
        assert!(
            runtime
                .observe_transactionally(batch(1), &config, now + chrono::Duration::minutes(1))
                .is_err()
        );
        // Once the old series are stale, they resolve and the new ones fire.
        let transitions = runtime
            .observe_transactionally(batch(1), &config, now + chrono::Duration::minutes(5))
            .unwrap();
        assert_eq!(
            transitions.iter().filter(|(_, _, firing)| *firing).count(),
            MAX_INSTANCES
        );
        assert_eq!(
            transitions.iter().filter(|(_, _, firing)| !*firing).count(),
            MAX_INSTANCES
        );
        assert_eq!(runtime.instances.len(), MAX_INSTANCES);
        assert!(
            runtime
                .instances
                .values()
                .all(|i| i.labels["host"].starts_with("1-"))
        );
    }
    #[test]
    fn pending_restarts_after_missing_sample_or_long_gap() {
        let config = config();
        let now = Utc::now();
        let mut runtime = Runtime::default();
        runtime
            .observe(vec![sample("a", 9.0)], &config, now)
            .unwrap();
        runtime
            .observe(vec![], &config, now + chrono::Duration::minutes(1))
            .unwrap();
        runtime
            .observe(
                vec![sample("a", 9.0)],
                &config,
                now + chrono::Duration::minutes(2),
            )
            .unwrap();
        assert_eq!(
            runtime.instances.values().next().unwrap().pending_since,
            Some(now + chrono::Duration::minutes(2))
        );
        runtime
            .observe(
                vec![sample("a", 9.0)],
                &config,
                now + chrono::Duration::minutes(10),
            )
            .unwrap();
        assert_eq!(
            runtime.instances.values().next().unwrap().pending_since,
            Some(now + chrono::Duration::minutes(10))
        );
        assert!(
            runtime
                .observe(vec![sample("b", f64::NAN)], &config, now)
                .is_err()
        );
    }
    #[test]
    fn capacity_errors_keep_holds_until_series_is_stale() {
        let config = config();
        let now = Utc::now();
        let minutes = |m| now + chrono::Duration::minutes(m);
        let mut runtime = Runtime::default();
        runtime
            .observe(vec![sample("a", 9.0)], &config, now)
            .unwrap();
        for m in 1..=4 {
            runtime.interrupted("PromQL alert query timed out".into(), &config, minutes(m));
        }
        assert_eq!(runtime.health, "error");
        let transitions = runtime
            .observe(vec![sample("a", 9.0)], &config, minutes(5))
            .unwrap();
        assert_eq!(transitions.len(), 1);
        assert_eq!(runtime.instances.values().next().unwrap().state, "firing");

        let mut runtime = Runtime::default();
        runtime
            .observe(vec![sample("a", 9.0)], &config, now)
            .unwrap();
        for m in 1..=5 {
            runtime.interrupted("PromQL alert query timed out".into(), &config, minutes(m));
        }
        assert!(
            runtime
                .observe(vec![sample("a", 9.0)], &config, minutes(6))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            runtime.instances.values().next().unwrap().pending_since,
            Some(minutes(6))
        );
    }
    #[tokio::test]
    async fn lifecycle_locks_are_per_alert_and_count_changes() {
        let (held_id, other_id) = (Ulid::new(), Ulid::new());
        let held = lock_lifecycle(held_id).await;
        // A change to one alert must not wait for another alert's lock.
        let other =
            tokio::time::timeout(std::time::Duration::from_secs(1), lock_lifecycle(other_id))
                .await
                .expect("an unrelated alert's lock should be free");
        assert_eq!((*held, *other), (1, 1));
        drop(held);
        assert_eq!(*lock_lifecycle(held_id).await, 2);
        forget_lifecycle(held_id);
        forget_lifecycle(other_id);
    }
    #[test]
    fn duplicate_labels_leave_instance_state_unchanged() {
        let mut config = config();
        config.other_fields.as_mut().unwrap().insert(
            "promqlConfig".into(),
            serde_json::json!({"holdDuration":"0s"}),
        );
        let mut runtime = Runtime::default();
        assert!(
            runtime
                .observe_transactionally(
                    vec![sample("a", 9.0), sample("a", 10.0)],
                    &config,
                    Utc::now()
                )
                .is_err()
        );
        assert!(runtime.instances.is_empty());
    }
    #[test]
    fn full_outbox_never_blocks_evaluation() {
        let mut config = config();
        config.targets = (0..20).map(|_| ulid::Ulid::new()).collect();
        config.other_fields.as_mut().unwrap().insert(
            "promqlConfig".into(),
            serde_json::json!({"holdDuration":"0s"}),
        );
        let failed = |attempts| Delivery {
            labels: Labels::new(),
            value: 1.0,
            firing: true,
            target: config.targets[0],
            attempts,
            error: Some("failed".into()),
        };
        let mut runtime = Runtime {
            deliveries: (0..1000)
                .map(|_| failed(MAX_DELIVERY_ATTEMPTS))
                .chain((0..10).map(|_| failed(1)))
                .collect(),
            ..Default::default()
        };
        // 51 series x 20 targets exceeds the queue limit on its own.
        let samples = (0..51)
            .map(|host| sample(&host.to_string(), 9.0))
            .collect::<Vec<_>>();
        let transitions = runtime
            .observe_transactionally(samples, &config, Utc::now())
            .unwrap();
        assert_eq!(transitions.len(), 51);
        assert_eq!(runtime.health, "ok");
        assert!(runtime.instances.values().all(|i| i.state == "firing"));
        assert_eq!(runtime.enqueue(&transitions, &config.targets), 30);
        let (queued, exhausted): (Vec<_>, Vec<_>) = runtime
            .deliveries
            .iter()
            .partition(|delivery| delivery.attempts < MAX_DELIVERY_ATTEMPTS);
        assert_eq!(queued.len(), MAX_QUEUED_DELIVERIES);
        assert_eq!(exhausted.len(), MAX_EXHAUSTED_DELIVERIES);
        // The oldest queued deliveries are dropped first; the newest survive.
        assert!(queued.iter().all(|delivery| delivery.attempts == 0));
        assert_eq!(queued.last().unwrap().labels["host"], "50");
        assert_eq!(
            queued.last().unwrap().target,
            *config.targets.last().unwrap()
        );
        let restored: Runtime =
            serde_json::from_value(serde_json::to_value(&runtime).unwrap()).unwrap();
        assert_eq!(restored.deliveries.len(), runtime.deliveries.len());
    }
    #[test]
    fn quiet_delivery_rounds_need_no_second_write() {
        let mut config = config();
        config.targets = vec![ulid::Ulid::new()];
        config.other_fields.as_mut().unwrap().insert(
            "promqlConfig".into(),
            serde_json::json!({"holdDuration":"0s"}),
        );
        let now = Utc::now();
        let mut runtime = Runtime::default();

        // A quiet evaluation: nothing to deliver, so nothing changes.
        let transitions = runtime
            .observe_transactionally(vec![sample("a", 1.0)], &config, now)
            .unwrap();
        assert!(transitions.is_empty());
        assert_eq!(runtime.enqueue(&transitions, &config.targets), 0);
        let committed = serde_json::to_value(&runtime).unwrap();
        runtime.settle_deliveries();
        assert_eq!(delivery_round_result(&runtime, &committed).unwrap(), None);

        // A transition queues a delivery; attempting it is a change to store.
        let transitions = runtime
            .observe_transactionally(vec![sample("a", 9.0)], &config, now)
            .unwrap();
        assert_eq!(transitions.len(), 1);
        runtime.enqueue(&transitions, &config.targets);
        let committed = serde_json::to_value(&runtime).unwrap();
        runtime.deliveries[0].attempts += 1;
        runtime.deliveries[0].error = Some("failed".into());
        runtime.settle_deliveries();
        let settled = delivery_round_result(&runtime, &committed)
            .unwrap()
            .expect("a failed attempt is recorded");
        assert_eq!(settled, serde_json::to_value(&runtime).unwrap());

        // A successful delivery leaves the outbox, which is also a change.
        let committed = serde_json::to_value(&runtime).unwrap();
        runtime.deliveries[0].attempts = u32::MAX;
        runtime.deliveries[0].error = None;
        runtime.settle_deliveries();
        assert!(runtime.deliveries.is_empty());
        assert!(
            delivery_round_result(&runtime, &committed)
                .unwrap()
                .is_some()
        );
        // And once settled, the next quiet round has nothing to write.
        let committed = serde_json::to_value(&runtime).unwrap();
        runtime.settle_deliveries();
        assert_eq!(delivery_round_result(&runtime, &committed).unwrap(), None);
    }
    #[test]
    fn mute_suppresses_delivery_until_expiry() {
        let now = Utc::now();
        assert!(notifications_allowed(&NotificationState::Notify, now));
        assert!(!notifications_allowed(
            &NotificationState::Mute("indefinite".into()),
            now
        ));
        assert!(!notifications_allowed(
            &NotificationState::Mute((now + chrono::Duration::minutes(1)).to_rfc3339()),
            now
        ));
        assert!(notifications_allowed(
            &NotificationState::Mute((now - chrono::Duration::minutes(1)).to_rfc3339()),
            now
        ));
        assert!(!notifications_allowed(
            &NotificationState::Mute("malformed".into()),
            now
        ));
    }
    #[test]
    fn recovery_supersedes_failed_firing_for_only_that_instance() {
        let mut config = config();
        config.other_fields.as_mut().unwrap().insert(
            "promqlConfig".into(),
            serde_json::json!({"holdDuration":"0s"}),
        );
        let now = Utc::now();
        let mut runtime = Runtime::default();
        runtime
            .observe_transactionally(vec![sample("a", 9.0), sample("b", 9.0)], &config, now)
            .unwrap();
        let target = ulid::Ulid::new();
        runtime.deliveries = ["a", "b"]
            .into_iter()
            .map(|host| Delivery {
                labels: sample(host, 9.0).0,
                value: 9.0,
                firing: true,
                target,
                attempts: 1,
                error: Some("target failed".into()),
            })
            .collect();
        // Superseding happens during evaluation even if mute suppresses new delivery.
        config.notification_state = NotificationState::Mute("indefinite".into());
        let recovered = runtime
            .observe_transactionally(
                vec![sample("a", 2.0), sample("b", 9.0)],
                &config,
                now + chrono::Duration::minutes(1),
            )
            .unwrap();
        assert_eq!(recovered.len(), 1);
        assert!(!recovered[0].2);
        assert_eq!(runtime.deliveries.len(), 1);
        assert_eq!(runtime.deliveries[0].labels["host"], "b");
        runtime.deliveries.push(Delivery {
            labels: sample("a", 2.0).0,
            value: 2.0,
            firing: false,
            target,
            attempts: 1,
            error: Some("recovery failed".into()),
        });
        runtime
            .observe_transactionally(
                vec![sample("a", 9.0), sample("b", 9.0)],
                &config,
                now + chrono::Duration::minutes(2),
            )
            .unwrap();
        assert_eq!(runtime.deliveries.len(), 1);
        assert_eq!(runtime.deliveries[0].labels["host"], "b");
    }
    #[test]
    fn disabling_clears_instances_and_requires_fresh_hold_when_enabled() {
        let mut config = config();
        let now = Utc::now();
        let mut runtime = Runtime::default();
        runtime
            .observe(vec![sample("a", 9.0)], &config, now)
            .unwrap();
        runtime.instances.values_mut().next().unwrap().state = "firing".into();
        config.other_fields.as_mut().unwrap().insert(
            "promqlRuntime".into(),
            serde_json::to_value(&runtime).unwrap(),
        );
        reset_runtime_for_disable(&mut config.other_fields);
        assert!(
            !config
                .other_fields
                .as_ref()
                .unwrap()
                .contains_key("promqlRuntime")
        );
        assert!(
            config
                .other_fields
                .as_ref()
                .unwrap()
                .contains_key("promqlConfig")
        );
        let mut enabled = Runtime::default();
        assert!(
            enabled
                .observe(
                    vec![sample("a", 9.0)],
                    &config,
                    now + chrono::Duration::minutes(1)
                )
                .unwrap()
                .is_empty()
        );
        assert_eq!(enabled.instances.values().next().unwrap().state, "pending");
        assert_eq!(
            enabled.instances.values().next().unwrap().pending_since,
            Some(now + chrono::Duration::minutes(1))
        );
    }
    #[test]
    fn alertmanager_alert_keeps_series_labels_and_stable_identity() {
        let config = config();
        let now = Utc::now();
        let deployment = DeploymentInfo::new(
            "http://localhost:8000".into(),
            ulid::Ulid::new(),
            "All".into(),
        );
        let labels = BTreeMap::from([
            ("__name__".into(), "load".into()),
            ("alertname".into(), "series".into()),
            ("host".into(), "a".into()),
            ("job".into(), "node".into()),
        ]);
        let valid_until = firing_valid_until(&config, now);
        let firing = alertmanager_alert(
            &config.title,
            &deployment,
            &labels,
            instance_message(&config, &labels, 9.0, true),
            valid_until,
        );
        assert_eq!(
            firing["labels"],
            serde_json::json!({
                "alertname": "Host load",
                "host": "a",
                "job": "node",
                "deployment_instance": "http://localhost:8000",
                "deployment_id": deployment.deployment_id.to_string(),
                "deployment_mode": "All",
            })
        );
        assert!(
            firing["annotations"]["message"]
                .as_str()
                .unwrap()
                .contains("firing\nValue: 9")
        );
        assert_eq!(valid_until, now + chrono::Duration::minutes(4));
        let resolved = alertmanager_alert(
            &config.title,
            &deployment,
            &labels,
            instance_message(&config, &labels, 2.0, false),
            now,
        );
        assert_eq!(resolved["labels"], firing["labels"]);
        assert_eq!(
            resolved["endsAt"],
            now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        );
    }

    #[test]
    fn restored_runtime_preserves_error_labels_and_prunes_stale_firing_delivery() {
        let mut config = config();
        config.other_fields.as_mut().unwrap().insert(
            "promqlConfig".into(),
            serde_json::json!({"holdDuration":"0s"}),
        );
        config.targets = vec![Ulid::new()];
        let labels = BTreeMap::from([
            ("host".into(), "a".into()),
            (
                "error".into(),
                format!("https://metric.example/failure/{}", "é".repeat(1500)),
            ),
        ]);
        let key = serde_json::to_string(&labels).unwrap();
        let now = Utc::now();
        let mut runtime = Runtime::default();
        let firing = runtime
            .observe_transactionally(vec![(labels.clone(), 9.0)], &config, now)
            .unwrap();
        assert_eq!(firing, vec![(labels.clone(), 9.0, true)]);
        runtime.enqueue(&firing, &config.targets);
        runtime.deliveries[0].error =
            Some("request failed for https://hook.example/TARGET-SECRET".into());

        // Exercise the JSON boundary used at startup and for API responses,
        // then restore the typed outbox as the next evaluation would.
        config.other_fields.as_mut().unwrap().insert(
            "promqlRuntime".into(),
            serde_json::to_value(runtime).unwrap(),
        );
        config.sanitize_runtime_diagnostics();
        let mut restored: Runtime =
            serde_json::from_value(config.other_fields.as_ref().unwrap()["promqlRuntime"].clone())
                .unwrap();
        restored.sanitize_errors();
        assert_eq!(restored.instances[&key].labels, labels);
        assert_eq!(restored.deliveries[0].labels, labels);
        assert_eq!(
            serde_json::to_string(&restored.instances[&key].labels).unwrap(),
            key
        );
        assert!(
            !restored.deliveries[0]
                .error
                .as_ref()
                .unwrap()
                .contains("TARGET-SECRET")
        );

        let recovery = restored
            .observe_transactionally(
                vec![(labels.clone(), 0.0)],
                &config,
                now + chrono::Duration::minutes(1),
            )
            .unwrap();
        assert_eq!(recovery, vec![(labels.clone(), 0.0, false)]);
        assert!(restored.deliveries.is_empty());
        restored.enqueue(&recovery, &config.targets);
        assert_eq!(restored.deliveries.len(), 1);
        assert_eq!(restored.deliveries[0].labels, labels);
        assert!(!restored.deliveries[0].firing);
    }

    #[test]
    fn restored_runtime_errors_are_sanitized_without_changing_state() {
        let target = ulid::Ulid::new();
        let delivery = |error: Option<&str>| Delivery {
            labels: BTreeMap::from([("host".into(), "a".into())]),
            value: 9.0,
            firing: true,
            target,
            attempts: 2,
            error: error.map(Into::into),
        };
        let stored = serde_json::json!({
            "health":"error",
            "error":"error sending request for url (https://am.example/api?token=SECRET)",
            "lastEvaluatedAt":Utc::now(),
            "instances":{"a":{"labels":{"host":"a"},"state":"firing","pendingSince":null,
                "lastSeen":Utc::now(),"value":9.0}},
            "deliveries":[
                delivery(Some("HTTP status server error (500) for url (HTTP://u:p@hook.example/SECRET)")),
                delivery(Some("Notification delivery timed out")),
                delivery(None),
            ]
        });
        let mut runtime: Runtime = serde_json::from_value(stored).unwrap();
        runtime.sanitize_errors();
        assert_eq!(
            runtime.error.as_deref(),
            Some("Request failed (URL details redacted)")
        );
        let errors: Vec<_> = runtime
            .deliveries
            .iter()
            .map(|delivery| delivery.error.as_deref())
            .collect();
        assert_eq!(
            errors,
            [
                Some("Request failed (URL details redacted)"),
                Some("Notification delivery timed out"),
                None
            ]
        );
        assert_eq!(runtime.health, "error");
        assert_eq!(runtime.instances["a"].state, "firing");
        assert!(runtime.deliveries.iter().all(|d| d.attempts == 2));
        assert!(!serde_json::to_string(&runtime).unwrap().contains("SECRET"));
    }
}
