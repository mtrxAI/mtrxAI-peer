use crate::client_config::{
    save_client_config, swarm_is_connected, ClientConfig, ClusterMembership, SwarmMembership,
    cluster_is_connected,
};
use chrono::Timelike;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedSchedule {
    pub enabled: bool,
    pub start_mins: u32,
    pub end_mins: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleWindow {
    Inside,
    Outside,
}

/// Parse `"HH:MM"` into minutes since midnight.
pub fn parse_hhmm(value: &str) -> Option<u32> {
    let value = value.trim();
    let (h, m) = value.split_once(':')?;
    let hour: u32 = h.parse().ok()?;
    let minute: u32 = m.parse().ok()?;
    if hour > 23 || minute > 59 {
        return None;
    }
    Some(hour * 60 + minute)
}

pub fn format_hhmm(mins: u32) -> String {
    format!("{:02}:{:02}", mins / 60, mins % 60)
}

pub fn is_inside_window(start_mins: u32, end_mins: u32, now_mins: u32) -> bool {
    if start_mins == end_mins {
        return true;
    }
    if start_mins < end_mins {
        now_mins >= start_mins && now_mins < end_mins
    } else {
        now_mins >= start_mins || now_mins < end_mins
    }
}

pub fn window_state(start_mins: u32, end_mins: u32, now_mins: u32) -> ScheduleWindow {
    if is_inside_window(start_mins, end_mins, now_mins) {
        ScheduleWindow::Inside
    } else {
        ScheduleWindow::Outside
    }
}

pub fn resolve_cluster_schedule(cfg: &ClientConfig, cluster: &ClusterMembership) -> ResolvedSchedule {
    resolve_schedule(
        cluster.schedule_enabled,
        cluster.schedule_start.as_deref(),
        cluster.schedule_end.as_deref(),
        cfg.default_schedule_enabled,
        cfg.default_schedule_start.as_deref(),
        cfg.default_schedule_end.as_deref(),
    )
}

pub fn resolve_swarm_schedule(cfg: &ClientConfig, swarm: &SwarmMembership) -> ResolvedSchedule {
    resolve_schedule(
        swarm.schedule_enabled,
        swarm.schedule_start.as_deref(),
        swarm.schedule_end.as_deref(),
        cfg.default_schedule_enabled,
        cfg.default_schedule_start.as_deref(),
        cfg.default_schedule_end.as_deref(),
    )
}

fn resolve_schedule(
    member_enabled: Option<bool>,
    member_start: Option<&str>,
    member_end: Option<&str>,
    default_enabled: Option<bool>,
    default_start: Option<&str>,
    default_end: Option<&str>,
) -> ResolvedSchedule {
    let enabled = member_enabled.or(default_enabled).unwrap_or(false);
    let start_raw = member_start.or(default_start).unwrap_or("09:00");
    let end_raw = member_end.or(default_end).unwrap_or("17:00");
    let start_mins = parse_hhmm(start_raw).unwrap_or(9 * 60);
    let end_mins = parse_hhmm(end_raw).unwrap_or(17 * 60);
    ResolvedSchedule {
        enabled,
        start_mins,
        end_mins,
    }
}

pub fn schedule_status_from_resolved(schedule: &ResolvedSchedule) -> ScheduleStatusFields {
    let now_mins = local_now_mins();
    ScheduleStatusFields {
        schedule_enabled: schedule.enabled,
        schedule_start: schedule
            .enabled
            .then(|| format_hhmm(schedule.start_mins)),
        schedule_end: schedule.enabled.then(|| format_hhmm(schedule.end_mins)),
        schedule_next_transition_at: next_transition_unix(schedule),
        schedule_inside_window: is_inside_window(schedule.start_mins, schedule.end_mins, now_mins),
    }
}

#[derive(Debug, Clone)]
pub struct ScheduleStatusFields {
    pub schedule_enabled: bool,
    pub schedule_start: Option<String>,
    pub schedule_end: Option<String>,
    pub schedule_next_transition_at: Option<u64>,
    pub schedule_inside_window: bool,
}

pub fn cluster_schedule_fields(cfg: &ClientConfig, cluster: &ClusterMembership) -> ScheduleStatusFields {
    schedule_status_from_resolved(&resolve_cluster_schedule(cfg, cluster))
}

pub fn swarm_schedule_fields(cfg: &ClientConfig, swarm: &SwarmMembership) -> ScheduleStatusFields {
    schedule_status_from_resolved(&resolve_swarm_schedule(cfg, swarm))
}

pub fn local_now_mins() -> u32 {
    let now = chrono::Local::now();
    now.hour() * 60 + now.minute()
}

pub fn next_transition_unix(schedule: &ResolvedSchedule) -> Option<u64> {
    if !schedule.enabled {
        return None;
    }
    let now = chrono::Local::now();
    let now_mins = now.hour() * 60 + now.minute();
    let inside = is_inside_window(schedule.start_mins, schedule.end_mins, now_mins);
    let target_mins = if inside {
        schedule.end_mins
    } else {
        schedule.start_mins
    };
    let mut target = now.date_naive().and_hms_opt(target_mins / 60, target_mins % 60, 0)?;
    if inside && target_mins <= now_mins {
        target += chrono::Duration::days(1);
    }
    if !inside && target_mins > now_mins {
        // same-day start still ahead
    } else if !inside && target_mins <= now_mins {
        target += chrono::Duration::days(1);
    }
    Some(target.and_utc().timestamp() as u64)
}

pub async fn apply_schedules(
    client_config: &Arc<RwLock<ClientConfig>>,
    cluster_notify_tx: &mpsc::Sender<()>,
    swarm_notify_tx: &mpsc::Sender<()>,
) -> bool {
    let now_mins = local_now_mins();
    let mut changed = false;
    {
        let mut cfg = client_config.write().await;
        let cluster_updates: Vec<(usize, bool)> = cfg
            .clusters
            .iter()
            .enumerate()
            .filter_map(|(i, cluster)| {
                let schedule = resolve_cluster_schedule(&cfg, cluster);
                if !schedule.enabled {
                    return None;
                }
                let want_connected = matches!(
                    window_state(schedule.start_mins, schedule.end_mins, now_mins),
                    ScheduleWindow::Inside
                );
                let current = cluster_is_connected(cluster);
                if current != want_connected {
                    Some((i, want_connected))
                } else {
                    None
                }
            })
            .collect();
        for (i, want_connected) in cluster_updates {
            cfg.clusters[i].connected = Some(want_connected);
            changed = true;
        }
        let swarm_updates: Vec<(usize, bool)> = cfg
            .swarms
            .iter()
            .enumerate()
            .filter_map(|(i, swarm)| {
                let schedule = resolve_swarm_schedule(&cfg, swarm);
                if !schedule.enabled {
                    return None;
                }
                let want_connected = matches!(
                    window_state(schedule.start_mins, schedule.end_mins, now_mins),
                    ScheduleWindow::Inside
                );
                let current = swarm_is_connected(swarm);
                if current != want_connected {
                    Some((i, want_connected))
                } else {
                    None
                }
            })
            .collect();
        for (i, want_connected) in swarm_updates {
            cfg.swarms[i].connected = Some(want_connected);
            changed = true;
        }
        if changed {
            if let Err(e) = save_client_config(&cfg) {
                eprintln!("network scheduler: failed to save config: {e}");
                return false;
            }
        }
    }
    if changed {
        let _ = cluster_notify_tx.send(()).await;
        let _ = swarm_notify_tx.send(()).await;
    }
    changed
}

pub fn spawn_network_scheduler(
    client_config: Arc<RwLock<ClientConfig>>,
    cluster_notify_tx: mpsc::Sender<()>,
    swarm_notify_tx: mpsc::Sender<()>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            apply_schedules(&client_config, &cluster_notify_tx, &swarm_notify_tx).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hhmm_valid() {
        assert_eq!(parse_hhmm("09:00"), Some(540));
        assert_eq!(parse_hhmm("17:30"), Some(17 * 60 + 30));
    }

    #[test]
    fn inside_day_window() {
        assert!(is_inside_window(540, 1020, 600));
        assert!(!is_inside_window(540, 1020, 300));
        assert!(!is_inside_window(540, 1020, 1020));
    }

    #[test]
    fn inside_overnight_window() {
        assert!(is_inside_window(1320, 360, 1380)); // 22:00-06:00 at 23:00
        assert!(is_inside_window(1320, 360, 180)); // 03:00
        assert!(!is_inside_window(1320, 360, 720)); // 12:00
    }
}
