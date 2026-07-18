use peer::network_scheduler::{is_inside_window, parse_hhmm, resolve_cluster_schedule};
use peer::client_config::{ClientConfig, ClusterMembership};

#[test]
fn schedule_resolves_membership_override() {
    let cfg = ClientConfig {
        default_schedule_enabled: Some(false),
        default_schedule_start: Some("08:00".into()),
        default_schedule_end: Some("18:00".into()),
        clusters: vec![ClusterMembership {
            cluster_id: "c1".into(),
            name: None,
            visibility: None,
            accepting_jobs: None,
            connected: None,
            room_secret: None,
            cluster_password: None,
            require_e2ee: None,
            require_tee: None,
            required_attestation_flags: None,
            schedule_enabled: Some(true),
            schedule_start: Some("10:00".into()),
            schedule_end: Some("16:00".into()),
        }],
        ..ClientConfig::default()
    };
    let schedule = resolve_cluster_schedule(&cfg, &cfg.clusters[0]);
    assert!(schedule.enabled);
    assert_eq!(schedule.start_mins, parse_hhmm("10:00").unwrap());
    assert_eq!(schedule.end_mins, parse_hhmm("16:00").unwrap());
    assert!(is_inside_window(schedule.start_mins, schedule.end_mins, 11 * 60));
    assert!(!is_inside_window(schedule.start_mins, schedule.end_mins, 9 * 60));
}

#[test]
fn client_config_defaults_disallow_unattested_peers() {
    let cfg = ClientConfig::default();
    assert!(!cfg.allow_unattested_peers);
}
