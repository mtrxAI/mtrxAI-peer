use crate::shared::{GpuHistory, GpuHostStatus, GpuSample};
use std::collections::VecDeque;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const SHORT_CAP: usize = 60;
const LONG_CAP: usize = 60;
const LONG_INTERVAL_SECS: u64 = 60;

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn sample_from_status(gpu: &GpuHostStatus) -> GpuSample {
    GpuSample {
        ts_unix: gpu.sampled_at_unix.unwrap_or_else(unix_now),
        utilization_pct: gpu.utilization_pct,
        memory_used_mb: gpu.memory_used_mb,
        memory_utilization_pct: gpu.memory_utilization_pct,
        temperature_c: gpu.temperature_c,
        power_draw_w: gpu.power_draw_w,
    }
}

fn push_ring(buf: &mut VecDeque<GpuSample>, cap: usize, sample: GpuSample) {
    if buf.len() >= cap {
        buf.pop_front();
    }
    buf.push_back(sample);
}

#[derive(Debug, Default)]
pub struct GpuHistoryTracker {
    short: VecDeque<GpuSample>,
    long: VecDeque<GpuSample>,
    last_long_at: Option<Instant>,
}

impl GpuHistoryTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, gpu: &GpuHostStatus) {
        if !gpu.available {
            return;
        }
        let sample = sample_from_status(gpu);
        push_ring(&mut self.short, SHORT_CAP, sample.clone());

        let now = Instant::now();
        let due = self
            .last_long_at
            .map(|t| now.duration_since(t).as_secs() >= LONG_INTERVAL_SECS)
            .unwrap_or(true);
        if due {
            push_ring(&mut self.long, LONG_CAP, sample);
            self.last_long_at = Some(now);
        }
    }

    pub fn snapshot(&self) -> GpuHistory {
        GpuHistory {
            short: self.short.iter().cloned().collect(),
            long: self.long.iter().cloned().collect(),
        }
    }
}

pub fn spawn_gpu_monitor(
    shared_state: crate::shared::SharedState,
    llm_registry: crate::llm_registry::RegistryHandle,
    client_config: std::sync::Arc<tokio::sync::RwLock<crate::client_config::ClientConfig>>,
    cluster_notify_tx: tokio::sync::mpsc::Sender<()>,
    swarm_notify_tx: tokio::sync::mpsc::Sender<()>,
) {
    let gpu_probe = crate::ollama_client::gpu_probe_mode();
    if gpu_probe == crate::ollama_client::GpuProbeMode::Off {
        return;
    }

    tokio::spawn(async move {
        let mut tracker = GpuHistoryTracker::new();
        let mut thermal_guard = crate::gpu_thermal_guard::GpuThermalGuardTracker::default();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            let backend = llm_registry
                .inner
                .first_ollama_backend()
                .await
                .or(llm_registry.inner.first_attached_backend().await);
            let (client, ollama_url) = match backend.as_deref() {
                Some(crate::llm_backend::LlmBackend::Ollama(b)) => {
                    (Some(b.client.clone()), Some(b.base_url.clone()))
                }
                Some(crate::llm_backend::LlmBackend::OpenAiCompat(b)) => {
                    (Some(b.client.clone()), None)
                }
                Some(crate::llm_backend::LlmBackend::Custom(b)) => {
                    (Some(b.client.clone()), None)
                }
                None => (None, None),
            };

            let gpu = crate::ollama_client::probe_gpu_host(
                gpu_probe,
                client.as_ref(),
                ollama_url.as_deref(),
            )
            .await;

            if let Some(gpu) = gpu {
                tracker.record(&gpu);
                let history = tracker.snapshot();
                let cfg = client_config.read().await.clone();
                thermal_guard
                    .evaluate(
                        &gpu,
                        &cfg,
                        &shared_state,
                        &client_config,
                        &cluster_notify_tx,
                        &swarm_notify_tx,
                    )
                    .await;
                let mut state = shared_state.lock().await;
                state.last_gpu_host = Some(gpu);
                state.gpu_history = history;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::GpuHostStatus;

    fn sample_gpu() -> GpuHostStatus {
        GpuHostStatus {
            available: true,
            utilization_pct: 42,
            memory_used_mb: 1000,
            memory_total_mb: 24000,
            memory_free_mb: 23000,
            name: Some("Test GPU".to_string()),
            producer: Some("NVIDIA".to_string()),
            architecture: Some("8.9".to_string()),
            driver_version: Some("550".to_string()),
            cuda_version: Some("12.4".to_string()),
            device_count: Some(1),
            temperature_c: Some(65),
            memory_utilization_pct: Some(10),
            power_draw_w: Some(120),
            source: Some("test".to_string()),
            sampled_at_unix: Some(1_700_000_000),
            devices: None,
        }
    }

    #[test]
    fn short_buffer_keeps_last_60_samples() {
        let mut tracker = GpuHistoryTracker::new();
        for i in 0..70 {
            let mut gpu = sample_gpu();
            gpu.utilization_pct = i as u8;
            gpu.sampled_at_unix = Some(i as u64);
            tracker.record(&gpu);
        }
        let snap = tracker.snapshot();
        assert_eq!(snap.short.len(), 60);
        assert_eq!(snap.short.first().map(|s| s.utilization_pct), Some(10));
        assert_eq!(snap.short.last().map(|s| s.utilization_pct), Some(69));
    }

    #[test]
    fn long_buffer_only_records_on_interval() {
        let mut tracker = GpuHistoryTracker::new();
        for _ in 0..5 {
            tracker.record(&sample_gpu());
        }
        assert_eq!(tracker.snapshot().long.len(), 1);
    }
}
