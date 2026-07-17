use crate::client_config::{effective_gpu_thermal_settings, ClientConfig, GpuThermalSettings};
use crate::network_actions::set_pause_all;
use crate::shared::{GpuHostStatus, GpuThermalGuardStatus, SharedState};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Default)]
pub struct GpuThermalGuardTracker {
    over_threshold_since: Option<Instant>,
    peak_temp_c: Option<u8>,
    caused_pause: bool,
}

impl GpuThermalGuardTracker {
    pub fn max_temp_c(gpu: &GpuHostStatus) -> Option<u8> {
        let mut max = gpu.temperature_c;
        if let Some(devices) = &gpu.devices {
            for dev in devices {
                if let Some(t) = dev.temperature_c {
                    max = Some(max.map(|m| m.max(t)).unwrap_or(t));
                }
            }
        }
        max
    }

    pub async fn evaluate(
        &mut self,
        gpu: &GpuHostStatus,
        cfg: &ClientConfig,
        shared_state: &SharedState,
        client_config: &Arc<RwLock<ClientConfig>>,
        cluster_notify_tx: &mpsc::Sender<()>,
        swarm_notify_tx: &mpsc::Sender<()>,
    ) {
        let settings = effective_gpu_thermal_settings(cfg);
        if !settings.enabled || !gpu.available {
            self.reset_if_inactive(shared_state).await;
            return;
        }

        let temp = Self::max_temp_c(gpu);
        let Some(temp) = temp else {
            return;
        };

        if temp >= settings.threshold_c {
            if self.over_threshold_since.is_none() {
                self.over_threshold_since = Some(Instant::now());
                self.peak_temp_c = Some(temp);
            } else {
                self.peak_temp_c = Some(
                    self.peak_temp_c
                        .map(|p| p.max(temp))
                        .unwrap_or(temp),
                );
            }
            let elapsed = self
                .over_threshold_since
                .map(|t| t.elapsed().as_secs())
                .unwrap_or(0);
            if elapsed >= settings.duration_secs && !self.caused_pause {
                if set_pause_all(client_config, cluster_notify_tx, swarm_notify_tx, true)
                    .await
                    .is_ok()
                {
                    self.caused_pause = true;
                    let mut app = shared_state.lock().await;
                    app.gpu_thermal_guard = GpuThermalGuardStatus {
                        active: true,
                        triggered_at: Some(crate::gpu_history::unix_now()),
                        peak_temp_c: self.peak_temp_c,
                        threshold_c: settings.threshold_c,
                        caused_pause: true,
                    };
                }
            } else {
                let mut app = shared_state.lock().await;
                if !app.gpu_thermal_guard.active {
                    app.gpu_thermal_guard.threshold_c = settings.threshold_c;
                }
            }
        } else if temp <= settings.cooldown_c {
            self.over_threshold_since = None;
            self.peak_temp_c = None;
            if self.caused_pause && settings.auto_resume {
                if set_pause_all(client_config, cluster_notify_tx, swarm_notify_tx, false)
                    .await
                    .is_ok()
                {
                    self.caused_pause = false;
                    let mut app = shared_state.lock().await;
                    app.gpu_thermal_guard = GpuThermalGuardStatus::default();
                }
            } else if self.caused_pause {
                let mut app = shared_state.lock().await;
                app.gpu_thermal_guard.active = false;
            }
        } else {
            // Between cooldown and threshold — keep tracking but do not reset timer fully
            self.over_threshold_since = None;
        }
    }

    async fn reset_if_inactive(&mut self, shared_state: &SharedState) {
        if self.caused_pause {
            return;
        }
        self.over_threshold_since = None;
        self.peak_temp_c = None;
        let mut app = shared_state.lock().await;
        if app.gpu_thermal_guard.active && !app.gpu_thermal_guard.caused_pause {
            app.gpu_thermal_guard = GpuThermalGuardStatus::default();
        }
    }
}

pub fn default_gpu_thermal_settings() -> GpuThermalSettings {
    GpuThermalSettings {
        enabled: true,
        threshold_c: 85,
        duration_secs: 60,
        cooldown_c: 75,
        auto_resume: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::GpuDeviceInfo;

    #[test]
    fn max_temp_from_devices() {
        let gpu = GpuHostStatus {
            available: true,
            utilization_pct: 0,
            memory_used_mb: 0,
            memory_total_mb: 0,
            memory_free_mb: 0,
            temperature_c: Some(70),
            devices: Some(vec![
                GpuDeviceInfo {
                    index: 0,
                    name: "A".into(),
                    producer: None,
                    architecture: None,
                    driver_version: None,
                    pci_bus_id: None,
                    utilization_pct: 0,
                    memory_utilization_pct: None,
                    memory_used_mb: 0,
                    memory_total_mb: 0,
                    temperature_c: Some(88),
                    power_draw_w: None,
                    power_limit_w: None,
                    fan_speed_pct: None,
                },
                GpuDeviceInfo {
                    index: 1,
                    name: "B".into(),
                    producer: None,
                    architecture: None,
                    driver_version: None,
                    pci_bus_id: None,
                    utilization_pct: 0,
                    memory_utilization_pct: None,
                    memory_used_mb: 0,
                    memory_total_mb: 0,
                    temperature_c: Some(82),
                    power_draw_w: None,
                    power_limit_w: None,
                    fan_speed_pct: None,
                },
            ]),
            name: None,
            producer: None,
            architecture: None,
            driver_version: None,
            cuda_version: None,
            device_count: None,
            memory_utilization_pct: None,
            power_draw_w: None,
            source: None,
            sampled_at_unix: None,
        };
        assert_eq!(GpuThermalGuardTracker::max_temp_c(&gpu), Some(88));
    }
}
