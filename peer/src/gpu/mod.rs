mod aggregate;
mod amd;
mod apple;
mod command;
mod intel;
mod nvidia;
mod ollama;
mod registry;
mod util;

pub use aggregate::{aggregate_gpu_devices, merge_vendor_statuses};
pub use amd::{probe_amd_smi, probe_rocm_smi};
pub use apple::probe_apple;
pub use intel::{probe_intel_gpu_top, probe_xpu_smi};
pub use nvidia::probe_nvidia_smi;
pub use ollama::{parse_gpu_from_ollama_info, probe_gpu_via_ollama};
pub use registry::gpu_probe_registry;
pub use util::infer_producer;

use crate::gpu::util::unavailable_gpu_status;
use crate::shared::GpuHostStatus;
use reqwest::Client;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpuProbeMode {
    Auto,
    Off,
    Force,
}

pub fn gpu_probe_mode() -> GpuProbeMode {
    match std::env::var("MTRXAI_GPU_PROBE")
        .unwrap_or_else(|_| "auto".to_string())
        .to_lowercase()
        .as_str()
    {
        "off" => GpuProbeMode::Off,
        "force" => GpuProbeMode::Force,
        _ => GpuProbeMode::Auto,
    }
}

pub fn probe_gpu_nvidia_smi() -> Option<GpuHostStatus> {
    let reg = gpu_probe_registry();
    reg.nvidia_smi
        .as_ref()
        .and_then(|path| probe_nvidia_smi(path))
}

fn probe_native_vendors() -> Option<GpuHostStatus> {
    let reg = gpu_probe_registry();
    let mut results = Vec::new();

    if let Some(path) = reg.nvidia_smi.as_ref() {
        if let Some(gpu) = probe_nvidia_smi(path) {
            results.push(gpu);
        }
    }
    if let Some(path) = reg.amd_smi.as_ref() {
        if let Some(gpu) = probe_amd_smi(path) {
            results.push(gpu);
        }
    } else if let Some(path) = reg.rocm_smi.as_ref() {
        if let Some(gpu) = probe_rocm_smi(path) {
            results.push(gpu);
        }
    }
    if let Some(path) = reg.xpu_smi.as_ref() {
        if let Some(gpu) = probe_xpu_smi(path) {
            results.push(gpu);
        }
    } else if let Some(path) = reg.intel_gpu_top.as_ref() {
        if let Some(gpu) = probe_intel_gpu_top(path) {
            results.push(gpu);
        }
    }
    if reg.apple {
        if let (Some(ioreg), Some(sysctl)) = (reg.apple_ioreg.as_ref(), reg.apple_sysctl.as_ref()) {
            if let Some(gpu) = probe_apple(ioreg, sysctl) {
                results.push(gpu);
            }
        }
    }

    if results.is_empty() {
        None
    } else if results.len() == 1 {
        results.pop()
    } else {
        Some(merge_vendor_statuses(results))
    }
}

pub async fn probe_gpu_host(
    mode: GpuProbeMode,
    client: Option<&Client>,
    ollama_url: Option<&str>,
) -> Option<GpuHostStatus> {
    if mode == GpuProbeMode::Off {
        return None;
    }

    if let Some(gpu) = probe_native_vendors() {
        return Some(gpu);
    }

    if let (Some(client), Some(url)) = (client, ollama_url) {
        if let Some(gpu) = probe_gpu_via_ollama(client, url).await {
            return Some(gpu);
        }
    }

    if mode == GpuProbeMode::Force {
        Some(unavailable_gpu_status())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::GpuDeviceInfo;
    use serde_json::json;

    fn sample_device(name: &str, producer: &str, util: u8) -> GpuDeviceInfo {
        GpuDeviceInfo {
            index: 0,
            name: name.to_string(),
            producer: Some(producer.to_string()),
            architecture: None,
            driver_version: None,
            pci_bus_id: None,
            utilization_pct: util,
            memory_utilization_pct: Some(50),
            memory_used_mb: 8000,
            memory_total_mb: 24000,
            temperature_c: Some(60),
            power_draw_w: Some(100),
            power_limit_w: None,
            fan_speed_pct: None,
        }
    }

    #[test]
    fn merge_nvidia_and_amd_devices() {
        let nvidia = aggregate_gpu_devices(
            vec![sample_device("RTX 4090", "NVIDIA", 40)],
            Some("12.4".to_string()),
            "nvidia-smi",
        );
        let amd = aggregate_gpu_devices(
            vec![sample_device("RX 7900 XTX", "AMD", 80)],
            None,
            "rocm-smi",
        );
        let merged = merge_vendor_statuses(vec![nvidia, amd]);
        assert_eq!(merged.device_count, Some(2));
        assert_eq!(merged.producer.as_deref(), Some("Mixed"));
        assert_eq!(merged.source.as_deref(), Some("nvidia-smi+rocm-smi"));
        assert_eq!(merged.cuda_version.as_deref(), Some("12.4"));
        assert_eq!(merged.utilization_pct, 60);
        assert_eq!(merged.devices.as_ref().map(|d| d.len()), Some(2));
    }

    #[test]
    fn parse_ollama_gpu_info() {
        let body = json!({
            "compute": {
                "supported_gpus": [{
                    "gpu_id": "0",
                    "name": "NVIDIA GeForce RTX 4090",
                    "total_memory": 25769803776_u64,
                    "free_memory": 23622320128_u64,
                    "driver": "550.54",
                    "compute": "8.9",
                    "runner": "cuda_v12"
                }]
            }
        });
        let gpu = parse_gpu_from_ollama_info(&body).expect("gpu");
        assert!(gpu.available);
        assert_eq!(gpu.name.as_deref(), Some("NVIDIA GeForce RTX 4090"));
        assert_eq!(gpu.producer.as_deref(), Some("NVIDIA"));
        assert_eq!(gpu.architecture.as_deref(), Some("Ada Lovelace"));
        assert_eq!(gpu.source.as_deref(), Some("ollama"));
    }
}
