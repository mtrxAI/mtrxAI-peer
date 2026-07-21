use crate::gpu::aggregate::aggregate_gpu_devices;
use crate::gpu::util::{infer_producer, nvidia_architecture_label, pct_from_used_total};
use crate::shared::{GpuDeviceInfo, GpuHostStatus};
use reqwest::Client;
use serde_json::Value;

pub fn parse_gpu_from_ollama_info(body: &Value) -> Option<GpuHostStatus> {
    let gpus = body
        .pointer("/compute/supported_gpus")
        .or_else(|| body.pointer("/supported_gpus"))
        .and_then(|v| v.as_array())
        .filter(|gpus| !gpus.is_empty())?;

    let mut devices = Vec::new();
    for (idx, gpu) in gpus.iter().enumerate() {
        let total = gpu.get("total_memory").and_then(|v| v.as_u64())?;
        let free = gpu
            .get("free_memory")
            .and_then(|v| v.as_u64())
            .unwrap_or(total);
        let used = total.saturating_sub(free);
        let name = gpu
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("GPU")
            .to_string();
        let runner = gpu.get("runner").and_then(|v| v.as_str());
        let compute = gpu.get("compute").and_then(|v| v.as_str()).unwrap_or("");
        devices.push(GpuDeviceInfo {
            index: idx.min(u8::MAX as usize) as u8,
            name: name.clone(),
            producer: infer_producer(&name, runner),
            architecture: if compute.is_empty() {
                None
            } else {
                Some(nvidia_architecture_label(compute))
            },
            driver_version: gpu
                .get("driver")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            pci_bus_id: gpu
                .get("gpu_id")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            utilization_pct: pct_from_used_total(used, total),
            memory_utilization_pct: if total > 0 {
                Some(pct_from_used_total(used, total))
            } else {
                None
            },
            memory_used_mb: used / (1024 * 1024),
            memory_total_mb: total / (1024 * 1024),
            temperature_c: None,
            power_draw_w: None,
            power_limit_w: None,
            fan_speed_pct: None,
        });
    }

    let status = aggregate_gpu_devices(devices, None, "ollama");
    Some(status)
}

pub async fn probe_gpu_via_ollama(client: &Client, ollama_url: &str) -> Option<GpuHostStatus> {
    let url = format!("{}/api/info", ollama_url.trim_end_matches('/'));
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: Value = resp.json().await.ok()?;
    parse_gpu_from_ollama_info(&body)
}
