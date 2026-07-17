use crate::gpu_history::unix_now;
use crate::gpu::util::{avg_optional_u8, infer_producer};
use crate::shared::{GpuDeviceInfo, GpuHostStatus};
use std::collections::HashSet;

pub fn aggregate_gpu_devices(
    devices: Vec<GpuDeviceInfo>,
    cuda_version: Option<String>,
    source: &str,
) -> GpuHostStatus {
    let device_count = devices.len().min(u8::MAX as usize) as u8;
    let utilization_pct = if device_count > 0 {
        (devices
            .iter()
            .map(|d| u64::from(d.utilization_pct))
            .sum::<u64>()
            / u64::from(device_count)) as u8
    } else {
        0
    };
    let memory_used_mb: u64 = devices.iter().map(|d| d.memory_used_mb).sum();
    let memory_total_mb: u64 = devices.iter().map(|d| d.memory_total_mb).sum();
    let memory_free_mb = memory_total_mb.saturating_sub(memory_used_mb);
    let memory_utilization_pct = if memory_total_mb > 0 {
        Some(((memory_used_mb * 100) / memory_total_mb).min(100) as u8)
    } else {
        None
    };
    let temperature_c = avg_optional_u8(devices.iter().filter_map(|d| d.temperature_c));
    let power_total: u32 = devices.iter().filter_map(|d| d.power_draw_w).sum();
    let power_draw_w = if power_total == 0 {
        None
    } else {
        Some(power_total)
    };
    let names: Vec<String> = devices.iter().map(|d| d.name.clone()).collect();
    let producer = host_producer_from_devices(&devices, &names);
    let architecture = devices.iter().find_map(|d| d.architecture.clone());
    let driver_version = devices.iter().find_map(|d| d.driver_version.clone());

    GpuHostStatus {
        available: true,
        utilization_pct,
        memory_used_mb,
        memory_total_mb,
        memory_free_mb,
        name: if names.is_empty() {
            None
        } else {
            Some(names.join(", "))
        },
        producer,
        architecture,
        driver_version,
        cuda_version,
        device_count: Some(device_count),
        temperature_c,
        memory_utilization_pct,
        power_draw_w,
        source: Some(source.to_string()),
        sampled_at_unix: Some(unix_now()),
        devices: Some(devices),
    }
}

fn host_producer_from_devices(devices: &[GpuDeviceInfo], names: &[String]) -> Option<String> {
    let mut producers: Vec<String> = devices
        .iter()
        .filter_map(|d| d.producer.clone())
        .collect();
    if producers.is_empty() {
        producers = names
            .iter()
            .filter_map(|n| infer_producer(n, None))
            .collect();
    }
    let unique: HashSet<String> = producers.into_iter().collect();
    if unique.is_empty() {
        None
    } else if unique.len() == 1 {
        unique.into_iter().next()
    } else {
        Some("Mixed".to_string())
    }
}

pub fn merge_vendor_statuses(mut statuses: Vec<GpuHostStatus>) -> GpuHostStatus {
    if statuses.len() == 1 {
        return statuses.pop().unwrap();
    }

    let mut devices = Vec::new();
    let mut sources = Vec::new();
    let mut cuda_version = None;

    for status in &statuses {
        if let Some(source) = &status.source {
            sources.push(source.clone());
        }
        if cuda_version.is_none() {
            cuda_version = status.cuda_version.clone();
        }
        if let Some(devs) = &status.devices {
            devices.extend(devs.iter().cloned());
        }
    }

    for (idx, dev) in devices.iter_mut().enumerate() {
        dev.index = idx.min(u8::MAX as usize) as u8;
    }

    let source = sources.join("+");
    let mut merged = aggregate_gpu_devices(devices, cuda_version, &source);
    merged.source = Some(source);
    merged
}
