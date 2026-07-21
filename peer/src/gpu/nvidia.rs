use crate::gpu::aggregate::aggregate_gpu_devices;
use crate::gpu::command::{command_no_window, run_command};
use crate::gpu::util::{
    infer_producer, nvidia_architecture_label, parse_optional_u32, parse_optional_u8,
};
use crate::shared::{GpuDeviceInfo, GpuHostStatus};
use std::path::Path;

fn probe_cuda_version(nvidia_smi: &Path) -> Option<String> {
    let text = run_command(nvidia_smi, &[])?;
    for line in text.lines() {
        if let Some(idx) = line.find("CUDA Version:") {
            let rest = line[idx + "CUDA Version:".len()..].trim();
            return rest.split_whitespace().next().map(str::to_string);
        }
    }
    None
}

pub fn probe_nvidia_smi(nvidia_smi: &Path) -> Option<GpuHostStatus> {
    let output = command_no_window(nvidia_smi)
        .args([
            "--query-gpu=index,name,pci.bus_id,driver_version,utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu,power.draw,power.limit,fan.speed,compute_cap",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let cuda_version = probe_cuda_version(nvidia_smi);
    let text = String::from_utf8_lossy(&output.stdout);
    let mut devices = Vec::new();

    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() < 13 {
            continue;
        }
        let index = parts[0].parse::<u8>().unwrap_or(devices.len() as u8);
        let name = parts[1].to_string();
        let compute_cap = parts[12].to_string();
        devices.push(GpuDeviceInfo {
            index,
            name: name.clone(),
            producer: infer_producer(&name, None),
            architecture: if compute_cap.is_empty() {
                None
            } else {
                Some(nvidia_architecture_label(&compute_cap))
            },
            driver_version: if parts[3].is_empty() {
                None
            } else {
                Some(parts[3].to_string())
            },
            pci_bus_id: if parts[2].is_empty() {
                None
            } else {
                Some(parts[2].to_string())
            },
            utilization_pct: parts[4].parse().unwrap_or(0).min(100),
            memory_utilization_pct: parse_optional_u8(parts[5]),
            memory_used_mb: parts[6].parse().unwrap_or(0),
            memory_total_mb: parts[7].parse().unwrap_or(0),
            temperature_c: parse_optional_u8(parts[8]),
            power_draw_w: parse_optional_u32(parts[9]),
            power_limit_w: parse_optional_u32(parts[10]),
            fan_speed_pct: parse_optional_u8(parts[11]),
        });
    }

    if devices.is_empty() {
        return None;
    }

    Some(aggregate_gpu_devices(devices, cuda_version, "nvidia-smi"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_nvidia_csv_line() {
        let line = "0, NVIDIA GeForce RTX 4090, 00000000:01:00.0, 550.54, 42, 10, 4096, 24576, 65, 120.5, 450.0, 30, 8.9";
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        assert_eq!(parts.len(), 13);
        assert_eq!(parts[1], "NVIDIA GeForce RTX 4090");
        assert_eq!(parts[4].parse::<u8>().unwrap(), 42);
        assert_eq!(nvidia_architecture_label(parts[12]), "Ada Lovelace");
    }
}
