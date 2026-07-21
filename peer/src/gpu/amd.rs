use crate::gpu::aggregate::aggregate_gpu_devices;
use crate::gpu::command::run_command;
use crate::gpu::util::{
    amd_architecture_label, parse_optional_u32, parse_optional_u8, pct_from_used_total,
};
use crate::shared::{GpuDeviceInfo, GpuHostStatus};
use serde_json::Value;
use std::path::Path;

pub fn probe_amd_smi(amd_smi: &Path) -> Option<GpuHostStatus> {
    if let Some(status) = probe_amd_smi_json(amd_smi) {
        return Some(status);
    }
    probe_amd_smi_monitor_csv(amd_smi)
}

fn probe_amd_smi_json(amd_smi: &Path) -> Option<GpuHostStatus> {
    let static_json = run_command(amd_smi, &["static", "--json"])?;
    let metric_json = run_command(amd_smi, &["metric", "--json"]).unwrap_or_default();

    let static_body: Value = serde_json::from_str(&static_json).ok()?;
    let metric_body: Value = if metric_json.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&metric_json).unwrap_or(Value::Null)
    };

    parse_amd_smi_json(&static_body, &metric_body)
}

fn parse_amd_smi_json(static_body: &Value, metric_body: &Value) -> Option<GpuHostStatus> {
    let gpu_entries = static_body
        .as_array()
        .or_else(|| static_body.get("gpus").and_then(|v| v.as_array()))
        .or_else(|| static_body.get("gpu_data").and_then(|v| v.as_array()))?;

    if gpu_entries.is_empty() {
        return None;
    }

    let metric_entries = metric_body
        .as_array()
        .or_else(|| metric_body.get("gpus").and_then(|v| v.as_array()))
        .or_else(|| metric_body.get("gpu_data").and_then(|v| v.as_array()));

    let mut devices = Vec::new();
    for (idx, gpu) in gpu_entries.iter().enumerate() {
        let gpu_id = gpu
            .get("gpu")
            .or_else(|| gpu.get("gpu_id"))
            .or_else(|| gpu.get("id"))
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(idx as u64);

        let metric = metric_entries.and_then(|entries| {
            entries.iter().find(|m| {
                m.get("gpu")
                    .or_else(|| m.get("gpu_id"))
                    .or_else(|| m.get("id"))
                    .and_then(|v| {
                        v.as_u64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
                    == Some(gpu_id)
            })
        });

        let name = gpu
            .get("card_series")
            .or_else(|| gpu.get("card_model"))
            .or_else(|| gpu.get("name"))
            .or_else(|| gpu.get("product_name"))
            .and_then(|v| v.as_str())
            .unwrap_or("AMD GPU")
            .to_string();

        let gfx = gpu
            .get("gfx_version")
            .or_else(|| gpu.get("gfx"))
            .and_then(|v| v.as_str());

        let total_mb = gpu
            .pointer("/memory/total/vram")
            .or_else(|| gpu.pointer("/vram/total"))
            .or_else(|| gpu.get("vram_total"))
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .map(|b| if b > 1_000_000 { b / (1024 * 1024) } else { b })
            .unwrap_or(0);

        let used_mb = metric
            .and_then(|m| {
                m.pointer("/memory/used/vram")
                    .or_else(|| m.pointer("/vram/used"))
                    .or_else(|| m.get("vram_used"))
                    .and_then(|v| {
                        v.as_u64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
            })
            .map(|b| if b > 1_000_000 { b / (1024 * 1024) } else { b })
            .unwrap_or(0);

        let util = metric
            .and_then(|m| {
                m.pointer("/utilization/gfx")
                    .or_else(|| m.get("gpu_utilization"))
                    .or_else(|| m.get("gfx_activity"))
                    .and_then(|v| {
                        v.as_u64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
            })
            .unwrap_or(0)
            .min(100) as u8;

        let temp = metric
            .and_then(|m| {
                m.pointer("/temperature/hotspot")
                    .or_else(|| m.pointer("/temperature/edge"))
                    .or_else(|| m.get("temperature"))
                    .and_then(|v| {
                        v.as_u64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
            })
            .map(|t| t.min(255) as u8);

        let power = metric
            .and_then(|m| {
                m.pointer("/power/average")
                    .or_else(|| m.get("power"))
                    .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
            })
            .map(|p| p.round() as u32);

        devices.push(GpuDeviceInfo {
            index: idx.min(u8::MAX as usize) as u8,
            name: name.clone(),
            producer: Some("AMD".to_string()),
            architecture: amd_architecture_label(&name, gfx),
            driver_version: gpu
                .get("driver_version")
                .or_else(|| gpu.get("driver"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            pci_bus_id: gpu
                .get("bdf")
                .or_else(|| gpu.get("pci_bdf"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            utilization_pct: util,
            memory_utilization_pct: if total_mb > 0 {
                Some(pct_from_used_total(used_mb, total_mb))
            } else {
                None
            },
            memory_used_mb: used_mb,
            memory_total_mb: total_mb,
            temperature_c: temp,
            power_draw_w: power,
            power_limit_w: None,
            fan_speed_pct: None,
        });
    }

    if devices.is_empty() {
        return None;
    }

    Some(aggregate_gpu_devices(devices, None, "amd-smi"))
}

fn probe_amd_smi_monitor_csv(amd_smi: &Path) -> Option<GpuHostStatus> {
    let text = run_command(amd_smi, &["monitor", "-putmv", "--csv"])?;
    parse_amd_monitor_csv(&text, "amd-smi")
}

pub fn probe_rocm_smi(rocm_smi: &Path) -> Option<GpuHostStatus> {
    let text = run_command(
        rocm_smi,
        &[
            "--showproductname",
            "--showuse",
            "--showmemuse",
            "--showtemp",
            "--showpower",
            "--csv",
        ],
    )?;
    parse_rocm_smi_csv(rocm_smi, &text)
}

fn parse_rocm_smi_csv(rocm_smi: &Path, text: &str) -> Option<GpuHostStatus> {
    let mut devices = Vec::new();
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();

    for (idx, line) in lines.iter().enumerate() {
        if line.starts_with("card,") || line.eq_ignore_ascii_case("card, gpu use (%),") {
            continue;
        }
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() < 2 {
            continue;
        }

        let card = parts[0].trim_start_matches("card").trim();
        if card.is_empty() || card.eq_ignore_ascii_case("card") {
            continue;
        }

        let name = if parts.len() > 5 {
            parts[5].to_string()
        } else {
            format!("AMD GPU {card}")
        };

        let util = parts.get(1).and_then(|v| parse_optional_u8(v)).unwrap_or(0);
        let mem_util = parts.get(2).and_then(|v| parse_optional_u8(v));
        let temp = parts.get(3).and_then(|v| parse_optional_u8(v));
        let power = parts.get(4).and_then(|v| parse_optional_u32(v));

        devices.push(GpuDeviceInfo {
            index: idx.min(u8::MAX as usize) as u8,
            name: name.clone(),
            producer: Some("AMD".to_string()),
            architecture: amd_architecture_label(&name, None),
            driver_version: None,
            pci_bus_id: None,
            utilization_pct: util,
            memory_utilization_pct: mem_util,
            memory_used_mb: 0,
            memory_total_mb: 0,
            temperature_c: temp,
            power_draw_w: power,
            fan_speed_pct: None,
            power_limit_w: None,
        });
    }

    if devices.is_empty() {
        return None;
    }

    if let Some(mem_text) = run_command(rocm_smi, &["--showmeminfo", "vram", "--csv"]) {
        enrich_rocm_memory(&mut devices, &mem_text);
    }

    Some(aggregate_gpu_devices(devices, None, "rocm-smi"))
}

fn enrich_rocm_memory(devices: &mut [GpuDeviceInfo], mem_text: &str) {
    for line in mem_text.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if line.starts_with("card,") {
            continue;
        }
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() < 4 {
            continue;
        }
        let card_idx = parts[0]
            .trim_start_matches("card")
            .trim()
            .parse::<usize>()
            .ok();
        let used = parts.get(2).and_then(|v| v.parse::<u64>().ok());
        let total = parts.get(3).and_then(|v| v.parse::<u64>().ok());
        if let (Some(idx), Some(used), Some(total)) = (card_idx, used, total) {
            if let Some(dev) = devices.get_mut(idx) {
                dev.memory_used_mb = used / (1024 * 1024);
                dev.memory_total_mb = total / (1024 * 1024);
                dev.memory_utilization_pct =
                    Some(pct_from_used_total(dev.memory_used_mb, dev.memory_total_mb));
            }
        }
    }
}

fn parse_amd_monitor_csv(text: &str, source: &str) -> Option<GpuHostStatus> {
    let mut devices = Vec::new();
    for (idx, line) in text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .enumerate()
    {
        if line.starts_with("gpu,") || line.starts_with("GPU,") {
            continue;
        }
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() < 4 {
            continue;
        }
        let name = format!("AMD GPU {}", parts[0]);
        let power = parts.get(1).and_then(|v| parse_optional_u32(v));
        let util = parts.get(2).and_then(|v| parse_optional_u8(v)).unwrap_or(0);
        let temp = parts.get(3).and_then(|v| parse_optional_u8(v));
        let mem_used = parts
            .get(4)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let vram_total = parts
            .get(5)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        devices.push(GpuDeviceInfo {
            index: idx.min(u8::MAX as usize) as u8,
            name: name.clone(),
            producer: Some("AMD".to_string()),
            architecture: amd_architecture_label(&name, None),
            driver_version: None,
            pci_bus_id: None,
            utilization_pct: util,
            memory_utilization_pct: if vram_total > 0 {
                Some(pct_from_used_total(mem_used, vram_total))
            } else {
                None
            },
            memory_used_mb: mem_used,
            memory_total_mb: vram_total,
            temperature_c: temp,
            power_draw_w: power,
            power_limit_w: None,
            fan_speed_pct: None,
        });
    }

    if devices.is_empty() {
        return None;
    }

    Some(aggregate_gpu_devices(devices, None, source))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parse_rocm_smi_sample() {
        let text = "card, gpu use (%), memory use (%), temperature, power, product name\n\
                    0, 45, 30, 62, 120, AMD Radeon RX 7900 XTX\n\
                    1, 10, 5, 55, 80, AMD Radeon RX 7900 XTX";
        let status = parse_rocm_smi_csv(Path::new("rocm-smi"), text).expect("parsed");
        assert_eq!(status.device_count, Some(2));
        assert_eq!(status.producer.as_deref(), Some("AMD"));
        assert_eq!(status.source.as_deref(), Some("rocm-smi"));
        assert_eq!(status.utilization_pct, 27);
    }
}
