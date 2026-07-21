use crate::gpu::aggregate::aggregate_gpu_devices;
use crate::gpu::command::run_command;
use crate::gpu::util::{mb_from_bytes, parse_optional_u32, parse_optional_u8, pct_from_used_total};
use crate::shared::{GpuDeviceInfo, GpuHostStatus};
use serde_json::Value;
use std::path::Path;

pub fn probe_xpu_smi(xpu_smi: &Path) -> Option<GpuHostStatus> {
    let discovery = run_command(xpu_smi, &["discovery", "-j"])
        .or_else(|| run_command(xpu_smi, &["discovery", "--json"]))
        .or_else(|| run_command(xpu_smi, &["discovery"]))?;

    let discovery_body: Value = serde_json::from_str(&discovery).ok()?;
    let devices_meta = discovery_device_entries(&discovery_body)?;
    if devices_meta.is_empty() {
        return None;
    }

    let mut devices = Vec::new();
    for (idx, meta) in devices_meta.iter().enumerate() {
        let device_id = meta
            .get("device_id")
            .or_else(|| meta.get("deviceId"))
            .or_else(|| meta.get("id"))
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(idx as u64);

        let name = meta
            .get("device_name")
            .or_else(|| meta.get("deviceName"))
            .or_else(|| meta.get("name"))
            .or_else(|| meta.get("pci_device_name"))
            .and_then(|v| v.as_str())
            .unwrap_or("Intel GPU")
            .to_string();

        let stats = run_command(xpu_smi, &["stats", "-d", &device_id.to_string(), "-j"])
            .or_else(|| run_command(xpu_smi, &["stats", "-d", &device_id.to_string(), "--json"]))
            .or_else(|| run_command(xpu_smi, &["stats", "-d", &device_id.to_string()]));

        let stats_body = stats
            .and_then(|s| serde_json::from_str::<Value>(&s).ok())
            .unwrap_or(Value::Null);

        let util = json_u64(
            &stats_body,
            &[
                "/device_utilization",
                "/gpu_utilization",
                "/utilization",
                "/tile_level/0/gpu_utilization",
            ],
        )
        .or_else(|| {
            stats_body
                .pointer("/device_level/gpu_util")
                .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
        })
        .unwrap_or(0)
        .min(100) as u8;

        let mem_used = json_u64(
            &stats_body,
            &[
                "/memory_used",
                "/gpu_memory_used",
                "/tile_level/0/gpu_memory_used",
            ],
        )
        .map(normalize_mem_to_mb)
        .unwrap_or(0);

        let mem_total = json_u64(
            &stats_body,
            &[
                "/memory_total",
                "/gpu_memory_total",
                "/tile_level/0/gpu_memory_total",
            ],
        )
        .or_else(|| {
            meta.get("memory_physical_size_byte")
                .or_else(|| meta.get("memory_size"))
                .and_then(|v| v.as_u64())
                .map(normalize_mem_to_mb)
        })
        .unwrap_or(0);

        let temp = json_u64(
            &stats_body,
            &[
                "/gpu_temperature",
                "/temperature",
                "/tile_level/0/gpu_core_temperature",
            ],
        )
        .map(|t| t.min(255) as u8);

        let power = json_u64(
            &stats_body,
            &["/power", "/gpu_power", "/tile_level/0/power"],
        )
        .map(|p| p as u32)
        .or_else(|| {
            stats_body
                .pointer("/power")
                .and_then(|v| v.as_f64())
                .map(|p| p.round() as u32)
        });

        devices.push(GpuDeviceInfo {
            index: idx.min(u8::MAX as usize) as u8,
            name: name.clone(),
            producer: Some("Intel".to_string()),
            architecture: meta
                .get("gfx_firmware_version")
                .or_else(|| meta.get("device_type"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            driver_version: meta
                .get("driver_version")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            pci_bus_id: meta
                .get("pci_bdf_address")
                .or_else(|| meta.get("pci_bdf"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            utilization_pct: util,
            memory_utilization_pct: if mem_total > 0 {
                Some(pct_from_used_total(mem_used, mem_total))
            } else {
                None
            },
            memory_used_mb: mem_used,
            memory_total_mb: mem_total,
            temperature_c: temp,
            power_draw_w: power,
            power_limit_w: None,
            fan_speed_pct: None,
        });
    }

    if devices.is_empty() {
        return None;
    }

    Some(aggregate_gpu_devices(devices, None, "xpu-smi"))
}

pub fn probe_intel_gpu_top(intel_gpu_top: &Path) -> Option<GpuHostStatus> {
    // -J JSON, -s sample period ms, run briefly then exit via timeout-friendly args.
    let text = run_command(intel_gpu_top, &["-J", "-s", "100", "-o", "-"])
        .or_else(|| run_command(intel_gpu_top, &["-J", "-s", "100"]))?;
    parse_intel_gpu_top_json(&text)
}

pub fn parse_intel_gpu_top_json(text: &str) -> Option<GpuHostStatus> {
    // intel_gpu_top may emit a stream of JSON objects; take the first object/array.
    let trimmed = text.trim();
    let body: Value = if let Ok(v) = serde_json::from_str(trimmed) {
        v
    } else {
        // Concatenated objects: try first `{...}` block
        let start = trimmed.find('{')?;
        let end = trimmed[start..]
            .find("}\n")
            .map(|i| start + i + 1)
            .or_else(|| {
                let mut depth = 0i32;
                for (i, c) in trimmed[start..].char_indices() {
                    match c {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                return Some(start + i + 1);
                            }
                        }
                        _ => {}
                    }
                }
                None
            })?;
        serde_json::from_str(&trimmed[start..end]).ok()?
    };

    let engines = body
        .get("engines")
        .or_else(|| body.pointer("/engines"))
        .cloned()
        .unwrap_or(Value::Null);

    let util = average_engine_busy(&engines).unwrap_or(0);

    let mem_used = body
        .pointer("/clients")
        .and_then(|c| c.as_object())
        .map(|clients| {
            clients
                .values()
                .filter_map(|client| {
                    client
                        .pointer("/memory/system/total")
                        .or_else(|| client.pointer("/memory/local/total"))
                        .and_then(|v| v.as_u64())
                })
                .sum::<u64>()
        })
        .map(mb_from_bytes)
        .unwrap_or(0);

    let device = GpuDeviceInfo {
        index: 0,
        name: body
            .get("name")
            .or_else(|| body.get("device"))
            .and_then(|v| v.as_str())
            .unwrap_or("Intel GPU")
            .to_string(),
        producer: Some("Intel".to_string()),
        architecture: None,
        driver_version: None,
        pci_bus_id: None,
        utilization_pct: util,
        memory_utilization_pct: None,
        memory_used_mb: mem_used,
        memory_total_mb: 0,
        temperature_c: parse_optional_u8(
            body.get("temperature")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
        ),
        power_draw_w: parse_optional_u32(
            &body
                .pointer("/power/GPU")
                .or_else(|| body.pointer("/power/gpu"))
                .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
                .map(|p| p.round().to_string())
                .unwrap_or_default(),
        ),
        power_limit_w: None,
        fan_speed_pct: None,
    };

    Some(aggregate_gpu_devices(vec![device], None, "intel_gpu_top"))
}

fn discovery_device_entries(body: &Value) -> Option<Vec<&Value>> {
    if let Some(arr) = body.as_array() {
        return Some(arr.iter().collect());
    }
    if let Some(arr) = body
        .get("device_list")
        .or_else(|| body.get("devices"))
        .or_else(|| body.get("deviceList"))
        .and_then(|v| v.as_array())
    {
        return Some(arr.iter().collect());
    }
    // Single-device object
    if body.get("device_id").is_some() || body.get("device_name").is_some() {
        return Some(vec![body]);
    }
    None
}

fn json_u64(body: &Value, pointers: &[&str]) -> Option<u64> {
    for ptr in pointers {
        if let Some(v) = body.pointer(ptr) {
            if let Some(n) = v.as_u64() {
                return Some(n);
            }
            if let Some(f) = v.as_f64() {
                return Some(f.round() as u64);
            }
            if let Some(s) = v.as_str() {
                if let Ok(n) = s.parse::<u64>() {
                    return Some(n);
                }
                if let Ok(f) = s.parse::<f64>() {
                    return Some(f.round() as u64);
                }
            }
        }
    }
    None
}

fn normalize_mem_to_mb(v: u64) -> u64 {
    // Heuristic: values that look like bytes → MiB
    if v > 1_000_000 {
        v / (1024 * 1024)
    } else {
        v
    }
}

fn average_engine_busy(engines: &Value) -> Option<u8> {
    let obj = engines.as_object()?;
    let mut sum = 0.0f64;
    let mut count = 0u64;
    for (_name, engine) in obj {
        let busy = engine
            .get("busy")
            .or_else(|| engine.get("busy%"))
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_u64().map(|n| n as f64))
                    .or_else(|| {
                        v.as_str()
                            .and_then(|s| s.trim_end_matches('%').parse().ok())
                    })
            });
        if let Some(b) = busy {
            sum += b;
            count += 1;
        }
    }
    if count == 0 {
        None
    } else {
        Some(((sum / count as f64).round() as u64).min(100) as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_xpu_discovery_and_stats_fields() {
        let discovery = json!({
            "device_list": [{
                "device_id": 0,
                "device_name": "Intel Data Center GPU Max 1550",
                "pci_bdf_address": "0000:3a:00.0",
                "driver_version": "1.2.3",
                "memory_physical_size_byte": 51539607552_u64
            }]
        });
        let entries = discovery_device_entries(&discovery).expect("entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].get("device_name").and_then(|v| v.as_str()),
            Some("Intel Data Center GPU Max 1550")
        );
        assert_eq!(normalize_mem_to_mb(51539607552), 49152);
    }

    #[test]
    fn parse_intel_gpu_top_sample() {
        let text = r#"{
            "engines": {
                "Render/3D": { "busy": 40.0 },
                "Blitter": { "busy": 10.0 },
                "Video": { "busy": 0.0 }
            },
            "clients": {
                "1234": {
                    "memory": { "system": { "total": 2147483648 } }
                }
            },
            "power": { "GPU": 25.5 }
        }"#;
        let status = parse_intel_gpu_top_json(text).expect("parsed");
        assert!(status.available);
        assert_eq!(status.source.as_deref(), Some("intel_gpu_top"));
        assert_eq!(status.producer.as_deref(), Some("Intel"));
        assert_eq!(status.utilization_pct, 17); // (40+10+0)/3
        assert_eq!(status.memory_used_mb, 2048);
        assert_eq!(status.power_draw_w, Some(26));
    }
}
