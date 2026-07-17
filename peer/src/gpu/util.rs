use crate::gpu_history::unix_now;
use crate::shared::GpuHostStatus;

pub fn unavailable_gpu_status() -> GpuHostStatus {
    GpuHostStatus {
        available: false,
        utilization_pct: 0,
        memory_used_mb: 0,
        memory_total_mb: 0,
        memory_free_mb: 0,
        name: None,
        producer: None,
        architecture: None,
        driver_version: None,
        cuda_version: None,
        device_count: None,
        temperature_c: None,
        memory_utilization_pct: None,
        power_draw_w: None,
        source: Some("unavailable".to_string()),
        sampled_at_unix: Some(unix_now()),
        devices: None,
    }
}

pub fn infer_producer(name: &str, runner: Option<&str>) -> Option<String> {
    let upper = name.to_uppercase();
    if upper.contains("NVIDIA") || runner.is_some_and(|r| r.contains("cuda")) {
        return Some("NVIDIA".to_string());
    }
    if upper.contains("AMD")
        || upper.contains("RADEON")
        || runner.is_some_and(|r| r.contains("rocm"))
    {
        return Some("AMD".to_string());
    }
    if upper.contains("INTEL") {
        return Some("Intel".to_string());
    }
    if upper.contains("APPLE")
        || upper.contains("M1")
        || upper.contains("M2")
        || upper.contains("M3")
        || upper.contains("M4")
    {
        return Some("Apple".to_string());
    }
    None
}

pub fn nvidia_architecture_label(compute: &str) -> String {
    match compute.trim() {
        "12.0" => "Blackwell".to_string(),
        "9.0" | "8.9" => "Ada Lovelace".to_string(),
        "8.6" | "8.7" | "8.0" => "Ampere".to_string(),
        "7.5" => "Turing".to_string(),
        "7.0" | "7.2" => "Volta".to_string(),
        "6.1" => "Pascal".to_string(),
        other if !other.is_empty() => format!("SM {other}"),
        _ => "Unknown".to_string(),
    }
}

pub fn amd_architecture_label(name: &str, gfx: Option<&str>) -> Option<String> {
    if let Some(g) = gfx {
        let g = g.to_lowercase();
        if g.contains("gfx120") {
            return Some("RDNA 4".to_string());
        }
        if g.contains("gfx110") {
            return Some("RDNA 3".to_string());
        }
        if g.contains("gfx103") || g.contains("gfx101") {
            return Some("RDNA 2".to_string());
        }
        if g.contains("gfx90") {
            return Some("CDNA".to_string());
        }
        return Some(g.to_string());
    }
    let upper = name.to_uppercase();
    if upper.contains("RX 7") || upper.contains("7900") || upper.contains("7800") {
        return Some("RDNA 3".to_string());
    }
    if upper.contains("RX 6") || upper.contains("6900") || upper.contains("6800") {
        return Some("RDNA 2".to_string());
    }
    None
}

pub fn parse_optional_u8(raw: &str) -> Option<u8> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("[N/A]") || trimmed == "N/A" {
        return None;
    }
    trimmed.parse().ok()
}

pub fn parse_optional_u32(raw: &str) -> Option<u32> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("[N/A]") || trimmed == "N/A" {
        return None;
    }
    trimmed.parse().ok()
}

pub fn avg_optional_u8(values: impl Iterator<Item = u8>) -> Option<u8> {
    let mut count = 0u64;
    let mut total = 0u64;
    for value in values {
        count += 1;
        total += u64::from(value);
    }
    if count == 0 {
        None
    } else {
        Some((total / count).min(255) as u8)
    }
}

#[cfg_attr(not(all(target_os = "macos", target_arch = "aarch64")), allow(dead_code))]
pub fn mb_from_bytes(bytes: u64) -> u64 {
    bytes / (1024 * 1024)
}

pub fn pct_from_used_total(used: u64, total: u64) -> u8 {
    if total == 0 {
        0
    } else {
        ((used * 100) / total).min(100) as u8
    }
}
