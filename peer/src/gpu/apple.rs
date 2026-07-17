#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
use crate::gpu::aggregate::aggregate_gpu_devices;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
use crate::gpu::command::run_command;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
use crate::gpu::util::{mb_from_bytes, pct_from_used_total};
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
use crate::shared::GpuDeviceInfo;
use crate::shared::GpuHostStatus;

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub fn probe_apple() -> Option<GpuHostStatus> {
    let ioreg_text = run_command("ioreg", &["-r", "-d", "1", "-c", "IOAccelerator", "-l"])?;
    let chip_name = run_command("sysctl", &["-n", "machdep.cpu.brand_string"])
        .or_else(|| run_command("sysctl", &["-n", "hw.model"]))
        .unwrap_or_else(|| "Apple GPU".to_string());

    let (util_pct, mem_used_bytes, mem_total_bytes) = parse_ioreg_performance(&ioreg_text);

    let mem_used_mb = mb_from_bytes(mem_used_bytes);
    let mem_total_mb = if mem_total_bytes > 0 {
        mb_from_bytes(mem_total_bytes)
    } else {
        run_command("sysctl", &["-n", "hw.memsize"])
            .and_then(|s| s.parse::<u64>().ok())
            .map(mb_from_bytes)
            .unwrap_or(0)
    };

    let device = GpuDeviceInfo {
        index: 0,
        name: chip_name.clone(),
        producer: Some("Apple".to_string()),
        architecture: Some(chip_name.clone()),
        driver_version: None,
        pci_bus_id: None,
        utilization_pct: util_pct,
        memory_utilization_pct: if mem_total_mb > 0 {
            Some(pct_from_used_total(mem_used_mb, mem_total_mb))
        } else {
            None
        },
        memory_used_mb: mem_used_mb,
        memory_total_mb: mem_total_mb,
        temperature_c: None,
        power_draw_w: None,
        power_limit_w: None,
        fan_speed_pct: None,
    };

    Some(aggregate_gpu_devices(vec![device], None, "apple-ioreg"))
}

#[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
pub fn probe_apple() -> Option<GpuHostStatus> {
    None
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
fn parse_ioreg_performance(text: &str) -> (u8, u64, u64) {
    let mut util_pct = 0u8;
    let mut mem_used = 0u64;
    let mut mem_total = 0u64;

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.contains("Device Utilization %") {
            if let Some(val) = extract_ioreg_number(trimmed) {
                util_pct = (val as u8).min(100);
            }
        } else if trimmed.contains("In use system memory") {
            if let Some(val) = extract_ioreg_number(trimmed) {
                mem_used = val;
            }
        } else if trimmed.contains("recommendedMaxWorkingSetSize") {
            if let Some(val) = extract_ioreg_number(trimmed) {
                mem_total = val;
            }
        } else if trimmed.contains("PerformanceStatistics") && mem_total == 0 {
            if trimmed.contains("Allocated PB Size") {
                if let Some(val) = extract_ioreg_number(trimmed) {
                    mem_used = val;
                }
            }
        }
    }

    (util_pct, mem_used, mem_total)
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
fn extract_ioreg_number(line: &str) -> Option<u64> {
    let rhs = line.rsplit('=').next()?.trim();
    let digits: String = rhs
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

#[cfg(all(test, target_os = "macos", target_arch = "aarch64"))]
mod tests {
    use super::*;

    #[test]
    fn parse_ioreg_sample() {
        let text = r#"
            "PerformanceStatistics" = {
                "Device Utilization %"=42
                "In use system memory"=3623878656
                "recommendedMaxWorkingSetSize"=26843545600
            }
        "#;
        let (util, used, total) = parse_ioreg_performance(text);
        assert_eq!(util, 42);
        assert_eq!(used, 3623878656);
        assert_eq!(total, 26843545600);
    }
}
