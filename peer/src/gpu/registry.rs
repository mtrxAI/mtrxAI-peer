use crate::gpu::command::command_available;
use std::sync::OnceLock;
use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GpuProbeRegistry {
    pub nvidia_smi: bool,
    pub amd_smi: bool,
    pub rocm_smi: bool,
    pub apple: bool,
}

static GPU_PROBE_REGISTRY: OnceLock<GpuProbeRegistry> = OnceLock::new();

pub fn gpu_probe_registry() -> &'static GpuProbeRegistry {
    GPU_PROBE_REGISTRY.get_or_init(detect_gpu_probes)
}

fn detect_gpu_probes() -> GpuProbeRegistry {
    let vendor_filter = vendor_filter_from_env();

    let nvidia_smi = vendor_filter.allows("nvidia") && command_available("nvidia-smi");
    let amd_smi = vendor_filter.allows("amd") && command_available("amd-smi");
    let rocm_smi =
        vendor_filter.allows("amd") && !amd_smi && command_available("rocm-smi");
    let apple = vendor_filter.allows("apple") && cfg_apple_probe_enabled();

    info!(
        "GPU probes: nvidia-smi={} amd-smi={} rocm-smi={} apple={}",
        yes_no(nvidia_smi),
        yes_no(amd_smi),
        yes_no(rocm_smi),
        yes_no(apple),
    );

    GpuProbeRegistry {
        nvidia_smi,
        amd_smi,
        rocm_smi,
        apple,
    }
}

fn cfg_apple_probe_enabled() -> bool {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        command_available("ioreg") && command_available("sysctl")
    }
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    {
        false
    }
}

fn yes_no(v: bool) -> &'static str {
    if v { "yes" } else { "no" }
}

struct VendorFilter {
    nvidia: bool,
    amd: bool,
    apple: bool,
}

impl VendorFilter {
    fn allows(&self, vendor: &str) -> bool {
        match vendor {
            "nvidia" => self.nvidia,
            "amd" => self.amd,
            "apple" => self.apple,
            _ => true,
        }
    }
}

fn vendor_filter_from_env() -> VendorFilter {
    let default = VendorFilter {
        nvidia: true,
        amd: true,
        apple: true,
    };
    let Ok(raw) = std::env::var("MTRXAI_GPU_PROBE_VENDORS") else {
        return default;
    };
    let lower = raw.to_lowercase();
    if lower.trim().is_empty() || lower.trim() == "all" {
        return default;
    }
    let tokens: Vec<&str> = lower.split(',').map(str::trim).collect();
    VendorFilter {
        nvidia: tokens.iter().any(|t| *t == "nvidia"),
        amd: tokens.iter().any(|t| *t == "amd"),
        apple: tokens.iter().any(|t| *t == "apple"),
    }
}
