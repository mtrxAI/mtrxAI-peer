use crate::gpu::command::{
    amd_smi_candidates, intel_gpu_top_candidates, nvidia_smi_candidates, resolve_command,
    rocm_smi_candidates, xpu_smi_candidates,
};
use std::path::PathBuf;
use std::sync::OnceLock;
use tracing::info;

#[derive(Debug, Clone)]
pub struct GpuProbeRegistry {
    pub nvidia_smi: Option<PathBuf>,
    pub amd_smi: Option<PathBuf>,
    pub rocm_smi: Option<PathBuf>,
    pub xpu_smi: Option<PathBuf>,
    pub intel_gpu_top: Option<PathBuf>,
    pub apple: bool,
    pub apple_ioreg: Option<PathBuf>,
    pub apple_sysctl: Option<PathBuf>,
}

static GPU_PROBE_REGISTRY: OnceLock<GpuProbeRegistry> = OnceLock::new();

pub fn gpu_probe_registry() -> &'static GpuProbeRegistry {
    GPU_PROBE_REGISTRY.get_or_init(detect_gpu_probes)
}

fn detect_gpu_probes() -> GpuProbeRegistry {
    let vendor_filter = vendor_filter_from_env();

    let nvidia_smi = if vendor_filter.allows("nvidia") {
        resolve_command("nvidia-smi", nvidia_smi_candidates())
    } else {
        None
    };

    let amd_smi = if vendor_filter.allows("amd") {
        resolve_command("amd-smi", amd_smi_candidates())
    } else {
        None
    };
    let rocm_smi = if vendor_filter.allows("amd") && amd_smi.is_none() {
        resolve_command("rocm-smi", rocm_smi_candidates())
    } else {
        None
    };

    let xpu_smi = if vendor_filter.allows("intel") {
        resolve_command("xpu-smi", xpu_smi_candidates())
    } else {
        None
    };
    let intel_gpu_top = if vendor_filter.allows("intel") && xpu_smi.is_none() {
        #[cfg(target_os = "linux")]
        {
            resolve_command("intel_gpu_top", intel_gpu_top_candidates())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = intel_gpu_top_candidates;
            None
        }
    } else {
        None
    };

    let (apple, apple_ioreg, apple_sysctl) = if vendor_filter.allows("apple") {
        cfg_apple_probe_paths()
    } else {
        (false, None, None)
    };

    let intel_display = xpu_smi
        .as_ref()
        .or(intel_gpu_top.as_ref())
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "no".to_string());

    info!(
        "GPU probes: nvidia-smi={} amd-smi={} rocm-smi={} intel={} apple={}",
        path_or_no(nvidia_smi.as_ref()),
        path_or_no(amd_smi.as_ref()),
        path_or_no(rocm_smi.as_ref()),
        intel_display,
        yes_no(apple),
    );

    GpuProbeRegistry {
        nvidia_smi,
        amd_smi,
        rocm_smi,
        xpu_smi,
        intel_gpu_top,
        apple,
        apple_ioreg,
        apple_sysctl,
    }
}

fn cfg_apple_probe_paths() -> (bool, Option<PathBuf>, Option<PathBuf>) {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        let ioreg = resolve_command("ioreg", &["/usr/sbin/ioreg"]);
        let sysctl = resolve_command("sysctl", &["/usr/sbin/sysctl"]);
        let ok = ioreg.is_some() && sysctl.is_some();
        (ok, ioreg, sysctl)
    }
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    {
        (false, None, None)
    }
}

fn path_or_no(path: Option<&PathBuf>) -> String {
    path.map(|p| p.display().to_string())
        .unwrap_or_else(|| "no".to_string())
}

fn yes_no(v: bool) -> &'static str {
    if v {
        "yes"
    } else {
        "no"
    }
}

struct VendorFilter {
    nvidia: bool,
    amd: bool,
    intel: bool,
    apple: bool,
}

impl VendorFilter {
    fn allows(&self, vendor: &str) -> bool {
        match vendor {
            "nvidia" => self.nvidia,
            "amd" => self.amd,
            "intel" => self.intel,
            "apple" => self.apple,
            _ => true,
        }
    }
}

fn vendor_filter_from_env() -> VendorFilter {
    let default = VendorFilter {
        nvidia: true,
        amd: true,
        intel: true,
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
        intel: tokens.iter().any(|t| *t == "intel"),
        apple: tokens.iter().any(|t| *t == "apple"),
    }
}
