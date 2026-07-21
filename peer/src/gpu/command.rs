use std::path::{Path, PathBuf};
use std::process::Command;

pub fn command_no_window(program: impl AsRef<Path>) -> Command {
    let mut command = Command::new(program.as_ref());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    command
}

/// Resolve `name` via PATH, then try absolute `candidates`. Returns the first existing path.
pub fn resolve_command(name: &str, candidates: &[&str]) -> Option<PathBuf> {
    if let Some(path) = resolve_on_path(name) {
        return Some(path);
    }
    for candidate in candidates {
        let path = expand_path_env(candidate);
        if path.is_file() {
            return Some(path);
        }
    }
    None
}

pub fn command_available(name: &str) -> bool {
    resolve_on_path(name).is_some()
}

pub fn run_command(program: impl AsRef<Path>, args: &[&str]) -> Option<String> {
    let output = command_no_window(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn resolve_on_path(name: &str) -> Option<PathBuf> {
    #[cfg(windows)]
    {
        let output = command_no_window("where.exe").arg(name).output().ok()?;
        if !output.status.success() {
            return None;
        }
        let text = String::from_utf8_lossy(&output.stdout);
        let first = text.lines().map(str::trim).find(|l| !l.is_empty())?;
        let path = PathBuf::from(first);
        if path.is_file() {
            Some(path)
        } else {
            None
        }
    }
    #[cfg(not(windows))]
    {
        let output = command_no_window("sh")
            .args(["-c", &format!("command -v {name}")])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let text = String::from_utf8_lossy(&output.stdout);
        let first = text.lines().map(str::trim).find(|l| !l.is_empty())?;
        let path = PathBuf::from(first);
        if path.is_file() || path.exists() {
            Some(path)
        } else {
            None
        }
    }
}

fn expand_path_env(raw: &str) -> PathBuf {
    #[cfg(windows)]
    {
        let mut out = String::new();
        let mut rest = raw;
        while let Some(start) = rest.find('%') {
            out.push_str(&rest[..start]);
            let after = &rest[start + 1..];
            if let Some(end) = after.find('%') {
                let var = &after[..end];
                if let Ok(val) = std::env::var(var) {
                    out.push_str(&val);
                } else {
                    out.push('%');
                    out.push_str(var);
                    out.push('%');
                }
                rest = &after[end + 1..];
            } else {
                out.push('%');
                out.push_str(after);
                rest = "";
            }
        }
        out.push_str(rest);
        PathBuf::from(out)
    }
    #[cfg(not(windows))]
    {
        PathBuf::from(raw)
    }
}

pub fn nvidia_smi_candidates() -> &'static [&'static str] {
    &[
        r"%SystemRoot%\System32\nvidia-smi.exe",
        r"C:\Windows\System32\nvidia-smi.exe",
        r"C:\Program Files\NVIDIA Corporation\NVSMI\nvidia-smi.exe",
        "/usr/bin/nvidia-smi",
        "/usr/local/bin/nvidia-smi",
    ]
}

pub fn amd_smi_candidates() -> &'static [&'static str] {
    &[
        r"C:\Program Files\AMD\ROCm\bin\amd-smi.exe",
        r"C:\Program Files\AMD\ROCm\5.7\bin\amd-smi.exe",
        r"C:\Program Files\AMD\ROCm\6.0\bin\amd-smi.exe",
        r"C:\Program Files\AMD\ROCm\6.1\bin\amd-smi.exe",
        r"C:\Program Files\AMD\ROCm\6.2\bin\amd-smi.exe",
        "/opt/rocm/bin/amd-smi",
        "/usr/bin/amd-smi",
    ]
}

pub fn rocm_smi_candidates() -> &'static [&'static str] {
    &[
        r"C:\Program Files\AMD\ROCm\bin\rocm-smi.exe",
        r"C:\Program Files\AMD\ROCm\5.7\bin\rocm-smi.exe",
        r"C:\Program Files\AMD\ROCm\6.0\bin\rocm-smi.exe",
        "/opt/rocm/bin/rocm-smi",
        "/usr/bin/rocm-smi",
    ]
}

pub fn xpu_smi_candidates() -> &'static [&'static str] {
    &[
        r"C:\Program Files\Intel\XPU-SMI\xpu-smi.exe",
        r"C:\Program Files (x86)\Intel\XPU-SMI\xpu-smi.exe",
        "/usr/bin/xpu-smi",
        "/usr/local/bin/xpu-smi",
    ]
}

pub fn intel_gpu_top_candidates() -> &'static [&'static str] {
    &["/usr/bin/intel_gpu_top", "/usr/local/bin/intel_gpu_top"]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn resolve_command_finds_candidate_file() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("mtrxai-gpu-resolve-{stamp}"));
        fs::create_dir_all(&dir).unwrap();
        let fake = dir.join(if cfg!(windows) {
            "fake-tool.exe"
        } else {
            "fake-tool"
        });
        fs::write(&fake, b"ok").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&fake).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&fake, perms).unwrap();
        }

        let found = resolve_command(
            "definitely-not-on-path-mtrxai-gpu",
            &[fake.to_str().unwrap()],
        );
        assert_eq!(found.as_deref(), Some(fake.as_path()));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expand_systemroot_style_path() {
        #[cfg(windows)]
        {
            let path = expand_path_env(r"%SystemRoot%\System32\nvidia-smi.exe");
            let s = path.to_string_lossy();
            assert!(!s.contains("%SystemRoot%"));
            assert!(
                s.ends_with(r"System32\nvidia-smi.exe") || s.ends_with("System32/nvidia-smi.exe")
            );
        }
        #[cfg(not(windows))]
        {
            assert_eq!(
                expand_path_env("/usr/bin/nvidia-smi"),
                PathBuf::from("/usr/bin/nvidia-smi")
            );
        }
    }
}
