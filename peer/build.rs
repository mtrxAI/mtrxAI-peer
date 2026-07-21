fn main() {
    let git_sha = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
            } else {
                None
            }
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    let build_id =
        std::env::var("MTRXAI_BUILD_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());
    let attestation_secret = std::env::var("MTRXAI_ATTESTATION_SECRET").unwrap_or_default();

    println!("cargo:rustc-env=MTRXAI_GIT_SHA={git_sha}");
    println!("cargo:rustc-env=MTRXAI_BUILD_ID={build_id}");
    println!("cargo:rustc-env=MTRXAI_ATTESTATION_SECRET={attestation_secret}");
    println!("cargo:rerun-if-env-changed=MTRXAI_BUILD_ID");
    println!("cargo:rerun-if-env-changed=MTRXAI_ATTESTATION_SECRET");
    println!("cargo:rerun-if-changed=build.rs");
}
