use chrono::Utc;
use mtrxai_attestation::{
    hash_file, sign_claims, signing_key_from_seed_hex, AttestationChallenge, AttestationClaims,
    AttestationProof,
};
use uuid::Uuid;

pub fn attestation_skip_enabled() -> bool {
    std::env::var("MTRXAI_ATTESTATION_SKIP")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub fn attestation_key_embedded() -> bool {
    !embedded_attestation_secret().trim().is_empty()
}

pub fn log_startup_diagnostics() {
    let skip = attestation_skip_enabled();
    let embedded = attestation_key_embedded();
    let build_id = embedded_build_id();
    println!(" Attestation: skip={skip} embedded_key={embedded} build_id={build_id}");
    if !skip && !embedded {
        eprintln!(
            " ⚠️ This binary cannot attest — use an official release build (e.g. docker.io/cosmicentropy/mtrxai-client:<version>)"
        );
    }
    if !skip && embedded {
        eprintln!(
            " ℹ️ If registration fails with 'build_id not on allowlist', run:\n    MTRXAI_ADMIN_KEY=... MTRXAI_LOBBY_URL=https://<your-lobby-admin> scripts/register-allowed-build.sh release/docker/allowed_build.json\n    (build_id={build_id})"
        );
    }
    if skip {
        eprintln!(
            " ⚠️ MTRXAI_ATTESTATION_SKIP=1 — registration will fail if the lobby requires attestation"
        );
    }
}

fn embedded_build_id() -> Uuid {
    Uuid::parse_str(env!("MTRXAI_BUILD_ID")).unwrap_or_else(|_| Uuid::nil())
}

fn embedded_git_sha() -> &'static str {
    env!("MTRXAI_GIT_SHA")
}

fn embedded_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

fn embedded_attestation_secret() -> &'static str {
    env!("MTRXAI_ATTESTATION_SECRET")
}

fn platform_string() -> String {
    format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH)
}

/// Path of the bytes we attest.
///
/// Desktop / container: the running executable.
/// Android: `libmtrxai_tauri.so` (allowlist hashes that `.so`, not `app_process`).
pub fn hash_current_binary() -> anyhow::Result<String> {
    let path = attested_binary_path()?;
    hash_file(&path).map_err(|e| anyhow::anyhow!(e))
}

fn attested_binary_path() -> anyhow::Result<std::path::PathBuf> {
    #[cfg(target_os = "android")]
    {
        android_native_lib_path()
    }
    #[cfg(not(target_os = "android"))]
    {
        Ok(std::env::current_exe()?)
    }
}

#[cfg(target_os = "android")]
fn android_native_lib_path() -> anyhow::Result<std::path::PathBuf> {
    const LIB_NAME: &str = "libmtrxai_tauri.so";

    // Prefer /proc/self/maps: points at the extracted jni lib when legacy packaging is on.
    if let Ok(maps) = std::fs::read_to_string("/proc/self/maps") {
        for line in maps.lines() {
            if !line.contains(LIB_NAME) {
                continue;
            }
            let Some(path) = line.split_whitespace().last() else {
                continue;
            };
            if path.starts_with('/') && !path.contains('!') && std::path::Path::new(path).is_file()
            {
                return Ok(std::path::PathBuf::from(path));
            }
        }
    }

    // Fallback: dladdr on a symbol in this cdylib.
    use std::os::raw::{c_char, c_void};

    #[repr(C)]
    struct DlInfo {
        dli_fname: *const c_char,
        dli_fbase: *mut c_void,
        dli_sname: *const c_char,
        dli_saddr: *mut c_void,
    }
    extern "C" {
        fn dladdr(addr: *const c_void, info: *mut DlInfo) -> i32;
    }

    let mut info = DlInfo {
        dli_fname: std::ptr::null(),
        dli_fbase: std::ptr::null_mut(),
        dli_sname: std::ptr::null(),
        dli_saddr: std::ptr::null_mut(),
    };
    let addr = android_native_lib_path as *const c_void;
    let rc = unsafe { dladdr(addr, &mut info) };
    if rc == 0 || info.dli_fname.is_null() {
        anyhow::bail!("could not locate {LIB_NAME} for attestation hashing");
    }
    let cstr = unsafe { std::ffi::CStr::from_ptr(info.dli_fname) };
    let path = std::path::PathBuf::from(cstr.to_string_lossy().as_ref());
    if !path.is_file() || path.to_string_lossy().contains('!') {
        anyhow::bail!(
            "attestation lib path is not a regular file ({}); enable jniLibs.useLegacyPackaging",
            path.display()
        );
    }
    if !path
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n == LIB_NAME)
    {
        anyhow::bail!(
            "dladdr returned {} (expected {LIB_NAME}); enable jniLibs.useLegacyPackaging",
            path.display()
        );
    }
    Ok(path)
}

pub async fn fetch_challenge(
    http_client: &reqwest::Client,
    lobby_host: &str,
) -> anyhow::Result<AttestationChallenge> {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/attestation/challenge");
    let resp = http_client.get(&url).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("Attestation challenge failed ({}): {}", status, text);
    }
    Ok(resp.json().await?)
}

pub fn build_proof(challenge: &AttestationChallenge) -> anyhow::Result<AttestationProof> {
    let secret = embedded_attestation_secret();
    if secret.trim().is_empty() {
        anyhow::bail!(
            "MTRXAI_ATTESTATION_SECRET not embedded; set MTRXAI_ATTESTATION_SKIP=1 for local dev"
        );
    }

    let signing_key = signing_key_from_seed_hex(secret)
        .map_err(|e| anyhow::anyhow!("invalid embedded attestation key: {e}"))?;
    let binary_sha256 = hash_current_binary()?;
    let claims = AttestationClaims {
        challenge_id: challenge.challenge_id,
        nonce: challenge.nonce.clone(),
        binary_sha256,
        build_id: embedded_build_id(),
        version: embedded_version().to_string(),
        git_sha: embedded_git_sha().to_string(),
        platform: platform_string(),
        issued_at: Utc::now().timestamp(),
    };
    let signature = sign_claims(&claims, &signing_key)
        .map_err(|e| anyhow::anyhow!("failed to sign attestation claims: {e}"))?;

    Ok(AttestationProof {
        challenge_id: claims.challenge_id,
        nonce: claims.nonce,
        binary_sha256: claims.binary_sha256,
        build_id: claims.build_id,
        version: claims.version,
        git_sha: claims.git_sha,
        platform: claims.platform,
        issued_at: claims.issued_at,
        signature,
    })
}

pub async fn maybe_build_attestation_proof(
    http_client: &reqwest::Client,
    lobby_host: &str,
) -> anyhow::Result<Option<AttestationProof>> {
    if attestation_skip_enabled() {
        eprintln!("ℹ️ MTRXAI_ATTESTATION_SKIP=1 — registering without attestation proof");
        return Ok(None);
    }
    if embedded_attestation_secret().trim().is_empty() {
        anyhow::bail!(
            "this client build has no embedded attestation key; use an official attested release image or set MTRXAI_ATTESTATION_SKIP=1 only when the lobby also skips attestation"
        );
    }
    let challenge = fetch_challenge(http_client, lobby_host).await?;
    let proof = build_proof(&challenge)?;
    Ok(Some(proof))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_string_is_non_empty() {
        assert!(!platform_string().is_empty());
    }

    #[test]
    fn hash_current_binary_succeeds() {
        hash_current_binary().expect("hash current test binary");
    }
}
