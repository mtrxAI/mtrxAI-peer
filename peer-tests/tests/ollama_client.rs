use peer::ollama_peer::compute_processor_status;

#[test]
fn cpu_only_when_vram_missing() {
    let (proc, cpu, gpu) = compute_processor_status(1_000_000, None);
    assert_eq!(proc, "cpu");
    assert_eq!(cpu, 100);
    assert_eq!(gpu, 0);
}

#[test]
fn gpu_only_when_fully_on_gpu() {
    let (proc, cpu, gpu) = compute_processor_status(1_000_000, Some(1_000_000));
    assert_eq!(proc, "gpu");
    assert_eq!(cpu, 0);
    assert_eq!(gpu, 100);
}

#[test]
fn mixed_split_when_partial_vram() {
    let (proc, cpu, gpu) = compute_processor_status(1_000_000, Some(500_000));
    assert_eq!(proc, "mixed");
    assert_eq!(cpu, 50);
    assert_eq!(gpu, 50);
}

#[test]
fn cpu_only_when_vram_zero() {
    let (proc, cpu, gpu) = compute_processor_status(1_000_000, Some(0));
    assert_eq!(proc, "cpu");
    assert_eq!(cpu, 100);
    assert_eq!(gpu, 0);
}
