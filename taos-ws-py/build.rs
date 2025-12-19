fn main() {
    shadow_rs::new().unwrap();

    let commit_id = std::process::Command::new("git")
        .args(["rev-parse", "--short=7", "HEAD"])
        .output()
        .map_or_else(
            |_| "ncid000".to_string(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_string(),
        );
    println!("cargo:rustc-env=GIT_COMMIT_ID={commit_id}");
}
