fn main() {
    // libduckdb-sys (bundled) references the Windows Restart Manager API
    // (RmStartSession/RmEndSession/RmRegisterResources/RmGetList in duckdb::AdditionalLockInfo,
    // used to report which process holds a DB-file lock) but does NOT emit the link directive on
    // MSVC. Without this, the link fails with 4 unresolved externals. (Phase 0 G0a finding, 2026-06-14.)
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rustc-link-lib=dylib=rstrtmgr");
    }
    tauri_build::build()
}
