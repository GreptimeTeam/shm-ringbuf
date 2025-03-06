fn main() {
    tonic_build::configure()
        .type_attribute(
            ".",
            "#[derive(::serde::Serialize, ::serde::Deserialize)]",
        )
        .compile_protos(&["proto/shm.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("{e:?}"));
}
