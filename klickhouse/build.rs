use rustc_version::{Version, version};

fn main() {
    if version().unwrap() >= Version::parse("1.51.0").unwrap() {
        println!("cargo:rustc-cfg=const_generics");
    }
}
