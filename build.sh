cargo build --release --target x86_64-unknown-linux-musl
zip -j lambda.zip ./target/x86_64-unknown-linux-musl/release/bootstrap
