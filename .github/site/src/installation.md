# Installation

You can install Goral
1) by downloading a prebuilt binary from https://github.com/maksimryndin/goral/releases

For example, for Linux
```sh
wget https://github.com/maksimryndin/goral/releases/download/0.1.2/goral-0.1.2-x86_64-unknown-linux-gnu.tar.gz
tar -xzf goral-0.1.2-x86_64-unknown-linux-gnu.tar.gz
cd goral-0.1.2-x86_64-unknown-linux-gnu/
shasum -a 256 -c sha256_checksum.txt 
sudo mv goral /usr/local/bin/goral
```

or just use an installer which will download the latest stable release, check sha256 and unpack it at the current directory

```sh
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/maksimryndin/goral/0.1.3rc22/.github/site/install.sh | sh
sudo mv goral /usr/local/bin/goral
```

2) from source (you need [Rust](https://www.rust-lang.org/tools/install)) with a command
```sh
git clone --depth 1 --branch 0.1.2 https://github.com/maksimryndin/goral
cd goral
RUSTFLAGS='-C target-feature=+crt-static' cargo build --release --target <target triple>
```

To run a binary
```sh
goral -c config.toml --id myhost
```

where an example `config.toml` is

```toml
[general]
service_account_credentials_path = "/etc/goral_service_account.json"
messenger.url = "https://discord.com/api/webhooks/123/zzz"

[system]
spreadsheet_id = "123XYZ"
messenger.url = "https://discord.com/api/webhooks/123/zzz"
```

See also [Services](./services.md) and [Recommended deployment](./recommended-deployment.md).