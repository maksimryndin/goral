# Installation

You can install (or update) Goral with 

```sh
curl --proto '=https' --tlsv1.2 -sSf https://maksimryndin.github.io/goral/install.sh | sh
sudo mv goral /usr/local/bin/goral
```

<details>
  <summary>or by downloading a prebuilt binary from https://github.com/maksimryndin/goral/releases manually
</summary>

```sh
wget https://github.com/maksimryndin/goral/releases/download/0.1.3/goral-0.1.3-x86_64-unknown-linux-gnu.tar.gz
tar -xzf goral-0.1.3-x86_64-unknown-linux-gnu.tar.gz
cd goral-0.1.3-x86_64-unknown-linux-gnu/
shasum -a 256 -c sha256_checksum.txt 
sudo mv goral /usr/local/bin/goral
```
</details>

<details>
  <summary>from source</summary>

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
git clone --depth 1 --branch 0.1.3 https://github.com/maksimryndin/goral
cd goral
RUSTFLAGS='-C target-feature=+crt-static' cargo build --release --target <target triple>
```
</details>

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