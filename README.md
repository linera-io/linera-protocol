<div align="center">
  <a href="https://linera.io">
    <img src="https://github.com/linera-io/linera-protocol/assets/1105398/fe08c941-93af-4114-bb83-bcc0eaec95f9" alt="Linera Protocol Logo" width="300">
  </a>
</div>
<div align="center">

[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE)
[![Build Status for Rust](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml)
[![Build Status for Documentation](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml)
[![Build Status for DynamoDB](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml)
[![Website](https://img.shields.io/badge/Website-Linera.io-blue?style=flat&logo=google-chrome)](https://linera.io)
[![Telegram](https://img.shields.io/badge/Telegram-26A5E4?logo=telegram&logoColor=white)](https://t.me/linera_io)
[![Discord](https://img.shields.io/badge/Discord-5865F2?logo=discord&logoColor=white)](https://discord.gg/linera)
[![Twitter](https://img.shields.io/twitter/follow/linera_io?style=social)](https://x.com/linera_io)

</div>


# 🌐 **Linera Protocol**

**Decentralized blockchain infrastructure for highly scalable, low-latency Web3 applications.**  

📖 **[Developer Page](https://linera.dev)** | 📄 **[Whitepaper](https://linera.io/whitepaper)**  

---

## 📂 **Repository Structure**

### 🛠 Core Components
- [`linera-base`](https://linera-io.github.io/linera-protocol/linera_base/index.html) – Base definitions, including cryptography.
- [`linera-version`](https://linera-io.github.io/linera-protocol/linera_version/index.html) – Manages version info in binaries and services.
- [`linera-storage`](https://linera-io.github.io/linera-protocol/linera_storage/index.html) – Defines storage abstractions.
- [`linera-core`](https://linera-io.github.io/linera-protocol/linera_core/index.html) – Core protocol logic for synchronization and validation.
- [`linera-sdk`](https://linera-io.github.io/linera-protocol/linera_sdk/index.html) – Develop Linera applications in Rust for Wasm VM.

### 🔗 **Other Components**
- [`linera-rpc`](https://linera-io.github.io/linera-protocol/linera_rpc/index.html) – Handles RPC messages and schemas.
- [`linera-client`](https://linera-io.github.io/linera-protocol/linera_client/index.html) – Command-line and Web clients.
- [`linera-service`](https://linera-io.github.io/linera-protocol/linera_service/index.html) – Executable for client operations.
- [`examples`](./examples) – Example applications built on Linera.

---

## 🚀 **Quickstart with Linera CLI**

**1️⃣ Compile Linera and add it to `$PATH`:**  

```bash
cargo build -p linera-storage-service -p linera-service --bins --features storage-service
```

```bash
export PATH="$PWD/target/debug:$PATH"
```

**2️⃣ Start a local test network:**  

```bash
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```
 
```bash
linera net up
```

**3️⃣ Query validators and balances:**  
```bash
linera query-validators
``` 
```bash
CHAIN1="e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
``` 
```bash
CHAIN2="69705f85ac4c9fef6c02b4d83426aaaf05154c645ec1c61665f8e450f0468bc0"
``` 
```bash
linera query-balance "$CHAIN1"  
```

```bash
linera query-balance "$CHAIN2"
```

**4️⃣ Transfer assets:**  

```bash
linera transfer 10 --from "$CHAIN1" --to "$CHAIN2" 
```
```bash
linera transfer 5 --from "$CHAIN2" --to "$CHAIN1"
```

**5️⃣ Check balances again:** 

```bash
linera query-balance "$CHAIN1" 
```
```bash
linera query-balance "$CHAIN2"
```
For more examples, check the **[Developer Manual](https://linera.dev)** and **[Example Applications](./examples)**.

---

## 💬 **Join the Community**

<p align="left">
  <a href="https://t.me/linera_io">
    <img src="https://img.shields.io/badge/Telegram-26A5E4?logo=telegram&logoColor=white&style=for-the-badge" alt="Telegram">
  </a>
  <a href="https://discord.gg/linera">
    <img src="https://img.shields.io/badge/Discord-5865F2?logo=discord&logoColor=white&style=for-the-badge" alt="Discord">
  </a>
  <a href="https://x.com/linera_io">
    <img src="https://img.shields.io/badge/Twitter-000000?logo=x&logoColor=white&style=for-the-badge" alt="Twitter (X)">
  </a>
</p>
