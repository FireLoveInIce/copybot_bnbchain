# 🤖 CopyBot: High-Performance On-Chain Copy-Trading Bot

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

CopyBot is a high-performance, **100% locally-run** Web3 copy-trading bot. By directly parsing low-level blockchain logs and events, it achieves millisecond-level trade execution to shadow "Smart Money" wallets on-chain. 

It natively supports both Bonding Curve platforms and Decentralized Exchanges (DEXs), featuring enterprise-grade Anti-MEV protection and an Adaptive Tax mechanism.

## 🛡️ Privacy & Security First (Zero-Trust Architecture)

Unlike cloud-based Telegram bots that hold your funds, CopyBot is designed for absolute self-custody and privacy:
- **Local Execution**: The bot runs entirely on your own machine or private VPS.
- **Local Key Management**: Private keys are generated, encrypted, and stored strictly locally on your SQLite database. Zero cloud syncing, zero third-party API tracking, and absolute control over your assets.
- **Local Management Dashboard**: Manage your copy-trading tasks, configure target wallets, and monitor your PnL through a secure, self-hosted local web interface. Your data never leaves your device.

## ✨ Core Features

- **Multi-Platform Smart Routing**: Natively supports PancakeSwap V2 & V3, Flap, Four.meme, and more.
- **Auto-Liquidity Detection**: Automatically detects when a bonding curve token has "graduated" to a DEX and dynamically switches the underlying execution route.
- **Anti-MEV Protection**: Dynamically applies ultra-tight slippage tolerances (e.g., 2%) based on the token's current liquidity stage, preventing high-slippage sandwich attacks.
- **Adaptive Tax Mechanism**: Automatically queries on-chain smart contracts to detect Tax Tokens. It calculates the exact safe sell amount, completely eliminating `Insufficient output amount / Revert` errors caused by attempting a 100% balance sell-off on taxed tokens.
- **Ghost Pool Prevention**: Probes PancakeSwap V3 liquidity using the Quoter contract with real test amounts to bypass fake or empty "ghost" pools.
- **Hidden Transaction Parsing**: Bypasses unreliable `Transfer` logs by directly extracting foundational `Swap` events. This allows it to flawlessly parse trades even when the target's address is obfuscated by DEX aggregators or private TG bot routers.

## ⚠️ Disclaimer (PLEASE READ CAREFULLY)

**This project is provided for educational, research, and technical exploration purposes ONLY.**

- **Not Financial Advice**: Nothing in this repository constitutes financial, investment, or trading advice.
- **Extreme Risk**: Cryptocurrency trading, especially involving memecoins and newly launched tokens, involves extreme risks including rug pulls, honeypots, dynamic high-tax tokens, and MEV attacks. You can easily lose 100% of your funds.
- **No Liability**: By compiling, installing, or running this software, you assume all risks and responsibilities for any profits or losses. The developers, contributors, and maintainers are **NOT** responsible for any financial losses, software bugs, network failures, or missed trades.
- **Test First**: Please ensure you fully understand the code logic and test thoroughly on a Testnet with disposable wallets before deploying on the Mainnet with real funds.

## ⚙️ Quick Start

### 1. Installation & Configuration

Clone the repository to your local machine

pip install -r requirements.txt

python main.py
