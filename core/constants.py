"""CopyBot core constants and defaults."""

SUPPORTED_CHAINS = ("bsc",)
SUPPORTED_PLATFORMS = ("fourmeme", "flap")

DEFAULT_CHAIN = "bsc"
DEFAULT_RPC_URL = "https://bsc-dataseed1.bnbchain.org"
DEFAULT_WS_URL  = "wss://bsc.drpc.org"
DEFAULT_CHAIN_ID = 56

# Platform contract addresses on BSC.
PLATFORM_CONTRACTS: dict[str, str] = {
    "fourmeme": "0x5c952063c7fc8610FFDB798152D69F0B9550762b",  # Four.meme TokenManager
    "flap":     "0xe2ce6ab80874fa9fa2aae65d277dd6b8e65c9de0",  # Flap Portal (proxy)
}

# Graduation thresholds (BNB) after which the token moves to PancakeSwap
GRADUATION_THRESHOLDS: dict[str, float] = {
    "fourmeme": 24.0,
    "flap": 16.0,
}

# PancakeSwap V2 router (used for panic-sell after graduation)
PANCAKESWAP_ROUTER  = "0x10ED43C718714eb63d5aA57B78B54704E256024E"
PANCAKESWAP_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
WBNB_ADDRESS        = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"

# Quote tokens tried when looking up PancakeSwap pairs (in priority order)
PANCAKESWAP_QUOTE_TOKENS: list[tuple[str, str]] = [
    ("bnb",  "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"),  # WBNB
    ("usdt", "0x55d398326f99059fF775485246999027B3197955"),  # USDT (BSC-USD)
    ("usdc", "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"),  # USDC
]

# Gas
DEFAULT_SLIPPAGE = 3          # percent
DEFAULT_GAS_MULTIPLIER = 1.1  # applied to network gas price

# Task statuses
TASK_STATUS_PENDING = "pending"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_PAUSED = "paused"
TASK_STATUS_INTERRUPTED = "interrupted"

# Listener block polling interval (seconds)
LISTENER_POLL_INTERVAL = 1

# Strategy engine price-check interval (seconds)
STRATEGY_CHECK_INTERVAL = 10
