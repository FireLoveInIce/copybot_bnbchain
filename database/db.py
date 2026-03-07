"""SQLite async database helpers."""

import aiosqlite

from core.constants import DEFAULT_RPC_URL

DB_PATH = "copybot.db"


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON;")

        # ------------------------------------------------------------------
        # listener_tasks migration: v1 (platform TEXT, copy_enabled) →
        #   v2 (platforms TEXT JSON array, UNIQUE target_address)
        # ------------------------------------------------------------------
        cur = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='listener_tasks'"
        )
        if await cur.fetchone():
            # Table exists — check if migration needed
            cur2 = await db.execute("PRAGMA table_info(listener_tasks)")
            cols = {row[1] for row in await cur2.fetchall()}
            if "platforms" not in cols:
                # Recreate with new schema, migrate existing data
                await db.execute(
                    """
                    CREATE TABLE listener_tasks_new (
                        id             INTEGER PRIMARY KEY AUTOINCREMENT,
                        target_address TEXT NOT NULL UNIQUE,
                        chain          TEXT NOT NULL DEFAULT 'bsc',
                        platforms      TEXT NOT NULL,
                        status         TEXT NOT NULL DEFAULT 'pending',
                        config         TEXT,
                        created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                await db.execute(
                    """
                    INSERT OR IGNORE INTO listener_tasks_new
                        (id, target_address, chain, platforms, status, config, created_at)
                    SELECT id, target_address, chain,
                           json_array(platform),
                           status, config, created_at
                    FROM listener_tasks
                    """
                )
                await db.execute("DROP TABLE listener_tasks")
                await db.execute(
                    "ALTER TABLE listener_tasks_new RENAME TO listener_tasks"
                )
                await db.commit()

        # ------------------------------------------------------------------
        # Tables
        # ------------------------------------------------------------------

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS wallets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                address     TEXT    NOT NULL UNIQUE,
                private_key TEXT    NOT NULL,
                mnemonic    TEXT,
                label       TEXT,
                chain       TEXT    NOT NULL DEFAULT 'bsc',
                target_bound TEXT,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS listener_tasks (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                target_address TEXT NOT NULL UNIQUE,
                chain          TEXT NOT NULL DEFAULT 'bsc',
                platforms      TEXT NOT NULL,
                label          TEXT NOT NULL DEFAULT '',
                status         TEXT NOT NULL DEFAULT 'pending',
                config         TEXT,
                created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Migration: add label column if it doesn't exist
        cur = await db.execute("PRAGMA table_info(listener_tasks)")
        lt_cols = {row[1] for row in await cur.fetchall()}
        if "label" not in lt_cols:
            await db.execute(
                "ALTER TABLE listener_tasks ADD COLUMN label TEXT NOT NULL DEFAULT ''"
            )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS copy_tasks (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                target_address TEXT  NOT NULL,
                wallet_id      INTEGER NOT NULL,
                buy_mode       TEXT  NOT NULL,
                buy_value      REAL  NOT NULL,
                sell_mode      TEXT  NOT NULL,
                slippage       INTEGER NOT NULL DEFAULT 3,
                gas_multiplier REAL  NOT NULL DEFAULT 1.1,
                status         TEXT  NOT NULL DEFAULT 'pending',
                config         TEXT,
                created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (wallet_id) REFERENCES wallets(id) ON DELETE CASCADE
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_tasks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_id   INTEGER NOT NULL,
                token       TEXT NOT NULL,
                take_profit REAL,
                stop_loss   REAL,
                status      TEXT NOT NULL DEFAULT 'pending',
                config      TEXT,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (wallet_id) REFERENCES wallets(id) ON DELETE CASCADE
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash          TEXT NOT NULL,
                target_address   TEXT,
                source_task_id   INTEGER,
                source_task_type TEXT,
                action           TEXT NOT NULL,
                token            TEXT NOT NULL,
                pair             TEXT,
                amount           REAL,
                amount_token     TEXT NOT NULL DEFAULT '0',
                price            REAL,
                status           TEXT NOT NULL DEFAULT 'detected',
                platform         TEXT,
                chain            TEXT NOT NULL DEFAULT 'bsc',
                extra            TEXT NOT NULL DEFAULT '{}',
                created_at       DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Migration: add amount_token / extra columns if they don't exist yet
        cur = await db.execute("PRAGMA table_info(transactions)")
        tx_cols = {row[1] for row in await cur.fetchall()}
        if "amount_token" not in tx_cols:
            await db.execute(
                "ALTER TABLE transactions ADD COLUMN amount_token TEXT NOT NULL DEFAULT '0'"
            )
        if "extra" not in tx_cols:
            await db.execute(
                "ALTER TABLE transactions ADD COLUMN extra TEXT NOT NULL DEFAULT '{}'"
            )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS rpc_configs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                chain      TEXT NOT NULL DEFAULT 'bsc',
                label      TEXT NOT NULL DEFAULT '',
                rpc_url    TEXT NOT NULL,
                ws_url     TEXT,
                chain_id   INTEGER NOT NULL DEFAULT 56,
                active     INTEGER NOT NULL DEFAULT 0,
                is_default INTEGER NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Migrations
        cur = await db.execute("PRAGMA table_info(rpc_configs)")
        rpc_cols = {row[1] for row in await cur.fetchall()}
        if "label" not in rpc_cols:
            await db.execute(
                "ALTER TABLE rpc_configs ADD COLUMN label TEXT NOT NULL DEFAULT ''"
            )
        if "is_default" not in rpc_cols:
            await db.execute(
                "ALTER TABLE rpc_configs ADD COLUMN is_default INTEGER NOT NULL DEFAULT 0"
            )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                level     TEXT NOT NULL,
                category  TEXT,
                message   TEXT NOT NULL,
                tx_hash   TEXT
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS token_cache (
                address    TEXT PRIMARY KEY,
                name       TEXT,
                symbol     TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Ensure built-in default RPC always exists
        cur = await db.execute(
            "SELECT COUNT(1) FROM rpc_configs WHERE is_default = 1"
        )
        if (await cur.fetchone())[0] == 0:
            # Check if anything is active
            cur2 = await db.execute(
                "SELECT COUNT(1) FROM rpc_configs WHERE active = 1"
            )
            has_active = (await cur2.fetchone())[0] > 0
            await db.execute(
                """
                INSERT INTO rpc_configs
                    (chain, label, rpc_url, ws_url, chain_id, active, is_default)
                VALUES ('bsc', 'Default (built-in)', ?, '', 56, ?, 1)
                """,
                (DEFAULT_RPC_URL, 0 if has_active else 1),
            )

        await db.commit()


async def fetch_all(query: str, params: tuple = ()) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        rows = await cur.fetchall()
        return [dict(row) for row in rows]


async def fetch_one(query: str, params: tuple = ()) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        row = await cur.fetchone()
        return dict(row) if row else None


async def execute(query: str, params: tuple = ()) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(query, params)
        await db.commit()
        return cur.lastrowid
