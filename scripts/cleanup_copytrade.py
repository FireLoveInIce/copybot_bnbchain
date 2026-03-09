"""Clean up copy trade records from the database.

Usage:
    python scripts/cleanup_copytrade.py              # delete all closed positions
    python scripts/cleanup_copytrade.py --all        # delete ALL positions (open + closed)
    python scripts/cleanup_copytrade.py --task 3     # delete positions for copy task #3 only
    python scripts/cleanup_copytrade.py --tx         # also delete copy trade transactions
"""

import argparse
import sqlite3
import sys
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "copybot.db")


def main():
    parser = argparse.ArgumentParser(description="Clean up copy trade records")
    parser.add_argument("--all", action="store_true", help="Delete ALL positions (open + closed)")
    parser.add_argument("--task", type=int, help="Only clean positions for this copy task ID")
    parser.add_argument("--tx", action="store_true", help="Also delete copy trade transactions")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting")
    args = parser.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Build WHERE clause
    where_parts = []
    params = []

    if not args.all:
        where_parts.append("status = 'closed'")

    if args.task:
        where_parts.append("copy_task_id = ?")
        params.append(args.task)

    where_clause = " AND ".join(where_parts) if where_parts else "1=1"

    # Count positions
    cur = conn.execute(f"SELECT COUNT(1) FROM copy_positions WHERE {where_clause}", params)
    pos_count = cur.fetchone()[0]

    # Count transactions
    tx_count = 0
    if args.tx:
        tx_where = "source_task_type = 'copy'"
        tx_params = []
        if args.task:
            tx_where += " AND source_task_id = ?"
            tx_params.append(args.task)
        cur = conn.execute(f"SELECT COUNT(1) FROM transactions WHERE {tx_where}", tx_params)
        tx_count = cur.fetchone()[0]

    scope = "all" if args.all else "closed"
    task_label = f" (task #{args.task})" if args.task else ""

    print(f"Found {pos_count} {scope} position(s){task_label}")
    if args.tx:
        print(f"Found {tx_count} copy trade transaction(s){task_label}")

    if pos_count == 0 and tx_count == 0:
        print("Nothing to clean up.")
        return

    if args.dry_run:
        print("[dry-run] No changes made.")
        return

    confirm = input(f"\nDelete {pos_count} positions{' + ' + str(tx_count) + ' transactions' if tx_count else ''}? [y/N] ")
    if confirm.lower() != "y":
        print("Cancelled.")
        return

    conn.execute(f"DELETE FROM copy_positions WHERE {where_clause}", params)

    if args.tx:
        tx_where = "source_task_type = 'copy'"
        tx_params = []
        if args.task:
            tx_where += " AND source_task_id = ?"
            tx_params.append(args.task)
        conn.execute(f"DELETE FROM transactions WHERE {tx_where}", tx_params)

    conn.commit()
    conn.close()

    print(f"Deleted {pos_count} position(s){' + ' + str(tx_count) + ' transaction(s)' if tx_count else ''}.")


if __name__ == "__main__":
    main()
