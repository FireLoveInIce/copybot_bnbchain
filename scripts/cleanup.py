"""Clear all detected transactions and logs. Run: .venv\Scripts\python.exe cleanup.py"""
import sqlite3
conn = sqlite3.connect("copybot.db")
t = conn.execute("DELETE FROM transactions").rowcount
l = conn.execute("DELETE FROM logs").rowcount
conn.commit()
conn.execute("VACUUM")
conn.close()
print(f"Deleted {t} transactions, {l} logs.")
