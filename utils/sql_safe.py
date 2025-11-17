# ============================
# FILE: utils/sql_safe.py (mínimo)
# ============================
# Asegura comillas simples seguras para strings en SQL

def sql_quote(s: str) -> str:
    return (s or "").replace("'", "''")