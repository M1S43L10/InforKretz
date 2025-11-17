# db/data_access.py
from typing import Any, Iterable, List, Dict, Optional
from config.config_file_conexion import Conexion_DBA

class DBAService:
    """Acceso a datos genérico para Sybase ASA9 (pypyodbc + DSN de tus DBF)."""
    def __init__(self, eleccion_dbf: str = "SYBASE"):
        self._conn = Conexion_DBA(eleccion_dbf)

    def _rows_to_dicts(self, cursor, rows) -> List[Dict[str, Any]]:
        cols = [d[0] for d in cursor.description] if cursor.description else []
        return [{c: v for c, v in zip(cols, r)} for r in rows]

    def query(self, sql: str, params: Optional[Iterable[Any]] = None) -> List[Dict[str, Any]]:
        try:
            self._conn.conectar()
            cur = self._conn.conexion.cursor()
            cur.execute(sql, params or [])
            data = self._rows_to_dicts(cur, cur.fetchall())
            cur.close()
            print(data)
            return data
        except Exception as e:
            print(f"[DBAService.query] {e} | SQL: {sql} | Params: {params}")
            return []
        finally:
            self._conn.desconectar()

    def execute(self, sql: str, params: Optional[Iterable[Any]] = None) -> int:
        try:
            self._conn.conectar()
            cur = self._conn.conexion.cursor()
            cur.execute(sql, params or [])
            self._conn.conexion.commit()
            rc = cur.rowcount
            cur.close()
            return rc
        except Exception as e:
            try: self._conn.conexion.rollback()
            except Exception: pass
            print(f"[DBAService.execute] {e} | SQL: {sql} | Params: {params}")
            return 0
        finally:
            self._conn.desconectar()
