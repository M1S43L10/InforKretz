from typing import Any, Dict, List, Optional
from db.data_access import DBAService
from utils.sql_safe import sql_quote

class BalaEquiposDAO:
    """
    Tabla: DBA.BALA_EQUIPOS
      - id          INTEGER AUTOINCREMENT PK
      - nombre      VARCHAR(80) NOT NULL UNIQUE
      - ip          VARCHAR(45) NOT NULL
      - puerto      INTEGER NOT NULL
      - autoreport  SMALLINT NOT NULL DEFAULT 0  (0/1)
      - creado_en   TIMESTAMP DEFAULT CURRENT TIMESTAMP
      - actualizado_en TIMESTAMP DEFAULT CURRENT TIMESTAMP
    """
    def __init__(self, eleccion_dbf: str = "SYBASE"):
        self.db = DBAService(eleccion_dbf)

    @staticmethod
    def _q(val):
        if val is None: return "NULL"
        if isinstance(val, (int, float)): return str(val)
        return f"'{sql_quote(str(val))}'"

    def ensure_schema(self) -> None:
        # ¿existe la tabla?
        rows = self.db.query(
            "SELECT COUNT(*) AS n "
            "FROM sys.systable "
            "WHERE table_name = 'BALA_DPTOS'"
        )
        if not (rows and int(rows[0].get("n", 0)) > 0):
            self.db.execute(
                "CREATE TABLE DBA.BALA_DPTOS ("
                " equipo_id INTEGER NOT NULL,"
                " cgrpconta VARCHAR(11) NOT NULL,"
                " PRIMARY KEY (equipo_id, cgrpconta),"
                " FOREIGN KEY (equipo_id) REFERENCES DBA.BALA_EQUIPOS(id) ON DELETE CASCADE,"
                " FOREIGN KEY (cgrpconta) REFERENCES DBA.GRP_VENT(CGRPCONTA))"
            )

        # índice por cgrpconta (si no existiera)
        rows = self.db.query(
            "SELECT COUNT(*) AS n FROM sys.sysindex WHERE index_name = 'BALA_DPTOS_cgrp_idx'"
        )
        if not (rows and int(rows[0].get("n", 0)) > 0):
            self.db.execute("CREATE INDEX BALA_DPTOS_cgrp_idx ON DBA.BALA_DPTOS(cgrpconta)")




    def listar(self, limite: int = 500) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT TOP {int(limite)}
                   id, nombre, ip, puerto, autoreport
            FROM DBA.BALA_EQUIPOS
            ORDER BY id
        """
        return self.db.query(sql)

    def get_id_por_nombre_ip(self, nombre: str, ip: str) -> Optional[int]:
        sql = "SELECT id FROM DBA.BALA_EQUIPOS WHERE nombre = ? AND ip = ? ORDER BY id DESC"
        rows = self.db.query(sql, params=[nombre, ip])
        return int(rows[0]["id"]) if rows else None

    def insertar(self, nombre: str, ip: str, puerto: int, autoreport: bool = False) -> int:
        sql = (
            "INSERT INTO DBA.BALA_EQUIPOS (nombre, ip, puerto, autoreport) VALUES ("
            f"{self._q(nombre)}, {self._q(ip)}, {self._q(int(puerto))}, {1 if autoreport else 0})"
        )
        return self.db.execute(sql)

    def actualizar(self, id_equipo: int, nombre: str, ip: str, puerto: int, autoreport: bool) -> int:
        sql = (
            "UPDATE DBA.BALA_EQUIPOS SET "
            f" nombre = {self._q(nombre)},"
            f" ip = {self._q(ip)},"
            f" puerto = {self._q(int(puerto))},"
            f" autoreport = {1 if autoreport else 0},"
            " actualizado_en = CURRENT TIMESTAMP"
            f" WHERE id = {int(id_equipo)}"
        )
        return self.db.execute(sql)

    def eliminar(self, id_equipo: int) -> int:
        return self.db.execute(f"DELETE FROM DBA.BALA_EQUIPOS WHERE id = {int(id_equipo)}")
