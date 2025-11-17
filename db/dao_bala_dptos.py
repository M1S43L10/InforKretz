# db/dao_bala_dptos.py
from typing import Any, Dict, List, Set
from db.data_access import DBAService
from utils.sql_safe import sql_quote

class BalaDeptosDAO:
    """
    Relación muchos-a-muchos equipo↔departamentos (GRP_VENT).
    Tabla: DBA.BALA_DPTOS
      - equipo_id  INTEGER NOT NULL -> FK DBA.BALA_EQUIPOS(id) ON DELETE CASCADE
      - cgrpconta  VARCHAR(11) NOT NULL -> FK DBA.GRP_VENT(CGRPCONTA)
      PK (equipo_id, cgrpconta)
    """
    def __init__(self, eleccion_dbf: str = "SYBASE"):
        self.db = DBAService(eleccion_dbf)

    # ---------- schema ----------
    def _tabla_existe(self, table: str) -> bool:
        sql = (
            "SELECT COUNT(*) AS n "
            "FROM sys.systable "
            f"WHERE table_name = '{sql_quote(table)}' AND user_name(creator) = 'DBA'"
        )
        rows = self.db.query(sql)
        return bool(rows and int(rows[0].get("n", 0)) > 0)

    def _indice_existe(self, index_name: str) -> bool:
        sql = (
            "SELECT COUNT(*) AS n "
            "FROM sys.sysindex "
            f"WHERE index_name = '{sql_quote(index_name)}'"
        )
        rows = self.db.query(sql)
        return bool(rows and int(rows[0].get("n", 0)) > 0)

    def ensure_schema(self) -> None:
        if not self._tabla_existe("BALA_DPTOS"):
            self.db.execute(
                "CREATE TABLE DBA.BALA_DPTOS ("
                " equipo_id INTEGER NOT NULL,"
                " cgrpconta VARCHAR(11) NOT NULL,"
                " PRIMARY KEY (equipo_id, cgrpconta),"
                " FOREIGN KEY (equipo_id) REFERENCES DBA.BALA_EQUIPOS(id) ON DELETE CASCADE,"
                " FOREIGN KEY (cgrpconta) REFERENCES DBA.GRP_VENT(CGRPCONTA))"
            )
        # índice útil para buscar por depto
        if not self._indice_existe("BALA_DPTOS_cgrp_idx"):
            self.db.execute("CREATE INDEX BALA_DPTOS_cgrp_idx ON DBA.BALA_DPTOS(cgrpconta)")

    # ---------- helpers ----------
    def _ci(self, d: dict, *names):
        ln = {k.lower(): k for k in d.keys()}
        for n in names:
            k = ln.get(str(n).lower())
            if k is not None:
                return d[k]
        return None

    def codigos_validos(self, codigos: List[str]) -> Set[str]:
        """Devuelve el subconjunto de codigos que sí existen en DBA.GRP_VENT(CGRPCONTA)."""
        if not codigos:
            return set()
        in_list = ",".join(f"'{sql_quote(str(c))}'" for c in codigos)
        rows = self.db.query(
            f"SELECT CGRPCONTA FROM DBA.GRP_VENT WHERE CGRPCONTA IN ({in_list})"
        )
        return {self._ci(r, "CGRPCONTA", "cgrpconta") for r in rows}

    # ---------- CRUD relación ----------
    def listar_por_equipo(self, equipo_id: int) -> List[Dict[str, Any]]:
        sql = (
            "SELECT equipo_id, cgrpconta "
            f"FROM DBA.BALA_DPTOS WHERE equipo_id = {int(equipo_id)} ORDER BY cgrpconta"
        )
        return self.db.query(sql)

    def listar_todo(self) -> List[Dict[str, Any]]:
        return self.db.query("SELECT equipo_id, cgrpconta FROM DBA.BALA_DPTOS")

    def listar_por_equipo_con_nombre(self, equipo_id: int) -> List[Dict[str, Any]]:
        """Útil si querés los nombres junto con el código (join a GRP_VENT)."""
        sql = (
            "SELECT d.equipo_id, d.cgrpconta, g.CGRPNOM AS nombre "
            "FROM DBA.BALA_DPTOS d "
            "JOIN DBA.GRP_VENT g ON g.CGRPCONTA = d.cgrpconta "
            f"WHERE d.equipo_id = {int(equipo_id)} "
            "ORDER BY d.cgrpconta"
        )
        return self.db.query(sql)

    def reemplazar_relaciones(self, equipo_id: int, codigos: List[str]) -> List[str]:
        """
        Borra las relaciones actuales y crea las nuevas.
        Devuelve la lista de códigos inválidos (que no existen en GRP_VENT).
        """
        # limpiar todo lo previo
        self.db.execute(f"DELETE FROM DBA.BALA_DPTOS WHERE equipo_id = {int(equipo_id)}")

        # validar contra GRP_VENT
        validos = self.codigos_validos(codigos)
        invalidos = [c for c in (codigos or []) if c not in validos]

        for c in validos:
            self.db.execute(
                "INSERT INTO DBA.BALA_DPTOS (equipo_id, cgrpconta) "
                f"VALUES ({int(equipo_id)}, '{sql_quote(str(c))}')"
            )
        return invalidos
