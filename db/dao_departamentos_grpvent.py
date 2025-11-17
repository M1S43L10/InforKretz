# db/dao_departamentos_grpvent.py
from typing import Any, Dict, List, Optional
from db.data_access import DBAService
from utils.sql_safe import sql_quote

class DepartamentosGRPVentDAO:
    """
    Tabla: DBA.GRP_VENT
      - CGRPCONTA (varchar(11))  PK/ID del dpto
      - CGRPNOM   (varchar(25))  Nombre del dpto
      - CCODFAM   (varchar(5))   (opcional)
      - uid       (varchar(20))  (opcional)
      - dFechaU   (datetime)     (opcional - actualizar en write)
      - sp        (varchar(1))   (opcional)
      - nDGR_P, nDGR_M_P, nDGR_R, nIVA_P (float) (opcionales)
    """

    def __init__(self, eleccion_dbf: str = "SYBASE"):
        self.db = DBAService(eleccion_dbf)

    # Helpers
    @staticmethod
    def _q(val):
        if val is None: return "NULL"
        if isinstance(val, (int, float)): return str(val)
        return f"'{sql_quote(str(val))}'"

    def listar(self, limite: int = 1000):
        sql = f"""
            SELECT TOP {int(limite)}
                CGRPCONTA AS codigo,
                CGRPNOM   AS nombre,
                CCODFAM   AS fam,
                uid, dFechaU, sp,
                nDGR_P, nDGR_M_P, nDGR_R, nIVA_P
            FROM DBA.GRP_VENT
            ORDER BY CGRPCONTA
        """
        return self.db.query(sql)

    def buscar_por_codigo(self, cgrpconta: str):
        sql = f"""
            SELECT CGRPCONTA AS codigo,
                CGRPNOM   AS nombre,
                CCODFAM   AS fam,
                uid, dFechaU, sp,
                nDGR_P, nDGR_M_P, nDGR_R, nIVA_P
            FROM DBA.GRP_VENT
            WHERE CGRPCONTA = {self._q(cgrpconta)}
        """
        rows = self.db.query(sql)
        return rows[0] if rows else None


    # WRITES (mínimos para ABM: solo code+name; resto opcionales)
    def insertar(self, cgrpconta: str, cgrpnom: str, ccodfam: Optional[str] = None) -> int:
        sql = f"""
            INSERT INTO DBA.GRP_VENT
              (CGRPCONTA, CGRPNOM, CCODFAM, dFechaU)
            VALUES
              ({self._q(cgrpconta)}, {self._q(cgrpnom)}, {self._q(ccodfam)}, CURRENT TIMESTAMP)
        """
        return self.db.execute(sql)

    def actualizar(self, cgrpconta: str, cgrpnom: str, ccodfam: Optional[str] = None) -> int:
        sql = f"""
            UPDATE DBA.GRP_VENT
               SET CGRPNOM = {self._q(cgrpnom)},
                   CCODFAM = {self._q(ccodfam)},
                   dFechaU = CURRENT TIMESTAMP
             WHERE CGRPCONTA = {self._q(cgrpconta)}
        """
        return self.db.execute(sql)

    def eliminar(self, cgrpconta: str) -> int:
        sql = f"DELETE FROM DBA.GRP_VENT WHERE CGRPCONTA = {self._q(cgrpconta)}"
        return self.db.execute(sql)
