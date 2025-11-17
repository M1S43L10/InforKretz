# db/dao_articulos_balanza.py
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple
from db.data_access import DBAService

class ArticulosBalanzaDAO:
    """
    Fuente: DBA.ARTICULO
    Filtro: CFORMATO = 'BALA' y CGRPCONTA ∈ deptos (comparación NUMÉRICA para evitar HY090).
    Devuelve columnas necesarias para armar los PLU (cmd 2005).
    """

    def __init__(self, eleccion_dbf: str = "SYBASE"):
        self.db = DBAService(eleccion_dbf)

    # ----------------- helpers internos -----------------
    @staticmethod
    def _only_digits(s: Any) -> str:
        return re.sub(r"\D", "", str(s or ""))

    @classmethod
    def _to_int(cls, s: Any) -> int | None:
        d = cls._only_digits(s)
        return int(d) if d else None

    @staticmethod
    def _placeholders(n: int) -> str:
        # "?, ?, ?" según n
        return ", ".join(["?"] * max(n, 1))
    
    @classmethod
    def _plu4_from_cref_barcode(cls, cref: Any, codebar: Any) -> str:
        # 1) ¿CREF exactamente 4 dígitos?
        cref_digits = cls._only_digits(cref)
        if len(cref_digits) == 4:
            return cref_digits

        # 2) Si no, intentamos con CCODEBAR: saltamos los 2 primeros y tomamos 4
        cb = cls._only_digits(codebar)
        # Necesitamos al menos 6 dígitos para poder tomar cb[2:6]
        if len(cb) >= 6:
            return cb[2:6]

        # 3) Sin PLU válido
        return ""


    # ----------------- API pública -----------------
    def listar_para_deptos(self, deptos: List[str | int]) -> List[Dict[str, Any]]:
        """
        deptos: puede venir como ["0005","0010",5,10] → se convierte a enteros [5,10].
        Usa CAST(CGRPCONTA AS INTEGER) para el filtro y así evitamos HY090.
        Devuelve: list[dict] con claves:
          CREF, CDETALLE, CCODFAM, CGRPCONTA, NPVP1, CCODEBAR, CVENCOM, CTPOIVA
        (Si tu columna de IVA tiene otro nombre, cambiá la SELECT)
        """
        nums = []
        for d in (deptos or []):
            n = self._to_int(d)
            if n is not None:
                nums.append(n)
        if not nums:
            return []

        if len(nums) == 1:
            sql = """
                SELECT CREF, CDETALLE, CCODFAM, CGRPCONTA, NPVP1, CCODEBAR, CVENCOM, CTIPOIVA
                FROM DBA.ARTICULO
                WHERE CFORMATO = 'BALA'
                  AND CAST(CGRPCONTA AS INTEGER) = ?
            """
            params: Tuple[Any, ...] = (nums[0],)
        else:
            ph = self._placeholders(len(nums))
            sql = f"""
                SELECT CREF, CDETALLE, CCODFAM, CGRPCONTA, NPVP1, CCODEBAR, CVENCOM, CTIPOIVA
                FROM DBA.ARTICULO
                WHERE CFORMATO = 'BALA'
                  AND CAST(CGRPCONTA AS INTEGER) IN ({ph})
            """
            params = tuple(nums)
        rows = self.db.query(sql, params=params)

         # Enriquecemos cada fila con PLU4 y PLU6 normalizados
        for r in rows:
            plu4 = self._plu4_from_cref_barcode(r.get("CREF"), r.get("CCODEBAR"))
            r["PLU4"] = plu4
            r["PLU6"] = plu4.rjust(6, "0") if plu4 else ""

        return rows
