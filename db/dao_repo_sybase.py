# db/dao_repo_sybase.py
from typing import Any, Dict, List, Optional
from db.dao_departamentos_grpvent import DepartamentosGRPVentDAO
from db.dao_bala_equipos import BalaEquiposDAO
from db.dao_bala_dptos import BalaDeptosDAO
from db.dao_articulos_balanza import ArticulosBalanzaDAO  

def _ci(d: dict, *names):
    ln = {k.lower(): k for k in d.keys()}
    for n in names:
        k = ln.get(str(n).lower())
        if k is not None:
            return d[k]
    return None

class RepoSybase:
    """
    Repo para la GUI:
      - Departamentos: DBA.GRP_VENT (CGRPCONTA/CGRPNOM)
      - Equipos: DBA.BALA_EQUIPOS
      - Relación: DBA.BALA_DPTOS (equipo_id, cgrpconta)
    Expone:
      - self.departamentos -> [{codigo, nombre}]
      - self.equipos -> [{nombre, ip, puerto, deptos:[...], autoreport:bool}]
      - add/update/delete_depto
      - add/update/delete_equipo (con deptos múltiples y autoreport)
    """
    def __init__(self, eleccion_dbf: str = "SYBASE"):
        self.dep = DepartamentosGRPVentDAO(eleccion_dbf)
        self.eq  = BalaEquiposDAO(eleccion_dbf)
        self.eqd = BalaDeptosDAO(eleccion_dbf)
        self.art = ArticulosBalanzaDAO(eleccion_dbf)
        # asegurar esquema necesario
        self.eq.ensure_schema()
        self.eqd.ensure_schema()
        self._refresh()

    def _refresh(self):
        # Departamentos normalizados (GRP_VENT puede venir en mayús/minus)
        d_rows = self.dep.listar()
        self.departamentos = [
            {"codigo": _ci(r, "CGRPCONTA", "cgrpconta", "codigo"),
             "nombre": _ci(r, "CGRPNOM",   "cgrpnom",   "nombre")}
            for r in d_rows
        ]

        # Equipos + relaciones
        e_rows = self.eq.listar()
        rel = self.eqd.listar_todo()
        rel_map: Dict[int, List[str]] = {}
        for rr in rel:
            eid = int(_ci(rr, "equipo_id"))
            rel_map.setdefault(eid, []).append(_ci(rr, "cgrpconta"))

        self._equipos_raw = e_rows
        self.equipos = []
        for e in e_rows:
            eid = int(_ci(e, "id"))
            self.equipos.append({
                "id": eid,
                "nombre": _ci(e, "nombre") or "",
                "ip": _ci(e, "ip") or "",
                "puerto": int(_ci(e, "puerto") or 1001),
                "deptos": sorted(rel_map.get(eid, [])),
                "autoreport": bool(int(_ci(e, "autoreport") or 0)),
            })

    # --- Departamentos (passthrough a GRP_VENT) ---
    def add_depto(self, codigo: str, nombre: str):
        self.dep.insertar(codigo, nombre)
        self._refresh()

    def update_depto(self, codigo: str, nombre: str):
        self.dep.actualizar(codigo, nombre)
        self._refresh()

    def delete_depto(self, codigo: str):
        # Bloquea si un equipo lo usa
        if any(codigo in (e.get("deptos") or []) for e in self.equipos):
            raise ValueError("No se puede borrar: hay equipos asociados a este depto.")
        self.dep.eliminar(codigo)
        self._refresh()

    # --- Equipos con múltiples deptos y autoreport ---

    def add_equipo(self, nombre: str, ip: str, puerto: int,
                   depto_codigos: Optional[List[str]] = None,
                   autoreport: bool = False):
        self.eq.insertar(nombre, ip, int(puerto), bool(autoreport))
        eid = self.eq.get_id_por_nombre_ip(nombre, ip)
        if eid is not None:
            invalidos = self.eqd.reemplazar_relaciones(eid, depto_codigos or [])
            if invalidos:
                # si querés que no sea bloqueante, podés solo loguear
                raise ValueError(f"Departamentos inexistentes (CGRPCONTA): {', '.join(invalidos)}")
        self._refresh()

    def update_equipo(self, idx: int, nombre: str, ip: str, puerto: int,
                      depto_codigos: Optional[List[str]] = None,
                      autoreport: bool = False):
        lista = self.eq.listar()
        if not (0 <= idx < len(lista)):
            raise IndexError("Índice fuera de rango")
        eid = int(_ci(lista[idx], "id"))
        self.eq.actualizar(eid, nombre, ip, int(puerto), bool(autoreport))
        invalidos = self.eqd.reemplazar_relaciones(eid, depto_codigos or [])
        if invalidos:
            raise ValueError(f"Departamentos inexistentes (CGRPCONTA): {', '.join(invalidos)}")
        self._refresh()
        
    def articulos_por_deptos(self, depto_codigos: List[str]):
        """Artículos formateados para balanza (crudos de DB)."""
        return self.art.listar_para_deptos(depto_codigos)


    def delete_equipo(self, idx: int):
        lista = self.eq.listar()
        if not (0 <= idx < len(lista)):
            raise IndexError("Índice fuera de rango")
        eid = int(_ci(lista[idx], "id"))
        # relaciones se borran por ON DELETE CASCADE
        self.eq.eliminar(eid)
        self._refresh()
