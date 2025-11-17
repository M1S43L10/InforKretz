"""
Módulo: envios_balanzas

Responsabilidad:
- Leer equipos desde RepoSybase (DBA.BALA_EQUIPOS) y sus deptos asociados
- Testear conexión (TCP/IP) a cada equipo Kretz (beep opcional) y conservar solo los OK
- Enviar departamentos (cmd 2003) a cada equipo OK, solo los deptos asociados a ese equipo
- Enviar artículos (cmd 2005) a cada equipo OK, filtrando por los deptos asociados al equipo

Notas de protocolo:
- El frame hacia la balanza es: STX(0x02) + cuerpo + CHK(2 bytes ASCII) + ETX(0x04)
  El "cuerpo" = TIPO('C') + ID('01') + COMANDO(4díg) + DATOS (según cmd)
- Checksum: suma algebraica de TODOS los bytes precedentes al checksum (incluye 0x02),
  tomar los 2 menos significativos (H y L) y sumar 0x30 a cada uno para convertirlos a ASCII.
- Test de comunicación: comando 0001 (con beep) o 0002 (sin beep).
- Alta/Mod de Departamento (2003): código 3 dígitos + nombre 16 chars (ambos ASCII, fixed len)
- Alta/Mod de PLU (2005): ver campos; este módulo incluye builder para los campos más comunes

Este módulo NO bloquea el hilo de la GUI; provee métodos síncronos y callbacks de progreso.
Podés envolverlos con threading en la GUI si querés barra de progreso.
"""
from __future__ import annotations

import socket
import time
from dataclasses import dataclass
from typing import Callable, Iterable, List, Dict, Optional
from GUI.kretz_driver import KretzTCP

# --- Importá tu repo real (ya existente en el proyecto) ---
try:
    from db.dao_repo_sybase import RepoSybase  # ya lo tenés en /db/
except Exception as _e:  # fallback para pruebas unitarias
    RepoSybase = None  # type: ignore

STX = 0x02
ETX = 0x04
RESP_STX = 0x07


import re
from math import isnan

def _only_digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def _norm3(val: str) -> str:
    # toma los últimos 3 dígitos; si faltan, left-pad con 0
    d = _only_digits(val or "")
    return d[-3:].rjust(3, "0") if d else "001"

def _norm6(val: str) -> str:
    d = _only_digits(val or "")
    return d[-6:].rjust(6, "0") if d else "000001"

def _to_int_cents(x) -> int:
    try:
        v = float(x)
        if v != v or isnan(v):  # NaN
            return 0
        return int(round(v * 100))
    except Exception:
        return 0

def _sql_in_placeholders(n: int) -> str:
    return ", ".join(["?"] * n) if n > 0 else "?"

def _upper_keys(row: dict) -> dict:
    return { (k.upper() if isinstance(k, str) else k): v for k, v in row.items() }

def _pick_iva(u: dict):
    # La columna puede venir como CTPOIVA / CTIPOIVA / CTPQIVA según tu origen
    return u.get("CTPOIVA") or u.get("CTIPOIVA") or u.get("CTPQIVA")

# --- reemplazá tu _fetch_articulos_balanza por esto ---
def _fetch_articulos_balanza(repo, deptos: list[str]) -> list[dict]:
    rows = repo.articulos_por_deptos(deptos)   # devuelve [{'cref':..., 'cdetalle':...}, ...]
    out = []
    for r in rows:
        u = _upper_keys(r)  # ahora u['CREF'], u['CDETALLE'], ...
        out.append({
            "CREF":       u.get("CREF"),
            "CDETALLE":   u.get("CDETALLE"),
            "CCODFAM":    u.get("CCODFAM"),
            "CGRPCONTA":  u.get("CGRPCONTA"),
            "NPVP1":      u.get("NPVP1"),
            "CCODEBAR":   u.get("CCODEBAR"),
            "CVENCOM":    u.get("CVENCOM"),
            "CTPOIVA":    _pick_iva(u),   # <- usa el que exista
        })
    return out


def _map_row_to_plu(r: dict) -> dict:
    """
    Mapea una fila de ARTICULO a los campos que consume cmd_plu().
    - nro_plu: de CREF (6 dígitos)
    - cod_depto: de CGRPCONTA (3 dígitos)
    - cod_familia: de CCODFAM (3 dígitos) (si no hay, '001')
    - nombre_plu: de CDETALLE (se recorta en el builder a 26)
    - precio: NPVP1 en centavos (int)
    - tipo: por ahora 'P' (peso). Si querés usar CVENCOM: 'P' si == '1' else 'U'
    - punto decimal: lo solemos fijar en 2 (centavos) si tu balanza trabaja con 2 decimales
    """
    return {
        "nro_plu": _norm6(r.get("CREF", "")),
        "cod_depto": _norm3(r.get("CGRPCONTA", "")),
        "cod_familia": _norm3(r.get("CCODFAM", "")),
        "nombre_plu": (r.get("CDETALLE") or "").strip(),
        "descripcion": "",                         # opcional
        "codigo_plu": "",                          # opcional (5 chars)
        "tipo": "P" if str(r.get("CVENCOM") or "1") in ("1", "P", "p") else "U",
        "valor_fijo": 0,
        "precio": _to_int_cents(r.get("NPVP1")),
        "precio_alt": 0,
        "precio_ant_o_puntodec": 2,               # 2 decimales
        "impuesto1": 0,                            # mapear si querés según CTPQIVA
        "impuesto2": 0,
        "tara_pre": 0,
        "tara_pub": 0,
        "cod_etiqueta": 1,
        "cod_receta": 0,
        "cod_nutri": 0,
        "fecha_envase_no": 0,
        "venc_dias": 0,
        "cod_imagen": 0,
    }


# =========================
# Utilidades de protocolo
# =========================

def _checksum_bytes(payload: bytes) -> bytes:
    """Calcula los 2 bytes ASCII del checksum, sumando desde STX hasta el último byte del cuerpo.
    Devuelve b"XY" (dos bytes ASCII) listos para anexar antes de ETX.
    """
    total = sum(payload)
    # dos menos significativos
    lo = total & 0x0F
    hi = (total >> 4) & 0x0F
    # ¡OJO! El protocolo pide "dos caracteres menos significativos" de la suma total.
    # El ejemplo oficial efectivamente resulta en 0x06 y 0x07 y luego +0x30 cada uno.
    # Para cubrir correctamente 0..255, usamos total & 0xFF y separamos nibbles.
    b = total & 0xFF
    hi = (b >> 4) & 0x0F
    lo = b & 0x0F
    return bytes([hi + 0x30, lo + 0x30])


def _build_body(cmd: str, data: str = "", equipo_id: str = "01") -> bytes:
    if not equipo_id or len(equipo_id) != 2:
        equipo_id = "01"
    return ("C" + equipo_id + cmd + data).encode("ascii", errors="ignore")


def build_frame(cmd: str, data: str = "", equipo_id: str = "01") -> bytes:
    """Arma el frame completo para enviar a la balanza.
    Frame: 0x02 + cuerpo + CHK(2 ASCII) + 0x04
    """
    body = _build_body(cmd, data, equipo_id)
    base = bytes([STX]) + body
    chk = _checksum_bytes(base)
    return base + chk + bytes([ETX])


def pad_num(valor: int | str, width: int) -> str:
    s = str(valor if valor is not None else "")
    s = "".join(ch for ch in s if ch.isdigit())  # solo dígitos
    return s.rjust(width, "0")[:width]


def pad_str(valor: str | None, width: int) -> str:
    s = (valor or "").encode("latin1", errors="ignore").decode("latin1")
    s = s.ljust(width)[:width]
    return s

# =========================
# Conexión TCP simple
# =========================

@dataclass
class Equipo:
    id: int
    nombre: str
    ip: str
    puerto: int
    deptos: List[str]
    autoreport: bool


def cmd_departamento(codigo_3: str, nombre_16: str) -> bytes:
    data = pad_num(codigo_3, 3) + pad_str(nombre_16, 16)
    return build_frame("2003", data)


def cmd_plu(
    nro_plu: int | str,
    cod_depto: str,
    cod_familia: str,
    nombre_plu: str,
    descripcion: str = "",
    codigo_plu: str = "",
    tipo: str = "P",
    valor_fijo: int | str = 0,
    precio: int | str = 0,
    precio_alt: int | str = 0,
    precio_ant_o_puntodec: int | str = 0,
    impuesto1: int | str = 0,
    impuesto2: int | str = 0,
    tara_pre: int | str = 0,
    tara_pub: int | str = 0,
    cod_etiqueta: int | str = 1,
    cod_receta: int | str = 0,
    cod_nutri: int | str = 0,
    fecha_envase_no: int | str = 0,
    venc_dias: int | str = 0,
    cod_imagen: int | str = 0,
) -> bytes:
    """Builder mínimo de 2005 según campos más usados.
    Asegurá mapear los valores a los tamaños del protocolo.
    """
    data = (
        pad_num(nro_plu, 6) +
        pad_num(cod_depto, 3) +
        pad_num(cod_familia, 3) +
        pad_str(nombre_plu, 26) +
        pad_str(descripcion, 26) +
        pad_str(codigo_plu, 5) +
        (tipo[:1] if tipo else "P") +
        pad_num(valor_fijo, 7) +
        pad_num(precio, 7) +
        pad_num(precio_alt, 7) +
        pad_num(precio_ant_o_puntodec, 7) +
        pad_num(impuesto1, 6) +
        pad_num(impuesto2, 6) +
        pad_num(tara_pre, 5) +
        pad_num(tara_pub, 5) +
        pad_num(cod_etiqueta, 2) +
        pad_num(cod_receta, 4) +
        pad_num(cod_nutri, 4) +
        pad_num(fecha_envase_no, 1) +
        pad_num(venc_dias, 3) +
        pad_num(cod_imagen, 4)
    )
    return build_frame("2005", data)

# =========================
# Servicio de envíos
# =========================

ProgressCb = Callable[[str, Dict], None]

class EnvioBalanzasService:
    def __init__(self, repo: Optional[RepoSybase] = None, timeout: float = 2.5):
        self.repo = repo or RepoSybase()
        self.timeout = timeout
        self._equipos_ok: List[Equipo] = []

    # ---- Descubrir & testear ----
    def descubrir_equipos(self) -> List[Equipo]:
        eqs: List[Equipo] = []
        for e in self.repo.equipos:
            eqs.append(Equipo(
                id=int(e.get("id") or 0),
                nombre=e.get("nombre", ""),
                ip=e.get("ip", ""),
                puerto=int(e.get("puerto") or 1001),
                deptos=list(e.get("deptos") or []),
                autoreport=bool(e.get("autoreport", False))
            ))
        return eqs

    def testear_conexiones(self, on_progress: Optional[ProgressCb] = None, beep: bool = False) -> List[Equipo]:
        self._equipos_ok.clear()
        for e in self.descubrir_equipos():
            ok = False
            try:
                cli = KretzTCP(e.ip, e.puerto, timeout=self.timeout)
                ok = cli.ping(beep=beep)
            except Exception as err:
                ok = False
                if on_progress:
                    on_progress("error", {"equipo": e, "error": str(err)})
            if ok:
                self._equipos_ok.append(e)
                if on_progress:
                    on_progress("ok", {"equipo": e})
            else:
                if on_progress:
                    on_progress("fail", {"equipo": e})
        return list(self._equipos_ok)

    # ---- Envíos ----
    # 1) Al enviar departamentos, prepará frames y mandá en batch:
    def enviar_departamentos(self, on_progress=None, inter_delay=0.05, max_retries=1):
        # mapa codigo(3)->nombre
        dep_map = {str(d["codigo"])[-3:].rjust(3, "0"): (d.get("nombre") or "") 
                for d in (self.repo.departamentos or [])}

        for e in self._equipos_ok:
            orig = list(e.deptos or [])
            cods3 = [str(c)[-3:].rjust(3, "0") for c in orig]

            if on_progress:
                on_progress("info", {"equipo": e, "msg": f"Enviando {len(cods3)} deptos"})

            # construir frames con nombre correcto; si no hay, usar "DEP-XXX" por claridad
            frames = []
            for c_raw, c3 in zip(orig, cods3):
                nombre = dep_map.get(c3) or dep_map.get(c_raw) or f"DEP-{c3}"
                frames.append(cmd_departamento(c3, nombre))

            # cliente con logger (lo agregamos en el driver abajo)
            cli = KretzTCP(e.ip, e.puerto, timeout=self.timeout)
            if on_progress:
                # cada trace del driver te llega con 'ev="trace"'
                cli.set_logger(lambda msg: on_progress("trace", {"equipo": e, "msg": msg}))

            resps = cli.send_many(frames, inter_delay=inter_delay, max_retries=max_retries)

            for idx, c3 in enumerate(cods3):
                resp = resps[idx] if idx < len(resps) else b""
                ok = (resp and len(resp) >= 4 and resp[0] == RESP_STX and resp[-1] == ETX)
                if ok:
                    if on_progress:
                        on_progress("depto_ok", {"equipo": e, "codigo": c3, "nombre": dep_map.get(c3, f"DEP-{c3}")})
                else:
                    if on_progress:
                        on_progress("depto_error", {"equipo": e, "codigo": c3, "error": "Respuesta inválida/timeout"})


# 2) Para artículos, mismo patrón: preparar frames -> cli.send_many(..., inter_delay=0.05)


    def _obtener_articulos_para_equipo(self, e: Equipo):
        """
        Trae artículos por los deptos del equipo y los mapea para cmd_plu().
        """
        if not e.deptos:
            return []
        rows = _fetch_articulos_balanza(self.repo, e.deptos)
        return [_map_row_to_plu(r) for r in rows]

    def enviar_articulos(self, on_progress: Optional[ProgressCb] = None) -> None:
        for e in self._equipos_ok:
            articulos = list(self._obtener_articulos_para_equipo(e))
            if on_progress:
                on_progress("info", {"equipo": e, "msg": f"Enviando {len(articulos)} artículos"})
            cli = KretzTCP(e.ip, e.puerto, timeout=self.timeout)
            for art in articulos:
                try:
                    frame = cmd_plu(
                        nro_plu=art.get("nro_plu"),
                        cod_depto=art.get("cod_depto"),
                        cod_familia=art.get("cod_familia", 1),
                        nombre_plu=art.get("nombre_plu", ""),
                        descripcion=art.get("descripcion", ""),
                        codigo_plu=art.get("codigo_plu", ""),
                        tipo=art.get("tipo", "P"),
                        valor_fijo=art.get("valor_fijo", 0),
                        precio=art.get("precio", 0),
                        precio_alt=art.get("precio_alt", 0),
                        precio_ant_o_puntodec=art.get("precio_ant_o_puntodec", 0),
                        impuesto1=art.get("impuesto1", 0),
                        impuesto2=art.get("impuesto2", 0),
                        tara_pre=art.get("tara_pre", 0),
                        tara_pub=art.get("tara_pub", 0),
                        cod_etiqueta=art.get("cod_etiqueta", 1),
                        cod_receta=art.get("cod_receta", 0),
                        cod_nutri=art.get("cod_nutri", 0),
                        fecha_envase_no=art.get("fecha_envase_no", 0),
                        venc_dias=art.get("venc_dias", 0),
                        cod_imagen=art.get("cod_imagen", 0),
                    )
                    resp = cli.send(frame)
                    if not (resp and resp[0] == RESP_STX and resp[-1] == ETX):
                        raise RuntimeError("Respuesta inválida")
                    if on_progress:
                        on_progress("plu_ok", {"equipo": e, "plu": art.get("nro_plu")})
                except Exception as err:
                    if on_progress:
                        on_progress("plu_error", {"equipo": e, "plu": art.get("nro_plu"), "error": str(err)})