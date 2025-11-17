# GUI/kretz_adapter.py
import re
from dataclasses import dataclass
from typing import Iterable, Callable, Sequence
import time
from collections import Counter
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from GUI.jdg_driver import JDataGatePool  # usa tu pool por IP (workdir/instancia única)

# ===== Constantes de protocolo (según manual) =====
TIPO_EQUIPO = "C"  # Familia Report Nx (manual 2.1 “Tipo de equipo = C”)

# Comandos frecuentes (ver índice sección 4.* del manual)
CMD_TEST_BEEP         = "0001"  # Test con sonido
CMD_TEST_SILENT       = "0002"  # Test sin sonido
CMD_SET_IDIOMA        = "1001"  # Seteo de idioma (00 esp / 01 eng)
CMD_RESTAURAR_MODELO  = "1002"  # Restaurar modelo de datos
CMD_FORMAT_NUEVO_MD   = "1003"  # Formateo modelo de datos
CMD_ALTA_DEPTO        = "2003"  # Alta/Modif Departamento
CMD_ALTA_FAMILIA      = "2004"  # Alta/Modif Familia
CMD_ALTA_PLU          = "2005"  # Alta/Modif PLU
CMD_BAJA_DEPTO        = "3003"  # Baja Departamento
CMD_BAJA_PLU          = "3005"  # Baja PLU
CMD_VACIAR_DEPTOS     = "4003"  # Baja todos los departamentos
CMD_VACIAR_PLUS       = "4005"  # Baja todos los PLUs
CMD_READ_FIELD_LEN = 5002  # devuelve longitud (en caracteres) de un campo de una entidad

# Tipos & callbacks
OnProgress = Callable[[str, dict], None]  # (evento, datos) -> None


@dataclass
class EquipoDef:
    nombre: str
    ip: str
    puerto: int = 1001
    id_equipo: int = 1     # aunque TCP/IP “no lo toma”, va en INFO por formato (manual 3.5.2)
    idioma: str = "00"     # 00 español, 01 inglés (manual 3.4 / 4.3)

def _pad_num(value: int | str, length: int) -> str:
    s = str(value or "").strip()
    if not s.isdigit():
        s = "0"
    return s[-length:].rjust(length, "0")

def _pad_txt(value: str, length: int) -> str:
    s = (value or "")
    # recortar y rellenar con espacios a derecha como exige el manual donde aplica
    s = s[:length]
    return s.ljust(length, " ")

def _ci(d: dict, *names):
    """Case-insensitive y alias-friendly getter."""
    if not isinstance(d, dict):
        return None
    ln = {k.lower(): k for k in d.keys()}
    for n in names:
        k = ln.get(str(n).lower())
        if k is not None:
            return d[k]
    return None

def _digits(s) -> str:
    """Extrae sólo dígitos (evita letras/espacios)."""
    return re.sub(r"\D+", "", str(s or ""))


def _coerce_plu6(it: dict) -> str | None:
    """
    Regla pedida por vos:
      - Si CREF tiene exactamente 4 dígitos → usarlo.
      - Si no, tomar los 4 dígitos siguientes a los 2 primeros de CCODEBAR.
        Ej: CCODEBAR '2010170000000' -> '1017'
      - Devolverlo como PLU6 rellenado a la izquierda con ceros.
    """
    cref = str(_ci(it, "CREF") or "").strip()
    ccb  = str(_ci(it, "CCODEBAR") or "").strip()

    base4 = None
    if len(cref) == 4 and cref.isdigit():
        base4 = cref
    else:
        # CCODEBAR: capturamos 2 primeros + 4 siguientes → usamos esos 4
        # Acepta EAN13 o similares mientras arranquen con 2 dígitos
        m = re.match(r"^\d{2}(\d{4})", ccb)
        if m:
            base4 = m.group(1)

    if not base4:
        return None

    # El modelo 2005 requiere PLU de 6 posiciones
    return base4.zfill(6)

def _coerce_dep3(item: dict) -> str | None:
    s = _digits(_ci(item, "CGRPCONTA", "depto", "cod_depto"))
    if not s:
        return None
    n = int(s[-3:])  # usamos últimos 3 si viniera con relleno
    if n <= 0:
        return None
    return _pad_num(n, 3)

def _tipo_from_cvencom(v) -> str:
    s = (str(v or "")).strip().upper()
    if s in {"U", "UNI", "UN", "UNIDAD", "N", "NO"}:
        return "N"
    return "P"  # default pesable

def _imp_from_ctipoiva(v) -> str:
    try:
        p = float(str(v).replace(",", "."))
    except Exception:
        p = 0.0
    n = int(round(p * 100))  # 21.00 -> 2100
    return _pad_num(n, 6)

def _valid_2005_data(datos: str) -> bool:
    return len(datos) == MODEL_2005_TOTAL  # 135 en tu equipo


def _trace_2005(label, s):
    bloques = [
        ("plu",6),("dep",3),("fam",3),("nombre",26),("desc",26),
        ("cod_plu5",5),("tipo",1),("valor_fijo",7),("precio",7),
        ("precio_alt",7),("precio_ant_pd",7),("imp1",6),("imp2",6),
        ("tara_pre",5),("tara_pub",5),("cod_etq",2),("cod_rec",4),
        ("cod_nut",4),("fecha_env",1),("vto_dias",3),("cod_img",4)
    ]
    i=0
    print(f"TRACE 2005 [{label}] len={len(s)}")
    for k,L in bloques:
        chunk = s[i:i+L]
        print(f"  {k:12}({L}): '{chunk}'")
        i+=L
        
def _rjust_num(n: int, width: int) -> str:
    return str(int(n)).rjust(width, "0")

_5002_RX = re.compile(
    r"^C(?P<ideq>\d{2})"
    r"(?P<entity>\d{2})"
    r"(?P<fn>\d{2})"
    r"(?P<ent2>\d{2})"
    r"(?P<campo>\d{2})"
    r"(?P<largo>\d{3})"
    r"(?P<crc>\d{2})?$"
)

# === Modelo de datos PLU leido por 5002 en tu equipo ===
MODEL_2005_PLU = {
    1: 6,  2: 3,  3: 3,  4: 26, 5: 26, 6: 5,  7: 1,  8: 7,
    9: 6, 10: 6, 11: 6, 12: 6, 13: 6, 14: 5, 15: 5, 16: 2,
    17: 4, 18: 4, 19: 4, 20: 0, 21: 0, 22: 4,
}
MODEL_2005_TOTAL = sum(MODEL_2005_PLU.values())  # = 135

def _pad_num_w(n: int, w: int) -> str:
    return str(int(n)).rjust(w, "0") if w > 0 else ""

def _pad_txt_w(s: str, w: int) -> str:
    s = (s or "")
    return (s[:w]).ljust(w) if w > 0 else ""

def _price_to_width(v, w: int) -> str:
    """Precio en centavos con ancho 'w'. Clampa al rango."""
    if w <= 0:
        return ""
    from decimal import Decimal, InvalidOperation
    s = str(v).strip().replace(",", ".")
    try:
        cents = int((Decimal(s) * 100).quantize(Decimal("1"))) if s and not s.isdigit() else int(s or 0)
    except (InvalidOperation, ValueError):
        cents = 0
    if cents < 0: cents = 0
    maxv = (10 ** w) - 1
    if cents > maxv: cents = maxv
    return str(cents).rjust(w, "0")

def _parse_5002_lines(lines, entidad: int) -> dict[int, int]:
    """
    lines: lista de líneas del EXT (strings 'C01....')
    entidad: la entidad consultada (ej: 5 = PLU)
    retorna {campo:int -> largo:int}
    """
    out = {}
    for ln in lines or []:
        ln = ln.strip()
        m = _5002_RX.match(ln)
        if not m:
            continue
        # Chequear que sea la función 02 (5002) y la entidad que pedimos
        if m.group("fn") != "02":
            continue
        # En algunos firmwares m.group("entity") ya es '05'; en otros duplican en ent2
        e1 = int(m.group("entity"))
        e2 = int(m.group("ent2"))
        if e1 != entidad or e2 != entidad:
            # algunos devuelven entity=01 (ID equipo) y ent2=05; si querés ser más laxo, comenta esta línea
            pass
        campo = int(m.group("campo"))
        largo = int(m.group("largo"))
        if largo > 0:
            out[campo] = largo
    return out


def _price_to_width_dec(valor, width: int, decimals: int) -> str:
    """Serializa el precio a 'width' dígitos con 'decimals' decimales implícitos."""
    try:
        p = Decimal(str(valor))
    except Exception:
        p = Decimal(0)
    factor = Decimal(10) ** decimals
    n = int((p * factor).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    s = str(n)
    if len(s) > width:
        # fuera de rango para EAN de 6 dígitos -> devolvé None para saltear o recortar
        return None
    return s.rjust(width, "0")

class KretzAdapter:
    def __init__(self, base_dir: str, exe_path: str, default_retries: int = 1, workdir_mode: str = "ip_subdirs"):
        self.pool = JDataGatePool(base_dir, exe_path, workdir_mode=workdir_mode)
        self.default_retries = int(default_retries)
        
    def leer_log_driver(self, eq: EquipoDef, tail_lines: int = 200) -> list[str]:
        inst = self.pool.get(eq.ip, eq.puerto, idioma=eq.idioma,
                            retries=self.default_retries, show_console=False, no_touch=True)
        return inst.read_driver_log_tail(lines=tail_lines)


    # ===== Helpers base =====
    def _mk_info_line(self, eq: EquipoDef, cmd: str, datos: str = "") -> str:
        # Formato INFO = TIPO(1) + ID(2) + CMD(4) + DATOS (manual 3.5.2)
        return f"{TIPO_EQUIPO}{_pad_num(eq.id_equipo, 2)}{cmd}{datos}"


    def _extract_cmds_from_info_lines(self, info_lines: list[str]) -> list[str]:
        # INFO/RESP formato: C + ID(2) + CMD(4) + ...
        cmds = []
        for s in info_lines or []:
            try:
                if s and s[0] in ("C", "S") and len(s) >= 7:
                    cmds.append(s[3:7])
            except Exception:
                pass
        return cmds

    def _send(self, eq: EquipoDef, lines, *,
            show_console=False, wait_for_ext=True, timeout: float = 12.0,
            on_progress=None, allow_retry: bool = False) -> list[str]:
        import time as _t

        # Instancia del driver (no tocamos archivos desde el pool)
        inst = self.pool.get(
            eq.ip, eq.puerto, idioma=eq.idioma,
            retries=self.default_retries, show_console=show_console, no_touch=True
        )

        expected_cmds = self._extract_cmds_from_info_lines(list(lines))
        req = Counter(expected_cmds)
        resp_lines: list[str] = []

        with inst.lock:
            # --- 0) Parar siempre el EXE para evitar que lea "a medias"
            try:
                inst._stop()
            except Exception:
                pass

            # --- 1) Asegurar COM/CONF del peer actual
            try:
                inst.write_conf(eq.idioma)
            except Exception:
                pass
            inst.write_com_tcp(eq.ip, eq.puerto)

            # --- 2) Preparar archivos base SIN truncar INFO a vacío
            ext_path = inst.ensure_ext_clear()   # baseline limpio para conteo
            inst.ensure_info()                   # crea INFO si no existe
            _t.sleep(0.05)

            # --- 3) Escribir INFO COMPLETO (in-place; ya tenés ese método robusto)
            # tip anti-colisión de segundo: si querés, esperá a cambiar de segundo
            try:
                prev_info_mtime = (inst.workdir / "INFO.JDG").stat().st_mtime
                while int(_t.time()) <= int(prev_info_mtime):
                    _t.sleep(0.05)
            except FileNotFoundError:
                pass

            inst.send_info_lines(list(lines))
            if on_progress:
                on_progress("info_sent", {"equipo": eq.nombre, "ip": eq.ip, "n": len(lines)})

            # --- 4) Arrancar el EXE y dejarlo enganchar watchers
            inst.ensure_running(show_console=show_console)
            _t.sleep(0.25)

            # --- 5) Espera por crecimiento en EXT y conteo de ACKs (vs baseline)
            prev_size = ext_path.stat().st_size if ext_path.exists() else 0

            if wait_for_ext:
                ok = inst.wait_for_ext_growth(prev_size, timeout=timeout)
                if on_progress:
                    on_progress("ext_growth", {"equipo": eq.nombre, "ok": ok, "timeout": timeout})

            pre_ext_lines = []  # baseline de conteo = 0 porque limpiamos EXT recién
            base_counts = Counter(self._extract_cmds_from_info_lines(pre_ext_lines))

            t_end = _t.time() + max(timeout, 2.0)
            have = Counter()

            def _update_have():
                nonlocal resp_lines, have
                resp_lines = inst.read_ext_lines()
                now_counts = Counter(self._extract_cmds_from_info_lines(resp_lines))
                have = Counter({k: max(0, now_counts.get(k, 0) - base_counts.get(k, 0)) for k in req})

            _update_have()
            while wait_for_ext and any(have.get(k, 0) < req[k] for k in req) and _t.time() < t_end:
                _t.sleep(0.1)
                _update_have()

            if on_progress:
                on_progress("acks", {"equipo": eq.nombre, "need": dict(req), "got": dict(have)})

            # --- 6) (Opcional) Reintento explícito sólo si lo pedís
            if allow_retry and wait_for_ext and any(have.get(k, 0) < req[k] for k in req):
                if on_progress:
                    on_progress("retry", {"equipo": eq.nombre, "msg": "No se completaron todos los ACKs; wake + reenvío"})
                wake = self._mk_info_line(eq, CMD_TEST_SILENT)
                inst.send_info_lines([wake])
                _t.sleep(0.15)
                # limpiar EXT para nuevo baseline y re-enviar
                ext_path = inst.ensure_ext_clear()
                prev_size = ext_path.stat().st_size if ext_path.exists() else 0
                inst.send_info_lines(list(lines))
                inst.wait_for_ext_growth(prev_size, timeout=timeout)
                _update_have()
                if on_progress:
                    on_progress("acks_after_retry", {"equipo": eq.nombre, "need": dict(req), "got": dict(have)})

            # ¡Importante!: no vaciar INFO a "" aquí.
        return resp_lines

    
    # ===== Comandos simples =====
    def test_comunicacion(self, eq: EquipoDef, con_beep: bool = True, **kw):
        cmd = CMD_TEST_BEEP if con_beep else CMD_TEST_SILENT
        line = self._mk_info_line(eq, cmd)
        return self._send(eq, [line], **kw)

    def set_idioma(self, eq: EquipoDef, idioma: str = "00", **kw):
        datos = _pad_num(idioma, 2)
        line = self._mk_info_line(eq, CMD_SET_IDIOMA, datos)
        return self._send(eq, [line], **kw)

    def restaurar_modelo(self, eq: EquipoDef, **kw):
        line = self._mk_info_line(eq, CMD_RESTAURAR_MODELO)
        return self._send(eq, [line], **kw)

    def formatear_nuevo_modelo(self, eq: EquipoDef, **kw):
        line = self._mk_info_line(eq, CMD_FORMAT_NUEVO_MD)
        return self._send(eq, [line], **kw)

    # ===== Altas / Modificaciones =====
    def alta_departamento(self, eq: EquipoDef, codigo: str | int, nombre: str, **kw):
        """
        2003 – Datos:
          - Código depto (3 dígitos)
          - Nombre depto (16 chars)
        """
        datos = _pad_num(codigo, 3) + _pad_txt(nombre, 16)
        line = self._mk_info_line(eq, CMD_ALTA_DEPTO, datos)
        return self._send(eq, [line], **kw)

    def alta_familia(self, eq: EquipoDef, cod_depto: str|int, cod_familia: str|int, nombre: str, **kw):
        """
        2004 – Datos:
          - Código depto (3)
          - Código familia (3)
          - Nombre (16)
        """
        datos = _pad_num(cod_depto, 3) + _pad_num(cod_familia, 3) + _pad_txt(nombre, 16)
        line = self._mk_info_line(eq, CMD_ALTA_FAMILIA, datos)
        return self._send(eq, [line], **kw)

    def alta_plu(
        self,
        eq: EquipoDef,
        nro_plu: str|int,
        cod_depto: str|int,
        cod_familia: str|int,
        nombre: str,
        descripcion: str = "",
        codigo_plu: str = "",
        tipo: str = "P",           # P/N/R
        valor_fijo: str|int = 0,   # 7 dígitos (ver nota consumo preferente)
        precio: str|int = 0,
        precio_alt: str|int = 0,
        precio_ant: str|int = 0,   # en nx LED/LCD se usa como punto decimal de precio (manual)
        imp1: str|int = 0,         # 6 dígitos (002100 para 21%)
        imp2: str|int = 0,
        tara_pre: str|int = 0,     # 5
        tara_pub: str|int = 0,     # 5
        cod_etq: str|int = 1,      # 2
        cod_receta: str|int = 0,   # 4
        cod_nutri: str|int = 0,    # 4
        fecha_env: int = 0,        # 1 (0 imprime, 1 no imprime)
        vto_dias: int = 0,         # 3 (0 no imprime)
        cod_imagen: int = 0,       # 4
        **kw
    ):
        """
        2005 – Campos por defecto (ver R32 – sección 4.39).
        NOTA: Las longitudes cambian según modelo (LCD/LED), aquí usamos las por defecto del doc.
        """
        datos = (
            _pad_num(nro_plu,     6) +
            _pad_num(cod_depto,   3) +
            _pad_num(cod_familia, 3) +
            _pad_txt(nombre,     26) +
            _pad_txt(descripcion,26) +
            _pad_txt(codigo_plu,  5) +
            (tipo or "P")[0] +
            _pad_num(valor_fijo,  7) +
            _pad_num(precio,      7) +
            _pad_num(precio_alt,  7) +
            _pad_num(precio_ant,  7) +
            _pad_num(imp1,        6) +
            _pad_num(imp2,        6) +
            _pad_num(tara_pre,    5) +
            _pad_num(tara_pub,    5) +
            _pad_num(cod_etq,     2) +
            _pad_num(cod_receta,  4) +
            _pad_num(cod_nutri,   4) +
            _pad_num(fecha_env,   1) +
            _pad_num(vto_dias,    3) +
            _pad_num(cod_imagen,  4)
        )
        line = self._mk_info_line(eq, CMD_ALTA_PLU, datos)
        return self._send(eq, [line], **kw)

    # ===== Bajas unitarias / masivas =====
    def baja_departamento(self, eq: EquipoDef, codigo: str|int, **kw):
        datos = _pad_num(codigo, 3)
        line = self._mk_info_line(eq, CMD_BAJA_DEPTO, datos)
        return self._send(eq, [line], **kw)

    def baja_plu(self, eq: EquipoDef, nro_plu: str|int, **kw):
        datos = _pad_num(nro_plu, 6)
        line = self._mk_info_line(eq, CMD_BAJA_PLU, datos)
        return self._send(eq, [line], **kw)

    def vaciar_departamentos(self, eq: EquipoDef, **kw):
        line = self._mk_info_line(eq, CMD_VACIAR_DEPTOS)
        return self._send(eq, [line], **kw)

    def vaciar_plus(self, eq: EquipoDef, **kw):
        line = self._mk_info_line(eq, CMD_VACIAR_PLUS)
        return self._send(eq, [line], **kw)

    # ===== Batch helpers =====
    def enviar_departamentos(self, eq: EquipoDef, items: Iterable[dict], **kw):
        print(kw)
        """
        items: [{"codigo": "001", "nombre": "PANADERIA"}, ...]
        Genera todas las líneas 2003 y las envía en un único INFO.JDG.
        """
        lines = []
        for it in items:
            datos = _pad_num(it.get("codigo"), 3) + _pad_txt(it.get("nombre"), 16)
            lines.append(self._mk_info_line(eq, CMD_ALTA_DEPTO, datos))
        return self._send(eq, lines, **kw)

    def enviar_por_equipo(self, eq: EquipoDef, info_lines: Sequence[str], **kw):
        """
        Si ya traes las líneas INFO “crudas”, las despacha tal cual.
        """
        return self._send(eq, info_lines, **kw)

    def enviar_plus(self, eq: EquipoDef, items, *, cod_familia_def=1, **kw):
        lines = []
        on_progress = kw.get("on_progress")

        for it in (items or []):
            datos = self._build_datos_2005_modelo(it, fam_def=cod_familia_def)
            if not datos:
                if on_progress:
                    on_progress("skip", {"equipo": getattr(eq, "nombre", ""), "motivo": "PLU/DEP inválido", "item": it})
                continue

            if not _valid_2005_data(datos):
                if on_progress:
                    on_progress("skip", {"equipo": getattr(eq, "nombre", ""), "motivo": f"Len inválida (len={len(datos)}, esperado={MODEL_2005_TOTAL})"})
                continue

            lines.append(self._mk_info_line(eq, CMD_ALTA_PLU, datos))

        if not lines:
            if on_progress:
                on_progress("info", {"equipo": getattr(eq, "nombre", ""), "msg": "Sin PLU válidos para enviar"})
            return []

        return self._send(eq, lines, **kw)

    def enviar_dptos_y_articulos(
            self,
            eq: EquipoDef,
            deptos: list,              # p.ej. ["005","010"] o [{"codigo":"005","nombre":"Carnes"}, ...]
            articulos: list[dict],     # filas del DAO con CREF, CDETALLE, CGRPCONTA, NPVP1, CCODEBAR, CVENCOM, CTIPOIVA
            *,
            cod_familia_def: int | str = 0,
            show_console: bool = True,
            on_progress=None,
            allow_retry: bool = False,
            timeout: float = 20.0
        ):
        lines: list[str] = []

        # 1) 2003 - Departamentos primero
        for d in deptos or []:
            cod = _pad_num(d.get("codigo") if isinstance(d, dict) else d, 3)
            nombre = d.get("nombre") if isinstance(d, dict) else f"DEP {cod}"
            nombre = _pad_txt(nombre or f"DEP {cod}", 16)
            datos = cod + nombre
            lines.append(self._mk_info_line(eq, CMD_ALTA_DEPTO, datos))

        # 2) 2005 - PLU (sin familias del origen; usamos fija)
        first_plu_traced = False
        for it in articulos or []:
            datos = self._build_datos_2005_modelo(it, fam_def=cod_familia_def)
            if not datos:
                if on_progress:
                    on_progress("skip", {"equipo": eq.nombre, "motivo": "PLU/DEP inválido", "item": _ci(it, "CREF")})
                continue

            if not _valid_2005_data(datos):
                if on_progress:
                    on_progress("skip", {
                        "equipo": eq.nombre,
                        "motivo": f"Len 2005 inválida (len={len(datos)}, esperado=135)",
                        "plu": _coerce_plu6(it), "dep": _coerce_dep3(it)
                    })
                continue

            # Traza del primer PLU (aunque ya se hayan agregado deptos)
            if on_progress and not first_plu_traced:
                _trace_2005(_ci(it, "CREF") or "?", datos)
                first_plu_traced = True

            # --- TRACE 2005 (línea cruda + chequeo del campo #22 con suma de anchos) ---
            try:
                linea_txt = datos  # str
                if on_progress:
                    on_progress("trace", {"equipo": eq.nombre, "msg": f"2005→ {linea_txt}"})
                else:
                    print(f"[TRACE 2005] {linea_txt}")

                # Extraer el campo #22 (decimales de precio en etiqueta) de forma robusta
                start_22 = sum(MODEL_2005_PLU[i] for i in range(1, 22))  # campos 1..21
                w22 = MODEL_2005_PLU[22]
                valor_22 = linea_txt[start_22:start_22 + w22]

                msg_22 = f"[TRACE 2005] campo#22(dec_prec)='{valor_22}' (esperado '1' para 1 decimal)"
                if on_progress:
                    on_progress("trace", {"equipo": eq.nombre, "msg": msg_22})
                else:
                    print(msg_22)
            except Exception as ex:
                if on_progress:
                    on_progress("trace", {"equipo": eq.nombre, "msg": f"[TRACE 2005] error mostrando dec_prec: {ex!r}"})

            lines.append(self._mk_info_line(eq, CMD_ALTA_PLU, datos))

        # 3) Enviar TODO en un solo INFO (nuestro _send ya hace stop -> write -> start -> wait)
        return self._send(
            eq,
            lines,
            show_console=show_console,
            wait_for_ext=True,
            timeout=timeout,
            on_progress=on_progress,
            allow_retry=allow_retry
        )

        
    def enviar_con_1_decimal(self, eq: EquipoDef, deptos: list, articulos: list[dict], *,
                         moneda=2, dec_precio=1, dec_peso=3, **kw):
        # 1) Seleccionar moneda y setear decimales
        self.seleccionar_moneda_1010(eq, moneda=moneda, show_console=False)
        self.configurar_moneda_2026(eq, moneda=moneda, dec_precio=dec_precio, dec_peso=dec_peso, show_console=False)

        # 2) Enviar deptos + artículos como siempre
        return self.enviar_dptos_y_articulos(eq, deptos, articulos, **kw)

        
    def leer_longitudes_campos(self, eq: EquipoDef, entidad: int, campos: Iterable[int], *,
                            show_console=True, timeout: float = 10.0, on_progress=None) -> dict[int, int]:
        """
        Consulta 5002 por cada 'campo' de 'entidad' y retorna {campo: largo}.
        """
        # 1) Armar todas las líneas 5002 a enviar
        lines = []
        ent2 = _rjust_num(entidad, 2)
        campos = list(campos)
        for c in campos:
            c2 = _rjust_num(c, 2)
            datos = ent2 + c2  # entidad(2) + campo(2)
            lines.append(self._mk_info_line(eq, CMD_READ_FIELD_LEN, datos))

        # 2) Enviar y esperar respuestas en EXT
        resp_lines = self._send(
            eq, lines,
            show_console=show_console,
            wait_for_ext=True,
            timeout=timeout,
            on_progress=on_progress,
            allow_retry=False
        )

        # 3) Parsear
        mapa = _parse_5002_lines(resp_lines, entidad)
        if on_progress:
            tot = sum(mapa.values())
            on_progress("modelo_entidad", {
                "equipo": getattr(eq, "nombre", ""),
                "entidad": entidad,
                "total_chars": tot,
                "campos_ok": sorted(mapa.items())
            })
            # Avisar qué campos no respondieron (si pedimos más)
            faltan = [c for c in campos if c not in mapa]
            if faltan:
                on_progress("modelo_entidad_warn", {
                    "equipo": getattr(eq, "nombre", ""),
                    "entidad": entidad,
                    "campos_sin_respuesta": faltan
                })
        return mapa
    
    def diagnosticar_modelo(self, eq: EquipoDef, *,
                            campos_plu: int = 32, campos_depto: int = 8,
                            show_console=True, on_progress=None):
        """
        Lee modelo de PLU (05) y Depto (03) y devuelve un dict:
        {
        'plu': {campo: largo, ... , '__total__': suma},
        'depto': {campo: largo, ... , '__total__': suma}
        }
        """
        out = {}

        # PLU (05)
        m_plu = self.leer_longitudes_campos(
            eq, entidad=5, campos=range(1, campos_plu+1),
            show_console=show_console, on_progress=on_progress
        )
        m_plu["__total__"] = sum(m_plu.values())
        out["plu"] = m_plu

        # DEPTO (03)
        m_dep = self.leer_longitudes_campos(
            eq, entidad=3, campos=range(1, campos_depto+1),
            show_console=show_console, on_progress=on_progress
        )
        m_dep["__total__"] = sum(m_dep.values())
        out["depto"] = m_dep

        # Log amigable
        if on_progress:
            on_progress("modelo_resumen", {
                "equipo": getattr(eq, "nombre", ""),
                "plu_total": m_plu["__total__"],
                "depto_total": m_dep["__total__"]
            })
        return out

    def _build_datos_2005_modelo(self, it: dict, *, fam_def: int | str = 0, dec_prec: int = 1) -> str | None:
        """
        Construye el registro 2005 usando:
        - PLU6 derivado de CREF/CCODEBAR (regla de 4 dígitos)
        - DEP con _coerce_dep3(it)
        - Familia provista por parámetro (por defecto 000 = sin familia)
        - dec_prec: selector de decimales del PRECIO impreso en la etiqueta
            0 = usa decimales de la moneda del equipo
            1 = 1 decimal
            2 = 0 decimales
        """
        plu6 = _coerce_plu6(it)          # p.ej. '001017'
        dep  = _coerce_dep3(it)          # p.ej. '005'
        if not (plu6 and dep):
            return None

        # PLU4 para barcode/“código PLU” (campo #6)
        plu4 = plu6[-4:]                 # p.ej. '1017'

        # 3) Familia: usar la que viene por parámetro (no hardcodear 000)
        try:
            fam_num = int(str(fam_def).strip() or "0")
        except Exception:
            fam_num = 0
        fam = _pad_num_w(fam_num, MODEL_2005_PLU[3])

        # 4) Descripciones y tipo de venta
        nom  = _pad_txt_w(_ci(it, "CDETALLE", "descripcion", "nombre") or f"PLU {plu6}", MODEL_2005_PLU[4])
        desc = _pad_txt_w("", MODEL_2005_PLU[5])                 # sin descripción extra
        tipo = (_tipo_from_cvencom(_ci(it, "CVENCOM")) or "P")   # P=precio, U=unidad, etc.
        tipo = tipo[:1].ljust(MODEL_2005_PLU[7])                 # 1 char

        # 5) Campo fijo previo a precios
        valor_fijo = _pad_num_w(0, MODEL_2005_PLU[8])

        # 6) Precios según ancho del modelo
        precio = _price_to_width(_ci(it, "NPVP1", "precio"), MODEL_2005_PLU[9])
        if precio is None:
            return None
        precio_alt = _pad_num_w(0, MODEL_2005_PLU[10])
        precio_ant = _pad_num_w(0, MODEL_2005_PLU[11])  # dejalo en 0 (no lo usamos para decimales)

        # 7) Impuestos y demás
        imp1 = _imp_from_ctipoiva(_ci(it, "CTIPOIVA")) or ""
        imp1 = imp1[-MODEL_2005_PLU[12]:].rjust(MODEL_2005_PLU[12], "0")
        imp2 = _pad_num_w(0, MODEL_2005_PLU[13])

        tara_pre = _pad_num_w(0, MODEL_2005_PLU[14])
        tara_pub = _pad_num_w(0, MODEL_2005_PLU[15])
        cod_etq  = _pad_num_w(1, MODEL_2005_PLU[16])   # etiqueta 01 por defecto
        cod_rec  = _pad_num_w(0, MODEL_2005_PLU[17])
        cod_nut  = _pad_num_w(0, MODEL_2005_PLU[18])

        campo19  = _pad_num_w(0, MODEL_2005_PLU[19])   # reservado/fecha/dec.pos
        # 20 y 21 longitud 0 → se omiten

        # === CLAVE: selector de decimales del PRECIO impreso en etiqueta ===
        # En tu modelo, el #22 es "reservado/imagen/dec.prec".
        # Forzamos 1 decimal seteando "1" (padding al ancho del campo).
        dec_prec = max(0, min(2, int(dec_prec)))       # clamp por seguridad: 0..2
        campo22  = _pad_num_w(dec_prec, MODEL_2005_PLU[22])

        # Campo #6 (ancho típico: 5) → "0" + PLU4 para que el EAN muestre los 4 dígitos del PLU
        cod5 = ("0" + plu4)
        if len(cod5) != MODEL_2005_PLU[6]:
            cod5 = cod5[-MODEL_2005_PLU[6]:].rjust(MODEL_2005_PLU[6], "0")

        datos = (
            plu6 +              # 1:6 (PLU)
            dep +               # 2:3 (DEP)
            fam +               # 3:3 (FAM)
            nom +               # 4:26
            desc +              # 5:26
            cod5 +              # 6:5  (0 + PLU4)
            tipo +              # 7:1
            valor_fijo +        # 8:7
            precio +            # 9
            precio_alt +        # 10
            precio_ant +        # 11
            imp1 +              # 12
            imp2 +              # 13
            tara_pre +          # 14
            tara_pub +          # 15
            cod_etq +           # 16
            cod_rec +           # 17
            cod_nut +           # 18
            campo19 +           # 19
            # 20 (0)
            # 21 (0)
            campo22             # 22 (DECIMALES PRECIO EN ETIQUETA)
        )
        return datos

    
    def configurar_codbarra_1070(
        self,
        eq: "EquipoDef",
        *,
        inicio_pesable: str = "24",
        incluir_peso_en_cb: bool = True,      # 1=peso, 0=importe
        inicio_no_pesable: str = "20",
        incluir_unidades_en_cb: bool = False, # 1=unidades, 0=importe
        formato: int = 2,                     # 1..6 (según manual)
        show_console: bool = False,
        timeout: float = 8.0,
        on_progress=None
    ):
        ip     = f"{int(inicio_pesable):02d}"
        inop   = f"{int(inicio_no_pesable):02d}"
        peso   = "1" if incluir_peso_en_cb else "0"
        unid   = "1" if incluir_unidades_en_cb else "0"
        fmt    = str(int(formato))

        datos  = f"{ip}{peso}{inop}{unid}{fmt}"   # campos de la 1070
        line   = self._mk_info_line(eq, "1070", datos)

        # Enviamos UNA sola línea por _send (usa pool/lock/INFO/EXT correctos)
        return self._send(
            eq,
            [line],
            show_console=show_console,
            wait_for_ext=True,
            timeout=timeout,
            on_progress=on_progress,
            allow_retry=False,
        )
        
    def configurar_moneda_2026(
        self,
        eq: "EquipoDef",
        *,
        moneda: int = 2,           # 1=moneda principal, 2=alternativa
        dec_precio: int = 1,       # << 1 decimal en precio
        dec_peso: int = 3,         # los típicos para peso (ajustá si querés)
        show_console: bool = False,
        timeout: float = 8.0,
        on_progress=None,
    ):
        """
        CMD 2026 – Configura decimales de la moneda (precio/peso).
        """
        m = f"{int(moneda):02d}"           # "01" | "02"
        dp = str(int(dec_precio))[:1]      # 0..3 según equipo/modelo
        dw = str(int(dec_peso))[:1]
        datos = f"{m}{dp}{dw}"             # según protocolo: moneda + dec_precio + dec_peso
        line  = self._mk_info_line(eq, "2026", datos)
        return self._send(eq, [line], show_console=show_console, wait_for_ext=True,
                        timeout=timeout, on_progress=on_progress, allow_retry=False)

    def seleccionar_moneda_1010(
        self,
        eq: "EquipoDef",
        *,
        moneda: int = 2,             # 1 o 2
        show_console: bool = False,
        timeout: float = 8.0,
        on_progress=None,
    ):
        """
        CMD 1010 – Selecciona moneda activa (01=principal, 02=alternativa).
        """
        datos = f"{int(moneda):02d}"
        line  = self._mk_info_line(eq, "1010", datos)
        return self._send(eq, [line], show_console=show_console, wait_for_ext=True,
                        timeout=timeout, on_progress=on_progress, allow_retry=False)


