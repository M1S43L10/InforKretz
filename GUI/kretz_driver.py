"""
Módulo: kretz_driver

Objetivo:
- Administrar el proceso del driver de Kretz (JDataGate.exe): detectar si está corriendo,
  iniciarlo una sola vez, y cerrarlo cuando se requiera.
- Ofrecer utilidades de conexión TCP a las balanzas (opcional) para pruebas rápidas
  (ping/comandos).

Ruta por defecto del driver (32-bit Windows):
    C:\\Program Files (x86)\\JDataGate\kSolutions\DataGate\JDataGate con consola.exe

Notas importantes:
- ¡No lances el driver en bucle! Usá `ensure_running()` que valida si ya está activo.
- Este módulo NO configura JDataGate (lista de equipos, etc.). Eso se hace en el propio
  JDataGate. Aquí sólo lo administramos.
- Si tu flujo envía frames TCP directo a la balanza, podés usar KretzTCP (abajo). Si tu
  arquitectura obliga a usar SIEMPRE JDataGate, igual conviene `ensure_running()` antes
  de los envíos.
"""
from __future__ import annotations

import os
import time
import socket
import subprocess
from dataclasses import dataclass
from typing import Optional, List, Iterable

DEFAULT_EXE = r"C:\Program Files (x86)\JDataGate\kSolutions\DataGate\JDataGate con consola.exe"
IMAGE_NAME = "JDataGate con consola.exe"

# =====================================================================================
# Helpers de proceso (sin dependencias externas)
# =====================================================================================

def _tasklist_contains(image_name: str = IMAGE_NAME) -> bool:
    """Devuelve True si el proceso aparece en `tasklist`.
    Compatible con Windows. Evita depender de psutil.
    """
    try:
        out = subprocess.run(
            ["tasklist", "/FI", f"IMAGENAME eq {image_name}"],
            capture_output=True, text=True, check=False
        )
        return image_name.lower() in (out.stdout or "").lower()
    except Exception:
        return False


def _start_hidden(cmd: List[str], cwd: Optional[str] = None, show_console: bool = False, log_path: Optional[str] = None) -> subprocess.Popen:
    """Inicia el proceso. Si show_console=True, abre consola visible; si no, lo oculta.
    Si log_path está definido, redirige stdout/stderr al archivo (append)."""
    stdout = subprocess.DEVNULL
    stderr = subprocess.DEVNULL
    if log_path:
        try:
            # Abrimos en append binario; stderr -> stdout
            stdout = open(log_path, "ab")
            stderr = subprocess.STDOUT
        except Exception:
            pass

    if show_console:
        startupinfo = None
        creationflags = subprocess.CREATE_NEW_CONSOLE
    else:
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 0  # SW_HIDE
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

    return subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=stdout,
        stderr=stderr,
        stdin=subprocess.DEVNULL,
        startupinfo=startupinfo,
        creationflags=creationflags,
        close_fds=True,
        shell=False,
    )

# =====================================================================================
# Manager del driver JDataGate
# =====================================================================================

class JDataGateManager:
    def __init__(self, exe_path: Optional[str] = None):
        self.exe_path = exe_path or DEFAULT_EXE
        self._proc: Optional[subprocess.Popen] = None

    # ----------------- Estado
    def is_installed(self) -> bool:
        return os.path.isfile(self.exe_path)

    def is_running(self) -> bool:
        # Preferimos tasklist por simplicidad (JDataGate no siempre expone PID accesible)
        return _tasklist_contains(IMAGE_NAME)

    # ----------------- Control
    def ensure_running(self, timeout: float = 3.0, show_console: bool = False, log_path: Optional[str] = None) -> bool:
        """Garantiza que el driver esté en ejecución. Si ya está, no hace nada.
        Devuelve True si está corriendo al final del método.
        """
        if not self.is_installed():
            raise FileNotFoundError(f"No se encuentra JDataGate en: {self.exe_path}")

        if self.is_running():
            return True

        # Lanzar
        exe_dir = os.path.dirname(self.exe_path) or None
        self._proc = _start_hidden([self.exe_path], cwd=exe_dir, show_console=show_console, log_path=log_path)

        # Esperar un poco a que inicialice
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.is_running():
                return True
            time.sleep(0.2)
        # Si no levantó, devolvemos False
        return self.is_running()

    def stop(self, force: bool = False) -> None:
        """Intenta cerrar JDataGate. Con `force=True` usa taskkill /F.
        Nota: JDataGate no siempre responde a señales; el `taskkill` suele ser lo más fiable.
        """
        if not self.is_running():
            return
        try:
            if force:
                subprocess.run(["taskkill", "/IM", IMAGE_NAME, "/F"], check=False)
            else:
                subprocess.run(["taskkill", "/IM", IMAGE_NAME], check=False)
        except Exception:
            pass

# =====================================================================================
# Cliente TCP opcional (para ping/comandos directos a balanza)
# =====================================================================================

STX = 0x02
ETX = 0x04
RESP_STX = 0x07

# --- helpers de logging ---
def _hex(b: bytes) -> str:
    return " ".join(f"{x:02X}" for x in b)

def _ascii_preview(b: bytes) -> str:
    return "".join(chr(x) if 32 <= x < 127 else "." for x in b)

def _parse_resp_status(resp: bytes):
    """Parser de status compatible con respuestas que comienzan con 'Cnn'.
    Retorna dict con: ok, code(str|None), data(bytes), body(bytes)
    """
    # 0x07 + BODY + CHK(2) + 0x04 ; BODY puede iniciar con 'Cnn'
    if not resp or resp[0] != RESP_STX or len(resp) < 4 or resp[-1] != ETX:
        return {"ok": False, "code": None, "data": b"", "body": b""}

    body = resp[1:-3]
    code_bytes = body[:2]
    data = body[2:]

    # Si viene 'C01' (o similar) primero, el código real va después
    if len(body) >= 5 and body[:1] == b"C" and all(48 <= x <= 57 for x in body[1:3]):
        code_bytes = body[3:5]
        data = body[5:]

    try:
        code = code_bytes.decode("ascii", "ignore")
    except Exception:
        code = None

    return {"ok": code == "01", "code": code, "data": data, "body": body}


def parse_ack_ok(resp: bytes) -> bool:
    """Algunos firmwares usan '03' como ACK además de '01'."""
    st = _parse_resp_status(resp)
    return bool(resp and st["code"] in ("01", "03"))

@dataclass
class TCPConfig:
    ip: str
    puerto: int = 1001
    timeout: float = 2.5

class KretzTCP:
    def __init__(self, ip: str, puerto: int, timeout: float = 2.5):
        self.ip = ip
        self.puerto = puerto
        self.timeout = timeout
        self._sock: Optional[socket.socket] = None
        self._logger = None

    def set_logger(self, logger):
        self._logger = logger

    def _log(self, msg: str):
        try:
            if self._logger:
                self._logger(msg)
        except Exception:
            pass

    def connect(self):
        if self._sock:
            return
        s = socket.create_connection((self.ip, self.puerto), timeout=self.timeout)
        s.settimeout(self.timeout)
        try:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            pass
        self._sock = s
        self._log(f"[tcp] connect {self.ip}:{self.puerto} timeout={self.timeout}")

    def close(self):
        try:
            if self._sock:
                self._sock.close()
        finally:
            self._sock = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def send(self, frame: bytes) -> bytes:
        self._log(f"[tcp] → {len(frame)} bytes | {_hex(frame)} | '{_ascii_preview(frame)}'")
        if not self._sock:
            with socket.create_connection((self.ip, self.puerto), timeout=self.timeout) as s:
                s.settimeout(self.timeout)
                try:
                    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except Exception:
                    pass
                s.sendall(frame)
                resp = self._recv_until_etx(s)
        else:
            self._sock.sendall(frame)
            resp = self._recv_until_etx(self._sock)
        self._log(f"[tcp] ← {len(resp)} bytes | {_hex(resp)} | '{_ascii_preview(resp)}'")
        st = _parse_resp_status(resp)
        if st["code"] is not None:
            self._log(f"[tcp] resp code={st['code']} ok={st['ok']} data_len={len(st['data'])}")
        return resp

    def _recv_until_etx(self, s: socket.socket) -> bytes:
        chunks: List[bytes] = []
        t0 = time.time()
        while True:
            try:
                b = s.recv(1024)
                if not b:
                    break
                chunks.append(b)
                if ETX in b:
                    break
            except socket.timeout:
                break
            if time.time() - t0 > self.timeout:
                break
        return b"".join(chunks)

    def send_many(self, frames: Iterable[bytes], inter_delay: float = 0.05, max_retries: int = 2) -> List[bytes]:
        resps: List[bytes] = []
        with self:
            for f in frames:
                intentos = 0
                while True:
                    self._log(f"[tcp] send_many frame #{len(resps)+1}")
                    resp = self.send(f)
                    if not (resp and resp[:1] == bytes([RESP_STX]) and resp[-1:] == bytes([ETX])):
                        self._log("[tcp] warn: resp inválida/timeout; reintentando..." if intentos <= max_retries else "[tcp] fail: sin resp válida")
                    if resp and resp[0] == RESP_STX and resp[-1] == ETX:
                        resps.append(resp)
                        break
                    intentos += 1
                    if intentos > max_retries:
                        resps.append(resp or b"")
                        break
                    time.sleep(0.1)
                if inter_delay:
                    time.sleep(inter_delay)
        return resps

    def ping(self, beep: bool = False) -> bool:
        cmd = b"0001" if beep else b"0002"
        body = b"C01" + cmd
        frame = bytes([STX]) + body + _checksum_bytes(bytes([STX]) + body) + bytes([ETX])
        resp = self.send(frame)
        return bool(resp and resp[0] == RESP_STX and resp[-1] == ETX)


# =====================================================================================
# Checksum util
# =====================================================================================

def _checksum_bytes(payload: bytes) -> bytes:
    total = sum(payload)
    b = total & 0xFF
    hi = (b >> 4) & 0x0F
    lo = b & 0x0F
    return bytes([hi + 0x30, lo + 0x30])

# =====================================================================================
# Helpers de frame y mappers (Protocolo Nx)
# =====================================================================================

import re

def _pad_num(val, n) -> bytes:
    s = "".join(ch for ch in str(val or "") if ch.isdigit())
    return s[-n:].rjust(n, "0").encode("ascii")


def _pad_str(val, n) -> bytes:
    s = (str(val or "")).strip()[:n].ljust(n, " ")
    return s.encode("latin1", "ignore")


def build_frame(cmd: str, payload: bytes, canal: str = "C03") -> bytes:
    """0x02 + canal('C03') + cmd + payload + CHK2 + 0x04"""
    body = canal.encode("ascii") + cmd.encode("ascii") + payload
    return bytes([STX]) + body + _checksum_bytes(bytes([STX]) + body) + bytes([ETX])


# Limpieza de nombre de dpto (evita duplicar el código en el nombre)

def _sanitize_depto_nombre(nombre: str, codigo3: str) -> str:
    n = (nombre or "").strip()
    c = (codigo3 or "").strip()
    c_no0 = c.lstrip("0") or "0"
    n = re.sub(rf"^\s*0*{re.escape(c_no0)}\s*[-:]\s*", "", n, flags=re.IGNORECASE)
    n = re.sub(rf"^\s*{re.escape(c)}\s*[-:]\s*", "", n, flags=re.IGNORECASE)
    return n[:16]


# ---------------- Departamentos ----------------

def cmd_departamento(codigo: str | int, nombre: str) -> bytes:
    c3 = _pad_num(codigo, 3).decode("ascii")
    nom = _sanitize_depto_nombre(nombre, c3)
    return build_frame("2003", _pad_num(c3, 3) + _pad_str(nom, 16))


def cmd_baja_departamento(codigo: str | int) -> bytes:
    return build_frame("3003", _pad_num(codigo, 3))


def cmd_vaciar_departamentos() -> bytes:
    return build_frame("4003", b"")


def cmd_leer_departamento(codigo: str | int) -> bytes:
    return build_frame("5003", _pad_num(codigo, 3))


def parse_resp_5003(resp: bytes) -> dict | None:
    st = _parse_resp_status(resp)
    if not (resp and st["code"] is not None):
        return None
    if st["ok"]:
        data = st["data"]
        try:
            cod = data[:3].decode("ascii", "ignore")
            nom = data[3:19].decode("latin1", "ignore").rstrip()
            return {"codigo": cod, "nombre": nom}
        except Exception:
            return None
    return None

# --- MONEDA (objeto 1010) ----------------------------------------------

def cmd_moneda_1010(nombre="PESOS", abrev="ARS", decimales=1):
    """
    Define la moneda de la balanza (decimales globales de precio/importe).
    """
    payload  = _pad_str(nombre, 15)     # Nombre moneda, ej: 'PESOS'
    payload += _pad_str(abrev, 3)       # Abreviatura, ej: 'ARS'
    payload += _pad_num(decimales, 3)   # Cantidad de decimales (precio global)
    payload += _pad_str('.', 1)         # Separador decimal (display/impresión)
    payload += _pad_str(',', 1)         # Separador de miles
    payload += _pad_num(decimales, 1)   # Decimales para PRECIO
    payload += _pad_num(decimales, 1)   # Decimales para IMPORTE
    return build_frame("1010", payload)




# ---------------- PLUs (bajas mas comunes) ----------------

def cmd_baja_plu(nro_plu: str | int) -> bytes:
    return build_frame("3005", _pad_num(nro_plu, 6))


def cmd_vaciar_plus() -> bytes:
    return build_frame("4005", b"")


# =====================================================================================
# Métodos de alto nivel en KretzTCP (kwargs por función)
# =====================================================================================

# Los definimos fuera y los bindeamos para no tocar la clase si no querés editarla.

def _ktcp_enviar_departamento(self, *, codigo, nombre) -> bool:
    resp = self.send(cmd_departamento(codigo, nombre))
    return parse_ack_ok(resp)


def _ktcp_baja_departamento(self, *, codigo) -> bool:
    resp = self.send(cmd_baja_departamento(codigo))
    return parse_ack_ok(resp)


def _ktcp_vaciar_departamentos(self) -> bool:
    resp = self.send(cmd_vaciar_departamentos())
    return parse_ack_ok(resp)


def _ktcp_leer_departamento(self, *, codigo) -> dict | None:
    resp = self.send(cmd_leer_departamento(codigo))
    return parse_resp_5003(resp)


def _ktcp_baja_plu(self, *, nro_plu) -> bool:
    resp = self.send(cmd_baja_plu(nro_plu))
    return parse_ack_ok(resp)


def _ktcp_vaciar_plus(self) -> bool:
    resp = self.send(cmd_vaciar_plus())
    return parse_ack_ok(resp)


def _ktcp_configurar_moneda(self, *, nombre="PESOS", abrev="ARS", decimales=1) -> bool:
    resp = self.send(cmd_moneda_1010(nombre=nombre, abrev=abrev, decimales=decimales))
    return parse_ack_ok(resp)



# Bind dinámico
KretzTCP.enviar_departamento = _ktcp_enviar_departamento
KretzTCP.baja_departamento = _ktcp_baja_departamento
KretzTCP.vaciar_departamentos = _ktcp_vaciar_departamentos
KretzTCP.leer_departamento = _ktcp_leer_departamento
KretzTCP.baja_plu = _ktcp_baja_plu
KretzTCP.vaciar_plus = _ktcp_vaciar_plus
KretzTCP.configurar_moneda = _ktcp_configurar_moneda


# =====================================================================================
# Wrappers con driver (para llamar directo desde la GUI)
# =====================================================================================

_JDG = None

def ensure_driver_running(show_console: bool = False) -> bool:
    global _JDG
    if _JDG is None:
        _JDG = JDataGateManager()
    return _JDG.ensure_running(show_console=show_console)


def driver_baja_departamento(*, ip: str, puerto: int = 1001, codigo: str | int,
                              timeout: float = 2.5, show_console: bool = True, on_trace=None) -> bool:
    ensure_driver_running(show_console=show_console)
    cli = KretzTCP(ip, puerto, timeout)
    if on_trace:
        cli.set_logger(on_trace)
    return cli.baja_departamento(codigo=codigo)


def driver_vaciar_departamentos(*, ip: str, puerto: int = 1001,
                                timeout: float = 2.5, show_console: bool = True, on_trace=None) -> bool:
    ensure_driver_running(show_console=show_console)
    cli = KretzTCP(ip, puerto, timeout)
    if on_trace:
        cli.set_logger(on_trace)
    return cli.vaciar_departamentos()


def driver_baja_plu(*, ip: str, puerto: int = 1001, nro_plu: str | int,
                     timeout: float = 2.5, show_console: bool = True, on_trace=None) -> bool:
    ensure_driver_running(show_console=show_console)
    cli = KretzTCP(ip, puerto, timeout)
    if on_trace:
        cli.set_logger(on_trace)
    return cli.baja_plu(nro_plu=nro_plu)


def driver_vaciar_plus(*, ip: str, puerto: int = 1001,
                        timeout: float = 2.5, show_console: bool = True, on_trace=None) -> bool:
    ensure_driver_running(show_console=show_console)
    cli = KretzTCP(ip, puerto, timeout)
    if on_trace:
        cli.set_logger(on_trace)
    return cli.vaciar_plus()
