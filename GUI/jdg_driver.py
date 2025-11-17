# jdg_driver.py
import os, subprocess, pathlib, threading
import time as _t

class JDataGateInstance:
    def __init__(self, workdir: str, exe_path: str,
                eq_id: str = "01", eq_type: str = "C",
                retries: int = 1, idioma: str = "00"):
        self.workdir = pathlib.Path(workdir)
        self.exe_path = exe_path
        self.eq_id = f"{int(eq_id):02d}"
        self.eq_type = eq_type
        self.retries = int(retries)
        self.idioma = idioma
        self.proc = None
        self.lock = threading.Lock()
        self.workdir.mkdir(parents=True, exist_ok=True)

        # <<< IMPORTANTE: inicializar acá, no fuera de métodos
        self._peer_ip = None
        self._peer_port = None

    def _stop(self):
        try:
            if self.proc and self.proc.poll() is None:
                self.proc.terminate()
                self.proc.wait(timeout=2.0)
        except Exception:
            pass
        finally:
            self.proc = None

    # --- Reemplazar JDataGateInstance.apply_peer por esta versión ---
    def apply_peer(self, ip: str, puerto: int, show_console: bool = False):
        """
        Escribe COM.JDG con ip/puerto. NO arranca el EXE aquí si el peer cambió;
        el arranque debe realizarse después de asegurarse de que INFO/EXT/CONF existen
        y estén limpios (evita condiciones de carrera).
        """
        ip = str(ip or "").strip()
        puerto = int(puerto or 0)
        changed = (ip != self._peer_ip) or (puerto != self._peer_port)

        # siempre escribimos COM para asegurarnos
        self.write_com_tcp(ip, puerto)

        if changed:
            # si cambió, detenemos el proceso para que no lea archivos a medias;
            # el arranque (ensure_running) lo hace el pool después de crear INFO/EXT
            self._stop()
            # no arrancamos aquí: el pool invocará ensure_running() en el momento seguro
            self._peer_ip, self._peer_port = ip, puerto


    # ---------- Archivos base ----------
    def write_conf(self, idioma: str | None = None):
        """Escribe CONF.JDG con el idioma (00/01). Si se pasa idioma, actualiza self.idioma."""
        if idioma is not None:
            self.idioma = str(idioma)
        (self.workdir / "CONF.JDG").write_text(f"{self.idioma}\r\n", encoding="latin-1")

    def write_com_tcp(self, ip: str, puerto: int):
        linea = f"\"{self.eq_id}\",\"{self.eq_type}\",\"{self.retries}\",\"TCP\",\"{ip}\",\"{int(puerto)}\"\r\n"
        (self.workdir / "COM.JDG").write_text(linea, encoding="latin-1")

    def ensure_ext_clear(self):
        p = self.workdir / "EXT.JDG"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="latin-1", newline="") as f:
            f.write("")
            f.flush(); os.fsync(f.fileno())
        return p
    

    def ensure_wait_clear(self):
        p = self.workdir / "WAIT.APP"
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass
        return p

    def clear_info(self):
        p = self.workdir / "INFO.JDG"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="latin-1", newline="") as f:
            f.write("")
            f.flush(); os.fsync(f.fileno())
        # “tocar” mtime para que el EXE detecte cambio
        ts = _t.time()
        os.utime(p, (ts, ts))
        return p

    def ensure_info(self):
        p = self.workdir / "INFO.JDG"
        if not p.exists():
            p.write_text("", encoding="latin-1")
        return p

    # ---------- Lanzamiento ----------
    def start(self, show_console: bool = True):
        if self.proc and self.proc.poll() is None:
            return

        creationflags = 0
        if os.name == "nt":
            if show_console:
                creationflags = 0x00000010   # CREATE_NEW_CONSOLE  -> fuerza una nueva consola visible
            else:
                creationflags = 0x08000000   # CREATE_NO_WINDOW    -> sin ventana

        self.proc = subprocess.Popen(
            [self.exe_path],
            cwd=str(self.workdir),
            creationflags=creationflags
        )
        _t.sleep(0.3)


    def is_running(self) -> bool:
        return (self.proc is not None) and (self.proc.poll() is None)

    def ensure_running(self, show_console: bool = True):
        self.start(show_console=show_console)

    # ---------- Helpers de formato ----------
    def mk_cmd(self, comando: str, datos: str = "") -> str:
        """Construye la línea para INFO.JDG: C + ID(2) + comando(4) + datos (ASCII)."""
        return f"C{self.eq_id}{comando}{datos}"

    # ---------- Envío / lectura ----------
    def send_info_lines(self, lines: list[str], overwrite: bool = True):
        info_path = self.workdir / "INFO.JDG"

        # contenido nuevo (CRLF)
        content = "".join(((l if l.endswith("\r\n") else (l + "\r\n")) for l in lines))
        data = content.encode("latin-1", errors="strict")

        # mtime previo (para empujar +1s si cae en el mismo segundo)
        prev_mtime_sec = None
        try:
            prev_mtime_sec = int(info_path.stat().st_mtime)
        except FileNotFoundError:
            pass

        # escribir IN-PLACE: abrir, seek(0), write, truncate, flush, fsync
        info_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "r+b" if info_path.exists() else "wb"
        with open(info_path, mode) as f:
            f.seek(0)
            f.write(data)
            f.truncate()
            f.flush()
            os.fsync(f.fileno())

        # asegurar mtime estrictamente mayor (granularidad de 1s del driver)
        ts = _t.time()
        if prev_mtime_sec is not None and int(ts) <= int(prev_mtime_sec):
            ts = prev_mtime_sec + 1.1
        os.utime(info_path, (ts, ts))

        _t.sleep(0.05)  # pequeño respiro para el watcher del driver




    def wait_for_ext_growth(self, prev_size: int, timeout: float = 5.0) -> bool:
        """Espera que EXT.JDG crezca respecto de prev_size (NO truncar aquí)."""
        ext = self.workdir / "EXT.JDG"
        t0 = _t.time()
        while _t.time() - t0 <= timeout:
            if ext.exists() and ext.stat().st_size > prev_size:
                return True
            _t.sleep(0.05)
        return False

        
        # --- PATHS útiles ---
    def paths(self) -> dict:
        return {
            "workdir": str(self.workdir),
            "CONF": str(self.workdir / "CONF.JDG"),
            "COM":  str(self.workdir / "COM.JDG"),
            "INFO": str(self.workdir / "INFO.JDG"),
            "EXT":  str(self.workdir / "EXT.JDG"),
        }

    # --- Lectores crudos (texto) ---
    def read_conf_text(self) -> str:
        p = self.workdir / "CONF.JDG"
        try:    return p.read_text(encoding="latin-1", errors="replace")
        except FileNotFoundError: return ""

    def read_com_text(self) -> str:
        p = self.workdir / "COM.JDG"
        try:    return p.read_text(encoding="latin-1", errors="replace")
        except FileNotFoundError: return ""

    def read_info_text(self) -> str:
        p = self.workdir / "INFO.JDG"
        try:    return p.read_text(encoding="latin-1", errors="replace")
        except FileNotFoundError: return ""

    def read_ext_text(self) -> str:
        p = self.workdir / "EXT.JDG"
        try:    return p.read_text(encoding="latin-1", errors="replace")
        except FileNotFoundError: return ""

    # --- Lectores en líneas (con .rstrip de CR/LF) ---
    def read_info_lines(self) -> list[str]:
        t = self.read_info_text()
        return [ln.rstrip("\r\n") for ln in t.splitlines()] if t else []

    def read_ext_lines(self) -> list[str]:
        t = self.read_ext_text()
        return [ln.rstrip("\r\n") for ln in t.splitlines()] if t else []

    # --- Parser COM (1ra línea CSV entrecomillada) ---
    def read_com(self) -> dict | None:
        raw = self.read_com_text().strip()
        if not raw:
            return None
        import csv, io
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            row = next(csv.reader(io.StringIO(line)))
            # Esperado: "01","C","3","TCP","192.168.1.41","1001"
            d = {}
            try:
                d["id"]       = row[0].strip('"')
                d["tipo"]     = row[1].strip('"')
                d["retries"]  = int(row[2].strip('"'))
                d["transp"]   = row[3].strip('"')
                d["ip"]       = row[4].strip('"')
                d["puerto"]   = int(row[5].strip('"'))
            except Exception:
                pass
            return d or None
        return None

    # --- Dump completo (texto + paths + COM parseado) ---
    def dump_all(self) -> dict:
        return {
            "paths": self.paths(),
            "CONF_text": self.read_conf_text(),
            "COM_text":  self.read_com_text(),
            "COM":       self.read_com(),
            "INFO_text": self.read_info_text(),
            "INFO_lines": self.read_info_lines(),
            "EXT_text":  self.read_ext_text(),
            "EXT_lines": self.read_ext_lines(),
        }


    # Parser tolerante: acepta respuesta con o sin 0x07/0x04
    def parse_ext_responses(self) -> list[str]:
        raw = self.read_ext_text()
        # separo por líneas si el EXE inserta CRLF por respuesta; si no, devuelvo como 1 bloque
        lines = [l for l in raw.splitlines() if l.strip()]
        return lines if lines else ([raw] if raw.strip() else [])
    
    # --- LOG del driver: JDataGate.log o LOG.JDG ---
    def read_driver_log_tail(self, lines: int = 200, max_bytes: int = 262144) -> list[str]:
        """
        Lee el final del log del driver (JDataGate.log o LOG.JDG) y devuelve las últimas 'lines' líneas.
        No modifica INFO/EXT ni lanza el EXE.
        """
        import io

        cand = [
            self.workdir / "JDataGate.log",
            self.workdir / "LOG.JDG",
            self.workdir / "log.txt",
        ]
        path = next((p for p in cand if p.exists()), None)
        if not path:
            return []

        try:
            with open(path, "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - max_bytes), 0)
                data = f.read()
            txt = data.decode("latin-1", errors="replace")
            arr = txt.splitlines()
            return arr[-lines:] if len(arr) > lines else arr
        except Exception:
            return []


# Un pool para manejar "una instancia por IP"
class JDataGatePool:
    def __init__(self, base_dir: str, exe_path: str, workdir_mode: str = "ip_subdirs"):
        self.base_dir = pathlib.Path(base_dir)
        self.exe_path = exe_path
        self.exe_dir = pathlib.Path(exe_path).parent
        self.workdir_mode = workdir_mode  # "ip_subdirs" | "exe_dir"
        self.by_key = {}
        self.global_lock = threading.Lock()

    # --- Reemplazar el bloque de creación dentro de JDataGatePool.get por esta lógica ---
    def get(self, ip: str, puerto: int, idioma: str = "00", retries: int = 3,
                show_console: bool = False, no_touch: bool = False) -> JDataGateInstance:

        # Definir key y workdir según el modo
        if self.workdir_mode == "exe_dir":
            key = "__single__"
            workdir = self.exe_dir                
        else:
            key = ip
            workdir = self.base_dir / ip.replace(":", "_")

        inst = self.by_key.get(key)

        if inst is None:
            inst = JDataGateInstance(str(workdir), self.exe_path, idioma=idioma, retries=retries)
            self.by_key[key] = inst

            if not no_touch:
                # 1) escribir CONF y COM (o COM tcp) y preparar archivos antes de arrancar
                inst.write_conf(idioma)
                if self.workdir_mode == "exe_dir":
                    # write COM but don't start the EXE here
                    inst.write_com_tcp(ip, puerto)
                else:
                    inst.write_com_tcp(ip, puerto)

                # 2) crear/limpiar EXT e INFO (ARCHIVOS) antes de arrancar
                inst.ensure_ext_clear()
                inst.ensure_info()

                # brevemente esperar a que el FS tenga los archivos listos
                import time
                _t.sleep(0.05)

                # 3) ahora sí arrancar (o aplicar peer y arrancar si exe_dir)
                if self.workdir_mode == "exe_dir":
                    # apply_peer already wrote COM above and we stopped any running proc;
                    # ahora arrancamos para que el EXE vea INFO/EXT/CONF listos
                    inst.ensure_running(show_console=show_console)
                    # actualizar peer fields
                    inst._peer_ip, inst._peer_port = ip, puerto
                else:
                    inst.ensure_running(show_console=show_console)

        else:
            # instancia existente: aseguramos estado y archivos en el orden correcto
            if not no_touch:
                inst.write_conf(idioma)
                # actualizar COM (no arrancamos aquí)
                if self.workdir_mode == "exe_dir":
                    inst.write_com_tcp(ip, puerto)
                    # si cambió peer -> reiniciar pero después de crear INFO/EXT
                    if (ip != inst._peer_ip) or (int(puerto) != int(inst._peer_port)):
                        inst._stop()
                else:
                    inst.write_com_tcp(ip, puerto)
                    if not inst.is_running():
                        # prepare files before starting
                        inst.ensure_ext_clear()
                        inst.ensure_info()
                        _t.sleep(0.05)
                        inst.ensure_running(show_console=show_console)

                # siempre aseguramos que INFO/EXT existen y están limpios
                inst.ensure_ext_clear()
                inst.ensure_info()

                # si usamos exe_dir y no está corriendo, arrancar ahora
                if self.workdir_mode == "exe_dir" and not inst.is_running():
                    _t.sleep(0.05)
                    inst.ensure_running(show_console=show_console)

                # actualizar peer cache
                inst._peer_ip, inst._peer_port = ip, puerto

        return inst
