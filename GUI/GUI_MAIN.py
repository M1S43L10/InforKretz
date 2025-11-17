# ============================
# FILE: GUI/GUI_MAIN.py
# ============================
import ttkbootstrap as ttk
from collections import defaultdict
from ttkbootstrap.constants import *
from Func.window_position import center_window
from GUI.abm_departamentos import VentanaDepartamentos
from GUI.kretz_driver import JDataGateManager
from db.dao_repo_sybase import RepoSybase as Repo
from .abm_equipos import VentanaEquipos
from GUI.kretz_adapter import KretzAdapter, EquipoDef
import tkinter.simpledialog as sd
import tkinter.messagebox as mb


def _to_int(v):
    try:
        return int(str(v).strip())
    except Exception:
        return None

class App(ttk.Window):
    def __init__(self):
        super().__init__(themename="flatly")  # podés cambiar el tema
        self.title("Gestión: ABM Equipos & Departamentos - Inforhard")
        center_window(self, 900, 520)  # usa tu util existente
        self._kretz =  KretzAdapter(
            base_dir=r"C:\Program Files (x86)\JDataGate\kSolutions\DataGate",
            exe_path=r"C:\Program Files (x86)\JDataGate\kSolutions\DataGate\JDataGate con consola.exe",
            workdir_mode="exe_dir",
        )


        self.repo = Repo()
        self._build_menu()
        self._build_home()

    def _build_menu(self):
        menubar = ttk.Menu(self)

        # --- ABMs (igual que antes) ---
        m_abm = ttk.Menu(menubar, tearoff=False)
        m_abm.add_command(label="ABM Equipos\t(Ctrl+E)", command=self._abrir_equipos)
        m_abm.add_command(label="ABM Departamentos\t(Ctrl+D)", command=self._abrir_departamentos)
        m_abm.add_separator()
        m_abm.add_command(label="Salir", command=self.destroy)
        menubar.add_cascade(label="ABMs", menu=m_abm)

        # --- NUEVO: Balanzas / Envíos ---
        m_bal = ttk.Menu(menubar, tearoff=False)
        m_bal.add_command(label="Testear conexiones\t(Ctrl+T)", command=self._testear_conexiones_balanzas)
        m_bal.add_command(label="Enviar Departamentos\t(Ctrl+Shift+D)", command=self._enviar_departamentos_balanzas)
        m_bal.add_command(label="Enviar Artículos\t(Ctrl+Shift+A)", command=self._enviar_articulos_balanzas)
        m_bal.add_command(label="Enviar formato de moneda", command=self._enviar_formato_moneda_un_decimal)
        m_bal.add_separator()
        m_bal.add_command(label="Testear y Enviar Todo\t(Ctrl+B)", command=self._enviar_dptos_y_articulos_balanzas)
        m_bal.add_command(label="Ver Modelo de Datos 5002", command=self._diagnosticar_modelo_balanzas)
        
        menubar.add_cascade(label="Balanzas", menu=m_bal)
        
        # dentro de _build_menu()
        m_bajas = ttk.Menu(menubar, tearoff=False)
        m_bajas.add_command(label="Vaciar Departamentos (Ctrl+Alt+Shift+D)", command=self._ui_vaciar_dptos)
        m_bajas.add_command(label="Vaciar PLUs (Ctrl+Alt+Shift+P)", command=self._ui_vaciar_plus)
        m_bajas.add_separator()
        m_bajas.add_command(label="Restaurar Modelo (1002) (Ctrl+Alt+R)", command=self._ui_restaurar_modelo)
        menubar.add_cascade(label="Bajas", menu=m_bajas)
        
        m_altas = ttk.Menu(menubar, tearoff=False)
        m_altas.add_command(label="Alta Departamento…", command=self._ui_alta_depto)
        m_altas.add_command(label="Alta Familia…", command=self._ui_alta_familia)
        m_altas.add_command(label="Alta PLU…", command=self._ui_alta_plu)
        menubar.add_cascade(label="Altas", menu=m_altas)

        self.config(menu=menubar)

        # Atajos existentes
        self.bind_all("<Control-e>", lambda e: self._abrir_equipos())
        self.bind_all("<Control-d>", lambda e: self._abrir_departamentos())

        # NUEVOS atajos
        self.bind_all("<Control-b>", lambda e: self._abrir_envios_balanzas())
        self.bind_all("<Control-t>", lambda e: self._testear_conexiones_balanzas())
        self.bind_all("<Control-Shift-D>", lambda e: self._enviar_departamentos_balanzas())
        self.bind_all("<Control-Shift-A>", lambda e: self._enviar_articulos_balanzas())
        self.bind_all("<Control-Alt-d>", lambda e: self._ui_baja_depto())
        self.bind_all("<Control-Alt-p>", lambda e: self._ui_baja_plu())
        self.bind_all("<Control-Alt-Shift-D>", lambda e: self._ui_vaciar_dptos())
        self.bind_all("<Control-Alt-Shift-P>", lambda e: self._ui_vaciar_plus())
        self.bind_all("<Control-Alt-r>", lambda e: self._ui_restaurar_modelo())
        
    def _cerrar_driver(self, forzar=False):
        if hasattr(self, "_jdg"):
            self._jdg.stop(force=forzar)


    def _build_home(self):
        frm = ttk.Frame(self, padding=20)
        frm.pack(fill=BOTH, expand=True)
        ttk.Label(frm, text="Bienvenido", font=("TkDefaultFont", 18, "bold")).pack(anchor=W)
        ttk.Label(frm, text="Usá el menú ABMs para gestionar Equipos y Departamentos.").pack(anchor=W, pady=(6,0))

        # Resumen rápido
        cards = ttk.Frame(frm)
        cards.pack(fill=X, pady=20)
        self.lbl_eq = ttk.Label(cards, text=f"Equipos: {len(self.repo.equipos)}", bootstyle=INFO)
        self.lbl_dp = ttk.Label(cards, text=f"Departamentos: {len(self.repo.departamentos)}", bootstyle=SUCCESS)
        self.lbl_eq.grid(row=0, column=0, sticky=W, padx=(0,20))
        self.lbl_dp.grid(row=0, column=1, sticky=W)

        ttk.Button(frm, text="Abrir ABM Equipos", bootstyle=PRIMARY, command=self._abrir_equipos).pack(anchor=W)
        ttk.Button(frm, text="Abrir ABM Departamentos", command=self._abrir_departamentos).pack(anchor=W, pady=6)

    def _abrir_equipos(self):
        win = VentanaEquipos(self, self.repo)
        win.wait_window()
        self._refresh_home()

    def _abrir_departamentos(self):
        def _refresh():
            # actualizar combos en ventanas de equipos abiertas
            for w in self.winfo_children():
                if isinstance(w, VentanaEquipos):
                    w._refresh_combos()
        win = VentanaDepartamentos(self, self.repo, on_change=_refresh)
        win.wait_window()
        self._refresh_home()

    def _refresh_home(self):
        self.lbl_eq.configure(text=f"Equipos: {len(self.repo.equipos)}")
        self.lbl_dp.configure(text=f"Departamentos: {len(self.repo.departamentos)}")
        
        
    def _get_envios_service(self):
        if not hasattr(self, "_envios_svc"):
            from GUI.envios_balanzas import EnvioBalanzasService
            self._envios_svc = EnvioBalanzasService(repo=self.repo, timeout=2.5)
        return self._envios_svc

    def _progress_envios(self, ev, data):
        # Cambiá por tu logger/console de la app
        print(ev, data)

    def _testear_conexiones_balanzas(self):
        svc = self._get_envios_service()
        ok_equipos = svc.testear_conexiones(on_progress=self._progress_envios, beep=False)
        try:
            import tkinter.messagebox as mb
            mb.showinfo("Balanzas", f"Equipos con conexión OK: {len(ok_equipos)}")
        except Exception:
            pass


    def _enviar_departamentos_balanzas(self):

        # === Mapa código->nombre (3 dígitos) ===
        dep_map = {
            str((d.get("codigo") if isinstance(d, dict) else getattr(d, "codigo", "")))[-3:].rjust(3, "0"):
            ((d.get("nombre") if isinstance(d, dict) else getattr(d, "nombre", "")) or "")
            for d in (self.repo.departamentos or [])
        }

        equipos = list(self.repo.equipos or [])

        # === RELACIONES equipo->depto ===
        rel_rows = None
        for attr in ("equipos_deptos", "equipos_dptos", "rel_equipos_deptos", "rel_dptos_equipos"):
            val = getattr(self.repo, attr, None)
            if callable(val):
                try:
                    val = val()
                except Exception:
                    val = None
            if isinstance(val, list) and val:
                rel_rows = val
                break

        if rel_rows is None and hasattr(self.repo, "eqd"):
            try:
                rel_rows = self.repo.eqd.listar_todo()
                self._progress_envios("trace", {"equipo": "GLOBAL", "msg": f"rel_rows={len(rel_rows)} filas"})
            except Exception as ex:
                self._progress_envios("trace", {"equipo": "GLOBAL", "msg": f"eqd.listar_todo() error: {ex!r}"})
                rel_rows = []

        # === mapa equipo_id(int) -> [códigos '000', '001', ...] ===
        from collections import defaultdict
        dpxeq: dict[int, list[str]] = defaultdict(list)

        def _to_int(x):
            try:
                return int(x) if x is not None and str(x).strip() != "" else None
            except Exception:
                return None

        for r in (rel_rows or []):
            if isinstance(r, dict):
                eq_id = r.get("equipo_id") or r.get("id_equipo") or r.get("equipo")
                depc  = r.get("cgrpconta")  or r.get("codigo")    or r.get("cod_depto")
            else:
                eq_id = getattr(r, "equipo_id", None) or getattr(r, "id_equipo", None) or getattr(r, "equipo", None)
                depc  = getattr(r, "cgrpconta", None)  or getattr(r, "codigo", None)     or getattr(r, "cod_depto", None)

            k = _to_int(eq_id)
            if k is None or depc is None:
                continue
            c3 = str(depc)[-3:].rjust(3, "0")
            if c3 not in dpxeq[k]:
                dpxeq[k].append(c3)

        # === Enviar por equipo usando KretzAdapter ===
        for e in equipos:
            if isinstance(e, dict):
                eid    = _to_int(e.get("id"))
                nombre = e.get("nombre", "")
                ip     = e.get("ip")
                puerto = int(e.get("puerto", 1001) or 1001)
                habil  = e.get("habilitado", 1)
            else:
                eid    = _to_int(getattr(e, "id", None))
                nombre = getattr(e, "nombre", "")
                ip     = getattr(e, "ip", None)
                puerto = int(getattr(e, "puerto", 1001) or 1001)
                habil  = getattr(e, "habilitado", True)

            if not habil:
                self._progress_envios("info", {"equipo": nombre, "msg": "Saltado (no habilitado)"})
                continue

            deptos = dpxeq.get(eid, [])
            if not deptos:
                self._progress_envios("trace", {"equipo": nombre, "msg": f"eid={eid} sin deptos. mapa={dict(dpxeq)}"})
                self._progress_envios("info", {"equipo": nombre, "msg": "Sin deptos asignados"})
                continue

            items = [{"codigo": c, "nombre": (dep_map.get(c) or f"DEP-{c}")} for c in deptos]

            # Construimos el EquipoDef para el adaptador
            eqdef = EquipoDef(
                nombre=nombre,
                ip=ip,
                puerto=puerto,
                id_equipo=1,  # el INFO usa 2 dígitos; en TCP/IP el equipo real se toma de COM.JDG (IP/puerto)
                idioma="00",
            )

            try:
                # Enviar todas las líneas 2003 en un INFO.JDG (el adaptador arma y despacha)
                self._kretz.enviar_departamentos(
                    eqdef,
                    items=items,
                    show_console=True,  # cambialo a False si no querés ventana del exe
                    on_progress=self._progress_envios
                )
                self._progress_envios("info", {"equipo": nombre, "msg": f"Departamentos enviados: {len(items)}"})
            except Exception as ex:
                self._progress_envios("error", {"equipo": nombre, "msg": repr(ex)})


    def _enviar_articulos_balanzas(self):
        # 1) Equipos y relaciones equipo->deptos (ya lo hacés para departamentos)
        equipos = list(self.repo.equipos or [])
        from collections import defaultdict
        dpxeq: dict[int, list[str]] = defaultdict(list)

        # preferimos leer desde la propia repo (se armó en _refresh)
        rel_map = { int(e["id"]): [str(d)[-3:].rjust(3,"0") for d in (e.get("deptos") or []) ]
                    for e in equipos if "id" in e }

        # 2) Por cada equipo, buscar artículos de SUS deptos y enviar
        for e in equipos:
            eid    = int(e.get("id"))
            nombre = e.get("nombre","")
            ip     = e.get("ip")
            puerto = int(e.get("puerto", 1001) or 1001)

            deptos = rel_map.get(eid, [])
            if not deptos:
                self._progress_envios("info", {"equipo": nombre, "msg": "Sin deptos asignados → sin artículos"})
                continue

            # 2.1) Traer artículos por deptos (solo CGRPCONTA, sin familias)
            rows = self.repo.articulos_por_deptos(deptos)
            if not rows:
                self._progress_envios("info", {"equipo": nombre, "msg": f"Sin artículos en deptos {deptos}"})
                continue

            # 2.2) Construir EquipoDef y enviar en bloque
            eqdef = EquipoDef(nombre=nombre, ip=ip, puerto=puerto, id_equipo=1, idioma="00")
            try:
                self._kretz.enviar_plus(
                    eqdef,
                    items=rows,
                    cod_familia_def=1,  # FAMILIA FIJA (no se usa del origen)
                    show_console=True,
                    on_progress=self._progress_envios
                )
                self._progress_envios("info", {"equipo": nombre, "msg": f"Artículos enviados: {len(rows)}"})
            except Exception as ex:
                self._progress_envios("error", {"equipo": nombre, "msg": repr(ex)})
                
    def _enviar_dptos_y_articulos_balanzas(self):
        equipos = list(self.repo.equipos or [])
        for e in equipos:
            nombre = e.get("nombre", "")
            ip     = e.get("ip")
            puerto = int(e.get("puerto", 1001) or 1001)

            deptos = [str(d).rjust(3, "0") for d in (e.get("deptos") or [])]
            if not deptos:
                self._progress_envios("info", {"equipo": nombre, "msg": "Sin dptos asignados → se salta"})
                continue

            rows = self.repo.articulos_por_deptos(deptos)
            if not rows:
                self._progress_envios("info", {"equipo": nombre, "msg": f"Sin artículos en dptos {deptos}"})
                continue

            dpt_defs = [{"codigo": d, "nombre": f"DEP {d}"} for d in deptos]
            eqdef = EquipoDef(nombre=nombre, ip=ip, puerto=puerto, id_equipo=1, idioma="00")

            try:
                # 1) Código de barras (prefijos/formato)
                self._kretz.configurar_codbarra_1070(
                    eqdef,
                    inicio_pesable="20",
                    incluir_peso_en_cb=False,
                    inicio_no_pesable="20",
                    incluir_unidades_en_cb=False,
                    formato=1,
                    show_console=True,
                    on_progress=self._progress_envios
                )

                # 2) Moneda alternativa con 1 decimal y seleccionarla
                self._kretz.configurar_moneda_2026(
                    eqdef,
                    moneda=2,
                    dec_precio=1,   # << 1 DECIMAL
                    dec_peso=3,
                    show_console=True,
                    on_progress=self._progress_envios
                )
                self._kretz.seleccionar_moneda_1010(
                    eqdef,
                    moneda=2,       # activar la 02
                    show_console=True,
                    on_progress=self._progress_envios
                )

                # 3) Enviar dptos + artículos (2005 con dec_prec=0 para usar la moneda)
                self._kretz.enviar_dptos_y_articulos(
                    eqdef,
                    deptos=dpt_defs,
                    articulos=rows,
                    cod_familia_def=0,
                    # si tu método permite kwargs al builder:
                    # builder_kwargs={"dec_prec": 0},
                    show_console=True,
                    on_progress=self._progress_envios,
                    allow_retry=False,
                    timeout=30.0
                )

                self._progress_envios("info", {"equipo": nombre, "msg": f"Dptos+Artículos enviados: {len(dpt_defs)} + {len(rows)}"})
            except Exception as ex:
                self._progress_envios("error", {"equipo": nombre, "msg": repr(ex)})
    
    def _enviar_formato_moneda_un_decimal(self):
        """Envía a todas las balanzas el formato de moneda con 1 decimal."""
        if not mb.askyesno("Confirmar", "¿Configurar todas las balanzas con 1 decimal en precios?"):
            return

        for eq in self._iter_equipos_ok_as_eqdefs():
            try:
                # Solo ajusta la moneda (no toca artículos)
                self._kretz.configurar_moneda_2026(
                    eq,
                    moneda=2,        # Moneda alternativa
                    dec_precio=1,    # 1 decimal en precios
                    dec_peso=3,      # 3 decimales en peso (por defecto)
                    show_console=True,
                    on_progress=self._progress_envios
                )
                self._kretz.seleccionar_moneda_1010(
                    eq,
                    moneda=2,
                    show_console=True,
                    on_progress=self._progress_envios
                )
                self._progress_envios("info", {"equipo": eq.nombre, "msg": "Moneda configurada con 1 decimal"})
            except Exception as ex:
                self._progress_envios("error", {"equipo": eq.nombre, "msg": repr(ex)})

        mb.showinfo("Balanzas", "Formato de moneda configurado en todas las balanzas OK.")




    def _abrir_envios_balanzas(self):
        # flujo “todo en uno”
        self._testear_conexiones_balanzas()
        self._enviar_departamentos_balanzas()
        self._enviar_articulos_balanzas()
        
        
    def _diagnosticar_modelo_balanzas(self):
        equipos = list(self.repo.equipos or [])
        for e in equipos:
            eqdef = EquipoDef(
                nombre=e.get("nombre",""),
                ip=e.get("ip"),
                puerto=int(e.get("puerto",1001) or 1001),
                id_equipo=1, idioma="00"
            )
            try:
                res = self._kretz.diagnosticar_modelo(
                    eqdef,
                    campos_plu=32, campos_depto=8,
                    show_console=True,
                    on_progress=self._progress_envios
                )
                self._progress_envios("info", {
                    "equipo": e.get("nombre",""),
                    "msg": f"Modelo PLU={res['plu']['__total__']} chars, DEPTO={res['depto']['__total__']} chars"
                })
            except Exception as ex:
                self._progress_envios("error", {"equipo": e.get("nombre",""), "msg": repr(ex)})


    def _ui_baja_plu(self):
        plu = sd.askstring("Baja PLU", "Nro PLU (6 dígitos):")
        if not plu: return
        if not mb.askyesno("Confirmar", f"¿Borrar PLU {plu} en TODOS los equipos OK?"):
            return
        svc = self._get_bajas_service()
        for eq in self._equipos_ok():
            ok = svc.baja_plu(eq, plu, on_progress=self._progress_envios)
        mb.showinfo("Bajas", "Listo.")

    """def _ui_vaciar_dptos(self):
        if not mb.askyesno("Peligroso", "¿Vaciar TODOS los departamentos en TODOS los equipos OK?"):
            return
        svc = self._get_bajas_service()
        for eq in self._equipos_ok():
            svc.vaciar_departamentos(eq, on_progress=self._progress_envios)
        mb.showinfo("Bajas", "Departamentos vaciados.")"""
    
    # arriba del archivo (o junto a los demás imports):


    def _iter_equipos_ok_as_eqdefs(self):
        # Usa tu helper existente para testear
        svc = self._get_envios_service()
        if not getattr(svc, "_equipos_ok", None):
            svc.testear_conexiones(on_progress=self._progress_envios)
        # Convertimos a EquipoDef (adapter)
        eqdefs = []
        for e in svc._equipos_ok:
            eqdefs.append(
                EquipoDef(
                    nombre=getattr(e, "nombre", ""),
                    ip=getattr(e, "ip", ""),
                    puerto=int(getattr(e, "puerto", 1001) or 1001),
                    id_equipo=1,  # El INFO exige ID(2). TCP/IP ignora, pero va en formato.
                    idioma="00",
                )
            )
        return eqdefs

    def _ui_vaciar_dptos(self):
        if not mb.askyesno("Peligroso", "¿Vaciar TODOS los departamentos en TODOS los equipos OK?"):
            return
        for eq in self._iter_equipos_ok_as_eqdefs():
            try:
                self._kretz.vaciar_departamentos(
                    eq,
                    show_console=True,
                    on_progress=self._progress_envios,
                )
                self._progress_envios("info", {"equipo": eq.nombre, "msg": "Vaciar departamentos enviado"})
            except Exception as ex:
                self._progress_envios("error", {"equipo": eq.nombre, "msg": repr(ex)})
        mb.showinfo("Bajas", "Departamentos vaciados.")


    def _ui_vaciar_plus(self):
        if not mb.askyesno("Peligroso", "¿Vaciar TODOS los PLUs en TODOS los equipos OK?"):
            return
        for eq in self._iter_equipos_ok_as_eqdefs():
            try:
                self._kretz.vaciar_plus(
                    eq,
                    show_console=True,
                    on_progress=self._progress_envios,
                )
                self._progress_envios("info", {"equipo": eq.nombre, "msg": "Vaciar PLUs enviado"})
            except Exception as ex:
                self._progress_envios("error", {"equipo": eq.nombre, "msg": repr(ex)})
        mb.showinfo("Bajas", "PLUs vaciados.")

    def _ui_restaurar_modelo(self):
        if not mb.askyesno("MUY PELIGROSO", "¿Restaurar modelo (1002) en TODOS los equipos OK?"):
            return
        svc = self._get_bajas_service()
        for eq in self._equipos_ok():
            svc.restaurar_modelo(eq, on_progress=self._progress_envios)
        mb.showinfo("Bajas", "Restauración enviada.")
        
        
    def _ui_alta_depto(self):
        codigo = sd.askstring("Alta Depto", "Código (3 dígitos):")
        if not codigo: return
        nombre = sd.askstring("Alta Depto", "Nombre (máx 16):") or ""
        for eq in self._iter_equipos_ok_as_eqdefs():
            try:
                self._kretz.alta_departamento(
                    eq, codigo=codigo, nombre=nombre,
                    show_console=True,
                    on_progress=self._progress_envios
                )
                self._progress_envios("info", {"equipo": eq.nombre, "msg": f"Alta depto {codigo}-{nombre}"})
            except Exception as ex:
                self._progress_envios("error", {"equipo": eq.nombre, "msg": repr(ex)})
        mb.showinfo("Altas", "Departamento enviado a los equipos OK.")
        
    def _ui_alta_familia(self):
        dep = sd.askstring("Alta Familia", "Código Depto (3):")
        if not dep: return
        fam = sd.askstring("Alta Familia", "Código Familia (3):")
        if not fam: return
        nombre = sd.askstring("Alta Familia", "Nombre (máx 16):") or ""
        for eq in self._iter_equipos_ok_as_eqdefs():
            try:
                self._kretz.alta_familia(
                    eq, cod_depto=dep, cod_familia=fam, nombre=nombre,
                    show_console=True,
                    on_progress=self._progress_envios
                )
                self._progress_envios("info", {"equipo": eq.nombre, "msg": f"Alta familia D{dep}-F{fam} {nombre}"})
            except Exception as ex:
                self._progress_envios("error", {"equipo": eq.nombre, "msg": repr(ex)})
        mb.showinfo("Altas", "Familia enviada a los equipos OK.")


    def _ui_alta_plu(self):
        nro    = sd.askstring("Alta PLU", "PLU (6 dígitos):")
        if not nro: return
        dep    = sd.askstring("Alta PLU", "Depto (3):") or "001"
        fam    = sd.askstring("Alta PLU", "Familia (3):") or "001"
        nombre = sd.askstring("Alta PLU", "Nombre (máx 26):") or ""
        precio = sd.askstring("Alta PLU", "Precio (####### sin coma):") or "0"

        for eq in self._iter_equipos_ok_as_eqdefs():
            try:
                self._kretz.alta_plu(
                    eq,
                    nro_plu=nro,
                    cod_depto=dep,
                    cod_familia=fam,
                    nombre=nombre,
                    precio=precio,
                    # Dejá lo demás por defecto o podés pedir más campos aquí
                    show_console=True,
                    on_progress=self._progress_envios
                )
                self._progress_envios("info", {"equipo": eq.nombre, "msg": f"Alta PLU {nro} ({nombre})"})
            except Exception as ex:
                self._progress_envios("error", {"equipo": eq.nombre, "msg": repr(ex)})
        mb.showinfo("Altas", "PLU enviado a los equipos OK.")






def run_app():
    App().mainloop()