# GUI/abm_equipos.py
import ipaddress
import tkinter as tk
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import messagebox
from Func.window_position import center_window


PANEL_BG = "#F4F7FB"  # fondo suave para diferenciar el bloque de deptos


class _ScrollFrame(ttk.Frame):
    """Contenedor scrollable simple para listas de widgets (checkboxes)."""
    def __init__(self, master, height=180, bg=None, **kwargs):
        super().__init__(master, **kwargs)
        self._bg = bg

        self.canvas = tk.Canvas(self, highlightthickness=0, height=height,
                                background=(bg or ""))
        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vsb.set)

        # estilo para frame interno (para heredar color de fondo)
        style = ttk.Style()
        if bg:
            style.configure("DeptoInner.TFrame", background=bg)

        self.inner = ttk.Frame(self.canvas, style="DeptoInner.TFrame" if bg else None)
        self.window_id = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")

        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.vsb.grid(row=0, column=1, sticky="ns")

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.inner.bind("<Configure>", self._on_inner_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

    def _on_inner_configure(self, _event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event=None):
        canvas_width = event.width if event else self.canvas.winfo_width()
        self.canvas.itemconfig(self.window_id, width=canvas_width)


class VentanaEquipos(ttk.Toplevel):
    """
    ABM Equipos con:
    - Checkboxes de Departamentos (multi)
    - Checkbox Autoreport (booleano)

    Requiere un `repo` con:
      - repo.departamentos -> list[{"codigo": str, "nombre": str}]
      - repo.equipos -> list[{ "nombre": str, "ip": str, "puerto": int,
                               "deptos": list[str], "autoreport": bool (opcional)}]
      - repo.add_equipo(nombre, ip, puerto, depto_codigos[, autoreport])
      - repo.update_equipo(idx, nombre, ip, puerto, depto_codigos[, autoreport])
      - repo.delete_equipo(idx)
    """
    def __init__(self, master, repo):
        super().__init__(master)
        self.title("ABM Equipos")
        self.repo = repo
        self.resizable(False, False)
        self._idx_edit: int | None = None

        self._build_ui()
        self._cargar_tabla()
        self._refresh_depto_checks()
        
        # ⬇️ Calcula tamaño según contenido y centra
        self.update_idletasks()  # asegura medidas correctas
        w = self.winfo_reqwidth()
        h = self.winfo_reqheight()
        center_window(self, w, h)

        self.grab_set()
        self.focus_force()

    # -----------------------------
    # UI
    # -----------------------------
    def _build_ui(self):
        frm = ttk.Frame(self, padding=12)
        frm.grid(row=0, column=0, sticky=NSEW)

        lf = ttk.Labelframe(frm, text="Datos del equipo", padding=10)
        lf.grid(row=0, column=0, sticky=EW)
        for c in range(4):
            lf.columnconfigure(c, weight=1)

        ttk.Label(lf, text="Nombre:").grid(row=0, column=0, sticky=W, pady=4)
        self.var_nombre = ttk.StringVar()
        ttk.Entry(lf, textvariable=self.var_nombre, width=28).grid(row=0, column=1, sticky=EW, pady=4)

        ttk.Label(lf, text="IP:").grid(row=0, column=2, sticky=W, padx=(10, 0), pady=4)
        self.var_ip = ttk.StringVar()
        ttk.Entry(lf, textvariable=self.var_ip, width=16).grid(row=0, column=3, sticky=EW, pady=4)

        ttk.Label(lf, text="Puerto:").grid(row=1, column=0, sticky=W, pady=4)
        self.var_puerto = ttk.StringVar(value="1001")
        ttk.Entry(lf, textvariable=self.var_puerto, width=10).grid(row=1, column=1, sticky=W, pady=4)

        # ---- Checkbox Autoreport ----
        self.var_autoreport = tk.BooleanVar(value=False)
        ttk.Checkbutton(lf, text="AutoReport", variable=self.var_autoreport).grid(
            row=1, column=2, columnspan=2, sticky=W, pady=4
        )

        # ---- Checkboxes (multi) de departamentos con scroll y fondo suave ----
        ttk.Label(lf, text="Departamentos:").grid(row=2, column=0, sticky=NW, pady=(6, 0))
        self.sf_deptos = _ScrollFrame(lf, height=200, bg=PANEL_BG)
        self.sf_deptos.grid(row=2, column=1, columnspan=3, sticky=EW, pady=(4, 0))
        self._depto_vars: dict[str, tk.BooleanVar] = {}     # code -> var
        self._map_code_to_name: dict[str, str] = {}         # code -> name

        # Botones acción
        btns = ttk.Frame(lf)
        btns.grid(row=3, column=0, columnspan=4, sticky=E, pady=(8, 0))
        ttk.Button(btns, text="Nuevo", command=self._limpiar).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text="Guardar", bootstyle=SUCCESS, command=self._guardar).grid(row=0, column=1, padx=4)
        ttk.Button(btns, text="Eliminar", bootstyle=DANGER, command=self._eliminar).grid(row=0, column=2, padx=4)

        # Tabla
        self.tree = ttk.Treeview(
            frm,
            columns=("idx", "nombre", "ip", "puerto", "autoreport", "deptos"),
            show="headings",
            height=10
        )
        self.tree.grid(row=1, column=0, sticky=NSEW, pady=(10, 0))
        self.tree.heading("idx", text="#")
        self.tree.heading("nombre", text="Nombre")
        self.tree.heading("ip", text="IP")
        self.tree.heading("puerto", text="Puerto")
        self.tree.heading("autoreport", text="Autoreport")
        self.tree.heading("deptos", text="Deptos")
        self.tree.column("idx", width=40, anchor=CENTER)
        self.tree.column("nombre", width=180)
        self.tree.column("ip", width=120)
        self.tree.column("puerto", width=80, anchor=CENTER)
        self.tree.column("autoreport", width=95, anchor=CENTER)
        self.tree.column("deptos", width=320)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

    # -----------------------------
    # Departamentos (checkboxes)
    # -----------------------------
    def _refresh_depto_checks(self):
        """Construye/actualiza la lista de checkboxes desde repo.departamentos."""
        # limpiar anteriores
        for w in self.sf_deptos.inner.winfo_children():
            w.destroy()
        self._depto_vars.clear()
        self._map_code_to_name = {d["codigo"]: d["nombre"] for d in (self.repo.departamentos or [])}

        # crear checkbuttons (una columna, scrolleable)
        for i, d in enumerate(self.repo.departamentos or []):
            code, name = d["codigo"], d["nombre"]
            var = tk.BooleanVar(value=False)
            self._depto_vars[code] = var
            cb = ttk.Checkbutton(self.sf_deptos.inner, text=f"{code} - {name}", variable=var)
            cb.grid(row=i, column=0, sticky=W, pady=2)

    def refresh_departamentos(self):
        """Público: permite que otra ventana llame para refrescar la lista."""
        self._refresh_depto_checks()

    def _get_depto_codigos_seleccionados(self) -> list[str]:
        return [code for code, var in self._depto_vars.items() if var.get()]

    def _set_depto_seleccion(self, codigos: list[str]):
        # desmarcar todo
        for var in self._depto_vars.values():
            var.set(False)
        # marcar seleccionados
        for c in codigos or []:
            if c in self._depto_vars:
                self._depto_vars[c].set(True)

    # -----------------------------
    # Helpers
    # -----------------------------
    def _format_deptos_display(self, codigos: list[str]) -> str:
        """Devuelve una cadena amigable para la columna 'Deptos'."""
        if not codigos:
            return ""
        out = []
        for c in codigos:
            nom = self._map_code_to_name.get(c, "")
            out.append(f"{c} - {nom}" if nom else c)
        return ", ".join(out)

    def _to_bool(self, value, default=False) -> bool:
        """Convierte varios posibles tipos a bool (por compatibilidad)."""
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            v = value.strip().lower()
            return v in ("1", "true", "t", "yes", "y", "si", "sí")
        return default

    # -----------------------------
    # Repo wrappers (soportan firmas viejas/nuevas)
    # -----------------------------
    def _repo_add_equipo(self, nombre, ip, puerto, deptos, autoreport: bool):
        # intento posicional con autoreport
        try:
            self.repo.add_equipo(nombre, ip, int(puerto), deptos, autoreport)
            return
        except TypeError:
            pass
        # intento keyword con autoreport
        try:
            self.repo.add_equipo(nombre, ip, int(puerto), deptos, autoreport=bool(autoreport))
            return
        except TypeError:
            pass
        # fallback a firma vieja
        self.repo.add_equipo(nombre, ip, int(puerto), deptos)

    def _repo_update_equipo(self, idx, nombre, ip, puerto, deptos, autoreport: bool):
        # intento posicional con autoreport
        try:
            self.repo.update_equipo(idx, nombre, ip, int(puerto), deptos, autoreport)
            return
        except TypeError:
            pass
        # intento keyword con autoreport
        try:
            self.repo.update_equipo(idx, nombre, ip, int(puerto), deptos, autoreport=bool(autoreport))
            return
        except TypeError:
            pass
        # fallback a firma vieja
        self.repo.update_equipo(idx, nombre, ip, int(puerto), deptos)

    # -----------------------------
    # Tabla (Treeview)
    # -----------------------------
    def _cargar_tabla(self):
        for i in self.tree.get_children():
            self.tree.delete(i)

        equipos = self.repo.equipos or []
        for idx, e in enumerate(equipos):
            # soporta 'deptos' o 'depto_codigo'
            deptos = e.get("deptos")
            if deptos is None:
                unico = e.get("depto_codigo")
                deptos = [unico] if unico else []

            # Soporta varias claves para autoreport
            auto = self._to_bool(e.get("autoreport") or e.get("auto_report") or e.get("es_autoreport"), False)
            auto_txt = "Sí" if auto else "No"

            desc = self._format_deptos_display(deptos)
            self.tree.insert(
                "", tk.END,
                values=(idx, e.get("nombre", ""), e.get("ip", ""), e.get("puerto", ""), auto_txt, desc)
            )

    def _limpiar(self):
        self._idx_edit = None
        self.var_nombre.set("")
        self.var_ip.set("")
        self.var_puerto.set("1001")
        self.var_autoreport.set(False)
        self._set_depto_seleccion([])
        self.tree.selection_remove(self.tree.selection())

    # -----------------------------
    # Acciones
    # -----------------------------
    def _guardar(self):
        nombre = (self.var_nombre.get() or "").strip()
        ip = (self.var_ip.get() or "").strip()
        puerto = (self.var_puerto.get() or "").strip()
        depto_codigos = self._get_depto_codigos_seleccionados()
        autoreport = bool(self.var_autoreport.get())

        # Validaciones
        if not nombre:
            messagebox.showerror("Validación", "El nombre es obligatorio.", parent=self)
            return
        try:
            ipaddress.ip_address(ip)
        except Exception:
            messagebox.showerror("Validación", "IP inválida.", parent=self)
            return
        if not (puerto.isdigit() and 1 <= int(puerto) <= 65535):
            messagebox.showerror("Validación", "Puerto inválido (1..65535).", parent=self)
            return

        try:
            if self._idx_edit is None:
                self._repo_add_equipo(nombre, ip, int(puerto), depto_codigos, autoreport)
            else:
                self._repo_update_equipo(self._idx_edit, nombre, ip, int(puerto), depto_codigos, autoreport)
            self._cargar_tabla()
            self._limpiar()
        except Exception as e:
            messagebox.showerror("Error", str(e), parent=self)

    def _eliminar(self):
        if self._idx_edit is None:
            return
        e = (self.repo.equipos or [])[self._idx_edit]
        if messagebox.askyesno("Confirmar", f"¿Borrar el equipo '{e.get('nombre','')}'?", parent=self):
            try:
                self.repo.delete_equipo(self._idx_edit)
                self._cargar_tabla()
                self._limpiar()
            except Exception as err:
                messagebox.showerror("Error", str(err), parent=self)

    # -----------------------------
    # Eventos
    # -----------------------------
    def _on_select(self, _):
        sel = self.tree.selection()
        if not sel:
            return
        idx, nombre, ip, puerto, auto_txt, _ = self.tree.item(sel[0], "values")
        self._idx_edit = int(idx)
        self.var_nombre.set(nombre)
        self.var_ip.set(ip)
        self.var_puerto.set(str(puerto))

        # set checkboxes de deptos y de Autoreport
        equipos = self.repo.equipos or []
        deptos = equipos[self._idx_edit].get("deptos")
        if deptos is None:
            unico = equipos[self._idx_edit].get("depto_codigo")
            deptos = [unico] if unico else []
        self._set_depto_seleccion(deptos)

        auto = self._to_bool(
            equipos[self._idx_edit].get("autoreport")
            or equipos[self._idx_edit].get("auto_report")
            or equipos[self._idx_edit].get("es_autoreport"),
            False
        )
        self.var_autoreport.set(bool(auto))
