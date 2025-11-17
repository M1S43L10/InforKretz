# ================================
# FILE: GUI/abm_departamentos.py
# ================================
import json
from pathlib import Path
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import messagebox
from Func.log_errorsV2 import log_error

DATA_FILE = Path("abm_data.json")

class Repo:
    """Repositorio simple basado en JSON (se comparte entre ventanas).
    Podés reemplazarlo luego por tu DAO a Sybase conservando la misma interfaz.
    """
    def __init__(self):
        self.departamentos: list[dict] = []
        self.equipos: list[dict] = []
        self._load()

    def _load(self):
        try:
            if DATA_FILE.exists():
                data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
                self.departamentos = data.get("departamentos", [])
                self.equipos = data.get("equipos", [])
            else:
                # Seed demo
                self.departamentos = [
                    {"codigo": "001", "nombre": "Panadería"},
                    {"codigo": "002", "nombre": "Fiambrería"},
                    {"codigo": "003", "nombre": "Carnicería"},
                ]
                self.equipos = []
                self._save()
        except Exception as e:
            log_error(str(e), "Repo._load")
            self.departamentos, self.equipos = [], []

    def _save(self):
        try:
            DATA_FILE.write_text(
                json.dumps({
                    "departamentos": self.departamentos,
                    "equipos": self.equipos,
                }, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            log_error(str(e), "Repo._save")

    # -------- Departamentos --------
    def add_depto(self, codigo: str, nombre: str):
        if any(d["codigo"] == codigo for d in self.departamentos):
            raise ValueError(f"Ya existe el departamento {codigo}.")
        self.departamentos.append({"codigo": codigo, "nombre": nombre})
        self.departamentos.sort(key=lambda d: d["codigo"])  # mantener orden
        self._save()

    def update_depto(self, codigo: str, nombre: str):
        for d in self.departamentos:
            if d["codigo"] == codigo:
                d["nombre"] = nombre
                self._save()
                return
        raise ValueError("Departamento no encontrado")

    def delete_depto(self, codigo: str):
        # Evitar borrar si hay equipos asociados
        if any(e.get("depto_codigo") == codigo for e in self.equipos):
            raise ValueError("No se puede borrar: hay equipos asociados a este depto.")
        before = len(self.departamentos)
        self.departamentos = [d for d in self.departamentos if d["codigo"] != codigo]
        if len(self.departamentos) == before:
            raise ValueError("Departamento no encontrado")
        self._save()

class VentanaDepartamentos(ttk.Toplevel):
    def __init__(self, master, repo: Repo, on_change=None):
        super().__init__(master)
        self.title("ABM Departamentos")
        self.repo = repo
        self.on_change = on_change
        self.resizable(False, False)
        self._build_ui()
        self._cargar_tabla()
        self.grab_set()
        self.focus_force()

    def _build_ui(self):
        frm = ttk.Frame(self, padding=12)
        frm.grid(row=0, column=0, sticky=NSEW)

        # Formulario
        lf = ttk.Labelframe(frm, text="Datos del departamento", padding=10)
        lf.grid(row=0, column=0, sticky=EW)
        lf.columnconfigure(1, weight=1)

        ttk.Label(lf, text="Código (3 dígitos):").grid(row=0, column=0, sticky=W, padx=(0,6), pady=4)
        self.var_cod = ttk.StringVar()
        e_cod = ttk.Entry(lf, textvariable=self.var_cod, width=8)
        e_cod.grid(row=0, column=1, sticky=W, pady=4)

        ttk.Label(lf, text="Nombre:").grid(row=1, column=0, sticky=W, padx=(0,6), pady=4)
        self.var_nom = ttk.StringVar()
        e_nom = ttk.Entry(lf, textvariable=self.var_nom, width=35)
        e_nom.grid(row=1, column=1, sticky=EW, pady=4)

        btns = ttk.Frame(lf)
        btns.grid(row=2, column=0, columnspan=2, sticky=E, pady=(6,0))
        ttk.Button(btns, text="Nuevo", command=self._limpiar).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text="Guardar", bootstyle=SUCCESS, command=self._guardar).grid(row=0, column=1, padx=4)
        ttk.Button(btns, text="Eliminar", bootstyle=DANGER, command=self._eliminar).grid(row=0, column=2, padx=4)

        # Tabla
        self.tree = ttk.Treeview(frm, columns=("codigo","nombre"), show="headings", height=8)
        self.tree.grid(row=1, column=0, sticky=NSEW, pady=(10,0))
        self.tree.heading("codigo", text="Código")
        self.tree.heading("nombre", text="Nombre")
        self.tree.column("codigo", width=90, anchor=CENTER)
        self.tree.column("nombre", width=260)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

    def _cargar_tabla(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for idx, e in enumerate(self.repo.equipos):
            depto_desc = ", ".join(e.get("deptos") or [])
            self.tree.insert("", ttk.END, values=(idx, e["nombre"], e["ip"], e["puerto"], depto_desc))

    def _limpiar(self):
        self._idx_edit = None
        self.var_nombre.set("")
        self.var_ip.set("")
        self.var_puerto.set("1001")
        self._set_depto_seleccion([])
        self.tree.selection_remove(self.tree.selection())

        self.var_cod.set("")
        self.var_nom.set("")
        self.tree.selection_remove(self.tree.selection())

    def _guardar(self):
        nombre = self.var_nombre.get().strip()
        ip = self.var_ip.get().strip()
        puerto = self.var_puerto.get().strip()
        depto_codigos = self._get_depto_codigos_seleccionados()

        if not nombre:
            messagebox.showerror("Validación", "El nombre es obligatorio.", parent=self)
            return
        from ipaddress import ip_address
        try:
            ip_address(ip)
        except Exception:
            messagebox.showerror("Validación", "IP inválida.", parent=self)
            return
        if not (puerto.isdigit() and 1 <= int(puerto) <= 65535):
            messagebox.showerror("Validación", "Puerto inválido (1..65535).", parent=self)
            return

        try:
            if self._idx_edit is None:
                self.repo.add_equipo(nombre, ip, int(puerto), depto_codigos)
            else:
                self.repo.update_equipo(self._idx_edit, nombre, ip, int(puerto), depto_codigos)
            self._cargar_tabla()
            self._limpiar()
        except Exception as e:
            messagebox.showerror("Error", str(e), parent=self)

    def _eliminar(self):
        if self._idx_edit is None:
            return
        e = self.repo.equipos[self._idx_edit]
        if messagebox.askyesno("Confirmar", f"¿Borrar el equipo '{e['nombre']}'?", parent=self):
            try:
                self.repo.delete_equipo(self._idx_edit)
                self._cargar_tabla()
                self._limpiar()
            except Exception as e:
                messagebox.showerror("Error", str(e), parent=self)

        sel = self.tree.selection()
        if not sel:
            return
        codigo = self.tree.item(sel[0], "values")[0]
        if messagebox.askyesno("Confirmar", f"¿Borrar el departamento {codigo}?", parent=self):
            try:
                self.repo.delete_depto(codigo)
                self._cargar_tabla()
                if self.on_change:
                    self.on_change()
                self._limpiar()
            except Exception as e:
                log_error(str(e), "VentanaDepartamentos._eliminar")
                messagebox.showerror("Error", str(e), parent=self)

    def _on_select(self, _):
        sel = self.tree.selection()
        if not sel:
            return
        idx, nombre, ip, puerto, _ = self.tree.item(sel[0], "values")
        self._idx_edit = int(idx)
        self.var_nombre.set(nombre)
        self.var_ip.set(ip)
        self.var_puerto.set(str(puerto))
        # set multi selección de deptos
        deptos = self.repo.equipos[self._idx_edit].get("deptos") or []
        self._set_depto_seleccion(deptos)