from dbfread import DBF
from pathlib import Path

class DBFReader:
    def __init__(self, dbf_path):
        self.dbf_path = Path(dbf_path)
        self.table = None
        self.open_dbf()
        
    def open_dbf(self):
        if not self.dbf_path.exists():
            print(f"El archivo {self.dbf_path} no existe.")
            return False
        try:
            self.table = DBF(self.dbf_path, encoding="latin1")  # Cambia la codificación si es necesario
            print(f"Archivo {self.dbf_path} cargado correctamente.")
            return True
        except Exception as e:
            print("Error al abrir el archivo DBF:", e)
            return False
        
    def get_field_names(self):
        if not self.table:
            print("No se ha cargado ninguna tabla.")
            return None
        return self.table.field_names
    
    def get_field_values(self, field_name):
        """
        Obtiene todos los valores de un campo específico.
        
        :param field_name: Nombre del campo.
        :return: Lista de valores del campo.
        """
        if not self.table:
            print("No se ha cargado ninguna tabla.")
            return None

        if field_name not in self.table.field_names:
            print(f"El campo '{field_name}' no existe en la tabla.")
            return None
        
        try:
            values = [record[field_name] for record in self.table]
            return values
        except Exception as e:
            print("Error al obtener los valores del campo:", e)
            return None
        
        
def obtener_datos_conexion(Elecion_DBF):
    if Elecion_DBF == "SYBASE":
        # Uso de la clase
        dbf_path = r"F:\Sp\FacturaP\Dbf\SYBASE.DBF"
        dbf_reader = DBFReader(dbf_path)
        datos_unidos = dbf_reader.get_field_values("DNSSISTEMA")[0]
        return parse_connection_string(datos_unidos)
    elif Elecion_DBF == "SYBASE0":
        # Uso de la clase
        dbf_path = r"F:\Sp\FacturaP\Dbf\SYBASE0.DBF"
        dbf_reader = DBFReader(dbf_path)
        datos_unidos = dbf_reader.get_field_values("DNSSISTEMA")[0]
        return parse_connection_string(datos_unidos)
    elif Elecion_DBF == "SYBASE5":
        # Uso de la clase
        dbf_path = r"F:\Sp\FacturaP\Dbf\SYBASE5.DBF"
        dbf_reader = DBFReader(dbf_path)
        datos_unidos = dbf_reader.get_field_values("DNSSISTEMA")[0]
        return parse_connection_string(datos_unidos)
    elif Elecion_DBF == "SYBASE10":
        # Uso de la clase
        dbf_path = r"F:\Sp\FacturaP\Dbf\SYBASE10.DBF"
        dbf_reader = DBFReader(dbf_path)
        datos_unidos = dbf_reader.get_field_values("DNSSISTEMA")[0]
        return parse_connection_string(datos_unidos)
        
        
def parse_connection_string(connection_string):
    """
    Convierte una cadena de conexión en un diccionario de argumentos.

    :param connection_string: Cadena de conexión en formato ";clave=valor;"
    :return: Diccionario con las claves y valores de la cadena.
    """
    # Eliminar espacios en blanco innecesarios y dividir por ';'
    parts = connection_string.strip().split(';')
    
    # Crear el diccionario a partir de las partes
    kwargs = {}
    for part in parts:
        if '=' in part:  # Solo procesar si tiene un formato clave=valor
            key, value = part.split('=', 1)
            kwargs[key.strip().lower()] = value.strip()
    
    return kwargs
        




"""
# Obtener los nombres de los campos
field_names = dbf_reader.get_field_names()
if field_names:
    print("Campos en la tabla:", field_names)

# ['DNSSISTEMA', 'DNSUSUARIO', 'CONEXION', 'PATHSP', 'PATHPALM', 'ADICIONAL', 'OTRO']

# Obtener los valores de un campo específico
campo = "OTRO"  # Reemplaza con el nombre del campo que deseas consultar
valores = dbf_reader.get_field_values(campo)

if valores:
    print(f"Valores del campo '{campo}':")
    for valor in valores:
        print(valor)
"""