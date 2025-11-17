from db.DBFReader import obtener_datos_conexion
from db.sybase_conexion import ConexionSybase


def Conexion_DBA(elecion_dbf):
    return ConexionSybase(**obtener_datos_conexion(elecion_dbf))