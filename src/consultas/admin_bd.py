"""
Módulo para administrar conexiones a bases de datos mediante ODBC.
Replica exactamente la metodología del proyecto CertificacionArqueo.
"""

import pyodbc
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class AdminBD:
    """
    Clase base para administrar conexiones a bases de datos mediante ODBC.
    Replica exactamente el patrón de CertificacionArqueo/utilidades/admin_bd.py
    """
    
    def __init__(self, servidor: str, usuario: str, clave: str):
        """
        Inicializa el administrador de base de datos.
        
        Args:
            servidor: Nombre del DSN del servidor ODBC
            usuario: Usuario para la conexión
            clave: Contraseña para la conexión
        """
        self.servidor = servidor
        self.usuario = usuario
        self.clave = clave
        self.conn = None  # Conexión que se mantendrá abierta
        self._conexion_abierta = False  # Flag para indicar si la conexión está abierta
    
    def conectar(self) -> pyodbc.Connection:
        """
        Establece la conexión a la base de datos.
        Si ya hay una conexión abierta, la reutiliza.
        
        Returns:
            Objeto de conexión pyodbc
        """
        # Si ya hay una conexión abierta y válida, reutilizarla
        if self._conexion_abierta and self.conn:
            try:
                # Verificar que la conexión sigue activa
                cursor = self.conn.cursor()
                cursor.close()
                logger.debug(f"Reutilizando conexión existente a DSN: {self.servidor}")
                return self.conn
            except:
                # La conexión se cerró, crear una nueva
                self._conexion_abierta = False
                self.conn = None
        
        try:
            self.conn = pyodbc.connect(f'''
                DSN={self.servidor}; 
                CCSID=37; 
                TRANSLATE=1; 
                UID={self.usuario}; 
                PWD={self.clave}''')
            self._conexion_abierta = True
            logger.info(f"Conexión establecida a DSN: {self.servidor}")
            return self.conn
        except Exception as e:
            logger.error(f"Error al conectar a {self.servidor}: {e}")
            self._conexion_abierta = False
            raise
    
    def consultar(self, consulta: str, mantener_conexion: bool = True) -> pd.DataFrame:
        """
        Ejecuta una consulta SQL y retorna un DataFrame.
        
        Args:
            consulta: Consulta SQL a ejecutar
            mantener_conexion: Si es True, mantiene la conexión abierta para reutilizarla.
                             Si es False, cierra la conexión después de la consulta (comportamiento original)
        
        Returns:
            DataFrame con los resultados de la consulta
        """
        try:
            # Conectar (reutiliza conexión si ya está abierta)
            self.conectar()
            logger.debug(f"Ejecutando consulta en {self.servidor}")
            df = pd.read_sql(consulta, self.conn)
            logger.debug(f"Consulta ejecutada exitosamente. Registros obtenidos: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"Error al ejecutar consulta: {e}")
            # Si hay error, cerrar la conexión para evitar problemas
            self._conexion_abierta = False
            raise
        finally:
            # Solo cerrar si mantener_conexion es False
            if not mantener_conexion:
                self.desconectar()
    
    def desconectar(self):
        """
        Cierra la conexión a la base de datos.
        """
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                self._conexion_abierta = False
                logger.info(f"Conexión cerrada a DSN: {self.servidor}")
            except Exception as e:
                logger.warning(f"Error al cerrar conexión: {e}")
                self._conexion_abierta = False


class AdminBDMedellin(AdminBD):
    """
    Administrador de base de datos para servidor MEDELLIN.
    Replica exactamente el patrón de CertificacionArqueo.
    """
    
    def __init__(self, usuario: str, clave: str):
        """
        Inicializa el administrador para servidor MEDELLIN.
        
        Args:
            usuario: Usuario para la conexión
            clave: Contraseña para la conexión
        """
        super().__init__('MEDELLIN', usuario, clave)


class AdminBDNacional(AdminBD):
    """
    Administrador de base de datos para servidor NACIONAL.
    Replica exactamente el patrón de CertificacionArqueo.
    """
    
    def __init__(self, usuario: str, clave: str):
        """
        Inicializa el administrador para servidor NACIONAL.
        
        Args:
            usuario: Usuario para la conexión
            clave: Contraseña para la conexión
        """
        super().__init__('NACIONAL', usuario, clave)


class AdminBDLZ(AdminBD):
    """
    Administrador de base de datos para servidor LZ (IMPALA_PROD).
    Replica exactamente el patrón de CertificacionArqueo.
    """
    
    def __init__(self, usuario: str, clave: str):
        """
        Inicializa el administrador para servidor LZ.
        
        Args:
            usuario: Usuario para la conexión (no se usa en este caso)
            clave: Contraseña para la conexión (no se usa en este caso)
        """
        super().__init__('LZ', usuario, clave)
    
    def conectar(self) -> pyodbc.Connection:
        """
        Establece la conexión a IMPALA_PROD (sin autenticación).
        Replica exactamente el método de CertificacionArqueo.
        
        Returns:
            Objeto de conexión pyodbc
        """
        try:
            self.conn = pyodbc.connect('DSN=IMPALA_PROD', autocommit=True)
            logger.info("Conexión establecida a DSN: IMPALA_PROD")
            return self.conn
        except Exception as e:
            logger.error(f"Error al conectar a IMPALA_PROD: {e}")
            raise


