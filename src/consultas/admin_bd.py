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
    
    def conectar(self) -> pyodbc.Connection:
        """
        Establece la conexión a la base de datos.
        Replica exactamente el método de CertificacionArqueo.
        
        Returns:
            Objeto de conexión pyodbc
        """
        try:
            self.conn = pyodbc.connect(f'''
                DSN={self.servidor}; 
                CCSID=37; 
                TRANSLATE=1; 
                UID={self.usuario}; 
                PWD={self.clave}''')
            logger.info(f"Conexión establecida a DSN: {self.servidor}")
            return self.conn
        except Exception as e:
            logger.error(f"Error al conectar a {self.servidor}: {e}")
            raise
    
    def consultar(self, consulta: str) -> pd.DataFrame:
        """
        Ejecuta una consulta SQL y retorna un DataFrame.
        Replica exactamente el método de CertificacionArqueo:
        - Siempre llama a conectar() antes de consultar
        - Usa pandas.read_sql() directamente
        
        Args:
            consulta: Consulta SQL a ejecutar
        
        Returns:
            DataFrame con los resultados de la consulta
        """
        try:
            # Replicar exactamente: siempre conectar antes de consultar
            self.conectar()
            logger.debug(f"Ejecutando consulta en {self.servidor}")
            df = pd.read_sql(consulta, self.conn)
            logger.info(f"Consulta ejecutada exitosamente. Registros obtenidos: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"Error al ejecutar consulta: {e}")
            raise
        finally:
            # Cerrar conexión después de la consulta
            self.desconectar()
    
    def desconectar(self):
        """
        Cierra la conexión a la base de datos.
        """
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
                self.conn = None
                logger.debug(f"Conexión cerrada a DSN: {self.servidor}")
            except Exception as e:
                logger.warning(f"Error al cerrar conexión: {e}")


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


