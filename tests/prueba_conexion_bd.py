"""
Script de prueba para verificar conexión a bases de datos NACIONAL y MEDELLIN.
"""

import sys
from pathlib import Path
import logging

# Agregar el directorio raíz al path para imports
sys.path.append(str(Path(__file__).parent.parent))

from src.config.cargador_config import CargadorConfig
from src.consultas.admin_bd import AdminBDNacional, AdminBDMedellin
from src.utils.logger_config import configurar_logger

# Configurar logging
configurar_logger(nivel=logging.INFO)
logger = logging.getLogger(__name__)


def probar_conexion_nacional():
    """Prueba la conexión a la base de datos NACIONAL."""
    print("\n" + "=" * 80)
    print("PRUEBA DE CONEXIÓN A BASE DE DATOS NACIONAL")
    print("=" * 80)
    logger.info("=" * 80)
    logger.info("PRUEBA DE CONEXIÓN A BASE DE DATOS NACIONAL")
    logger.info("=" * 80)
    
    try:
        # Cargar configuración
        config = CargadorConfig()
        config_data = config.cargar()
        base_datos_config = config_data.get('base_datos', {})
        
        usuario_nal = base_datos_config.get('usuario_nal', '')
        clave_nal = base_datos_config.get('clave_nal', '')
        
        if not usuario_nal or not clave_nal:
            logger.error("ERROR: Credenciales de NACIONAL no configuradas en insumos.yaml")
            logger.error("   Configure usuario_nal y clave_nal en config/insumos.yaml")
            return False
        
        print(f"Usuario: {usuario_nal}")
        print(f"DSN: NACIONAL")
        print("")
        logger.info(f"Usuario: {usuario_nal}")
        logger.info(f"DSN: NACIONAL")
        logger.info("")
        
        # Crear instancia de AdminBDNacional
        admin_bd = AdminBDNacional(usuario_nal, clave_nal)
        
        # Probar conexión y consulta
        print("Intentando conectar y ejecutar consulta de prueba...")
        logger.info("Intentando conectar y ejecutar consulta de prueba...")
        consulta_prueba = "SELECT 1 as TEST FROM SYSIBM.SYSDUMMY1"
        
        try:
            df = admin_bd.consultar(consulta_prueba)
            resultado = df.iloc[0]['TEST']
            print(f"✓ Consulta ejecutada exitosamente. Resultado: {resultado}")
            print("")
            print("✓ PRUEBA NACIONAL: EXITOSA")
            logger.info(f"✓ Consulta ejecutada exitosamente. Resultado: {resultado}")
            logger.info("")
            logger.info("✓ PRUEBA NACIONAL: EXITOSA")
            return True
        except Exception as e:
            print(f"✗ Error al ejecutar consulta: {e}")
            logger.error(f"✗ Error al ejecutar consulta: {e}")
            return False
    
    except Exception as e:
        logger.error(f"✗ ERROR EN PRUEBA NACIONAL: {e}")
        logger.error("", exc_info=True)
        return False


def probar_conexion_medellin():
    """Prueba la conexión a la base de datos MEDELLIN."""
    print("")
    print("=" * 80)
    print("PRUEBA DE CONEXIÓN A BASE DE DATOS MEDELLIN")
    print("=" * 80)
    logger.info("")
    logger.info("=" * 80)
    logger.info("PRUEBA DE CONEXIÓN A BASE DE DATOS MEDELLIN")
    logger.info("=" * 80)
    
    try:
        # Cargar configuración
        config = CargadorConfig()
        config_data = config.cargar()
        base_datos_config = config_data.get('base_datos', {})
        
        usuario_med = base_datos_config.get('usuario_med', '')
        clave_med = base_datos_config.get('clave_med', '')
        
        if not usuario_med or not clave_med:
            logger.error("ERROR: Credenciales de MEDELLIN no configuradas en insumos.yaml")
            logger.error("   Configure usuario_med y clave_med en config/insumos.yaml")
            return False
        
        print(f"Usuario: {usuario_med}")
        print(f"DSN: MEDELLIN")
        print("")
        logger.info(f"Usuario: {usuario_med}")
        logger.info(f"DSN: MEDELLIN")
        logger.info("")
        
        # Crear instancia de AdminBDMedellin
        admin_bd = AdminBDMedellin(usuario_med, clave_med)
        
        # Probar conexión y consulta
        print("Intentando conectar y ejecutar consulta de prueba...")
        logger.info("Intentando conectar y ejecutar consulta de prueba...")
        consulta_prueba = "SELECT 1 as TEST FROM SYSIBM.SYSDUMMY1"
        
        try:
            df = admin_bd.consultar(consulta_prueba)
            resultado = df.iloc[0]['TEST']
            print(f"✓ Consulta ejecutada exitosamente. Resultado: {resultado}")
            print("")
            print("✓ PRUEBA MEDELLIN: EXITOSA")
            logger.info(f"✓ Consulta ejecutada exitosamente. Resultado: {resultado}")
            logger.info("")
            logger.info("✓ PRUEBA MEDELLIN: EXITOSA")
            return True
        except Exception as e:
            print(f"✗ Error al ejecutar consulta: {e}")
            logger.error(f"✗ Error al ejecutar consulta: {e}")
            return False
    
    except Exception as e:
        logger.error(f"✗ ERROR EN PRUEBA MEDELLIN: {e}")
        logger.error("", exc_info=True)
        return False


def main():
    """Función principal que ejecuta todas las pruebas."""
    print("")
    print("=" * 80)
    print("PRUEBA DE CONEXIÓN A BASES DE DATOS")
    print("=" * 80)
    print("")
    logger.info("")
    logger.info("=" * 80)
    logger.info("PRUEBA DE CONEXIÓN A BASES DE DATOS")
    logger.info("=" * 80)
    logger.info("")
    
    # Verificar que pyodbc esté instalado
    try:
        import pyodbc
        print(f"pyodbc versión: {pyodbc.version}")
        logger.info(f"pyodbc versión: {pyodbc.version}")
    except ImportError:
        print("ERROR: pyodbc no está instalado")
        print("   Instale con: pip install pyodbc")
        logger.error("ERROR: pyodbc no está instalado")
        logger.error("   Instale con: pip install pyodbc")
        return
    
    print("")
    logger.info("")
    
    # Ejecutar pruebas
    resultado_nacional = probar_conexion_nacional()
    resultado_medellin = probar_conexion_medellin()
    
    # Resumen final
    print("")
    print("=" * 80)
    print("RESUMEN DE PRUEBAS")
    print("=" * 80)
    print(f"NACIONAL:  {'✓ EXITOSA' if resultado_nacional else '✗ FALLIDA'}")
    print(f"MEDELLIN:  {'✓ EXITOSA' if resultado_medellin else '✗ FALLIDA'}")
    print("=" * 80)
    logger.info("")
    logger.info("=" * 80)
    logger.info("RESUMEN DE PRUEBAS")
    logger.info("=" * 80)
    logger.info(f"NACIONAL:  {'✓ EXITOSA' if resultado_nacional else '✗ FALLIDA'}")
    logger.info(f"MEDELLIN:  {'✓ EXITOSA' if resultado_medellin else '✗ FALLIDA'}")
    logger.info("=" * 80)
    
    if resultado_nacional and resultado_medellin:
        print("")
        print("✓ Todas las pruebas fueron exitosas")
        logger.info("")
        logger.info("✓ Todas las pruebas fueron exitosas")
        return 0
    else:
        print("")
        print("✗ Algunas pruebas fallaron. Revise los errores anteriores.")
        logger.info("")
        logger.warning("✗ Algunas pruebas fallaron. Revise los errores anteriores.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

