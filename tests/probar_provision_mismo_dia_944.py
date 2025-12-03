"""
Script para probar la nueva regla de provisión mismo día con el cajero 944.
"""
import sys
from pathlib import Path

# Agregar el directorio raíz del proyecto al path
proyecto_root = Path(__file__).parent.parent
sys.path.insert(0, str(proyecto_root))

from src.config.cargador_config import CargadorConfig
from src.consultas.consultor_bd import ConsultorBD
from src.consultas.admin_bd import AdminBDNacional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def probar_provision_mismo_dia():
    """Prueba la consulta de provisión mismo día para el cajero 944."""
    
    logger.info("=" * 100)
    logger.info("PRUEBA: CONSULTA DE PROVISIÓN MISMO DÍA - CAJERO 944")
    logger.info("=" * 100)
    
    # Cargar configuración
    config = CargadorConfig()
    config_data = config.cargar()
    
    # Datos del cajero 944
    codigo_cajero = 944
    fecha_arqueo = '2025-11-24'
    sobrante_arqueo = -57730000
    faltante_diario = 570000
    
    logger.info(f"\nDatos del cajero 944:")
    logger.info(f"  Fecha arqueo: {fecha_arqueo}")
    logger.info(f"  Sobrante ARQUEO: {sobrante_arqueo:,.0f}")
    logger.info(f"  Faltante DIARIO: {faltante_diario:,.0f}")
    logger.info(f"  Diferencia esperada: {abs(sobrante_arqueo) + faltante_diario:,.0f}")
    
    # Conectar a la base de datos
    try:
        db_config = config_data.get('base_datos', {})
        admin_bd = AdminBDNacional(
            usuario=db_config.get('usuario_nal', ''),
            clave=db_config.get('clave_nal', '')
        )
        
        if not admin_bd.conectar():
            logger.error("No se pudo conectar a la base de datos")
            return
        
        logger.info("\n✓ Conexión a BD establecida")
        
        # Crear consultor BD
        consultor_bd = ConsultorBD(
            usuario=db_config.get('usuario_nal', ''),
            clave=db_config.get('clave_nal', '')
        )
        
        # Consultar provisión mismo día
        logger.info(f"\nConsultando provisión (NROCMP 810291) para el mismo día del arqueo...")
        provision = consultor_bd.consultar_provision_mismo_dia(
            codigo_cajero=codigo_cajero,
            fecha_arqueo=fecha_arqueo
        )
        
        if provision:
            valor_provision = abs(float(provision.get('VALOR', 0)))
            fecha_provision = provision.get('FECHA', 'N/A')
            
            logger.info(f"\n✓ PROVISIÓN ENCONTRADA:")
            logger.info(f"  Valor: {valor_provision:,.0f}")
            logger.info(f"  Fecha: {fecha_provision}")
            logger.info(f"  NROCMP: {provision.get('NROCMP', 'N/A')}")
            logger.info(f"  NIT: {provision.get('NIT', 'N/A')}")
            
            # Verificar si la provisión explica la diferencia
            valor_sobrante_abs = abs(sobrante_arqueo)
            diferencia_calculada = valor_provision - valor_sobrante_abs
            
            logger.info(f"\nVerificación:")
            logger.info(f"  Provisión: {valor_provision:,.0f}")
            logger.info(f"  Sobrante ARQUEO: {valor_sobrante_abs:,.0f}")
            logger.info(f"  Diferencia calculada (Provisión - Sobrante): {diferencia_calculada:,.0f}")
            logger.info(f"  Faltante DIARIO esperado: {faltante_diario:,.0f}")
            
            if abs(diferencia_calculada - faltante_diario) <= 1000:
                logger.info(f"\n✓ LA PROVISIÓN EXPLICA LA DIFERENCIA")
                logger.info(f"  {valor_provision:,.0f} - {valor_sobrante_abs:,.0f} = {diferencia_calculada:,.0f} ≈ {faltante_diario:,.0f}")
            else:
                logger.warning(f"\n✗ LA PROVISIÓN NO EXPLICA LA DIFERENCIA")
                logger.warning(f"  Diferencia: {abs(diferencia_calculada - faltante_diario):,.0f}")
        else:
            logger.warning(f"\n✗ No se encontró provisión el mismo día del arqueo")
        
        # Desconectar
        consultor_bd.desconectar()
        logger.info("\n✓ Conexión cerrada")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    
    logger.info("\n" + "=" * 100)
    logger.info("PRUEBA COMPLETADA")
    logger.info("=" * 100)

if __name__ == '__main__':
    probar_provision_mismo_dia()

