"""
Script de prueba específico para el cajero 544.
Muestra todos los detalles de las consultas realizadas.
"""

import sys
from pathlib import Path
from datetime import datetime
import json
import pandas as pd

# Agregar el directorio raíz del proyecto al path
proyecto_root = Path(__file__).parent.parent
sys.path.insert(0, str(proyecto_root))

from src.config.cargador_config import CargadorConfig
from src.procesamiento.procesador_arqueos import ProcesadorArqueos
from src.utils.logger_config import configurar_logger
from src.consultas.consultor_movimientos import ConsultorMovimientos
import logging
import pandas as pd

# Configurar logging
logger = configurar_logger(nivel=logging.DEBUG)


def main():
    """Función principal de prueba para cajero 544."""
    try:
        logger.info("=" * 100)
        logger.info("PRUEBA ESPECÍFICA: CAJERO 544")
        logger.info("=" * 100)
        
        # Cargar configuración
        logger.info("\n1. Cargando configuración...")
        config = CargadorConfig(usar_fecha_actual=True)
        config_data = config.cargar()
        
        fecha_proceso = config_data['proceso']['fecha_proceso']
        fecha_arqueo = config_data['proceso']['fecha_arqueo']
        
        logger.info(f"   Fecha de proceso: {fecha_proceso}")
        logger.info(f"   Fecha de arqueo: {fecha_arqueo}")
        
        # Cargar archivo y filtrar solo cajero 544
        logger.info("\n2. Cargando archivo y filtrando cajero 544...")
        procesador = ProcesadorArqueos(config, consultar_movimientos=True)
        
        nombre_insumo = "gestion_ksgarro"
        ruta_archivo = config.obtener_ruta_insumo(nombre_insumo, buscar_mas_reciente=True)
        
        logger.info(f"   Archivo: {ruta_archivo}")
        
        # Cargar archivo completo
        df_completo = procesador.cargar_archivo_excel(ruta_archivo)
        logger.info(f"   Total de registros en archivo: {len(df_completo)}")
        
        # Filtrar por tipo_registro = ARQUEO
        tipo_registro = config.obtener_tipo_registro_filtro(nombre_insumo)
        df_arqueos = procesador.filtrar_por_tipo_registro(df_completo, tipo_registro)
        logger.info(f"   Registros ARQUEO: {len(df_arqueos)}")
        
        # Filtrar solo cajero 544
        df_544 = df_arqueos[df_arqueos['codigo_cajero'] == 544].copy()
        logger.info(f"   Registros cajero 544: {len(df_544)}")
        
        if len(df_544) == 0:
            logger.error("No se encontró el cajero 544 en el archivo")
            return
        
        # Mostrar información del registro
        logger.info("\n3. Información del registro del cajero 544:")
        logger.info("-" * 100)
        registro = df_544.iloc[0]
        logger.info(f"   Código cajero: {registro['codigo_cajero']}")
        logger.info(f"   Código sucursal: {registro['codigo_suc']}")
        logger.info(f"   Faltante: {registro['faltantes']}")
        logger.info(f"   Sobrante: {registro['sobrantes']}")
        logger.info(f"   Fecha arqueo: {registro.get('fecha_arqueo', 'N/A')}")
        
        sobrante = float(registro['sobrantes']) if pd.notna(registro['sobrantes']) else 0.0
        faltante = float(registro['faltantes']) if pd.notna(registro['faltantes']) else 0.0
        
        # Determinar tipo de descuadre
        if sobrante != 0:
            valor_descuadre = sobrante
            es_sobrante = True
            tipo_descuadre = "SOBRANTE"
        elif faltante != 0:
            valor_descuadre = faltante
            es_sobrante = False
            tipo_descuadre = "FALTANTE"
        else:
            logger.warning("El cajero 544 no tiene descuadre")
            return
        
        logger.info(f"\n   Tipo de descuadre: {tipo_descuadre}")
        logger.info(f"   Valor descuadre: {valor_descuadre}")
        logger.info(f"   Es sobrante: {es_sobrante}")
        
        # Consultar movimientos manualmente con detalles
        logger.info("\n4. Consultando movimientos con detalles...")
        logger.info("-" * 100)
        
        consultor = ConsultorMovimientos(config)
        
        # Obtener fecha de arqueo del registro
        fecha_arqueo_registro = registro.get('fecha_arqueo')
        if pd.notna(fecha_arqueo_registro):
            if isinstance(fecha_arqueo_registro, pd.Timestamp):
                fecha_arqueo_real = fecha_arqueo_registro.strftime('%Y-%m-%d')
            elif isinstance(fecha_arqueo_registro, datetime):
                fecha_arqueo_real = fecha_arqueo_registro.strftime('%Y-%m-%d')
            else:
                fecha_arqueo_real = str(fecha_arqueo_registro).split(' ')[0] if ' ' in str(fecha_arqueo_registro) else str(fecha_arqueo_registro)
        else:
            fecha_arqueo_real = fecha_arqueo
        
        logger.info(f"   Fecha de arqueo del registro: {fecha_arqueo_real}")
        logger.info(f"   Fecha de arqueo calculada (fallback): {fecha_arqueo}")
        logger.info(f"   Usando fecha: {fecha_arqueo_real}")
        
        # Mostrar qué valor se buscará en BD
        if es_sobrante:
            # SOBRANTE: en archivo es negativo, en BD es positivo
            valor_busqueda_bd = abs(valor_descuadre)
            logger.info(f"   Buscando en BD NACIONAL:")
            logger.info(f"     - Valor en archivo (sobrante): {valor_descuadre}")
            logger.info(f"     - Valor a buscar en BD (positivo): {valor_busqueda_bd}")
        else:
            # FALTANTE: en archivo es positivo, en BD es negativo
            valor_busqueda_bd = -abs(valor_descuadre)
            logger.info(f"   Buscando en BD NACIONAL:")
            logger.info(f"     - Valor en archivo (faltante): {valor_descuadre}")
            logger.info(f"     - Valor a buscar en BD (negativo): {valor_busqueda_bd}")
        
        logger.info(f"     - Fecha arqueo: {fecha_arqueo_real}")
        logger.info(f"     - Código cajero (NIT): 544")
        
        # Buscar en NACIONAL
        logger.info("\n   4.1. Consultando en BD NACIONAL...")
        movimiento_nacional = consultor.buscar_en_nacional(
            codigo_cajero=544,
            fecha_arqueo=fecha_arqueo_real,
            valor_descuadre=valor_busqueda_bd
        )
        
        if movimiento_nacional:
            logger.info("   ✓ MOVIMIENTO ENCONTRADO EN NACIONAL:")
            logger.info(f"     {json.dumps(movimiento_nacional, indent=6, default=str, ensure_ascii=False)}")
        else:
            logger.info("   ✗ No se encontró movimiento en NACIONAL")
        
        # Buscar en SOBRANTES o FALTANTES según corresponda
        if es_sobrante:
            logger.info("\n   4.2. Consultando en FALTANTES (hoja FORMATO FALTANTES)...")
            movimiento_faltantes = consultor.buscar_en_faltantes(
                codigo_cajero=544,
                valor_descuadre=valor_descuadre,
                usar_historico=False
            )
            
            if movimiento_faltantes:
                logger.info("   ✓ MOVIMIENTO ENCONTRADO EN FALTANTES (FORMATO):")
                logger.info(f"     {json.dumps(movimiento_faltantes, indent=6, default=str, ensure_ascii=False)}")
            else:
                logger.info("   ✗ No se encontró en FALTANTES (FORMATO)")
            
            logger.info("\n   4.3. Consultando en FALTANTES (hoja HISTORICO FALTANTES)...")
            movimiento_historico_faltantes = consultor.buscar_en_faltantes(
                codigo_cajero=544,
                valor_descuadre=valor_descuadre,
                usar_historico=True
            )
            
            if movimiento_historico_faltantes:
                logger.info("   ✓ MOVIMIENTO ENCONTRADO EN FALTANTES (HISTORICO):")
                logger.info(f"     {json.dumps(movimiento_historico_faltantes, indent=6, default=str, ensure_ascii=False)}")
            else:
                logger.info("   ✗ No se encontró en FALTANTES (HISTORICO)")
        else:
            logger.info("\n   4.2. Consultando en SOBRANTES (hoja SOBRANTE CTA 279510020)...")
            movimiento_sobrantes = consultor.buscar_en_sobrantes(
                codigo_cajero=544,
                valor_descuadre=valor_descuadre,
                usar_historico=False
            )
            
            if movimiento_sobrantes:
                logger.info("   ✓ MOVIMIENTO ENCONTRADO EN SOBRANTES (SOBRANTE):")
                logger.info(f"     {json.dumps(movimiento_sobrantes, indent=6, default=str, ensure_ascii=False)}")
            else:
                logger.info("   ✗ No se encontró en SOBRANTES (SOBRANTE)")
            
            logger.info("\n   4.3. Consultando en SOBRANTES (hoja HISTORICO 279510020)...")
            movimiento_historico = consultor.buscar_en_sobrantes(
                codigo_cajero=544,
                valor_descuadre=valor_descuadre,
                usar_historico=True
            )
            
            if movimiento_historico:
                logger.info("   ✓ MOVIMIENTO ENCONTRADO EN SOBRANTES (HISTORICO):")
                logger.info(f"     {json.dumps(movimiento_historico, indent=6, default=str, ensure_ascii=False)}")
            else:
                logger.info("   ✗ No se encontró en SOBRANTES (HISTORICO)")
        
        # Ejecutar búsqueda completa
        logger.info("\n5. Ejecutando búsqueda completa con buscar_movimiento()...")
        logger.info("-" * 100)
        
        resultado_completo = consultor.buscar_movimiento(
            codigo_cajero=544,
            fecha_arqueo=fecha_arqueo_real,
            valor_descuadre=valor_descuadre,
            es_sobrante=es_sobrante
        )
        
        logger.info(f"   Resultado completo:")
        logger.info(f"     {json.dumps(resultado_completo, indent=6, default=str, ensure_ascii=False)}")
        
        # Procesar el registro completo
        logger.info("\n6. Procesando registro completo con ProcesadorArqueos...")
        logger.info("-" * 100)
        
        # Agregar columnas necesarias para el procesamiento
        df_544['movimiento_encontrado'] = False
        df_544['movimiento_fuente'] = None
        df_544['movimiento_valor'] = None
        df_544['movimiento_detalle'] = None
        
        # Procesar consulta de movimientos
        df_procesado = procesador._consultar_movimientos(df_544)
        
        logger.info("\n   Resultado del procesamiento:")
        logger.info(f"     Movimiento encontrado: {df_procesado.iloc[0]['movimiento_encontrado']}")
        logger.info(f"     Fuente: {df_procesado.iloc[0]['movimiento_fuente']}")
        logger.info(f"     Valor: {df_procesado.iloc[0]['movimiento_valor']}")
        
        # Determinar clasificación final
        movimiento_encontrado = df_procesado.iloc[0]['movimiento_encontrado']
        
        if es_sobrante:
            if movimiento_encontrado:
                justificacion = 'SOBRANTE CONTABLE'
                nuevo_estado = 'SOBRANTE CONTABLE'
            else:
                justificacion = 'SOBRANTE EN ARQUEO'
                nuevo_estado = 'SOBRANTE EN ARQUEO'
        else:
            if movimiento_encontrado:
                justificacion = 'FALTANTE CONTABLE'
                nuevo_estado = 'FALTANTE CONTABLE'
            else:
                justificacion = 'Fisico'
                nuevo_estado = 'FALTANTE EN ARQUEO'
        
        logger.info(f"\n   Clasificación final:")
        logger.info(f"     Justificación: {justificacion}")
        logger.info(f"     Nuevo estado: {nuevo_estado}")
        
        # Nota sobre la fecha encontrada manualmente
        logger.info("\n" + "=" * 100)
        logger.info("NOTA: Consulta manual encontró registro en fecha 20251121 (21 de noviembre)")
        logger.info("      El proceso busca en fecha de arqueo: " + fecha_arqueo.replace('-', ''))
        logger.info("=" * 100)
        
    except Exception as e:
        logger.error(f"Error en la prueba: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

