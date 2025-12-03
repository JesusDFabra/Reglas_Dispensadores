"""
Script principal para procesar arqueos de cajeros electrónicos.
Procesa los archivos Excel según la configuración en config/insumos.yaml
Compatible con ejecución desde n8n.
"""

import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd

# Agregar el directorio src al path
sys.path.insert(0, str(Path(__file__).parent))

from src.config.cargador_config import CargadorConfig
from src.procesamiento.procesador_arqueos import ProcesadorArqueos
from src.utils.logger_config import configurar_logger
import logging

# Configurar logging
logger = configurar_logger(nivel=logging.INFO)


def main(usar_fecha_actual: bool = True, retornar_json: bool = False, fecha_especifica: Optional[str] = None):
    """
    Función principal del script.
    
    Args:
        usar_fecha_actual: Si es True, calcula fechas automáticamente desde la fecha actual.
        retornar_json: Si es True, retorna un JSON con los resultados (útil para n8n).
        fecha_especifica: Fecha específica en formato DD_MM_YYYY (ej: "28_11_2025").
                         Si se proporciona, busca ese archivo específico.
    
    Returns:
        Dict con resultados si retornar_json es True, None en caso contrario.
    """
    resultados = {
        "exito": False,
        "fecha_proceso": None,
        "fecha_arqueo": None,
        "insumos_procesados": [],
        "errores": []
    }
    
    try:
        logger.info("=" * 80)
        logger.info("INICIO DEL PROCESO DE ARQUEO DE CAJEROS")
        logger.info("=" * 80)
        
        # Cargar configuración con fecha actual automática
        logger.info("Cargando configuración...")
        config = CargadorConfig(usar_fecha_actual=usar_fecha_actual)
        config_data = config.cargar()
        
        # Obtener fechas
        fecha_proceso = config_data['proceso']['fecha_proceso']
        fecha_arqueo = config_data['proceso']['fecha_arqueo']
        resultados['fecha_proceso'] = fecha_proceso
        resultados['fecha_arqueo'] = fecha_arqueo
        
        logger.info(f"Fecha de proceso: {fecha_proceso}")
        logger.info(f"Fecha de arqueo: {fecha_arqueo}")
        
        # Verificar si se debe buscar archivo más reciente
        buscar_mas_reciente = config_data['proceso'].get('buscar_archivo_mas_reciente', True)
        
        # Obtener insumos activos
        insumos_activos = config.obtener_insumos_activos()
        
        if not insumos_activos:
            logger.warning("No hay insumos activos en la configuración.")
            resultados['errores'].append("No hay insumos activos en la configuración")
            if retornar_json:
                return resultados
            return None
        
        logger.info(f"Procesando {len(insumos_activos)} insumo(s) activo(s)")
        
        # Procesar cada insumo activo
        procesador = ProcesadorArqueos(config)
        
        for nombre_insumo, datos_insumo in insumos_activos.items():
            logger.info("-" * 80)
            logger.info(f"Procesando insumo: {nombre_insumo}")
            logger.info(f"Descripción: {datos_insumo.get('descripcion', 'N/A')}")
            logger.info("-" * 80)
            
            resultado_insumo = {
                "nombre": nombre_insumo,
                "exito": False,
                "registros_procesados": 0,
                "registros_con_descuadre": 0,
                "movimientos_encontrados": 0,
                "movimientos_no_encontrados": 0,
                "registros_actualizados": {
                    "faltante_contable": 0,
                    "faltante_en_arqueo": 0,
                    "sobrante_contable": 0,
                    "sobrante_en_arqueo": 0
                },
                "archivo_salida": None,
                "archivo_procesado": None,
                "registros": [],
                "error": None
            }
            
            try:
                # Procesar el insumo
                df_procesado = procesador.procesar_insumo(
                    nombre_insumo, 
                    buscar_mas_reciente,
                    fecha_especifica=fecha_especifica
                )
                
                # Mostrar resumen
                logger.info(f"Total de registros ARQUEO procesados: {len(df_procesado)}")
                logger.info(f"Columnas en el resultado: {list(df_procesado.columns)}")
                
                resultado_insumo["registros_procesados"] = len(df_procesado)
                
                # Analizar registros con descuadre
                registros_con_descuadre = df_procesado[
                    ((df_procesado['faltantes'].notna()) & (df_procesado['faltantes'] != 0)) |
                    ((df_procesado['sobrantes'].notna()) & (df_procesado['sobrantes'] != 0))
                ]
                resultado_insumo["registros_con_descuadre"] = len(registros_con_descuadre)
                
                # Analizar movimientos encontrados
                if 'movimiento_encontrado' in df_procesado.columns:
                    movimientos_encontrados = df_procesado['movimiento_encontrado'].sum()
                    movimientos_no_encontrados = len(registros_con_descuadre) - movimientos_encontrados
                    resultado_insumo["movimientos_encontrados"] = int(movimientos_encontrados)
                    resultado_insumo["movimientos_no_encontrados"] = int(movimientos_no_encontrados)
                    
                    # Contar por tipo de clasificación
                    from src.procesamiento.procesador_arqueos import limpiar_valor_numerico
                    for idx, row in registros_con_descuadre.iterrows():
                        movimiento_encontrado = row.get('movimiento_encontrado', False)
                        sobrante = limpiar_valor_numerico(row['sobrantes'])
                        faltante = limpiar_valor_numerico(row['faltantes'])
                        
                        if sobrante != 0:
                            if movimiento_encontrado:
                                resultado_insumo["registros_actualizados"]["sobrante_contable"] += 1
                            else:
                                resultado_insumo["registros_actualizados"]["sobrante_en_arqueo"] += 1
                        elif faltante != 0:
                            if movimiento_encontrado:
                                resultado_insumo["registros_actualizados"]["faltante_contable"] += 1
                            else:
                                resultado_insumo["registros_actualizados"]["faltante_en_arqueo"] += 1
                    
                    # Agregar información detallada de cada registro
                    for idx, row in registros_con_descuadre.iterrows():
                        movimiento_encontrado = row.get('movimiento_encontrado', False)
                        sobrante = limpiar_valor_numerico(row['sobrantes'])
                        faltante = limpiar_valor_numerico(row['faltantes'])
                        
                        # Determinar justificacion y nuevo_estado según las reglas de negocio
                        if sobrante != 0:
                            if movimiento_encontrado:
                                justificacion = 'SOBRANTE CONTABLE'
                                nuevo_estado = 'SOBRANTE CONTABLE'
                            else:
                                justificacion = 'SOBRANTE EN ARQUEO'
                                nuevo_estado = 'SOBRANTE EN ARQUEO'
                        elif faltante != 0:
                            if movimiento_encontrado:
                                justificacion = 'FALTANTE CONTABLE'
                                nuevo_estado = 'FALTANTE CONTABLE'
                            else:
                                justificacion = 'Fisico'
                                nuevo_estado = 'FALTANTE EN ARQUEO'
                        else:
                            justificacion = None
                            nuevo_estado = None
                        
                        registro_info = {
                            "codigo_cajero": int(row['codigo_cajero']) if pd.notna(row.get('codigo_cajero')) else None,
                            "codigo_suc": int(row['codigo_suc']) if pd.notna(row.get('codigo_suc')) else None,
                            "faltante": faltante,
                            "sobrante": sobrante,
                            "movimiento_encontrado": bool(movimiento_encontrado),
                            "movimiento_fuente": row.get('movimiento_fuente') if pd.notna(row.get('movimiento_fuente')) else None,
                            "justificacion": justificacion,
                            "nuevo_estado": nuevo_estado
                        }
                        resultado_insumo["registros"].append(registro_info)
                
                # Guardar resultados detallados
                nombre_salida = f"arqueos_procesados_{nombre_insumo}_{fecha_proceso.replace('-', '_')}"
                ruta_salida = procesador.guardar_resultados(df_procesado, nombre_salida)
                logger.info(f"Resultados detallados guardados en: {ruta_salida}")
                
                resultado_insumo["archivo_salida"] = str(ruta_salida)
                
                # Obtener ruta del archivo procesado (copia con actualizaciones)
                ruta_procesado = procesador.obtener_ruta_archivo_procesado()
                if ruta_procesado:
                    resultado_insumo["archivo_procesado"] = str(ruta_procesado)
                    logger.info(f"Archivo procesado (copia con actualizaciones) guardado en: {ruta_procesado}")
                
                resultado_insumo["exito"] = True
                
                # Mostrar muestra de datos
                if len(df_procesado) > 0:
                    logger.info("\nMuestra de datos procesados (primeras 5 filas):")
                    logger.info("\n" + str(df_procesado.head().to_string()))
                
            except Exception as e:
                error_msg = f"Error al procesar insumo {nombre_insumo}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                resultado_insumo["error"] = error_msg
                resultados['errores'].append(error_msg)
            
            resultados['insumos_procesados'].append(resultado_insumo)
        
        # Determinar éxito general
        resultados['exito'] = any(
            insumo['exito'] for insumo in resultados['insumos_procesados']
        )
        
        logger.info("=" * 80)
        logger.info("PROCESO COMPLETADO")
        logger.info("=" * 80)
        
        if retornar_json:
            return resultados
        
        return None
    
    except Exception as e:
        error_msg = f"Error crítico en el proceso principal: {str(e)}"
        logger.error(error_msg, exc_info=True)
        resultados['errores'].append(error_msg)
        resultados['exito'] = False
        
        if retornar_json:
            return resultados
        
        sys.exit(1)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Procesar arqueos de cajeros electrónicos')
    parser.add_argument(
        '--json',
        action='store_true',
        help='Retornar resultados en formato JSON (útil para n8n)'
    )
    parser.add_argument(
        '--fecha-fija',
        action='store_true',
        help='Usar fechas fijas del archivo de configuración en lugar de fecha actual'
    )
    parser.add_argument(
        '--fecha',
        type=str,
        help='Fecha específica en formato DD_MM_YYYY (ej: 28_11_2025) para procesar ese archivo específico'
    )
    
    args = parser.parse_args()
    
    usar_fecha_actual = not args.fecha_fija
    retornar_json = args.json
    fecha_especifica = args.fecha
    
    resultado = main(
        usar_fecha_actual=usar_fecha_actual, 
        retornar_json=retornar_json,
        fecha_especifica=fecha_especifica
    )
    
    if retornar_json and resultado:
        print(json.dumps(resultado, indent=2, ensure_ascii=False))

