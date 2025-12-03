"""
Script de prueba para mostrar los primeros 10 códigos de cajeros
con tipo_registro = ARQUEO y sus valores de Sobrante/Faltante.
"""

import sys
from pathlib import Path

# Agregar el directorio raíz del proyecto al path
proyecto_root = Path(__file__).parent.parent
sys.path.insert(0, str(proyecto_root))

from src.config.cargador_config import CargadorConfig
from src.procesamiento.procesador_arqueos import ProcesadorArqueos
from src.utils.logger_config import configurar_logger
import logging

# Configurar logging
logger = configurar_logger(nivel=logging.INFO)


def main():
    """Función principal de prueba."""
    try:
        logger.info("=" * 80)
        logger.info("PRUEBA: Primeros 10 códigos de cajeros con tipo_registro = ARQUEO")
        logger.info("=" * 80)
        
        # Cargar configuración
        logger.info("Cargando configuración...")
        config = CargadorConfig(usar_fecha_actual=True)
        config_data = config.cargar()
        
        logger.info(f"Fecha de proceso: {config_data['proceso']['fecha_proceso']}")
        logger.info(f"Fecha de arqueo: {config_data['proceso']['fecha_arqueo']}")
        
        # Procesar el insumo
        procesador = ProcesadorArqueos(config)
        nombre_insumo = "gestion_ksgarro"
        
        logger.info(f"\nProcesando insumo: {nombre_insumo}")
        logger.info("-" * 80)
        
        # Buscar archivo más reciente
        buscar_mas_reciente = config_data['proceso'].get('buscar_archivo_mas_reciente', True)
        df_procesado = procesador.procesar_insumo(nombre_insumo, buscar_mas_reciente)
        
        logger.info(f"\nTotal de registros ARQUEO encontrados: {len(df_procesado)}")
        logger.info(f"Columnas disponibles: {list(df_procesado.columns)}")
        
        if len(df_procesado) == 0:
            logger.warning("No se encontraron registros con tipo_registro = ARQUEO")
            return
        
        # Identificar columnas relevantes
        # Buscar columnas que puedan contener código de cajero
        posibles_columnas_codigo = [
            'codigo_cajero', 'codigo', 'cajero', 'cod_cajero', 
            'codigo_cajero_electronico', 'cajero_electronico', 'id_cajero'
        ]
        
        columna_codigo = None
        for col in posibles_columnas_codigo:
            if col in df_procesado.columns:
                columna_codigo = col
                break
        
        # Si no se encuentra, usar la primera columna que parezca un código
        if columna_codigo is None:
            for col in df_procesado.columns:
                if 'cod' in col.lower() or 'cajero' in col.lower():
                    columna_codigo = col
                    break
        
        # Buscar columnas de sobrante/faltante (prioridad a estas columnas específicas)
        columna_sobrantes = None
        columna_faltantes = None
        
        if 'sobrantes' in df_procesado.columns:
            columna_sobrantes = 'sobrantes'
        if 'faltantes' in df_procesado.columns:
            columna_faltantes = 'faltantes'
        
        # Si no están, buscar variaciones
        if columna_sobrantes is None:
            posibles_columnas_sobrante = ['sobrante', 'sobrante_faltante', 'sobrante/faltante']
            for col in posibles_columnas_sobrante:
                if col in df_procesado.columns:
                    columna_sobrantes = col
                    break
        
        if columna_faltantes is None:
            posibles_columnas_faltante = ['faltante', 'sobrante_faltante', 'sobrante/faltante']
            for col in posibles_columnas_faltante:
                if col in df_procesado.columns:
                    columna_faltantes = col
                    break
        
        logger.info("\n" + "=" * 80)
        logger.info("RESULTADOS - PRIMEROS 10 REGISTROS")
        logger.info("=" * 80)
        
        # Mostrar información de columnas identificadas
        logger.info(f"\nColumna de código de cajero: {columna_codigo}")
        logger.info(f"Columna de sobrantes: {columna_sobrantes}")
        logger.info(f"Columna de faltantes: {columna_faltantes}")
        logger.info("\n" + "-" * 80)
        
        # Seleccionar las primeras 10 filas
        df_muestra = df_procesado.head(10)
        
        # Mostrar todas las columnas relevantes
        columnas_a_mostrar = []
        if columna_codigo:
            columnas_a_mostrar.append(columna_codigo)
        if columna_sobrantes:
            columnas_a_mostrar.append(columna_sobrantes)
        if columna_faltantes:
            columnas_a_mostrar.append(columna_faltantes)
        
        # Agregar código de sucursal si existe
        if 'codigo_suc' in df_procesado.columns:
            columnas_a_mostrar.append('codigo_suc')
        
        # Si no hay columnas específicas, mostrar todas
        if not columnas_a_mostrar:
            columnas_a_mostrar = list(df_procesado.columns)
        
        # Mostrar los datos
        print("\n" + "=" * 100)
        print("PRIMEROS 10 CÓDIGOS DE CAJEROS (tipo_registro = ARQUEO)")
        print("=" * 100)
        print(f"\nColumnas mostradas: {', '.join(columnas_a_mostrar)}")
        print("\n" + df_muestra[columnas_a_mostrar].to_string(index=False))
        print("\n" + "=" * 100)
        
        # Mostrar también en formato más legible
        logger.info("\n" + "=" * 100)
        logger.info("DETALLE DE LOS PRIMEROS 10 REGISTROS")
        logger.info("=" * 100)
        
        for idx, (_, row) in enumerate(df_muestra.iterrows(), 1):
            logger.info(f"\nRegistro #{idx}:")
            logger.info(f"  Código de cajero: {row[columna_codigo] if columna_codigo else 'N/A'}")
            if 'codigo_suc' in df_procesado.columns:
                logger.info(f"  Código de sucursal: {row['codigo_suc']}")
            if columna_sobrantes:
                valor_sobrante = row[columna_sobrantes]
                logger.info(f"  Sobrante: {valor_sobrante}")
            if columna_faltantes:
                valor_faltante = row[columna_faltantes]
                logger.info(f"  Faltante: {valor_faltante}")
        
        # Resumen estadístico
        logger.info("\n" + "=" * 100)
        logger.info("RESUMEN ESTADÍSTICO")
        logger.info("=" * 100)
        logger.info(f"Total de registros ARQUEO: {len(df_procesado)}")
        
        if columna_sobrantes:
            logger.info(f"\nSOBRANTES:")
            logger.info(f"  Valor mínimo: {df_procesado[columna_sobrantes].min()}")
            logger.info(f"  Valor máximo: {df_procesado[columna_sobrantes].max()}")
            logger.info(f"  Valor promedio: {df_procesado[columna_sobrantes].mean():.2f}")
            logger.info(f"  Suma total: {df_procesado[columna_sobrantes].sum():.2f}")
            logger.info(f"  Registros con sobrante > 0: {len(df_procesado[df_procesado[columna_sobrantes] > 0])}")
            logger.info(f"  Registros con sobrante = 0: {len(df_procesado[df_procesado[columna_sobrantes] == 0])}")
        
        if columna_faltantes:
            logger.info(f"\nFALTANTES:")
            logger.info(f"  Valor mínimo: {df_procesado[columna_faltantes].min()}")
            logger.info(f"  Valor máximo: {df_procesado[columna_faltantes].max()}")
            logger.info(f"  Valor promedio: {df_procesado[columna_faltantes].mean():.2f}")
            logger.info(f"  Suma total: {df_procesado[columna_faltantes].sum():.2f}")
            logger.info(f"  Registros con faltante > 0: {len(df_procesado[df_procesado[columna_faltantes] > 0])}")
            logger.info(f"  Registros con faltante = 0: {len(df_procesado[df_procesado[columna_faltantes] == 0])}")
        
        logger.info("\n" + "=" * 100)
        logger.info("PRUEBA COMPLETADA")
        logger.info("=" * 100)
        
    except Exception as e:
        logger.error(f"Error en la prueba: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

