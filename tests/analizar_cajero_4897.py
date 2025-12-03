"""
Script para analizar en detalle el cajero 4897 y explicar qué regla se aplicó.
"""
import sys
from pathlib import Path
import pandas as pd

# Agregar el directorio raíz del proyecto al path
proyecto_root = Path(__file__).parent.parent
sys.path.insert(0, str(proyecto_root))

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def print_and_log(msg):
    print(msg)
    logger.info(msg)

def analizar_cajero_4897():
    """Analiza en detalle el cajero 4897."""
    
    # Cargar archivo procesado más reciente (excluir archivos temporales que empiezan con ~$)
    archivos = sorted([f for f in Path("insumos_excel").glob("*procesado.xlsx") if not f.name.startswith("~$")], 
                     key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not archivos:
        print_and_log("No se encontraron archivos procesados")
        return
    
    archivo_procesado = archivos[0]
    print_and_log(f"Analizando archivo: {archivo_procesado}")
    
    print_and_log("=" * 100)
    print_and_log("ANÁLISIS DETALLADO DEL CAJERO 4897")
    print_and_log("=" * 100)
    
    # Cargar archivo procesado
    df_procesado = pd.read_excel(archivo_procesado)
    cajero_4897 = df_procesado[df_procesado['codigo_cajero'] == 4897]
    
    if len(cajero_4897) == 0:
        print_and_log("Cajero 4897 no encontrado en el archivo procesado")
        return
    
    print_and_log(f"\n1. REGISTROS ENCONTRADOS: {len(cajero_4897)}")
    print_and_log("-" * 100)
    
    for idx, row in cajero_4897.iterrows():
        print_and_log(f"\nREGISTRO {idx}:")
        print_and_log(f"  Tipo: {row.get('tipo_registro', 'N/A')}")
        print_and_log(f"  Fecha Arqueo: {row.get('fecha_arqueo', 'N/A')}")
        print_and_log(f"  Sobrantes: {row.get('sobrantes', 'N/A')}")
        print_and_log(f"  Faltantes: {row.get('faltantes', 'N/A')}")
        print_and_log(f"  Dispensado: {row.get('dispensado_corte_arqueo', 'N/A')}")
        print_and_log(f"  Recibido: {row.get('recibido_corte_arqueo', 'N/A')}")
        print_and_log(f"  Arqueo Físico/Saldo Contadores: {row.get('arqueo_fisico/saldo_contadores', 'N/A')}")
        print_and_log(f"  Saldo Contable: {row.get('saldo_contable', 'N/A')}")
        print_and_log(f"  Ratificar Grabar Diferencia: {row.get('ratificar_grabar_diferencia', 'N/A')}")
        print_and_log(f"  Justificación: {row.get('justificacion', 'N/A')}")
        print_and_log(f"  Nuevo Estado: {row.get('nuevo_estado', 'N/A')}")
        print_and_log(f"  Observaciones: {row.get('observaciones', 'N/A')}")
        print_and_log(f"  Movimiento Encontrado: {row.get('movimiento_encontrado', 'N/A')}")
        print_and_log(f"  Movimiento Fuente: {row.get('movimiento_fuente', 'N/A')}")
        print_and_log(f"  Movimiento Valor: {row.get('movimiento_valor', 'N/A')}")
        
        # Mostrar resumen_pasos si existe
        if 'resumen_pasos' in row and pd.notna(row['resumen_pasos']):
            print_and_log(f"\n  RESUMEN DE PASOS:")
            print_and_log(f"  {row['resumen_pasos']}")
    
    # También buscar en el archivo original para comparar
    archivo_original = Path("insumos_excel") / archivo_procesado.name.replace("_procesado", "")
    if archivo_original.exists():
        print_and_log("\n" + "=" * 100)
        print_and_log("COMPARACIÓN CON ARCHIVO ORIGINAL")
        print_and_log("=" * 100)
        df_original = pd.read_excel(archivo_original)
        cajero_4897_original = df_original[df_original['codigo_cajero'] == 4897]
        
        if len(cajero_4897_original) > 0:
            print_and_log(f"\nRegistros en original: {len(cajero_4897_original)}")
            for idx, row in cajero_4897_original.iterrows():
                print_and_log(f"\nREGISTRO ORIGINAL {idx}:")
                print_and_log(f"  Tipo: {row.get('tipo_registro', 'N/A')}")
                print_and_log(f"  Sobrantes: {row.get('sobrantes', 'N/A')}")
                print_and_log(f"  Faltantes: {row.get('faltantes', 'N/A')}")
                print_and_log(f"  Dispensado: {row.get('dispensado_corte_arqueo', 'N/A')}")
                print_and_log(f"  Recibido: {row.get('recibido_corte_arqueo', 'N/A')}")

if __name__ == "__main__":
    analizar_cajero_4897()

