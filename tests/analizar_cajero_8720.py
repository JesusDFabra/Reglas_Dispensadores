"""
Script para analizar paso a paso el procesamiento del cajero 8720.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Agregar el directorio raíz al path
proyecto_root = Path(__file__).parent.parent
sys.path.insert(0, str(proyecto_root))

from src.config.cargador_config import CargadorConfig
from src.consultas.consultor_movimientos import ConsultorMovimientos
from src.consultas.consultor_bd import ConsultorBD
from src.consultas.admin_bd import AdminBDNacional

def analizar_cajero_8720():
    """Analiza paso a paso el procesamiento del cajero 8720."""
    
    print("=" * 80)
    print("ANÁLISIS DETALLADO DEL CAJERO 8720")
    print("=" * 80)
    print()
    
    # Cargar archivo procesado
    archivo_procesado = proyecto_root / "insumos_excel" / "gestion_28_11_2025_ksgarro_procesado.xlsx"
    df = pd.read_excel(archivo_procesado)
    
    # Filtrar cajero 8720
    cajero_8720 = df[df['codigo_cajero'] == 8720].copy()
    
    if len(cajero_8720) == 0:
        print("❌ No se encontró el cajero 8720 en el archivo procesado")
        return
    
    print(f"📋 Registros encontrados para cajero 8720: {len(cajero_8720)}")
    print()
    
    # Mostrar información básica de cada registro
    for idx, row in cajero_8720.iterrows():
        tipo_registro = row.get('tipo_registro', 'N/A')
        fecha_arqueo = row.get('fecha_arqueo', 'N/A')
        faltante = row.get('faltantes', 0)
        sobrante = row.get('sobrantes', 0)
        justificacion = row.get('justificacion', 'N/A')
        nuevo_estado = row.get('nuevo_estado', 'N/A')
        observaciones = row.get('observaciones', 'N/A')
        ratificar_grabar = row.get('ratificar_grabar_diferencia', 'N/A')
        
        print(f"📝 Registro {idx}: {tipo_registro}")
        print(f"   Fecha arqueo: {fecha_arqueo}")
        print(f"   Faltante: ${faltante:,.0f}" if faltante > 0 else f"   Sobrante: ${abs(sobrante):,.0f}")
        print(f"   Justificación: {justificacion}")
        print(f"   Nuevo estado: {nuevo_estado}")
        print(f"   Ratificar grabar: {ratificar_grabar}")
        print(f"   Observaciones: {observaciones}")
        print()
    
    # Analizar qué regla se aplicó
    print("=" * 80)
    print("ANÁLISIS DE REGLAS APLICADAS")
    print("=" * 80)
    print()
    
    # Verificar si hay ARQUEO y DIARIO
    registros_arqueo = cajero_8720[cajero_8720['tipo_registro'] == 'ARQUEO']
    registros_diario = cajero_8720[cajero_8720['tipo_registro'] == 'DIARIO']
    
    if len(registros_arqueo) > 0 and len(registros_diario) > 0:
        print("✅ Se encontraron registros ARQUEO y DIARIO para este cajero")
        print()
        
        row_arqueo = registros_arqueo.iloc[0]
        row_diario = registros_diario.iloc[0]
        
        faltante_arqueo = float(row_arqueo.get('faltantes', 0)) if pd.notna(row_arqueo.get('faltantes')) else 0.0
        faltante_diario = float(row_diario.get('faltantes', 0)) if pd.notna(row_diario.get('faltantes')) else 0.0
        
        print(f"📊 Comparación de diferencias:")
        print(f"   ARQUEO faltante: ${faltante_arqueo:,.0f}")
        print(f"   DIARIO faltante: ${faltante_diario:,.0f}")
        print()
        
        # Verificar si son iguales
        if abs(faltante_arqueo - faltante_diario) < 0.01:
            print("✅ ARQUEO y DIARIO tienen la MISMA diferencia (FALTANTE)")
            print()
            print("🔍 REGLA APLICADA: ARQUEO y DIARIO iguales (FALTANTE)")
            print()
            print("📋 Pasos seguidos por el sistema:")
            print()
            print("   1️⃣ Verificación inicial:")
            print(f"      - Tipo registro: {row_diario.get('tipo_registro')}")
            print(f"      - Faltante: ${faltante_diario:,.0f}")
            print(f"      - Fecha arqueo: {row_diario.get('fecha_arqueo')}")
            print()
            
            # Cargar configuración y consultor BD
            config = CargadorConfig()
            config_data = config.cargar()
            query_params = config_data.get('base_datos', {}).get('query_params', {})
            
            usuario_nal = config_data.get('base_datos', {}).get('usuario_nal', '')
            clave_nal = config_data.get('base_datos', {}).get('clave_nal', '')
            
            if usuario_nal and clave_nal:
                print("   2️⃣ Búsqueda en NACIONAL (PASO 1):")
                print(f"      - Cuenta: {query_params.get('cuenta', 110505075)}")
                print(f"      - NROCMP: {query_params.get('nrocmp', 770500)}")
                print(f"      - Fecha arqueo: {row_diario.get('fecha_arqueo')}")
                print(f"      - Valor buscado: ${faltante_diario:,.0f} (faltante positivo)")
                print()
                
                try:
                    admin_bd = AdminBDNacional(usuario_nal, clave_nal)
                    consultor_bd = ConsultorBD(usuario_nal, clave_nal)
                    
                    fecha_arqueo_str = pd.to_datetime(row_diario['fecha_arqueo']).strftime('%Y-%m-%d')
                    
                    movimiento_nacional = consultor_bd.consultar_movimientos_nacional(
                        codigo_cajero=8720,
                        fecha_arqueo=fecha_arqueo_str,
                        valor_descuadre=faltante_diario,
                        cuenta=query_params.get('cuenta', 110505075),
                        codofi_excluir=query_params.get('codofi_excluir', 976),
                        nrocmp=query_params.get('nrocmp', 770500)
                    )
                    
                    if movimiento_nacional:
                        print("      ✅ Movimiento encontrado en NACIONAL cuenta 110505075")
                        print(f"         Detalle: {movimiento_nacional}")
                        print()
                        print("      📌 RESULTADO: Error en Transmicion de contadores")
                        print("         - Justificación: Contable")
                        print("         - Nuevo estado: Error en Transmicion de contadores")
                        print("         - Ratificar grabar: No")
                        print("         - Observaciones: Cajero cuadrado en arqueo")
                    else:
                        print("      ❌ NO se encontró movimiento en NACIONAL cuenta 110505075")
                        print()
                        print("   3️⃣ Búsqueda en NACIONAL (PASO 2):")
                        print(f"      - Cuenta de sobrantes: 279510020")
                        print(f"      - Fecha arqueo: {row_diario.get('fecha_arqueo')}")
                        print(f"      - Valor buscado: ${faltante_diario:,.0f}")
                        print()
                        
                        movimiento_sobrantes = consultor_bd.consultar_cuenta_sobrantes(
                            codigo_cajero=8720,
                            fecha_arqueo=fecha_arqueo_str,
                            valor_descuadre=faltante_diario,
                            cuenta=279510020,
                            codofi_excluir=query_params.get('codofi_excluir', 976)
                        )
                        
                        if movimiento_sobrantes:
                            print("      ✅ Movimiento encontrado en cuenta de sobrantes 279510020")
                            print(f"         Detalle: {movimiento_sobrantes}")
                            print()
                            print("      📌 RESULTADO: CRUCE DE NOVEDADES (para ARQUEO)")
                            print("         ARQUEO:")
                            print("         - Justificación: Cruzar")
                            print("         - Nuevo estado: CRUCE DE NOVEDADES")
                            print("         - Ratificar grabar: Reverso")
                            print("         - Observaciones: YYYYMMDD (fecha del arqueo)")
                            print()
                            print("         DIARIO:")
                            print("         - Justificación: Contable")
                            print("         - Nuevo estado: Error en Transmicion de contadores")
                            print("         - Ratificar grabar: No")
                            print("         - Observaciones: Se reversa diferencia no real")
                        else:
                            print("      ❌ NO se encontró movimiento en cuenta de sobrantes 279510020")
                            print()
                            print("      📌 RESULTADO FINAL: Error en Transmicion de contadores")
                            print("         - Justificación: Contable")
                            print("         - Nuevo estado: Error en Transmicion de contadores")
                            print("         - Ratificar grabar: No")
                            print("         - Observaciones: Se le solicita aclaración de diferencia")
                    
                    admin_bd.desconectar()
                    
                except Exception as e:
                    print(f"      ⚠️ Error al consultar BD: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print("   2️⃣ No se puede consultar BD (faltan credenciales)")
        else:
            print("❌ ARQUEO y DIARIO tienen diferencias DIFERENTES")
            print(f"   Diferencia: ${abs(faltante_arqueo - faltante_diario):,.0f}")
    else:
        print("⚠️ No se encontraron ambos registros (ARQUEO y DIARIO)")
    
    print()
    print("=" * 80)
    print("RESUMEN FINAL")
    print("=" * 80)
    print()
    
    for idx, row in cajero_8720.iterrows():
        tipo_registro = row.get('tipo_registro', 'N/A')
        justificacion = row.get('justificacion', 'N/A')
        nuevo_estado = row.get('nuevo_estado', 'N/A')
        observaciones = row.get('observaciones', 'N/A')
        
        print(f"📌 {tipo_registro}:")
        print(f"   Estado: {nuevo_estado}")
        print(f"   Justificación: {justificacion}")
        print(f"   Observaciones: {observaciones}")
        print()

if __name__ == "__main__":
    analizar_cajero_8720()

