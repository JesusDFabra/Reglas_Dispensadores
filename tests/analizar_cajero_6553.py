import pandas as pd
from pathlib import Path

# Cargar archivo procesado
archivo = Path('insumos_excel/gestion_03_12_2025_ksgarro_procesado.xlsx')
df = pd.read_excel(archivo)

# Filtrar cajero 6553
cajero = df[df['codigo_cajero'] == 6553]

print("=" * 80)
print("ANÁLISIS DEL CAJERO 6553")
print("=" * 80)

for idx, row in cajero.iterrows():
    print(f"\n--- REGISTRO {row['tipo_registro']} (fila {idx}) ---")
    print(f"Faltantes: {row['faltantes']}")
    print(f"Sobrantes: {row['sobrantes']}")
    print(f"Dispensado: {row.get('dispensado_corte_arqueo', 'N/A')}")
    print(f"Recibido: {row.get('recibido_corte_arqueo', 'N/A')}")
    print(f"Justificación: {row['justificacion']}")
    print(f"Nuevo estado: {row['nuevo_estado']}")
    print(f"Ratificar grabar diferencia: {row['ratificar_grabar_diferencia']}")
    print(f"Observaciones: {row['observaciones']}")
    print(f"Movimiento encontrado: {row.get('movimiento_encontrado', 'N/A')}")
    print(f"Movimiento fuente: {row.get('movimiento_fuente', 'N/A')}")
    print(f"Movimiento valor: {row.get('movimiento_valor', 'N/A')}")
    print(f"\nResumen de pasos:")
    print(row.get('resumen_pasos', 'N/A'))
    print("\n" + "-" * 80)

# Comparar valores entre ARQUEO y DIARIO
arqueo = cajero[cajero['tipo_registro'] == 'ARQUEO'].iloc[0]
diario = cajero[cajero['tipo_registro'] == 'DIARIO'].iloc[0]

print("\n" + "=" * 80)
print("COMPARACIÓN ARQUEO vs DIARIO")
print("=" * 80)
print(f"Faltantes: ARQUEO={arqueo['faltantes']}, DIARIO={diario['faltantes']} → {'IGUALES' if arqueo['faltantes'] == diario['faltantes'] else 'DIFERENTES'}")
print(f"Sobrantes: ARQUEO={arqueo['sobrantes']}, DIARIO={diario['sobrantes']} → {'IGUALES' if arqueo['sobrantes'] == diario['sobrantes'] else 'DIFERENTES'}")
print(f"Dispensado: ARQUEO={arqueo.get('dispensado_corte_arqueo', 'N/A')}, DIARIO={diario.get('dispensado_corte_arqueo', 'N/A')}")
print(f"Recibido: ARQUEO={arqueo.get('recibido_corte_arqueo', 'N/A')}, DIARIO={diario.get('recibido_corte_arqueo', 'N/A')}")

