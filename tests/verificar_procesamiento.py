import pandas as pd
import sys
from pathlib import Path

# Agregar el directorio raíz al path
proyecto_root = Path(__file__).parent.parent
sys.path.insert(0, str(proyecto_root))

# Buscar el archivo procesado del 28 de noviembre específicamente
archivo_28 = Path('insumos_excel/gestion_28_11_2025_ksgarro_procesado.xlsx')
if not archivo_28.exists():
    # Si no existe, buscar el más reciente
    archivos = list(Path('insumos_excel').glob('*gestion*procesado.xlsx'))
    if not archivos:
        print("No se encontró ningún archivo procesado")
        sys.exit(1)
    archivo = archivos[-1]  # El más reciente
    print(f"Archivo procesado encontrado (más reciente): {archivo.name}")
else:
    archivo = archivo_28
    print(f"Archivo procesado encontrado: {archivo.name}")

# Cargar el archivo procesado
df = pd.read_excel(archivo)

print("=" * 80)
print("VERIFICACIÓN DE PROCESAMIENTO")
print("=" * 80)
print(f"\nTotal de registros: {len(df)}")

# Identificar registros sin procesar (sin justificacion o nuevo_estado)
sin_procesar = df[(df['justificacion'].isna()) | (df['nuevo_estado'].isna())]

print(f"Registros sin procesar: {len(sin_procesar)}")

if len(sin_procesar) > 0:
    print("\n" + "=" * 80)
    print("REGISTROS SIN PROCESAR:")
    print("=" * 80)
    for idx, row in sin_procesar.iterrows():
        print(f"\nFila {idx + 2} (índice {idx}):")
        print(f"  Código cajero: {row.get('codigo_cajero', 'N/A')}")
        print(f"  Tipo registro: {row.get('tipo_registro', 'N/A')}")
        print(f"  Sobrantes: {row.get('sobrantes', 'N/A')}")
        print(f"  Faltantes: {row.get('faltantes', 'N/A')}")
        print(f"  Justificación: {row.get('justificacion', 'N/A')}")
        print(f"  Nuevo estado: {row.get('nuevo_estado', 'N/A')}")
else:
    print("\n✓ Todos los registros fueron procesados correctamente")

print("\n" + "=" * 80)

