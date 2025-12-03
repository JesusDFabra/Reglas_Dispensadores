# Cómo Cambiar el Origen del Archivo gestion_DD_MM_YYYY

Este documento explica qué líneas de código o configuración debes modificar si necesitas cambiar el origen (ruta/directorio) del archivo de gestión.

## ⚠️ IMPORTANTE: Búsqueda Automática por Fecha Actual

**El sistema busca automáticamente el archivo más reciente con la fecha actual** en el directorio `insumos_excel/`. La ruta especificada en el YAML es solo de referencia para extraer el patrón de búsqueda (ej: `gestion_*_ksgarro.xlsx`).

**No necesitas cambiar la fecha en el YAML** - el sistema siempre usará el archivo con la fecha más reciente disponible.

## Opción 1: Cambiar la Ruta en el Archivo de Configuración (RECOMENDADO)

**Archivo a modificar:** `config/insumos.yaml`

### Cambiar la ruta del archivo

**Líneas 7-11** en `config/insumos.yaml`:

```yaml
gestion_ksgarro:
  ruta: "gestion_21_11_2025_ksgarro.xlsx"  # ← CAMBIAR AQUÍ
  descripcion: "Archivo de gestión de arqueos - ksgarro"
  tipo_registro_filtro: "ARQUEO"
  activo: true
```

**Ejemplo:** Si el archivo está en otra carpeta:
```yaml
gestion_ksgarro:
  ruta: "carpeta_nueva/gestion_21_11_2025_ksgarro.xlsx"  # Ruta relativa
  # o
  ruta: "C:/ruta/absoluta/gestion_21_11_2025_ksgarro.xlsx"  # Ruta absoluta
```

### Cambiar el directorio base de búsqueda

**Líneas 42-48** en `config/insumos.yaml`:

```yaml
directorios:
  datos_entrada: "."  # ← Directorio base para rutas relativas
  insumos_excel: "insumos_excel"  # ← Directorio donde se buscan archivos por fecha
  datos_salida: "output"
  logs: "logs"
  configuracion: "config"
```

**Ejemplo:** Si quieres buscar en otro directorio:
```yaml
directorios:
  datos_entrada: "C:/mis_archivos"  # Cambiar directorio base
  insumos_excel: "C:/mis_archivos/insumos"  # Cambiar directorio de insumos
```

## Opción 2: Modificar el Código (Solo si necesitas cambiar la lógica)

Si necesitas cambiar la **lógica de búsqueda** (no solo la ruta), modifica estos archivos:

### 1. Cambiar el directorio donde se buscan archivos más recientes

**Archivo:** `src/config/cargador_config.py`

**Líneas 135-140:**
```python
if buscar_mas_reciente:
    # Buscar archivo más reciente en insumos_excel
    directorios = config.get('directorios', {})
    insumos_excel = directorios.get('insumos_excel', 'insumos_excel')  # ← Línea 138
    proyecto_root = Path(__file__).parent.parent.parent
    directorio_insumos = proyecto_root / insumos_excel  # ← Línea 140
```

**Para cambiar:** Modifica la línea 138 o 140 para usar otro directorio.

### 2. Cambiar el patrón de búsqueda de archivos

**Archivo:** `src/config/cargador_config.py`

**Líneas 142-150:**
```python
# Extraer patrón del nombre del insumo (ej: gestion_*_ksgarro)
ruta_original = insumos[nombre_insumo]['ruta']
# Extraer el sufijo después de la fecha (ej: _ksgarro.xlsx)
import re
patron_sufijo = r'gestion_\d{2}_\d{2}_\d{4}(.*)'  # ← Línea 146: Patrón regex
match = re.search(patron_sufijo, ruta_original)
if match:
    sufijo = match.group(1)  # ej: _ksgarro.xlsx
    patron_busqueda = f"gestion_*{sufijo.replace('.xlsx', '')}"  # ← Línea 150: Patrón de búsqueda
```

**Para cambiar:** 
- Línea 146: Modifica el patrón regex si el formato del nombre cambia
- Línea 150: Modifica el patrón de búsqueda si el formato es diferente

### 3. Cambiar la ruta estática (cuando no se busca el más reciente)

**Archivo:** `src/config/cargador_config.py`

**Líneas 168-174:**
```python
# Usar ruta estática
ruta_relativa = insumos[nombre_insumo]['ruta']  # ← Línea 169: Obtiene ruta del YAML
directorios = config.get('directorios', {})
datos_entrada = directorios.get('datos_entrada', '.')  # ← Línea 171: Directorio base

proyecto_root = Path(__file__).parent.parent.parent
ruta_completa = proyecto_root / datos_entrada / ruta_relativa  # ← Línea 174: Ruta final
```

**Para cambiar:** Modifica la línea 171 para cambiar el directorio base, o la línea 174 para cambiar cómo se construye la ruta.

## Resumen de Ubicaciones

| Qué cambiar | Archivo | Líneas |
|------------|---------|--------|
| **Ruta del archivo** | `config/insumos.yaml` | 8 |
| **Directorio base** | `config/insumos.yaml` | 44 |
| **Directorio insumos_excel** | `config/insumos.yaml` | 45 |
| **Lógica de búsqueda más reciente** | `src/config/cargador_config.py` | 135-166 |
| **Patrón de nombre de archivo** | `src/config/cargador_config.py` | 146, 150 |
| **Ruta estática** | `src/config/cargador_config.py` | 168-174 |

## Ejemplos de Cambios Comunes

### Ejemplo 1: Archivo en otra carpeta del mismo proyecto

**Modificar:** `config/insumos.yaml` línea 8
```yaml
gestion_ksgarro:
  ruta: "archivos_entrada/gestion_21_11_2025_ksgarro.xlsx"
```

### Ejemplo 2: Archivo en ruta absoluta diferente

**Modificar:** `config/insumos.yaml` línea 44
```yaml
directorios:
  datos_entrada: "D:/Bancolombia/Arqueos"
```

### Ejemplo 3: Cambiar el formato del nombre del archivo

Si el formato cambia de `gestion_DD_MM_YYYY_ksgarro.xlsx` a otro formato:

**Modificar:** `src/config/cargador_config.py` línea 146
```python
# De:
patron_sufijo = r'gestion_\d{2}_\d{2}_\d{4}(.*)'
# A (ejemplo):
patron_sufijo = r'arqueo_\d{4}-\d{2}-\d{2}(.*)'  # Para formato: arqueo_2025-11-21_ksgarro.xlsx
```

## Notas Importantes

1. **Recomendación:** Siempre modifica primero `config/insumos.yaml` antes de tocar el código
2. **Backup:** Haz backup del archivo YAML antes de modificarlo
3. **Formato:** Si cambias el formato del nombre del archivo, también debes actualizar el patrón regex
4. **Pruebas:** Después de cambiar, ejecuta `python tests/prueba_proceso_completo.py` para verificar

## Verificación

Después de hacer cambios, verifica que funciona:

```bash
python tests/prueba_proceso_completo.py
```

O ejecuta el proceso principal:

```bash
python main.py
```

