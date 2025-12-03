# Proyecto de Arqueo de Cajeros Electrónicos - Bancolombia

Sistema para procesar y aplicar reglas de negocio sobre arqueos de cajeros electrónicos en sucursales Bancolombia.

## Descripción

Este proyecto procesa archivos Excel que contienen información de arqueos realizados en sucursales. Los arqueos del día anterior se procesan para realizar el arqueo diario del día siguiente.

## Estructura del Proyecto

```
EUC/
├── config/
│   └── insumos.yaml          # Configuración de rutas de insumos
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── cargador_config.py    # Carga y gestión de configuración
│   ├── procesamiento/
│   │   ├── __init__.py
│   │   └── procesador_arqueos.py # Procesamiento de archivos Excel
│   └── utils/
│       ├── __init__.py
│       ├── logger_config.py      # Configuración de logging
│       └── buscador_archivos.py   # Búsqueda de archivos por fecha
├── tests/                       # Carpeta de pruebas y tests
│   ├── __init__.py
│   ├── prueba_arqueos.py        # Prueba de códigos de cajeros
│   ├── ejemplo_uso.py           # Ejemplo de uso del sistema
│   └── README.md                # Documentación de tests
├── output/                      # Directorio para archivos de salida
├── logs/                        # Directorio para archivos de log
├── insumos_excel/               # Directorio con archivos Excel de entrada
├── main.py                      # Script principal
├── ejecutar_n8n.py             # Script optimizado para n8n
├── requirements.txt             # Dependencias del proyecto
└── README.md                    # Este archivo
```

## Requisitos

- Python 3.9.12
- Dependencias listadas en `requirements.txt`

## Instalación

1. Crear un entorno virtual (recomendado):
```bash
python -m venv venv
venv\Scripts\activate  # En Windows
```

2. Instalar dependencias:
```bash
pip install -r requirements.txt
```

## Configuración

El archivo `config/insumos.yaml` contiene la configuración de los insumos (archivos Excel) a procesar. 

### Ejemplo de configuración:

```yaml
insumos:
  gestion_ksgarro:
    ruta: "gestion_21_11_2025_ksgarro.xlsx"
    descripcion: "Archivo de gestión de arqueos - ksgarro"
    tipo_registro_filtro: "ARQUEO"
    activo: true
```

- `ruta`: Ruta relativa al archivo Excel
- `tipo_registro_filtro`: Valor para filtrar la columna `tipo_registro` (por defecto "ARQUEO")
- `activo`: Si es `true`, el insumo se procesará al ejecutar el script

## Uso

### Ejecución Normal

```bash
python main.py
```

### Ejecución para n8n (con salida JSON)

```bash
python ejecutar_n8n.py
```

O:

```bash
python main.py --json
```

### Opciones de línea de comandos

```bash
# Usar fecha actual (por defecto)
python main.py

# Usar fechas fijas del archivo de configuración
python main.py --fecha-fija

# Retornar resultados en JSON (útil para n8n)
python main.py --json
```

El script:
1. Calcula automáticamente la fecha de proceso (fecha actual) y fecha de arqueo (día anterior)
2. Busca el archivo más reciente en `insumos_excel/` que coincida con el patrón configurado
3. Carga la configuración desde `config/insumos.yaml`
4. Procesa todos los insumos marcados como `activo: true`
5. Filtra los registros donde `tipo_registro = ARQUEO`
6. Guarda los resultados en el directorio `output/`
7. Genera logs en el directorio `logs/`

## Procesamiento Automático

El sistema procesa automáticamente:
- **Detección de fecha**: Usa la fecha actual del sistema
- **Búsqueda de archivo**: Busca el archivo más reciente en `insumos_excel/` que coincida con el patrón
- **Filtro**: Registros donde `tipo_registro = ARQUEO`
- **Fecha de proceso**: Fecha actual (calculada automáticamente)
- **Fecha de arqueo**: Día anterior a la fecha de proceso (calculada automáticamente)

### Ejemplo:
- Si se ejecuta el 22 de noviembre de 2025:
  - Fecha de proceso: 2025-11-22
  - Fecha de arqueo: 2025-11-21
  - Buscará automáticamente: `gestion_*_ksgarro.xlsx` más reciente en `insumos_excel/`
  - Seleccionará el archivo con la fecha más reciente (ej: `gestion_22_11_2025_ksgarro.xlsx`)

**Nota:** La ruta en `config/insumos.yaml` es solo de referencia. El sistema siempre busca el archivo con la fecha actual automáticamente.

## Logs

Los logs se guardan en el directorio `logs/` con el formato:
- `arqueo_cajeros_YYYYMMDD.log`

## Salidas

Los archivos procesados se guardan en el directorio `output/` con el formato:
- `arqueos_procesados_{nombre_insumo}_{fecha_proceso}.xlsx`

## Buenas Prácticas Implementadas

- ✅ Separación de responsabilidades (config, procesamiento, utils)
- ✅ Logging estructurado
- ✅ Manejo de excepciones
- ✅ Configuración externa (YAML)
- ✅ Documentación en código
- ✅ Type hints
- ✅ Validación de datos
- ✅ Estructura modular y escalable

## Integración con n8n

El proyecto está preparado para ejecutarse desde n8n. Ver documentación completa en:
- `docs/INTEGRACION_N8N.md`

### Resumen rápido para n8n:

1. Usa el nodo **Execute Command** en n8n
2. Ejecuta: `python ejecutar_n8n.py`
3. El script retorna JSON con los resultados del proceso

## Tests y Pruebas

La carpeta `tests/` contiene scripts de prueba y ejemplos:

- `prueba_arqueos.py`: Muestra los primeros 10 códigos de cajeros con sus valores de sobrante/faltante
- `ejemplo_uso.py`: Ejemplo básico de uso del sistema

Para ejecutar las pruebas:

```bash
python tests/prueba_arqueos.py
python tests/ejemplo_uso.py
```

Ver más detalles en `tests/README.md`.

## Reglas de Negocio

El sistema aplica las siguientes reglas de negocio para los arqueos con descuadres:

### Proceso de Consulta de Movimientos

Para cada arqueo con descuadre (sobrante o faltante), el sistema:

1. **Busca en NACIONAL**: Consulta en la base de datos NACIONAL o en el archivo `NACIONAL_movimientos.xlsx` buscando el código del cajero (columna `NIT`) y la fecha de arqueo (columna `FECHA` en formato YYYYMMDD).

2. **Busca en SOBRANTES**: Si no encuentra en NACIONAL, busca en el archivo `SOBRANTES SUCURSALES CTA 279510020 85.xlsx`:
   - Primero en la hoja `SOBRANTE CTA 279510020` (columna `CODIGO` y `NUEVO VALOR`)
   - Si no encuentra, busca en la hoja `HISTORICO 279510020`

### Actualización de Registros No Encontrados

Si el movimiento **NO se encuentra** en ninguna de las fuentes anteriores:

- **Para SOBRANTES** (sobrantes > 0):
  - `justificacion` = `"SOBRANTE CONTABLE"`
  - `nuevo_estado` = `"SOBRANTE CONTABLE"`

- **Para FALTANTES** (faltantes > 0):
  - `justificacion` = `"Fisico"`
  - `nuevo_estado` = `"FALTANTE EN ARQUEO"`

El archivo original se actualiza automáticamente y se crea un backup (`.backup.xlsx`) antes de la modificación.

## Próximos Pasos

- Agregar validaciones de datos adicionales
- Crear módulos de análisis y reportes
- Implementar pruebas unitarias

