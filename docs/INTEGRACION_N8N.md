# Integración con n8n

Este documento explica cómo integrar el proceso de arqueo de cajeros con n8n.

## Características para n8n

El proyecto está diseñado para ejecutarse automáticamente desde n8n con las siguientes características:

1. **Detección automática de fecha**: Usa la fecha actual del sistema
2. **Búsqueda de archivo más reciente**: Busca automáticamente el archivo con la fecha más reciente en `insumos_excel`
3. **Salida JSON**: Retorna resultados en formato JSON para facilitar el procesamiento

## Configuración en n8n

### Opción 1: Usar el script ejecutar_n8n.py (Recomendado)

1. En n8n, crea un nodo **Execute Command**
2. Configura el comando:
   ```bash
   python ejecutar_n8n.py
   ```
3. Asegúrate de que el working directory sea el directorio del proyecto (`C:\Users\Administrador\EUC`)
4. El nodo capturará la salida JSON

### Opción 2: Usar main.py con argumentos

1. En n8n, crea un nodo **Execute Command**
2. Configura el comando:
   ```bash
   python main.py --json
   ```
3. El nodo capturará la salida JSON

## Formato de Salida JSON

El script retorna un objeto JSON con la siguiente estructura:

```json
{
  "exito": true,
  "fecha_proceso": "2025-11-21",
  "fecha_arqueo": "2025-11-20",
  "insumos_procesados": [
    {
      "nombre": "gestion_ksgarro",
      "exito": true,
      "registros_procesados": 150,
      "archivo_salida": "output/arqueos_procesados_gestion_ksgarro_2025_11_21.xlsx",
      "error": null
    }
  ],
  "errores": []
}
```

### Campos del JSON

- **exito**: `boolean` - Indica si el proceso fue exitoso
- **fecha_proceso**: `string` - Fecha en que se ejecuta el proceso (YYYY-MM-DD)
- **fecha_arqueo**: `string` - Fecha del arqueo procesado (día anterior, YYYY-MM-DD)
- **insumos_procesados**: `array` - Lista de insumos procesados
  - **nombre**: Nombre del insumo
  - **exito**: Si el insumo se procesó correctamente
  - **registros_procesados**: Cantidad de registros ARQUEO encontrados
  - **archivo_salida**: Ruta del archivo Excel generado
  - **error**: Mensaje de error si hubo algún problema
- **errores**: `array` - Lista de errores encontrados durante el proceso

## Ejemplo de Workflow n8n

### Workflow Básico

1. **Schedule Trigger** - Ejecuta diariamente a la hora deseada
2. **Execute Command** - Ejecuta `python ejecutar_n8n.py`
3. **IF Node** - Verifica si `exito === true`
4. **Send Email** (opcional) - Envía notificación de éxito/error

### Workflow con Procesamiento de Resultados

1. **Schedule Trigger**
2. **Execute Command** - Ejecuta el script
3. **Code Node** - Parsea el JSON de salida
   ```javascript
   const resultado = JSON.parse($input.item.json.stdout);
   return {
     json: {
       fecha: resultado.fecha_proceso,
       registros: resultado.insumos_procesados[0].registros_procesados,
       archivo: resultado.insumos_procesados[0].archivo_salida
     }
   };
   ```
4. **HTTP Request** - Envía resultados a otro sistema
5. **Send Email** - Notifica el resultado

## Variables de Entorno (Opcional)

Si necesitas configurar rutas específicas, puedes usar variables de entorno:

```bash
# Windows PowerShell
$env:PYTHONPATH = "C:\Users\Administrador\EUC"
python ejecutar_n8n.py
```

## Manejo de Errores

El script retorna exit codes estándar:
- **0**: Proceso exitoso
- **1**: Error en el proceso

En n8n, puedes usar el exit code para controlar el flujo:
- Si exit code = 0 → Continuar con el siguiente nodo
- Si exit code = 1 → Ejecutar nodo de manejo de errores

## Logs

Los logs se guardan en el directorio `logs/` con el formato:
- `arqueos_cajeros_YYYYMMDD.log`

Puedes configurar n8n para leer estos logs si necesitas más detalles sobre el proceso.

## Notas Importantes

1. **Fecha Automática**: El sistema calcula automáticamente:
   - Fecha de proceso = Fecha actual
   - Fecha de arqueo = Fecha actual - 1 día

2. **Búsqueda de Archivos**: El sistema busca en `insumos_excel/` el archivo más reciente que coincida con el patrón:
   - `gestion_*_ksgarro.xlsx` (para gestion_ksgarro)
   - El archivo debe tener formato: `gestion_DD_MM_YYYY_ksgarro.xlsx`

3. **Archivos de Salida**: Los resultados se guardan en `output/` con el formato:
   - `arqueos_procesados_{nombre_insumo}_{fecha_proceso}.xlsx`

## Troubleshooting

### Error: "No se encontró archivo"
- Verifica que existan archivos en `insumos_excel/`
- Verifica que el formato del nombre sea correcto: `gestion_DD_MM_YYYY_*.xlsx`

### Error: "No hay insumos activos"
- Revisa `config/insumos.yaml`
- Asegúrate de que al menos un insumo tenga `activo: true`

### Error: "ModuleNotFoundError"
- Instala las dependencias: `pip install -r requirements.txt`
- Verifica que el entorno virtual esté activado si lo usas

