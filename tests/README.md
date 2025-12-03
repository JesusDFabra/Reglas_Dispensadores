# Tests y Pruebas

Esta carpeta contiene scripts de prueba y ejemplos para el proyecto de arqueo de cajeros.

## Archivos

### `prueba_arqueos.py`
Script de prueba que muestra los primeros 10 códigos de cajeros con `tipo_registro = ARQUEO` y sus valores de Sobrante/Faltante.

**Uso:**
```bash
python tests/prueba_arqueos.py
```

**Salida:**
- Lista de los primeros 10 códigos de cajeros
- Valores de sobrantes y faltantes
- Resumen estadístico completo

### `ejemplo_uso.py`
Script de ejemplo que demuestra cómo usar el procesador de arqueos.

**Uso:**
```bash
python tests/ejemplo_uso.py
```

**Contenido:**
- Ejemplo de carga de configuración
- Ejemplo de procesamiento de insumos
- Muestra de resultados

## Ejecución

Desde el directorio raíz del proyecto:

```bash
# Ejecutar prueba de arqueos
python tests/prueba_arqueos.py

# Ejecutar ejemplo de uso
python tests/ejemplo_uso.py
```

O desde la carpeta tests:

```bash
cd tests
python prueba_arqueos.py
python ejemplo_uso.py
```

## Notas

- Los scripts están configurados para usar rutas relativas al directorio raíz del proyecto
- Todos los scripts usan el sistema de logging configurado
- Los resultados se muestran tanto en consola como en los logs

