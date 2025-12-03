# Configuración de Base de Datos ODBC

Este documento explica cómo configurar la conexión a la base de datos para consultar movimientos nacionales.

## Configuración

### 1. Configurar en `config/insumos.yaml`

**Líneas 64-75** en `config/insumos.yaml`:

```yaml
base_datos:
  usar_bd: true  # Cambiar a true para activar consultas a BD
  servidor: "NACIONAL"  # DSN del servidor ODBC
  usuario: "tu_usuario"  # Usuario para conexión
  clave: "tu_clave"  # Contraseña para conexión
  query_params:
    cuenta: 110505075
    anio: 2025
    mes_inicio: 6
    mes_fin: 12
    codofi_excluir: 976
    nrocmp: 770500
```

### 2. Requisitos Previos

1. **DSN ODBC configurado**: Debe existir un DSN llamado "NACIONAL" en el sistema
2. **Credenciales**: Usuario y contraseña válidos para la base de datos
3. **Dependencias**: `pyodbc` debe estar instalado (ya está en `requirements.txt`)

## Cómo Funciona

### Modo Excel (por defecto)
- `usar_bd: false` → Consulta en `NACIONAL_movimientos.xlsx`

### Modo Base de Datos
- `usar_bd: true` → Consulta directamente en la base de datos usando ODBC

## Servidores Disponibles

El sistema soporta tres tipos de servidores (basado en `admin_bd.py`):

1. **NACIONAL**: Servidor nacional (requiere usuario y clave)
2. **MEDELLIN**: Servidor Medellín (requiere usuario y clave)
3. **LZ**: Servidor LZ/IMPALA_PROD (sin autenticación)

## Query Ejecutada

Cuando `usar_bd: true`, se ejecuta la siguiente consulta:

```sql
SELECT  ANOELB, 
        MESELB, 
        DIAELB, 
        CODOFI, 
        (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) as CUENTA, 
        NIT, 
        NUMDOC, 
        NROCMP, 
        (ANOELB*10000+MESELB*100+DIAELB) as FECHA, 
        VALOR 
FROM gcolibranl.gcoffmvint 
WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC in (110505075)) 
    and ANOELB = 2025 
    and MESELB between 6 and 12
    and DIAELB between 01 and 31 
    and CODOFI <> 976
    and NROCMP = 770500
    and NIT = {codigo_cajero}
    and (ANOELB*10000+MESELB*100+DIAELB) = {fecha_arqueo_formateada}
```

## Parámetros de la Query

Los parámetros se configuran en `query_params`:

- **cuenta**: Número de cuenta (default: 110505075)
- **anio**: Año de consulta (default: 2025)
- **mes_inicio**: Mes inicial del rango (default: 6)
- **mes_fin**: Mes final del rango (default: 12)
- **codofi_excluir**: Código de oficina a excluir (default: 976)
- **nrocmp**: Número de comprobante (default: 770500)

## Fallback Automático

Si `usar_bd: true` pero hay un error en la conexión o consulta, el sistema automáticamente:
1. Registra el error en los logs
2. Intenta usar el archivo Excel como fallback
3. Continúa el proceso normalmente

## Ejemplo de Configuración

```yaml
base_datos:
  usar_bd: true
  servidor: "NACIONAL"
  usuario_nal: "usuario_bd"
  clave_nal: "password_bd"
  usuario_med: ""  # Opcional para uso futuro
  clave_med: ""    # Opcional para uso futuro
  query_params:
    cuenta: 110505075
    anio: 2025
    mes_inicio: 6
    mes_fin: 12
    codofi_excluir: 976
    nrocmp: 770500
```

## Metodología Replicada de CertificacionArqueo

### Clases Implementadas

1. **`AdminBD`**: Clase base que replica exactamente `CertificacionArqueo/utilidades/admin_bd.py`
   - Método `conectar()`: Establece conexión ODBC con DSN, CCSID=37, TRANSLATE=1
   - Método `consultar()`: Ejecuta consulta SQL y retorna DataFrame (siempre llama a `conectar()` antes)

2. **`AdminBDNacional`**: Hereda de `AdminBD` con DSN="NACIONAL"

3. **`AdminBDMedellin`**: Hereda de `AdminBD` con DSN="MEDELLIN"

4. **`AdminBDLZ`**: Hereda de `AdminBD` con conexión especial a IMPALA_PROD (sin autenticación)

### Patrón de Uso

El patrón es idéntico a `CertificacionArqueo/proyectos/arqueos_cajeros_sucursales/capa_datos/admin_consultas.py`:

```python
# Inicialización
bd_nal = AdminBDNacional(usuario_nal, clave_nal)

# Consulta (siempre conecta antes de consultar)
df = bd_nal.consultar(consulta_sql)
```

### Formato de Consultas SQL

Las consultas usan `CAST` para compatibilidad, igual que en CertificacionArqueo:

```sql
WHERE CAST(CLASE*100000000 as INT)+CAST(GRUPO*10000000 as INT)+...
    and CAST(anoelb*10000 as INT)+CAST(meselb*100 AS INT)+CAST(diaelb as INT) = {fecha}
```

## Verificación

Para verificar que la conexión funciona:

1. Configura `usar_bd: true` y las credenciales
2. Ejecuta el proceso:
   ```bash
   python main.py
   ```
3. Revisa los logs en `logs/` para ver si la conexión fue exitosa

## Troubleshooting

### Error: "DSN not found"
- Verifica que el DSN "NACIONAL" esté configurado en el Administrador de Orígenes de Datos ODBC de Windows

### Error: "Login failed"
- Verifica que `usuario_nal` y `clave_nal` sean correctos
- Verifica que el usuario tenga permisos para consultar la tabla `gcolibranl.gcoffmvint`

### Error: "pyodbc not installed"
- Instala las dependencias: `pip install -r requirements.txt`

### El sistema sigue usando Excel
- Verifica que `usar_bd: true` esté configurado
- Verifica que `usuario_nal` y `clave_nal` no estén vacíos
- Revisa los logs para ver mensajes de advertencia

## Compatibilidad con CertificacionArqueo

Esta implementación es 100% compatible con la metodología de `CertificacionArqueo`:
- ✅ Mismas clases `AdminBD`, `AdminBDNacional`, `AdminBDMedellin`
- ✅ Mismo formato de conexión ODBC (DSN, CCSID=37, TRANSLATE=1)
- ✅ Mismo patrón de consultas SQL con `CAST`
- ✅ Mismo uso de `pandas.read_sql()` en el método `consultar()`
- ✅ Mismo formato de credenciales (`usuario_nal`, `clave_nal`)

Cuando tengas acceso a la base de datos, simplemente configura `usar_bd: true` y las credenciales, y funcionará inmediatamente.


