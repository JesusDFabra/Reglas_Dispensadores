"""
Utilidades para calcular días hábiles y determinar qué registros procesar.
"""

from datetime import datetime, timedelta
import logging

try:
    import holidays_co
    TIENE_HOLIDAYS_CO = True
except ImportError:
    try:
        import holidays
        TIENE_HOLIDAYS = True
        TIENE_HOLIDAYS_CO = False
    except ImportError:
        TIENE_HOLIDAYS_CO = False
        TIENE_HOLIDAYS = False
        logger.warning("No se encontró holidays-co ni holidays. Los días festivos no se considerarán.")

logger = logging.getLogger(__name__)


def es_dia_habil(fecha: datetime) -> bool:
    """
    Determina si una fecha es un día hábil (lunes a viernes, excluyendo festivos de Colombia).
    
    Args:
        fecha: Fecha a verificar
        
    Returns:
        True si es día hábil, False si es fin de semana o festivo
    """
    # 0 = lunes, 6 = domingo
    if fecha.weekday() >= 5:  # Sábado o domingo
        return False
    
    # Verificar si es día festivo en Colombia
    if TIENE_HOLIDAYS_CO:
        try:
            if holidays_co.is_holiday_date(fecha.date()):
                return False
        except Exception as e:
            logger.warning(f"Error al verificar festivos con holidays-co: {e}")
    elif TIENE_HOLIDAYS:
        try:
            colombia_holidays = holidays.Colombia(years=fecha.year)
            if fecha.date() in colombia_holidays:
                return False
        except Exception as e:
            logger.warning(f"Error al verificar festivos con holidays: {e}")
    
    return True


def obtener_ultimo_dia_habil(fecha: datetime) -> datetime:
    """
    Obtiene el último día hábil anterior a la fecha dada.
    Considera días festivos de Colombia.
    
    Args:
        fecha: Fecha de referencia
        
    Returns:
        Último día hábil (lunes a viernes, excluyendo festivos)
    """
    fecha_actual = fecha
    
    # Retroceder hasta encontrar un día hábil (máximo 10 días para evitar loops infinitos)
    intentos = 0
    while not es_dia_habil(fecha_actual) and intentos < 10:
        fecha_actual = fecha_actual - timedelta(days=1)
        intentos += 1
    
    if intentos >= 10:
        logger.warning(f"No se pudo encontrar día hábil en los últimos 10 días desde {fecha}")
    
    return fecha_actual


def obtener_fechas_a_procesar(fecha_proceso: datetime) -> dict:
    """
    Determina qué fechas procesar según las reglas de negocio.
    
    Regla: Si el último día hábil fue el viernes, entonces hoy solo tener en cuenta:
    - Los arqueos y registros DIARIO del viernes
    - Los días sábado, domingo y lunes (si es festivo): solo los SOBRANTES
    
    Args:
        fecha_proceso: Fecha actual del proceso
        
    Returns:
        Diccionario con:
        - 'ultimo_dia_habil': datetime del último día hábil
        - 'fechas_arqueo_diario': lista de fechas para procesar ARQUEO y DIARIO
        - 'fechas_solo_sobrantes': lista de fechas para procesar solo SOBRANTES
        - 'procesar_solo_sobrantes': bool indicando si solo se procesan sobrantes
    """
    ultimo_dia_habil = obtener_ultimo_dia_habil(fecha_proceso)
    
    # Si el último día hábil fue viernes (weekday = 4)
    es_viernes = ultimo_dia_habil.weekday() == 4
    
    fechas_arqueo_diario = []
    fechas_solo_sobrantes = []
    
    if es_viernes:
        # Procesar viernes (ARQUEO y DIARIO)
        fechas_arqueo_diario.append(ultimo_dia_habil)
        
        # Procesar sábado, domingo y lunes (solo SOBRANTES)
        sabado = ultimo_dia_habil + timedelta(days=1)
        domingo = ultimo_dia_habil + timedelta(days=2)
        lunes = ultimo_dia_habil + timedelta(days=3)
        
        # Solo agregar si no han pasado de la fecha de proceso
        if sabado <= fecha_proceso:
            fechas_solo_sobrantes.append(sabado)
        if domingo <= fecha_proceso:
            fechas_solo_sobrantes.append(domingo)
        if lunes <= fecha_proceso and lunes.weekday() == 0:  # Solo si es lunes
            fechas_solo_sobrantes.append(lunes)
        
        # Agregar días festivos que caigan después del viernes y antes de la fecha de proceso
        fecha_actual = ultimo_dia_habil + timedelta(days=1)
        while fecha_actual <= fecha_proceso:
            # Si es festivo y no es día hábil, agregarlo a solo sobrantes
            if not es_dia_habil(fecha_actual) and fecha_actual.weekday() < 5:
                # Es un festivo en día de semana
                if fecha_actual not in fechas_solo_sobrantes:
                    fechas_solo_sobrantes.append(fecha_actual)
            fecha_actual = fecha_actual + timedelta(days=1)
    else:
        # Si no es viernes, procesar normalmente el último día hábil
        fechas_arqueo_diario.append(ultimo_dia_habil)
    
    return {
        'ultimo_dia_habil': ultimo_dia_habil,
        'fechas_arqueo_diario': fechas_arqueo_diario,
        'fechas_solo_sobrantes': fechas_solo_sobrantes,
        'procesar_solo_sobrantes': len(fechas_solo_sobrantes) > 0
    }


def obtener_ultimos_dias_habiles(fecha: datetime, cantidad: int = 2) -> list:
    """
    Obtiene los últimos N días hábiles anteriores a la fecha dada.
    Considera días festivos de Colombia.
    
    Args:
        fecha: Fecha de referencia
        cantidad: Cantidad de días hábiles a obtener (default: 2)
        
    Returns:
        Lista de fechas (datetime) de los últimos N días hábiles, ordenadas de más reciente a más antigua
    """
    dias_habiles = []
    fecha_actual = fecha - timedelta(days=1)  # Empezar desde el día anterior
    
    # Buscar hasta encontrar N días hábiles (máximo 30 días para evitar loops infinitos)
    intentos = 0
    while len(dias_habiles) < cantidad and intentos < 30:
        if es_dia_habil(fecha_actual):
            dias_habiles.append(fecha_actual)
        fecha_actual = fecha_actual - timedelta(days=1)
        intentos += 1
    
    if len(dias_habiles) < cantidad:
        logger.warning(
            f"No se pudieron encontrar {cantidad} días hábiles en los últimos 30 días desde {fecha}"
        )
    
    # Ordenar de más reciente a más antigua
    dias_habiles.sort(reverse=True)
    
    return dias_habiles


def debe_procesar_registro(
    fecha_arqueo: datetime,
    tipo_registro: str,
    tiene_sobrante: bool,
    tiene_faltante: bool,
    fecha_proceso: datetime
) -> bool:
    """
    Determina si un registro debe ser procesado según las reglas de días hábiles.
    
    Args:
        fecha_arqueo: Fecha del arqueo del registro
        tipo_registro: Tipo de registro ('ARQUEO' o 'DIARIO')
        tiene_sobrante: Si el registro tiene sobrante
        tiene_faltante: Si el registro tiene faltante
        fecha_proceso: Fecha actual del proceso
        
    Returns:
        True si el registro debe ser procesado, False en caso contrario
    """
    fechas_info = obtener_fechas_a_procesar(fecha_proceso)
    
    # Convertir fecha_arqueo a datetime si es string
    if isinstance(fecha_arqueo, str):
        try:
            fecha_arqueo = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
        except:
            try:
                fecha_arqueo = datetime.strptime(fecha_arqueo, '%Y-%m-%d %H:%M:%S')
            except:
                logger.warning(f"No se pudo parsear fecha_arqueo: {fecha_arqueo}")
                return True  # Por defecto, procesar si no se puede determinar
    
    # Normalizar fecha_arqueo a solo fecha (sin hora)
    fecha_arqueo = fecha_arqueo.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Verificar si está en fechas de ARQUEO/DIARIO
    if fecha_arqueo in fechas_info['fechas_arqueo_diario']:
        return True
    
    # Verificar si está en fechas de solo SOBRANTES
    if fecha_arqueo in fechas_info['fechas_solo_sobrantes']:
        # Solo procesar si tiene sobrante (no faltante)
        return tiene_sobrante and not tiene_faltante
    
    # Si no está en ninguna lista, no procesar
    return False

