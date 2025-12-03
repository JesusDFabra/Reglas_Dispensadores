"""
Utilidades para extraer fechas de identificadores de arqueo.
"""
import pandas as pd
from datetime import datetime
from typing import Optional, Any


def extraer_fecha_desde_arqid(arqid: str) -> Optional[datetime]:
    """
    Extrae la fecha del arqid cuando fecha_arqueo está vacía.
    Formato: ARQ + codigo_cajero (4 dígitos) + DDMMYYYY (8 dígitos) + ...
    
    Args:
        arqid: String con el formato ARQ{codigo_cajero}{DDMMYYYY}...
    
    Returns:
        datetime o None si no se puede extraer
    """
    if pd.isna(arqid) or not isinstance(arqid, str) or len(arqid) < 15:
        return None
    
    try:
        # ARQ (3) + codigo_cajero (4) = posición 7
        # Fecha DDMMYYYY empieza en posición 7 y tiene 8 caracteres
        fecha_str = arqid[7:15]  # DDMMYYYY
        
        if len(fecha_str) == 8 and fecha_str.isdigit():
            dia = int(fecha_str[0:2])
            mes = int(fecha_str[2:4])
            anio = int(fecha_str[4:8])
            
            # Validar que la fecha sea válida
            fecha = datetime(anio, mes, dia)
            return fecha
    except (ValueError, IndexError) as e:
        return None
    
    return None


def obtener_fecha_arqueo(arqid: Optional[str], fecha_arqueo: Optional[Any]) -> Optional[datetime]:
    """
    Obtiene la fecha del arqueo desde fecha_arqueo o desde el arqid si está vacía.
    
    Args:
        arqid: Identificador del arqueo (formato ARQ...)
        fecha_arqueo: Fecha del arqueo (puede ser string, datetime, o None)
    
    Returns:
        datetime o None si no se puede obtener
    """
    # Primero intentar usar fecha_arqueo
    if pd.notna(fecha_arqueo) and fecha_arqueo != '':
        try:
            if isinstance(fecha_arqueo, str):
                fecha = pd.to_datetime(fecha_arqueo, errors='coerce')
            elif isinstance(fecha_arqueo, datetime):
                fecha = fecha_arqueo
            else:
                fecha = pd.to_datetime(fecha_arqueo, errors='coerce')
            
            if pd.notna(fecha):
                if isinstance(fecha, pd.Timestamp):
                    return fecha.to_pydatetime()
                elif isinstance(fecha, datetime):
                    return fecha
        except Exception:
            pass
    
    # Si fecha_arqueo está vacía, intentar extraer del arqid
    if arqid:
        fecha_desde_arqid = extraer_fecha_desde_arqid(arqid)
        if fecha_desde_arqid:
            return fecha_desde_arqid
    
    return None

