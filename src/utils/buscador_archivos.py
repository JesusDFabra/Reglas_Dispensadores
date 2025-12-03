"""
Utilidades para buscar y seleccionar archivos por fecha.
"""

import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
import logging

logger = logging.getLogger(__name__)


class BuscadorArchivos:
    """Clase para buscar archivos por patrón de fecha."""
    
    def __init__(self, directorio: Path):
        """
        Inicializa el buscador de archivos.
        
        Args:
            directorio: Directorio donde buscar archivos.
        """
        self.directorio = Path(directorio)
        if not self.directorio.exists():
            raise FileNotFoundError(f"El directorio no existe: {directorio}")
    
    def extraer_fecha_del_nombre(self, nombre_archivo: str) -> Optional[datetime]:
        """
        Extrae la fecha del nombre de archivo.
        
        Busca patrones como: gestion_DD_MM_YYYY_*.xlsx
        Ejemplo: gestion_21_11_2025_ksgarro.xlsx -> 2025-11-21
        
        Args:
            nombre_archivo: Nombre del archivo.
        
        Returns:
            Objeto datetime con la fecha o None si no se encuentra.
        """
        # Patrón: gestion_DD_MM_YYYY_*.xlsx
        patron = r'gestion_(\d{2})_(\d{2})_(\d{4})_'
        coincidencia = re.search(patron, nombre_archivo)
        
        if coincidencia:
            try:
                dia = int(coincidencia.group(1))
                mes = int(coincidencia.group(2))
                anio = int(coincidencia.group(3))
                fecha = datetime(anio, mes, dia)
                return fecha
            except ValueError as e:
                logger.warning(f"No se pudo parsear la fecha del archivo {nombre_archivo}: {e}")
                return None
        
        return None
    
    def buscar_archivos_por_patron(
        self, 
        patron: str,
        extension: str = ".xlsx"
    ) -> List[Tuple[Path, datetime]]:
        """
        Busca archivos que coincidan con un patrón y extrae sus fechas.
        
        Args:
            patron: Patrón a buscar (ej: "gestion_*_ksgarro")
            extension: Extensión del archivo.
        
        Returns:
            Lista de tuplas (Path, datetime) ordenadas por fecha descendente.
        """
        archivos_encontrados = []
        
        # Convertir patrón a regex
        patron_regex = patron.replace('*', '.*')
        patron_completo = f"{patron_regex}{extension}"
        
        logger.info(f"Buscando archivos con patrón: {patron_completo} en {self.directorio}")
        
        for archivo in self.directorio.glob(f"*{extension}"):
            if re.match(patron_regex, archivo.stem):
                fecha = self.extraer_fecha_del_nombre(archivo.name)
                if fecha:
                    archivos_encontrados.append((archivo, fecha))
                    logger.debug(f"Archivo encontrado: {archivo.name} - Fecha: {fecha.strftime('%Y-%m-%d')}")
        
        # Ordenar por fecha descendente (más reciente primero)
        archivos_encontrados.sort(key=lambda x: x[1], reverse=True)
        
        logger.info(f"Se encontraron {len(archivos_encontrados)} archivo(s) con el patrón")
        
        return archivos_encontrados
    
    def obtener_archivo_mas_reciente(
        self,
        patron: str,
        extension: str = ".xlsx"
    ) -> Optional[Path]:
        """
        Obtiene el archivo más reciente que coincida con el patrón.
        
        Args:
            patron: Patrón a buscar (ej: "gestion_*_ksgarro")
            extension: Extensión del archivo.
        
        Returns:
            Path al archivo más reciente o None si no se encuentra.
        """
        archivos = self.buscar_archivos_por_patron(patron, extension)
        
        if archivos:
            archivo_mas_reciente = archivos[0][0]
            fecha_mas_reciente = archivos[0][1]
            logger.info(
                f"Archivo más reciente encontrado: {archivo_mas_reciente.name} "
                f"(Fecha: {fecha_mas_reciente.strftime('%Y-%m-%d')})"
            )
            return archivo_mas_reciente
        
        logger.warning(f"No se encontró ningún archivo con el patrón: {patron}{extension}")
        return None


def calcular_fechas_proceso(fecha_referencia: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """
    Calcula las fechas de proceso y arqueo.
    
    La fecha de proceso es la fecha actual (o la fecha de referencia).
    La fecha de arqueo es el día anterior a la fecha de proceso.
    
    Args:
        fecha_referencia: Fecha de referencia. Si es None, usa la fecha actual.
    
    Returns:
        Tupla (fecha_proceso, fecha_arqueo)
    """
    if fecha_referencia is None:
        fecha_referencia = datetime.now()
    
    fecha_proceso = fecha_referencia.replace(hour=0, minute=0, second=0, microsecond=0)
    fecha_arqueo = fecha_proceso - timedelta(days=1)
    
    return fecha_proceso, fecha_arqueo

