"""
Configuración de logging para el proyecto.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


def configurar_logger(
    nombre_logger: str = "arqueo_cajeros",
    nivel: int = logging.INFO,
    log_archivo: bool = True,
    log_consola: bool = True
) -> logging.Logger:
    """
    Configura el logger del proyecto.
    
    Args:
        nombre_logger: Nombre del logger.
        nivel: Nivel de logging (logging.INFO, logging.DEBUG, etc.).
        log_archivo: Si True, guarda logs en archivo.
        log_consola: Si True, muestra logs en consola.
    
    Returns:
        Logger configurado.
    """
    logger = logging.getLogger(nombre_logger)
    logger.setLevel(nivel)
    
    # Evitar duplicar handlers si ya están configurados
    if logger.handlers:
        return logger
    
    # Formato de los mensajes
    formato = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para consola
    if log_consola:
        consola_handler = logging.StreamHandler(sys.stdout)
        consola_handler.setLevel(nivel)
        consola_handler.setFormatter(formato)
        logger.addHandler(consola_handler)
    
    # Handler para archivo
    if log_archivo:
        proyecto_root = Path(__file__).parent.parent.parent
        directorio_logs = proyecto_root / "logs"
        directorio_logs.mkdir(parents=True, exist_ok=True)
        
        fecha_actual = datetime.now().strftime("%Y%m%d")
        archivo_log = directorio_logs / f"{nombre_logger}_{fecha_actual}.log"
        
        archivo_handler = logging.FileHandler(archivo_log, encoding='utf-8')
        archivo_handler.setLevel(nivel)
        archivo_handler.setFormatter(formato)
        logger.addHandler(archivo_handler)
    
    return logger

