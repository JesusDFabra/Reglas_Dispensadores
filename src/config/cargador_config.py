"""
Módulo para cargar y validar la configuración desde archivos YAML.
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class CargadorConfig:
    """Clase para cargar y gestionar la configuración del proyecto."""
    
    def __init__(
        self, 
        ruta_config: Optional[str] = None,
        usar_fecha_actual: bool = True,
        fecha_referencia: Optional[datetime] = None
    ):
        """
        Inicializa el cargador de configuración.
        
        Args:
            ruta_config: Ruta al archivo YAML de configuración.
                        Si es None, busca en config/insumos.yaml
            usar_fecha_actual: Si es True, calcula fechas automáticamente desde la fecha actual.
            fecha_referencia: Fecha de referencia para cálculos. Si es None y usar_fecha_actual=True,
                            usa datetime.now()
        """
        if ruta_config is None:
            # Buscar el archivo de configuración relativo al directorio del proyecto
            proyecto_root = Path(__file__).parent.parent.parent
            ruta_config = proyecto_root / "config" / "insumos.yaml"
        
        self.ruta_config = Path(ruta_config)
        self._config: Optional[Dict[str, Any]] = None
        self.usar_fecha_actual = usar_fecha_actual
        self.fecha_referencia = fecha_referencia or datetime.now() if usar_fecha_actual else None
        
        if not self.ruta_config.exists():
            raise FileNotFoundError(
                f"No se encontró el archivo de configuración: {self.ruta_config}"
            )
    
    def cargar(self) -> Dict[str, Any]:
        """
        Carga la configuración desde el archivo YAML.
        
        Si usar_fecha_actual es True, actualiza las fechas automáticamente.
        
        Returns:
            Diccionario con la configuración cargada.
        """
        if self._config is None:
            try:
                with open(self.ruta_config, 'r', encoding='utf-8') as archivo:
                    self._config = yaml.safe_load(archivo)
                logger.info(f"Configuración cargada desde: {self.ruta_config}")
                
                # Actualizar fechas si se usa fecha actual
                if self.usar_fecha_actual:
                    self._actualizar_fechas_automaticas()
                    
            except yaml.YAMLError as e:
                logger.error(f"Error al parsear YAML: {e}")
                raise
            except Exception as e:
                logger.error(f"Error al cargar configuración: {e}")
                raise
        
        return self._config
    
    def _actualizar_fechas_automaticas(self):
        """Actualiza las fechas de proceso y arqueo basándose en la fecha actual."""
        if 'proceso' not in self._config:
            self._config['proceso'] = {}
        
        fecha_proceso = self.fecha_referencia.replace(hour=0, minute=0, second=0, microsecond=0)
        fecha_arqueo = fecha_proceso - timedelta(days=1)
        
        self._config['proceso']['fecha_proceso'] = fecha_proceso.strftime('%Y-%m-%d')
        self._config['proceso']['fecha_arqueo'] = fecha_arqueo.strftime('%Y-%m-%d')
        
        logger.info(
            f"Fechas actualizadas automáticamente - "
            f"Proceso: {self._config['proceso']['fecha_proceso']}, "
            f"Arqueo: {self._config['proceso']['fecha_arqueo']}"
        )
    
    def obtener_insumos_activos(self) -> Dict[str, Any]:
        """
        Obtiene solo los insumos marcados como activos.
        
        Returns:
            Diccionario con los insumos activos.
        """
        config = self.cargar()
        insumos = config.get('insumos', {})
        
        insumos_activos = {
            nombre: datos 
            for nombre, datos in insumos.items() 
            if datos.get('activo', False)
        }
        
        logger.info(f"Se encontraron {len(insumos_activos)} insumo(s) activo(s)")
        return insumos_activos
    
    def obtener_ruta_insumo(
        self, 
        nombre_insumo: str,
        buscar_mas_reciente: bool = False,
        fecha_especifica: Optional[str] = None
    ) -> Path:
        """
        Obtiene la ruta completa de un insumo.
        
        Args:
            nombre_insumo: Nombre del insumo en la configuración.
            buscar_mas_reciente: Si es True, busca el archivo más reciente en insumos_excel
                                en lugar de usar la ruta estática.
            fecha_especifica: Fecha específica en formato DD_MM_YYYY (ej: "28_11_2025").
                            Si se proporciona, busca ese archivo específico.
        
        Returns:
            Path completo al archivo del insumo.
        """
        config = self.cargar()
        insumos = config.get('insumos', {})
        
        if nombre_insumo not in insumos:
            raise KeyError(f"Insumo '{nombre_insumo}' no encontrado en la configuración")
        
        # Si se especifica una fecha, buscar ese archivo específico
        if fecha_especifica:
            directorios = config.get('directorios', {})
            insumos_excel = directorios.get('insumos_excel', 'insumos_excel')
            proyecto_root = Path(__file__).parent.parent.parent
            directorio_insumos = proyecto_root / insumos_excel
            
            # Extraer el sufijo después de la fecha (ej: _ksgarro.xlsx)
            ruta_original = insumos[nombre_insumo]['ruta']
            import re
            patron_sufijo = r'gestion_\d{2}_\d{2}_\d{4}(.*)'
            match = re.search(patron_sufijo, ruta_original)
            if match:
                sufijo = match.group(1)  # ej: _ksgarro.xlsx
                nombre_archivo = f"gestion_{fecha_especifica}{sufijo}"
                ruta_archivo = directorio_insumos / nombre_archivo
                
                if ruta_archivo.exists():
                    logger.info(f"Usando archivo específico: {ruta_archivo.name}")
                    return ruta_archivo
                else:
                    raise FileNotFoundError(
                        f"No se encontró el archivo específico: {nombre_archivo} "
                        f"en {directorio_insumos}"
                    )
            else:
                raise ValueError(
                    f"No se pudo extraer el patrón de fecha del insumo {nombre_insumo}"
                )
        
        if buscar_mas_reciente:
            # Buscar archivo más reciente en insumos_excel
            directorios = config.get('directorios', {})
            insumos_excel = directorios.get('insumos_excel', 'insumos_excel')
            proyecto_root = Path(__file__).parent.parent.parent
            directorio_insumos = proyecto_root / insumos_excel
            
            # Extraer patrón del nombre del insumo (ej: gestion_*_ksgarro)
            ruta_original = insumos[nombre_insumo]['ruta']
            # Extraer el sufijo después de la fecha (ej: _ksgarro.xlsx)
            import re
            patron_sufijo = r'gestion_\d{2}_\d{2}_\d{4}(.*)'
            match = re.search(patron_sufijo, ruta_original)
            if match:
                sufijo = match.group(1)  # ej: _ksgarro.xlsx
                patron_busqueda = f"gestion_*{sufijo.replace('.xlsx', '')}"
                
                from src.utils.buscador_archivos import BuscadorArchivos
                buscador = BuscadorArchivos(directorio_insumos)
                archivo_mas_reciente = buscador.obtener_archivo_mas_reciente(
                    patron_busqueda, 
                    extension='.xlsx'
                )
                
                if archivo_mas_reciente:
                    logger.info(f"Usando archivo más reciente: {archivo_mas_reciente.name}")
                    return archivo_mas_reciente
                else:
                    logger.warning(
                        f"No se encontró archivo reciente con patrón {patron_busqueda}. "
                        f"Usando ruta estática."
                    )
        
        # Usar ruta estática
        ruta_relativa = insumos[nombre_insumo]['ruta']
        directorios = config.get('directorios', {})
        datos_entrada = directorios.get('datos_entrada', '.')
        
        proyecto_root = Path(__file__).parent.parent.parent
        ruta_completa = proyecto_root / datos_entrada / ruta_relativa
        
        return ruta_completa
    
    def obtener_tipo_registro_filtro(self, nombre_insumo: str) -> str:
        """
        Obtiene el tipo de registro para filtrar de un insumo.
        
        Args:
            nombre_insumo: Nombre del insumo en la configuración.
        
        Returns:
            Tipo de registro para filtrar.
        """
        config = self.cargar()
        insumos = config.get('insumos', {})
        proceso = config.get('proceso', {})
        
        if nombre_insumo not in insumos:
            raise KeyError(f"Insumo '{nombre_insumo}' no encontrado en la configuración")
        
        tipo_registro = insumos[nombre_insumo].get(
            'tipo_registro_filtro',
            proceso.get('tipo_registro_default', 'ARQUEO')
        )
        
        return tipo_registro

