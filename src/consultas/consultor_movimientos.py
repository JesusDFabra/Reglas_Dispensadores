"""
Módulo para consultar movimientos en archivos Excel y bases de datos.
Por ahora consulta en archivos Excel, pero está preparado para migrar a consultas ODBC.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
import sys

sys.path.append(str(Path(__file__).parent.parent))

from src.config.cargador_config import CargadorConfig
from src.consultas.consultor_bd import ConsultorBD
from src.utils.extraccion_fechas import obtener_fecha_arqueo

logger = logging.getLogger(__name__)


class ConsultorMovimientos:
    """Clase para consultar movimientos en archivos Excel y bases de datos."""
    
    def __init__(self, config: Optional[CargadorConfig] = None):
        """
        Inicializa el consultor de movimientos.
        
        Args:
            config: Instancia de CargadorConfig. Si es None, crea una nueva.
        """
        self.config = config or CargadorConfig()
        self._df_nacional: Optional[pd.DataFrame] = None
        self._df_sobrantes: Optional[pd.DataFrame] = None
        self._df_historico: Optional[pd.DataFrame] = None
        self._df_faltantes: Optional[pd.DataFrame] = None
        self._df_historico_faltantes: Optional[pd.DataFrame] = None
        self._df_historico_cuadre: Optional[pd.DataFrame] = None  # Histórico CSV
        self._consultor_bd: Optional[ConsultorBD] = None
        self._usar_bd: bool = False
        
        # Verificar si se debe usar BD
        try:
            config_data = self.config.cargar()
            base_datos_config = config_data.get('base_datos', {})
            self._usar_bd = base_datos_config.get('usar_bd', False)
            
            if self._usar_bd:
                # Replicar patrón de CertificacionArqueo: usuario_nal y clave_nal
                usuario_nal = base_datos_config.get('usuario_nal', '')
                clave_nal = base_datos_config.get('clave_nal', '')
                
                if usuario_nal and clave_nal:
                    self._consultor_bd = ConsultorBD(usuario_nal, clave_nal)
                    logger.info("ConsultorMovimientos configurado para usar base de datos NACIONAL")
                else:
                    logger.warning("usar_bd está activo pero faltan credenciales (usuario_nal/clave_nal). Usando archivos Excel.")
                    self._usar_bd = False
        except Exception as e:
            logger.warning(f"Error al verificar configuración de BD: {e}. Usando archivos Excel.")
            self._usar_bd = False
    
    def _cargar_archivo_nacional(self) -> pd.DataFrame:
        """
        Carga el archivo NACIONAL_movimientos.xlsx.
        
        Returns:
            DataFrame con los movimientos nacionales.
        """
        if self._df_nacional is None:
            try:
                config_data = self.config.cargar()
                insumos = config_data.get('insumos', {})
                
                if 'movimientos_nacional' not in insumos:
                    raise KeyError("No se encontró 'movimientos_nacional' en la configuración")
                
                ruta_relativa = insumos['movimientos_nacional']['ruta']
                directorios = config_data.get('directorios', {})
                # Buscar primero en insumos_excel, luego en datos_entrada
                insumos_excel = directorios.get('insumos_excel', 'insumos_excel')
                datos_entrada = directorios.get('datos_entrada', '.')
                
                proyecto_root = Path(__file__).parent.parent.parent
                # Intentar primero en insumos_excel
                ruta_archivo = proyecto_root / insumos_excel / ruta_relativa
                if not ruta_archivo.exists():
                    # Si no existe, buscar en datos_entrada
                    ruta_archivo = proyecto_root / datos_entrada / ruta_relativa
                
                if not ruta_archivo.exists():
                    raise FileNotFoundError(f"Archivo no encontrado: {ruta_archivo}")
                
                logger.info(f"Cargando archivo NACIONAL: {ruta_archivo}")
                self._df_nacional = pd.read_excel(ruta_archivo)
                logger.info(f"Archivo NACIONAL cargado: {len(self._df_nacional)} registros")
                
            except Exception as e:
                logger.error(f"Error al cargar archivo NACIONAL: {e}")
                raise
        
        return self._df_nacional
    
    def _cargar_archivo_sobrantes(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Carga el archivo SOBRANTES SUCURSALES y sus hojas.
        
        Returns:
            Tupla (DataFrame de SOBRANTE, DataFrame de HISTORICO)
        """
        if self._df_sobrantes is None or self._df_historico is None:
            try:
                config_data = self.config.cargar()
                insumos = config_data.get('insumos', {})
                
                if 'sobrantes' not in insumos:
                    raise KeyError("No se encontró 'sobrantes' en la configuración")
                
                ruta_relativa = insumos['sobrantes']['ruta']
                directorios = config_data.get('directorios', {})
                # Buscar primero en insumos_excel, luego en datos_entrada
                insumos_excel = directorios.get('insumos_excel', 'insumos_excel')
                datos_entrada = directorios.get('datos_entrada', '.')
                
                proyecto_root = Path(__file__).parent.parent.parent
                # Intentar primero en insumos_excel
                ruta_archivo = proyecto_root / insumos_excel / ruta_relativa
                if not ruta_archivo.exists():
                    # Si no existe, buscar en datos_entrada
                    ruta_archivo = proyecto_root / datos_entrada / ruta_relativa
                
                if not ruta_archivo.exists():
                    raise FileNotFoundError(f"Archivo no encontrado: {ruta_archivo}")
                
                logger.info(f"Cargando archivo SOBRANTES: {ruta_archivo}")
                
                # Cargar hoja SOBRANTE CTA 279510020
                try:
                    self._df_sobrantes = pd.read_excel(
                        ruta_archivo, 
                        sheet_name='SOBRANTE CTA 279510020'
                    )
                    logger.info(f"Hoja SOBRANTE cargada: {len(self._df_sobrantes)} registros")
                except Exception as e:
                    logger.warning(f"No se pudo cargar hoja SOBRANTE: {e}")
                    self._df_sobrantes = pd.DataFrame()
                
                # Cargar hoja HISTORICO 279510020
                try:
                    self._df_historico = pd.read_excel(
                        ruta_archivo,
                        sheet_name='HISTORICO 279510020'
                    )
                    logger.info(f"Hoja HISTORICO cargada: {len(self._df_historico)} registros")
                except Exception as e:
                    logger.warning(f"No se pudo cargar hoja HISTORICO: {e}")
                    self._df_historico = pd.DataFrame()
                
            except Exception as e:
                logger.error(f"Error al cargar archivo SOBRANTES: {e}")
                raise
        
        return self._df_sobrantes, self._df_historico
    
    def _cargar_archivo_faltantes(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Carga el archivo FALTANTES SUCURSALES y sus hojas.
        
        Returns:
            Tupla (DataFrame de FORMATO FALTANTES, DataFrame de HISTORICO FALTANTES)
        """
        if self._df_faltantes is None or self._df_historico_faltantes is None:
            try:
                config_data = self.config.cargar()
                insumos = config_data.get('insumos', {})
                
                if 'faltantes' not in insumos:
                    raise KeyError("No se encontró 'faltantes' en la configuración")
                
                ruta_relativa = insumos['faltantes']['ruta']
                directorios = config_data.get('directorios', {})
                # Buscar primero en insumos_excel, luego en datos_entrada
                insumos_excel = directorios.get('insumos_excel', 'insumos_excel')
                datos_entrada = directorios.get('datos_entrada', '.')
                
                proyecto_root = Path(__file__).parent.parent.parent
                # Intentar primero en insumos_excel
                ruta_archivo = proyecto_root / insumos_excel / ruta_relativa
                if not ruta_archivo.exists():
                    # Si no existe, buscar en datos_entrada
                    ruta_archivo = proyecto_root / datos_entrada / ruta_relativa
                
                if not ruta_archivo.exists():
                    raise FileNotFoundError(f"Archivo no encontrado: {ruta_archivo}")
                
                logger.info(f"Cargando archivo FALTANTES: {ruta_archivo}")
                
                # Cargar hoja FORMATO FALTANTES
                try:
                    self._df_faltantes = pd.read_excel(
                        ruta_archivo, 
                        sheet_name='FORMATO FALTANTES'
                    )
                    logger.info(f"Hoja FORMATO FALTANTES cargada: {len(self._df_faltantes)} registros")
                except Exception as e:
                    logger.warning(f"No se pudo cargar hoja FORMATO FALTANTES: {e}")
                    self._df_faltantes = pd.DataFrame()
                
                # Cargar hoja HISTORICO FALTANTES
                try:
                    self._df_historico_faltantes = pd.read_excel(
                        ruta_archivo,
                        sheet_name='HISTORICO FALTANTES'
                    )
                    logger.info(f"Hoja HISTORICO FALTANTES cargada: {len(self._df_historico_faltantes)} registros")
                except Exception as e:
                    logger.warning(f"No se pudo cargar hoja HISTORICO FALTANTES: {e}")
                    self._df_historico_faltantes = pd.DataFrame()
                
            except Exception as e:
                logger.error(f"Error al cargar archivo FALTANTES: {e}")
                raise
        
        return self._df_faltantes, self._df_historico_faltantes
    
    def _cargar_historico_cuadre(self) -> pd.DataFrame:
        """
        Carga el archivo Excel HISTORICO_CUADRE_CAJEROS_SUCURSALES.xlsx.
        
        Returns:
            DataFrame con el histórico de cuadres de cajeros.
        """
        if self._df_historico_cuadre is None:
            try:
                proyecto_root = Path(__file__).parent.parent.parent
                ruta_archivo = proyecto_root / 'insumos_excel' / 'HISTORICO_CUADRE_CAJEROS_SUCURSALES.xlsx'
                
                if not ruta_archivo.exists():
                    logger.warning(f"Archivo histórico no encontrado: {ruta_archivo}")
                    self._df_historico_cuadre = pd.DataFrame()
                    return self._df_historico_cuadre
                
                logger.info(f"Cargando archivo histórico: {ruta_archivo}")
                self._df_historico_cuadre = pd.read_excel(
                    ruta_archivo,
                    engine='openpyxl'
                )
                
                # Convertir codigo_cajero a numérico si es string
                if 'codigo_cajero' in self._df_historico_cuadre.columns:
                    self._df_historico_cuadre['codigo_cajero'] = pd.to_numeric(
                        self._df_historico_cuadre['codigo_cajero'],
                        errors='coerce'
                    )
                
                # Normalizar fechas: si fecha_arqueo está vacía, extraer del arqid
                if 'arqid' in self._df_historico_cuadre.columns and 'fecha_arqueo' in self._df_historico_cuadre.columns:
                    # Aplicar función para obtener fecha desde arqid si fecha_arqueo está vacía
                    mask_vacios = (
                        self._df_historico_cuadre['fecha_arqueo'].isna() |
                        (self._df_historico_cuadre['fecha_arqueo'] == '')
                    )
                    
                    if mask_vacios.any():
                        logger.info(f"Normalizando {mask_vacios.sum()} fechas desde arqid")
                        for idx in self._df_historico_cuadre[mask_vacios].index:
                            arqid = self._df_historico_cuadre.loc[idx, 'arqid']
                            fecha_arqueo = self._df_historico_cuadre.loc[idx, 'fecha_arqueo']
                            fecha_normalizada = obtener_fecha_arqueo(arqid, fecha_arqueo)
                            if fecha_normalizada:
                                self._df_historico_cuadre.loc[idx, 'fecha_arqueo'] = fecha_normalizada.strftime('%Y-%m-%d')
                
                logger.info(f"Archivo histórico cargado: {len(self._df_historico_cuadre)} registros")
                
            except Exception as e:
                logger.error(f"Error al cargar archivo histórico: {e}")
                self._df_historico_cuadre = pd.DataFrame()
        
        return self._df_historico_cuadre
    
    def buscar_en_historico_cuadre(
        self,
        codigo_cajero: int,
        fecha_arqueo: Optional[str] = None,
        valor_descuadre: Optional[float] = None,
        tipo_registro: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Busca registros en el histórico de cuadres de cajeros.
        
        Args:
            codigo_cajero: Código del cajero a buscar
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD (opcional, para filtrar)
            valor_descuadre: Valor del descuadre (opcional, para filtrar)
            tipo_registro: Tipo de registro (ARQUEO o DIARIO) (opcional, para filtrar)
        
        Returns:
            DataFrame con los registros encontrados o None si no se encuentra
        """
        try:
            df_historico = self._cargar_historico_cuadre()
            
            if df_historico.empty:
                logger.debug("Archivo histórico está vacío")
                return None
            
            # Filtrar por código de cajero
            filtro = df_historico['codigo_cajero'] == codigo_cajero
            
            # Filtrar por fecha si se proporciona
            if fecha_arqueo:
                try:
                    fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
                    # Convertir fecha_arqueo del DataFrame a datetime para comparar
                    df_historico['fecha_arqueo_dt'] = pd.to_datetime(
                        df_historico['fecha_arqueo'],
                        errors='coerce'
                    )
                    filtro = filtro & (df_historico['fecha_arqueo_dt'].dt.date == fecha_obj.date())
                except Exception as e:
                    logger.debug(f"Error al filtrar por fecha: {e}")
            
            # Filtrar por valor si se proporciona
            if valor_descuadre is not None:
                # Buscar en sobrantes o faltantes
                if valor_descuadre < 0:  # Sobrante
                    filtro = filtro & (
                        (df_historico['sobrantes'] == valor_descuadre) |
                        (df_historico['sobrantes'].abs() == abs(valor_descuadre))
                    )
                else:  # Faltante
                    filtro = filtro & (
                        (df_historico['faltantes'] == valor_descuadre) |
                        (df_historico['faltantes'].abs() == abs(valor_descuadre))
                    )
            
            # Filtrar por tipo de registro si se proporciona
            if tipo_registro:
                filtro = filtro & (df_historico['tipo_registro'] == tipo_registro)
            
            resultados = df_historico[filtro]
            
            if len(resultados) == 0:
                logger.debug(
                    f"No se encontró en histórico: cajero={codigo_cajero}, "
                    f"fecha={fecha_arqueo}, valor={valor_descuadre}"
                )
                return None
            
            # Eliminar columna temporal si existe
            if 'fecha_arqueo_dt' in resultados.columns:
                resultados = resultados.drop(columns=['fecha_arqueo_dt'])
            
            logger.info(
                f"Encontrado en histórico: {len(resultados)} registros para cajero={codigo_cajero}"
            )
            
            return resultados
        
        except Exception as e:
            logger.error(f"Error al buscar en histórico: {e}")
            return None
    
    def buscar_arqueo_en_historico_rango_fechas(
        self,
        codigo_cajero: int,
        fechas: List[datetime],
        tipo_registro: str = 'ARQUEO'
    ) -> Optional[pd.DataFrame]:
        """
        Busca registros de ARQUEO en el histórico de cuadres para un cajero en un rango de fechas.
        
        Args:
            codigo_cajero: Código del cajero a buscar
            fechas: Lista de fechas (datetime) en las que buscar
            tipo_registro: Tipo de registro a buscar (default: 'ARQUEO')
        
        Returns:
            DataFrame con los registros encontrados o None si no se encuentra
        """
        try:
            df_historico = self._cargar_historico_cuadre()
            
            if df_historico.empty:
                logger.debug("Archivo histórico está vacío")
                return None
            
            # Filtrar por código de cajero
            filtro = df_historico['codigo_cajero'] == codigo_cajero
            
            # Filtrar por tipo de registro
            if tipo_registro:
                filtro = filtro & (df_historico['tipo_registro'] == tipo_registro)
            
            # Convertir fecha_arqueo del DataFrame a datetime para comparar
            df_historico['fecha_arqueo_dt'] = pd.to_datetime(
                df_historico['fecha_arqueo'],
                errors='coerce'
            )
            
            # Filtrar por fechas (convertir fechas a date para comparar)
            fechas_date = [fecha.date() for fecha in fechas]
            filtro_fechas = df_historico['fecha_arqueo_dt'].dt.date.isin(fechas_date)
            filtro = filtro & filtro_fechas
            
            resultados = df_historico[filtro]
            
            if len(resultados) == 0:
                logger.debug(
                    f"No se encontró ARQUEO en histórico para cajero={codigo_cajero} "
                    f"en fechas {[fecha.strftime('%Y-%m-%d') for fecha in fechas]}"
                )
                return None
            
            # Eliminar columna temporal si existe
            if 'fecha_arqueo_dt' in resultados.columns:
                resultados = resultados.drop(columns=['fecha_arqueo_dt'])
            
            logger.info(
                f"Encontrado ARQUEO en histórico: {len(resultados)} registros para cajero={codigo_cajero} "
                f"en fechas {[fecha.strftime('%Y-%m-%d') for fecha in fechas]}"
            )
            
            return resultados
        
        except Exception as e:
            logger.error(f"Error al buscar ARQUEO en histórico por rango de fechas: {e}")
            return None
    
    def obtener_ultimos_registros_historico(
        self,
        codigo_cajero: int,
        num_registros: int = 3,
        tipo_registro: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Obtiene los últimos N registros del histórico de un cajero, ordenados por fecha descendente.
        
        Args:
            codigo_cajero: Código del cajero a buscar
            num_registros: Número de registros a obtener (default: 3)
            tipo_registro: Tipo de registro a filtrar (ARQUEO o DIARIO) (opcional)
        
        Returns:
            DataFrame con los últimos N registros o None si no se encuentra
        """
        try:
            df_historico = self._cargar_historico_cuadre()
            
            if df_historico.empty:
                logger.debug("Archivo histórico está vacío")
                return None
            
            # Filtrar por código de cajero
            filtro = df_historico['codigo_cajero'] == codigo_cajero
            
            # Filtrar por tipo de registro si se proporciona
            if tipo_registro:
                filtro = filtro & (df_historico['tipo_registro'] == tipo_registro)
            
            # Convertir fecha_arqueo del DataFrame a datetime para ordenar
            df_historico['fecha_arqueo_dt'] = pd.to_datetime(
                df_historico['fecha_arqueo'],
                errors='coerce'
            )
            
            # Aplicar filtro
            resultados = df_historico[filtro].copy()
            
            if len(resultados) == 0:
                logger.debug(
                    f"No se encontraron registros en histórico para cajero={codigo_cajero}"
                )
                return None
            
            # Ordenar por fecha descendente (más reciente primero)
            resultados = resultados.sort_values('fecha_arqueo_dt', ascending=False)
            
            # Tomar los últimos N registros
            resultados = resultados.head(num_registros)
            
            # Eliminar columna temporal si existe
            if 'fecha_arqueo_dt' in resultados.columns:
                resultados = resultados.drop(columns=['fecha_arqueo_dt'])
            
            logger.info(
                f"Obtenidos {len(resultados)} últimos registros del histórico para cajero={codigo_cajero}"
            )
            
            return resultados
        
        except Exception as e:
            logger.error(f"Error al obtener últimos registros del histórico: {e}")
            return None
    
    def _formatear_fecha_arqueo(self, fecha_arqueo: str) -> int:
        """
        Convierte fecha de arqueo (YYYY-MM-DD) a formato YYYYMMDD (entero).
        
        Args:
            fecha_arqueo: Fecha en formato YYYY-MM-DD
        
        Returns:
            Fecha como entero YYYYMMDD
        """
        try:
            fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
            return int(fecha_obj.strftime('%Y%m%d'))
        except Exception as e:
            logger.error(f"Error al formatear fecha {fecha_arqueo}: {e}")
            raise
    
    def buscar_en_nacional(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        valor_descuadre: float
    ) -> Optional[Dict[str, Any]]:
        """
        Busca un movimiento en la base de datos NACIONAL o en el archivo Excel.
        
        Si usar_bd está activo, consulta en la base de datos.
        Si no, busca en el archivo NACIONAL_movimientos.xlsx.
        
        Args:
            codigo_cajero: Código del cajero a buscar
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            valor_descuadre: Valor del descuadre (sobrante o faltante)
        
        Returns:
            Diccionario con los datos encontrados o None si no se encuentra
        """
        # Si está configurado para usar BD, consultar en BD
        if self._usar_bd and self._consultor_bd:
            try:
                config_data = self.config.cargar()
                query_params = config_data.get('base_datos', {}).get('query_params', {})
                
                resultado = self._consultor_bd.consultar_movimientos_nacional(
                    codigo_cajero=codigo_cajero,
                    fecha_arqueo=fecha_arqueo,
                    valor_descuadre=valor_descuadre,
                    cuenta=query_params.get('cuenta', 110505075),
                    codofi_excluir=query_params.get('codofi_excluir', 976),
                    nrocmp=query_params.get('nrocmp', 770500)
                )
                
                if resultado:
                    # Convertir a formato compatible con el resto del sistema
                    # Asegurar que tenga las columnas esperadas
                    resultado_formateado = {
                        'ANOELB': resultado.get('ANOELB'),
                        'MESELB': resultado.get('MESELB'),
                        'DIAELB': resultado.get('DIAELB'),
                        'CODOFI': resultado.get('CODOFI'),
                        'CUENTA': resultado.get('CUENTA'),
                        'NIT': resultado.get('NIT'),
                        'NUMDOC': resultado.get('NUMDOC'),
                        'NROCMP': resultado.get('NROCMP'),
                        'FECHA': resultado.get('FECHA'),
                        'VALOR': resultado.get('VALOR')
                    }
                    return resultado_formateado
                
                return None
                
            except Exception as e:
                logger.error(f"Error al consultar en BD: {e}. Intentando con archivo Excel.")
                # Si falla la BD, intentar con Excel como fallback
                pass
        
        # Buscar en archivo Excel (modo por defecto o fallback)
        try:
            df_nacional = self._cargar_archivo_nacional()
            
            if df_nacional.empty:
                logger.warning("Archivo NACIONAL está vacío")
                return None
            
            # Verificar que existan las columnas necesarias
            if 'NIT' not in df_nacional.columns:
                logger.error("Columna 'NIT' no encontrada en archivo NACIONAL")
                return None
            if 'FECHA' not in df_nacional.columns:
                logger.error("Columna 'FECHA' no encontrada en archivo NACIONAL")
                return None
            if 'VALOR' not in df_nacional.columns:
                logger.error("Columna 'VALOR' no encontrada en archivo NACIONAL")
                return None
            
            # Formatear fecha
            fecha_formateada = self._formatear_fecha_arqueo(fecha_arqueo)
            
            # Buscar coincidencias
            # Buscar por código de cajero (NIT) y fecha
            filtro = (
                (df_nacional['NIT'] == codigo_cajero) &
                (df_nacional['FECHA'] == fecha_formateada)
            )
            
            resultados = df_nacional[filtro]
            
            if len(resultados) == 0:
                logger.debug(
                    f"No se encontró en NACIONAL: cajero={codigo_cajero}, "
                    f"fecha={fecha_formateada}"
                )
                return None
            
            # Si hay múltiples resultados, buscar el que coincida con el valor
            if len(resultados) > 1:
                # Buscar coincidencia exacta de valor
                coincidencia_valor = resultados[resultados['VALOR'] == abs(valor_descuadre)]
                if len(coincidencia_valor) > 0:
                    resultados = coincidencia_valor
            
            # Tomar el primer resultado
            resultado = resultados.iloc[0].to_dict()
            
            logger.info(
                f"Encontrado en NACIONAL: cajero={codigo_cajero}, "
                f"fecha={fecha_formateada}, valor={resultado.get('VALOR')}"
            )
            
            return resultado
        
        except Exception as e:
            logger.error(f"Error al buscar en NACIONAL: {e}")
            return None
    
    def buscar_en_sobrantes(
        self,
        codigo_cajero: int,
        valor_descuadre: float,
        usar_historico: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Busca un movimiento en el archivo SOBRANTES SUCURSALES.
        
        Args:
            codigo_cajero: Código del cajero a buscar
            valor_descuadre: Valor del descuadre (sobrante o faltante)
            usar_historico: Si es True, busca en HISTORICO, si es False, busca en SOBRANTE
        
        Returns:
            Diccionario con los datos encontrados o None si no se encuentra
        """
        try:
            df_sobrantes, df_historico = self._cargar_archivo_sobrantes()
            
            # Seleccionar el DataFrame a usar
            if usar_historico:
                df_a_buscar = df_historico
                nombre_hoja = "HISTORICO"
            else:
                df_a_buscar = df_sobrantes
                nombre_hoja = "SOBRANTE"
            
            if df_a_buscar.empty:
                logger.debug(f"Hoja {nombre_hoja} está vacía")
                return None
            
            # Verificar columnas necesarias - buscar diferentes variantes de nombres
            columna_codigo = None
            posibles_columnas_codigo = ['CODIGO', 'codigo_cajero', 'codigo', 'CAJERO', 'COD. CAJERO']
            for col in posibles_columnas_codigo:
                if col in df_a_buscar.columns:
                    columna_codigo = col
                    break
            
            if not columna_codigo:
                logger.debug(f"Columna de código no encontrada en hoja {nombre_hoja}. Columnas disponibles: {list(df_a_buscar.columns)[:10]}")
                return None
            
            # Buscar columna de valor - buscar diferentes variantes
            columna_valor = None
            posibles_columnas_valor = ['NUEVO VALOR', 'VALOR SOBRANTE', 'valor_sobrante', 'VALOR', 'valor']
            for col in posibles_columnas_valor:
                if col in df_a_buscar.columns:
                    columna_valor = col
                    break
            
            if not columna_valor:
                logger.debug(f"Columna de valor no encontrada en hoja {nombre_hoja}. Columnas disponibles: {list(df_a_buscar.columns)[:10]}")
                return None
            
            # Buscar por código de cajero
            filtro_codigo = df_a_buscar[columna_codigo] == codigo_cajero
            resultados = df_a_buscar[filtro_codigo]
            
            if len(resultados) == 0:
                logger.debug(
                    f"No se encontró en {nombre_hoja}: cajero={codigo_cajero}"
                )
                return None
            
            # Si hay múltiples resultados, buscar el que coincida con el valor
            if len(resultados) > 1:
                coincidencia_valor = resultados[
                    resultados[columna_valor].abs() == abs(valor_descuadre)
                ]
                if len(coincidencia_valor) > 0:
                    resultados = coincidencia_valor
            
            # Tomar el primer resultado
            resultado = resultados.iloc[0].to_dict()
            
            logger.info(
                f"Encontrado en {nombre_hoja}: cajero={codigo_cajero}, "
                f"valor={resultado.get(columna_valor)}"
            )
            
            return resultado
        
        except Exception as e:
            logger.error(f"Error al buscar en SOBRANTES: {e}")
            return None
    
    def buscar_en_faltantes(
        self,
        codigo_cajero: int,
        valor_descuadre: float,
        usar_historico: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Busca un movimiento en el archivo FALTANTES SUCURSALES.
        
        Args:
            codigo_cajero: Código del cajero a buscar
            valor_descuadre: Valor del descuadre (sobrante o faltante)
            usar_historico: Si es True, busca en HISTORICO FALTANTES, si es False, busca en FORMATO FALTANTES
        
        Returns:
            Diccionario con los datos encontrados o None si no se encuentra
        """
        try:
            df_faltantes, df_historico_faltantes = self._cargar_archivo_faltantes()
            
            # Seleccionar el DataFrame a usar
            if usar_historico:
                df_a_buscar = df_historico_faltantes
                nombre_hoja = "HISTORICO FALTANTES"
            else:
                df_a_buscar = df_faltantes
                nombre_hoja = "FORMATO FALTANTES"
            
            if df_a_buscar.empty:
                logger.debug(f"Hoja {nombre_hoja} está vacía")
                return None
            
            # Verificar columnas necesarias
            # Buscar columna de código (puede ser CODIGO, codigo_cajero, COD. CAJERO, etc.)
            columna_codigo = None
            posibles_columnas_codigo = ['CODIGO', 'codigo_cajero', 'codigo', 'CAJERO', 'COD. CAJERO']
            for col in posibles_columnas_codigo:
                if col in df_a_buscar.columns:
                    columna_codigo = col
                    break
            
            if not columna_codigo:
                logger.debug(f"Columna de código no encontrada en hoja {nombre_hoja}. Columnas disponibles: {list(df_a_buscar.columns)[:10]}")
                return None
            
            # Buscar columna de valor (puede ser VALOR FALTANTE, valor_faltante, etc.)
            columna_valor = None
            posibles_columnas_valor = ['VALOR FALTANTE', 'valor_faltante', 'VALOR', 'valor']
            for col in posibles_columnas_valor:
                if col in df_a_buscar.columns:
                    columna_valor = col
                    break
            
            if not columna_valor:
                logger.debug(f"Columna de valor no encontrada en hoja {nombre_hoja}. Columnas disponibles: {list(df_a_buscar.columns)[:10]}")
                return None
            
            # Buscar por código de cajero
            filtro_codigo = df_a_buscar[columna_codigo] == codigo_cajero
            resultados = df_a_buscar[filtro_codigo]
            
            if len(resultados) == 0:
                logger.debug(
                    f"No se encontró en {nombre_hoja}: cajero={codigo_cajero}"
                )
                return None
            
            # Si hay múltiples resultados, buscar el que coincida con el valor
            if len(resultados) > 1:
                coincidencia_valor = resultados[
                    resultados[columna_valor] == abs(valor_descuadre)
                ]
                if len(coincidencia_valor) > 0:
                    resultados = coincidencia_valor
            
            # Tomar el primer resultado
            resultado = resultados.iloc[0].to_dict()
            
            logger.info(
                f"Encontrado en {nombre_hoja}: cajero={codigo_cajero}, "
                f"valor={resultado.get(columna_valor)}"
            )
            
            return resultado
        
        except Exception as e:
            logger.error(f"Error al buscar en FALTANTES: {e}")
            return None
    
    def buscar_movimiento(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        valor_descuadre: float,
        es_sobrante: bool = False
    ) -> Dict[str, Any]:
        """
        Busca un movimiento siguiendo la lógica de negocio:
        
        Para FALTANTES:
        1. Buscar en NACIONAL cuenta 110505075 (BD)
        2. Si no encuentra, buscar en cuenta de SOBRANTES 279510020 (BD)
        
        Para SOBRANTES:
        1. Buscar en NACIONAL cuenta 110505075 (BD) - los sobrantes son valores negativos
        2. Si no encuentra, buscar en cuenta de FALTANTES 168710093 (BD)
        
        Args:
            codigo_cajero: Código del cajero a buscar
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            valor_descuadre: Valor del descuadre (sobrante o faltante)
            es_sobrante: Si es True, indica que es un sobrante
        
        Returns:
            Diccionario con información del movimiento encontrado y fuente
        """
        resultado = {
            'encontrado': False,
            'fuente': None,
            'datos': None,
            'codigo_cajero': codigo_cajero,
            'fecha_arqueo': fecha_arqueo,
            'valor_descuadre': valor_descuadre
        }
        
        # 1. Buscar en NACIONAL cuenta 110505075 (BD)
        # IMPORTANTE: En BD NACIONAL, los valores mantienen el mismo signo:
        # - FALTANTE: en archivo es positivo, en BD también es POSITIVO
        # - SOBRANTE: en archivo es negativo, en BD también es NEGATIVO
        if es_sobrante:
            # SOBRANTE: en archivo es negativo, en BD también es negativo
            valor_busqueda = valor_descuadre  # Mantener negativo
        else:
            # FALTANTE: en archivo es positivo, en BD también es positivo
            valor_busqueda = abs(valor_descuadre)  # Mantener positivo
        
        movimiento_nacional = self.buscar_en_nacional(
            codigo_cajero, fecha_arqueo, valor_busqueda
        )
        
        if movimiento_nacional:
            resultado['encontrado'] = True
            resultado['fuente'] = 'NACIONAL'
            resultado['datos'] = movimiento_nacional
            return resultado
        
        # 2. Según el tipo de descuadre, buscar en la cuenta correspondiente de BD
        if self._usar_bd and self._consultor_bd:
            try:
                config_data = self.config.cargar()
                query_params = config_data.get('base_datos', {}).get('query_params', {})
                
                if es_sobrante:
                    # SOBRANTE: Buscar en cuenta de FALTANTES 168710093
                    # Primero mismo día, luego días anteriores
                    movimiento_faltantes = self._consultor_bd.consultar_cuenta_faltantes(
                        codigo_cajero=codigo_cajero,
                        fecha_arqueo=fecha_arqueo,
                        valor_descuadre=valor_descuadre,  # Sobrante es negativo
                        cuenta=168710093,
                        codofi_excluir=query_params.get('codofi_excluir', 976)
                    )
                    
                    if not movimiento_faltantes:
                        movimiento_faltantes = self._consultor_bd.consultar_cuenta_faltantes_dias_anteriores(
                            codigo_cajero=codigo_cajero,
                            fecha_arqueo=fecha_arqueo,
                            valor_descuadre=valor_descuadre,
                            cuenta=168710093,
                            codofi_excluir=query_params.get('codofi_excluir', 976),
                            dias_anteriores=30
                        )
                    
                    if movimiento_faltantes:
                        resultado['encontrado'] = True
                        resultado['fuente'] = 'FALTANTES_BD'
                        resultado['datos'] = movimiento_faltantes
                        return resultado
                else:
                    # FALTANTE: Buscar en cuenta de SOBRANTES 279510020
                    # Primero mismo día, luego días anteriores
                    movimiento_sobrantes = self._consultor_bd.consultar_cuenta_sobrantes(
                        codigo_cajero=codigo_cajero,
                        fecha_arqueo=fecha_arqueo,
                        valor_descuadre=valor_descuadre,  # Faltante es positivo
                        cuenta=279510020,
                        codofi_excluir=query_params.get('codofi_excluir', 976)
                    )
                    
                    if not movimiento_sobrantes:
                        movimiento_sobrantes = self._consultor_bd.consultar_cuenta_sobrantes_dias_anteriores(
                            codigo_cajero=codigo_cajero,
                            fecha_arqueo=fecha_arqueo,
                            valor_descuadre=valor_descuadre,
                            cuenta=279510020,
                            codofi_excluir=query_params.get('codofi_excluir', 976),
                            dias_anteriores=30
                        )
                    
                    if movimiento_sobrantes:
                        resultado['encontrado'] = True
                        resultado['fuente'] = 'SOBRANTES_BD'
                        resultado['datos'] = movimiento_sobrantes
                        return resultado
            except Exception as e:
                logger.warning(f"Error al consultar cuentas de sobrantes/faltantes en BD: {e}")
        
        logger.debug(
            f"No se encontró movimiento para cajero={codigo_cajero}, "
            f"fecha={fecha_arqueo}, valor={valor_descuadre}, es_sobrante={es_sobrante}"
        )
        
        return resultado

