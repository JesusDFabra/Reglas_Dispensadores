"""
Módulo para procesar archivos Excel de arqueos de cajeros.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import sys
import shutil

# Agregar el directorio raíz al path para imports
sys.path.append(str(Path(__file__).parent.parent))

from src.config.cargador_config import CargadorConfig
from src.consultas.consultor_movimientos import ConsultorMovimientos

logger = logging.getLogger(__name__)


def limpiar_valor_numerico(valor):
    """
    Función auxiliar para limpiar y convertir valores numéricos de texto a float.
    Maneja casos como '$ -   ', valores con comas, puntos, etc.
    """
    if pd.isna(valor):
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    # Si es string, limpiar y convertir
    valor_str = str(valor).strip()
    # Remover caracteres no numéricos excepto punto, coma y signo negativo
    valor_limpio = ''.join(c for c in valor_str if c.isdigit() or c in '.,-')
    # Reemplazar coma por punto si existe
    valor_limpio = valor_limpio.replace(',', '.')
    # Si está vacío o solo tiene guiones/espacios, retornar 0
    if not valor_limpio or valor_limpio in ['-', '.', '']:
        return 0.0
    try:
        return float(valor_limpio)
    except (ValueError, TypeError):
        return 0.0


class ProcesadorArqueos:
    """Clase para procesar archivos Excel de arqueos."""
    
    def __init__(self, config: Optional[CargadorConfig] = None, consultar_movimientos: bool = True):
        """
        Inicializa el procesador de arqueos.
        
        Args:
            config: Instancia de CargadorConfig. Si es None, crea una nueva.
            consultar_movimientos: Si es True, consulta movimientos en archivos complementarios.
        """
        self.config = config or CargadorConfig()
        self._datos_procesados: Optional[pd.DataFrame] = None
        self.consultar_movimientos = consultar_movimientos
        self.consultor = ConsultorMovimientos(config) if consultar_movimientos else None
        self._ruta_archivo_original: Optional[Path] = None
        self._df_archivo_original: Optional[pd.DataFrame] = None
        self._ruta_archivo_procesado: Optional[Path] = None
    
    def cargar_archivo_excel(
        self, 
        ruta_archivo: Path, 
        hoja: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Carga un archivo Excel en un DataFrame.
        
        Args:
            ruta_archivo: Ruta al archivo Excel.
            hoja: Nombre de la hoja a cargar. Si es None, carga la primera.
        
        Returns:
            DataFrame con los datos del archivo.
        
        Raises:
            FileNotFoundError: Si el archivo no existe.
            ValueError: Si hay problemas al leer el archivo.
        """
        if not ruta_archivo.exists():
            raise FileNotFoundError(f"El archivo no existe: {ruta_archivo}")
        
        try:
            logger.info(f"Cargando archivo: {ruta_archivo}")
            
            if hoja:
                df = pd.read_excel(ruta_archivo, sheet_name=hoja)
            else:
                df = pd.read_excel(ruta_archivo)
            
            logger.info(f"Archivo cargado exitosamente. Filas: {len(df)}, Columnas: {len(df.columns)}")
            logger.debug(f"Columnas encontradas: {list(df.columns)}")
            
            return df
        
        except Exception as e:
            logger.error(f"Error al cargar el archivo {ruta_archivo}: {e}")
            raise ValueError(f"No se pudo cargar el archivo: {e}")
    
    def filtrar_por_tipo_registro(
        self, 
        df: pd.DataFrame, 
        tipo_registro: str,
        columna_tipo: str = "tipo_registro"
    ) -> pd.DataFrame:
        """
        Filtra un DataFrame por tipo de registro.
        
        Args:
            df: DataFrame a filtrar.
            tipo_registro: Valor del tipo de registro a filtrar.
            columna_tipo: Nombre de la columna que contiene el tipo de registro.
        
        Returns:
            DataFrame filtrado.
        
        Raises:
            KeyError: Si la columna no existe en el DataFrame.
        """
        if columna_tipo not in df.columns:
            columnas_disponibles = ', '.join(df.columns.tolist())
            raise KeyError(
                f"La columna '{columna_tipo}' no existe en el DataFrame. "
                f"Columnas disponibles: {columnas_disponibles}"
            )
        
        filas_antes = len(df)
        df_filtrado = df[df[columna_tipo] == tipo_registro].copy()
        filas_despues = len(df_filtrado)
        
        logger.info(
            f"Filtrado por tipo_registro='{tipo_registro}': "
            f"{filas_antes} filas -> {filas_despues} filas"
        )
        
        return df_filtrado
    
    def procesar_insumo(
        self, 
        nombre_insumo: str, 
        buscar_mas_reciente: bool = True,
        fecha_especifica: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Procesa un insumo completo: carga el archivo y aplica los filtros.
        
        Args:
            nombre_insumo: Nombre del insumo en la configuración.
            buscar_mas_reciente: Si es True, busca el archivo más reciente en insumos_excel.
            fecha_especifica: Fecha específica en formato DD_MM_YYYY (ej: "28_11_2025").
                            Si se proporciona, busca ese archivo específico.
        
        Returns:
            DataFrame con los datos procesados.
        """
        logger.info(f"Iniciando procesamiento del insumo: {nombre_insumo}")
        
        # Obtener ruta y tipo de registro desde la configuración
        ruta_archivo = self.config.obtener_ruta_insumo(
            nombre_insumo, 
            buscar_mas_reciente,
            fecha_especifica=fecha_especifica
        )
        tipo_registro = self.config.obtener_tipo_registro_filtro(nombre_insumo)
        
        # Guardar ruta del archivo original para actualización posterior
        self._ruta_archivo_original = ruta_archivo
        
        # Cargar el archivo completo (sin filtrar) para poder actualizarlo después
        self._df_archivo_original = self.cargar_archivo_excel(ruta_archivo)
        
        # Aplicar regla de días hábiles si es necesario
        from src.utils.dias_habiles import debe_procesar_registro
        from datetime import datetime as dt
        
        config_data = self.config.cargar()
        fecha_proceso_str = config_data['proceso']['fecha_proceso']
        fecha_proceso = dt.strptime(fecha_proceso_str, '%Y-%m-%d')
        
        # Filtrar por tipo de registro
        df_filtrado = self.filtrar_por_tipo_registro(self._df_archivo_original, tipo_registro)
        
        # Aplicar filtro de días hábiles solo si está habilitado en la configuración
        # Por defecto, procesar todos los registros del tipo especificado
        aplicar_filtro_dias_habiles = config_data.get('proceso', {}).get('aplicar_filtro_dias_habiles', False)
        
        if aplicar_filtro_dias_habiles and len(df_filtrado) > 0:
            # Verificar si cada registro debe ser procesado según días hábiles
            mascara_procesar = []
            for idx, row in df_filtrado.iterrows():
                fecha_arqueo = row.get('fecha_arqueo')
                tiene_sobrante = pd.notna(row.get('sobrantes')) and limpiar_valor_numerico(row.get('sobrantes', 0)) != 0
                tiene_faltante = pd.notna(row.get('faltantes')) and limpiar_valor_numerico(row.get('faltantes', 0)) != 0
                
                debe_procesar = debe_procesar_registro(
                    fecha_arqueo=fecha_arqueo,
                    tipo_registro=tipo_registro,
                    tiene_sobrante=tiene_sobrante,
                    tiene_faltante=tiene_faltante,
                    fecha_proceso=fecha_proceso
                )
                mascara_procesar.append(debe_procesar)
            
            df_filtrado = df_filtrado[mascara_procesar].copy()
            logger.info(f"Después de filtrar por días hábiles: {len(df_filtrado)} registros")
        else:
            logger.info(f"Filtro de días hábiles deshabilitado. Procesando todos los registros: {len(df_filtrado)} registros")
        
        self._datos_procesados = df_filtrado
        
        logger.info(f"Procesamiento completado. Total de registros ARQUEO: {len(df_filtrado)}")
        
        # Verificar si hay registros ARQUEO y DIARIO iguales
        self._procesar_arqueo_diario_iguales()
        
        # Si está habilitado, consultar movimientos
        if self.consultar_movimientos and self.consultor:
            df_filtrado = self._consultar_movimientos(df_filtrado)
            # Actualizar archivo original con los registros no encontrados
            self._actualizar_archivo_original(df_filtrado)
        
        return df_filtrado
    
    def _consultar_movimientos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Consulta movimientos para cada registro en el DataFrame.
        
        Args:
            df: DataFrame con los registros procesados.
        
        Returns:
            DataFrame con columnas adicionales de consulta de movimientos.
        """
        if df.empty:
            return df
        
        logger.info("Iniciando consulta de movimientos...")
        
        # Obtener fecha de arqueo de la configuración (fallback)
        config_data = self.config.cargar()
        fecha_arqueo_fallback = config_data['proceso']['fecha_arqueo']
        
        # Verificar columnas necesarias
        columnas_requeridas = ['codigo_cajero', 'sobrantes', 'faltantes']
        columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
        
        if columnas_faltantes:
            logger.warning(
                f"Columnas faltantes para consulta de movimientos: {columnas_faltantes}. "
                f"Se omitirá la consulta."
            )
            return df
        
        # Agregar columnas para resultados de consulta
        df['movimiento_encontrado'] = False
        df['movimiento_fuente'] = None
        df['movimiento_valor'] = None
        df['movimiento_detalle'] = None
        
        # Procesar cada registro
        total_registros = len(df)
        encontrados = 0
        
        for idx, row in df.iterrows():
            codigo_cajero = int(row['codigo_cajero'])
            sobrante = limpiar_valor_numerico(row['sobrantes'])
            faltante = limpiar_valor_numerico(row['faltantes'])
            
            # Determinar valor de descuadre (prioridad a faltante si ambos existen)
            if faltante != 0:
                valor_descuadre = faltante
                es_sobrante = False
            elif sobrante != 0:
                valor_descuadre = sobrante
                es_sobrante = True
            else:
                # Sin descuadre, no hay nada que buscar
                continue
            
            # Obtener fecha de arqueo del registro si está disponible, sino usar fallback
            if 'fecha_arqueo' in row.index and pd.notna(row['fecha_arqueo']):
                fecha_arqueo_registro = row['fecha_arqueo']
                # Convertir a string si es datetime
                if isinstance(fecha_arqueo_registro, pd.Timestamp):
                    fecha_arqueo = fecha_arqueo_registro.strftime('%Y-%m-%d')
                elif isinstance(fecha_arqueo_registro, datetime):
                    fecha_arqueo = fecha_arqueo_registro.strftime('%Y-%m-%d')
                else:
                    # Intentar parsear como string
                    try:
                        fecha_arqueo = str(fecha_arqueo_registro)
                        # Si tiene formato datetime, extraer solo la fecha
                        if ' ' in fecha_arqueo:
                            fecha_arqueo = fecha_arqueo.split(' ')[0]
                    except:
                        fecha_arqueo = fecha_arqueo_fallback
            else:
                fecha_arqueo = fecha_arqueo_fallback
            
            logger.debug(
                f"Consultando cajero {codigo_cajero}: "
                f"fecha_arqueo={fecha_arqueo}, valor={valor_descuadre}, es_sobrante={es_sobrante}"
            )
            
            # Buscar movimiento
            resultado = self.consultor.buscar_movimiento(
                codigo_cajero=codigo_cajero,
                fecha_arqueo=fecha_arqueo,
                valor_descuadre=valor_descuadre,
                es_sobrante=es_sobrante
            )
            
            # Actualizar DataFrame
            if resultado['encontrado']:
                df.at[idx, 'movimiento_encontrado'] = True
                df.at[idx, 'movimiento_fuente'] = resultado['fuente']
                
                datos = resultado['datos']
                if datos:
                    # Extraer valor según la fuente
                    if resultado['fuente'] == 'NACIONAL':
                        df.at[idx, 'movimiento_valor'] = datos.get('VALOR')
                    elif resultado['fuente'] == 'SOBRANTES_BD':
                        # Cuenta 279510020 - SOBRANTES
                        df.at[idx, 'movimiento_valor'] = datos.get('VALOR')
                    elif resultado['fuente'] == 'FALTANTES_BD':
                        # Cuenta 168710093 - FALTANTES
                        df.at[idx, 'movimiento_valor'] = datos.get('VALOR')
                    
                    # Guardar detalles completos como string JSON (para referencia)
                    import json
                    df.at[idx, 'movimiento_detalle'] = json.dumps(datos, default=str, ensure_ascii=False)
                
                encontrados += 1
        
        logger.info(
            f"Consulta de movimientos completada: {encontrados}/{total_registros} "
            f"movimientos encontrados"
        )
        
        return df
    
    def _procesar_arqueo_diario_iguales(self):
        """
        Procesa la regla de negocio cuando ARQUEO y DIARIO tienen los mismos valores.
        
        Regla: Cuando en el archivo de gestión el cajero tiene ARQUEO y DIARIO exactamente iguales
        (mismos valores en sobrante, faltante, dispensado, recibido), entonces:
        
        - En ARQUEO:
          - ratificar_grabar_diferencia = "Si"
          - justificacion = "Fisico"
          - nuevo_estado = "CONTABILIZACION SOBRANTE FISICO"
          - observaciones = "CONTABILIZACION SOBRANTE FISICO"
        
        - En DIARIO:
          - ratificar_grabar_diferencia = "No"
          - (mismo resto)
        """
        if self._df_archivo_original is None:
            return
        
        # Verificar que existan las columnas necesarias
        columnas_requeridas = ['tipo_registro', 'codigo_cajero', 'sobrantes', 'faltantes', 
                               'dispensado', 'recibido', 'justificacion', 'nuevo_estado', 
                               'ratificar_grabar_diferencia', 'observaciones']
        columnas_faltantes = [col for col in columnas_requeridas if col not in self._df_archivo_original.columns]
        
        if columnas_faltantes:
            logger.debug(f"No se puede aplicar regla ARQUEO/DIARIO: faltan columnas {columnas_faltantes}")
            return
        
        # Obtener registros ARQUEO y DIARIO
        registros_arqueo = self._df_archivo_original[
            self._df_archivo_original['tipo_registro'] == 'ARQUEO'
        ].copy()
        registros_diario = self._df_archivo_original[
            self._df_archivo_original['tipo_registro'] == 'DIARIO'
        ].copy()
        
        if len(registros_arqueo) == 0 or len(registros_diario) == 0:
            logger.debug("No hay registros ARQUEO o DIARIO para comparar")
            return
        
        # Convertir columnas numéricas a float para comparación
        columnas_numericas = ['sobrantes', 'faltantes', 'dispensado', 'recibido']
        for col in columnas_numericas:
            if col in registros_arqueo.columns:
                registros_arqueo[col] = pd.to_numeric(registros_arqueo[col], errors='coerce').fillna(0)
            if col in registros_diario.columns:
                registros_diario[col] = pd.to_numeric(registros_diario[col], errors='coerce').fillna(0)
        
        # Buscar coincidencias por código de cajero
        coincidencias = 0
        
        for idx_arqueo, row_arqueo in registros_arqueo.iterrows():
            codigo_cajero = row_arqueo['codigo_cajero']
            
            # Buscar registro DIARIO con el mismo código de cajero
            registros_diario_mismo_cajero = registros_diario[
                registros_diario['codigo_cajero'] == codigo_cajero
            ]
            
            if len(registros_diario_mismo_cajero) == 0:
                continue
            
            # Comparar valores
            for idx_diario, row_diario in registros_diario_mismo_cajero.iterrows():
                # Verificar si todos los valores son iguales
                valores_iguales = True
                for col in columnas_numericas:
                    if col in row_arqueo.index and col in row_diario.index:
                        valor_arqueo = float(row_arqueo[col]) if pd.notna(row_arqueo[col]) else 0.0
                        valor_diario = float(row_diario[col]) if pd.notna(row_diario[col]) else 0.0
                        if abs(valor_arqueo - valor_diario) > 0.01:  # Tolerancia para comparación de floats
                            valores_iguales = False
                            break
                
                if valores_iguales:
                    # Aplicar regla: ARQUEO y DIARIO son iguales (todos los valores)
                    logger.info(
                        f"Cajero {codigo_cajero}: ARQUEO y DIARIO tienen los mismos valores. "
                        f"Aplicando regla de CONTABILIZACION SOBRANTE FISICO"
                    )
                    
                    # Convertir columnas a string si es necesario
                    if self._df_archivo_original['justificacion'].dtype != 'object':
                        self._df_archivo_original['justificacion'] = self._df_archivo_original['justificacion'].astype(str)
                    if self._df_archivo_original['nuevo_estado'].dtype != 'object':
                        self._df_archivo_original['nuevo_estado'] = self._df_archivo_original['nuevo_estado'].astype(str)
                    if 'ratificar_grabar_diferencia' in self._df_archivo_original.columns:
                        if self._df_archivo_original['ratificar_grabar_diferencia'].dtype != 'object':
                            self._df_archivo_original['ratificar_grabar_diferencia'] = self._df_archivo_original['ratificar_grabar_diferencia'].astype(str)
                    if 'observaciones' in self._df_archivo_original.columns:
                        if self._df_archivo_original['observaciones'].dtype != 'object':
                            self._df_archivo_original['observaciones'] = self._df_archivo_original['observaciones'].astype(str)
                    
                    # Actualizar ARQUEO
                    self._df_archivo_original.loc[idx_arqueo, 'ratificar_grabar_diferencia'] = 'Si'
                    self._df_archivo_original.loc[idx_arqueo, 'justificacion'] = 'Fisico'
                    self._df_archivo_original.loc[idx_arqueo, 'nuevo_estado'] = 'CONTABILIZACION SOBRANTE FISICO'
                    self._df_archivo_original.loc[idx_arqueo, 'observaciones'] = 'CONTABILIZACION SOBRANTE FISICO'
                    
                    # Actualizar DIARIO
                    self._df_archivo_original.loc[idx_diario, 'ratificar_grabar_diferencia'] = 'No'
                    self._df_archivo_original.loc[idx_diario, 'justificacion'] = 'Fisico'
                    self._df_archivo_original.loc[idx_diario, 'nuevo_estado'] = 'CONTABILIZACION SOBRANTE FISICO'
                    self._df_archivo_original.loc[idx_diario, 'observaciones'] = 'CONTABILIZACION SOBRANTE FISICO'
                    
                    coincidencias += 1
                    break  # Solo procesar el primer DIARIO que coincida
                else:
                    # Si no todos los valores son iguales, verificar si al menos faltantes/sobrantes coinciden
                    faltante_arqueo = limpiar_valor_numerico(row_arqueo.get('faltantes', 0))
                    sobrante_arqueo = limpiar_valor_numerico(row_arqueo.get('sobrantes', 0))
                    faltante_diario = limpiar_valor_numerico(row_diario.get('faltantes', 0))
                    sobrante_diario = limpiar_valor_numerico(row_diario.get('sobrantes', 0))
                    
                    # Calcular diferencia del ARQUEO (puede ser faltante positivo o sobrante negativo)
                    diferencia_arqueo = faltante_arqueo if faltante_arqueo > 0 else (abs(sobrante_arqueo) if sobrante_arqueo < 0 else 0)
                    
                    # Calcular diferencia del DIARIO (puede ser faltante positivo o sobrante negativo)
                    diferencia_diario = faltante_diario if faltante_diario > 0 else (abs(sobrante_diario) if sobrante_diario < 0 else 0)
                    
                    # Comparar solo las diferencias (faltantes/sobrantes)
                    misma_diferencia = False
                    if diferencia_arqueo > 0 and diferencia_diario > 0:
                        if abs(diferencia_arqueo - diferencia_diario) < 0.01:  # Tolerancia para floats
                            misma_diferencia = True
                    elif diferencia_arqueo == 0 and diferencia_diario == 0:
                        # Ambos sin diferencia
                        misma_diferencia = True
                    
                    if misma_diferencia:
                        # ARQUEO y DIARIO tienen la misma diferencia (faltantes/sobrantes) aunque otros valores difieran
                        # Marcar estos registros para que la lógica en _actualizar_archivo_original() los procese
                        # No aplicamos la regla aquí porque necesita consultas a BD que se hacen en _actualizar_archivo_original()
                        logger.debug(
                            f"Cajero {codigo_cajero}: ARQUEO y DIARIO tienen la misma diferencia "
                            f"(FALTANTE/SOBRANTE: {diferencia_arqueo:,.0f}) aunque otros valores difieran. "
                            f"Se procesará en _actualizar_archivo_original()"
                        )
                        # No hacemos nada aquí, la lógica en _actualizar_archivo_original() se encargará
                    break  # Solo procesar el primer DIARIO que coincida
        
        if coincidencias > 0:
            logger.info(f"Se encontraron {coincidencias} cajero(s) con ARQUEO y DIARIO iguales")
    
    def _actualizar_archivo_original(self, df_procesado: pd.DataFrame):
        """
        Actualiza el archivo original con justificacion y nuevo_estado
        para todos los registros con descuadre según las reglas de negocio:
        - Si encuentra movimiento: FALTANTE CONTABLE o SOBRANTE CONTABLE
        - Si NO encuentra movimiento: FALTANTE EN ARQUEO o SOBRANTE EN ARQUEO
        
        Args:
            df_procesado: DataFrame con los registros procesados y resultados de consulta.
        """
        if self._df_archivo_original is None or self._ruta_archivo_original is None:
            logger.warning("No se puede actualizar archivo original: no se guardó referencia")
            return
        
        if df_procesado.empty:
            logger.warning("No hay registros procesados para actualizar")
            return
        
        # Verificar que existan las columnas necesarias
        if 'movimiento_encontrado' not in df_procesado.columns:
            logger.warning("Columna 'movimiento_encontrado' no encontrada. No se actualizará el archivo.")
            return
        
        # Identificar registros que tienen descuadre en el ARCHIVO ORIGINAL (no solo en df_procesado)
        # Esto permite procesar registros DIARIO que no fueron incluidos en df_procesado
        registros_a_actualizar = self._df_archivo_original[
            (
                ((self._df_archivo_original['faltantes'].notna()) & (self._df_archivo_original['faltantes'] != 0)) |
                ((self._df_archivo_original['sobrantes'].notna()) & (self._df_archivo_original['sobrantes'] != 0))
            )
        ]
        
        if len(registros_a_actualizar) == 0:
            logger.info("No hay registros con descuadre que requieran actualización")
            return
        
        logger.info(f"Total de registros con descuadre: {len(registros_a_actualizar)}")
        
        # PRIORIZAR PROCESAMIENTO según el orden solicitado:
        # 1. Primero: Cajeros que tienen registro de ARQUEO y DIARIO
        # 2. Segundo: Cajeros que tienen solo ARQUEO
        # 3. Tercero: Cajeros que tienen solo DIARIO
        
        # Identificar cajeros únicos
        if 'codigo_cajero' in registros_a_actualizar.columns:
            cajeros_unicos = registros_a_actualizar['codigo_cajero'].dropna().unique()
            
            # Clasificar cajeros según qué tipos de registro tienen
            cajeros_con_ambos = []
            cajeros_solo_arqueo = []
            cajeros_solo_diario = []
            
            for cajero in cajeros_unicos:
                registros_cajero = registros_a_actualizar[registros_a_actualizar['codigo_cajero'] == cajero]
                tipos_registro = registros_cajero['tipo_registro'].unique() if 'tipo_registro' in registros_cajero.columns else []
                
                tiene_arqueo = 'ARQUEO' in tipos_registro
                tiene_diario = 'DIARIO' in tipos_registro
                
                if tiene_arqueo and tiene_diario:
                    cajeros_con_ambos.append(cajero)
                elif tiene_arqueo:
                    cajeros_solo_arqueo.append(cajero)
                elif tiene_diario:
                    cajeros_solo_diario.append(cajero)
            
            # Crear lista ordenada de índices según prioridad
            indices_ordenados = []
            
            # 1. Primero: Cajeros con ARQUEO y DIARIO
            for cajero in cajeros_con_ambos:
                indices_cajero = registros_a_actualizar[registros_a_actualizar['codigo_cajero'] == cajero].index.tolist()
                indices_ordenados.extend(indices_cajero)
            
            # 2. Segundo: Cajeros solo con ARQUEO
            for cajero in cajeros_solo_arqueo:
                indices_cajero = registros_a_actualizar[registros_a_actualizar['codigo_cajero'] == cajero].index.tolist()
                indices_ordenados.extend(indices_cajero)
            
            # 3. Tercero: Cajeros solo con DIARIO
            for cajero in cajeros_solo_diario:
                indices_cajero = registros_a_actualizar[registros_a_actualizar['codigo_cajero'] == cajero].index.tolist()
                indices_ordenados.extend(indices_cajero)
            
            # Reordenar registros_a_actualizar según la prioridad
            if len(indices_ordenados) > 0:
                registros_a_actualizar = registros_a_actualizar.loc[indices_ordenados]
                logger.info(
                    f"Registros ordenados por prioridad: "
                    f"{len(cajeros_con_ambos)} cajeros con ARQUEO+DIARIO, "
                    f"{len(cajeros_solo_arqueo)} cajeros solo ARQUEO, "
                    f"{len(cajeros_solo_diario)} cajeros solo DIARIO"
                )
            else:
                logger.warning("No se pudo ordenar por prioridad, procesando en orden original")
        else:
            logger.warning("No se encontró columna 'codigo_cajero', procesando en orden original")
        
        logger.info(f"Actualizando {len(registros_a_actualizar)} registros con descuadre en archivo original")
        
        # Verificar que el archivo original tenga las columnas necesarias
        columnas_requeridas = ['justificacion', 'nuevo_estado', 'ratificar_grabar_diferencia', 'observaciones']
        columnas_faltantes = [col for col in columnas_requeridas if col not in self._df_archivo_original.columns]
        
        if columnas_faltantes:
            logger.warning(
                f"Columnas faltantes en archivo original: {columnas_faltantes}. "
                f"Se intentará crear las columnas faltantes."
            )
            # Crear columnas faltantes si no existen
            for col in columnas_faltantes:
                if col not in self._df_archivo_original.columns:
                    self._df_archivo_original[col] = None
        
        # Crear columna temporal para resumen de pasos si no existe
        if 'resumen_pasos' not in self._df_archivo_original.columns:
            self._df_archivo_original['resumen_pasos'] = None
        
        # Necesitamos identificar las filas en el archivo original que corresponden
        # a los registros procesados. Usaremos una combinación de columnas únicas.
        # Asumimos que 'codigo_cajero' y posiblemente otras columnas pueden identificar únicamente
        
        # Identificar columnas que puedan servir como clave
        posibles_claves = ['codigo_cajero', 'arqid', 'fecha_arqueo']
        columnas_clave = [col for col in posibles_claves if col in self._df_archivo_original.columns]
        
        if not columnas_clave:
            logger.warning("No se encontraron columnas clave para identificar registros. No se actualizará.")
            return
        
        # Convertir columnas a string si es necesario para evitar warnings (una sola vez)
        if 'justificacion' in self._df_archivo_original.columns:
                if self._df_archivo_original['justificacion'].dtype != 'object':
                    self._df_archivo_original['justificacion'] = self._df_archivo_original['justificacion'].astype(str)
        if 'nuevo_estado' in self._df_archivo_original.columns:
                if self._df_archivo_original['nuevo_estado'].dtype != 'object':
                    self._df_archivo_original['nuevo_estado'] = self._df_archivo_original['nuevo_estado'].astype(str)
                if 'ratificar_grabar_diferencia' in self._df_archivo_original.columns:
                    if self._df_archivo_original['ratificar_grabar_diferencia'].dtype != 'object':
                        self._df_archivo_original['ratificar_grabar_diferencia'] = self._df_archivo_original['ratificar_grabar_diferencia'].astype(str)
                if 'observaciones' in self._df_archivo_original.columns:
                    if self._df_archivo_original['observaciones'].dtype != 'object':
                        self._df_archivo_original['observaciones'] = self._df_archivo_original['observaciones'].astype(str)
                
        # Actualizar registros en el archivo original
        actualizados = 0
        
        for idx_original, row_original in registros_a_actualizar.iterrows():
            # Usar directamente el registro del archivo original
            # Determinar si es sobrante o faltante
            sobrante = limpiar_valor_numerico(row_original['sobrantes'])
            faltante = limpiar_valor_numerico(row_original['faltantes'])
            
            # Intentar obtener información de movimiento desde df_procesado si existe
            movimiento_encontrado = False
            movimiento_fuente = None
            movimiento_detalle = None
            
            # Buscar en df_procesado si el registro fue procesado
            movimiento_encontrado = False
            movimiento_fuente = None
            movimiento_detalle = None
            
            if not df_procesado.empty:
                # Construir filtro para encontrar el registro en df_procesado
                filtro_procesado = pd.Series([True] * len(df_procesado))
                for col_clave in columnas_clave:
                    if col_clave in row_original.index and col_clave in df_procesado.columns:
                        valor = row_original[col_clave]
                        filtro_procesado = filtro_procesado & (df_procesado[col_clave] == valor)
                
                registros_procesados = df_procesado[filtro_procesado]
                if len(registros_procesados) > 0:
                    row_procesado = registros_procesados.iloc[0]
                    movimiento_encontrado = row_procesado.get('movimiento_encontrado', False)
                    movimiento_fuente = row_procesado.get('movimiento_fuente')
                    movimiento_detalle = row_procesado.get('movimiento_detalle')
                else:
                    movimiento_encontrado = False
                    movimiento_fuente = None
                    movimiento_detalle = None
            
            # Procesar el registro directamente usando el índice del archivo original
            indices_original = [idx_original]
            
            logger.debug(f"Procesando registro: idx={idx_original}, cajero={row_original.get('codigo_cajero')}, tipo={row_original.get('tipo_registro')}")
            
            if idx_original in self._df_archivo_original.index:
                
                # Obtener código de cajero
                codigo_cajero = row_original.get('codigo_cajero')
                
                # Obtener fecha de arqueo del registro (del archivo original)
                fecha_arqueo_registro = None
                primera_fila_original = row_original
                if 'fecha_arqueo' in primera_fila_original.index and pd.notna(primera_fila_original['fecha_arqueo']):
                    fecha_arqueo_registro = primera_fila_original['fecha_arqueo']
                    if isinstance(fecha_arqueo_registro, pd.Timestamp):
                        fecha_arqueo_registro = fecha_arqueo_registro.to_pydatetime()
                    elif isinstance(fecha_arqueo_registro, str):
                        try:
                            fecha_arqueo_registro = datetime.strptime(fecha_arqueo_registro.split(' ')[0], '%Y-%m-%d')
                        except:
                            fecha_arqueo_registro = None
                
                # Obtener tipo de registro del archivo original
                tipo_registro = None
                if 'tipo_registro' in primera_fila_original.index:
                    tipo_registro = primera_fila_original['tipo_registro']
                
                # Inicializar variables de clasificación
                justificacion = None
                nuevo_estado = None
                ratificar_grabar = None
                observaciones = None
                resumen_pasos = []  # Lista para almacenar los pasos seguidos
                
                # Inicializar variables de reglas (todas deben estar inicializadas)
                regla_arqueo_sin_diario = False
                regla_diario_sin_arqueo = False
                regla_arqueo_diario_igual_faltante = False
                regla_arqueo_diario_igual_sobrante = False
                regla_provision_aplicada = False
                
                # NUEVA REGLA PRIORITARIA: Cuando solo llega ARQUEO pero NO DIARIO
                # Esta regla se aplica ANTES de las otras porque es más específica
                
                logger.info(
                    f"DEBUG: Antes de verificar ARQUEO sin DIARIO - "
                    f"cajero={codigo_cajero}, tipo={tipo_registro}, "
                    f"regla_arqueo_sin_diario={regla_arqueo_sin_diario}, regla_diario_sin_arqueo={regla_diario_sin_arqueo}"
                )
                
                if tipo_registro == 'ARQUEO' and codigo_cajero is not None:
                    # Verificar si NO hay registro DIARIO para este cajero
                    registros_mismo_cajero = self._df_archivo_original[
                        self._df_archivo_original['codigo_cajero'] == codigo_cajero
                    ]
                    registros_diario_mismo_cajero = registros_mismo_cajero[
                        registros_mismo_cajero['tipo_registro'] == 'DIARIO'
                    ]
                    
                    logger.info(
                        f"Cajero {codigo_cajero} (ARQUEO): "
                        f"Total registros mismo cajero: {len(registros_mismo_cajero)}, "
                        f"Registros DIARIO: {len(registros_diario_mismo_cajero)}"
                    )
                    
                    # Verificar si hay DIARIO con la misma diferencia (aunque aún no procesado)
                    tiene_diario_misma_diferencia = False
                    if len(registros_diario_mismo_cajero) > 0:
                        registro_diario = registros_diario_mismo_cajero.iloc[0]
                        faltante_diario = limpiar_valor_numerico(registro_diario.get('faltantes', 0))
                        sobrante_diario = limpiar_valor_numerico(registro_diario.get('sobrantes', 0))
                        diferencia_diario = faltante_diario if faltante_diario > 0 else (abs(sobrante_diario) if sobrante_diario < 0 else 0)
                        diferencia_arqueo = faltante if faltante > 0 else (abs(sobrante) if sobrante < 0 else 0)
                        logger.info(
                            f"Cajero {codigo_cajero}: Comparando diferencias - "
                            f"ARQUEO: {diferencia_arqueo}, DIARIO: {diferencia_diario}"
                        )
                        if diferencia_arqueo > 0 and diferencia_diario > 0:
                            if abs(diferencia_arqueo - diferencia_diario) < 0.01:
                                tiene_diario_misma_diferencia = True
                                logger.info(
                                    f"Cajero {codigo_cajero}: ¡Misma diferencia detectada! "
                                    f"ARQUEO={diferencia_arqueo}, DIARIO={diferencia_diario}"
                                )
                    
                    if len(registros_diario_mismo_cajero) == 0:
                        # NO hay registro DIARIO, aplicar regla
                        logger.info(
                            f"Cajero {codigo_cajero}: Solo llega registro ARQUEO sin DIARIO. "
                            f"Aplicando regla específica para ARQUEO sin DIARIO"
                        )
                    elif tiene_diario_misma_diferencia:
                        # Si hay DIARIO con la misma diferencia, NO aplicar regla "ARQUEO sin DIARIO"
                        logger.info(
                            f"Cajero {codigo_cajero}: ARQUEO tiene DIARIO con la misma diferencia. "
                            f"Saltando regla 'ARQUEO sin DIARIO' para aplicar regla de misma diferencia más abajo."
                        )
                        # No hacer nada más aquí, la regla de misma diferencia se aplicará más abajo
                        # NO establecer regla_arqueo_diario_igual_faltante aquí, dejar que el código más abajo lo detecte
                    else:
                        # Hay DIARIO pero con diferente diferencia, aplicar regla "ARQUEO sin DIARIO"
                        logger.info(
                            f"Cajero {codigo_cajero}: Solo llega registro ARQUEO sin DIARIO (o DIARIO con diferente diferencia). "
                            f"Aplicando regla específica para ARQUEO sin DIARIO"
                        )
                    
                    if len(registros_diario_mismo_cajero) == 0 or (len(registros_diario_mismo_cajero) > 0 and not tiene_diario_misma_diferencia):
                        
                        # Obtener consultor BD si está disponible
                        consultor_bd = None
                        if self.consultor and hasattr(self.consultor, '_consultor_bd'):
                            consultor_bd = self.consultor._consultor_bd
                        
                        if fecha_arqueo_registro and consultor_bd:
                            try:
                                config_data = self.config.cargar()
                                query_params = config_data.get('base_datos', {}).get('query_params', {})
                                
                                if faltante > 0:
                                    # CASO FALTANTE: Buscar en NACIONAL con NROCMP 770500, CRÉDITO (valor positivo del faltante)
                                    # IMPORTANTE: Buscar SOLO el día del arqueo (no rango)
                                    logger.info(
                                        f"Cajero {codigo_cajero}: ARQUEO sin DIARIO con FALTANTE ({faltante}). "
                                        f"Buscando en NACIONAL con NROCMP 770500, CRÉDITO (SOLO DÍA DEL ARQUEO)..."
                                    )
                                    
                                    resumen_pasos.append(f"1. Verificado: Solo llega ARQUEO, no llega DIARIO")
                                    resumen_pasos.append(f"2. Tipo: FALTANTE (${faltante:,.0f})")
                                    
                                    movimiento_nacional = consultor_bd.consultar_movimientos_nacional(
                                        codigo_cajero=codigo_cajero,
                                        fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                        valor_descuadre=faltante,  # Faltante es positivo (CRÉDITO)
                                        cuenta=query_params.get('cuenta', 110505075),
                                        codofi_excluir=query_params.get('codofi_excluir', 976),
                                        nrocmp=query_params.get('nrocmp', 770500),
                                        solo_dia_arqueo=True  # SOLO el día del arqueo
                                    )
                                    
                                    if movimiento_nacional:
                                        # Aparece en NACIONAL (día del arqueo)
                                        logger.info(
                                            f"Cajero {codigo_cajero}: Movimiento encontrado en NACIONAL (día del arqueo). "
                                            f"Aplicando regla: Pendiente de gestión"
                                        )
                                        
                                        regla_arqueo_sin_diario = True
                                        justificacion = 'Pendiente de gestion'
                                        nuevo_estado = 'Pendiente de gestion'
                                        ratificar_grabar = 'No'
                                        observaciones = 'Cajero cuadrado en arqueo'
                                        resumen_pasos.append(f"3. Buscado en NACIONAL con NROCMP 770500, CRÉDITO (SOLO DÍA DEL ARQUEO)")
                                        resumen_pasos.append("4. ✓ Movimiento encontrado en NACIONAL (día del arqueo)")
                                        resumen_pasos.append("5. Clasificación: PENDIENTE DE GESTION - Cajero cuadrado en arqueo")
                                    else:
                                        # NO aparece en NACIONAL - Revisar histórico
                                        logger.info(
                                            f"Cajero {codigo_cajero}: No se encontró movimiento en NACIONAL (día del arqueo). "
                                            f"Revisando histórico..."
                                        )
                                        
                                        resumen_pasos.append(f"3. Buscado en NACIONAL con NROCMP 770500, CRÉDITO (SOLO DÍA DEL ARQUEO)")
                                        resumen_pasos.append("4. ✗ No encontrado en NACIONAL (día del arqueo)")
                                        
                                        # Revisar histórico para ver si arqueo_fisico/saldo_contadores está en 0 el día del arqueo
                                        arqueo_fisico = None
                                        if self.consultor:
                                            registro_historico = self.consultor.buscar_en_historico_cuadre(
                                                codigo_cajero=codigo_cajero,
                                                fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                                tipo_registro='ARQUEO'
                                            )
                                            if registro_historico is not None and len(registro_historico) > 0:
                                                # Obtener el primer registro (debería ser único para esa fecha)
                                                registro_hist = registro_historico.iloc[0]
                                                arqueo_fisico = limpiar_valor_numerico(registro_hist.get('arqueo_fisico/saldo_contadores', 0))
                                                logger.debug(
                                                    f"Cajero {codigo_cajero}: Valor arqueo_fisico/saldo_contadores en histórico "
                                                    f"(fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}): {arqueo_fisico}"
                                                )
                                        
                                        # Si no se encontró en histórico, usar el valor del registro actual como fallback
                                        if arqueo_fisico is None:
                                            arqueo_fisico = limpiar_valor_numerico(row_original.get('arqueo_fisico/saldo_contadores', 0))
                                            logger.debug(
                                                f"Cajero {codigo_cajero}: No se encontró en histórico, usando valor del registro actual: {arqueo_fisico}"
                                            )
                                        
                                        if abs(arqueo_fisico) < 0.01:  # Está en 0
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Arqueo físico/saldo contadores está en 0 (consultado en histórico). "
                                                f"Consultando cuenta de sobrantes días anteriores (valores negativos)..."
                                            )
                                            
                                            resumen_pasos.append("5. Consultado histórico: arqueo_fisico/saldo_contadores está en 0")
                                            
                                            # Consultar cuenta de sobrantes días anteriores para buscar valores negativos que sumen el faltante
                                            movimiento_sobrantes = consultor_bd.consultar_sobrantes_negativos_suman_faltante(
                                                codigo_cajero=codigo_cajero,
                                                fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                                valor_faltante=faltante,  # Faltante es positivo
                                                cuenta=279510020,
                                                codofi_excluir=query_params.get('codofi_excluir', 976),
                                                dias_anteriores=30
                                            )
                                            
                                            if movimiento_sobrantes:
                                                # Se encontraron sobrantes negativos que suman el faltante
                                                num_movimientos = movimiento_sobrantes.get('total_movimientos', 0)
                                                suma_encontrada = movimiento_sobrantes.get('suma', 0)
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Se encontraron {num_movimientos} movimientos negativos "
                                                    f"en cuenta de sobrantes que suman {suma_encontrada:,.0f} (faltante: {faltante:,.0f}). "
                                                    f"Aplicando regla: Cruce de novedades"
                                                )
                                                
                                                regla_arqueo_sin_diario = True
                                                justificacion = 'Cruzar'
                                                nuevo_estado = 'Cruce de novedades'
                                                ratificar_grabar = 'Reverso'
                                                observaciones = 'Cruce de novedades'
                                                resumen_pasos.append("6. Buscado en cuenta de sobrantes 279510020, días anteriores (valores negativos)")
                                                resumen_pasos.append(f"7. ✓ Encontrados {num_movimientos} movimientos negativos que suman ${suma_encontrada:,.0f}")
                                                resumen_pasos.append("8. Clasificación: CRUCE DE NOVEDADES - Reverso")
                                            else:
                                                # NO se encontraron sobrantes negativos que sumen el faltante
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: No se encontraron sobrantes negativos que sumen el faltante. "
                                                    f"Aplicando regla: Faltante en arqueo"
                                                )
                                                
                                                regla_arqueo_sin_diario = True
                                                justificacion = 'Fisico'
                                                nuevo_estado = 'Faltante en arqueo'
                                                ratificar_grabar = 'Si'
                                                observaciones = 'Faltante en arqueo'
                                                resumen_pasos.append("6. Buscado en cuenta de sobrantes 279510020, días anteriores (valores negativos)")
                                                resumen_pasos.append("7. ✗ No encontrados sobrantes negativos que sumen el faltante")
                                                resumen_pasos.append("8. Clasificación: FALTANTE EN ARQUEO - Ratificar grabar")
                                        else:
                                            # NO está en 0
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Arqueo físico/saldo contadores NO está en 0 (consultado en histórico: {arqueo_fisico:,.0f}). "
                                                f"Aplicando regla: Pendiente de gestión (solicitar arqueo)"
                                            )
                                            
                                            regla_arqueo_sin_diario = True
                                            justificacion = 'PENDIENTE GESTION'
                                            nuevo_estado = 'Pendiente gestion'
                                            ratificar_grabar = 'No'
                                            observaciones = 'Se le solicita arqueo a la sucursal nuevamente'
                                            resumen_pasos.append(f"5. Consultado histórico: arqueo_fisico/saldo_contadores = ${arqueo_fisico:,.0f} (NO está en 0)")
                                            resumen_pasos.append("6. Clasificación: PENDIENTE GESTION - Solicitar arqueo nuevamente")
                                
                                elif sobrante < 0:
                                    # CASO SOBRANTE: Buscar en NACIONAL con NROCMP 770500, DEBITO (valor del sobrante, negativo)
                                    # IMPORTANTE: Buscar SOLO el día del arqueo (no rango)
                                    logger.info(
                                        f"Cajero {codigo_cajero}: ARQUEO sin DIARIO con SOBRANTE ({sobrante}). "
                                        f"Buscando en NACIONAL con NROCMP 770500, DEBITO (SOLO DÍA DEL ARQUEO)..."
                                    )
                                    
                                    valor_sobrante_abs = abs(sobrante)
                                    resumen_pasos.append(f"1. Verificado: Solo llega ARQUEO, no llega DIARIO")
                                    resumen_pasos.append(f"2. Tipo: SOBRANTE (${valor_sobrante_abs:,.0f})")
                                    
                                    movimiento_nacional = consultor_bd.consultar_movimientos_nacional(
                                        codigo_cajero=codigo_cajero,
                                        fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                        valor_descuadre=sobrante,  # Sobrante es negativo (DEBITO)
                                        cuenta=query_params.get('cuenta', 110505075),
                                        codofi_excluir=query_params.get('codofi_excluir', 976),
                                        nrocmp=query_params.get('nrocmp', 770500),
                                        solo_dia_arqueo=True  # SOLO el día del arqueo
                                    )
                                    
                                    if movimiento_nacional:
                                        # Aparece en NACIONAL (día del arqueo)
                                        logger.info(
                                            f"Cajero {codigo_cajero}: Movimiento encontrado en NACIONAL (día del arqueo). "
                                            f"Aplicando regla: Pendiente de gestión"
                                        )
                                        
                                        regla_arqueo_sin_diario = True
                                        justificacion = 'PENDIENTE GESTION'
                                        nuevo_estado = 'Pendiente gestion'
                                        ratificar_grabar = 'No'
                                        observaciones = 'Cajero cuadrado en arqueo'
                                        resumen_pasos.append(f"3. Buscado en NACIONAL con NROCMP 770500, DEBITO (SOLO DÍA DEL ARQUEO)")
                                        resumen_pasos.append("4. ✓ Movimiento encontrado en NACIONAL (día del arqueo)")
                                        resumen_pasos.append("5. Clasificación: PENDIENTE GESTION - Cajero cuadrado en arqueo")
                                    else:
                                        # NO aparece en NACIONAL - Consultar cuenta de faltantes últimos 30 días
                                        logger.info(
                                            f"Cajero {codigo_cajero}: No se encontró movimiento en NACIONAL (día del arqueo). "
                                            f"Consultando cuenta de faltantes últimos 30 días..."
                                        )
                                        
                                        resumen_pasos.append(f"3. Buscado en NACIONAL con NROCMP 770500, DEBITO (SOLO DÍA DEL ARQUEO)")
                                        resumen_pasos.append("4. ✗ No encontrado en NACIONAL (día del arqueo)")
                                        
                                        movimiento_faltantes = consultor_bd.consultar_cuenta_faltantes_dias_anteriores(
                                            codigo_cajero=codigo_cajero,
                                            fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                            valor_descuadre=sobrante,  # Sobrante es negativo
                                            cuenta=168710093,
                                            codofi_excluir=query_params.get('codofi_excluir', 976),
                                            dias_anteriores=30
                                        )
                                        
                                        if movimiento_faltantes:
                                            # Aparece en cuenta de faltantes
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Movimiento encontrado en cuenta de faltantes. "
                                                f"Aplicando regla: Cruce de novedades"
                                            )
                                            
                                            regla_arqueo_sin_diario = True
                                            justificacion = 'Cruzar'
                                            nuevo_estado = 'Cruce de novedades'
                                            ratificar_grabar = 'Reverso'
                                            observaciones = 'Cruce de novedades'
                                            resumen_pasos.append("5. Buscado en cuenta de faltantes 168710093 (últimos 30 días)")
                                            resumen_pasos.append("6. ✓ Movimiento encontrado en cuenta de faltantes")
                                            resumen_pasos.append("7. Clasificación: CRUCE DE NOVEDADES - Reverso")
                                        else:
                                            # NO aparece en cuenta de faltantes - Verificar monto
                                            resumen_pasos.append("5. Buscado en cuenta de faltantes 168710093 (últimos 30 días)")
                                            resumen_pasos.append("6. ✗ No encontrado en cuenta de faltantes")
                                            
                                            if valor_sobrante_abs < 10000000:  # < $10M
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Sobrante < $10M ({valor_sobrante_abs:,.0f}). "
                                                    f"Aplicando regla: CONTABILIZACION SOBRANTE FISICO"
                                                )
                                                
                                                regla_arqueo_sin_diario = True
                                                justificacion = 'Fisico'
                                                nuevo_estado = 'Contabilizacion sobrante fisico'
                                                ratificar_grabar = 'Si'
                                                observaciones = 'Contabilizacion sobrante fisico'
                                                resumen_pasos.append(f"7. Monto < $10M (${valor_sobrante_abs:,.0f})")
                                                resumen_pasos.append("8. Clasificación: CONTABILIZACION SOBRANTE FISICO - Ratificar grabar")
                                            else:  # >= $10M
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Sobrante >= $10M ({valor_sobrante_abs:,.0f}). "
                                                    f"Consultando histórico..."
                                                )
                                                
                                                resumen_pasos.append(f"7. Monto >= $10M (${valor_sobrante_abs:,.0f})")
                                                
                                                # Obtener últimos 3 registros ARQUEO del histórico
                                                ultimos_registros = None
                                                if self.consultor:
                                                    ultimos_registros = self.consultor.obtener_ultimos_registros_historico(
                                                        codigo_cajero=codigo_cajero,
                                                        num_registros=3,
                                                        tipo_registro='ARQUEO'
                                                    )
                                                
                                                if ultimos_registros is not None and len(ultimos_registros) >= 3:
                                                    # Obtener los últimos 3 sobrantes (son negativos)
                                                    sobrantes_ultimos_3 = []
                                                    for idx, row in ultimos_registros.head(3).iterrows():
                                                        sobrante_val = limpiar_valor_numerico(row.get('sobrantes', 0))
                                                        sobrantes_ultimos_3.append(sobrante_val)
                                                    
                                                    # Convertir a valores absolutos para comparar
                                                    sobrantes_abs = [abs(s) for s in sobrantes_ultimos_3]
                                                    
                                                    resumen_pasos.append(f"8. Últimos 3 sobrantes del histórico: {sobrantes_abs}")
                                                    
                                                    # Verificar patrón: (0, 0, >= 10M)
                                                    if (sobrantes_abs[0] == 0 and sobrantes_abs[1] == 0 and sobrantes_abs[2] >= 10000000):
                                                        # Patrón detectado: (0, 0, >= 10M)
                                                        logger.info(
                                                            f"Cajero {codigo_cajero}: Patrón detectado (0, 0, >= 10M). "
                                                            f"Aplicando regla: Pendiente de gestión (solicitar arqueo)"
                                                        )
                                                        
                                                        regla_arqueo_sin_diario = True
                                                        justificacion = 'PENDIENTE GESTION'
                                                        nuevo_estado = 'Pendiente gestion'
                                                        ratificar_grabar = 'No'
                                                        observaciones = 'Se le solicita arqueo a la sucursal'
                                                        resumen_pasos.append("9. Patrón detectado: (0, 0, >= 10M)")
                                                        resumen_pasos.append("10. Clasificación: PENDIENTE GESTION - Solicitar arqueo")
                                                    else:
                                                        # No cumple el patrón - Contabilizar sobrante físico
                                                        logger.info(
                                                            f"Cajero {codigo_cajero}: No cumple patrón (0, 0, >= 10M). "
                                                            f"Aplicando regla: CONTABILIZACION SOBRANTE FISICO"
                                                        )
                                                        
                                                        regla_arqueo_sin_diario = True
                                                        justificacion = 'Fisico'
                                                        nuevo_estado = 'Contabilizacion sobrante fisico'
                                                        ratificar_grabar = 'Si'
                                                        observaciones = 'Contabilizacion sobrante fisico'
                                                        resumen_pasos.append("9. No cumple patrón (0, 0, >= 10M)")
                                                        resumen_pasos.append("10. Clasificación: CONTABILIZACION SOBRANTE FISICO - Ratificar grabar")
                                                else:
                                                    # No hay suficiente histórico - Contabilizar sobrante físico
                                                    logger.info(
                                                        f"Cajero {codigo_cajero}: No hay suficiente histórico. "
                                                        f"Aplicando regla: CONTABILIZACION SOBRANTE FISICO"
                                                    )
                                                    
                                                    regla_arqueo_sin_diario = True
                                                    justificacion = 'Fisico'
                                                    nuevo_estado = 'Contabilizacion sobrante fisico'
                                                    ratificar_grabar = 'Si'
                                                    observaciones = 'Contabilizacion sobrante fisico'
                                                    resumen_pasos.append("8. No hay suficiente histórico (menos de 3 registros)")
                                                    resumen_pasos.append("9. Clasificación: CONTABILIZACION SOBRANTE FISICO - Ratificar grabar")
                            
                            except Exception as e:
                                logger.warning(f"Error al aplicar regla ARQUEO sin DIARIO: {e}", exc_info=True)
                        else:
                            # No hay fecha_arqueo_registro o consultor_bd, aplicar revisión manual
                            logger.warning(
                                f"Cajero {codigo_cajero}: ARQUEO sin DIARIO pero falta fecha_arqueo_registro o consultor_bd. "
                                f"Aplicando revisión manual"
                            )
                            regla_arqueo_sin_diario = True
                            justificacion = 'PENDIENTE REVISION MANUAL'
                            nuevo_estado = 'Pendiente de revisión manual'
                            ratificar_grabar = 'No'
                            observaciones = 'Este caso requiere la supervisión de personal encargado.'
                            resumen_pasos.append("1. Verificado: Solo llega ARQUEO, no llega DIARIO")
                            resumen_pasos.append("2. Error: Falta fecha_arqueo_registro o consultor_bd")
                            resumen_pasos.append("3. Clasificación: REVISION MANUAL")
                            
                            # Actualizar el registro inmediatamente
                            self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                            self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                            self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                            if 'resumen_pasos' in self._df_archivo_original.columns:
                                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                            actualizados += len(indices_original)
                
                # Si ya se aplicó la regla de ARQUEO sin DIARIO, actualizar el registro y saltar las otras reglas
                if regla_arqueo_sin_diario and justificacion is not None and nuevo_estado is not None:
                    # Actualizar el registro con la clasificación determinada
                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                    self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                    self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                    if 'observaciones' in self._df_archivo_original.columns and observaciones:
                        self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                    if 'resumen_pasos' in self._df_archivo_original.columns and resumen_pasos:
                        self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                    actualizados += len(indices_original)
                    
                    logger.debug(
                        f"Actualizado registro: cajero {codigo_cajero}, tipo {tipo_registro} - "
                        f"justificacion='{justificacion}', nuevo_estado='{nuevo_estado}'"
                    )
                
                # Si ya se aplicó la regla de ARQUEO sin DIARIO, saltar las otras reglas
                if not regla_arqueo_sin_diario and not regla_diario_sin_arqueo:
                    # REGLA 4: Solo llega DIARIO, no llega ARQUEO
                    # Esta regla se aplica ANTES de las otras porque es más específica
                    
                    if tipo_registro == 'DIARIO' and codigo_cajero is not None:
                        # Verificar si NO hay registro ARQUEO para este cajero
                        registros_mismo_cajero = self._df_archivo_original[
                            self._df_archivo_original['codigo_cajero'] == codigo_cajero
                        ]
                        registros_arqueo_mismo_cajero = registros_mismo_cajero[
                            registros_mismo_cajero['tipo_registro'] == 'ARQUEO'
                        ]
                        
                        if len(registros_arqueo_mismo_cajero) == 0:
                            # NO hay registro ARQUEO, aplicar REGLA 4
                            logger.info(
                                f"Cajero {codigo_cajero}: Solo llega registro DIARIO sin ARQUEO. "
                                f"Aplicando REGLA 4: Solo llega Diario, no llega Arqueo"
                            )
                            
                            # Obtener consultor BD si está disponible
                            consultor_bd = None
                            if self.consultor and hasattr(self.consultor, '_consultor_bd'):
                                consultor_bd = self.consultor._consultor_bd
                            
                            try:
                                config_data = self.config.cargar()
                                query_params = config_data.get('base_datos', {}).get('query_params', {})
                                
                                # Inicializar resumen de pasos
                                resumen_pasos = []
                                resumen_pasos.append(f"1. Verificado: Solo llega DIARIO, no llega ARQUEO para cajero {codigo_cajero}")
                                
                                # Determinar si es SOBRANTE o FALTANTE
                                if sobrante < 0:
                                    # CASO SOBRANTE (números negativos)
                                    valor_sobrante_abs = abs(sobrante)
                                    resumen_pasos.append(f"2. Tipo: SOBRANTE (${valor_sobrante_abs:,.0f})")
                                    
                                    if valor_sobrante_abs < 10000000:  # Menor a $10M
                                        # SOBRANTE < $10M
                                        logger.info(
                                            f"Cajero {codigo_cajero}: DIARIO con SOBRANTE < $10M ({valor_sobrante_abs:,.0f}). "
                                            f"Aplicando regla: CONTABILIZACION SOBRANTE CONTABLE"
                                        )
                                        
                                        regla_diario_sin_arqueo = True
                                        justificacion = 'Contable'
                                        nuevo_estado = 'CONTABILIZACION SOBRANTE CONTABLE'
                                        ratificar_grabar = 'Si'
                                        observaciones = 'contabilizacion sobrante contable'
                                        
                                        resumen_pasos.append("3. Monto < $10M")
                                        resumen_pasos.append("4. Clasificación: CONTABILIZACION SOBRANTE CONTABLE - Ratificar grabar")
                                    
                                    else:  # >= $10M
                                        # SOBRANTE >= $10M: Consultar histórico del cajero
                                        logger.info(
                                            f"Cajero {codigo_cajero}: DIARIO con SOBRANTE >= $10M ({valor_sobrante_abs:,.0f}). "
                                            f"Consultando histórico del cajero..."
                                        )
                                        
                                        resumen_pasos.append(f"3. Monto >= $10M (${valor_sobrante_abs:,.0f})")
                                        
                                        # Obtener últimos 3 registros DIARIO del histórico
                                        ultimos_registros = None
                                        if self.consultor:
                                            ultimos_registros = self.consultor.obtener_ultimos_registros_historico(
                                                codigo_cajero=codigo_cajero,
                                                num_registros=3,
                                                tipo_registro='DIARIO'
                                            )
                                        
                                        if ultimos_registros is not None and len(ultimos_registros) >= 3:
                                            # Obtener los últimos 3 sobrantes (son negativos)
                                            sobrantes_ultimos_3 = []
                                            for idx, row in ultimos_registros.head(3).iterrows():
                                                sobrante_val = limpiar_valor_numerico(row.get('sobrantes', 0))
                                                sobrantes_ultimos_3.append(sobrante_val)
                                            
                                            # Convertir a valores absolutos para comparar
                                            sobrantes_abs = [abs(s) for s in sobrantes_ultimos_3]
                                            
                                            resumen_pasos.append(f"4. Últimos 3 sobrantes del histórico: {sobrantes_abs}")
                                            
                                            # Verificar patrones: (0, 0, >= 10M) o (0, >= 10M, >= 10M)
                                            if (sobrantes_abs[0] == 0 and sobrantes_abs[1] == 0 and sobrantes_abs[2] >= 10000000):
                                                # 1 vez: (0, 0, >= 10M)
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Patrón detectado (0, 0, >= 10M). "
                                                    f"Aplicando regla: PENDIENTE GESTION - Revisar Diario día siguiente"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'PENDIENTE GESTION'
                                                nuevo_estado = 'Pendiente gestion'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Revisar el Diario día siguiente'
                                                
                                                resumen_pasos.append("5. Patrón: (0, 0, >= 10M) - Primera vez")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE GESTION - Revisar Diario día siguiente")
                                            
                                            elif (sobrantes_abs[0] == 0 and sobrantes_abs[1] >= 10000000 and sobrantes_abs[2] >= 10000000):
                                                # 2 vez: (0, >= 10M, >= 10M)
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Patrón detectado (0, >= 10M, >= 10M). "
                                                    f"Aplicando regla: PENDIENTE GESTION - Solicitar arqueo"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'PENDIENTE GESTION'
                                                nuevo_estado = 'Pendiente gestion'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Se le solicita arqueo a la sucursal nuevamente'
                                                
                                                resumen_pasos.append("5. Patrón: (0, >= 10M, >= 10M) - Segunda vez")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE GESTION - Solicitar arqueo")
                                            
                                            else:
                                                # No cumple ningún patrón, revisión manual
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: No cumple patrón esperado. "
                                                    f"Aplicando regla: REVISION MANUAL"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'PENDIENTE REVISION MANUAL'
                                                nuevo_estado = 'Pendiente de revisión manual'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                                
                                                resumen_pasos.append("5. No cumple patrón esperado")
                                                resumen_pasos.append("6. Clasificación: REVISION MANUAL")
                                        
                                        else:
                                            # No hay suficientes registros en histórico, revisión manual
                                            logger.info(
                                                f"Cajero {codigo_cajero}: No hay suficientes registros en histórico. "
                                                f"Aplicando regla: REVISION MANUAL"
                                            )
                                            
                                            regla_diario_sin_arqueo = True
                                            justificacion = 'PENDIENTE REVISION MANUAL'
                                            nuevo_estado = 'Pendiente de revisión manual'
                                            ratificar_grabar = 'No'
                                            observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                            
                                            resumen_pasos.append("4. No hay suficientes registros en histórico")
                                            resumen_pasos.append("5. Clasificación: REVISION MANUAL")
                                
                                elif faltante > 0:
                                    # CASO FALTANTE (números positivos)
                                    resumen_pasos.append(f"2. Tipo: FALTANTE (${faltante:,.0f})")
                                    
                                    if faltante < 10000000:  # Menor a $10M
                                        # FALTANTE < $10M: Revisar en nacional cuenta de sobrantes días anteriores
                                        logger.info(
                                            f"Cajero {codigo_cajero}: DIARIO con FALTANTE < $10M ({faltante:,.0f}). "
                                            f"Buscando en cuenta de sobrantes días anteriores..."
                                        )
                                        
                                        resumen_pasos.append("3. Monto < $10M")
                                        
                                        if consultor_bd and fecha_arqueo_registro:
                                            # Buscar en cuenta de sobrantes 279510020 días anteriores
                                            movimiento_sobrantes = consultor_bd.consultar_cuenta_sobrantes_dias_anteriores(
                                                codigo_cajero=codigo_cajero,
                                                fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                                valor_descuadre=faltante,
                                                cuenta=279510020,
                                                codofi_excluir=query_params.get('codofi_excluir', 976),
                                                dias_anteriores=30
                                            )
                                            
                                            if movimiento_sobrantes:
                                                # Aparece en cuenta de sobrantes
                                                fecha_movimiento = movimiento_sobrantes.get('FECHA')
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Movimiento encontrado en cuenta de sobrantes "
                                                    f"(fecha: {fecha_movimiento}). Aplicando regla: CRUCE DE NOVEDADES"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'Cruzar'
                                                nuevo_estado = 'CRUCE DE NOVEDADES'
                                                ratificar_grabar = 'Reverso'
                                                # Convertir fecha a entero para evitar ".0" al final
                                                if fecha_movimiento is not None:
                                                    fecha_int = int(float(fecha_movimiento))
                                                    observaciones = str(fecha_int)  # YYYYMMDD
                                                else:
                                                    observaciones = str(fecha_movimiento)  # YYYYMMDD
                                                
                                                resumen_pasos.append("4. Buscado en cuenta sobrantes 279510020 días anteriores")
                                                resumen_pasos.append(f"5. ✓ Movimiento encontrado (fecha: {fecha_movimiento})")
                                                resumen_pasos.append("6. Clasificación: CRUCE DE NOVEDADES - Reverso")
                                            
                                            else:
                                                # No aparece, revisión manual
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: No se encontró movimiento en cuenta de sobrantes. "
                                                    f"Aplicando regla: REVISION MANUAL"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'PENDIENTE REVISION MANUAL'
                                                nuevo_estado = 'Pendiente de revisión manual'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Situación ambigua.'
                                                
                                                resumen_pasos.append("4. Buscado en cuenta sobrantes 279510020 días anteriores")
                                                resumen_pasos.append("5. ✗ No encontrado")
                                                resumen_pasos.append("6. Clasificación: REVISION MANUAL - Situación ambigua")
                                        
                                        else:
                                            # No hay consultor BD, revisión manual
                                            regla_diario_sin_arqueo = True
                                            justificacion = 'PENDIENTE REVISION MANUAL'
                                            nuevo_estado = 'Pendiente de revisión manual'
                                            ratificar_grabar = 'No'
                                            observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                            
                                            resumen_pasos.append("4. No hay acceso a BD")
                                            resumen_pasos.append("5. Clasificación: REVISION MANUAL")
                                    
                                    else:  # >= $10M
                                        # FALTANTE >= $10M: Consultar histórico de faltantes
                                        logger.info(
                                            f"Cajero {codigo_cajero}: DIARIO con FALTANTE >= $10M ({faltante:,.0f}). "
                                            f"Consultando histórico de faltantes..."
                                        )
                                        
                                        resumen_pasos.append(f"3. Monto >= $10M (${faltante:,.0f})")
                                        
                                        # Obtener últimos 3 registros DIARIO del histórico
                                        ultimos_registros = None
                                        if self.consultor:
                                            ultimos_registros = self.consultor.obtener_ultimos_registros_historico(
                                                codigo_cajero=codigo_cajero,
                                                num_registros=3,
                                                tipo_registro='DIARIO'
                                            )
                                        
                                        if ultimos_registros is not None and len(ultimos_registros) >= 3:
                                            # Obtener los últimos 3 faltantes (son positivos)
                                            faltantes_ultimos_3 = []
                                            for idx, row in ultimos_registros.head(3).iterrows():
                                                faltante_val = limpiar_valor_numerico(row.get('faltantes', 0))
                                                faltantes_ultimos_3.append(faltante_val)
                                            
                                            resumen_pasos.append(f"4. Últimos 3 faltantes del histórico: {faltantes_ultimos_3}")
                                            
                                            # Verificar patrones: (0, 0, >= 10M) o (0, >= 10M, >= 10M)
                                            if (faltantes_ultimos_3[0] == 0 and faltantes_ultimos_3[1] == 0 and faltantes_ultimos_3[2] >= 10000000):
                                                # 1 vez: (0, 0, >= 10M)
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Patrón detectado (0, 0, >= 10M). "
                                                    f"Aplicando regla: PENDIENTE GESTION - Revisar Diario día siguiente"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'PENDIENTE GESTION'
                                                nuevo_estado = 'Pendiente gestion'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Revisar el Diario día siguiente'
                                                
                                                resumen_pasos.append("5. Patrón: (0, 0, >= 10M) - Primera vez")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE GESTION - Revisar Diario día siguiente")
                                            
                                            elif (faltantes_ultimos_3[0] == 0 and faltantes_ultimos_3[1] >= 10000000 and faltantes_ultimos_3[2] >= 10000000):
                                                # 2 vez: (0, >= 10M, >= 10M)
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Patrón detectado (0, >= 10M, >= 10M). "
                                                    f"Aplicando regla: PENDIENTE GESTION - Solicitar arqueo"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'PENDIENTE GESTION'
                                                nuevo_estado = 'Pendiente gestion'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Se le solicita arqueo a la sucursal nuevamente'
                                                
                                                resumen_pasos.append("5. Patrón: (0, >= 10M, >= 10M) - Segunda vez")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE GESTION - Solicitar arqueo")
                                            
                                            else:
                                                # No cumple ningún patrón, revisión manual
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: No cumple patrón esperado. "
                                                    f"Aplicando regla: REVISION MANUAL"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'PENDIENTE REVISION MANUAL'
                                                nuevo_estado = 'Pendiente de revisión manual'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                                
                                                resumen_pasos.append("5. No cumple patrón esperado")
                                                resumen_pasos.append("6. Clasificación: REVISION MANUAL")
                                        
                                        else:
                                            # No hay suficientes registros en histórico, revisión manual
                                            logger.info(
                                                f"Cajero {codigo_cajero}: No hay suficientes registros en histórico. "
                                                f"Aplicando regla: REVISION MANUAL"
                                            )
                                            
                                            regla_diario_sin_arqueo = True
                                            justificacion = 'PENDIENTE REVISION MANUAL'
                                            nuevo_estado = 'Pendiente de revisión manual'
                                            ratificar_grabar = 'No'
                                            observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                            
                                            resumen_pasos.append("4. No hay suficientes registros en histórico")
                                            resumen_pasos.append("5. Clasificación: REVISION MANUAL")
                                
                                else:
                                    # No hay sobrante ni faltante, revisión manual
                                    logger.info(
                                        f"Cajero {codigo_cajero}: No hay sobrante ni faltante. "
                                        f"Aplicando regla: REVISION MANUAL"
                                    )
                                    
                                    regla_diario_sin_arqueo = True
                                    justificacion = 'PENDIENTE REVISION MANUAL'
                                    nuevo_estado = 'Pendiente de revisión manual'
                                    ratificar_grabar = 'No'
                                    observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                    
                                    resumen_pasos.append("2. No hay sobrante ni faltante")
                                    resumen_pasos.append("3. Clasificación: REVISION MANUAL")
                                
                                # Actualizar el registro DIARIO
                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                            
                            except Exception as e:
                                logger.warning(f"Error al aplicar REGLA 4 (DIARIO sin ARQUEO): {e}", exc_info=True)
                                # En caso de error, aplicar revisión manual
                                regla_diario_sin_arqueo = True
                                justificacion = 'PENDIENTE REVISION MANUAL'
                                nuevo_estado = 'Pendiente de revisión manual'
                                ratificar_grabar = 'No'
                                observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                
                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                    
                # Si ya se aplicó la regla de DIARIO sin ARQUEO, saltar las otras reglas
                logger.info(
                    f"Cajero {codigo_cajero} ({tipo_registro}): "
                    f"Antes de verificar ARQUEO/DIARIO igual: "
                    f"regla_arqueo_sin_diario={regla_arqueo_sin_diario}, regla_diario_sin_arqueo={regla_diario_sin_arqueo}"
                )
                if not regla_arqueo_sin_diario and not regla_diario_sin_arqueo:
                    # NUEVA REGLA PRIORITARIA: Cuando ARQUEO y DIARIO tienen la misma diferencia (FALTANTE)
                    # Esta regla se aplica ANTES de las otras porque es más específica
                    
                    # Calcular diferencia del registro actual (puede ser faltante positivo o sobrante negativo)
                    diferencia_actual = faltante if faltante > 0 else (abs(sobrante) if sobrante < 0 else 0)
                    
                    logger.info(
                        f"Cajero {codigo_cajero} ({tipo_registro}): "
                        f"faltante={faltante}, sobrante={sobrante}, diferencia_actual={diferencia_actual}, "
                        f"regla_arqueo_sin_diario={regla_arqueo_sin_diario}, regla_diario_sin_arqueo={regla_diario_sin_arqueo}"
                    )
                    
                    # Verificar si hay registro del otro tipo (ARQUEO o DIARIO) con la misma diferencia
                    if codigo_cajero is not None and diferencia_actual > 0:
                        # Buscar registro del otro tipo
                        registros_mismo_cajero = self._df_archivo_original[
                            self._df_archivo_original['codigo_cajero'] == codigo_cajero
                        ]
                        
                        if tipo_registro == 'ARQUEO':
                            # Buscar DIARIO con la misma diferencia
                            registros_otro_tipo = registros_mismo_cajero[
                                registros_mismo_cajero['tipo_registro'] == 'DIARIO'
                            ]
                        elif tipo_registro == 'DIARIO':
                            # Buscar ARQUEO con la misma diferencia
                            registros_otro_tipo = registros_mismo_cajero[
                                registros_mismo_cajero['tipo_registro'] == 'ARQUEO'
                            ]
                        else:
                            registros_otro_tipo = pd.DataFrame()
                        
                        if len(registros_otro_tipo) > 0:
                            # Hay registro del otro tipo, verificar si tienen la misma diferencia (FALTANTE)
                            registro_otro_tipo = registros_otro_tipo.iloc[0]
                            
                            faltante_otro = limpiar_valor_numerico(registro_otro_tipo.get('faltantes', 0))
                            sobrante_otro = limpiar_valor_numerico(registro_otro_tipo.get('sobrantes', 0))
                            
                            # Calcular diferencia del otro registro (puede ser faltante positivo o sobrante negativo)
                            diferencia_otro = faltante_otro if faltante_otro > 0 else (abs(sobrante_otro) if sobrante_otro < 0 else 0)
                            
                            # Comparar diferencias (deben ser iguales)
                            misma_diferencia_faltante = False
                            if diferencia_actual > 0 and diferencia_otro > 0:
                                if abs(diferencia_actual - diferencia_otro) < 0.01:  # Tolerancia para floats
                                    misma_diferencia_faltante = True
                            
                            # Verificar si el registro del otro tipo ya tiene una justificación aplicada
                            # (esto significa que la regla ya se aplicó cuando se procesó el otro registro)
                            justificacion_otro = registro_otro_tipo.get('justificacion')
                            ya_procesado = pd.notna(justificacion_otro) and str(justificacion_otro).strip() not in ['', 'nan', 'None']
                            
                            if misma_diferencia_faltante:
                                # Aplicar nueva regla: ARQUEO y DIARIO con misma diferencia (FALTANTE)
                                tipo_otro = 'DIARIO' if tipo_registro == 'ARQUEO' else 'ARQUEO'
                                
                                # Si el registro del otro tipo ya está procesado, copiar los valores correspondientes
                                if ya_procesado:
                                    logger.info(
                                        f"Cajero {codigo_cajero}: {tipo_registro} y {tipo_otro} tienen la misma diferencia (FALTANTE: {diferencia_actual:,.0f}). "
                                        f"El {tipo_otro} ya fue procesado, copiando valores correspondientes"
                                    )
                                    
                                    # Determinar qué valores copiar según el tipo de registro actual
                                    if tipo_registro == 'ARQUEO':
                                        # Si es ARQUEO, copiar valores de DIARIO pero ajustar ratificar_grabar
                                        justificacion_actual = registro_otro_tipo.get('justificacion')
                                        nuevo_estado_actual = registro_otro_tipo.get('nuevo_estado')
                                        observaciones_actual = registro_otro_tipo.get('observaciones')
                                        resumen_pasos_actual = registro_otro_tipo.get('resumen_pasos', '')
                                        
                                        # ARQUEO: ratificar_grabar según la regla (puede ser 'Si', 'No', o 'Reverso')
                                        # Si el DIARIO tiene 'No', el ARQUEO debe tener el valor opuesto según la regla
                                        ratificar_grabar_otro = registro_otro_tipo.get('ratificar_grabar_diferencia', 'No')
                                        if ratificar_grabar_otro == 'No':
                                            # Si DIARIO es 'No', ARQUEO puede ser 'Si' o 'Reverso' según la justificación
                                            if justificacion_actual == 'CRUZAR':
                                                ratificar_grabar_actual = 'Reverso'
                                            else:
                                                ratificar_grabar_actual = 'Si'
                                        else:
                                            ratificar_grabar_actual = ratificar_grabar_otro
                                    else:
                                        # Si es DIARIO, copiar valores de ARQUEO pero ratificar_grabar = 'No'
                                        justificacion_actual = registro_otro_tipo.get('justificacion')
                                        nuevo_estado_actual = registro_otro_tipo.get('nuevo_estado')
                                        observaciones_actual = registro_otro_tipo.get('observaciones')
                                        resumen_pasos_actual = registro_otro_tipo.get('resumen_pasos', '')
                                        ratificar_grabar_actual = 'No'  # DIARIO siempre 'No' cuando hay misma diferencia
                                    
                                    # Actualizar el registro actual
                                    regla_arqueo_diario_igual_faltante = True
                                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                    self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_actual
                                    self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_actual
                                    self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_actual
                                    if 'resumen_pasos' in self._df_archivo_original.columns:
                                        # Ajustar el resumen de pasos para reflejar que se copió del otro registro
                                        resumen_ajustado = resumen_pasos_actual.replace(tipo_otro, tipo_registro) if resumen_pasos_actual else f"1. Copiado de {tipo_otro} procesado anteriormente"
                                        self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = resumen_ajustado
                                    
                                    continue  # Saltar el procesamiento normal ya que se copiaron los valores
                                
                                logger.info(
                                    f"Cajero {codigo_cajero}: {tipo_registro} y {tipo_otro} tienen la misma diferencia (FALTANTE: {diferencia_actual:,.0f}). "
                                    f"Aplicando regla de Error en Transmicion de contadores"
                                )
                                
                                # Obtener consultor BD si está disponible
                                consultor_bd = None
                                if self.consultor and hasattr(self.consultor, '_consultor_bd'):
                                    consultor_bd = self.consultor._consultor_bd
                                
                                if fecha_arqueo_registro and consultor_bd:
                                    try:
                                        config_data = self.config.cargar()
                                        query_params = config_data.get('base_datos', {}).get('query_params', {})
                                        
                                        # PASO 1: Buscar en NACIONAL cuenta 110505075, el día del arqueo, por el valor del faltante
                                        # Usar diferencia_actual (siempre positivo para la consulta)
                                        movimiento_nacional = consultor_bd.consultar_movimientos_nacional(
                                            codigo_cajero=codigo_cajero,
                                            fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                            valor_descuadre=diferencia_actual,  # Diferencia es positiva
                                            cuenta=query_params.get('cuenta', 110505075),
                                            codofi_excluir=query_params.get('codofi_excluir', 976),
                                            nrocmp=query_params.get('nrocmp', 770500)
                                        )
                                        
                                        if movimiento_nacional:
                                            # CASO 1: Aparece en NACIONAL cuenta 110505075
                                            # Verificar si la fecha del movimiento coincide con la fecha del arqueo
                                            fecha_movimiento_diferente = False
                                            fecha_movimiento = None
                                            
                                            try:
                                                # Extraer fecha del movimiento
                                                fecha_movimiento_num = movimiento_nacional.get('FECHA')
                                                if fecha_movimiento_num:
                                                    # Convertir a datetime: YYYYMMDD
                                                    fecha_movimiento_num_int = int(float(fecha_movimiento_num))
                                                    anio = fecha_movimiento_num_int // 10000
                                                    mes = (fecha_movimiento_num_int % 10000) // 100
                                                    dia = fecha_movimiento_num_int % 100
                                                    fecha_movimiento = datetime(anio, mes, dia)
                                                    
                                                    # Comparar solo la fecha (sin hora)
                                                    fecha_arqueo_sin_hora = fecha_arqueo_registro.replace(hour=0, minute=0, second=0, microsecond=0)
                                                    if fecha_movimiento != fecha_arqueo_sin_hora:
                                                        fecha_movimiento_diferente = True
                                            except Exception as e:
                                                logger.debug(f"Error al comparar fechas del movimiento: {e}")
                                            
                                            regla_arqueo_diario_igual_faltante = True
                                            
                                            if fecha_movimiento_diferente:
                                                # CASO 1a: Movimiento encontrado pero con fecha diferente
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Movimiento encontrado en NACIONAL cuenta 110505075 "
                                                    f"pero con fecha diferente (movimiento: {fecha_movimiento.strftime('%Y-%m-%d')}, "
                                                    f"arqueo: {fecha_arqueo_registro.strftime('%Y-%m-%d')}). "
                                                    f"Aplicando regla: CRUCE DE NOVEDADES"
                                                )
                                                
                                                # ARQUEO
                                                justificacion_arqueo = 'CRUZAR'
                                                nuevo_estado_arqueo = 'Cruce de novedades'
                                                ratificar_grabar_arqueo = 'Reverso'
                                                observaciones_arqueo = 'Se reversa diferencia con cuadre anterior'
                                                
                                                # Resumen de pasos para ARQUEO
                                                resumen_pasos_arqueo = []
                                                resumen_pasos_arqueo.append(f"1. Verificado: {tipo_registro} y {tipo_otro} tienen misma diferencia (FALTANTE: ${diferencia_actual:,.0f})")
                                                resumen_pasos_arqueo.append(f"2. Buscado en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${diferencia_actual:,.0f}")
                                                resumen_pasos_arqueo.append("3. ✓ Movimiento encontrado en cuenta 110505075")
                                                resumen_pasos_arqueo.append(f"4. Fecha movimiento: {fecha_movimiento.strftime('%Y-%m-%d')}, Fecha arqueo: {fecha_arqueo_registro.strftime('%Y-%m-%d')}")
                                                resumen_pasos_arqueo.append("5. Fechas diferentes → Clasificación: CRUCE DE NOVEDADES - Reverso")
                                                
                                                # DIARIO
                                                justificacion_diario = 'CRUZAR'
                                                nuevo_estado_diario = 'Cruce de novedades'
                                                ratificar_grabar_diario = 'No'
                                                observaciones_diario = 'Se reversa diferencia con cuadre anterior'
                                                
                                                # Resumen de pasos para DIARIO
                                                resumen_pasos_diario = resumen_pasos_arqueo.copy()
                                                resumen_pasos_diario[-1] = "5. Fechas diferentes → Clasificación: CRUCE DE NOVEDADES - No ratificar"
                                                
                                                # Actualizar ARQUEO
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_arqueo
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_arqueo
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_arqueo
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                                
                                                # Actualizar el registro del otro tipo
                                                idx_otro_tipo = registro_otro_tipo.name
                                                self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_diario
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                            else:
                                                # CASO 1b: Movimiento encontrado con la misma fecha
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Movimiento encontrado en NACIONAL cuenta 110505075 "
                                                    f"con fecha coincidente. Aplicando regla: Pendiente de gestión"
                                                )
                                                
                                                # ARQUEO Y DIARIO - Ambos deben tener la misma clasificación
                                                justificacion = 'PENDIENTE DE GESTION'
                                                nuevo_estado = 'Pendiente de gestión'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Cajero cuadrado con arqueo de la sucursal'
                                                
                                                # Resumen de pasos
                                                resumen_pasos.append(f"1. Verificado: {tipo_registro} y {tipo_otro} tienen misma diferencia (FALTANTE: ${diferencia_actual:,.0f})")
                                                resumen_pasos.append(f"2. Buscado en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${diferencia_actual:,.0f}")
                                                resumen_pasos.append("3. ✓ Movimiento encontrado en cuenta 110505075")
                                                if fecha_movimiento:
                                                    resumen_pasos.append(f"4. Fecha movimiento: {fecha_movimiento.strftime('%Y-%m-%d')}, Fecha arqueo: {fecha_arqueo_registro.strftime('%Y-%m-%d')}")
                                                    resumen_pasos.append("5. Fechas coinciden → Clasificación: PENDIENTE DE GESTION - Cajero cuadrado con arqueo de la sucursal")
                                                else:
                                                    resumen_pasos.append("4. Clasificación: PENDIENTE DE GESTION - Cajero cuadrado con arqueo de la sucursal")
                                                
                                                # Actualizar el registro ARQUEO (usando indices_original)
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                
                                                # Actualizar también el registro del otro tipo
                                                idx_otro_tipo = registro_otro_tipo.name
                                                self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                                self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion
                                                self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado
                                                self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                        
                                        else:
                                            # CASO 2: NO aparece en NACIONAL cuenta 110505075
                                            # Buscar en cuenta de sobrantes 279510020 días anteriores (valores negativos que sumen el faltante)
                                            logger.info(
                                                f"Cajero {codigo_cajero}: No se encontró movimiento en NACIONAL cuenta 110505075. "
                                                f"Buscando en cuenta de sobrantes 279510020 días anteriores (valores negativos que sumen el faltante)..."
                                            )
                                            
                                            # Usar el nuevo método que busca sobrantes negativos que sumen el faltante
                                            movimiento_sobrantes = consultor_bd.consultar_sobrantes_negativos_suman_faltante(
                                                codigo_cajero=codigo_cajero,
                                                fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                                valor_faltante=diferencia_actual,
                                                cuenta=279510020,
                                                codofi_excluir=query_params.get('codofi_excluir', 976),
                                                dias_anteriores=30
                                            )
                                            
                                            if movimiento_sobrantes:
                                                # CASO 2a: Aparece en cuenta de sobrantes 279510020 (valores negativos que suman el faltante)
                                                num_movimientos = movimiento_sobrantes.get('total_movimientos', 0)
                                                suma_encontrada = movimiento_sobrantes.get('suma', 0)
                                                
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Se encontraron {num_movimientos} movimientos negativos "
                                                    f"en cuenta de sobrantes que suman {suma_encontrada:,.0f} (faltante: {diferencia_actual:,.0f}). "
                                                    f"Aplicando regla: CRUCE DE NOVEDADES"
                                                )
                                                
                                                regla_arqueo_diario_igual_faltante = True
                                                
                                                # Verificar si es un solo registro o varios
                                                if num_movimientos == 1:
                                                    # CASO 2a.1: Un solo registro de sobrantes
                                                    # Convertir fecha a string entero sin ".0"
                                                    fecha_arqueo_str = fecha_arqueo_registro.strftime('%Y%m%d')
                                                    # Asegurar que sea entero (por si acaso)
                                                    fecha_arqueo_str = str(int(fecha_arqueo_str))
                                                    
                                                    # ARQUEO
                                                    justificacion_arqueo = 'CRUZAR'
                                                    nuevo_estado_arqueo = 'Cruce de novedades'
                                                    ratificar_grabar_arqueo = 'Reverso'
                                                    observaciones_arqueo = fecha_arqueo_str
                                                    
                                                    # DIARIO
                                                    justificacion_diario = 'CRUZAR'
                                                    nuevo_estado_diario = 'Cruce de novedades'
                                                    ratificar_grabar_diario = 'No'
                                                    observaciones_diario = fecha_arqueo_str
                                                    
                                                    # Resumen de pasos para ARQUEO
                                                    resumen_pasos_arqueo = []
                                                    resumen_pasos_arqueo.append(f"1. Verificado: {tipo_registro} y {tipo_otro} tienen misma diferencia (FALTANTE: ${diferencia_actual:,.0f})")
                                                    resumen_pasos_arqueo.append(f"2. Buscado en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${diferencia_actual:,.0f}")
                                                    resumen_pasos_arqueo.append("3. ✗ No encontrado en cuenta 110505075")
                                                    resumen_pasos_arqueo.append(f"4. Buscado en cuenta sobrantes 279510020 días anteriores (valores negativos), valor ${diferencia_actual:,.0f}")
                                                    resumen_pasos_arqueo.append(f"5. ✓ Encontrado 1 movimiento negativo que suma ${suma_encontrada:,.0f}")
                                                    resumen_pasos_arqueo.append("6. Clasificación: CRUCE DE NOVEDADES - Reverso (un solo registro)")
                                                    
                                                    # Resumen de pasos para DIARIO
                                                    resumen_pasos_diario = resumen_pasos_arqueo.copy()
                                                    resumen_pasos_diario[-1] = "6. Clasificación: CRUCE DE NOVEDADES - No ratificar (un solo registro)"
                                                else:
                                                    # CASO 2a.2: La suma de varios sobrantes es igual o mayor al faltante
                                                    logger.info(
                                                        f"Cajero {codigo_cajero}: La suma de {num_movimientos} sobrantes ({suma_encontrada:,.0f}) "
                                                        f"es igual o mayor al faltante ({diferencia_actual:,.0f}). "
                                                        f"Aplicando regla: PENDIENTE REVISION MANUAL"
                                                    )
                                                    
                                                    # ARQUEO Y DIARIO - Ambos PENDIENTE REVISION MANUAL
                                                    justificacion_arqueo = 'PENDIENTE REVISION MANUAL'
                                                    nuevo_estado_arqueo = 'Pendiente de revisión manual'
                                                    ratificar_grabar_arqueo = 'No'
                                                    observaciones_arqueo = 'Este caso requiere la supervisión de personal encargado.'
                                                    
                                                    justificacion_diario = 'PENDIENTE REVISION MANUAL'
                                                    nuevo_estado_diario = 'Pendiente de revisión manual'
                                                    ratificar_grabar_diario = 'No'
                                                    observaciones_diario = 'Este caso requiere la supervisión de personal encargado.'
                                                    
                                                    # Resumen de pasos para ARQUEO
                                                    resumen_pasos_arqueo = []
                                                    resumen_pasos_arqueo.append(f"1. Verificado: {tipo_registro} y {tipo_otro} tienen misma diferencia (FALTANTE: ${diferencia_actual:,.0f})")
                                                    resumen_pasos_arqueo.append(f"2. Buscado en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${diferencia_actual:,.0f}")
                                                    resumen_pasos_arqueo.append("3. ✗ No encontrado en cuenta 110505075")
                                                    resumen_pasos_arqueo.append(f"4. Buscado en cuenta sobrantes 279510020 días anteriores (valores negativos), valor ${diferencia_actual:,.0f}")
                                                    resumen_pasos_arqueo.append(f"5. ✓ Encontrados {num_movimientos} movimientos negativos que suman ${suma_encontrada:,.0f}")
                                                    resumen_pasos_arqueo.append("6. Clasificación: PENDIENTE REVISION MANUAL (suma de varios sobrantes)")
                                                    
                                                    # Resumen de pasos para DIARIO
                                                    resumen_pasos_diario = resumen_pasos_arqueo.copy()
                                                
                                                # Actualizar ARQUEO
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_arqueo
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_arqueo
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_arqueo
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                                
                                                # Actualizar el registro del otro tipo
                                                idx_otro_tipo = registro_otro_tipo.name
                                                self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_diario
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                            
                                            else:
                                                # CASO 2b: NO aparece en cuenta de sobrantes 279510020 (valores negativos que sumen el faltante)
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: No se encontraron sobrantes negativos que sumen el faltante en cuenta 279510020. "
                                                    f"Aplicando regla: FALTANTE EN ARQUEO"
                                                )
                                                
                                                regla_arqueo_diario_igual_faltante = True
                                                
                                                # ARQUEO: Ratificar grabar = Si (se graba el faltante)
                                                justificacion_arqueo = 'Fisico'
                                                nuevo_estado_arqueo = 'FALTANTE EN ARQUEO'
                                                ratificar_grabar_arqueo = 'Si'
                                                observaciones_arqueo = 'Faltante en arqueo'
                                                
                                                # DIARIO: Ratificar grabar = No (no se graba para evitar duplicar)
                                                justificacion_diario = 'Fisico'
                                                nuevo_estado_diario = 'FALTANTE EN ARQUEO'
                                                ratificar_grabar_diario = 'No'
                                                observaciones_diario = 'Faltante en arqueo'
                                                
                                                # Resumen de pasos para ARQUEO
                                                resumen_pasos_arqueo = []
                                                resumen_pasos_arqueo.append(f"1. Verificado: {tipo_registro} y {tipo_otro} tienen misma diferencia (FALTANTE: ${diferencia_actual:,.0f})")
                                                resumen_pasos_arqueo.append(f"2. Buscado en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${diferencia_actual:,.0f}")
                                                resumen_pasos_arqueo.append("3. ✗ No encontrado en cuenta 110505075")
                                                resumen_pasos_arqueo.append(f"4. Buscado en cuenta sobrantes 279510020 días anteriores (valores negativos que sumen el faltante), valor ${diferencia_actual:,.0f}")
                                                resumen_pasos_arqueo.append("5. ✗ No encontrados sobrantes negativos que sumen el faltante")
                                                resumen_pasos_arqueo.append("6. Clasificación: FALTANTE EN ARQUEO - Ratificar grabar (solo ARQUEO)")
                                                
                                                # Resumen de pasos para DIARIO
                                                resumen_pasos_diario = resumen_pasos_arqueo.copy()
                                                resumen_pasos_diario[-1] = "6. Clasificación: FALTANTE EN ARQUEO - No ratificar (evitar duplicar)"
                                                
                                                # Actualizar el registro ARQUEO (usando indices_original)
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_arqueo
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_arqueo
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_arqueo
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                                
                                                # Actualizar también el registro del otro tipo
                                                idx_otro_tipo = registro_otro_tipo.name
                                                self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_diario
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                    
                                    except Exception as e:
                                        logger.warning(f"Error al aplicar regla ARQUEO/DIARIO igual faltante: {e}", exc_info=True)
                                else:
                                    # Si no hay fecha_arqueo_registro o consultor_bd, aplicar regla básica
                                    logger.info(
                                        f"Cajero {codigo_cajero}: {tipo_registro} y {tipo_otro} tienen la misma diferencia (FALTANTE: {diferencia_actual:,.0f}), "
                                        f"pero no se puede consultar BD. Aplicando regla básica."
                                    )
                                    
                                    regla_arqueo_diario_igual_faltante = True
                                    
                                    # Aplicar regla básica: FALTANTE EN ARQUEO
                                    if tipo_registro == 'ARQUEO':
                                        justificacion_actual = 'Fisico'
                                        nuevo_estado_actual = 'FALTANTE EN ARQUEO'
                                        ratificar_grabar_actual = 'Si'
                                        observaciones_actual = 'Faltante en arqueo'
                                    else:
                                        justificacion_actual = 'Fisico'
                                        nuevo_estado_actual = 'FALTANTE EN ARQUEO'
                                        ratificar_grabar_actual = 'No'
                                        observaciones_actual = 'Faltante en arqueo'
                                    
                                    # Actualizar el registro actual
                                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                    self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_actual
                                    self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_actual
                                    self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_actual
                                    
                                    # Actualizar también el registro del otro tipo
                                    idx_otro_tipo = registro_otro_tipo.name
                                    if tipo_registro == 'ARQUEO':
                                        ratificar_grabar_otro = 'No'
                                    else:
                                        ratificar_grabar_otro = 'Si'
                                    
                                    self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_otro
                                    self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_actual
                                    self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_actual
                                    self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_actual
                
                # Si ya se aplicó la regla de ARQUEO/DIARIO igual faltante, saltar las otras reglas
                if not regla_arqueo_sin_diario and not regla_diario_sin_arqueo and not regla_arqueo_diario_igual_faltante:
                    # NUEVA REGLA PRIORITARIA: Cuando ARQUEO y DIARIO tienen la misma diferencia (SOBRANTE)
                    
                    if tipo_registro == 'ARQUEO' and codigo_cajero is not None and sobrante < 0:
                        # Verificar si hay registro DIARIO para este cajero
                        registros_mismo_cajero = self._df_archivo_original[
                            self._df_archivo_original['codigo_cajero'] == codigo_cajero
                        ]
                        registros_diario_mismo_cajero = registros_mismo_cajero[
                            registros_mismo_cajero['tipo_registro'] == 'DIARIO'
                        ]
                        
                        if len(registros_diario_mismo_cajero) > 0:
                            # Hay registro DIARIO, verificar si tienen la misma diferencia (SOBRANTE)
                            registro_diario = registros_diario_mismo_cajero.iloc[0]
                            
                            sobrante_diario = limpiar_valor_numerico(registro_diario.get('sobrantes', 0))
                            
                            # Comparar sobrantes (deben ser iguales)
                            misma_diferencia_sobrante = False
                            if sobrante < 0 and sobrante_diario < 0:
                                if abs(sobrante - sobrante_diario) < 0.01:  # Tolerancia para floats
                                    misma_diferencia_sobrante = True
                            
                            if misma_diferencia_sobrante:
                                # Aplicar nueva regla: ARQUEO y DIARIO con misma diferencia (SOBRANTE)
                                logger.info(
                                    f"Cajero {codigo_cajero}: ARQUEO y DIARIO tienen la misma diferencia (SOBRANTE: {sobrante}). "
                                    f"Aplicando regla de Error en Transmicion de contadores"
                                )
                                
                                # Obtener consultor BD si está disponible
                                consultor_bd = None
                                if self.consultor and hasattr(self.consultor, '_consultor_bd'):
                                    consultor_bd = self.consultor._consultor_bd
                                
                                if fecha_arqueo_registro and consultor_bd:
                                    try:
                                        config_data = self.config.cargar()
                                        query_params = config_data.get('base_datos', {}).get('query_params', {})
                                        
                                        # PASO 1: Buscar en NACIONAL cuenta 110505075, el día del arqueo, por el valor del sobrante
                                        movimiento_nacional = consultor_bd.consultar_movimientos_nacional(
                                            codigo_cajero=codigo_cajero,
                                            fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                            valor_descuadre=sobrante,  # Sobrante es negativo
                                            cuenta=query_params.get('cuenta', 110505075),
                                            codofi_excluir=query_params.get('codofi_excluir', 976),
                                            nrocmp=query_params.get('nrocmp', 770500)
                                        )
                                        
                                        if movimiento_nacional:
                                            # CASO 1: Aparece en NACIONAL cuenta 110505075
                                            # Verificar si la fecha del movimiento coincide con la fecha del arqueo
                                            fecha_movimiento_diferente = False
                                            fecha_movimiento = None
                                            
                                            try:
                                                # Extraer fecha del movimiento
                                                fecha_movimiento_num = movimiento_nacional.get('FECHA')
                                                if fecha_movimiento_num:
                                                    # Convertir a datetime: YYYYMMDD
                                                    fecha_movimiento_num_int = int(float(fecha_movimiento_num))
                                                    anio = fecha_movimiento_num_int // 10000
                                                    mes = (fecha_movimiento_num_int % 10000) // 100
                                                    dia = fecha_movimiento_num_int % 100
                                                    fecha_movimiento = datetime(anio, mes, dia)
                                                    
                                                    # Comparar solo la fecha (sin hora)
                                                    fecha_arqueo_sin_hora = fecha_arqueo_registro.replace(hour=0, minute=0, second=0, microsecond=0)
                                                    if fecha_movimiento != fecha_arqueo_sin_hora:
                                                        fecha_movimiento_diferente = True
                                            except Exception as e:
                                                logger.debug(f"Error al comparar fechas del movimiento: {e}")
                                            
                                            regla_arqueo_diario_igual_sobrante = True
                                            
                                            if fecha_movimiento_diferente:
                                                # CASO 1a: Movimiento encontrado pero con fecha diferente
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Movimiento encontrado en NACIONAL cuenta 110505075 "
                                                    f"pero con fecha diferente (movimiento: {fecha_movimiento.strftime('%Y-%m-%d')}, "
                                                    f"arqueo: {fecha_arqueo_registro.strftime('%Y-%m-%d')}). "
                                                    f"Aplicando regla: CRUCE DE NOVEDADES"
                                                )
                                                
                                                # ARQUEO
                                                justificacion_arqueo = 'CRUZAR'
                                                nuevo_estado_arqueo = 'Cruce de novedades'
                                                ratificar_grabar_arqueo = 'Reverso'
                                                observaciones_arqueo = 'Se reversa diferencia con arqueo anterior'
                                                
                                                # Resumen de pasos para ARQUEO
                                                resumen_pasos_arqueo = []
                                                resumen_pasos_arqueo.append(f"1. Verificado: ARQUEO y DIARIO tienen misma diferencia (SOBRANTE: ${abs(sobrante):,.0f})")
                                                resumen_pasos_arqueo.append(f"2. Buscado en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${abs(sobrante):,.0f}")
                                                resumen_pasos_arqueo.append("3. ✓ Movimiento encontrado en cuenta 110505075")
                                                resumen_pasos_arqueo.append(f"4. Fecha movimiento: {fecha_movimiento.strftime('%Y-%m-%d')}, Fecha arqueo: {fecha_arqueo_registro.strftime('%Y-%m-%d')}")
                                                resumen_pasos_arqueo.append("5. Fechas diferentes → Clasificación: CRUCE DE NOVEDADES - Reverso")
                                                
                                                # DIARIO
                                                justificacion_diario = 'CRUZAR'
                                                nuevo_estado_diario = 'Cruce de novedades'
                                                ratificar_grabar_diario = 'No'
                                                observaciones_diario = 'Se reversa diferencia con arqueo anterior'
                                                
                                                # Resumen de pasos para DIARIO
                                                resumen_pasos_diario = resumen_pasos_arqueo.copy()
                                                resumen_pasos_diario[-1] = "5. Fechas diferentes → Clasificación: CRUCE DE NOVEDADES - No ratificar"
                                                
                                                # Actualizar ARQUEO
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_arqueo
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_arqueo
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_arqueo
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                                
                                                # Actualizar el registro del otro tipo
                                                idx_otro_tipo = registro_otro_tipo.name
                                                self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_diario
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                            else:
                                                # CASO 1b: Movimiento encontrado con la misma fecha
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Movimiento encontrado en NACIONAL cuenta 110505075 "
                                                    f"con fecha coincidente. Aplicando regla: Pendiente de gestión"
                                                )
                                                
                                                # ARQUEO Y DIARIO - Ambos deben tener la misma clasificación
                                                justificacion = 'PENDIENTE DE GESTION'
                                                nuevo_estado = 'Pendiente de gestión'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Cajero cuadrado con arqueo de la sucursal'
                                                
                                                # Resumen de pasos
                                                resumen_pasos.append(f"1. Verificado: ARQUEO y DIARIO tienen misma diferencia (SOBRANTE: ${abs(sobrante):,.0f})")
                                                resumen_pasos.append(f"2. Buscado en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${abs(sobrante):,.0f}")
                                                resumen_pasos.append("3. ✓ Movimiento encontrado en cuenta 110505075")
                                                if fecha_movimiento:
                                                    resumen_pasos.append(f"4. Fecha movimiento: {fecha_movimiento.strftime('%Y-%m-%d')}, Fecha arqueo: {fecha_arqueo_registro.strftime('%Y-%m-%d')}")
                                                    resumen_pasos.append("5. Fechas coinciden → Clasificación: PENDIENTE DE GESTION - Cajero cuadrado con arqueo de la sucursal")
                                                else:
                                                    resumen_pasos.append("4. Clasificación: PENDIENTE DE GESTION - Cajero cuadrado con arqueo de la sucursal")
                                            
                                            # Actualizar el registro ARQUEO (usando indices_original)
                                            self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                            self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                            self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                            if 'resumen_pasos' in self._df_archivo_original.columns:
                                                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                            
                                            # Actualizar también el registro del otro tipo
                                            idx_otro_tipo = registro_otro_tipo.name
                                            self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                            self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion
                                            self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado
                                            self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones
                                            if 'resumen_pasos' in self._df_archivo_original.columns:
                                                self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                        
                                        else:
                                            # CASO 2: NO aparece en NACIONAL cuenta 110505075
                                            # Buscar en NACIONAL cuenta de faltantes 168710093 en días anteriores
                                            logger.info(
                                                f"Cajero {codigo_cajero}: No se encontró movimiento en NACIONAL cuenta 110505075. "
                                                f"Buscando en cuenta de faltantes 168710093 en días anteriores..."
                                            )
                                            
                                            # Primero buscar el mismo día
                                            movimiento_faltantes = consultor_bd.consultar_cuenta_faltantes(
                                                codigo_cajero=codigo_cajero,
                                                fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                                valor_descuadre=sobrante,  # Sobrante es negativo
                                                cuenta=168710093,
                                                codofi_excluir=query_params.get('codofi_excluir', 976)
                                            )
                                            
                                            # Si no se encuentra el mismo día, buscar en días anteriores
                                            if not movimiento_faltantes:
                                                movimiento_faltantes = consultor_bd.consultar_cuenta_faltantes_dias_anteriores(
                                                    codigo_cajero=codigo_cajero,
                                                    fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                                    valor_descuadre=sobrante,
                                                    cuenta=168710093,
                                                    codofi_excluir=query_params.get('codofi_excluir', 976),
                                                    dias_anteriores=30
                                                )
                                            
                                            if movimiento_faltantes:
                                                # CASO 2a: Aparece en cuenta de faltantes 168710093 (mismo día o días anteriores)
                                                fecha_movimiento = movimiento_faltantes.get('FECHA')
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Movimiento encontrado en cuenta de faltantes 168710093 "
                                                    f"(fecha movimiento: {fecha_movimiento}). "
                                                    f"Aplicando regla: CRUCE DE NOVEDADES"
                                                )
                                                
                                                regla_arqueo_diario_igual_sobrante = True
                                                
                                                # Consultar documento responsable
                                                documento_responsable = consultor_bd.consultar_documento_responsable(
                                                    codigo_sucursal=64,  # Por defecto 64, podría venir de configuración
                                                    cuenta=168710093,
                                                    nrocmp=770500,
                                                    anio=fecha_arqueo_registro.year,
                                                    mes_inicio=fecha_arqueo_registro.month,
                                                    mes_fin=fecha_arqueo_registro.month
                                                )
                                                
                                                # ARQUEO
                                                justificacion_arqueo = 'CRUZAR'
                                                nuevo_estado_arqueo = 'Cruce de novedades'
                                                ratificar_grabar_arqueo = 'Reverso'
                                                observaciones_arqueo = 'Se reversa diferencia con arqueo anterior'
                                                
                                                # Resumen de pasos para ARQUEO
                                                resumen_pasos_arqueo = []
                                                resumen_pasos_arqueo.append(f"1. Verificado: ARQUEO y DIARIO tienen misma diferencia (SOBRANTE: ${abs(sobrante):,.0f})")
                                                resumen_pasos_arqueo.append(f"2. Buscado en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${abs(sobrante):,.0f}")
                                                resumen_pasos_arqueo.append("3. ✗ No encontrado en cuenta 110505075")
                                                resumen_pasos_arqueo.append(f"4. Buscado en cuenta faltantes 168710093 días anteriores, valor ${abs(sobrante):,.0f}")
                                                resumen_pasos_arqueo.append(f"5. ✓ Movimiento encontrado en cuenta 168710093 (fecha: {fecha_movimiento})")
                                                resumen_pasos_arqueo.append("6. Clasificación: CRUCE DE NOVEDADES - Reverso")
                                                
                                                # DIARIO
                                                justificacion_diario = 'CRUZAR'
                                                nuevo_estado_diario = 'Cruce de novedades'
                                                ratificar_grabar_diario = 'No'
                                                observaciones_diario = 'Se reversa diferencia con arqueo anterior'
                                                
                                                # Resumen de pasos para DIARIO (igual que ARQUEO pero con clasificación diferente)
                                                resumen_pasos_diario = resumen_pasos_arqueo.copy()
                                                resumen_pasos_diario[-1] = "6. Clasificación: CRUCE DE NOVEDADES - No ratificar"
                                                
                                                # Actualizar ARQUEO
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_arqueo
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_arqueo
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_arqueo
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                                
                                                # Agregar documento_responsable si existe la columna
                                                if 'documento_responsable' in self._df_archivo_original.columns and documento_responsable:
                                                    self._df_archivo_original.loc[indices_original, 'documento_responsable'] = documento_responsable
                                                
                                                # Actualizar el registro del otro tipo
                                                idx_otro_tipo = registro_otro_tipo.name
                                                self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_diario
                                                self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_diario
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                            
                                            else:
                                                # CASO 2b: NO aparece en cuenta de faltantes 168710093
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: No se encontró movimiento en cuenta de faltantes 168710093. "
                                                    f"Aplicando regla: CONTABILIZACION SOBRANTE FISICO"
                                                )
                                                
                                                regla_arqueo_diario_igual_sobrante = True
                                                
                                                # ARQUEO: Ratificar grabar = Si (se graba el sobrante)
                                                justificacion_arqueo = 'Fisico'
                                                nuevo_estado_arqueo = 'CONTABILIZACION SOBRANTE FISICO'
                                                ratificar_grabar_arqueo = 'Si'
                                                observaciones_arqueo = 'Contabilización sobrante físico'
                                                
                                                # DIARIO: Ratificar grabar = No (no se graba para evitar duplicar)
                                                justificacion_diario = 'Fisico'
                                                nuevo_estado_diario = 'CONTABILIZACION SOBRANTE FISICO'
                                                ratificar_grabar_diario = 'No'
                                                observaciones_diario = 'Contabilización sobrante físico'
                                                
                                                # Resumen de pasos para ARQUEO
                                                resumen_pasos_arqueo = []
                                                resumen_pasos_arqueo.append(f"1. Verificado: ARQUEO y DIARIO tienen misma diferencia (SOBRANTE: ${abs(sobrante):,.0f})")
                                                resumen_pasos_arqueo.append(f"2. Buscado en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${abs(sobrante):,.0f}")
                                                resumen_pasos_arqueo.append("3. ✗ No encontrado en cuenta 110505075")
                                                resumen_pasos_arqueo.append(f"4. Buscado en cuenta faltantes 168710093 días anteriores, valor ${abs(sobrante):,.0f}")
                                                resumen_pasos_arqueo.append("5. ✗ No encontrado en cuenta 168710093")
                                                resumen_pasos_arqueo.append("6. Clasificación: CONTABILIZACION SOBRANTE FISICO - Ratificar grabar (solo ARQUEO)")
                                                
                                                # Resumen de pasos para DIARIO
                                                resumen_pasos_diario = resumen_pasos_arqueo.copy()
                                                resumen_pasos_diario[-1] = "6. Clasificación: CONTABILIZACION SOBRANTE FISICO - No ratificar (evitar duplicar)"
                                                
                                                # Actualizar el registro ARQUEO (usando indices_original)
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_arqueo
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_arqueo
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_arqueo
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                                
                                                # Actualizar también el registro DIARIO (en esta sección todavía se usa registro_diario porque solo se ejecuta cuando tipo_registro == 'ARQUEO')
                                                idx_diario = registro_diario.name
                                                self._df_archivo_original.loc[idx_diario, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                self._df_archivo_original.loc[idx_diario, 'justificacion'] = justificacion_diario
                                                self._df_archivo_original.loc[idx_diario, 'nuevo_estado'] = nuevo_estado_diario
                                                self._df_archivo_original.loc[idx_diario, 'observaciones'] = observaciones_diario
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_diario, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                    
                                    except Exception as e:
                                        logger.warning(f"Error al aplicar regla ARQUEO/DIARIO igual sobrante: {e}", exc_info=True)
                
                # Si ya se aplicó alguna regla de ARQUEO/DIARIO igual, saltar las otras reglas
                if not regla_arqueo_sin_diario and not regla_diario_sin_arqueo and not regla_arqueo_diario_igual_faltante and not regla_arqueo_diario_igual_sobrante:
                    # NUEVA REGLA: Verificar si aplica regla de provisión para sobrantes exagerados
                    # Hay dos casos:
                    # 1. ARQUEO con sobrante >= $10M, SIN registro DIARIO -> buscar provisión día anterior
                    # 2. ARQUEO con sobrante >= $10M, CON registro DIARIO con faltante -> buscar provisión mismo día
                    
                    if (tipo_registro == 'ARQUEO' and 
                        sobrante < 0 and 
                        abs(sobrante) >= 10000000):
                        
                        # Verificar si hay registro DIARIO para este cajero en el archivo original
                        tiene_diario = False
                        faltante_diario = 0.0
                        if codigo_cajero is not None:
                            registros_mismo_cajero = self._df_archivo_original[
                                self._df_archivo_original['codigo_cajero'] == codigo_cajero
                            ]
                            registros_diario = registros_mismo_cajero[
                                registros_mismo_cajero['tipo_registro'] == 'DIARIO'
                            ]
                            tiene_diario = len(registros_diario) > 0
                            
                            if tiene_diario:
                                # Obtener el faltante del registro DIARIO
                                row_diario = registros_diario.iloc[0]
                                faltante_diario = limpiar_valor_numerico(row_diario['faltantes'])
                        
                        # Obtener consultor BD si está disponible
                        consultor_bd = None
                        if self.consultor and hasattr(self.consultor, '_consultor_bd'):
                            consultor_bd = self.consultor._consultor_bd
                        
                        if fecha_arqueo_registro and consultor_bd:
                            try:
                                config_data = self.config.cargar()
                                query_params = config_data.get('base_datos', {}).get('query_params', {})
                                
                                if tiene_diario and faltante_diario > 0:
                                    # CASO 2: ARQUEO con sobrante >= $10M, CON registro DIARIO con faltante
                                    # Buscar provisión el mismo día del arqueo
                                    logger.info(
                                        f"Cajero {codigo_cajero}: Aplicando regla de provisión mismo día. "
                                        f"ARQUEO sobrante: {sobrante}, DIARIO faltante: {faltante_diario}"
                                    )
                                    
                                    provision = consultor_bd.consultar_provision_mismo_dia(
                                        codigo_cajero=codigo_cajero,
                                        fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                        cuenta=query_params.get('cuenta', 110505075),
                                        codofi_excluir=query_params.get('codofi_excluir', 976),
                                        nrocmp_provision=810291
                                    )
                                    
                                    if provision:
                                        valor_provision = abs(float(provision.get('VALOR', 0)))
                                        valor_sobrante_abs = abs(sobrante)
                                        
                                        # Calcular la diferencia entre provisión y sobrante
                                        # Esta diferencia puede explicar parte del faltante del DIARIO
                                        diferencia_provision_sobrante = valor_provision - valor_sobrante_abs
                                        
                                        # Tolerancia de 1000 pesos para diferencias por redondeo
                                        # Verificar si la provisión explica el sobrante (provisión ≈ sobrante)
                                        if abs(valor_provision - valor_sobrante_abs) <= 1000000:  # Tolerancia de 1M para sobrantes grandes
                                            # La provisión explica el sobrante
                                            regla_provision_aplicada = True
                                            
                                            # Verificar si la diferencia explica parte del faltante del DIARIO
                                            if diferencia_provision_sobrante > 0 and faltante_diario > 0:
                                                # La diferencia entre provisión y sobrante puede explicar parte del faltante
                                                faltante_explicado = min(diferencia_provision_sobrante, faltante_diario)
                                                faltante_restante = faltante_diario - faltante_explicado
                                                
                                                # ARQUEO: El sobrante se explica por la provisión
                                                justificacion_arqueo = 'Pendiente de gestion'
                                                nuevo_estado_arqueo = 'PENDIENTE DE GESTION'
                                                ratificar_grabar_arqueo = 'No'
                                                observaciones_arqueo = f'Cajero cuadrado con arqueo en la sucursal. Provisión del mismo día ({valor_provision:,.0f}) explica el sobrante ({valor_sobrante_abs:,.0f}).'
                                                
                                                # Resumen de pasos para ARQUEO
                                                resumen_pasos_arqueo = []
                                                resumen_pasos_arqueo.append(f"1. Identificado: ARQUEO con sobrante ${valor_sobrante_abs:,.0f} y DIARIO con faltante ${faltante_diario:,.0f}")
                                                resumen_pasos_arqueo.append(f"2. Buscada provisión mismo día (NROCMP 810291)")
                                                resumen_pasos_arqueo.append(f"3. ✓ Provisión encontrada: ${valor_provision:,.0f}")
                                                resumen_pasos_arqueo.append(f"4. Provisión explica sobrante: ${valor_provision:,.0f} - ${valor_sobrante_abs:,.0f} = ${diferencia_provision_sobrante:,.0f}")
                                                resumen_pasos_arqueo.append(f"5. Diferencia explica ${faltante_explicado:,.0f} del faltante del DIARIO")
                                                resumen_pasos_arqueo.append("6. Clasificación: PENDIENTE DE GESTION - Cajero cuadrado con arqueo")
                                                
                                                # DIARIO: Parte explicada por provisión, parte restante se graba
                                                if faltante_restante > 0:
                                                    # Hay faltante restante que no se explica
                                                    justificacion_diario = 'Fisico'
                                                    nuevo_estado_diario = 'FALTANTE EN ARQUEO'
                                                    ratificar_grabar_diario = 'Si'
                                                    observaciones_diario = f'Faltante parcialmente explicado por provisión (${faltante_explicado:,.0f} de ${faltante_diario:,.0f}). Faltante restante (${faltante_restante:,.0f}) se graba.'
                                                    
                                                    # Resumen de pasos para DIARIO
                                                    resumen_pasos_diario = []
                                                    resumen_pasos_diario.append(f"1. Identificado: DIARIO con faltante ${faltante_diario:,.0f}")
                                                    resumen_pasos_diario.append(f"2. ARQUEO tiene sobrante ${valor_sobrante_abs:,.0f} explicado por provisión ${valor_provision:,.0f}")
                                                    resumen_pasos_diario.append(f"3. Diferencia provisión-sobrante (${diferencia_provision_sobrante:,.0f}) explica ${faltante_explicado:,.0f} del faltante")
                                                    resumen_pasos_diario.append(f"4. Faltante restante: ${faltante_restante:,.0f} (no explicado)")
                                                    resumen_pasos_diario.append("5. Clasificación: FALTANTE EN ARQUEO - Ratificar grabar")
                                                else:
                                                    # Todo el faltante se explica
                                                    justificacion_diario = 'Pendiente de gestion'
                                                    nuevo_estado_diario = 'PENDIENTE DE GESTION'
                                                    ratificar_grabar_diario = 'No'
                                                    observaciones_diario = f'Cajero cuadrado con arqueo en la sucursal. Provisión del mismo día ({valor_provision:,.0f}) explica el sobrante ({valor_sobrante_abs:,.0f}) y el faltante ({faltante_diario:,.0f}).'
                                                    
                                                    # Resumen de pasos para DIARIO
                                                    resumen_pasos_diario = []
                                                    resumen_pasos_diario.append(f"1. Identificado: DIARIO con faltante ${faltante_diario:,.0f}")
                                                    resumen_pasos_diario.append(f"2. ARQUEO tiene sobrante ${valor_sobrante_abs:,.0f} explicado por provisión ${valor_provision:,.0f}")
                                                    resumen_pasos_diario.append(f"3. Diferencia provisión-sobrante (${diferencia_provision_sobrante:,.0f}) explica completamente el faltante")
                                                    resumen_pasos_diario.append("4. Clasificación: PENDIENTE DE GESTION - Cajero cuadrado con arqueo")
                                                
                                                # Actualizar ARQUEO
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_arqueo
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_arqueo
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_arqueo
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                                
                                                # Actualizar DIARIO
                                                if codigo_cajero is not None:
                                                    registros_diario = self._df_archivo_original[
                                                        (self._df_archivo_original['codigo_cajero'] == codigo_cajero) &
                                                        (self._df_archivo_original['tipo_registro'] == 'DIARIO')
                                                    ]
                                                    if len(registros_diario) > 0:
                                                        idx_diario = registros_diario.index[0]
                                                        self._df_archivo_original.loc[idx_diario, 'justificacion'] = justificacion_diario
                                                        self._df_archivo_original.loc[idx_diario, 'nuevo_estado'] = nuevo_estado_diario
                                                        if 'ratificar_grabar_diferencia' in self._df_archivo_original.columns:
                                                            self._df_archivo_original.loc[idx_diario, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                        if 'observaciones' in self._df_archivo_original.columns:
                                                            self._df_archivo_original.loc[idx_diario, 'observaciones'] = observaciones_diario
                                                        if 'resumen_pasos' in self._df_archivo_original.columns:
                                                            self._df_archivo_original.loc[idx_diario, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                                
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Provisión mismo día encontrada. "
                                                    f"Provisión: {valor_provision:,.0f}, Sobrante ARQUEO: {valor_sobrante_abs:,.0f}, "
                                                    f"Faltante DIARIO: {faltante_diario:,.0f}, Diferencia: {diferencia_provision_sobrante:,.0f}, "
                                                    f"Faltante explicado: {faltante_explicado:,.0f}, Faltante restante: {faltante_restante:,.0f}"
                                                )
                                            else:
                                                # La provisión explica el sobrante pero no hay faltante en DIARIO o la diferencia no explica nada
                                                justificacion = 'Pendiente de gestion'
                                                nuevo_estado = 'PENDIENTE DE GESTION'
                                                ratificar_grabar = 'No'
                                                observaciones = f'Cajero cuadrado con arqueo en la sucursal. Provisión del mismo día ({valor_provision:,.0f}) explica el sobrante ({valor_sobrante_abs:,.0f}).'
                                                
                                                resumen_pasos.append(f"1. Identificado: ARQUEO con sobrante ${valor_sobrante_abs:,.0f}")
                                                resumen_pasos.append(f"2. Buscada provisión mismo día (NROCMP 810291)")
                                                resumen_pasos.append(f"3. ✓ Provisión encontrada: ${valor_provision:,.0f}")
                                                resumen_pasos.append(f"4. Provisión explica sobrante")
                                                resumen_pasos.append("5. Clasificación: PENDIENTE DE GESTION - Cajero cuadrado con arqueo")
                                                
                                                # Actualizar ARQUEO
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Provisión mismo día encontrada y explica el sobrante. "
                                                    f"Provisión: {valor_provision:,.0f}, Sobrante ARQUEO: {valor_sobrante_abs:,.0f}"
                                                )
                                        else:
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Provisión mismo día encontrada pero NO explica el sobrante. "
                                                f"Provisión: {valor_provision:,.0f}, Sobrante ARQUEO: {valor_sobrante_abs:,.0f}, "
                                                f"Diferencia: {abs(valor_provision - valor_sobrante_abs):,.0f}"
                                            )
                                    else:
                                        logger.info(
                                            f"Cajero {codigo_cajero}: No se encontró provisión mismo día. "
                                            f"Continuando con otras reglas."
                                        )
                                elif not tiene_diario and abs(sobrante) % 100000 == 0:
                                    # CASO 1: ARQUEO con sobrante >= $10M, SIN registro DIARIO, múltiplo de 100k
                                    # Buscar provisión el día anterior
                                    logger.info(
                                        f"Cajero {codigo_cajero}: Aplicando regla de provisión día anterior. "
                                        f"Sobrante: {sobrante}, Tipo: ARQUEO, Sin DIARIO"
                                    )
                                    
                                    provision = consultor_bd.consultar_provision(
                                        codigo_cajero=codigo_cajero,
                                        fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                        valor_sobrante=sobrante,
                                        cuenta=query_params.get('cuenta', 110505075),
                                        codofi_excluir=query_params.get('codofi_excluir', 976),
                                        nrocmp_provision=810291
                                    )
                                    
                                    if provision:
                                        valor_provision = abs(float(provision.get('VALOR', 0)))
                                        valor_sobrante_abs = abs(sobrante)
                                        
                                        if valor_provision == valor_sobrante_abs:
                                            # Caso 1: Valor igual al sobrante
                                            justificacion = 'Pendiente de gestion'
                                            nuevo_estado = 'PENDIENTE DE GESTION'
                                            ratificar_grabar = 'No'
                                            observaciones = 'Cajero cuadrado con arqueo en la sucursal'
                                            regla_provision_aplicada = True
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Provisión día anterior encontrada con valor igual "
                                                f"al sobrante ({valor_provision:,.0f}). Caso cerrado."
                                            )
                                        elif valor_provision < valor_sobrante_abs:
                                            # Caso 2: Valor menor al sobrante (hay otros motivos)
                                            justificacion = 'Pendiente de gestion'
                                            nuevo_estado = 'PENDIENTE DE GESTION'
                                            ratificar_grabar = 'No'
                                            observaciones = 'Varios motivos de descuadre, uno de ellos es provición el día anterior.'
                                            regla_provision_aplicada = True
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Provisión día anterior encontrada con valor menor "
                                                f"al sobrante ({valor_provision:,.0f} < {valor_sobrante_abs:,.0f}). "
                                                f"Hay otros motivos de descuadre."
                                            )
                                    
                                    if not regla_provision_aplicada:
                                        # Caso 3: No se encontró provisión en NACIONAL
                                        logger.info(
                                            f"Cajero {codigo_cajero}: No se encontró provisión día anterior en NACIONAL. "
                                            f"Continuando con otras reglas."
                                        )
                            except Exception as e:
                                logger.warning(f"Error al consultar provisión: {e}", exc_info=True)
                
                # Si ya se aplicó la regla de provisión, saltar las otras reglas
                if not regla_arqueo_sin_diario and not regla_diario_sin_arqueo and not regla_arqueo_diario_igual_faltante and not regla_arqueo_diario_igual_sobrante and not regla_provision_aplicada:
                    # Aplicar reglas de negocio según donde se encontró el movimiento
                    if movimiento_encontrado and movimiento_fuente == 'NACIONAL':
                        # Verificar si la fecha del movimiento es diferente a la fecha del arqueo
                        fecha_movimiento_diferente = False
                        fecha_movimiento = None
                        fecha_arqueo_sin_hora = None
                        
                        if movimiento_detalle and fecha_arqueo_registro:
                            try:
                                import json
                                if isinstance(movimiento_detalle, str):
                                    detalle = json.loads(movimiento_detalle)
                                else:
                                    detalle = movimiento_detalle
                                
                                # Extraer fecha del movimiento (formato: ANOELB*10000+MESELB*100+DIAELB)
                                if 'FECHA' in detalle:
                                    fecha_movimiento_num = int(float(detalle['FECHA']))
                                    # Convertir a datetime: YYYYMMDD
                                    anio = fecha_movimiento_num // 10000
                                    mes = (fecha_movimiento_num % 10000) // 100
                                    dia = fecha_movimiento_num % 100
                                    fecha_movimiento = datetime(anio, mes, dia)
                                    
                                    # Comparar solo la fecha (sin hora)
                                    fecha_arqueo_sin_hora = fecha_arqueo_registro.replace(hour=0, minute=0, second=0, microsecond=0)
                                    if fecha_movimiento != fecha_arqueo_sin_hora:
                                        fecha_movimiento_diferente = True
                                        logger.info(
                                            f"Cajero {codigo_cajero}: "
                                            f"Movimiento encontrado en NACIONAL con fecha diferente "
                                            f"(movimiento: {fecha_movimiento.strftime('%Y-%m-%d')}, "
                                            f"arqueo: {fecha_arqueo_sin_hora.strftime('%Y-%m-%d')}). "
                                            f"Aplicando regla CRUCE DE NOVEDADES"
                                        )
                            except Exception as e:
                                logger.debug(f"Error al comparar fechas: {e}")
                        
                        if fecha_movimiento_diferente:
                            # NUEVA REGLA: Movimiento en NACIONAL con fecha diferente al arqueo
                            justificacion = 'Cruzar'
                            nuevo_estado = 'CRUCE DE NOVEDADES'
                            observaciones = 'Se reversa diferencia con cuadre anterior.'
                            
                            # Diferencia según tipo de registro
                            if tipo_registro == 'ARQUEO':
                                ratificar_grabar = 'Reverso'
                            elif tipo_registro == 'DIARIO':
                                ratificar_grabar = 'No'
                            else:
                                # Por defecto, si no se identifica el tipo, usar 'Reverso' para ARQUEO
                                ratificar_grabar = 'Reverso'
                            
                            # Resumen de pasos
                            valor_descuadre = abs(sobrante) if sobrante != 0 else faltante
                            tipo_descuadre = 'SOBRANTE' if sobrante != 0 else 'FALTANTE'
                            resumen_pasos.append(f"1. Identificado: {tipo_descuadre} de ${valor_descuadre:,.0f}")
                            resumen_pasos.append(f"2. Buscado movimiento en NACIONAL cuenta 110505075")
                            resumen_pasos.append(f"3. ✓ Movimiento encontrado en NACIONAL")
                            if fecha_movimiento and fecha_arqueo_sin_hora:
                                resumen_pasos.append(f"4. Fecha movimiento: {fecha_movimiento.strftime('%Y-%m-%d')}, Fecha arqueo: {fecha_arqueo_sin_hora.strftime('%Y-%m-%d')}")
                                resumen_pasos.append(f"5. Fechas diferentes → Clasificación: CRUCE DE NOVEDADES")
                            else:
                                resumen_pasos.append(f"4. Fecha movimiento diferente a fecha arqueo")
                                resumen_pasos.append(f"5. Clasificación: CRUCE DE NOVEDADES")
                            resumen_pasos.append(f"6. Ratificar grabar: {ratificar_grabar}")
                        else:
                            # REGLA ORIGINAL: Si se encuentra en NACIONAL con la misma fecha del arqueo
                            justificacion = 'PENDIENTE DE GESTION'
                            nuevo_estado = 'PARTIDA YA CONTABILIZADA'
                            ratificar_grabar = 'No'
                            observaciones = 'Cajero cuadrado con arqueo de la sucursal'
                            
                            # Resumen de pasos
                            valor_descuadre = abs(sobrante) if sobrante != 0 else faltante
                            tipo_descuadre = 'SOBRANTE' if sobrante != 0 else 'FALTANTE'
                            resumen_pasos.append(f"1. Identificado: {tipo_descuadre} de ${valor_descuadre:,.0f}")
                            resumen_pasos.append(f"2. Buscado movimiento en NACIONAL cuenta 110505075")
                            resumen_pasos.append(f"3. ✓ Movimiento encontrado en NACIONAL")
                            if fecha_arqueo_registro:
                                resumen_pasos.append(f"4. Fecha movimiento: {fecha_arqueo_registro.strftime('%Y-%m-%d')}, Fecha arqueo: {fecha_arqueo_registro.strftime('%Y-%m-%d')}")
                                resumen_pasos.append(f"5. Fechas iguales → Clasificación: PARTIDA YA CONTABILIZADA")
                            else:
                                resumen_pasos.append(f"4. Fecha movimiento igual a fecha arqueo")
                                resumen_pasos.append(f"5. Clasificación: PARTIDA YA CONTABILIZADA")
                            resumen_pasos.append(f"6. Ratificar grabar: No")
                    elif movimiento_encontrado:
                        # Si se encuentra en cuentas de SOBRANTES o FALTANTES (BD)
                        valor_descuadre = abs(sobrante) if sobrante != 0 else faltante
                        tipo_descuadre = 'SOBRANTE' if sobrante != 0 else 'FALTANTE'
                        
                        if movimiento_fuente == 'SOBRANTES_BD':
                            justificacion = 'SOBRANTE CONTABLE'
                            nuevo_estado = 'SOBRANTE CONTABLE'
                            resumen_pasos.append(f"1. Identificado: {tipo_descuadre} de ${valor_descuadre:,.0f}")
                            resumen_pasos.append(f"2. Buscado movimiento en BD SOBRANTES")
                            resumen_pasos.append(f"3. ✓ Movimiento encontrado en SOBRANTES_BD")
                            resumen_pasos.append(f"4. Clasificación: SOBRANTE CONTABLE")
                            resumen_pasos.append(f"5. Ratificar grabar: No")
                        elif movimiento_fuente == 'FALTANTES_BD':
                            justificacion = 'FALTANTE CONTABLE'
                            nuevo_estado = 'FALTANTE CONTABLE'
                            resumen_pasos.append(f"1. Identificado: {tipo_descuadre} de ${valor_descuadre:,.0f}")
                            resumen_pasos.append(f"2. Buscado movimiento en BD FALTANTES")
                            resumen_pasos.append(f"3. ✓ Movimiento encontrado en FALTANTES_BD")
                            resumen_pasos.append(f"4. Clasificación: FALTANTE CONTABLE")
                            resumen_pasos.append(f"5. Ratificar grabar: No")
                        else:
                            # Por defecto según el tipo de descuadre
                            if sobrante != 0:
                                justificacion = 'SOBRANTE CONTABLE'
                                nuevo_estado = 'SOBRANTE CONTABLE'
                            else:
                                justificacion = 'FALTANTE CONTABLE'
                                nuevo_estado = 'FALTANTE CONTABLE'
                            resumen_pasos.append(f"1. Identificado: {tipo_descuadre} de ${valor_descuadre:,.0f}")
                            resumen_pasos.append(f"2. Buscado movimiento en BD")
                            resumen_pasos.append(f"3. ✓ Movimiento encontrado en {movimiento_fuente}")
                            resumen_pasos.append(f"4. Clasificación: {nuevo_estado}")
                            resumen_pasos.append(f"5. Ratificar grabar: No")
                        ratificar_grabar = 'No'
                        observaciones = None
                    else:
                        # Si NO se encuentra movimiento en ningún lado
                        valor_descuadre = abs(sobrante) if sobrante != 0 else faltante
                        tipo_descuadre = 'SOBRANTE' if sobrante != 0 else 'FALTANTE'
                        
                        if sobrante != 0:
                            justificacion = 'SOBRANTE EN ARQUEO'
                            nuevo_estado = 'SOBRANTE EN ARQUEO'
                        else:
                            justificacion = 'Fisico'
                            nuevo_estado = 'FALTANTE EN ARQUEO'
                            
                        resumen_pasos.append(f"1. Identificado: {tipo_descuadre} de ${valor_descuadre:,.0f}")
                        resumen_pasos.append(f"2. Buscado movimiento en NACIONAL cuenta 110505075")
                        resumen_pasos.append(f"3. ✗ No encontrado en NACIONAL")
                        resumen_pasos.append(f"4. Buscado movimiento en BD SOBRANTES/FALTANTES")
                        resumen_pasos.append(f"5. ✗ No encontrado en BD")
                        resumen_pasos.append(f"6. Clasificación: {nuevo_estado} (descuadre físico)")
                        resumen_pasos.append(f"7. Ratificar grabar: Si")
                        
                        ratificar_grabar = 'Si'
                        observaciones = None
                        
                        # Actualizar el registro con la clasificación determinada
                        if justificacion is not None and nuevo_estado is not None:
                            self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                            if 'ratificar_grabar_diferencia' in self._df_archivo_original.columns:
                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                            if 'observaciones' in self._df_archivo_original.columns and observaciones:
                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                            if 'resumen_pasos' in self._df_archivo_original.columns and resumen_pasos:
                                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                            actualizados += len(indices_original)
                            
                            logger.debug(
                                f"Actualizado registro: cajero {codigo_cajero}, tipo {tipo_registro} - "
                                f"justificacion='{justificacion}', nuevo_estado='{nuevo_estado}'"
                            )
                
                # CASO POR DEFECTO: Si no se aplicó ninguna regla, clasificar como "Pendiente de revisión manual"
                if justificacion is None or nuevo_estado is None:
                    logger.warning(
                        f"Cajero {codigo_cajero} (tipo {tipo_registro}): No se aplicó ninguna regla. "
                        f"Clasificando como 'Pendiente de revisión manual'"
                    )
                    justificacion = 'PENDIENTE REVISION MANUAL'
                    nuevo_estado = 'Pendiente de revisión manual'
                    ratificar_grabar = 'No'
                    observaciones = 'Este caso requiere la supervisión de personal encargado.'
                    resumen_pasos.append("1. Verificado: No se aplicó ninguna regla implementada")
                    resumen_pasos.append("2. Clasificación: PENDIENTE REVISION MANUAL")
                    
                    # Actualizar el registro
                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                    self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                    self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                    self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                    if 'resumen_pasos' in self._df_archivo_original.columns:
                        self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                    actualizados += len(indices_original)
        
        # Guardar el archivo actualizado en una copia (NO modificar el original)
        try:
            # Crear nombre para el archivo de salida (copia con actualizaciones)
            nombre_original = self._ruta_archivo_original.stem
            # Remover ".backup" si existe en el nombre
            if nombre_original.endswith('.backup'):
                nombre_original = nombre_original.replace('.backup', '')
            ruta_salida = self._ruta_archivo_original.parent / f"{nombre_original}_procesado.xlsx"
            
            # Si ya existe, intentar eliminarlo (si está abierto, se sobrescribirá directamente)
            if ruta_salida.exists():
                try:
                    ruta_salida.unlink()
                except PermissionError:
                    # Si el archivo está abierto, intentar sobrescribirlo directamente
                    logger.warning(f"El archivo {ruta_salida} está abierto. Intentando sobrescribirlo directamente...")
            
            # Guardar archivo actualizado en la copia
            self._df_archivo_original.to_excel(
                ruta_salida,
                index=False,
                engine='openpyxl'
            )
            
            logger.info(
                f"Archivo procesado guardado: {actualizados} registros modificados. "
                f"Archivo: {ruta_salida}"
            )
            logger.info(f"Archivo original NO modificado: {self._ruta_archivo_original}")
            
            # Guardar ruta de salida para retornarla
            self._ruta_archivo_procesado = ruta_salida
            
        except Exception as e:
            logger.error(f"Error al guardar archivo procesado: {e}", exc_info=True)
            raise
    
    def obtener_ruta_archivo_procesado(self) -> Optional[Path]:
        """
        Obtiene la ruta del archivo procesado (copia con actualizaciones).
        
        Returns:
            Path al archivo procesado o None si no se ha procesado.
        """
        return self._ruta_archivo_procesado
    
    def obtener_datos_procesados(self) -> Optional[pd.DataFrame]:
        """
        Obtiene los datos procesados más recientes.
        
        Returns:
            DataFrame con los datos procesados o None si no hay datos.
        """
        return self._datos_procesados
    
    def guardar_resultados(
        self, 
        df: pd.DataFrame, 
        nombre_archivo: str,
        directorio_salida: Optional[Path] = None
    ) -> Path:
        """
        Guarda los resultados procesados en un archivo Excel.
        
        Args:
            df: DataFrame a guardar.
            nombre_archivo: Nombre del archivo de salida.
            directorio_salida: Directorio donde guardar. Si es None, usa el de la config.
        
        Returns:
            Path al archivo guardado.
        """
        if directorio_salida is None:
            config_data = self.config.cargar()
            directorios = config_data.get('directorios', {})
            datos_salida = directorios.get('datos_salida', 'output')
            proyecto_root = Path(__file__).parent.parent.parent
            directorio_salida = proyecto_root / datos_salida
        
        # Crear directorio si no existe
        directorio_salida.mkdir(parents=True, exist_ok=True)
        
        # Asegurar extensión .xlsx
        if not nombre_archivo.endswith('.xlsx'):
            nombre_archivo += '.xlsx'
        
        ruta_salida = directorio_salida / nombre_archivo
        
        try:
            df.to_excel(ruta_salida, index=False, engine='openpyxl')
            logger.info(f"Resultados guardados en: {ruta_salida}")
            return ruta_salida
        except Exception as e:
            logger.error(f"Error al guardar resultados: {e}")
            raise

