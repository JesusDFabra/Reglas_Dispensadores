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

# Usar el logger principal configurado en main.py
logger = logging.getLogger("arqueo_cajeros")


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


def normalizar_sobrante(valor):
    """
    Normaliza el valor de sobrante para que siempre sea negativo.
    Los sobrantes en el archivo de gestión deben tratarse como valores negativos.
    
    Args:
        valor: Valor del sobrante (puede ser positivo o negativo)
    
    Returns:
        Valor normalizado como negativo (o 0 si es 0)
    """
    valor_limpio = limpiar_valor_numerico(valor)
    if valor_limpio == 0:
        return 0.0
    # Si es positivo, convertirlo a negativo
    if valor_limpio > 0:
        return -valor_limpio
    # Si ya es negativo, retornarlo tal cual
    return valor_limpio


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
        self._movimientos_despues12: Optional[Dict[int, float]] = None  # Cache de movimientos después de 12
    
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
    
    def cargar_movimientos_despues12(self, fecha_gestion: Optional[str] = None) -> Dict[int, float]:
        """
        Carga los movimientos del archivo Trx_Despues12 para el día anterior a la gestión.
        
        El archivo Trx_Despues12_DDMM.xlsx contiene movimientos entre 0:00h y 0:05h
        que no fueron contabilizados el día anterior. Estos movimientos son siempre faltantes
        que deben ser considerados al procesar la gestión del día siguiente.
        
        Args:
            fecha_gestion: Fecha de gestión en formato DD_MM_YYYY (ej: "10_12_2025").
                          Si es None, intenta obtenerla del archivo procesado.
        
        Returns:
            Diccionario con los movimientos por cajero: {codigo_cajero: monto_total}
        """
        if self._movimientos_despues12 is not None:
            # Ya se cargó previamente, retornar cache
            return self._movimientos_despues12
        
        movimientos = {}
        
        try:
            # Determinar fecha de gestión
            if fecha_gestion is None:
                # Intentar obtener del archivo procesado
                if self._ruta_archivo_original:
                    # Extraer fecha del nombre del archivo (ej: gestion_10_12_2025_ksgarro.xlsx)
                    nombre_archivo = self._ruta_archivo_original.stem
                    partes = nombre_archivo.split('_')
                    if len(partes) >= 4 and partes[0] == 'gestion':
                        fecha_gestion = f"{partes[1]}_{partes[2]}_{partes[3]}"
            
            if fecha_gestion is None:
                logger.warning("No se pudo determinar la fecha de gestión para cargar Trx_Despues12")
                return movimientos
            
            # Parsear fecha de gestión (DD_MM_YYYY)
            try:
                dia, mes, anio = fecha_gestion.split('_')
                fecha_obj = datetime(int(anio), int(mes), int(dia))
            except (ValueError, AttributeError) as e:
                logger.warning(f"Error al parsear fecha de gestión '{fecha_gestion}': {e}")
                return movimientos
            
            # Calcular fecha del día anterior
            fecha_anterior = fecha_obj - timedelta(days=1)
            dia_anterior = fecha_anterior.day
            mes_anterior = fecha_anterior.month
            
            # Construir nombre del archivo: Trx_Despues12_DDMM.xlsx
            nombre_archivo = f"Trx_Despues12_{dia_anterior:02d}{mes_anterior:02d}.xlsx"
            
            # Construir ruta del archivo
            # La ruta correcta es insumos_excel/Trx_Despues12
            config_data = self.config.cargar()
            directorios = config_data.get('directorios', {})
            dir_insumos = directorios.get('insumos_excel', 'insumos_excel')
            ruta_archivo = Path(dir_insumos) / "Trx_Despues12" / nombre_archivo
            
            if not ruta_archivo.exists():
                logger.debug(f"Archivo Trx_Despues12 no encontrado: {ruta_archivo}")
                self._movimientos_despues12 = movimientos  # Cache vacío
                return movimientos
            
            # Leer archivo
            logger.info(f"Cargando movimientos después de 12: {ruta_archivo}")
            df = pd.read_excel(ruta_archivo)
            
            # Verificar columnas requeridas
            if 'AST_TERMINAL_ID' not in df.columns or 'TOTAL_MONTO' not in df.columns:
                logger.warning(
                    f"Archivo Trx_Despues12 no tiene las columnas requeridas. "
                    f"Columnas encontradas: {list(df.columns)}"
                )
                self._movimientos_despues12 = movimientos
                return movimientos
            
            # Agrupar movimientos por cajero y sumar montos
            for _, row in df.iterrows():
                codigo_cajero = int(row['AST_TERMINAL_ID']) if pd.notna(row['AST_TERMINAL_ID']) else None
                monto = limpiar_valor_numerico(row['TOTAL_MONTO'])
                
                if codigo_cajero is not None and monto > 0:
                    if codigo_cajero in movimientos:
                        movimientos[codigo_cajero] += monto
                    else:
                        movimientos[codigo_cajero] = monto
            
            logger.info(
                f"Movimientos después de 12 cargados: {len(movimientos)} cajeros con movimientos"
            )
            
            self._movimientos_despues12 = movimientos
            return movimientos
        
        except Exception as e:
            logger.error(f"Error al cargar movimientos después de 12: {e}", exc_info=True)
            self._movimientos_despues12 = {}  # Cache vacío en caso de error
            return movimientos
    
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
        
        # Agregar columna tipo_registro si no existe (el archivo original puede no tenerla)
        if 'tipo_registro' not in self._df_archivo_original.columns:
            self._df_archivo_original['tipo_registro'] = tipo_registro
            logger.info(f"Columna 'tipo_registro' agregada con valor '{tipo_registro}'")
        
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
            
            # Cerrar conexión a BD al finalizar el procesamiento
            if hasattr(self.consultor, '_consultor_bd') and self.consultor._consultor_bd:
                try:
                    self.consultor._consultor_bd.desconectar()
                    logger.info("Conexión a BD cerrada al finalizar procesamiento")
                except Exception as e:
                    logger.warning(f"Error al cerrar conexión BD: {e}")
        
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
            sobrante = normalizar_sobrante(row['sobrantes'])  # Los sobrantes siempre son negativos
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
                    nombre_regla_aplicada = "REGLA 1: ARQUEO y DIARIO tienen los mismos valores (todos los campos iguales)"
                    logger.info(
                        f"Cajero {codigo_cajero}: ARQUEO y DIARIO tienen los mismos valores. "
                        f"Aplicando {nombre_regla_aplicada}: CONTABILIZACION SOBRANTE FISICO"
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
                    sobrante_arqueo = normalizar_sobrante(row_arqueo.get('sobrantes', 0))  # Los sobrantes siempre son negativos
                    faltante_diario = limpiar_valor_numerico(row_diario.get('faltantes', 0))
                    sobrante_diario = normalizar_sobrante(row_diario.get('sobrantes', 0))  # Los sobrantes siempre son negativos
                    
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
    
    def _limpiar_resumen_pasos_regla_generica(self, resumen_pasos_str: str, nombre_regla_correcta: str = None) -> str:
        """
        Limpia el resumen de pasos eliminando mensajes incorrectos de REGLA GENÉRICA
        cuando se detecta que ambos registros (ARQUEO y DIARIO) existen.
        
        Args:
            resumen_pasos_str: String con el resumen de pasos a limpiar
            nombre_regla_correcta: Nombre de la regla correcta a asegurar que esté presente
        
        Returns:
            String con el resumen de pasos limpio
        """
        if not resumen_pasos_str:
            return resumen_pasos_str
        
        # Dividir por " | " para procesar cada parte del resumen
        partes = str(resumen_pasos_str).split(' | ')
        partes_limpias = []
        
        for parte in partes:
            # Eliminar mensajes de "Solo ARQUEO sin DIARIO" o "Solo DIARIO sin ARQUEO"
            # cuando sabemos que ambos registros existen
            if "REGLA GENÉRICA" in parte:
                if "Solo ARQUEO sin DIARIO" in parte or "Solo DIARIO sin ARQUEO" in parte:
                    continue  # Saltar esta parte
            partes_limpias.append(parte)
        
        # Reconstruir el resumen sin los mensajes incorrectos
        resumen_limpiado = ' | '.join(partes_limpias)
        
        # Asegurar que el mensaje de la regla correcta esté presente
        if nombre_regla_correcta and f"REGLA APLICADA: {nombre_regla_correcta}" not in resumen_limpiado:
            resumen_limpiado = f"REGLA APLICADA: {nombre_regla_correcta} | {resumen_limpiado}" if resumen_limpiado else f"REGLA APLICADA: {nombre_regla_correcta}"
        
        # Corregir referencias incorrectas
        resumen_limpiado = resumen_limpiado.replace("ARQUEO y ARQUEO", "ARQUEO y DIARIO")
        resumen_limpiado = resumen_limpiado.replace("DIARIO y DIARIO", "ARQUEO y DIARIO")
        
        return resumen_limpiado
    
    def _marcar_registro_procesado(self, indices: list, nombre_regla: str):
        """
        Marca un registro como procesado por una regla específica.
        Esto previene que el registro sea sobrescrito por reglas posteriores.
        
        Args:
            indices: Lista de índices del DataFrame a marcar
            nombre_regla: Nombre de la regla que se aplicó
        """
        if 'regla_aplicada' not in self._df_archivo_original.columns:
            self._df_archivo_original['regla_aplicada'] = None
        
        for idx in indices:
            if idx in self._df_archivo_original.index:
                self._df_archivo_original.loc[idx, 'regla_aplicada'] = nombre_regla
                logger.debug(f"Registro {idx} marcado como procesado por regla: {nombre_regla}")
    
    def _procesar_busqueda_sobrantes_faltante(
        self,
        consultor_bd,
        fecha_arqueo_registro,
        codigo_cajero: int,
        faltante: float,
        indices_original,
        row_original,
        resumen_pasos: list,
        query_params: dict,
        movimiento_sobrantes_encontrado_ref: dict
    ):
        """
        Procesa la búsqueda de sobrantes positivos en cuenta 279510020 para un faltante.
        Implementa la nueva regla completa con creación de múltiples registros cuando sea necesario.
        
        Args:
            consultor_bd: Instancia de ConsultorBD
            fecha_arqueo_registro: Fecha del arqueo como datetime
            codigo_cajero: Código del cajero
            faltante: Valor del faltante (positivo)
            indices_original: Índices del registro original en el DataFrame
            row_original: Fila original del registro
            resumen_pasos: Lista de pasos del resumen
            query_params: Parámetros de consulta
            movimiento_sobrantes_encontrado_ref: Diccionario con referencia para indicar si se encontró movimiento
        """
        import logging
        logger = logging.getLogger(__name__)
        
        if not consultor_bd or not fecha_arqueo_registro:
            # No hay consultor BD, revisión manual
            resumen_pasos.append("No hay acceso a BD")
            resumen_pasos.append("Clasificación: PENDIENTE DE GESTION")
            return
        
        # Buscar sobrantes positivos múltiples
        resultado_sobrantes = consultor_bd.consultar_sobrantes_positivos_multiples(
            codigo_cajero=codigo_cajero,
            fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
            valor_faltante=faltante,
            cuenta=279510020,
            codofi_excluir=query_params.get('codofi_excluir', 976),
            dias_anteriores=30
        )
        
        if not resultado_sobrantes:
            # No aparece: Solicitar arqueo a la sucursal
            nombre_regla_aplicada = "REGLA: Solo DIARIO - FALTANTE < $10M (No encontrado en sobrantes)"
            logger.info(
                f"Cajero {codigo_cajero}: No se encontró movimiento en cuenta de sobrantes. "
                f"Aplicando {nombre_regla_aplicada}: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal"
            )
            
            resumen_pasos.append("Buscado en cuenta sobrantes 279510020 días anteriores")
            resumen_pasos.append("✗ No encontrado")
            resumen_pasos.append("Clasificación: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal")
            
            # Actualizar registro original
            self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = 'No'
            self._df_archivo_original.loc[indices_original, 'justificacion'] = 'Pendiente de gestion'
            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = 'Pendiente gestion'
            self._df_archivo_original.loc[indices_original, 'observaciones'] = 'Se le solicita arqueo a la sucursal'
            if 'resumen_pasos' in self._df_archivo_original.columns:
                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
            
            # Marcar registro como procesado
            self._marcar_registro_procesado(indices_original, f"{nombre_regla_aplicada} - No encontrado (solicitar arqueo)")
            return
        
        movimientos = resultado_sobrantes['movimientos']
        caso = resultado_sobrantes['caso']
        suma_total = resultado_sobrantes['suma']
        
        resumen_pasos.append(f"Buscado en cuenta sobrantes 279510020 días anteriores")
        resumen_pasos.append(f"✓ Encontrados {len(movimientos)} movimiento(s) positivo(s), suma: ${suma_total:,.0f}")
        
        nombre_regla_aplicada = "REGLA: Solo DIARIO - FALTANTE < $10M encontrado en cuenta sobrantes"
        
        if caso == 'exacto':
            # Valor exacto encontrado: un solo movimiento
            movimiento = movimientos[0]
            numdoc = movimiento.get('NUMDOC')
            numdoc_str = str(int(float(numdoc))) if numdoc is not None else str(numdoc)
            
            logger.info(
                f"Cajero {codigo_cajero}: Movimiento exacto encontrado en cuenta de sobrantes "
                f"(NUMDOC: {numdoc_str}). Aplicando {nombre_regla_aplicada}: CRUCE DE NOVEDADES"
            )
            
            resumen_pasos.append(f"Movimiento exacto encontrado (NUMDOC: {numdoc_str})")
            resumen_pasos.append("Clasificación: CRUCE DE NOVEDADES - Reverso")
            
            # Actualizar registro original
            self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = 'Reverso'
            self._df_archivo_original.loc[indices_original, 'justificacion'] = 'Cruzar'
            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = 'CRUCE DE NOVEDADES'
            self._df_archivo_original.loc[indices_original, 'observaciones'] = numdoc_str
            if 'resumen_pasos' in self._df_archivo_original.columns:
                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
            
            # Marcar registro como procesado
            self._marcar_registro_procesado(indices_original, f"{nombre_regla_aplicada} - NUMDOC {numdoc_str}")
            
            movimiento_sobrantes_encontrado_ref['value'] = True
            
        elif caso == 'menor':
            # Valor menor encontrado: solo hay un movimiento y es menor
            # El método ya busca todos los positivos hasta el primer negativo
            # Si solo hay uno y es menor, significa que el siguiente es negativo o no hay más
            # Por lo tanto, se debe solicitar arqueo
            movimiento = movimientos[0]
            valor_movimiento = float(movimiento['VALOR'])
            
            logger.info(
                f"Cajero {codigo_cajero}: Movimiento menor encontrado (${valor_movimiento:,.0f} < ${faltante:,.0f}). "
                f"No hay más movimientos positivos disponibles. Aplicando {nombre_regla_aplicada}: PENDIENTE DE GESTION"
            )
            
            resumen_pasos.append(f"Movimiento menor encontrado (${valor_movimiento:,.0f} < ${faltante:,.0f})")
            resumen_pasos.append("No hay más movimientos positivos disponibles (siguiente es negativo o no existe)")
            resumen_pasos.append("Clasificación: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal")
            
            # Actualizar registro original
            self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = 'No'
            self._df_archivo_original.loc[indices_original, 'justificacion'] = 'Pendiente de gestion'
            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = 'Pendiente gestion'
            self._df_archivo_original.loc[indices_original, 'observaciones'] = 'Se le solicita arqueo a la sucursal'
            if 'resumen_pasos' in self._df_archivo_original.columns:
                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
            
            # Marcar registro como procesado
            self._marcar_registro_procesado(indices_original, f"{nombre_regla_aplicada} - Menor (solicitar arqueo)")
            
        elif caso == 'mayor':
            # Valor mayor encontrado: usar solo el primero y crear registro nuevo con diferencia
            movimiento1 = movimientos[0]
            valor1 = float(movimiento1['VALOR'])
            numdoc1 = movimiento1.get('NUMDOC')
            numdoc1_str = str(int(float(numdoc1))) if numdoc1 is not None else str(numdoc1)
            
            # El faltante se completa con el primer movimiento
            # No se crea registro nuevo porque el faltante es menor que el movimiento
            
            logger.info(
                f"Cajero {codigo_cajero}: Movimiento mayor encontrado (${valor1:,.0f} > ${faltante:,.0f}). "
                f"Aplicando {nombre_regla_aplicada}: CRUCE DE NOVEDADES"
            )
            
            resumen_pasos.append(f"Movimiento mayor encontrado (${valor1:,.0f} > ${faltante:,.0f})")
            resumen_pasos.append(f"Usando movimiento (NUMDOC: {numdoc1_str})")
            resumen_pasos.append("Clasificación: CRUCE DE NOVEDADES - Reverso")
            
            # Actualizar registro original
            self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = 'Reverso'
            self._df_archivo_original.loc[indices_original, 'justificacion'] = 'Cruzar'
            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = 'CRUCE DE NOVEDADES'
            self._df_archivo_original.loc[indices_original, 'observaciones'] = numdoc1_str
            if 'resumen_pasos' in self._df_archivo_original.columns:
                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
            
            # Marcar registro como procesado
            self._marcar_registro_procesado(indices_original, f"{nombre_regla_aplicada} - Mayor (NUMDOC {numdoc1_str})")
            
            movimiento_sobrantes_encontrado_ref['value'] = True
            
        elif caso == 'suma_igual':
            # Suma igual: crear un registro por cada movimiento encontrado
            logger.info(
                f"Cajero {codigo_cajero}: Suma de movimientos igual al faltante ({len(movimientos)} movimientos). "
                f"Creando {len(movimientos)} registro(s) adicional(es)"
            )
            
            resumen_pasos.append(f"Suma de movimientos igual al faltante ({len(movimientos)} movimientos)")
            resumen_pasos.append(f"Creando {len(movimientos)} registro(s) adicional(es)")
            
            # Actualizar registro original con el primer movimiento
            movimiento1 = movimientos[0]
            numdoc1 = movimiento1.get('NUMDOC')
            numdoc1_str = str(int(float(numdoc1))) if numdoc1 is not None else str(numdoc1)
            
            self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = 'Reverso'
            self._df_archivo_original.loc[indices_original, 'justificacion'] = 'Cruzar'
            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = 'CRUCE DE NOVEDADES'
            self._df_archivo_original.loc[indices_original, 'observaciones'] = numdoc1_str
            self._df_archivo_original.loc[indices_original, 'faltantes'] = float(movimiento1['VALOR'])
            if 'resumen_pasos' in self._df_archivo_original.columns:
                resumen_pasos_original = resumen_pasos.copy()
                resumen_pasos_original[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                resumen_pasos_original.append(f"Registro original actualizado con movimiento 1 (NUMDOC: {numdoc1_str})")
                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_original)
            
            # Marcar registro original como procesado
            self._marcar_registro_procesado(indices_original, f"{nombre_regla_aplicada} - Suma igual (movimiento 1, NUMDOC {numdoc1_str})")
            
            # Crear nuevos registros para los movimientos restantes
            nuevos_registros = []
            for i, movimiento in enumerate(movimientos[1:], start=2):
                numdoc = movimiento.get('NUMDOC')
                numdoc_str = str(int(float(numdoc))) if numdoc is not None else str(numdoc)
                
                # Crear copia del registro original
                nuevo_registro = row_original.copy()
                nuevo_registro['faltantes'] = float(movimiento['VALOR'])
                nuevo_registro['ratificar_grabar_diferencia'] = 'Reverso'
                nuevo_registro['justificacion'] = 'Cruzar'
                nuevo_registro['nuevo_estado'] = 'CRUCE DE NOVEDADES'
                nuevo_registro['observaciones'] = numdoc_str
                
                resumen_pasos_nuevo = resumen_pasos.copy()
                resumen_pasos_nuevo[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                resumen_pasos_nuevo.append(f"Registro adicional {i} creado con movimiento (NUMDOC: {numdoc_str})")
                nuevo_registro['resumen_pasos'] = ' | '.join(resumen_pasos_nuevo)
                
                # Marcar nuevo registro como procesado
                nuevo_registro['regla_aplicada'] = f"{nombre_regla_aplicada} - Suma igual (movimiento {i}, NUMDOC {numdoc_str})"
                
                nuevos_registros.append(nuevo_registro)
            
            # Agregar nuevos registros al DataFrame original
            if nuevos_registros:
                nuevos_df = pd.DataFrame(nuevos_registros)
                # Insertar después del registro original
                idx_insertar = indices_original[0] + 1 if len(indices_original) == 1 else indices_original[-1] + 1
                self._df_archivo_original = pd.concat([
                    self._df_archivo_original.iloc[:idx_insertar],
                    nuevos_df,
                    self._df_archivo_original.iloc[idx_insertar:]
                ]).reset_index(drop=True)
                
                logger.info(f"Cajero {codigo_cajero}: {len(nuevos_registros)} registro(s) adicional(es) creado(s)")
            
            movimiento_sobrantes_encontrado_ref['value'] = True
            
        elif caso == 'suma_menor':
            # Suma menor: solicitar arqueo
            logger.info(
                f"Cajero {codigo_cajero}: Suma de movimientos menor al faltante (${suma_total:,.0f} < ${faltante:,.0f}). "
                f"Aplicando {nombre_regla_aplicada}: PENDIENTE DE GESTION"
            )
            
            resumen_pasos.append(f"Suma de movimientos menor al faltante (${suma_total:,.0f} < ${faltante:,.0f})")
            resumen_pasos.append("Clasificación: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal")
            
            # Actualizar registro original
            self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = 'No'
            self._df_archivo_original.loc[indices_original, 'justificacion'] = 'Pendiente de gestion'
            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = 'Pendiente gestion'
            self._df_archivo_original.loc[indices_original, 'observaciones'] = 'Se le solicita arqueo a la sucursal'
            if 'resumen_pasos' in self._df_archivo_original.columns:
                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
            
            # Marcar registro como procesado
            self._marcar_registro_procesado(indices_original, f"{nombre_regla_aplicada} - Suma menor (solicitar arqueo)")
            
        elif caso == 'suma_mayor':
            # Suma mayor: usar los primeros movimientos que sumen el faltante
            # Calcular cuántos movimientos necesitamos
            suma_acumulada = 0.0
            movimientos_a_usar = []
            
            for movimiento in movimientos:
                valor = float(movimiento['VALOR'])
                suma_acumulada += valor
                movimientos_a_usar.append(movimiento)
                
                if suma_acumulada >= faltante:
                    break
            
            logger.info(
                f"Cajero {codigo_cajero}: Suma de movimientos mayor al faltante. "
                f"Usando {len(movimientos_a_usar)} movimiento(s) que suman ${suma_acumulada:,.0f}"
            )
            
            if len(movimientos_a_usar) == 1:
                # Solo un movimiento necesario
                movimiento1 = movimientos_a_usar[0]
                numdoc1 = movimiento1.get('NUMDOC')
                numdoc1_str = str(int(float(numdoc1))) if numdoc1 is not None else str(numdoc1)
                
                resumen_pasos.append(f"Movimiento mayor encontrado (${float(movimiento1['VALOR']):,.0f} > ${faltante:,.0f})")
                resumen_pasos.append(f"Usando movimiento (NUMDOC: {numdoc1_str})")
                resumen_pasos.append("Clasificación: CRUCE DE NOVEDADES - Reverso")
                
                # Actualizar registro original
                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = 'Reverso'
                self._df_archivo_original.loc[indices_original, 'justificacion'] = 'Cruzar'
                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = 'CRUCE DE NOVEDADES'
                self._df_archivo_original.loc[indices_original, 'observaciones'] = numdoc1_str
                if 'resumen_pasos' in self._df_archivo_original.columns:
                    resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                
                # Marcar registro como procesado
                self._marcar_registro_procesado(indices_original, f"{nombre_regla_aplicada} - Suma mayor (1 movimiento, NUMDOC {numdoc1_str})")
                
            else:
                # Múltiples movimientos necesarios
                movimiento1 = movimientos_a_usar[0]
                numdoc1 = movimiento1.get('NUMDOC')
                numdoc1_str = str(int(float(numdoc1))) if numdoc1 is not None else str(numdoc1)
                valor1 = float(movimiento1['VALOR'])
                
                # Calcular diferencia restante
                diferencia_restante = faltante - valor1
                
                resumen_pasos.append(f"Usando {len(movimientos_a_usar)} movimiento(s) para completar faltante")
                resumen_pasos.append(f"Movimiento 1: ${valor1:,.0f} (NUMDOC: {numdoc1_str})")
                
                # Actualizar registro original con el primer movimiento
                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = 'Reverso'
                self._df_archivo_original.loc[indices_original, 'justificacion'] = 'Cruzar'
                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = 'CRUCE DE NOVEDADES'
                self._df_archivo_original.loc[indices_original, 'observaciones'] = numdoc1_str
                self._df_archivo_original.loc[indices_original, 'faltantes'] = valor1
                if 'resumen_pasos' in self._df_archivo_original.columns:
                    resumen_pasos_original = resumen_pasos.copy()
                    resumen_pasos_original[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                    resumen_pasos_original.append(f"Registro original actualizado con movimiento 1 (NUMDOC: {numdoc1_str})")
                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_original)
                
                # Marcar registro original como procesado
                self._marcar_registro_procesado(indices_original, f"{nombre_regla_aplicada} - Suma mayor (movimiento 1, NUMDOC {numdoc1_str})")
                
                # Crear registro nuevo con la diferencia restante del segundo movimiento
                movimiento2 = movimientos_a_usar[1]
                numdoc2 = movimiento2.get('NUMDOC')
                numdoc2_str = str(int(float(numdoc2))) if numdoc2 is not None else str(numdoc2)
                
                nuevo_registro = row_original.copy()
                nuevo_registro['faltantes'] = diferencia_restante
                nuevo_registro['ratificar_grabar_diferencia'] = 'Reverso'
                nuevo_registro['justificacion'] = 'Cruzar'
                nuevo_registro['nuevo_estado'] = 'CRUCE DE NOVEDADES'
                nuevo_registro['observaciones'] = numdoc2_str
                
                resumen_pasos_nuevo = resumen_pasos.copy()
                resumen_pasos_nuevo[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                resumen_pasos_nuevo.append(f"Registro adicional creado con diferencia restante (${diferencia_restante:,.0f}) del movimiento 2 (NUMDOC: {numdoc2_str})")
                nuevo_registro['resumen_pasos'] = ' | '.join(resumen_pasos_nuevo)
                
                # Marcar nuevo registro como procesado
                nuevo_registro['regla_aplicada'] = f"{nombre_regla_aplicada} - Suma mayor (movimiento 2, NUMDOC {numdoc2_str})"
                
                # Insertar después del registro original
                idx_insertar = indices_original[0] + 1 if len(indices_original) == 1 else indices_original[-1] + 1
                nuevo_df = pd.DataFrame([nuevo_registro])
                self._df_archivo_original = pd.concat([
                    self._df_archivo_original.iloc[:idx_insertar],
                    nuevo_df,
                    self._df_archivo_original.iloc[idx_insertar:]
                ]).reset_index(drop=True)
                
                logger.info(f"Cajero {codigo_cajero}: 1 registro adicional creado con diferencia restante")
            
            movimiento_sobrantes_encontrado_ref['value'] = True
    
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
        # 1. Agrupar por código de sucursal (codigo_suc)
        # 2. Para cada sucursal:
        #    a. Primero: Cajeros que tienen registro de ARQUEO y DIARIO
        #    b. Segundo: Cajeros que tienen solo ARQUEO
        #    c. Tercero: Cajeros que tienen solo DIARIO
        # 3. Luego pasar a la siguiente sucursal
        
        # Identificar sucursales únicas
        if 'codigo_suc' in registros_a_actualizar.columns and 'codigo_cajero' in registros_a_actualizar.columns:
            sucursales_unicas = registros_a_actualizar['codigo_suc'].dropna().unique()
            logger.info(f"Procesando {len(sucursales_unicas)} sucursales únicas")
            
            # Crear lista ordenada de índices según prioridad (por sucursal)
            indices_ordenados = []
            total_cajeros_con_ambos = 0
            total_cajeros_solo_arqueo = 0
            total_cajeros_solo_diario = 0
            
            # Procesar cada sucursal
            for sucursal in sorted(sucursales_unicas):
                logger.info(f"\n{'='*80}")
                logger.info(f"Procesando SUCURSAL: {sucursal}")
                logger.info(f"{'='*80}")
                
                # Filtrar registros de esta sucursal
                registros_sucursal = registros_a_actualizar[registros_a_actualizar['codigo_suc'] == sucursal]
                
                # Identificar cajeros únicos en esta sucursal
                cajeros_unicos_sucursal = registros_sucursal['codigo_cajero'].dropna().unique()
                logger.info(f"Sucursal {sucursal}: {len(cajeros_unicos_sucursal)} cajeros únicos")
                
                # Clasificar cajeros según qué tipos de registro tienen
                cajeros_con_ambos = []
                cajeros_solo_arqueo = []
                cajeros_solo_diario = []
                
                for cajero in cajeros_unicos_sucursal:
                    registros_cajero = registros_sucursal[registros_sucursal['codigo_cajero'] == cajero]
                    tipos_registro = registros_cajero['tipo_registro'].unique() if 'tipo_registro' in registros_cajero.columns else []
                    
                    tiene_arqueo = 'ARQUEO' in tipos_registro
                    tiene_diario = 'DIARIO' in tipos_registro
                    
                    if tiene_arqueo and tiene_diario:
                        cajeros_con_ambos.append(cajero)
                    elif tiene_arqueo:
                        cajeros_solo_arqueo.append(cajero)
                    elif tiene_diario:
                        cajeros_solo_diario.append(cajero)
                
                logger.info(
                    f"Sucursal {sucursal}: "
                    f"{len(cajeros_con_ambos)} cajeros con ARQUEO+DIARIO, "
                    f"{len(cajeros_solo_arqueo)} cajeros solo ARQUEO, "
                    f"{len(cajeros_solo_diario)} cajeros solo DIARIO"
                )
                
                total_cajeros_con_ambos += len(cajeros_con_ambos)
                total_cajeros_solo_arqueo += len(cajeros_solo_arqueo)
                total_cajeros_solo_diario += len(cajeros_solo_diario)
                
                # 1. Primero: Cajeros con ARQUEO y DIARIO en esta sucursal
                for cajero in cajeros_con_ambos:
                    indices_cajero = registros_sucursal[registros_sucursal['codigo_cajero'] == cajero].index.tolist()
                    indices_ordenados.extend(indices_cajero)
                
                # 2. Segundo: Cajeros solo con ARQUEO en esta sucursal
                for cajero in cajeros_solo_arqueo:
                    indices_cajero = registros_sucursal[registros_sucursal['codigo_cajero'] == cajero].index.tolist()
                    indices_ordenados.extend(indices_cajero)
                
                # 3. Tercero: Cajeros solo con DIARIO en esta sucursal
                for cajero in cajeros_solo_diario:
                    indices_cajero = registros_sucursal[registros_sucursal['codigo_cajero'] == cajero].index.tolist()
                    indices_ordenados.extend(indices_cajero)
            
            # Reordenar registros_a_actualizar según la prioridad
            if len(indices_ordenados) > 0:
                registros_a_actualizar = registros_a_actualizar.loc[indices_ordenados]
                logger.info(
                    f"\n{'='*80}"
                    f"\nRESUMEN GENERAL - Registros ordenados por sucursal y prioridad:"
                    f"\nTotal sucursales: {len(sucursales_unicas)}"
                    f"\nTotal cajeros con ARQUEO+DIARIO: {total_cajeros_con_ambos}"
                    f"\nTotal cajeros solo ARQUEO: {total_cajeros_solo_arqueo}"
                    f"\nTotal cajeros solo DIARIO: {total_cajeros_solo_diario}"
                    f"\n{'='*80}\n"
                )
            else:
                logger.warning("No se pudo ordenar por prioridad, procesando en orden original")
        elif 'codigo_cajero' in registros_a_actualizar.columns:
            # Si no hay codigo_suc pero sí codigo_cajero, usar la lógica anterior
            logger.warning("No se encontró columna 'codigo_suc', ordenando solo por cajero")
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
                    f"Registros ordenados por prioridad (sin agrupar por sucursal): "
                    f"{len(cajeros_con_ambos)} cajeros con ARQUEO+DIARIO, "
                    f"{len(cajeros_solo_arqueo)} cajeros solo ARQUEO, "
                    f"{len(cajeros_solo_diario)} cajeros solo DIARIO"
                )
            else:
                logger.warning("No se pudo ordenar por prioridad, procesando en orden original")
        else:
            logger.warning("No se encontró columna 'codigo_cajero' ni 'codigo_suc', procesando en orden original")
        
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
        
        # Crear columna para indicar si el registro ya fue procesado por una regla
        if 'regla_aplicada' not in self._df_archivo_original.columns:
            self._df_archivo_original['regla_aplicada'] = None
        
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
        
        # IMPORTANTE: Convertir registros_a_actualizar a lista de diccionarios para evitar problemas con índices
        # cuando se insertan nuevos registros durante la iteración
        registros_lista = registros_a_actualizar.to_dict('records')
        indices_originales_lista = registros_a_actualizar.index.tolist()
        
        for i, (idx_original, row_original) in enumerate(zip(indices_originales_lista, registros_lista)):
            # Buscar el registro actual en el DataFrame original usando una clave única
            # Esto es necesario porque los índices pueden cambiar cuando se insertan nuevos registros
            filtro_busqueda = pd.Series([True] * len(self._df_archivo_original))
            registro_encontrado = False
            idx_actual = None
            
            # Intentar encontrar el registro usando las columnas clave
            for col_clave in columnas_clave:
                if col_clave in row_original and col_clave in self._df_archivo_original.columns:
                    valor = row_original[col_clave]
                    filtro_busqueda = filtro_busqueda & (self._df_archivo_original[col_clave] == valor)
            
            registros_encontrados = self._df_archivo_original[filtro_busqueda]
            if len(registros_encontrados) > 0:
                # Si hay múltiples, usar el primero que coincida con el índice original si aún existe
                if idx_original in registros_encontrados.index:
                    idx_actual = idx_original
                else:
                    idx_actual = registros_encontrados.index[0]
                registro_encontrado = True
            
            # Si no se encontró con las claves, intentar usar el índice original si aún existe
            if not registro_encontrado and idx_original in self._df_archivo_original.index:
                idx_actual = idx_original
                registro_encontrado = True
            
            if not registro_encontrado:
                logger.warning(f"Registro con índice original {idx_original} no encontrado en DataFrame. Puede haber sido eliminado o movido.")
                continue
            
            # Obtener el registro actualizado del DataFrame original
            row_original_actual = self._df_archivo_original.loc[idx_actual]
            
            # Usar directamente el registro del archivo original
            # Determinar si es sobrante o faltante
            sobrante = normalizar_sobrante(row_original_actual['sobrantes'])  # Los sobrantes siempre son negativos
            faltante = limpiar_valor_numerico(row_original_actual['faltantes'])
            
            # Intentar obtener información de movimiento desde df_procesado si existe
            movimiento_encontrado = False
            movimiento_fuente = None
            movimiento_detalle = None
            
            # Buscar en df_procesado si el registro fue procesado
            if not df_procesado.empty:
                # Construir filtro para encontrar el registro en df_procesado
                filtro_procesado = pd.Series([True] * len(df_procesado))
                for col_clave in columnas_clave:
                    if col_clave in row_original_actual.index and col_clave in df_procesado.columns:
                        valor = row_original_actual[col_clave]
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
            
            # Procesar el registro directamente usando el índice actual del archivo original
            indices_original = [idx_actual]
            
            logger.debug(f"Procesando registro: idx_original={idx_original}, idx_actual={idx_actual}, cajero={row_original_actual.get('codigo_cajero')}, tipo={row_original_actual.get('tipo_registro')}")
            
            if idx_actual in self._df_archivo_original.index:
                
                # VERIFICACIÓN PRIORITARIA: Si el registro ya tiene una regla aplicada, NO procesarlo
                regla_aplicada_actual = self._df_archivo_original.loc[idx_actual, 'regla_aplicada'] if 'regla_aplicada' in self._df_archivo_original.columns else None
                if pd.notna(regla_aplicada_actual) and str(regla_aplicada_actual).strip():
                    logger.info(
                        f"Registro {idx_actual} (cajero {row_original_actual.get('codigo_cajero')}): "
                        f"Ya procesado por regla '{regla_aplicada_actual}'. Saltando procesamiento adicional."
                    )
                    continue
                
                # VERIFICACIÓN PRIORITARIA: Si el registro ya fue procesado con alguna regla específica,
                # NO hacer más validaciones y saltar este registro
                # IMPORTANTE: Convertir a string y limpiar espacios para comparación robusta
                justificacion_actual = str(self._df_archivo_original.loc[idx_actual, 'justificacion']).strip() if 'justificacion' in self._df_archivo_original.columns and pd.notna(self._df_archivo_original.loc[idx_actual, 'justificacion']) else None
                nuevo_estado_actual = str(self._df_archivo_original.loc[idx_actual, 'nuevo_estado']).strip() if 'nuevo_estado' in self._df_archivo_original.columns and pd.notna(self._df_archivo_original.loc[idx_actual, 'nuevo_estado']) else None
                observaciones_actual = str(self._df_archivo_original.loc[idx_actual, 'observaciones']).strip() if 'observaciones' in self._df_archivo_original.columns and pd.notna(self._df_archivo_original.loc[idx_actual, 'observaciones']) else None
                
                # Verificar si el registro ya tiene la clasificación de Trx_Despues12
                if observaciones_actual == 'INCIDENTES O EVENTOS MASIVOS':
                    # El registro ya fue procesado con la regla de Trx_Despues12, asegurar que todos los campos sean correctos y saltar
                    logger.info(
                        f"Registro {idx_actual} (cajero {row_original_actual.get('codigo_cajero')}): Ya procesado con regla Trx_Despues12 (INCIDENTES O EVENTOS MASIVOS). "
                        f"Asegurando valores correctos y saltando procesamiento adicional."
                    )
                    # Asegurar que todos los campos sean correctos
                    self._df_archivo_original.loc[idx_actual, 'justificacion'] = 'Pendiente de gestion'
                    self._df_archivo_original.loc[idx_actual, 'nuevo_estado'] = 'INCIDENTES O EVENTOS MASIVOS'
                    self._df_archivo_original.loc[idx_actual, 'ratificar_grabar_diferencia'] = 'No'
                    self._df_archivo_original.loc[idx_actual, 'observaciones'] = 'INCIDENTES O EVENTOS MASIVOS'
                    self._marcar_registro_procesado([idx_actual], 'Trx_Despues12 - INCIDENTES O EVENTOS MASIVOS')
                    continue
                # Verificar si el registro ya tiene la clasificación de PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal
                elif (observaciones_actual == 'Se le solicita arqueo a la sucursal' or 
                      observaciones_actual == 'Se le solicita arqueo a la sucursal nuevamente'):
                    # El registro ya fue procesado con la regla de PENDIENTE DE GESTION, asegurar que todos los campos sean correctos y saltar
                    logger.info(
                        f"Registro {idx_actual} (cajero {row_original_actual.get('codigo_cajero')}): Ya procesado con regla PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal. "
                        f"Asegurando valores correctos y saltando procesamiento adicional."
                    )
                    # Asegurar que los valores sean correctos según el tipo de observaciones
                    if observaciones_actual == 'Se le solicita arqueo a la sucursal':
                        self._df_archivo_original.loc[idx_actual, 'justificacion'] = 'Pendiente de gestion'
                        self._df_archivo_original.loc[idx_actual, 'nuevo_estado'] = 'PENDIENTE DE GESTION'
                    else:  # 'Se le solicita arqueo a la sucursal nuevamente'
                        self._df_archivo_original.loc[idx_actual, 'justificacion'] = 'Pendiente de gestion'
                        self._df_archivo_original.loc[idx_actual, 'nuevo_estado'] = 'PENDIENTE DE GESTION'
                    self._df_archivo_original.loc[idx_actual, 'ratificar_grabar_diferencia'] = 'No'
                    self._df_archivo_original.loc[idx_actual, 'observaciones'] = observaciones_actual
                    self._marcar_registro_procesado([idx_actual], 'PENDIENTE DE GESTION - Solicitar arqueo')
                    continue
                # Verificar si el registro ya tiene la clasificación de CONTABILIZACION SOBRANTE CONTABLE
                elif observaciones_actual == 'CONTABILIZACION SOBRANTE CONTABLE':
                    # El registro ya fue procesado con la regla de CONTABILIZACION SOBRANTE CONTABLE, asegurar que todos los campos sean correctos y saltar
                    logger.info(
                        f"Registro {idx_actual} (cajero {row_original_actual.get('codigo_cajero')}): Ya procesado con regla CONTABILIZACION SOBRANTE CONTABLE. "
                        f"Asegurando valores correctos y saltando procesamiento adicional."
                    )
                    # Asegurar que todos los campos sean correctos
                    self._df_archivo_original.loc[idx_actual, 'justificacion'] = 'Contable'
                    self._df_archivo_original.loc[idx_actual, 'nuevo_estado'] = 'CONTABILIZACION SOBRANTE CONTABLE'
                    self._df_archivo_original.loc[idx_actual, 'ratificar_grabar_diferencia'] = 'Si'
                    self._df_archivo_original.loc[idx_actual, 'observaciones'] = 'CONTABILIZACION SOBRANTE CONTABLE'
                    self._marcar_registro_procesado([idx_actual], 'CONTABILIZACION SOBRANTE CONTABLE')
                    continue
                # Verificar si el registro ya tiene la clasificación de CRUCE DE NOVEDADES (observaciones es un NUMDOC YYYYMMDD)
                # Puede venir como string, int o float (ej: 20251112.0)
                elif observaciones_actual:
                    observaciones_str = str(observaciones_actual).strip().replace('.0', '')
                    if observaciones_str.isdigit() and len(observaciones_str) == 8:
                        # El registro ya fue procesado con la regla de CRUCE DE NOVEDADES, asegurar que todos los campos sean correctos y saltar
                        logger.info(
                            f"Registro {idx_actual} (cajero {row_original_actual.get('codigo_cajero')}): Ya procesado con regla CRUCE DE NOVEDADES (NUMDOC: {observaciones_str}). "
                            f"Asegurando valores correctos y saltando procesamiento adicional."
                        )
                        # Asegurar que todos los campos sean correctos
                        self._df_archivo_original.loc[idx_actual, 'justificacion'] = 'Cruzar'
                        self._df_archivo_original.loc[idx_actual, 'nuevo_estado'] = 'CRUCE DE NOVEDADES'
                        self._df_archivo_original.loc[idx_actual, 'ratificar_grabar_diferencia'] = 'Reverso'
                        self._df_archivo_original.loc[idx_actual, 'observaciones'] = observaciones_str
                        self._marcar_registro_procesado([idx_actual], f'CRUCE DE NOVEDADES - NUMDOC {observaciones_str}')
                        continue
                
                # Obtener código de cajero
                codigo_cajero = row_original_actual.get('codigo_cajero')
                
                # Obtener fecha de arqueo del registro (del archivo original)
                fecha_arqueo_registro = None
                primera_fila_original = row_original_actual
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
                nombre_regla_aplicada = None  # Nombre de la regla que se está aplicando
                resumen_pasos = []  # Lista para almacenar los pasos seguidos
                
                # Inicializar variables de reglas (todas deben estar inicializadas)
                regla_arqueo_sin_diario = False
                regla_diario_sin_arqueo = False
                regla_arqueo_diario_igual_faltante = False
                regla_arqueo_diario_igual_sobrante = False
                regla_arqueo_diario_diferente_faltante = False
                regla_arqueo_diario_diferente_sobrante = False
                regla_diferencias_opuestas = False
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
                    ambos_son_sobrantes = False
                    ambos_son_faltantes = False
                    if len(registros_diario_mismo_cajero) > 0:
                        registro_diario = registros_diario_mismo_cajero.iloc[0]
                        faltante_diario = limpiar_valor_numerico(registro_diario.get('faltantes', 0))
                        sobrante_diario = normalizar_sobrante(registro_diario.get('sobrantes', 0))  # Los sobrantes siempre son negativos
                        diferencia_diario = faltante_diario if faltante_diario > 0 else (abs(sobrante_diario) if sobrante_diario < 0 else 0)
                        diferencia_arqueo = faltante if faltante > 0 else (abs(sobrante) if sobrante < 0 else 0)
                        logger.info(
                            f"Cajero {codigo_cajero}: Comparando diferencias - "
                            f"ARQUEO: {diferencia_arqueo}, DIARIO: {diferencia_diario}"
                        )
                        
                        # Verificar si ambos son sobrantes o ambos son faltantes
                        if sobrante < 0 and sobrante_diario < 0:
                            # Ambos son sobrantes
                            ambos_son_sobrantes = True
                            if abs(sobrante - sobrante_diario) < 0.01:
                                tiene_diario_misma_diferencia = True
                                logger.info(
                                    f"Cajero {codigo_cajero}: ¡Misma diferencia detectada (SOBRANTE)! "
                                    f"ARQUEO={abs(sobrante):,.0f}, DIARIO={abs(sobrante_diario):,.0f}"
                                )
                                
                                if codigo_cajero == 2042:
                                    logger.info(
                                        f"DEBUG Cajero 2042: ✓ Ambos son sobrantes con misma diferencia detectado en verificación inicial"
                                    )
                        elif faltante > 0 and faltante_diario > 0:
                            # Ambos son faltantes
                            ambos_son_faltantes = True
                            if abs(faltante - faltante_diario) < 0.01:
                                tiene_diario_misma_diferencia = True
                                logger.info(
                                    f"Cajero {codigo_cajero}: ¡Misma diferencia detectada (FALTANTE)! "
                                    f"ARQUEO={faltante:,.0f}, DIARIO={faltante_diario:,.0f}"
                                )
                        elif diferencia_arqueo > 0 and diferencia_diario > 0:
                            # Comparación genérica (por si acaso)
                            if abs(diferencia_arqueo - diferencia_diario) < 0.01:
                                tiene_diario_misma_diferencia = True
                                logger.info(
                                    f"Cajero {codigo_cajero}: ¡Misma diferencia detectada (genérica)! "
                                    f"ARQUEO={diferencia_arqueo:,.0f}, DIARIO={diferencia_diario:,.0f}"
                                )
                    
                    if len(registros_diario_mismo_cajero) == 0:
                        # NO hay registro DIARIO, aplicar regla
                        logger.info(
                            f"Cajero {codigo_cajero}: Solo llega registro ARQUEO sin DIARIO. "
                            f"Aplicando regla específica para ARQUEO sin DIARIO"
                        )
                    elif tiene_diario_misma_diferencia and ambos_son_sobrantes:
                        # Si hay DIARIO con la misma diferencia y ambos son SOBRANTES, NO aplicar regla "ARQUEO sin DIARIO"
                        # La regla específica de SOBRANTE se aplicará más abajo
                        logger.info(
                            f"Cajero {codigo_cajero}: ARQUEO tiene DIARIO con la misma diferencia (SOBRANTE). "
                            f"Saltando regla 'ARQUEO sin DIARIO' para aplicar regla específica de SOBRANTE más abajo."
                        )
                        # No hacer nada más aquí, la regla específica de SOBRANTE se aplicará más abajo
                        # IMPORTANTE: No establecer regla_arqueo_sin_diario = True para que la regla específica se pueda aplicar
                    elif tiene_diario_misma_diferencia and ambos_son_faltantes:
                        # Si hay DIARIO con la misma diferencia y ambos son FALTANTES, NO aplicar regla "ARQUEO sin DIARIO"
                        # La regla específica de FALTANTE se aplicará más abajo
                        logger.info(
                            f"Cajero {codigo_cajero}: ARQUEO tiene DIARIO con la misma diferencia (FALTANTE). "
                            f"Saltando regla 'ARQUEO sin DIARIO' para aplicar regla específica de FALTANTE más abajo."
                        )
                        # No hacer nada más aquí, la regla específica de FALTANTE se aplicará más abajo
                    elif tiene_diario_misma_diferencia:
                        # Si hay DIARIO con la misma diferencia (genérica), NO aplicar regla "ARQUEO sin DIARIO"
                        logger.info(
                            f"Cajero {codigo_cajero}: ARQUEO tiene DIARIO con la misma diferencia (genérica). "
                            f"Saltando regla 'ARQUEO sin DIARIO' para aplicar regla de misma diferencia más abajo."
                        )
                        # No hacer nada más aquí, la regla de misma diferencia se aplicará más abajo
                    else:
                        # Hay DIARIO pero con diferente diferencia (o diferente tipo: faltante vs sobrante)
                        # NO aplicar regla "ARQUEO sin DIARIO" porque SÍ hay DIARIO
                        # Las reglas de "diferentes diferencias" se aplicarán más abajo
                        logger.info(
                            f"Cajero {codigo_cajero}: ARQUEO tiene DIARIO pero con diferente diferencia. "
                            f"Saltando regla 'ARQUEO sin DIARIO' porque SÍ hay DIARIO. "
                            f"Las reglas de diferentes diferencias se aplicarán más abajo."
                        )
                    
                    # SOLO aplicar regla "ARQUEO sin DIARIO" si realmente NO hay DIARIO
                    if len(registros_diario_mismo_cajero) == 0:
                        
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
                                    
                                    nombre_regla_aplicada = "REGLA 4: Solo llega ARQUEO, no llega DIARIO"
                                    if not resumen_pasos:
                                        resumen_pasos = [f"REGLA APLICADA: {nombre_regla_aplicada}"]
                                    else:
                                        resumen_pasos.insert(0, f"REGLA APLICADA: {nombre_regla_aplicada}")
                                    resumen_pasos.append(f"1. Verificado: Solo llega ARQUEO, no llega DIARIO")
                                    resumen_pasos.append(f"2. Tipo: FALTANTE (${faltante:,.0f})")
                                    
                                    logger.info(
                                        f"Cajero {codigo_cajero}: Aplicando {nombre_regla_aplicada}. "
                                        f"Faltante: ${faltante:,.0f}"
                                    )
                                    
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
                                        observaciones = 'Cajero cuadrado con arqueo de la sucursal'
                                        resumen_pasos.append(f"3. Buscado en NACIONAL con NROCMP 770500, CRÉDITO (SOLO DÍA DEL ARQUEO)")
                                        resumen_pasos.append("4. ✓ Movimiento encontrado en NACIONAL (día del arqueo)")
                                        resumen_pasos.append("5. Clasificación: PENDIENTE DE GESTION - Cajero cuadrado con arqueo de la sucursal")
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
                                                    f"Aplicando regla: CRUCE DE NOVEDADES"
                                                )
                                                
                                                regla_arqueo_sin_diario = True
                                                justificacion = 'Cruzar'
                                                nuevo_estado = 'CRUCE DE NOVEDADES'
                                                ratificar_grabar = 'Reverso'
                                                observaciones = 'CRUCE DE NOVEDADES'
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
                                            justificacion = 'Pendiente de gestion'
                                            nuevo_estado = 'PENDIENTE DE GESTION'
                                            ratificar_grabar = 'No'
                                            observaciones = 'Se le solicita arqueo a la sucursal nuevamente'
                                            resumen_pasos.append(f"5. Consultado histórico: arqueo_fisico/saldo_contadores = ${arqueo_fisico:,.0f} (NO está en 0)")
                                            resumen_pasos.append("6. Clasificación: Pendiente de gestion - Solicitar arqueo nuevamente")
                                
                                elif sobrante < 0:
                                    # CASO SOBRANTE: Los sobrantes aparecen negativos en el archivo
                                    # PASO 1: Verificar si hay provisión con comprobante 810291 el día del arqueo
                                    # Si hay provisión, ajustar el sobrante restando el valor de la provisión
                                    logger.info(
                                        f"Cajero {codigo_cajero}: ARQUEO sin DIARIO con SOBRANTE ({sobrante}). "
                                        f"Verificando si hay provisión con comprobante 810291 (día del arqueo)..."
                                    )
                                    
                                    valor_sobrante_abs = abs(sobrante)
                                    resumen_pasos.append(f"1. Verificado: Solo llega ARQUEO, no llega DIARIO")
                                    resumen_pasos.append(f"2. Tipo: SOBRANTE (${valor_sobrante_abs:,.0f})")
                                    
                                    # PASO 1: Consultar movimientos (positivos y negativos) con comprobantes 770500 y 810291 (día del arqueo)
                                    # Similar a la lógica del cajero 4376, buscamos todos los movimientos, no solo por valor exacto
                                    provision_encontrada = None
                                    movimientos_nacional = consultor_bd.consultar_movimientos_negativos_mismo_dia(
                                        codigo_cajero=codigo_cajero,
                                        fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                        cuenta=query_params.get('cuenta', 110505075),
                                        codofi_excluir=query_params.get('codofi_excluir', 976),
                                        nrocmps=[770500, 810291]  # Buscar ambos comprobantes
                                    )
                                    
                                    sobrante_ajustado = sobrante  # Inicialmente sin ajustar
                                    valor_sobrante_ajustado_abs = valor_sobrante_abs
                                    suma_positivos = 0
                                    suma_negativos = 0
                                    
                                    if movimientos_nacional and movimientos_nacional.get('encontrado', False):
                                        # Se encontraron movimientos - usar los positivos (provisiones) para ajustar
                                        suma_positivos = movimientos_nacional.get('suma_positivos', 0)  # Provisiones (positivas)
                                        suma_negativos = movimientos_nacional.get('suma_negativos', 0)  # Ya viene en valor absoluto
                                        
                                        # VERIFICACIÓN: Solo usar provisiones si están relacionadas con el descuadre
                                        # Una provisión está relacionada si el valor es similar al sobrante (dentro de un 20% de diferencia)
                                        usar_provisiones_sobrante = False
                                        diferencia_porcentual = None
                                        if suma_positivos > 0:
                                            # Calcular diferencia porcentual entre provisión y sobrante
                                            diferencia_porcentual = abs(suma_positivos - valor_sobrante_abs) / max(valor_sobrante_abs, 1) * 100
                                            
                                            if diferencia_porcentual <= 20:
                                                usar_provisiones_sobrante = True
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Provisión (${suma_positivos:,.0f}) está relacionada con el descuadre. "
                                                    f"Diferencia porcentual: {diferencia_porcentual:.1f}% (similar al sobrante: ${valor_sobrante_abs:,.0f})"
                                                )
                                            else:
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Provisión (${suma_positivos:,.0f}) NO está relacionada con el descuadre. "
                                                    f"Sobrante: ${valor_sobrante_abs:,.0f}, Diferencia porcentual: {diferencia_porcentual:.1f}%. "
                                                    f"No se usará para ajustar el sobrante."
                                                )
                                        
                                        # Ajustar el sobrante sumando las provisiones (porque sobrante es negativo)
                                        # Las provisiones reducen el sobrante, así que las sumamos
                                        # sobrante ya es negativo (normalizado), sumar positivos lo hace menos negativo
                                        # Solo usar provisiones si están relacionadas con el descuadre
                                        suma_positivos_ajuste = suma_positivos if usar_provisiones_sobrante else 0
                                        sobrante_ajustado = sobrante + suma_positivos_ajuste
                                        # Asegurar que el sobrante ajustado siga siendo negativo (o 0)
                                        # Si resulta positivo (provisión mayor que sobrante), convertirlo a negativo
                                        if sobrante_ajustado > 0:
                                            sobrante_ajustado = -sobrante_ajustado
                                        elif sobrante_ajustado == 0:
                                            sobrante_ajustado = 0.0
                                        valor_sobrante_ajustado_abs = abs(sobrante_ajustado)
                                        
                                        logger.info(
                                            f"Cajero {codigo_cajero}: Movimientos encontrados en NACIONAL. "
                                            f"Negativos: ${suma_negativos:,.0f}, Positivos (provisiones): ${suma_positivos:,.0f}, "
                                            f"Provisión usada para ajustar: {'Sí' if usar_provisiones_sobrante else 'No'}, "
                                            f"Sobrante original: ${valor_sobrante_abs:,.0f}, Sobrante ajustado: ${valor_sobrante_ajustado_abs:,.0f}"
                                        )
                                        
                                        resumen_pasos.append(f"3. Buscado movimientos en NACIONAL cuenta 110505075 (comprobantes 770500 y 810291)")
                                        if suma_positivos > 0 and suma_negativos > 0:
                                            resumen_pasos.append(f"4. ✓ Movimientos encontrados: Negativos = ${suma_negativos:,.0f}, Positivos (provisiones) = ${suma_positivos:,.0f}")
                                            if not usar_provisiones_sobrante and diferencia_porcentual is not None:
                                                resumen_pasos.append(f"   ⚠ Provisión NO relacionada con descuadre (diferencia: {diferencia_porcentual:.1f}%), no se usará para ajustar")
                                        elif suma_positivos > 0:
                                            resumen_pasos.append(f"4. ✓ Movimientos encontrados: Positivos (provisiones) = ${suma_positivos:,.0f}")
                                            if not usar_provisiones_sobrante and diferencia_porcentual is not None:
                                                resumen_pasos.append(f"   ⚠ Provisión NO relacionada con descuadre (diferencia: {diferencia_porcentual:.1f}%), no se usará para ajustar")
                                        else:
                                            resumen_pasos.append(f"4. ✓ Movimientos encontrados: Negativos = ${suma_negativos:,.0f}")
                                        
                                        if usar_provisiones_sobrante:
                                            resumen_pasos.append(f"5. Sobrante ajustado: ${valor_sobrante_abs:,.0f} + ${suma_positivos:,.0f} = ${valor_sobrante_ajustado_abs:,.0f}")
                                        else:
                                            resumen_pasos.append(f"5. Sobrante NO ajustado (provisión no relacionada): ${valor_sobrante_abs:,.0f}")
                                        
                                        # Actualizar el archivo de gestión con el sobrante ajustado solo si se usó la provisión
                                        if usar_provisiones_sobrante:
                                            self._df_archivo_original.loc[indices_original, 'sobrantes'] = sobrante_ajustado
                                        
                                        provision_encontrada = True if suma_positivos > 0 and usar_provisiones_sobrante else False
                                    else:
                                        logger.info(
                                            f"Cajero {codigo_cajero}: No se encontraron movimientos en NACIONAL (comprobantes 770500 y 810291). "
                                            f"Usando sobrante original: ${valor_sobrante_abs:,.0f}"
                                        )
                                        
                                        resumen_pasos.append(f"3. Buscado movimientos en NACIONAL cuenta 110505075 (comprobantes 770500 y 810291)")
                                        resumen_pasos.append(f"4. ✗ No se encontraron movimientos")
                                        provision_encontrada = False
                                    
                                    # PASO 2: Con el sobrante ajustado, buscar en cuenta de faltantes
                                    logger.info(
                                        f"Cajero {codigo_cajero}: Buscando en cuenta de faltantes con sobrante ajustado (${valor_sobrante_ajustado_abs:,.0f})..."
                                    )
                                    
                                    movimiento_faltantes = consultor_bd.consultar_cuenta_faltantes_dias_anteriores(
                                        codigo_cajero=codigo_cajero,
                                        fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                        valor_descuadre=sobrante_ajustado,  # Usar sobrante ajustado (negativo)
                                        cuenta=168710093,
                                        codofi_excluir=query_params.get('codofi_excluir', 976),
                                        dias_anteriores=30
                                    )
                                    
                                    if movimiento_faltantes:
                                        # Aparece en cuenta de faltantes
                                        logger.info(
                                            f"Cajero {codigo_cajero}: Movimiento encontrado en cuenta de faltantes. "
                                            f"Aplicando regla: CRUCE DE NOVEDADES"
                                        )
                                        
                                        regla_arqueo_sin_diario = True
                                        justificacion = 'Cruzar'
                                        nuevo_estado = 'CRUCE DE NOVEDADES'
                                        ratificar_grabar = 'Reverso'
                                        observaciones = 'CRUCE DE NOVEDADES'
                                        resumen_pasos.append("6. Buscado en cuenta de faltantes 168710093 (últimos 30 días)")
                                        resumen_pasos.append("7. ✓ Movimiento encontrado en cuenta de faltantes")
                                        resumen_pasos.append("8. Clasificación: CRUCE DE NOVEDADES - Reverso")
                                    else:
                                        # NO aparece en cuenta de faltantes - Contabilizar como SOBRANTE FISICO
                                        logger.info(
                                            f"Cajero {codigo_cajero}: No se encontró movimiento en cuenta de faltantes. "
                                            f"Aplicando regla: CONTABILIZACION SOBRANTE FISICO"
                                        )
                                        
                                        regla_arqueo_sin_diario = True
                                        justificacion = 'Fisico'
                                        nuevo_estado = 'Contabilizacion sobrante fisico'
                                        ratificar_grabar = 'Si'
                                        observaciones = 'Contabilizacion sobrante fisico'
                                        resumen_pasos.append("6. Buscado en cuenta de faltantes 168710093 (últimos 30 días)")
                                        resumen_pasos.append("7. ✗ No encontrado en cuenta de faltantes")
                                        resumen_pasos.append(f"8. Clasificación: CONTABILIZACION SOBRANTE FISICO - Ratificar grabar (sobrante ajustado: ${valor_sobrante_ajustado_abs:,.0f})")
                            
                            except Exception as e:
                                logger.warning(f"Error al aplicar regla ARQUEO sin DIARIO: {e}", exc_info=True)
                        else:
                            # No hay fecha_arqueo_registro o consultor_bd, aplicar revisión manual
                            logger.warning(
                                f"Cajero {codigo_cajero}: ARQUEO sin DIARIO pero falta fecha_arqueo_registro o consultor_bd. "
                                f"Aplicando revisión manual"
                            )
                            regla_arqueo_sin_diario = True
                            justificacion = 'Pendiente de gestion'
                            nuevo_estado = 'PENDIENTE DE GESTION'
                            ratificar_grabar = 'No'
                            observaciones = 'Este caso requiere la supervisión de personal encargado.'
                            resumen_pasos.append("1. Verificado: Solo llega ARQUEO, no llega DIARIO")
                            resumen_pasos.append("2. Error: Falta fecha_arqueo_registro o consultor_bd")
                            resumen_pasos.append("3. Clasificación: PENDIENTE DE GESTION")
                            
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
                    # Verificar que el registro no haya sido procesado ya
                    regla_aplicada_actual = None
                    if len(indices_original) > 0 and 'regla_aplicada' in self._df_archivo_original.columns:
                        regla_aplicada_actual = self._df_archivo_original.loc[indices_original[0], 'regla_aplicada']
                    
                    if pd.notna(regla_aplicada_actual) and str(regla_aplicada_actual).strip():
                        logger.info(
                            f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya procesado por regla '{regla_aplicada_actual}'. "
                            f"No se sobrescribirán los valores."
                        )
                        continue
                    
                    # Actualizar el registro con la clasificación determinada
                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                    self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                    self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                    if 'observaciones' in self._df_archivo_original.columns and observaciones:
                        self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                    if 'resumen_pasos' in self._df_archivo_original.columns and resumen_pasos:
                        self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                    
                    # Marcar registro como procesado
                    if nombre_regla_aplicada:
                        self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                    elif justificacion and nuevo_estado:
                        regla_desc = f"ARQUEO sin DIARIO - {nuevo_estado}"
                        self._marcar_registro_procesado(indices_original, regla_desc)
                    
                    actualizados += len(indices_original)
                    
                    # Log del resultado para todos los registros procesados
                    logger.info(
                        f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                        f"justificacion='{justificacion}', nuevo_estado='{nuevo_estado}', "
                        f"ratificar_grabar='{ratificar_grabar}'"
                    )
                
                # Si ya se aplicó la regla de ARQUEO sin DIARIO, saltar las otras reglas
                if not regla_arqueo_sin_diario and not regla_diario_sin_arqueo and not regla_diferencias_opuestas:
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
                            
                            # VERIFICACIÓN PRIORITARIA: Si el registro ya tiene una clasificación válida en el archivo original, NO procesar nuevamente
                            # Usar el DataFrame actualizado (self._df_archivo_original) en lugar de row_original
                            if idx_original in self._df_archivo_original.index:
                                observaciones_actual = self._df_archivo_original.loc[idx_actual, 'observaciones'] if 'observaciones' in self._df_archivo_original.columns else None
                                justificacion_actual = self._df_archivo_original.loc[idx_actual, 'justificacion'] if 'justificacion' in self._df_archivo_original.columns else None
                                nuevo_estado_actual = self._df_archivo_original.loc[idx_actual, 'nuevo_estado'] if 'nuevo_estado' in self._df_archivo_original.columns else None
                                
                                # Verificar si ya tiene clasificación de CRUCE DE NOVEDADES (observaciones es un NUMDOC YYYYMMDD)
                                if (observaciones_actual and 
                                    str(observaciones_actual).isdigit() and 
                                    len(str(observaciones_actual)) == 8 and
                                    justificacion_actual in ['Cruzar', 'Cruzar'] and
                                    nuevo_estado_actual == 'CRUCE DE NOVEDADES'):
                                    logger.info(
                                        f"Cajero {codigo_cajero}: Ya tiene clasificación CRUCE DE NOVEDADES (NUMDOC: {observaciones_actual}). "
                                        f"Saltando procesamiento para evitar sobrescritura."
                                    )
                                    regla_diario_sin_arqueo = True
                                    continue
                                
                                # Verificar si ya tiene clasificación de CONTABILIZACION SOBRANTE CONTABLE
                                if (observaciones_actual == 'CONTABILIZACION SOBRANTE CONTABLE' and
                                    justificacion_actual == 'Contable' and
                                    nuevo_estado_actual == 'CONTABILIZACION SOBRANTE CONTABLE'):
                                    logger.info(
                                        f"Cajero {codigo_cajero}: Ya tiene clasificación CONTABILIZACION SOBRANTE CONTABLE. "
                                        f"Saltando procesamiento para evitar sobrescritura."
                                    )
                                    regla_diario_sin_arqueo = True
                                    continue
                                
                                # Verificar si ya tiene clasificación de INCIDENTES O EVENTOS MASIVOS
                                if (observaciones_actual == 'INCIDENTES O EVENTOS MASIVOS' and
                                    justificacion_actual == 'Pendiente de gestion' and
                                    nuevo_estado_actual == 'INCIDENTES O EVENTOS MASIVOS'):
                                    logger.info(
                                        f"Cajero {codigo_cajero}: Ya tiene clasificación INCIDENTES O EVENTOS MASIVOS. "
                                        f"Saltando procesamiento para evitar sobrescritura."
                                    )
                                    regla_diario_sin_arqueo = True
                                    continue
                                
                                # Verificar si ya tiene clasificación de PENDIENTE DE GESTION
                                if ((observaciones_actual == 'Se le solicita arqueo a la sucursal' or 
                                     observaciones_actual == 'Se le solicita arqueo a la sucursal nuevamente' or
                                     observaciones_actual == 'Revisar el Diario día siguiente') and
                                    justificacion_actual == 'Pendiente de gestion' and
                                    nuevo_estado_actual == 'PENDIENTE DE GESTION'):
                                    logger.info(
                                        f"Cajero {codigo_cajero}: Ya tiene clasificación PENDIENTE DE GESTION. "
                                        f"Saltando procesamiento para evitar sobrescritura."
                                    )
                                    regla_diario_sin_arqueo = True
                                    continue
                            
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
                                
                                # Inicializar resumen de pasos y nombre de regla
                                nombre_regla_aplicada = None
                                resumen_pasos = []
                                resumen_pasos.append(f"REGLA: Solo llega DIARIO, no llega ARQUEO")
                                resumen_pasos.append(f"1. Verificado: Solo llega DIARIO, no llega ARQUEO para cajero {codigo_cajero}")
                                
                                # Inicializar variables de control
                                movimiento_sobrantes_encontrado = False
                                regla_trx_despues12_aplicada = False
                                
                                # Inicializar variables de clasificación (por defecto: revisión manual)
                                justificacion = 'Pendiente de gestion'
                                nuevo_estado = 'PENDIENTE DE GESTION'
                                ratificar_grabar = 'No'
                                observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                
                                # Determinar si es SOBRANTE (negativo) o FALTANTE (positivo)
                                if sobrante < 0:
                                    # CASO SOBRANTE (números negativos en DIARIO)
                                    valor_sobrante_abs = abs(sobrante)
                                    resumen_pasos.append(f"2. Tipo: SOBRANTE (${valor_sobrante_abs:,.0f})")
                                    
                                    if valor_sobrante_abs < 10000000:  # Menor a $10M
                                        # SOBRANTE < $10M: CONTABILIZACION SOBRANTE CONTABLE
                                        nombre_regla_aplicada = "REGLA: Solo DIARIO - SOBRANTE < $10M"
                                        logger.info(
                                            f"Cajero {codigo_cajero}: DIARIO con SOBRANTE < $10M ({valor_sobrante_abs:,.0f}). "
                                            f"Aplicando {nombre_regla_aplicada}: CONTABILIZACION SOBRANTE CONTABLE"
                                        )
                                        
                                        regla_diario_sin_arqueo = True
                                        justificacion = 'Contable'
                                        nuevo_estado = 'CONTABILIZACION SOBRANTE CONTABLE'
                                        ratificar_grabar = 'Si'
                                        observaciones = 'CONTABILIZACION SOBRANTE CONTABLE'
                                        
                                        resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                                        resumen_pasos.append("3. Monto < $10M")
                                        resumen_pasos.append("4. Clasificación: CONTABILIZACION SOBRANTE CONTABLE - Ratificar grabar")
                                    
                                    else:  # >= $10M
                                        # SOBRANTE >= $10M: Consultar histórico del cajero y validar comportamiento
                                        nombre_regla_aplicada = "REGLA: Solo DIARIO - SOBRANTE >= $10M (con patrones históricos)"
                                        logger.info(
                                            f"Cajero {codigo_cajero}: DIARIO con SOBRANTE >= $10M ({valor_sobrante_abs:,.0f}). "
                                            f"Consultando histórico del cajero... Aplicando {nombre_regla_aplicada}"
                                        )
                                        
                                        resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
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
                                                sobrante_val = normalizar_sobrante(row.get('sobrantes', 0))  # Los sobrantes siempre son negativos
                                                sobrantes_ultimos_3.append(sobrante_val)
                                            
                                            # Convertir a valores absolutos para comparar
                                            sobrantes_abs = [abs(s) for s in sobrantes_ultimos_3]
                                            
                                            resumen_pasos.append(f"4. Últimos 3 sobrantes del histórico: {sobrantes_abs}")
                                            
                                            # Verificar patrones según especificación:
                                            # 1 vez: (0, 0, >= 10M) - los últimos 3 sobrantes son 0, 0, >= 10M
                                            # 2 vez: (0, >= 10M, >= 10M) - los últimos 3 sobrantes son 0, >= 10M, >= 10M
                                            # Nota: sobrantes_abs está ordenado del más reciente [0] al más antiguo [2]
                                            # Patrón (0, 0, >= 10M) en orden cronológico = (>= 10M, 0, 0) en el array
                                            
                                            if (sobrantes_abs[2] == 0 and sobrantes_abs[1] == 0 and sobrantes_abs[0] >= 10000000):
                                                # 1 vez: (0, 0, >= 10M) - los últimos 3 sobrantes son 0, 0, >= 10M
                                                nombre_regla_aplicada = "REGLA: Solo DIARIO - SOBRANTE >= $10M (0,0,>=10M) Primera vez"
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Patrón detectado (0, 0, >= 10M). "
                                                    f"Aplicando {nombre_regla_aplicada}: PENDIENTE DE GESTION - Revisar el Diario día siguiente"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'PENDIENTE GESTION'
                                                nuevo_estado = 'Pendiente de gestion'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Revisar el Diario día siguiente'
                                                
                                                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                                                resumen_pasos.append("5. Patrón: (0, 0, >= 10M) - Primera vez")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE DE GESTION - Revisar el Diario día siguiente")
                                            
                                            elif (sobrantes_abs[2] == 0 and sobrantes_abs[1] >= 10000000 and sobrantes_abs[0] >= 10000000):
                                                # 2 vez: (0, >= 10M, >= 10M) - los últimos 3 sobrantes son 0, >= 10M, >= 10M
                                                nombre_regla_aplicada = "REGLA: Solo DIARIO - SOBRANTE >= $10M (0,>=10M,>=10M) Segunda vez"
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Patrón detectado (0, >= 10M, >= 10M). "
                                                    f"Aplicando {nombre_regla_aplicada}: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal nuevamente"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'PENDIENTE GESTION'
                                                nuevo_estado = 'Pendiente gestion'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Se le solicita arqueo a la sucursal nuevamente'
                                                
                                                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                                                resumen_pasos.append("5. Patrón: (0, >= 10M, >= 10M) - Segunda vez")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal nuevamente")
                                            
                                            else:
                                                # No cumple ningún patrón, revisión manual
                                                nombre_regla_aplicada = "REGLA: Solo DIARIO - SOBRANTE >= $10M (No cumple patrón)"
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: No cumple patrón esperado. "
                                                    f"Aplicando {nombre_regla_aplicada}: PENDIENTE DE GESTION"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'Pendiente de gestion'
                                                nuevo_estado = 'PENDIENTE DE GESTION'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                                
                                                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                                                resumen_pasos.append("5. No cumple patrón esperado")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE DE GESTION")
                                        
                                        else:
                                            # No hay suficientes registros en histórico, revisión manual
                                            logger.info(
                                                f"Cajero {codigo_cajero}: No hay suficientes registros en histórico. "
                                                f"Aplicando regla: PENDIENTE DE GESTION"
                                            )
                                            
                                            regla_diario_sin_arqueo = True
                                            justificacion = 'Pendiente de gestion'
                                            nuevo_estado = 'PENDIENTE DE GESTION'
                                            ratificar_grabar = 'No'
                                            observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                            
                                            resumen_pasos.append("4. No hay suficientes registros en histórico")
                                            resumen_pasos.append("5. Clasificación: PENDIENTE DE GESTION")
                                
                                elif faltante > 0:
                                    # CASO FALTANTE (números positivos en DIARIO)
                                    resumen_pasos.append(f"2. Tipo: FALTANTE (${faltante:,.0f})")
                                    
                                    if faltante < 10000000:  # Menor a $10M
                                        # FALTANTE < $10M: Revisar Trx_Despues12 del día anterior
                                        resumen_pasos.append("3. Monto < $10M")
                                        
                                        # Revisar Trx_Despues12 del día anterior a la gestión
                                        movimientos_despues12 = self.cargar_movimientos_despues12()
                                        movimiento_despues12 = movimientos_despues12.get(codigo_cajero, 0)
                                        
                                        if movimiento_despues12 > 0:
                                            # Aparece en Trx_Despues12
                                            resumen_pasos.append("4. Buscado movimiento en Trx_Despues12 (movimientos entre 0:00h y 0:05h del día anterior)")
                                            resumen_pasos.append(f"5. ✓ Movimiento encontrado: ${movimiento_despues12:,.0f}")
                                            
                                            # El valor por el que aparece es igual al del faltante?
                                            if abs(faltante - movimiento_despues12) < 0.01:
                                                # Si: Cerrar el registro con INCIDENTES O EVENTOS MASIVOS
                                                nombre_regla_aplicada = "REGLA: Solo DIARIO - FALTANTE < $10M coincide con Trx_Despues12"
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Faltante (${faltante:,.0f}) coincide con movimiento en Trx_Despues12 (${movimiento_despues12:,.0f}). "
                                                    f"Aplicando {nombre_regla_aplicada}: INCIDENTES O EVENTOS MASIVOS"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'Pendiente de gestion'
                                                nuevo_estado = 'INCIDENTES O EVENTOS MASIVOS'
                                                ratificar_grabar = 'No'
                                                observaciones = 'INCIDENTES O EVENTOS MASIVOS'
                                                
                                                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                                                resumen_pasos.append(f"6. Faltante (${faltante:,.0f}) = Movimiento Trx_Despues12 (${movimiento_despues12:,.0f})")
                                                resumen_pasos.append("7. Clasificación: INCIDENTES O EVENTOS MASIVOS - Cerrar registro")
                                                
                                                # IMPORTANTE: Actualizar el archivo original INMEDIATAMENTE
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                
                                                # Marcar registro como procesado
                                                self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                                                
                                                regla_trx_despues12_aplicada = True
                                            
                                            else:
                                                # No: Restar este valor encontrado al valor del faltante en el registro
                                                faltante_restante = faltante - movimiento_despues12
                                                resumen_pasos.append(f"6. Faltante (${faltante:,.0f}) ≠ Movimiento Trx_Despues12 (${movimiento_despues12:,.0f})")
                                                resumen_pasos.append(f"7. Resta: ${faltante:,.0f} - ${movimiento_despues12:,.0f} = ${faltante_restante:,.0f}")
                                                
                                                # El valor da negativo?
                                                if faltante_restante < 0:
                                                    # Si: Ejecutar regla SOLO DIARIO SOBRANTE
                                                    # Convertir a sobrante y aplicar regla de sobrante
                                                    sobrante_resultante = faltante_restante
                                                    valor_sobrante_abs = abs(sobrante_resultante)
                                                    
                                                    nombre_regla_aplicada = "REGLA: Solo DIARIO - FALTANTE < $10M con Trx_Despues12 (resultado negativo = SOBRANTE)"
                                                    logger.info(
                                                        f"Cajero {codigo_cajero}: Después de restar Trx_Despues12, resultado es negativo (${valor_sobrante_abs:,.0f}). "
                                                        f"Aplicando regla SOBRANTE: CONTABILIZACION SOBRANTE CONTABLE"
                                                    )
                                                    
                                                    regla_diario_sin_arqueo = True
                                                    justificacion = 'Contable'
                                                    nuevo_estado = 'CONTABILIZACION SOBRANTE CONTABLE'
                                                    ratificar_grabar = 'Si'
                                                    observaciones = 'CONTABILIZACION SOBRANTE CONTABLE'
                                                    
                                                    resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                                                    resumen_pasos.append("8. Resultado negativo → Aplicar regla SOBRANTE")
                                                    resumen_pasos.append("9. Clasificación: CONTABILIZACION SOBRANTE CONTABLE - Ratificar grabar")
                                                
                                                else:
                                                    # No: Saltar a la sección *** (revisar en nacional cuenta de sobrantes)
                                                    resumen_pasos.append("8. Resultado positivo → Continuar con búsqueda en cuenta sobrantes")
                                                    
                                                    # *** Revisar en nacional cuenta de sobrantes (valores positivos)
                                                    # Usar faltante_restante (faltante original - Trx_Despues12)
                                                    self._procesar_busqueda_sobrantes_faltante(
                                                        consultor_bd=consultor_bd,
                                                        fecha_arqueo_registro=fecha_arqueo_registro,
                                                        codigo_cajero=codigo_cajero,
                                                        faltante=faltante_restante,
                                                        indices_original=indices_original,
                                                        row_original=row_original,
                                                        resumen_pasos=resumen_pasos,
                                                        query_params=query_params,
                                                        movimiento_sobrantes_encontrado_ref={'value': False}
                                                    )
                                        
                                        else:
                                            # No aparece en Trx_Despues12: Revisar en nacional cuenta de sobrantes
                                            resumen_pasos.append("4. Buscado movimiento en Trx_Despues12 (movimientos entre 0:00h y 0:05h del día anterior)")
                                            resumen_pasos.append("5. ✗ No se encontró movimiento en Trx_Despues12")
                                            
                                            # *** Revisar en nacional cuenta de sobrantes (valores positivos)
                                            # Usar faltante original (no hay ajuste de Trx_Despues12)
                                            self._procesar_busqueda_sobrantes_faltante(
                                                consultor_bd=consultor_bd,
                                                fecha_arqueo_registro=fecha_arqueo_registro,
                                                codigo_cajero=codigo_cajero,
                                                faltante=faltante,
                                                indices_original=indices_original,
                                                row_original=row_original,
                                                resumen_pasos=resumen_pasos,
                                                query_params=query_params,
                                                movimiento_sobrantes_encontrado_ref={'value': False}
                                            )
                                    
                                    else:  # >= $10M
                                        # FALTANTE >= $10M: Consultar histórico de faltantes
                                        nombre_regla_aplicada = "REGLA: Solo DIARIO - FALTANTE >= $10M (con patrones históricos)"
                                        logger.info(
                                            f"Cajero {codigo_cajero}: DIARIO con FALTANTE >= $10M ({faltante:,.0f}). "
                                            f"Consultando histórico de faltantes... Aplicando {nombre_regla_aplicada}"
                                        )
                                        
                                        resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
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
                                            
                                            # Verificar patrones según especificación:
                                            # 1 vez: (0, 0, >= 10M) - los últimos 3 faltantes son 0, 0, >= 10M
                                            # 2 vez: (0, >0, >= 10M) - los últimos 3 faltantes son 0, >0, >= 10M
                                            # Caso especial: (0, 0, 0) - los últimos 3 faltantes son 0, 0, 0 (solicitar arqueo)
                                            # Nota: faltantes_ultimos_3 está ordenado del más reciente [0] al más antiguo [2]
                                            
                                            if (faltantes_ultimos_3[2] == 0 and faltantes_ultimos_3[1] == 0 and faltantes_ultimos_3[0] >= 10000000):
                                                # 1 vez: (0, 0, >= 10M) - los últimos 3 faltantes son 0, 0, >= 10M
                                                nombre_regla_aplicada = "REGLA: Solo DIARIO - FALTANTE >= $10M (0,0,>=10M) Primera vez"
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Patrón detectado (0, 0, >= 10M). "
                                                    f"Aplicando {nombre_regla_aplicada}: PENDIENTE DE GESTION - Se le solicita a la sucursal realizar arqueo"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'Pendiente de gestion'
                                                nuevo_estado = 'PENDIENTE DE GESTION'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Se le solicita arqueo a la sucursal'
                                                
                                                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                                                resumen_pasos.append("5. Patrón: (0, 0, >= 10M) - Primera vez")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal")
                                                
                                                # Actualizar archivo original INMEDIATAMENTE y marcar como procesado
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                                            
                                            elif (faltantes_ultimos_3[2] == 0 and faltantes_ultimos_3[1] > 0 and faltantes_ultimos_3[0] >= 10000000):
                                                # 2 vez: (0, >0, >= 10M) - los últimos 3 faltantes son 0, >0, >= 10M
                                                nombre_regla_aplicada = "REGLA: Solo DIARIO - FALTANTE >= $10M (0,>0,>=10M) Segunda vez"
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Patrón detectado (0, >0, >= 10M). "
                                                    f"Aplicando {nombre_regla_aplicada}: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal nuevamente"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'Pendiente de gestion'
                                                nuevo_estado = 'PENDIENTE DE GESTION'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Se le solicita arqueo a la sucursal'
                                                
                                                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                                                resumen_pasos.append("5. Patrón: (0, >0, >= 10M) - Segunda vez")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal")
                                                
                                                # Actualizar archivo original INMEDIATAMENTE y marcar como procesado
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                                            
                                            elif (faltantes_ultimos_3[2] == 0 and faltantes_ultimos_3[1] == 0 and faltantes_ultimos_3[0] == 0):
                                                # Caso especial: (0, 0, 0) - los últimos 3 faltantes son 0, 0, 0 (solicitar arqueo)
                                                nombre_regla_aplicada = "REGLA: Solo DIARIO - FALTANTE >= $10M (0,0,0) Solicitar arqueo"
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Patrón detectado (0, 0, 0). "
                                                    f"Aplicando {nombre_regla_aplicada}: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'Pendiente de gestion'
                                                nuevo_estado = 'PENDIENTE DE GESTION'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Se le solicita arqueo a la sucursal'
                                                
                                                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                                                resumen_pasos.append("5. Patrón: (0, 0, 0) - Solicitar arqueo")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal")
                                                
                                                # Actualizar archivo original INMEDIATAMENTE y marcar como procesado
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                                            
                                            else:
                                                # No cumple ningún patrón, revisión manual
                                                nombre_regla_aplicada = "REGLA: Solo DIARIO - FALTANTE >= $10M (No cumple patrón)"
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: No cumple patrón esperado. "
                                                    f"Aplicando {nombre_regla_aplicada}: PENDIENTE DE GESTION"
                                                )
                                                
                                                regla_diario_sin_arqueo = True
                                                justificacion = 'Pendiente de gestion'
                                                nuevo_estado = 'PENDIENTE DE GESTION'
                                                ratificar_grabar = 'No'
                                                observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                                
                                                resumen_pasos[0] = f"REGLA APLICADA: {nombre_regla_aplicada}"
                                                resumen_pasos.append("5. No cumple patrón esperado")
                                                resumen_pasos.append("6. Clasificación: PENDIENTE DE GESTION")
                                                
                                                # Actualizar archivo original INMEDIATAMENTE y marcar como procesado
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                                        
                                        else:
                                            # No hay suficientes registros en histórico, revisión manual
                                            nombre_regla_aplicada = "REGLA: Solo DIARIO - FALTANTE >= $10M (No hay suficientes registros en histórico)"
                                            logger.info(
                                                f"Cajero {codigo_cajero}: No hay suficientes registros en histórico. "
                                                f"Aplicando regla: PENDIENTE DE GESTION"
                                            )
                                            
                                            regla_diario_sin_arqueo = True
                                            justificacion = 'Pendiente de gestion'
                                            nuevo_estado = 'PENDIENTE DE GESTION'
                                            ratificar_grabar = 'No'
                                            observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                            
                                            resumen_pasos.append("4. No hay suficientes registros en histórico")
                                            resumen_pasos.append("5. Clasificación: PENDIENTE DE GESTION")
                                            
                                            # Actualizar archivo original INMEDIATAMENTE y marcar como procesado
                                            self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                            self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                            self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                            if 'resumen_pasos' in self._df_archivo_original.columns:
                                                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                            self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                                
                                else:
                                    # No hay sobrante ni faltante, revisión manual
                                    logger.info(
                                        f"Cajero {codigo_cajero}: No hay sobrante ni faltante. "
                                        f"Aplicando regla: PENDIENTE DE GESTION"
                                    )
                                    
                                    regla_diario_sin_arqueo = True
                                    justificacion = 'Pendiente de gestion'
                                    nuevo_estado = 'PENDIENTE DE GESTION'
                                    ratificar_grabar = 'No'
                                    observaciones = 'Este caso requiere la supervisión de personal encargado.'
                                    
                                    resumen_pasos.append("2. No hay sobrante ni faltante")
                                    resumen_pasos.append("3. Clasificación: PENDIENTE DE GESTION")
                                
                                # Actualizar el registro DIARIO INMEDIATAMENTE para evitar que se sobrescriba
                                # IMPORTANTE: Solo actualizar si NO se encontró movimiento en sobrantes Y NO se aplicó Trx_Despues12 (ya se actualizó antes)
                                # Inicializar regla_trx_despues12_aplicada si no existe
                                if 'regla_trx_despues12_aplicada' not in locals():
                                    regla_trx_despues12_aplicada = False
                                
                                # Verificar si el registro ya tiene una regla aplicada (puede haber sido actualizado por _procesar_busqueda_sobrantes_faltante)
                                regla_aplicada_verificar = None
                                if 'regla_aplicada' in self._df_archivo_original.columns and len(indices_original) > 0:
                                    regla_val_verificar = self._df_archivo_original.loc[indices_original[0], 'regla_aplicada']
                                    if pd.notna(regla_val_verificar):
                                        regla_aplicada_verificar = str(regla_val_verificar).strip()
                                
                                if not movimiento_sobrantes_encontrado and not regla_trx_despues12_aplicada and not regla_aplicada_verificar:
                                    # IMPORTANTE: Si se detectó Trx_Despues12, asegurar que los valores sean correctos
                                    if observaciones == 'INCIDENTES O EVENTOS MASIVOS' and justificacion == 'Pendiente de gestion':
                                        # Asegurar que el estado sea correcto
                                        nuevo_estado = 'INCIDENTES O EVENTOS MASIVOS'
                                        ratificar_grabar = 'No'
                                        # Marcar que se aplicó la regla de Trx_Despues12 para evitar que se sobrescriba
                                        regla_trx_despues12_aplicada = True
                                    else:
                                        regla_trx_despues12_aplicada = False
                                    
                                    # Actualizar el archivo original INMEDIATAMENTE
                                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                    self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                    self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                    self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                    if 'resumen_pasos' in self._df_archivo_original.columns:
                                        self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                    
                                    # Log del resultado final
                                    logger.info(
                                        f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                        f"justificacion='{justificacion}', nuevo_estado='{nuevo_estado}', "
                                        f"ratificar_grabar='{ratificar_grabar}', observaciones='{observaciones}'"
                                    )
                                    
                                    # Si se aplicó la regla de Trx_Despues12, log adicional
                                    if regla_trx_despues12_aplicada:
                                        logger.info(
                                            f"Cajero {codigo_cajero}: Regla Trx_Despues12 aplicada. "
                                            f"Saltando procesamiento adicional para este registro."
                                        )
                                else:
                                    # Ya se actualizó el archivo original con Trx_Despues12 o CRUCE DE NOVEDADES, no sobrescribir
                                    if regla_trx_despues12_aplicada:
                                        logger.info(
                                            f"Cajero {codigo_cajero}: Ya se actualizó el archivo original con INCIDENTES O EVENTOS MASIVOS (Trx_Despues12). "
                                            f"No se sobrescribirán los valores."
                                        )
                                    elif movimiento_sobrantes_encontrado:
                                        logger.info(
                                            f"Cajero {codigo_cajero}: Ya se actualizó el archivo original con CRUCE DE NOVEDADES. "
                                            f"No se sobrescribirán los valores."
                                        )
                                
                                # IMPORTANTE: Marcar regla_diario_sin_arqueo = True para evitar que se procese nuevamente
                                regla_diario_sin_arqueo = True
                            
                            except Exception as e:
                                logger.warning(f"Error al aplicar REGLA 4 (DIARIO sin ARQUEO): {e}", exc_info=True)
                                # En caso de error, aplicar revisión manual
                                regla_diario_sin_arqueo = True
                                justificacion = 'Pendiente de gestion'
                                nuevo_estado = 'PENDIENTE DE GESTION'
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
                
                # VERIFICACIÓN PRIORITARIA: Si el registro ya fue clasificado con Trx_Despues12, CRUCE DE NOVEDADES, etc.
                # NO procesar nuevamente
                observaciones_actual = str(self._df_archivo_original.loc[idx_actual, 'observaciones']).strip() if 'observaciones' in self._df_archivo_original.columns and pd.notna(self._df_archivo_original.loc[idx_actual, 'observaciones']) else None
                logger.debug(f"DEBUG: Registro {idx_original} (cajero {codigo_cajero}): observaciones_actual='{observaciones_actual}'")
                if observaciones_actual == 'INCIDENTES O EVENTOS MASIVOS':
                    logger.info(
                        f"Registro {idx_original} (cajero {codigo_cajero}): Ya tiene clasificación Trx_Despues12. "
                        f"No se sobrescribirán los valores."
                    )
                    continue
                elif observaciones_actual and str(observaciones_actual).strip().replace('.0', '').isdigit() and len(str(observaciones_actual).strip().replace('.0', '')) == 8:
                    logger.info(
                        f"Registro {idx_original} (cajero {codigo_cajero}): Ya tiene clasificación CRUCE DE NOVEDADES (NUMDOC: {observaciones_actual}). "
                        f"No se sobrescribirán los valores."
                    )
                    continue
                
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
                    # IMPORTANTE: Solo aplicar regla de FALTANTE si realmente hay un faltante (faltante > 0), no un sobrante
                    if codigo_cajero is not None and diferencia_actual > 0 and faltante > 0:
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
                            sobrante_otro = normalizar_sobrante(registro_otro_tipo.get('sobrantes', 0))  # Los sobrantes siempre son negativos
                            
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
                            
                            # Verificar si tienen diferencias opuestas (uno faltante y otro sobrante)
                            # diferencia_actual puede ser faltante (positivo) o sobrante (abs de negativo)
                            # Necesitamos verificar los valores reales, no solo las diferencias
                            faltante_actual_real = limpiar_valor_numerico(row_original_actual.get('faltantes', 0))
                            sobrante_actual_real = normalizar_sobrante(row_original_actual.get('sobrantes', 0))
                            
                            tiene_faltante_actual = faltante_actual_real > 0
                            tiene_sobrante_actual = sobrante_actual_real < 0
                            tiene_faltante_otro = faltante_otro > 0
                            tiene_sobrante_otro = sobrante_otro < 0
                            
                            # Caso: uno tiene faltante y el otro tiene sobrante (diferencias opuestas)
                            diferencias_opuestas = (
                                (tiene_faltante_actual and tiene_sobrante_otro) or 
                                (tiene_sobrante_actual and tiene_faltante_otro)
                            )
                            
                            if diferencias_opuestas:
                                # REGLA: ARQUEO y DIARIO con diferencias opuestas (uno faltante, otro sobrante)
                                # No hay regla específica para este caso, dejar para gestión manual
                                nombre_regla_aplicada = "REGLA: ARQUEO y DIARIO con diferencias opuestas (uno faltante, otro sobrante)"
                                tipo_otro = 'DIARIO' if tipo_registro == 'ARQUEO' else 'ARQUEO'
                                
                                logger.info(
                                    f"Cajero {codigo_cajero}: {tipo_registro} y {tipo_otro} tienen diferencias opuestas. "
                                    f"{tipo_registro}: {'FALTANTE' if tiene_faltante_actual else 'SOBRANTE'} ${diferencia_actual:,.0f}, "
                                    f"{tipo_otro}: {'FALTANTE' if tiene_faltante_otro else 'SOBRANTE'} ${diferencia_otro:,.0f}. "
                                    f"Aplicando regla: Pendiente de gestion"
                                )
                                
                                regla_diferencias_opuestas = True
                                justificacion_actual = 'Pendiente de gestion'
                                nuevo_estado_actual = 'PENDIENTE DE GESTION'
                                ratificar_grabar_actual = 'No'
                                observaciones_actual = 'ARQUEO y DIARIO con diferencias opuestas (uno faltante, otro sobrante). Requiere revisión manual.'
                                
                                # Resumen de pasos
                                if not resumen_pasos:
                                    resumen_pasos = [f"REGLA APLICADA: {nombre_regla_aplicada}"]
                                else:
                                    resumen_pasos.insert(0, f"REGLA APLICADA: {nombre_regla_aplicada}")
                                
                                resumen_pasos.append(f"1. Verificado: {tipo_registro} y {tipo_otro} tienen diferencias opuestas")
                                resumen_pasos.append(f"2. {tipo_registro}: {'FALTANTE' if tiene_faltante_actual else 'SOBRANTE'} ${diferencia_actual:,.0f}")
                                resumen_pasos.append(f"3. {tipo_otro}: {'FALTANTE' if tiene_faltante_otro else 'SOBRANTE'} ${diferencia_otro:,.0f}")
                                resumen_pasos.append(f"4. Clasificación: Pendiente de gestion - Requiere revisión manual")
                                
                                # Actualizar el registro actual
                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_actual
                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_actual
                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_actual
                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                
                                # Actualizar también el registro del otro tipo
                                idx_otro_tipo = registro_otro_tipo.name
                                self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_actual
                                self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_actual
                                self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_actual
                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                    self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                
                                logger.info(
                                    f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                    f"justificacion='{justificacion_actual}', nuevo_estado='{nuevo_estado_actual}', "
                                    f"ratificar_grabar='{ratificar_grabar_actual}'"
                                )
                                
                                continue  # Saltar el procesamiento normal
                            
                            if misma_diferencia_faltante:
                                # Aplicar nueva regla: ARQUEO y DIARIO con misma diferencia (FALTANTE)
                                nombre_regla_aplicada = "REGLA 2: ARQUEO y DIARIO tienen la misma diferencia (FALTANTE)"
                                tipo_otro = 'DIARIO' if tipo_registro == 'ARQUEO' else 'ARQUEO'
                                
                                logger.info(
                                    f"Cajero {codigo_cajero}: Aplicando {nombre_regla_aplicada}. "
                                    f"Diferencia: ${diferencia_actual:,.0f}"
                                )
                                
                                # Inicializar resumen de pasos con el nombre de la regla
                                if not resumen_pasos:
                                    resumen_pasos = [f"REGLA APLICADA: {nombre_regla_aplicada}"]
                                else:
                                    resumen_pasos.insert(0, f"REGLA APLICADA: {nombre_regla_aplicada}")
                                
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
                                            if justificacion_actual in ['Cruzar', 'Cruzar']:
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
                                        if resumen_pasos_actual:
                                            # Limpiar el resumen de pasos eliminando mensajes incorrectos de REGLA GENÉRICA
                                            resumen_ajustado = self._limpiar_resumen_pasos_regla_generica(
                                                str(resumen_pasos_actual),
                                                nombre_regla_aplicada
                                            )
                                        else:
                                            resumen_ajustado = f"REGLA APLICADA: {nombre_regla_aplicada} | 1. Copiado de {tipo_otro} procesado anteriormente"
                                        self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = resumen_ajustado
                                        
                                        # También actualizar el resumen de pasos del otro registro para limpiarlo
                                        idx_otro_tipo = registro_otro_tipo.name
                                        resumen_otro_actual = registro_otro_tipo.get('resumen_pasos', '')
                                        if resumen_otro_actual:
                                            resumen_otro_limpiado = self._limpiar_resumen_pasos_regla_generica(
                                                str(resumen_otro_actual),
                                                nombre_regla_aplicada
                                            )
                                            self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = resumen_otro_limpiado
                                    
                                    # Log del resultado antes de continuar
                                    logger.info(
                                        f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                        f"justificacion='{justificacion_actual}', nuevo_estado='{nuevo_estado_actual}', "
                                        f"ratificar_grabar='{ratificar_grabar_actual}' (copiado de {tipo_otro})"
                                    )
                                    
                                    continue  # Saltar el procesamiento normal ya que se copiaron los valores
                                
                                logger.info(
                                    f"Cajero {codigo_cajero}: {tipo_registro} y {tipo_otro} tienen la misma diferencia (FALTANTE: {diferencia_actual:,.0f}). "
                                    f"Aplicando {nombre_regla_aplicada}: Error en Transmicion de contadores"
                                )
                                
                                # Inicializar resumen de pasos con el nombre de la regla
                                if not resumen_pasos:
                                    resumen_pasos = [f"REGLA APLICADA: {nombre_regla_aplicada}"]
                                else:
                                    resumen_pasos.insert(0, f"REGLA APLICADA: {nombre_regla_aplicada}")
                                
                                # Obtener consultor BD si está disponible
                                consultor_bd = None
                                if self.consultor and hasattr(self.consultor, '_consultor_bd'):
                                    consultor_bd = self.consultor._consultor_bd
                                
                                if fecha_arqueo_registro and consultor_bd:
                                    try:
                                        config_data = self.config.cargar()
                                        query_params = config_data.get('base_datos', {}).get('query_params', {})
                                        
                                        # PASO 1: Buscar en NACIONAL cuenta 110505075, el día del arqueo, por el valor del faltante
                                        # Para faltantes, buscar un Crédito (valor positivo) por el valor del faltante
                                        # Usar diferencia_actual positiva para buscar Crédito
                                        valor_para_bd = diferencia_actual if diferencia_actual > 0 else abs(diferencia_actual)
                                        movimiento_nacional = consultor_bd.consultar_movimientos_nacional(
                                            codigo_cajero=codigo_cajero,
                                            fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                            valor_descuadre=valor_para_bd,  # Buscar Crédito (positivo) por el valor del faltante
                                            cuenta=query_params.get('cuenta', 110505075),
                                            codofi_excluir=query_params.get('codofi_excluir', 976),
                                            nrocmp=query_params.get('nrocmp', 770500),
                                            solo_dia_arqueo=True  # Buscar SOLO el día del arqueo
                                        )
                                        
                                        # Variable para controlar si debemos buscar en sobrantes
                                        buscar_en_sobrantes = False
                                        
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
                                                        logger.info(
                                                            f"Cajero {codigo_cajero}: Movimiento encontrado en NACIONAL cuenta 110505075 "
                                                            f"pero con fecha diferente (movimiento: {fecha_movimiento.strftime('%Y-%m-%d')}, "
                                                            f"arqueo: {fecha_arqueo_registro.strftime('%Y-%m-%d')}). "
                                                            f"Tratando como 'no aparece' y buscando en sobrantes..."
                                                        )
                                                        buscar_en_sobrantes = True
                                            except Exception as e:
                                                logger.debug(f"Error al comparar fechas del movimiento: {e}")
                                            
                                            regla_arqueo_diario_igual_faltante = True
                                            
                                            # Si la fecha es diferente, tratar como "no aparece" y buscar en sobrantes
                                            if not buscar_en_sobrantes:
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
                                                
                                                # Marcar ambos registros como procesados
                                                self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                                                self._marcar_registro_procesado([idx_otro_tipo], nombre_regla_aplicada)
                                        
                                        # Si no se encontró movimiento en NACIONAL o la fecha es diferente, buscar en sobrantes
                                        if not movimiento_nacional or buscar_en_sobrantes:
                                            # CASO 2: NO aparece en NACIONAL cuenta 110505075 (o fecha diferente)
                                            # Buscar en cuenta de sobrantes 279510020 días anteriores (valores positivos que sumen el faltante)
                                            logger.info(
                                                f"Cajero {codigo_cajero}: No se encontró movimiento en NACIONAL cuenta 110505075. "
                                                f"Buscando en cuenta de sobrantes 279510020 días anteriores (valores positivos que sumen el faltante)..."
                                            )
                                            
                                            # Usar el método que busca múltiples sobrantes positivos que sumen el faltante
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Llamando a consultar_sobrantes_positivos_multiples "
                                                f"con faltante ${diferencia_actual:,.0f}"
                                            )
                                            resultado_sobrantes = consultor_bd.consultar_sobrantes_positivos_multiples(
                                                codigo_cajero=codigo_cajero,
                                                fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                                valor_faltante=diferencia_actual,
                                                cuenta=279510020,
                                                codofi_excluir=query_params.get('codofi_excluir', 976),
                                                dias_anteriores=30
                                            )
                                            
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Resultado de consultar_sobrantes_positivos_multiples: "
                                                f"encontrado={resultado_sobrantes.get('encontrado') if resultado_sobrantes else None}, "
                                                f"caso={resultado_sobrantes.get('caso') if resultado_sobrantes else None}, "
                                                f"suma={resultado_sobrantes.get('suma') if resultado_sobrantes else None}"
                                            )
                                            
                                            if resultado_sobrantes and resultado_sobrantes.get('encontrado'):
                                                # Verificar si la suma de los sobrantes encontrados coincide con el faltante
                                                suma_encontrada = resultado_sobrantes.get('suma', 0)
                                                caso = resultado_sobrantes.get('caso', '')
                                                movimientos = resultado_sobrantes.get('movimientos', [])
                                                
                                                # Solo aplicar CRUCE DE NOVEDADES si la suma coincide exactamente con el faltante
                                                # (caso 'exacto' o 'suma_igual')
                                                if caso in ['exacto', 'suma_igual']:
                                                    logger.info(
                                                        f"Cajero {codigo_cajero}: Se encontraron sobrantes positivos "
                                                        f"que suman {suma_encontrada:,.0f} (faltante: {diferencia_actual:,.0f}). "
                                                        f"Aplicando regla: CRUCE DE NOVEDADES"
                                                    )
                                                    
                                                    regla_arqueo_diario_igual_faltante = True
                                                    
                                                    # Obtener NUMDOC del movimiento más reciente (primer movimiento de la lista)
                                                    numdoc = None
                                                    if movimientos and len(movimientos) > 0:
                                                        numdoc = movimientos[0].get('NUMDOC')
                                                    
                                                    # Convertir NUMDOC a string entero sin ".0"
                                                    if numdoc is not None:
                                                        numdoc_int = int(float(numdoc))
                                                        numdoc_str = str(numdoc_int)  # YYYYMMDD
                                                    else:
                                                        # Fallback: usar fecha del arqueo si no hay NUMDOC
                                                        fecha_arqueo_str = fecha_arqueo_registro.strftime('%Y%m%d')
                                                        numdoc_str = str(int(fecha_arqueo_str))
                                                    
                                                    # ARQUEO
                                                    justificacion_arqueo = 'Cruzar'
                                                    nuevo_estado_arqueo = 'CRUCE DE NOVEDADES'
                                                    ratificar_grabar_arqueo = 'Reverso'
                                                    observaciones_arqueo = numdoc_str
                                                    
                                                    # DIARIO
                                                    justificacion_diario = 'Cruzar'
                                                    nuevo_estado_diario = 'CRUCE DE NOVEDADES'
                                                    ratificar_grabar_diario = 'No'
                                                    observaciones_diario = 'Se reversa diferencia con cuadre anterior'
                                                    
                                                    # Resumen de pasos para ARQUEO
                                                    resumen_pasos_arqueo = []
                                                    resumen_pasos_arqueo.append(f"1. Verificado: {tipo_registro} y {tipo_otro} tienen misma diferencia (FALTANTE: ${diferencia_actual:,.0f})")
                                                    resumen_pasos_arqueo.append(f"2. Buscado en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${diferencia_actual:,.0f}")
                                                    resumen_pasos_arqueo.append("3. ✗ No encontrado en cuenta 110505075")
                                                    resumen_pasos_arqueo.append(f"4. Buscado en cuenta sobrantes 279510020 días anteriores (valores positivos que sumen el faltante), valor ${diferencia_actual:,.0f}")
                                                    if len(movimientos) == 1:
                                                        resumen_pasos_arqueo.append(f"5. ✓ Encontrado 1 movimiento positivo que suma ${suma_encontrada:,.0f} (NUMDOC: {numdoc_str})")
                                                    else:
                                                        resumen_pasos_arqueo.append(f"5. ✓ Encontrados {len(movimientos)} movimientos positivos que suman ${suma_encontrada:,.0f} (NUMDOC más reciente: {numdoc_str})")
                                                    resumen_pasos_arqueo.append("6. Clasificación: CRUCE DE NOVEDADES - Reverso")
                                                    
                                                    # Resumen de pasos para DIARIO
                                                    resumen_pasos_diario = resumen_pasos_arqueo.copy()
                                                    resumen_pasos_diario[-1] = "6. Clasificación: CRUCE DE NOVEDADES - No ratificar"
                                                    
                                                    # Buscar explícitamente los índices de ARQUEO y DIARIO para asegurar actualización correcta
                                                    idx_otro_tipo = registro_otro_tipo.name
                                                    
                                                    # Buscar índices de ARQUEO y DIARIO explícitamente
                                                    registros_mismo_cajero = self._df_archivo_original[
                                                        (self._df_archivo_original['codigo_cajero'] == codigo_cajero) &
                                                        (self._df_archivo_original['fecha_arqueo'] == fecha_arqueo_registro)
                                                    ]
                                                    
                                                    idx_arqueo = registros_mismo_cajero[registros_mismo_cajero['tipo_registro'] == 'ARQUEO'].index
                                                    idx_diario = registros_mismo_cajero[registros_mismo_cajero['tipo_registro'] == 'DIARIO'].index
                                                    
                                                    # Actualizar ARQUEO con valores de ARQUEO
                                                    if len(idx_arqueo) > 0:
                                                        self._df_archivo_original.loc[idx_arqueo, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                                        self._df_archivo_original.loc[idx_arqueo, 'justificacion'] = justificacion_arqueo
                                                        self._df_archivo_original.loc[idx_arqueo, 'nuevo_estado'] = nuevo_estado_arqueo
                                                        self._df_archivo_original.loc[idx_arqueo, 'observaciones'] = observaciones_arqueo
                                                        if 'resumen_pasos' in self._df_archivo_original.columns:
                                                            self._df_archivo_original.loc[idx_arqueo, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                                        # Marcar ARQUEO como procesado
                                                        self._marcar_registro_procesado(idx_arqueo.tolist(), nombre_regla_aplicada)
                                                    
                                                    # Actualizar DIARIO con valores de DIARIO
                                                    if len(idx_diario) > 0:
                                                        self._df_archivo_original.loc[idx_diario, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                        self._df_archivo_original.loc[idx_diario, 'justificacion'] = justificacion_diario
                                                        self._df_archivo_original.loc[idx_diario, 'nuevo_estado'] = nuevo_estado_diario
                                                        self._df_archivo_original.loc[idx_diario, 'observaciones'] = observaciones_diario
                                                        if 'resumen_pasos' in self._df_archivo_original.columns:
                                                            self._df_archivo_original.loc[idx_diario, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                                        # Marcar DIARIO como procesado
                                                        self._marcar_registro_procesado(idx_diario.tolist(), nombre_regla_aplicada)
                                                else:
                                                    # La suma no coincide exactamente con el faltante, no aplicar CRUCE DE NOVEDADES
                                                    # Continuar con el siguiente caso (FALTANTE EN ARQUEO)
                                                    resultado_sobrantes = None
                                            
                                            if not resultado_sobrantes or not resultado_sobrantes.get('encontrado'):
                                                # CASO 2b: NO aparece en cuenta de sobrantes 279510020 (valores positivos que sumen el faltante)
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: No se encontraron sobrantes positivos que sumen el faltante en cuenta 279510020. "
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
                                                resumen_pasos_arqueo.append(f"4. Buscado en cuenta sobrantes 279510020 días anteriores (valores positivos que sumen el faltante), valor ${diferencia_actual:,.0f}")
                                                resumen_pasos_arqueo.append("5. ✗ No encontrados sobrantes positivos que sumen el faltante")
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
                                                
                                                # Marcar ambos registros como procesados
                                                self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                                                self._marcar_registro_procesado([idx_otro_tipo], nombre_regla_aplicada)
                                    
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
                            
                            elif diferencia_actual > 0 and diferencia_otro > 0 and not misma_diferencia_faltante:
                                # REGLA: ARQUEO Y DIARIO, Diferentes diferencias - FALTANTE
                                # Ambos tienen faltantes pero con valores diferentes
                                tipo_otro = 'DIARIO' if tipo_registro == 'ARQUEO' else 'ARQUEO'
                                
                                logger.info(
                                    f"Cajero {codigo_cajero}: {tipo_registro} y {tipo_otro} tienen diferentes diferencias (FALTANTE). "
                                    f"ARQUEO: {diferencia_actual:,.0f}, DIARIO: {diferencia_otro:,.0f}. "
                                    f"Aplicando regla de Diferentes diferencias - FALTANTE"
                                )
                                
                                # Obtener consultor BD si está disponible
                                consultor_bd = None
                                if self.consultor and hasattr(self.consultor, '_consultor_bd'):
                                    consultor_bd = self.consultor._consultor_bd
                                
                                if fecha_arqueo_registro and consultor_bd:
                                    try:
                                        config_data = self.config.cargar()
                                        query_params = config_data.get('base_datos', {}).get('query_params', {})
                                        
                                        # Determinar faltante ARQUEO y diferencia DIARIO (puede ser faltante o sobrante)
                                        # IMPORTANTE: Verificar si realmente es faltante o sobrante, no solo usar diferencia_otro
                                        if tipo_registro == 'ARQUEO':
                                            faltante_arqueo = diferencia_actual
                                            # Verificar si el DIARIO realmente tiene faltante o sobrante
                                            faltante_diario_real = limpiar_valor_numerico(registro_otro_tipo.get('faltantes', 0))
                                            sobrante_diario_real = normalizar_sobrante(registro_otro_tipo.get('sobrantes', 0))
                                            # Si tiene faltante, usar faltante; si tiene sobrante, usar 0 (no es un faltante)
                                            faltante_diario = faltante_diario_real if faltante_diario_real > 0 else 0
                                            es_sobrante_diario = sobrante_diario_real < 0
                                        else:
                                            faltante_arqueo = diferencia_otro
                                            # Verificar si el registro actual (DIARIO) realmente tiene faltante o sobrante
                                            faltante_diario_real = limpiar_valor_numerico(row_original_actual.get('faltantes', 0))
                                            sobrante_diario_real = normalizar_sobrante(row_original_actual.get('sobrantes', 0))
                                            # Si tiene faltante, usar faltante; si tiene sobrante, usar 0 (no es un faltante)
                                            faltante_diario = faltante_diario_real if faltante_diario_real > 0 else 0
                                            es_sobrante_diario = sobrante_diario_real < 0
                                        
                                        # Si el DIARIO tiene sobrante, no aplicar esta regla (es para diferentes faltantes)
                                        if es_sobrante_diario:
                                            logger.info(
                                                f"Cajero {codigo_cajero}: DIARIO tiene SOBRANTE (${abs(sobrante_diario_real):,.0f}), "
                                                f"no un faltante. Esta regla es solo para diferentes faltantes. "
                                                f"Saltando esta regla."
                                            )
                                            continue
                                        
                                        # PASO 1: Revisar en Nacional cuenta 110505075 movimientos (positivos y negativos)
                                        # con fecha del arqueo y comprobantes 770500 o 810291
                                        movimientos_negativos = consultor_bd.consultar_movimientos_negativos_mismo_dia(
                                            codigo_cajero=codigo_cajero,
                                            fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                            cuenta=110505075,
                                            codofi_excluir=query_params.get('codofi_excluir', 976),
                                            nrocmps=[770500, 810291]
                                        )
                                        
                                        if movimientos_negativos and movimientos_negativos.get('encontrado', False):
                                            # Aparecen movimientos (positivos y/o negativos)
                                            suma_negativos = movimientos_negativos.get('suma_negativos', 0)  # Ya viene en valor absoluto
                                            suma_positivos = movimientos_negativos.get('suma_positivos', 0)  # Provisiones (positivas)
                                            
                                            # Inicializar variable para controlar si se usan las provisiones
                                            usar_provisiones = False
                                            
                                            # VERIFICACIÓN: Solo usar provisiones si están relacionadas con el descuadre
                                            # Una provisión está relacionada si:
                                            # 1. El valor de la provisión es similar al faltante ARQUEO (dentro de un 20% de diferencia)
                                            # 2. O si el faltante ajustado se acerca más al faltante DIARIO
                                            if suma_positivos > 0:
                                                # Calcular diferencia porcentual entre provisión y faltante ARQUEO
                                                diferencia_porcentual = abs(suma_positivos - faltante_arqueo) / max(faltante_arqueo, 1) * 100
                                                
                                                # Calcular cómo quedaría el faltante ajustado con y sin provisiones
                                                faltante_sin_provisiones = faltante_arqueo - suma_negativos
                                                faltante_con_provisiones = faltante_arqueo - suma_negativos - suma_positivos
                                                
                                                # Calcular qué tan cerca queda del faltante DIARIO
                                                diferencia_sin_provisiones = abs(faltante_sin_provisiones - faltante_diario)
                                                diferencia_con_provisiones = abs(faltante_con_provisiones - faltante_diario)
                                                
                                                # Usar provisiones si:
                                                # - La provisión es similar al faltante (dentro del 20%)
                                                # - O si el faltante ajustado se acerca más al faltante DIARIO (mejora en al menos 10%)
                                                if diferencia_porcentual <= 20:
                                                    usar_provisiones = True
                                                    logger.info(
                                                        f"Cajero {codigo_cajero}: Provisión (${suma_positivos:,.0f}) está relacionada con el descuadre. "
                                                        f"Diferencia porcentual: {diferencia_porcentual:.1f}% (similar al faltante ARQUEO: ${faltante_arqueo:,.0f})"
                                                    )
                                                elif diferencia_con_provisiones < diferencia_sin_provisiones * 0.9:
                                                    # El faltante ajustado se acerca más al DIARIO (mejora de al menos 10%)
                                                    usar_provisiones = True
                                                    logger.info(
                                                        f"Cajero {codigo_cajero}: Provisión (${suma_positivos:,.0f}) mejora la coincidencia con DIARIO. "
                                                        f"Sin provisión: diferencia ${diferencia_sin_provisiones:,.0f}, "
                                                        f"Con provisión: diferencia ${diferencia_con_provisiones:,.0f}"
                                                    )
                                                else:
                                                    logger.info(
                                                        f"Cajero {codigo_cajero}: Provisión (${suma_positivos:,.0f}) NO está relacionada con el descuadre. "
                                                        f"Faltante ARQUEO: ${faltante_arqueo:,.0f}, Diferencia porcentual: {diferencia_porcentual:.1f}%. "
                                                        f"No se usará para ajustar el faltante."
                                                    )
                                            
                                            # Si el ARQUEO ya fue procesado y ajustado, calcular el faltante ARQUEO original
                                            # sumando la provisión al faltante actual (solo si se usó la provisión)
                                            # Esto es necesario porque cuando se procesa el DIARIO, el ARQUEO ya fue modificado
                                            if tipo_registro == 'DIARIO' and suma_positivos > 0 and usar_provisiones:
                                                # El faltante ARQUEO actual puede ser el ajustado, calcular el original
                                                faltante_arqueo_original = faltante_arqueo + suma_positivos
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: ARQUEO ya fue ajustado. "
                                                    f"Faltante ARQUEO actual (ajustado): ${faltante_arqueo:,.0f}, "
                                                    f"Provisión encontrada: ${suma_positivos:,.0f}, "
                                                    f"Faltante ARQUEO original calculado: ${faltante_arqueo_original:,.0f}"
                                                )
                                                faltante_arqueo = faltante_arqueo_original
                                            
                                            # Calcular ajuste total: restar negativos y restar positivos (provisiones) solo si están relacionadas
                                            # Los negativos reducen el faltante, las provisiones también reducen el faltante
                                            suma_positivos_ajuste = suma_positivos if usar_provisiones else 0
                                            ajuste_total = suma_negativos + suma_positivos_ajuste
                                            faltante_arqueo_ajustado = faltante_arqueo - ajuste_total
                                            
                                            if suma_positivos > 0 and not usar_provisiones:
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Provisión encontrada (${suma_positivos:,.0f}) pero NO se usará para ajustar. "
                                                    f"Faltante ARQUEO ajustado: ${faltante_arqueo_ajustado:,.0f} (solo con movimientos negativos: ${suma_negativos:,.0f})"
                                                )
                                            
                                            # PASO 1.5: Ajustar con movimientos después de 12 (Trx_Despues12)
                                            # Estos movimientos son faltantes que ocurrieron entre 0:00h y 0:05h
                                            # y no fueron contabilizados el día anterior
                                            movimientos_despues12 = self.cargar_movimientos_despues12()
                                            movimiento_despues12 = movimientos_despues12.get(codigo_cajero, 0)
                                            
                                            if movimiento_despues12 > 0:
                                                faltante_arqueo_ajustado = faltante_arqueo_ajustado - movimiento_despues12
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Movimiento después de 12 encontrado: ${movimiento_despues12:,.0f}. "
                                                    f"Faltante ARQUEO ajustado (después de provisiones): ${faltante_arqueo_ajustado + movimiento_despues12:,.0f}, "
                                                    f"Faltante ARQUEO ajustado (después de mov. después 12): ${faltante_arqueo_ajustado:,.0f}"
                                                )
                                            
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Movimientos encontrados en NACIONAL. "
                                                f"Negativos: ${suma_negativos:,.0f}, Positivos (provisiones): ${suma_positivos:,.0f}, "
                                                f"Provisión usada para ajustar: {'Sí' if usar_provisiones else 'No'}, "
                                                f"Ajuste total: ${ajuste_total:,.0f}, "
                                                f"Faltante ARQUEO original: ${faltante_arqueo:,.0f}, "
                                                f"Faltante ARQUEO ajustado: ${faltante_arqueo_ajustado:,.0f}, "
                                                f"Faltante DIARIO: ${faltante_diario:,.0f}"
                                            )
                                            
                                            # Actualizar el faltante ARQUEO en el archivo con el faltante ajustado
                                            # Solo actualizar si realmente se ajustó el faltante (hay movimientos negativos o provisiones relacionadas)
                                            idx_arqueo = registro_otro_tipo.name if tipo_registro == 'DIARIO' else indices_original[0]
                                            
                                            # Solo actualizar el sobrante si hay un ajuste significativo
                                            if ajuste_total > 0:
                                                # Si el faltante ajustado es negativo, significa que se convirtió en sobrante
                                                # Normalizar correctamente: los sobrantes deben ser negativos
                                                if faltante_arqueo_ajustado < 0:
                                                    # Es un sobrante, normalizar como negativo
                                                    self._df_archivo_original.loc[idx_arqueo, 'sobrantes'] = faltante_arqueo_ajustado
                                                    self._df_archivo_original.loc[idx_arqueo, 'faltantes'] = 0
                                                    logger.info(
                                                        f"Cajero {codigo_cajero}: Faltante ajustado es negativo (${faltante_arqueo_ajustado:,.0f}), "
                                                        f"actualizando como sobrante: ${faltante_arqueo_ajustado:,.0f}"
                                                    )
                                                else:
                                                    # Sigue siendo faltante, actualizar faltantes
                                                    self._df_archivo_original.loc[idx_arqueo, 'faltantes'] = faltante_arqueo_ajustado
                                                    self._df_archivo_original.loc[idx_arqueo, 'sobrantes'] = 0
                                                    logger.info(
                                                        f"Cajero {codigo_cajero}: Faltante ajustado sigue siendo positivo (${faltante_arqueo_ajustado:,.0f}), "
                                                        f"actualizando faltantes: ${faltante_arqueo_ajustado:,.0f}"
                                                    )
                                            else:
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: No hay ajuste significativo (ajuste_total: ${ajuste_total:,.0f}), "
                                                    f"no se actualizarán los valores de sobrantes/faltantes"
                                                )
                                            
                                            # Evaluar si el resultado es igual al faltante DIARIO
                                            if abs(faltante_arqueo_ajustado - faltante_diario) < 0.01:
                                                # SI: El movimiento es igual al Faltante en DIARIO
                                                # EJECUTAR REGLA: ARQUEO Y DIARIO MISMAS DIFERENCIAS
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Faltante ARQUEO ajustado ({faltante_arqueo_ajustado:,.0f}) "
                                                    f"es igual al Faltante DIARIO ({faltante_diario:,.0f}). "
                                                    f"Ejecutando regla: ARQUEO Y DIARIO MISMAS DIFERENCIAS"
                                                )
                                                
                                                # Actualizar el faltante ARQUEO en el archivo
                                                idx_arqueo = registro_otro_tipo.name if tipo_registro == 'DIARIO' else indices_original[0]
                                                self._df_archivo_original.loc[idx_arqueo, 'faltantes'] = faltante_diario
                                                
                                                # Aplicar la regla de "ARQUEO Y DIARIO MISMAS DIFERENCIAS" sin buscar en cuenta del cajero
                                                # (ya hicimos esa consulta)
                                                # Esta lógica se ejecutará en la siguiente iteración cuando se procese el registro
                                                # Por ahora, marcar como procesado para que se aplique la regla de "misma diferencia"
                                                regla_arqueo_diario_igual_faltante = True
                                                
                                                # Aplicar clasificación básica que será refinada en la siguiente iteración
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
                                                
                                                resumen_pasos = []
                                                resumen_pasos.append(f"1. Identificado: ARQUEO y DIARIO con diferentes diferencias (FALTANTE)")
                                                resumen_pasos.append(f"2. ARQUEO: ${faltante_arqueo:,.0f}, DIARIO: ${faltante_diario:,.0f}")
                                                resumen_pasos.append(f"3. Buscado movimientos en NACIONAL cuenta 110505075 (comprobantes 770500 y 810291)")
                                                if suma_positivos > 0 and suma_negativos > 0:
                                                    resumen_pasos.append(f"4. ✓ Movimientos encontrados: Negativos = ${suma_negativos:,.0f}, Positivos (provisiones) = ${suma_positivos:,.0f}, Ajuste total = ${ajuste_total:,.0f}")
                                                elif suma_positivos > 0:
                                                    resumen_pasos.append(f"4. ✓ Movimientos encontrados: Positivos (provisiones) = ${suma_positivos:,.0f}")
                                                else:
                                                    resumen_pasos.append(f"4. ✓ Movimientos encontrados: Negativos = ${suma_negativos:,.0f}")
                                                faltante_despues_provisiones = faltante_arqueo - ajuste_total
                                                resumen_pasos.append(f"5. Faltante ARQUEO ajustado (después de provisiones): ${faltante_arqueo:,.0f} - ${ajuste_total:,.0f} = ${faltante_despues_provisiones:,.0f}")
                                                if movimiento_despues12 > 0:
                                                    resumen_pasos.append(f"6. Buscado movimientos después de 12 (Trx_Despues12)")
                                                    resumen_pasos.append(f"7. ✓ Movimiento encontrado: ${movimiento_despues12:,.0f}")
                                                    resumen_pasos.append(f"8. Faltante ARQUEO ajustado (después de mov. después 12): ${faltante_despues_provisiones:,.0f} - ${movimiento_despues12:,.0f} = ${faltante_arqueo_ajustado:,.0f}")
                                                    resumen_pasos.append(f"9. Faltante ARQUEO ajustado = Faltante DIARIO → Ejecutar regla: ARQUEO Y DIARIO MISMAS DIFERENCIAS")
                                                else:
                                                    resumen_pasos.append(f"6. Faltante ARQUEO ajustado = Faltante DIARIO → Ejecutar regla: ARQUEO Y DIARIO MISMAS DIFERENCIAS")
                                                
                                                # Actualizar el registro actual
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_actual
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_actual
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_actual
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                
                                                # Log del resultado
                                                logger.info(
                                                    f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                                    f"justificacion='{justificacion_actual}', nuevo_estado='{nuevo_estado_actual}', "
                                                    f"ratificar_grabar='{ratificar_grabar_actual}'"
                                                )
                                                
                                                continue  # Saltar el procesamiento normal
                                            
                                            elif faltante_arqueo_ajustado > faltante_diario:
                                                # NO, es mayor
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Faltante ARQUEO ajustado ({faltante_arqueo_ajustado:,.0f}) "
                                                    f"es MAYOR que Faltante DIARIO ({faltante_diario:,.0f}). "
                                                    f"Aplicando regla: Pendiente de gestion"
                                                )
                                                
                                                regla_arqueo_diario_diferente_faltante = True
                                                justificacion_actual = 'Pendiente de gestion'
                                                nuevo_estado_actual = 'PENDIENTE DE GESTION'
                                                ratificar_grabar_actual = 'No'
                                                observaciones_actual = 'Este caso requiere la supervisión de personal encargado.'
                                                
                                                resumen_pasos = []
                                                resumen_pasos.append(f"1. Identificado: ARQUEO y DIARIO con diferentes diferencias (FALTANTE)")
                                                resumen_pasos.append(f"2. ARQUEO: ${faltante_arqueo:,.0f}, DIARIO: ${faltante_diario:,.0f}")
                                                resumen_pasos.append(f"3. Buscado movimientos en NACIONAL cuenta 110505075 (comprobantes 770500 y 810291)")
                                                if suma_positivos > 0 and suma_negativos > 0:
                                                    resumen_pasos.append(f"4. ✓ Movimientos encontrados: Negativos = ${suma_negativos:,.0f}, Positivos (provisiones) = ${suma_positivos:,.0f}, Ajuste total = ${ajuste_total:,.0f}")
                                                elif suma_positivos > 0:
                                                    resumen_pasos.append(f"4. ✓ Movimientos encontrados: Positivos (provisiones) = ${suma_positivos:,.0f}")
                                                else:
                                                    resumen_pasos.append(f"4. ✓ Movimientos encontrados: Negativos = ${suma_negativos:,.0f}")
                                                faltante_despues_provisiones = faltante_arqueo - ajuste_total
                                                resumen_pasos.append(f"5. Faltante ARQUEO ajustado (después de provisiones): ${faltante_arqueo:,.0f} - ${ajuste_total:,.0f} = ${faltante_despues_provisiones:,.0f}")
                                                if movimiento_despues12 > 0:
                                                    resumen_pasos.append(f"6. Buscado movimientos después de 12 (Trx_Despues12)")
                                                    resumen_pasos.append(f"7. ✓ Movimiento encontrado: ${movimiento_despues12:,.0f}")
                                                    resumen_pasos.append(f"8. Faltante ARQUEO ajustado (después de mov. después 12): ${faltante_despues_provisiones:,.0f} - ${movimiento_despues12:,.0f} = ${faltante_arqueo_ajustado:,.0f}")
                                                    resumen_pasos.append(f"9. Faltante ARQUEO ajustado > Faltante DIARIO → Clasificación: Pendiente de gestion")
                                                else:
                                                    resumen_pasos.append(f"6. Faltante ARQUEO ajustado > Faltante DIARIO → Clasificación: Pendiente de gestion")
                                                
                                                # Actualizar ambos registros
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_actual
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_actual
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_actual
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                
                                                idx_otro_tipo = registro_otro_tipo.name
                                                self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                                self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_actual
                                                self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_actual
                                                self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_actual
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                
                                                # Log del resultado
                                                logger.info(
                                                    f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                                    f"justificacion='{justificacion_actual}', nuevo_estado='{nuevo_estado_actual}', "
                                                    f"ratificar_grabar='{ratificar_grabar_actual}'"
                                                )
                                                
                                                continue  # Saltar el procesamiento normal
                                        
                                        # No aparecen movimientos (ni positivos ni negativos)
                                        logger.info(
                                            f"Cajero {codigo_cajero}: No se encontraron movimientos en NACIONAL cuenta 110505075 "
                                            f"(comprobantes 770500 y 810291) para el día del arqueo."
                                        )
                                        
                                        # PASO 1.5: Ajustar con movimientos después de 12 (Trx_Despues12)
                                        faltante_arqueo_ajustado = faltante_arqueo
                                        movimientos_despues12 = self.cargar_movimientos_despues12()
                                        movimiento_despues12 = movimientos_despues12.get(codigo_cajero, 0)
                                        
                                        if movimiento_despues12 > 0:
                                            faltante_arqueo_ajustado = faltante_arqueo_ajustado - movimiento_despues12
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Movimiento después de 12 encontrado: ${movimiento_despues12:,.0f}. "
                                                f"Faltante ARQUEO original: ${faltante_arqueo:,.0f}, "
                                                f"Faltante ARQUEO ajustado (después de mov. después 12): ${faltante_arqueo_ajustado:,.0f}"
                                            )
                                            
                                            # Evaluar si el faltante ajustado es igual al faltante DIARIO
                                            if abs(faltante_arqueo_ajustado - faltante_diario) < 0.01:
                                                logger.info(
                                                    f"Cajero {codigo_cajero}: Faltante ARQUEO ajustado ({faltante_arqueo_ajustado:,.0f}) "
                                                    f"es igual al Faltante DIARIO ({faltante_diario:,.0f}) después de ajustar por mov. después 12. "
                                                    f"Ejecutando regla: ARQUEO Y DIARIO MISMAS DIFERENCIAS"
                                                )
                                                
                                                regla_arqueo_diario_igual_faltante = True
                                                
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
                                                
                                                resumen_pasos = []
                                                resumen_pasos.append(f"1. Identificado: ARQUEO y DIARIO con diferentes diferencias (FALTANTE)")
                                                resumen_pasos.append(f"2. ARQUEO: ${faltante_arqueo:,.0f}, DIARIO: ${faltante_diario:,.0f}")
                                                resumen_pasos.append(f"3. Buscado movimientos en NACIONAL cuenta 110505075 (comprobantes 770500 y 810291)")
                                                resumen_pasos.append(f"4. ✗ No encontrado")
                                                resumen_pasos.append(f"5. Buscado movimientos después de 12 (Trx_Despues12)")
                                                resumen_pasos.append(f"6. ✓ Movimiento encontrado: ${movimiento_despues12:,.0f}")
                                                resumen_pasos.append(f"7. Faltante ARQUEO ajustado: ${faltante_arqueo:,.0f} - ${movimiento_despues12:,.0f} = ${faltante_arqueo_ajustado:,.0f}")
                                                resumen_pasos.append(f"8. Faltante ARQUEO ajustado = Faltante DIARIO → Ejecutar regla: ARQUEO Y DIARIO MISMAS DIFERENCIAS")
                                                
                                                # Actualizar ambos registros
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_actual
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_actual
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_actual
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                
                                                idx_otro_tipo = registro_otro_tipo.name
                                                self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual if tipo_registro == 'DIARIO' else 'No'
                                                self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_actual
                                                self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_actual
                                                self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_actual
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                                
                                                logger.info(
                                                    f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                                    f"justificacion='{justificacion_actual}', nuevo_estado='{nuevo_estado_actual}', "
                                                    f"ratificar_grabar='{ratificar_grabar_actual}'"
                                                )
                                                
                                                continue  # Saltar el procesamiento normal
                                        
                                        # PASO 2: Buscar en cuenta de sobrantes días anteriores
                                        logger.info(
                                            f"Cajero {codigo_cajero}: No se encontraron movimientos en NACIONAL (comprobantes 770500 y 810291). "
                                            f"Buscando en cuenta de sobrantes días anteriores..."
                                        )
                                        
                                        movimiento_sobrantes = consultor_bd.consultar_cuenta_sobrantes_dias_anteriores(
                                            codigo_cajero=codigo_cajero,
                                            fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                            valor_descuadre=faltante_arqueo_ajustado,  # Usar faltante ajustado (si hubo movimientos) o original
                                            cuenta=279510020,
                                            codofi_excluir=query_params.get('codofi_excluir', 976),
                                            dias_anteriores=30
                                        )
                                        
                                        if movimiento_sobrantes:
                                            # Aparece en cuenta de sobrantes
                                            # Usar NUMDOC (fecha del documento) en lugar de FECHA
                                            numdoc = movimiento_sobrantes.get('NUMDOC')
                                            fecha_movimiento = movimiento_sobrantes.get('FECHA')  # Para logging
                                            logger.info(
                                                f"Cajero {codigo_cajero}: Movimiento encontrado en cuenta de sobrantes "
                                                f"(NUMDOC: {numdoc}, FECHA: {fecha_movimiento}). Aplicando regla: CRUCE DE NOVEDADES"
                                            )
                                            
                                            regla_arqueo_diario_diferente_faltante = True
                                            
                                            # ARQUEO
                                            justificacion_arqueo = 'Cruzar'
                                            nuevo_estado_arqueo = 'CRUCE DE NOVEDADES'
                                            ratificar_grabar_arqueo = 'Reverso'
                                            # Convertir NUMDOC a entero para evitar ".0" al final
                                            if numdoc is not None:
                                                numdoc_int = int(float(numdoc))
                                                observaciones_arqueo = str(numdoc_int)  # YYYYMMDD
                                            else:
                                                observaciones_arqueo = str(numdoc)  # YYYYMMDD
                                            
                                            # DIARIO
                                            justificacion_diario = 'Cruzar'
                                            nuevo_estado_diario = 'CRUCE DE NOVEDADES'
                                            ratificar_grabar_diario = 'No'
                                            observaciones_diario = 'Se reversa diferencia con cuadre anterior'
                                            
                                            resumen_pasos_arqueo = []
                                            resumen_pasos_arqueo.append(f"1. Identificado: ARQUEO y DIARIO con diferentes diferencias (FALTANTE)")
                                            resumen_pasos_arqueo.append(f"2. ARQUEO: ${faltante_arqueo:,.0f}, DIARIO: ${faltante_diario:,.0f}")
                                            resumen_pasos_arqueo.append(f"3. Buscado movimientos en NACIONAL cuenta 110505075 (comprobantes 770500 y 810291)")
                                            resumen_pasos_arqueo.append(f"4. ✗ No encontrado")
                                            if movimiento_despues12 > 0:
                                                resumen_pasos_arqueo.append(f"5. Buscado movimientos después de 12 (Trx_Despues12)")
                                                resumen_pasos_arqueo.append(f"6. ✓ Movimiento encontrado: ${movimiento_despues12:,.0f}")
                                                resumen_pasos_arqueo.append(f"7. Faltante ARQUEO ajustado: ${faltante_arqueo:,.0f} - ${movimiento_despues12:,.0f} = ${faltante_arqueo_ajustado:,.0f}")
                                                resumen_pasos_arqueo.append(f"8. Buscado en cuenta sobrantes 279510020 días anteriores (con faltante ajustado)")
                                                resumen_pasos_arqueo.append(f"9. ✓ Movimiento encontrado (NUMDOC: {numdoc})")
                                                resumen_pasos_arqueo.append(f"10. Clasificación: CRUCE DE NOVEDADES - Reverso")
                                            else:
                                                resumen_pasos_arqueo.append(f"5. Buscado en cuenta sobrantes 279510020 días anteriores")
                                                resumen_pasos_arqueo.append(f"6. ✓ Movimiento encontrado (NUMDOC: {numdoc})")
                                                resumen_pasos_arqueo.append(f"7. Clasificación: CRUCE DE NOVEDADES - Reverso")
                                            
                                            resumen_pasos_diario = []
                                            resumen_pasos_diario.append(f"1. Identificado: ARQUEO y DIARIO con diferentes diferencias (FALTANTE)")
                                            resumen_pasos_diario.append(f"2. ARQUEO: ${faltante_arqueo:,.0f}, DIARIO: ${faltante_diario:,.0f}")
                                            resumen_pasos_diario.append(f"3. Buscado movimientos en NACIONAL cuenta 110505075 (comprobantes 770500 y 810291)")
                                            resumen_pasos_diario.append(f"4. ✗ No encontrado")
                                            if movimiento_despues12 > 0:
                                                resumen_pasos_diario.append(f"5. Buscado movimientos después de 12 (Trx_Despues12)")
                                                resumen_pasos_diario.append(f"6. ✓ Movimiento encontrado: ${movimiento_despues12:,.0f}")
                                                resumen_pasos_diario.append(f"7. Faltante ARQUEO ajustado: ${faltante_arqueo:,.0f} - ${movimiento_despues12:,.0f} = ${faltante_arqueo_ajustado:,.0f}")
                                                resumen_pasos_diario.append(f"8. Buscado en cuenta sobrantes 279510020 días anteriores (con faltante ajustado)")
                                                resumen_pasos_diario.append(f"9. ✓ Movimiento encontrado (NUMDOC: {numdoc})")
                                                resumen_pasos_diario.append(f"10. Clasificación: CRUCE DE NOVEDADES")
                                            else:
                                                resumen_pasos_diario.append(f"5. Buscado en cuenta sobrantes 279510020 días anteriores")
                                                resumen_pasos_diario.append(f"6. ✓ Movimiento encontrado (NUMDOC: {numdoc})")
                                                resumen_pasos_diario.append(f"7. Clasificación: CRUCE DE NOVEDADES")
                                            
                                            # Actualizar ambos registros
                                            if tipo_registro == 'ARQUEO':
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_arqueo
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_arqueo
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_arqueo
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                                
                                                idx_diario = registro_otro_tipo.name
                                                self._df_archivo_original.loc[idx_diario, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                self._df_archivo_original.loc[idx_diario, 'justificacion'] = justificacion_diario
                                                self._df_archivo_original.loc[idx_diario, 'nuevo_estado'] = nuevo_estado_diario
                                                self._df_archivo_original.loc[idx_diario, 'observaciones'] = observaciones_diario
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_diario, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                            else:
                                                self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_diario
                                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_diario
                                                self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_diario
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                                
                                                idx_arqueo = registro_otro_tipo.name
                                                self._df_archivo_original.loc[idx_arqueo, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                                self._df_archivo_original.loc[idx_arqueo, 'justificacion'] = justificacion_arqueo
                                                self._df_archivo_original.loc[idx_arqueo, 'nuevo_estado'] = nuevo_estado_arqueo
                                                self._df_archivo_original.loc[idx_arqueo, 'observaciones'] = observaciones_arqueo
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_arqueo, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                            
                                            # Log del resultado
                                            logger.info(
                                                f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                                f"justificacion='{justificacion_arqueo if tipo_registro == 'ARQUEO' else justificacion_diario}', "
                                                f"nuevo_estado='{nuevo_estado_arqueo if tipo_registro == 'ARQUEO' else nuevo_estado_diario}', "
                                                f"ratificar_grabar='{ratificar_grabar_arqueo if tipo_registro == 'ARQUEO' else ratificar_grabar_diario}'"
                                            )
                                            
                                            continue  # Saltar el procesamiento normal
                                        
                                        # No aparece en cuenta de sobrantes
                                        logger.info(
                                            f"Cajero {codigo_cajero}: No se encontró movimiento en cuenta de sobrantes. "
                                            f"Aplicando regla: PENDIENTE DE GESTION"
                                        )
                                        
                                        regla_arqueo_diario_diferente_faltante = True
                                        justificacion_actual = 'PENDIENTE DE GESTION'
                                        nuevo_estado_actual = 'Pendiente de gestión'
                                        ratificar_grabar_actual = 'No'
                                        observaciones_actual = 'Se le solicita arqueo a la sucursal'
                                        
                                        resumen_pasos = []
                                        resumen_pasos.append(f"1. Identificado: ARQUEO y DIARIO con diferentes diferencias (FALTANTE)")
                                        resumen_pasos.append(f"2. ARQUEO: ${faltante_arqueo:,.0f}, DIARIO: ${faltante_diario:,.0f}")
                                        resumen_pasos.append(f"3. Buscado movimientos en NACIONAL cuenta 110505075 (comprobantes 770500 y 810291)")
                                        resumen_pasos.append(f"4. ✗ No encontrado")
                                        if movimiento_despues12 > 0:
                                            resumen_pasos.append(f"5. Buscado movimientos después de 12 (Trx_Despues12)")
                                            resumen_pasos.append(f"6. ✓ Movimiento encontrado: ${movimiento_despues12:,.0f}")
                                            resumen_pasos.append(f"7. Faltante ARQUEO ajustado: ${faltante_arqueo:,.0f} - ${movimiento_despues12:,.0f} = ${faltante_arqueo_ajustado:,.0f}")
                                            resumen_pasos.append(f"8. Buscado en cuenta sobrantes 279510020 días anteriores (con faltante ajustado)")
                                            resumen_pasos.append(f"9. ✗ No encontrado")
                                            resumen_pasos.append(f"10. Clasificación: PENDIENTE DE GESTION")
                                        else:
                                            resumen_pasos.append(f"5. Buscado en cuenta sobrantes 279510020 días anteriores")
                                            resumen_pasos.append(f"6. ✗ No encontrado")
                                            resumen_pasos.append(f"7. Clasificación: PENDIENTE DE GESTION")
                                        
                                        # Actualizar ambos registros
                                        self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                        self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_actual
                                        self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_actual
                                        self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_actual
                                        if 'resumen_pasos' in self._df_archivo_original.columns:
                                            self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                        
                                        idx_otro_tipo = registro_otro_tipo.name
                                        self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                        self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_actual
                                        self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_actual
                                        self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_actual
                                        if 'resumen_pasos' in self._df_archivo_original.columns:
                                            self._df_archivo_original.loc[idx_otro_tipo, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                        
                                        # Log del resultado
                                        logger.info(
                                            f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                            f"justificacion='{justificacion_actual}', nuevo_estado='{nuevo_estado_actual}', "
                                            f"ratificar_grabar='{ratificar_grabar_actual}'"
                                        )
                                        
                                        continue  # Saltar el procesamiento normal
                                    
                                    except Exception as e:
                                        logger.warning(f"Error al aplicar regla ARQUEO Y DIARIO Diferentes diferencias - FALTANTE: {e}", exc_info=True)
                                        # En caso de error, continuar con otras reglas
                                
                                else:
                                    # No hay consultor BD o fecha, aplicar regla básica
                                    logger.warning(
                                        f"Cajero {codigo_cajero}: {tipo_registro} y {tipo_otro} tienen diferentes diferencias (FALTANTE), "
                                        f"pero no se puede consultar BD. Aplicando regla básica: PENDIENTE DE GESTION"
                                    )
                                    
                                    regla_arqueo_diario_diferente_faltante = True
                                    justificacion_actual = 'PENDIENTE DE GESTION'
                                    nuevo_estado_actual = 'Pendiente de gestión'
                                    ratificar_grabar_actual = 'No'
                                    observaciones_actual = 'Se le solicita arqueo a la sucursal'
                                    
                                    # Actualizar ambos registros
                                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                    self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_actual
                                    self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_actual
                                    self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_actual
                                    
                                    idx_otro_tipo = registro_otro_tipo.name
                                    self._df_archivo_original.loc[idx_otro_tipo, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                    self._df_archivo_original.loc[idx_otro_tipo, 'justificacion'] = justificacion_actual
                                    self._df_archivo_original.loc[idx_otro_tipo, 'nuevo_estado'] = nuevo_estado_actual
                                    self._df_archivo_original.loc[idx_otro_tipo, 'observaciones'] = observaciones_actual
                                    
                                    continue  # Saltar el procesamiento normal
                
                # Si ya se aplicó la regla de ARQUEO/DIARIO igual faltante o diferentes diferencias, saltar las otras reglas
                # Log para debug: verificar qué está bloqueando la regla específica de SOBRANTE
                if tipo_registro == 'ARQUEO' and codigo_cajero == 2042:
                    logger.info(
                        f"DEBUG Cajero 2042: Verificando condición para regla específica SOBRANTE - "
                        f"regla_arqueo_sin_diario={regla_arqueo_sin_diario}, "
                        f"regla_diario_sin_arqueo={regla_diario_sin_arqueo}, "
                        f"regla_arqueo_diario_igual_faltante={regla_arqueo_diario_igual_faltante}, "
                        f"regla_arqueo_diario_diferente_faltante={regla_arqueo_diario_diferente_faltante}, "
                        f"regla_diferencias_opuestas={regla_diferencias_opuestas}, "
                        f"sobrante={sobrante}"
                    )
                
                if not regla_arqueo_sin_diario and not regla_diario_sin_arqueo and not regla_arqueo_diario_igual_faltante and not regla_arqueo_diario_diferente_faltante and not regla_diferencias_opuestas:
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
                            
                            sobrante_diario = normalizar_sobrante(registro_diario.get('sobrantes', 0))  # Los sobrantes siempre son negativos
                            
                            # Log para debug
                            if codigo_cajero == 2042:
                                logger.info(
                                    f"DEBUG Cajero 2042: Verificando misma diferencia SOBRANTE - "
                                    f"sobrante_arqueo={sobrante}, sobrante_diario={sobrante_diario}"
                                )
                            
                            # Comparar sobrantes (deben ser iguales)
                            misma_diferencia_sobrante = False
                            if sobrante < 0 and sobrante_diario < 0:
                                if abs(sobrante - sobrante_diario) < 0.01:  # Tolerancia para floats
                                    misma_diferencia_sobrante = True
                                    if codigo_cajero == 2042:
                                        logger.info(
                                            f"DEBUG Cajero 2042: ✓ Misma diferencia SOBRANTE detectada!"
                                        )
                            
                            if misma_diferencia_sobrante:
                                # Aplicar nueva regla: ARQUEO y DIARIO con misma diferencia (SOBRANTE)
                                logger.info(
                                    f"Cajero {codigo_cajero}: ARQUEO y DIARIO tienen la misma diferencia (SOBRANTE: {sobrante}). "
                                    f"Aplicando regla específica de SOBRANTE"
                                )
                                
                                if codigo_cajero == 2042:
                                    logger.info(
                                        f"DEBUG Cajero 2042: ✓ Aplicando regla específica de SOBRANTE"
                                    )
                                
                                # Obtener consultor BD si está disponible
                                consultor_bd = None
                                if self.consultor and hasattr(self.consultor, '_consultor_bd'):
                                    consultor_bd = self.consultor._consultor_bd
                                
                                if fecha_arqueo_registro and consultor_bd:
                                    try:
                                        config_data = self.config.cargar()
                                        query_params = config_data.get('base_datos', {}).get('query_params', {})
                                        
                                        # PASO 1: Buscar en NACIONAL cuenta 110505075 algún DÉBITO por el valor del Sobrante con fecha del arqueo
                                        # Buscar SOLO el día del arqueo (solo_dia_arqueo=True)
                                        movimiento_nacional = consultor_bd.consultar_movimientos_nacional(
                                            codigo_cajero=codigo_cajero,
                                            fecha_arqueo=fecha_arqueo_registro.strftime('%Y-%m-%d'),
                                            valor_descuadre=sobrante,  # Sobrante es negativo (DÉBITO)
                                            cuenta=query_params.get('cuenta', 110505075),
                                            codofi_excluir=query_params.get('codofi_excluir', 976),
                                            nrocmp=query_params.get('nrocmp', 770500),
                                            solo_dia_arqueo=True  # Buscar SOLO el día del arqueo
                                        )
                                        
                                        if movimiento_nacional:
                                            # CASO 1: Aparece en NACIONAL cuenta 110505075 (DÉBITO) con fecha del arqueo
                                            logger.info(
                                                f"Cajero {codigo_cajero}: DÉBITO encontrado en NACIONAL cuenta 110505075 "
                                                f"con fecha del arqueo ({fecha_arqueo_registro.strftime('%Y-%m-%d')}). "
                                                f"Aplicando regla: PENDIENTE DE GESTION"
                                            )
                                            
                                            regla_arqueo_diario_igual_sobrante = True
                                            
                                            # ARQUEO Y DIARIO - Ambos deben tener la misma clasificación
                                            justificacion = 'Pendiente de gestion'
                                            nuevo_estado = 'Pendiente de gestión'
                                            ratificar_grabar = 'No'
                                            observaciones = 'Cajero cuadrado con arqueo de la sucursal'
                                            
                                            # Resumen de pasos
                                            resumen_pasos = []
                                            resumen_pasos.append(f"1. Verificado: ARQUEO y DIARIO tienen misma diferencia (SOBRANTE: ${abs(sobrante):,.0f})")
                                            resumen_pasos.append(f"2. Buscado DÉBITO en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${abs(sobrante):,.0f}")
                                            resumen_pasos.append("3. ✓ DÉBITO encontrado en cuenta 110505075 con fecha del arqueo")
                                            resumen_pasos.append("4. Clasificación: PENDIENTE DE GESTION - Cajero cuadrado con arqueo de la sucursal")
                                            
                                            # Actualizar el registro ARQUEO (usando indices_original)
                                            self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                            self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                            self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                            self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                            if 'resumen_pasos' in self._df_archivo_original.columns:
                                                self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                            
                                            # Actualizar también el registro del otro tipo (DIARIO)
                                            idx_diario = registro_diario.name
                                            self._df_archivo_original.loc[idx_diario, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                            self._df_archivo_original.loc[idx_diario, 'justificacion'] = justificacion
                                            self._df_archivo_original.loc[idx_diario, 'nuevo_estado'] = nuevo_estado
                                            self._df_archivo_original.loc[idx_diario, 'observaciones'] = observaciones
                                            if 'resumen_pasos' in self._df_archivo_original.columns:
                                                self._df_archivo_original.loc[idx_diario, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                            
                                            # Marcar como procesado para evitar que se sobrescriba
                                            self._marcar_registro_procesado(indices_original, 'ARQUEO y DIARIO misma diferencia SOBRANTE - PENDIENTE DE GESTION')
                                            self._marcar_registro_procesado([idx_diario], 'ARQUEO y DIARIO misma diferencia SOBRANTE - PENDIENTE DE GESTION')
                                            
                                            # IMPORTANTE: Saltar el resto del procesamiento para evitar que la regla genérica sobrescriba
                                            continue
                                        
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
                                                resumen_pasos_arqueo.append(f"2. Buscado DÉBITO en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${abs(sobrante):,.0f}")
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
                                                
                                                # Actualizar el registro del otro tipo (DIARIO)
                                                idx_diario = registro_diario.name
                                                self._df_archivo_original.loc[idx_diario, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                                self._df_archivo_original.loc[idx_diario, 'justificacion'] = justificacion_diario
                                                self._df_archivo_original.loc[idx_diario, 'nuevo_estado'] = nuevo_estado_diario
                                                self._df_archivo_original.loc[idx_diario, 'observaciones'] = observaciones_diario
                                                if 'resumen_pasos' in self._df_archivo_original.columns:
                                                    self._df_archivo_original.loc[idx_diario, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                                
                                                # Marcar como procesado para evitar que se sobrescriba
                                                self._marcar_registro_procesado(indices_original, 'ARQUEO y DIARIO misma diferencia SOBRANTE - CRUCE DE NOVEDADES')
                                                self._marcar_registro_procesado([idx_diario], 'ARQUEO y DIARIO misma diferencia SOBRANTE - CRUCE DE NOVEDADES')
                                                
                                                # IMPORTANTE: Saltar el resto del procesamiento para evitar que la regla genérica sobrescriba
                                                continue
                                            
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
                                                resumen_pasos_arqueo.append(f"2. Buscado DÉBITO en NACIONAL cuenta 110505075, fecha {fecha_arqueo_registro.strftime('%Y-%m-%d')}, valor ${abs(sobrante):,.0f}")
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
                                                
                                                # Marcar como procesado para evitar que se sobrescriba
                                                self._marcar_registro_procesado(indices_original, 'ARQUEO y DIARIO misma diferencia SOBRANTE - CONTABILIZACION SOBRANTE FISICO')
                                                self._marcar_registro_procesado([idx_diario], 'ARQUEO y DIARIO misma diferencia SOBRANTE - CONTABILIZACION SOBRANTE FISICO')
                                                
                                                # IMPORTANTE: Saltar el resto del procesamiento para evitar que la regla genérica sobrescriba
                                                continue
                                    
                                    except Exception as e:
                                        logger.warning(f"Error al aplicar regla ARQUEO/DIARIO igual sobrante: {e}", exc_info=True)
                            
                            elif sobrante < 0 and sobrante_diario < 0 and not misma_diferencia_sobrante:
                                # REGLA: ARQUEO Y DIARIO, Diferentes diferencias - SOBRANTE
                                # Ambos tienen sobrantes pero con valores diferentes
                                logger.info(
                                    f"Cajero {codigo_cajero}: ARQUEO y DIARIO tienen diferentes diferencias (SOBRANTE). "
                                    f"ARQUEO: {sobrante:,.0f}, DIARIO: {sobrante_diario:,.0f}. "
                                    f"Aplicando regla de Diferentes diferencias - SOBRANTE"
                                )
                                
                                # Determinar sobrante ARQUEO y sobrante DIARIO (valores absolutos)
                                sobrante_arqueo_abs = abs(sobrante)
                                sobrante_diario_abs = abs(sobrante_diario)
                                
                                # Usar el mayor de los dos para determinar la clasificación
                                sobrante_mayor = max(sobrante_arqueo_abs, sobrante_diario_abs)
                                
                                regla_arqueo_diario_diferente_sobrante = True
                                
                                if sobrante_mayor >= 10000000:
                                    # Si la cantidad es 10M o más
                                    logger.info(
                                        f"Cajero {codigo_cajero}: Sobrante mayor >= $10M ({sobrante_mayor:,.0f}). "
                                        f"Aplicando regla: PENDIENTE DE GESTION"
                                    )
                                    
                                    justificacion_actual = 'PENDIENTE DE GESTION'
                                    nuevo_estado_actual = 'Pendiente de gestión'
                                    ratificar_grabar_actual = 'No'
                                    observaciones_actual = 'Se le solicita arqueo a la sucursal'
                                    
                                    resumen_pasos = []
                                    resumen_pasos.append(f"1. Identificado: ARQUEO y DIARIO con diferentes diferencias (SOBRANTE)")
                                    resumen_pasos.append(f"2. ARQUEO: ${sobrante_arqueo_abs:,.0f}, DIARIO: ${sobrante_diario_abs:,.0f}")
                                    resumen_pasos.append(f"3. Sobrante mayor >= $10M (${sobrante_mayor:,.0f})")
                                    resumen_pasos.append(f"4. Clasificación: PENDIENTE DE GESTION")
                                    
                                    # Actualizar ambos registros
                                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                    self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_actual
                                    self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_actual
                                    self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_actual
                                    if 'resumen_pasos' in self._df_archivo_original.columns:
                                        self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                    
                                    idx_diario = registro_diario.name
                                    self._df_archivo_original.loc[idx_diario, 'ratificar_grabar_diferencia'] = ratificar_grabar_actual
                                    self._df_archivo_original.loc[idx_diario, 'justificacion'] = justificacion_actual
                                    self._df_archivo_original.loc[idx_diario, 'nuevo_estado'] = nuevo_estado_actual
                                    self._df_archivo_original.loc[idx_diario, 'observaciones'] = observaciones_actual
                                    if 'resumen_pasos' in self._df_archivo_original.columns:
                                        self._df_archivo_original.loc[idx_diario, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                    
                                    # Log del resultado
                                    logger.info(
                                        f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                        f"justificacion='{justificacion_actual}', nuevo_estado='{nuevo_estado_actual}', "
                                        f"ratificar_grabar='{ratificar_grabar_actual}'"
                                    )
                                    
                                    continue  # Saltar el procesamiento normal
                                
                                else:
                                    # Si la cantidad es menor a 10M
                                    logger.info(
                                        f"Cajero {codigo_cajero}: Sobrante menor < $10M ({sobrante_mayor:,.0f}). "
                                        f"Aplicando regla: CONTABILIZACION SOBRANTE FISICO"
                                    )
                                    
                                    # ARQUEO
                                    justificacion_arqueo = 'CONTABILIZAR'
                                    nuevo_estado_arqueo = 'CONTABILIZACION SOBRANTE FISICO'
                                    ratificar_grabar_arqueo = 'Si'
                                    observaciones_arqueo = 'Contabilizacion sobrante fisico'
                                    
                                    # DIARIO
                                    justificacion_diario = 'CONTABILIZAR'
                                    nuevo_estado_diario = 'CONTABILIZACION SOBRANTE FISICO'
                                    ratificar_grabar_diario = 'No'
                                    observaciones_diario = 'Contabilizacion sobrante fisico'
                                    
                                    resumen_pasos_arqueo = []
                                    resumen_pasos_arqueo.append(f"1. Identificado: ARQUEO y DIARIO con diferentes diferencias (SOBRANTE)")
                                    resumen_pasos_arqueo.append(f"2. ARQUEO: ${sobrante_arqueo_abs:,.0f}, DIARIO: ${sobrante_diario_abs:,.0f}")
                                    resumen_pasos_arqueo.append(f"3. Sobrante menor < $10M (${sobrante_mayor:,.0f})")
                                    resumen_pasos_arqueo.append(f"4. Clasificación: CONTABILIZACION SOBRANTE FISICO")
                                    resumen_pasos_arqueo.append(f"5. Ratificar grabar: Si")
                                    
                                    resumen_pasos_diario = []
                                    resumen_pasos_diario.append(f"1. Identificado: ARQUEO y DIARIO con diferentes diferencias (SOBRANTE)")
                                    resumen_pasos_diario.append(f"2. ARQUEO: ${sobrante_arqueo_abs:,.0f}, DIARIO: ${sobrante_diario_abs:,.0f}")
                                    resumen_pasos_diario.append(f"3. Sobrante menor < $10M (${sobrante_mayor:,.0f})")
                                    resumen_pasos_diario.append(f"4. Clasificación: CONTABILIZACION SOBRANTE FISICO")
                                    resumen_pasos_diario.append(f"5. Ratificar grabar: No")
                                    
                                    # Actualizar ambos registros
                                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar_arqueo
                                    self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion_arqueo
                                    self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado_arqueo
                                    self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones_arqueo
                                    if 'resumen_pasos' in self._df_archivo_original.columns:
                                        self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos_arqueo)
                                    
                                    idx_diario = registro_diario.name
                                    self._df_archivo_original.loc[idx_diario, 'ratificar_grabar_diferencia'] = ratificar_grabar_diario
                                    self._df_archivo_original.loc[idx_diario, 'justificacion'] = justificacion_diario
                                    self._df_archivo_original.loc[idx_diario, 'nuevo_estado'] = nuevo_estado_diario
                                    self._df_archivo_original.loc[idx_diario, 'observaciones'] = observaciones_diario
                                    if 'resumen_pasos' in self._df_archivo_original.columns:
                                        self._df_archivo_original.loc[idx_diario, 'resumen_pasos'] = ' | '.join(resumen_pasos_diario)
                                    
                                    # Log del resultado
                                    logger.info(
                                        f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                        f"justificacion='{justificacion_arqueo if tipo_registro == 'ARQUEO' else justificacion_diario}', "
                                        f"nuevo_estado='{nuevo_estado_arqueo if tipo_registro == 'ARQUEO' else nuevo_estado_diario}', "
                                        f"ratificar_grabar='{ratificar_grabar_arqueo if tipo_registro == 'ARQUEO' else ratificar_grabar_diario}'"
                                    )
                                    
                                    continue  # Saltar el procesamiento normal
                
                # Si ya se aplicó alguna regla de ARQUEO/DIARIO igual, saltar las otras reglas
                if not regla_arqueo_sin_diario and not regla_diario_sin_arqueo and not regla_arqueo_diario_igual_faltante and not regla_arqueo_diario_igual_sobrante and not regla_arqueo_diario_diferente_faltante and not regla_arqueo_diario_diferente_sobrante and not regla_diferencias_opuestas:
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
                if not regla_arqueo_sin_diario and not regla_diario_sin_arqueo and not regla_arqueo_diario_igual_faltante and not regla_arqueo_diario_igual_sobrante and not regla_arqueo_diario_diferente_faltante and not regla_arqueo_diario_diferente_sobrante and not regla_diferencias_opuestas and not regla_provision_aplicada:
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
                        ratificar_grabar = 'No'
                        observaciones = None
                else:
                    # Si NO se encuentra movimiento en ningún lado
                    # Verificar si el registro ya tiene la clasificación de Trx_Despues12 antes de sobrescribir
                    # IMPORTANTE: Leer el valor actualizado del DataFrame, convirtiendo a string y limpiando espacios
                    observaciones_actual = None
                    if 'observaciones' in self._df_archivo_original.columns and len(indices_original) > 0:
                        obs_val = self._df_archivo_original.loc[indices_original[0], 'observaciones']
                        if pd.notna(obs_val):
                            observaciones_actual = str(obs_val).strip()
                    
                    logger.debug(f"DEBUG: Registro {indices_original[0]} (cajero {codigo_cajero}): observaciones_actual='{observaciones_actual}'")
                    
                    if observaciones_actual == 'INCIDENTES O EVENTOS MASIVOS':
                        # El registro ya fue procesado con la regla de Trx_Despues12, NO sobrescribir
                        logger.info(
                            f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya tiene clasificación Trx_Despues12. "
                            f"No se sobrescribirán los valores."
                        )
                        # IMPORTANTE: Saltar el resto del procesamiento para evitar sobrescribir
                        continue
                    else:
                        # Verificar si el registro ya tiene una clasificación aplicada (no sobrescribir)
                        observaciones_actual = self._df_archivo_original.loc[indices_original[0], 'observaciones'] if 'observaciones' in self._df_archivo_original.columns and len(indices_original) > 0 else None
                        justificacion_actual = self._df_archivo_original.loc[indices_original[0], 'justificacion'] if 'justificacion' in self._df_archivo_original.columns and len(indices_original) > 0 else None
                        nuevo_estado_actual = self._df_archivo_original.loc[indices_original[0], 'nuevo_estado'] if 'nuevo_estado' in self._df_archivo_original.columns and len(indices_original) > 0 else None
                        
                        # Verificar si ya tiene clasificación de CONTABILIZACION SOBRANTE CONTABLE
                        if observaciones_actual == 'CONTABILIZACION SOBRANTE CONTABLE':
                            # El registro ya fue procesado con la regla de CONTABILIZACION SOBRANTE CONTABLE, NO sobrescribir
                            # Asegurar que los valores sean correctos
                            logger.info(
                                f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya tiene clasificación CONTABILIZACION SOBRANTE CONTABLE. "
                                f"Asegurando valores correctos y no sobrescribiendo."
                            )
                            # Asegurar que los valores sean correctos
                            self._df_archivo_original.loc[indices_original[0], 'justificacion'] = 'Contable'
                            self._df_archivo_original.loc[indices_original[0], 'nuevo_estado'] = 'CONTABILIZACION SOBRANTE CONTABLE'
                            self._df_archivo_original.loc[indices_original[0], 'ratificar_grabar_diferencia'] = 'Si'
                            self._df_archivo_original.loc[indices_original[0], 'observaciones'] = 'CONTABILIZACION SOBRANTE CONTABLE'
                            # IMPORTANTE: Saltar el resto del procesamiento para evitar sobrescribir
                            continue
                        # Verificar si ya tiene clasificación de PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal
                        elif (observaciones_actual == 'Se le solicita arqueo a la sucursal' or 
                              observaciones_actual == 'Se le solicita arqueo a la sucursal nuevamente'):
                            # El registro ya fue procesado con la regla de PENDIENTE DE GESTION, NO sobrescribir
                            # Asegurar que los valores sean correctos
                            logger.info(
                                f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya tiene clasificación PENDIENTE DE GESTION - Se le solicita arqueo a la sucursal. "
                                f"Asegurando valores correctos y no sobrescribiendo."
                            )
                            # Asegurar que los valores sean correctos según el tipo de observaciones
                            if observaciones_actual == 'Se le solicita arqueo a la sucursal':
                                self._df_archivo_original.loc[indices_original[0], 'justificacion'] = 'Pendiente de gestion'
                                self._df_archivo_original.loc[indices_original[0], 'nuevo_estado'] = 'PENDIENTE DE GESTION'
                            else:  # 'Se le solicita arqueo a la sucursal nuevamente'
                                self._df_archivo_original.loc[indices_original[0], 'justificacion'] = 'Pendiente de gestion'
                                self._df_archivo_original.loc[indices_original[0], 'nuevo_estado'] = 'PENDIENTE DE GESTION'
                            self._df_archivo_original.loc[indices_original[0], 'ratificar_grabar_diferencia'] = 'No'
                            self._df_archivo_original.loc[indices_original[0], 'observaciones'] = observaciones_actual
                            # IMPORTANTE: Saltar el resto del procesamiento para evitar sobrescribir
                            continue
                        # Verificar si ya tiene clasificación de CRUCE DE NOVEDADES (observaciones es un NUMDOC YYYYMMDD)
                        elif (observaciones_actual and 
                              str(observaciones_actual).isdigit() and 
                              len(str(observaciones_actual)) == 8):
                            # El registro ya fue procesado con la regla de CRUCE DE NOVEDADES, NO sobrescribir
                            # Asegurar que los valores sean correctos
                            logger.info(
                                f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya tiene clasificación CRUCE DE NOVEDADES (NUMDOC: {observaciones_actual}). "
                                f"Asegurando valores correctos y no sobrescribiendo."
                            )
                            # Asegurar que los valores sean correctos
                            self._df_archivo_original.loc[indices_original[0], 'justificacion'] = 'Cruzar'
                            self._df_archivo_original.loc[indices_original[0], 'nuevo_estado'] = 'CRUCE DE NOVEDADES'
                            self._df_archivo_original.loc[indices_original[0], 'ratificar_grabar_diferencia'] = 'Reverso'
                            self._df_archivo_original.loc[indices_original[0], 'observaciones'] = str(observaciones_actual)
                            # IMPORTANTE: Saltar el resto del procesamiento para evitar sobrescribir
                            continue
                        else:
                            # VERIFICACIÓN PRIORITARIA: Verificar si el registro ya tiene una regla aplicada ANTES de aplicar REGLA GENÉRICA
                            regla_aplicada_antes_generica = None
                            if 'regla_aplicada' in self._df_archivo_original.columns and len(indices_original) > 0:
                                regla_val_antes = self._df_archivo_original.loc[indices_original[0], 'regla_aplicada']
                                if pd.notna(regla_val_antes):
                                    regla_aplicada_antes_generica = str(regla_val_antes).strip()
                            
                            if regla_aplicada_antes_generica:
                                logger.info(
                                    f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya procesado por regla '{regla_aplicada_antes_generica}'. "
                                    f"No se aplicará REGLA GENÉRICA."
                                )
                                continue
                            
                            # Ajustar el nombre de la regla según el tipo de registro
                            if tipo_registro == 'ARQUEO':
                                nombre_regla_aplicada = "REGLA GENÉRICA: Solo ARQUEO sin DIARIO - Descuadre físico (no encontrado en BD)"
                            elif tipo_registro == 'DIARIO':
                                nombre_regla_aplicada = "REGLA GENÉRICA: Solo DIARIO sin ARQUEO - Descuadre físico (no encontrado en BD)"
                            else:
                                nombre_regla_aplicada = "REGLA GENÉRICA: Descuadre físico (no encontrado en BD)"
                            valor_descuadre = abs(sobrante) if sobrante != 0 else faltante
                            tipo_descuadre = 'SOBRANTE' if sobrante != 0 else 'FALTANTE'
                            
                            if sobrante != 0:
                                justificacion = 'Contable'
                                nuevo_estado = 'CONTABILIZACION SOBRANTE CONTABLE'
                                observaciones = 'CONTABILIZACION SOBRANTE CONTABLE'
                            else:
                                justificacion = 'Fisico'
                                nuevo_estado = 'FALTANTE EN ARQUEO'
                                observaciones = None
                            
                            logger.info(
                                f"Cajero {codigo_cajero}: Aplicando {nombre_regla_aplicada}. "
                                f"Tipo: {tipo_descuadre}, Valor: ${valor_descuadre:,.0f}"
                            )
                            
                            if not resumen_pasos:
                                resumen_pasos = [f"REGLA APLICADA: {nombre_regla_aplicada}"]
                            else:
                                resumen_pasos.insert(0, f"REGLA APLICADA: {nombre_regla_aplicada}")
                            resumen_pasos.append(f"1. Identificado: {tipo_descuadre} de ${valor_descuadre:,.0f}")
                            resumen_pasos.append(f"2. Buscado movimiento en NACIONAL cuenta 110505075")
                            resumen_pasos.append(f"3. ✗ No encontrado en NACIONAL")
                            resumen_pasos.append(f"4. Buscado movimiento en BD SOBRANTES/FALTANTES")
                            resumen_pasos.append(f"5. ✗ No encontrado en BD")
                            if sobrante != 0:
                                resumen_pasos.append(f"6. Clasificación: CONTABILIZACION SOBRANTE CONTABLE (descuadre físico)")
                            else:
                                resumen_pasos.append(f"6. Clasificación: FALTANTE EN ARQUEO (descuadre físico)")
                            resumen_pasos.append(f"7. Ratificar grabar: Si")
                            
                            ratificar_grabar = 'Si'
                            
                            # VERIFICACIÓN FINAL: Antes de actualizar, verificar si el registro ya fue clasificado con reglas específicas
                            # PRIMERO: Verificar el indicador regla_aplicada
                            regla_aplicada_final = None
                            if 'regla_aplicada' in self._df_archivo_original.columns and len(indices_original) > 0:
                                regla_val_final = self._df_archivo_original.loc[indices_original[0], 'regla_aplicada']
                                if pd.notna(regla_val_final):
                                    regla_aplicada_final = str(regla_val_final).strip()
                            
                            if regla_aplicada_final:
                                logger.info(
                                    f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya procesado por regla '{regla_aplicada_final}'. "
                                    f"No se sobrescribirán los valores con REGLA GENÉRICA."
                                )
                                continue
                            
                            observaciones_actual_final = None
                            nuevo_estado_actual_final = None
                            if 'observaciones' in self._df_archivo_original.columns and len(indices_original) > 0:
                                obs_val_final = self._df_archivo_original.loc[indices_original[0], 'observaciones']
                                if pd.notna(obs_val_final):
                                    observaciones_actual_final = str(obs_val_final).strip()
                            if 'nuevo_estado' in self._df_archivo_original.columns and len(indices_original) > 0:
                                estado_val_final = self._df_archivo_original.loc[indices_original[0], 'nuevo_estado']
                                if pd.notna(estado_val_final):
                                    nuevo_estado_actual_final = str(estado_val_final).strip().upper()
                            
                            # Verificar si ya tiene clasificación de PENDIENTE DE GESTION (puede ser de SOBRANTE > $500M u otras reglas)
                            # Comparación case-insensitive para el estado
                            if (observaciones_actual_final == 'Se le solicita arqueo a la sucursal' or 
                                observaciones_actual_final == 'Se le solicita arqueo a la sucursal nuevamente') and \
                               (nuevo_estado_actual_final == 'PENDIENTE DE GESTION' or 
                                nuevo_estado_actual_final == 'PENDIENTE GESTION' or
                                nuevo_estado_actual_final == 'PENDIENTE DE GESTION'):
                                logger.info(
                                    f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya tiene clasificación PENDIENTE DE GESTION "
                                    f"(observaciones: {observaciones_actual_final}). No se sobrescribirán los valores con REGLA GENÉRICA."
                                )
                                continue
                            
                            if observaciones_actual_final == 'INCIDENTES O EVENTOS MASIVOS':
                                # El registro ya fue procesado con la regla de Trx_Despues12, NO sobrescribir
                                logger.info(
                                    f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya tiene clasificación Trx_Despues12. "
                                    f"No se sobrescribirán los valores en la sección final."
                                )
                                continue
                            elif observaciones_actual_final and str(observaciones_actual_final).strip().replace('.0', '').isdigit() and len(str(observaciones_actual_final).strip().replace('.0', '')) == 8:
                                # El registro ya fue procesado con la regla de CRUCE DE NOVEDADES, NO sobrescribir
                                logger.info(
                                    f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya tiene clasificación CRUCE DE NOVEDADES (NUMDOC: {observaciones_actual_final}). "
                                    f"No se sobrescribirán los valores en la sección final."
                                )
                                continue

                            # Actualizar el registro con la clasificación determinada
                            if justificacion is not None and nuevo_estado is not None:
                                # Verificar que el registro no haya sido procesado ya
                                regla_aplicada_actual = None
                                if len(indices_original) > 0 and 'regla_aplicada' in self._df_archivo_original.columns:
                                    regla_aplicada_actual = self._df_archivo_original.loc[indices_original[0], 'regla_aplicada']
                                
                                if pd.notna(regla_aplicada_actual) and str(regla_aplicada_actual).strip():
                                    logger.info(
                                        f"Registro {indices_original[0]} (cajero {codigo_cajero}): Ya procesado por regla '{regla_aplicada_actual}'. "
                                        f"No se sobrescribirán los valores con REGLA GENÉRICA."
                                    )
                                    continue
                                
                                self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                                self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                                if 'ratificar_grabar_diferencia' in self._df_archivo_original.columns:
                                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                                if 'observaciones' in self._df_archivo_original.columns:
                                    # IMPORTANTE: Solo actualizar observaciones si no está vacío Y el registro no tiene ya una clasificación especial
                                    observaciones_actual_antes = None
                                    if len(indices_original) > 0:
                                        obs_val_antes = self._df_archivo_original.loc[indices_original[0], 'observaciones']
                                        if pd.notna(obs_val_antes):
                                            observaciones_actual_antes = str(obs_val_antes).strip()
                                    
                                    # No sobrescribir si ya tiene clasificación especial
                                    if observaciones_actual_antes != 'INCIDENTES O EVENTOS MASIVOS' and not (observaciones_actual_antes and str(observaciones_actual_antes).strip().replace('.0', '').isdigit() and len(str(observaciones_actual_antes).strip().replace('.0', '')) == 8):
                                        if observaciones:
                                            self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                                
                                # Marcar registro como procesado
                                if nombre_regla_aplicada:
                                    self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                                elif justificacion and nuevo_estado:
                                    # Si no hay nombre_regla_aplicada específico, usar una descripción genérica
                                    regla_desc = f"REGLA GENÉRICA - {nuevo_estado}"
                                    self._marcar_registro_procesado(indices_original, regla_desc)
                                
                                # Marcar registro como procesado
                                if nombre_regla_aplicada:
                                    self._marcar_registro_procesado(indices_original, nombre_regla_aplicada)
                                elif justificacion and nuevo_estado:
                                    # Si no hay nombre_regla_aplicada específico, usar una descripción genérica
                                    regla_desc = f"REGLA GENÉRICA - {nuevo_estado}"
                                    self._marcar_registro_procesado(indices_original, regla_desc)
                                
                                if 'resumen_pasos' in self._df_archivo_original.columns and resumen_pasos:
                                    self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                                actualizados += len(indices_original)
                                
                                # Log del resultado para todos los registros procesados
                                logger.info(
                                    f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                                    f"justificacion='{justificacion}', nuevo_estado='{nuevo_estado}', "
                                    f"ratificar_grabar='{ratificar_grabar}'"
                                )
                
                # CASO POR DEFECTO: Si no se aplicó ninguna regla, clasificar como "PENDIENTE DE GESTION"
                if justificacion is None or nuevo_estado is None:
                    logger.warning(
                        f"Cajero {codigo_cajero} (tipo {tipo_registro}): No se aplicó ninguna regla. "
                        f"Clasificando como 'PENDIENTE DE GESTION'"
                    )
                    justificacion = 'Pendiente de gestion'
                    nuevo_estado = 'PENDIENTE DE GESTION'
                    ratificar_grabar = 'No'
                    observaciones = 'Este caso requiere la supervisión de personal encargado.'
                    resumen_pasos.append("1. Verificado: No se aplicó ninguna regla implementada")
                    resumen_pasos.append("2. Clasificación: Pendiente de gestion")
                    
                    # Actualizar el registro
                    self._df_archivo_original.loc[indices_original, 'ratificar_grabar_diferencia'] = ratificar_grabar
                    self._df_archivo_original.loc[indices_original, 'justificacion'] = justificacion
                    self._df_archivo_original.loc[indices_original, 'nuevo_estado'] = nuevo_estado
                    self._df_archivo_original.loc[indices_original, 'observaciones'] = observaciones
                    if 'resumen_pasos' in self._df_archivo_original.columns:
                        self._df_archivo_original.loc[indices_original, 'resumen_pasos'] = ' | '.join(resumen_pasos)
                    actualizados += len(indices_original)
                    
                    # Log del resultado también para el caso por defecto
                    logger.info(
                        f"Cajero {codigo_cajero} (tipo {tipo_registro}): "
                        f"justificacion='{justificacion}', nuevo_estado='{nuevo_estado}', "
                        f"ratificar_grabar='{ratificar_grabar}'"
                )
        
        # REGLA ADICIONAL: Manejar múltiples registros DIARIO del mismo cajero
        logger.info("Verificando múltiples registros DIARIO del mismo cajero...")
        if 'codigo_cajero' in self._df_archivo_original.columns and 'tipo_registro' in self._df_archivo_original.columns:
            # Obtener todos los registros DIARIO
            registros_diario = self._df_archivo_original[
                self._df_archivo_original['tipo_registro'] == 'DIARIO'
            ].copy()
            
            if len(registros_diario) > 0:
                # Agrupar por cajero
                cajeros_diario = registros_diario['codigo_cajero'].dropna().unique()
                
                for cajero in cajeros_diario:
                    registros_cajero = registros_diario[
                        registros_diario['codigo_cajero'] == cajero
                    ]
                    
                    # Solo procesar si hay más de un registro DIARIO para este cajero
                    if len(registros_cajero) > 1:
                        logger.info(
                            f"Cajero {cajero}: Se encontraron {len(registros_cajero)} registros DIARIO. "
                            f"Aplicando reglas para múltiples registros..."
                        )
                        
                        indices_cajero = registros_cajero.index.tolist()
                        
                        # Obtener valores actuales de los registros
                        ratificar_grabar_vals = registros_cajero['ratificar_grabar_diferencia'].fillna('').astype(str)
                        nuevo_estado_vals = registros_cajero['nuevo_estado'].fillna('').astype(str)
                        sobrantes_vals = registros_cajero['sobrantes'].fillna(0)
                        
                        # Verificar si hay sobrantes >= 10M
                        sobrantes_abs = [abs(limpiar_valor_numerico(s)) for s in sobrantes_vals]
                        max_sobrante = max(sobrantes_abs) if sobrantes_abs else 0
                        
                        # REGLA 1: Si hay sobrante >= 10M, todos a "PENDIENTE DE GESTION" y No grabar
                        if max_sobrante >= 10000000:
                            logger.info(
                                f"Cajero {cajero}: Sobrante >= $10M (${max_sobrante:,.0f}). "
                                f"Cambiando todos los registros a 'PENDIENTE DE GESTION' y 'No' grabar"
                            )
                            
                            for idx in indices_cajero:
                                self._df_archivo_original.loc[idx, 'justificacion'] = 'Pendiente de gestion'
                                self._df_archivo_original.loc[idx, 'nuevo_estado'] = 'PENDIENTE DE GESTION'
                                self._df_archivo_original.loc[idx, 'ratificar_grabar_diferencia'] = 'No'
                                if 'observaciones' in self._df_archivo_original.columns:
                                    self._df_archivo_original.loc[idx, 'observaciones'] = 'Este caso requiere la supervisión de personal encargado.'
                        
                        # REGLA 2: Si hay ratificar_grabar = 'Reverso' (para faltantes), todos a 'No' y revisión manual
                        # EXCEPTO si la justificación es 'Cruzar' o 'Cruzar' (son cruces de novedades creados por la regla)
                        elif any(ratificar_grabar_vals.str.contains('Reverso', case=False, na=False)):
                            # Verificar si alguno tiene justificación 'Cruzar' o 'Cruzar' (cruces de novedades)
                            justificacion_vals = registros_cajero['justificacion'].fillna('').astype(str)
                            tiene_cruce_novedades = any(justificacion_vals.str.contains('Cruzar', case=False, na=False))
                            
                            if tiene_cruce_novedades:
                                logger.info(
                                    f"Cajero {cajero}: Se encontró 'Reverso' pero con justificación 'Cruzar' (CRUCE DE NOVEDADES). "
                                    f"Manteniendo clasificación de cruce de novedades."
                                )
                                # No cambiar nada, mantener la clasificación de cruce de novedades
                            else:
                                logger.info(
                                    f"Cajero {cajero}: Se encontró 'Reverso' para faltante. "
                                    f"Cambiando todos los registros a 'No' grabar y 'PENDIENTE DE GESTION'"
                                )
                                
                                for idx in indices_cajero:
                                    self._df_archivo_original.loc[idx, 'justificacion'] = 'Pendiente de gestion'
                                    self._df_archivo_original.loc[idx, 'nuevo_estado'] = 'PENDIENTE DE GESTION'
                                    self._df_archivo_original.loc[idx, 'ratificar_grabar_diferencia'] = 'No'
                                    if 'observaciones' in self._df_archivo_original.columns:
                                        self._df_archivo_original.loc[idx, 'observaciones'] = 'Este caso requiere la supervisión de personal encargado.'
                        
                        # REGLA 3: Si hay sobrante < 10M y ratificar_grabar = 'Si', solo uno debe tener 'Si'
                        elif any(ratificar_grabar_vals.str.contains('Si', case=False, na=False)):
                            # Contar cuántos tienen 'Si'
                            indices_con_si = [
                                idx for idx, val in zip(indices_cajero, ratificar_grabar_vals)
                                if 'Si' in str(val)
                            ]
                            
                            if len(indices_con_si) > 1:
                                # Obtener fechas_arqueo de los registros con 'Si' para identificar el más reciente
                                fechas_con_si = []
                                for idx in indices_con_si:
                                    fecha_arqueo_val = self._df_archivo_original.loc[idx, 'fecha_arqueo']
                                    # Convertir a datetime si es necesario
                                    if pd.notna(fecha_arqueo_val):
                                        if isinstance(fecha_arqueo_val, str):
                                            try:
                                                fecha_dt = pd.to_datetime(fecha_arqueo_val)
                                            except:
                                                fecha_dt = None
                                        elif isinstance(fecha_arqueo_val, pd.Timestamp):
                                            fecha_dt = fecha_arqueo_val
                                        else:
                                            fecha_dt = pd.to_datetime(fecha_arqueo_val)
                                    else:
                                        fecha_dt = None
                                    fechas_con_si.append((idx, fecha_dt))
                                
                                # Identificar el registro con la fecha más reciente
                                fechas_validas = [(idx, fecha) for idx, fecha in fechas_con_si if fecha is not None]
                                
                                if fechas_validas:
                                    # Ordenar por fecha descendente (más reciente primero)
                                    fechas_validas.sort(key=lambda x: x[1], reverse=True)
                                    idx_mas_reciente = fechas_validas[0][0]
                                    fecha_mas_reciente = fechas_validas[0][1]
                                    
                                    logger.info(
                                        f"Cajero {cajero}: Se encontraron {len(indices_con_si)} registros con 'Si'. "
                                        f"Manteniendo solo el más reciente (fecha: {fecha_mas_reciente.strftime('%Y-%m-%d') if fecha_mas_reciente else 'N/A'}) en 'Si', el resto en 'No'"
                                    )
                                    
                                    # Cambiar todos a 'No' excepto el más reciente
                                    for idx in indices_con_si:
                                        if idx != idx_mas_reciente:
                                            self._df_archivo_original.loc[idx, 'ratificar_grabar_diferencia'] = 'No'
                                            logger.info(
                                                f"Cajero {cajero}: Registro índice {idx} cambiado de 'Si' a 'No' "
                                                f"(múltiples registros DIARIO del mismo cajero, manteniendo solo el más reciente)"
                                            )
                                else:
                                    # Si no hay fechas válidas, mantener el primero (fallback)
                                    logger.warning(
                                        f"Cajero {cajero}: No se pudieron obtener fechas válidas. "
                                        f"Manteniendo el primer registro en 'Si' como fallback"
                                    )
                                    for i, idx in enumerate(indices_con_si):
                                        if i > 0:  # Todos excepto el primero
                                            self._df_archivo_original.loc[idx, 'ratificar_grabar_diferencia'] = 'No'
                                            logger.info(
                                                f"Cajero {cajero}: Registro índice {idx} cambiado de 'Si' a 'No' "
                                                f"(múltiples registros DIARIO del mismo cajero)"
                )
        
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

