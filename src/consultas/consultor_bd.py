"""
Módulo para consultar movimientos en base de datos mediante ODBC.
Implementa consultas a la base de datos NACIONAL usando el patrón AdminBD.
"""

import logging
import pandas as pd
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

from src.consultas.admin_bd import AdminBDNacional

logger = logging.getLogger(__name__)


class ConsultorBD:
    """
    Clase para consultar movimientos en base de datos mediante ODBC.
    Usa AdminBDNacional para conectarse a la base de datos NACIONAL.
    """
    
    def __init__(self, usuario: str, clave: str):
        """
        Inicializa el consultor de base de datos.
        
        Args:
            usuario: Usuario para la conexión a la BD NACIONAL
            clave: Contraseña para la conexión a la BD NACIONAL
        """
        self.usuario = usuario
        self.clave = clave
        self.admin_bd: Optional[AdminBDNacional] = None
        
        if usuario and clave:
            self.admin_bd = AdminBDNacional(usuario, clave)
            logger.info("ConsultorBD inicializado con credenciales")
        else:
            logger.warning("ConsultorBD inicializado sin credenciales")
    
    def conectar(self):
        """Establece la conexión a la base de datos."""
        if not self.admin_bd:
            raise ValueError("No se ha configurado el administrador de BD")
        return self.admin_bd.conectar()
    
    def consultar_movimientos_nacional(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        valor_descuadre: float,
        cuenta: int = 110505075,
        codofi_excluir: int = 976,
        nrocmp: int = 770500,
        solo_dia_arqueo: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta movimientos en la base de datos nacional.
        
        Busca movimientos desde la fecha del arqueo hacia atrás, máximo 1 mes.
        Prioriza los movimientos más recientes (ordenados por fecha DESC).
        Si encuentra múltiples movimientos con el mismo valor, retorna el más reciente.
        
        Query:
        SELECT ANOELB, MESELB, DIAELB, CODOFI,
               (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) AS CUENTA,
               NIT, NUMDOC, NROCMP,
               (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, VALOR
        FROM gcolibranl.gcoffmvint
        WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta}))
          AND CODOFI <> {codofi_excluir}
          AND NROCMP = {nrocmp}
          AND NIT = {codigo_cajero}
          AND (ANOELB*10000+MESELB*100+DIAELB) BETWEEN {fecha_inicio} AND {fecha_fin}
        ORDER BY FECHA DESC
        
        Args:
            codigo_cajero: Código del cajero a buscar (filtro por NIT)
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            valor_descuadre: Valor del descuadre (ya viene con el signo correcto para BD:
                          - FALTANTE: negativo (ej: -100000)
                          - SOBRANTE: positivo (ej: 100000))
            cuenta: Número de cuenta (default: 110505075)
            codofi_excluir: Código de oficina a excluir (default: 976)
            nrocmp: Número de comprobante (default: 770500)
        
        Returns:
            Diccionario con los datos del movimiento más reciente encontrado o None
        """
        if not self.admin_bd:
            logger.error("No se ha configurado el administrador de BD")
            return None
        
        try:
            # Formatear fecha de YYYY-MM-DD a YYYYMMDD (entero)
            fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
            fecha_formateada = int(fecha_obj.strftime('%Y%m%d'))
            
            # Construir la consulta SQL optimizada
            if solo_dia_arqueo:
                # Buscar SOLO el día del arqueo (sin rango)
                fecha_inicio = fecha_formateada
                fecha_fin = fecha_formateada
            else:
                # Busca desde la fecha del arqueo hacia atrás, máximo 1 mes
                # Ejemplo: Si arqueo es 2025-12-01, busca desde 2025-11-01 hasta 2025-12-01
                fecha_fin = fecha_formateada  # Fecha de arqueo (límite superior)
                
                # Calcular fecha de inicio (1 mes hacia atrás)
            if fecha_obj.month == 1:
                # Si es enero, el mes anterior es diciembre del año anterior
                mes_anterior = 12
                anio_anterior = fecha_obj.year - 1
            else:
                mes_anterior = fecha_obj.month - 1
                anio_anterior = fecha_obj.year
            
                # Usar el mismo día del mes anterior (o el último día del mes si no existe)
                try:
                    fecha_inicio_obj = datetime(anio_anterior, mes_anterior, fecha_obj.day)
                except ValueError:
                    # Si el día no existe en el mes anterior (ej: 31 de marzo -> 31 de febrero no existe)
                    # Usar el último día del mes anterior
                    if mes_anterior == 2:
                        # Febrero: verificar si es año bisiesto
                        if anio_anterior % 4 == 0 and (anio_anterior % 100 != 0 or anio_anterior % 400 == 0):
                            ultimo_dia = 29
                        else:
                            ultimo_dia = 28
                    elif mes_anterior in [4, 6, 9, 11]:
                        ultimo_dia = 30
                    else:
                        ultimo_dia = 31
                    fecha_inicio_obj = datetime(anio_anterior, mes_anterior, ultimo_dia)
                
                fecha_inicio = int(fecha_inicio_obj.strftime('%Y%m%d'))  # Fecha límite inferior (1 mes antes)
            
            consulta = f"""
            SELECT  ANOELB, 
                    MESELB, 
                    DIAELB, 
                    CODOFI, 
                    (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) AS CUENTA, 
                    NIT, 
                    NUMDOC, 
                    NROCMP, 
                    (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, 
                    VALOR 
            FROM gcolibranl.gcoffmvint 
            WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta})) 
              AND CODOFI <> {codofi_excluir}
              AND NROCMP = {nrocmp}
              AND NIT = {codigo_cajero}
              AND (ANOELB*10000+MESELB*100+DIAELB) BETWEEN {fecha_inicio} AND {fecha_fin}
            ORDER BY FECHA DESC
            """
            
            if solo_dia_arqueo:
                logger.debug(
                    f"Ejecutando consulta para cajero {codigo_cajero}, fecha arqueo {fecha_arqueo} (SOLO DÍA DEL ARQUEO)"
                )
            else:
                fecha_inicio_obj = datetime.strptime(str(fecha_inicio), '%Y%m%d')
                logger.debug(
                    f"Ejecutando consulta para cajero {codigo_cajero}, fecha arqueo {fecha_arqueo}, "
                    f"rango: {fecha_inicio_obj.strftime('%Y-%m-%d')} a {fecha_obj.strftime('%Y-%m-%d')}"
                )
            
            # Ejecutar consulta
            df = self.admin_bd.consultar(consulta)
            
            if df.empty:
                if solo_dia_arqueo:
                    logger.debug(
                        f"No se encontraron movimientos para cajero {codigo_cajero}, "
                        f"fecha arqueo {fecha_arqueo} (SOLO DÍA DEL ARQUEO)"
                    )
                else:
                    fecha_inicio_obj = datetime.strptime(str(fecha_inicio), '%Y%m%d')
                    logger.debug(
                        f"No se encontraron movimientos para cajero {codigo_cajero}, "
                        f"fecha arqueo {fecha_arqueo}, rango: {fecha_inicio_obj.strftime('%Y-%m-%d')} a {fecha_obj.strftime('%Y-%m-%d')}"
                    )
                return None
            
            # Buscar el que coincida con el valor (exacto o por valor absoluto)
            # El valor_descuadre ya viene con el signo correcto (negativo para sobrantes, positivo para faltantes)
            # La consulta ya está ordenada por FECHA DESC, así que tomamos el más reciente que coincida
            # Primero intentar coincidencia exacta
            coincidencia_valor = df[df['VALOR'] == valor_descuadre]
            if len(coincidencia_valor) > 0:
                df = coincidencia_valor
            else:
                # Si no encuentra con signo exacto, intentar con valor absoluto
                coincidencia_valor = df[df['VALOR'].abs() == abs(valor_descuadre)]
                if len(coincidencia_valor) > 0:
                    df = coincidencia_valor
                else:
                    # Si no hay coincidencia de valor, retornar None
                    # Esto es importante porque un movimiento que no coincide con el valor no es válido
                    logger.debug(
                        f"Movimiento encontrado en BD pero valor no coincide: "
                        f"cajero={codigo_cajero}, fecha={fecha_arqueo}, "
                        f"valor_buscado={valor_descuadre}, valores_encontrados={df['VALOR'].tolist()}"
                    )
                    return None
            
            # Asegurar que esté ordenado por fecha DESC (más reciente primero)
            df = df.sort_values('FECHA', ascending=False)
            
            # Convertir el primer resultado a diccionario (el más reciente que coincida)
            resultado = df.iloc[0].to_dict()
            
            fecha_movimiento_num = resultado.get('FECHA')
            fecha_movimiento_str = 'N/A'
            if fecha_movimiento_num:
                try:
                    fecha_mov_int = int(float(fecha_movimiento_num))
                    anio_mov = fecha_mov_int // 10000
                    mes_mov = (fecha_mov_int % 10000) // 100
                    dia_mov = fecha_mov_int % 100
                    fecha_movimiento_str = f"{anio_mov:04d}-{mes_mov:02d}-{dia_mov:02d}"
                except:
                    pass
            
            logger.info(
                f"Movimiento encontrado en BD: cajero={codigo_cajero}, "
                f"fecha arqueo={fecha_arqueo}, fecha movimiento={fecha_movimiento_str}, "
                f"valor={resultado.get('VALOR')}"
            )
            
            return resultado
        
        except Exception as e:
            logger.error(f"Error al consultar movimientos en BD: {e}")
            return None
    
    def consultar_provision(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        valor_sobrante: float,
        cuenta: int = 110505075,
        codofi_excluir: int = 976,
        nrocmp_provision: int = 810291
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta provisiones (NROCMP = 810291) en la base de datos nacional
        para el día anterior al arqueo.
        
        Esta consulta se usa para la regla de sobrantes exagerados (>= 10M y múltiplos de 100k).
        
        Args:
            codigo_cajero: Código del cajero a buscar (filtro por NIT)
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            valor_sobrante: Valor del sobrante (debe ser negativo, ej: -195000000)
            cuenta: Número de cuenta (default: 110505075)
            codofi_excluir: Código de oficina a excluir (default: 976)
            nrocmp_provision: Número de comprobante de provisión (default: 810291)
        
        Returns:
            Diccionario con los datos encontrados o None
        """
        if not self.admin_bd:
            logger.error("No se ha configurado el administrador de BD")
            return None
        
        try:
            # Calcular fecha del día anterior
            fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
            fecha_anterior = fecha_obj - timedelta(days=1)
            fecha_anterior_str = fecha_anterior.strftime('%Y-%m-%d')
            fecha_anterior_formateada = int(fecha_anterior.strftime('%Y%m%d'))
            
            # Construir la consulta SQL
            consulta = f"""
            SELECT  ANOELB, 
                    MESELB, 
                    DIAELB, 
                    CODOFI, 
                    (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) AS CUENTA, 
                    NIT, 
                    NUMDOC, 
                    NROCMP, 
                    (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, 
                    VALOR 
            FROM gcolibranl.gcoffmvint 
            WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta})) 
              AND CODOFI <> {codofi_excluir}
              AND NROCMP = {nrocmp_provision}
              AND NIT = {codigo_cajero}
              AND (ANOELB*10000+MESELB*100+DIAELB) = {fecha_anterior_formateada}
            ORDER BY FECHA DESC
            """
            
            logger.debug(
                f"Consultando provisión para cajero {codigo_cajero}, "
                f"fecha anterior {fecha_anterior_str}, valor sobrante {valor_sobrante}"
            )
            
            # Ejecutar consulta
            df = self.admin_bd.consultar(consulta)
            
            if df.empty:
                logger.debug(
                    f"No se encontró provisión para cajero {codigo_cajero}, "
                    f"fecha anterior {fecha_anterior_str}"
                )
                return None
            
            # Si hay múltiples resultados, tomar el que tenga el valor más cercano al sobrante
            # (pero debe ser <= al sobrante en valor absoluto)
            valor_sobrante_abs = abs(valor_sobrante)
            
            # Filtrar por valores que sean <= al sobrante
            df_filtrado = df[df['VALOR'].abs() <= valor_sobrante_abs]
            
            if df_filtrado.empty:
                logger.debug(
                    f"No se encontró provisión con valor <= {valor_sobrante_abs} "
                    f"para cajero {codigo_cajero}"
                )
                return None
            
            # Si hay múltiples, tomar el de mayor valor (más cercano al sobrante)
            df_filtrado = df_filtrado.sort_values('VALOR', key=lambda x: x.abs(), ascending=False)
            resultado = df_filtrado.iloc[0].to_dict()
            
            logger.info(
                f"Provisión encontrada en BD: cajero={codigo_cajero}, "
                f"fecha anterior={fecha_anterior_str}, valor={resultado.get('VALOR')}"
            )
            
            return resultado
        
        except Exception as e:
            logger.error(f"Error al consultar provisión en BD: {e}")
            return None
    
    def consultar_provision_mismo_dia(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        cuenta: int = 110505075,
        codofi_excluir: int = 976,
        nrocmp_provision: int = 810291
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta provisiones (NROCMP = 810291) en la base de datos nacional
        para el mismo día del arqueo.
        
        Esta consulta se usa cuando hay un sobrante >= $10M en ARQUEO y un faltante en DIARIO.
        La provisión del mismo día explica la diferencia entre ambos.
        
        Args:
            codigo_cajero: Código del cajero a buscar (filtro por NIT)
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            cuenta: Número de cuenta (default: 110505075)
            codofi_excluir: Código de oficina a excluir (default: 976)
            nrocmp_provision: Número de comprobante de provisión (default: 810291)
        
        Returns:
            Diccionario con los datos encontrados o None
        """
        if not self.admin_bd:
            logger.error("No se ha configurado el administrador de BD")
            return None
        
        try:
            # Formatear fecha del arqueo
            fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
            fecha_formateada = int(fecha_obj.strftime('%Y%m%d'))
            
            # Construir la consulta SQL
            consulta = f"""
            SELECT  ANOELB, 
                    MESELB, 
                    DIAELB, 
                    CODOFI, 
                    (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) AS CUENTA, 
                    NIT, 
                    NUMDOC, 
                    NROCMP, 
                    (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, 
                    VALOR 
            FROM gcolibranl.gcoffmvint 
            WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta})) 
              AND CODOFI <> {codofi_excluir}
              AND NROCMP = {nrocmp_provision}
              AND NIT = {codigo_cajero}
              AND (ANOELB*10000+MESELB*100+DIAELB) = {fecha_formateada}
            ORDER BY FECHA DESC
            """
            
            logger.debug(
                f"Consultando provisión mismo día para cajero {codigo_cajero}, "
                f"fecha arqueo {fecha_arqueo}"
            )
            
            # Ejecutar consulta
            df = self.admin_bd.consultar(consulta)
            
            if df.empty:
                logger.debug(
                    f"No se encontró provisión mismo día para cajero {codigo_cajero}, "
                    f"fecha {fecha_arqueo}"
                )
                return None
            
            # Si hay múltiples resultados, tomar el de mayor valor
            df = df.sort_values('VALOR', key=lambda x: x.abs(), ascending=False)
            resultado = df.iloc[0].to_dict()
            
            logger.info(
                f"Provisión mismo día encontrada en BD: cajero={codigo_cajero}, "
                f"fecha={fecha_arqueo}, valor={resultado.get('VALOR')}"
            )
            
            return resultado
        
        except Exception as e:
            logger.error(f"Error al consultar provisión mismo día en BD: {e}")
            return None
    
    def consultar_movimientos_negativos_mismo_dia(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        cuenta: int = 110505075,
        codofi_excluir: int = 976,
        nrocmps: list = [770500, 810291]
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta movimientos negativos (VALOR < 0) en la base de datos nacional
        para el mismo día del arqueo, con comprobantes 770500 o 810291.
        Suma todos los valores encontrados.
        
        Esta consulta se usa para la regla de ARQUEO y DIARIO con diferentes diferencias (FALTANTE).
        
        Args:
            codigo_cajero: Código del cajero a buscar (filtro por NIT)
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            cuenta: Número de cuenta (default: 110505075)
            codofi_excluir: Código de oficina a excluir (default: 976)
            nrocmps: Lista de números de comprobante a buscar (default: [770500, 810291])
        
        Returns:
            Diccionario con la suma de todos los movimientos encontrados y la lista de movimientos, o None
        """
        if not self.admin_bd:
            logger.error("No se ha configurado el administrador de BD")
            return None
        
        try:
            # Formatear fecha del arqueo
            fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
            fecha_formateada = int(fecha_obj.strftime('%Y%m%d'))
            
            # Construir la lista de comprobantes para la consulta SQL
            nrocmps_str = ','.join([str(nrocmp) for nrocmp in nrocmps])
            
            # Construir la consulta SQL
            consulta = f"""
            SELECT  ANOELB, 
                    MESELB, 
                    DIAELB, 
                    CODOFI, 
                    (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) AS CUENTA, 
                    NIT, 
                    NUMDOC, 
                    NROCMP, 
                    (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, 
                    VALOR 
            FROM gcolibranl.gcoffmvint 
            WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta})) 
              AND CODOFI <> {codofi_excluir}
              AND NROCMP IN ({nrocmps_str})
              AND NIT = {codigo_cajero}
              AND (ANOELB*10000+MESELB*100+DIAELB) = {fecha_formateada}
              AND VALOR < 0
            ORDER BY FECHA DESC, VALOR ASC
            """
            
            logger.debug(
                f"Consultando movimientos negativos mismo día para cajero {codigo_cajero}, "
                f"fecha arqueo {fecha_arqueo}, comprobantes {nrocmps_str}"
            )
            
            # Ejecutar consulta
            df = self.admin_bd.consultar(consulta)
            
            if df.empty:
                logger.debug(
                    f"No se encontraron movimientos negativos mismo día para cajero {codigo_cajero}, "
                    f"fecha {fecha_arqueo}"
                )
                return None
            
            # Sumar todos los valores negativos encontrados
            suma_valores = df['VALOR'].sum()
            movimientos = df.to_dict('records')
            
            logger.info(
                f"Movimientos negativos mismo día encontrados en BD: cajero={codigo_cajero}, "
                f"fecha={fecha_arqueo}, cantidad={len(movimientos)}, suma={suma_valores:,.0f}"
            )
            
            return {
                'encontrado': True,
                'suma': abs(suma_valores),  # Retornar valor absoluto (positivo)
                'suma_original': suma_valores,  # Suma original (negativa)
                'movimientos': movimientos,
                'total_movimientos': len(movimientos)
            }
        
        except Exception as e:
            logger.error(f"Error al consultar movimientos negativos mismo día en BD: {e}")
            return None
    
    def consultar_cuenta_sobrantes(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        valor_descuadre: float,
        cuenta: int = 279510020,
        codofi_excluir: int = 976
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta movimientos en la cuenta de sobrantes (279510020) en la base de datos nacional
        para el mismo día del arqueo.
        
        Esta consulta se usa para la regla de ARQUEO y DIARIO con la misma diferencia (FALTANTE).
        
        Args:
            codigo_cajero: Código del cajero a buscar (filtro por NIT)
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            valor_descuadre: Valor del descuadre (faltante, positivo)
            cuenta: Número de cuenta de sobrantes (default: 279510020)
            codofi_excluir: Código de oficina a excluir (default: 976)
        
        Returns:
            Diccionario con los datos encontrados o None
        """
        if not self.admin_bd:
            logger.error("No se ha configurado el administrador de BD")
            return None
        
        try:
            # Formatear fecha del arqueo
            fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
            fecha_formateada = int(fecha_obj.strftime('%Y%m%d'))
            
            # Construir la consulta SQL
            consulta = f"""
            SELECT  ANOELB, 
                    MESELB, 
                    DIAELB, 
                    CODOFI, 
                    (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) AS CUENTA, 
                    NIT, 
                    NUMDOC, 
                    NROCMP, 
                    (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, 
                    VALOR 
            FROM gcolibranl.gcoffmvint 
            WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta})) 
              AND CODOFI <> {codofi_excluir}
              AND NIT = {codigo_cajero}
              AND (ANOELB*10000+MESELB*100+DIAELB) = {fecha_formateada}
            ORDER BY FECHA DESC
            """
            
            logger.debug(
                f"Consultando cuenta sobrantes {cuenta} para cajero {codigo_cajero}, "
                f"fecha arqueo {fecha_arqueo}, valor descuadre {valor_descuadre}"
            )
            
            # Ejecutar consulta
            df = self.admin_bd.consultar(consulta)
            
            if df.empty:
                logger.debug(
                    f"No se encontraron movimientos en cuenta {cuenta} para cajero {codigo_cajero}, "
                    f"fecha {fecha_arqueo}"
                )
                return None
            
            # Buscar el que coincida con el valor (exacto o por valor absoluto)
            coincidencia_valor = df[df['VALOR'].abs() == abs(valor_descuadre)]
            if len(coincidencia_valor) > 0:
                df = coincidencia_valor
            else:
                # Si no hay coincidencia exacta, retornar None
                logger.debug(
                    f"Movimiento encontrado en cuenta {cuenta} pero valor no coincide: "
                    f"cajero={codigo_cajero}, fecha={fecha_arqueo}, "
                    f"valor_buscado={valor_descuadre}, valores_encontrados={df['VALOR'].tolist()}"
                )
                return None
            
            # Convertir el primer resultado a diccionario
            resultado = df.iloc[0].to_dict()
            
            logger.info(
                f"Movimiento encontrado en cuenta sobrantes {cuenta}: cajero={codigo_cajero}, "
                f"fecha={fecha_arqueo}, valor={resultado.get('VALOR')}"
            )
            
            return resultado
        
        except Exception as e:
            logger.error(f"Error al consultar cuenta sobrantes en BD: {e}")
            return None
    
    def consultar_cuenta_sobrantes_dias_anteriores(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        valor_descuadre: float,
        cuenta: int = 279510020,
        codofi_excluir: int = 976,
        dias_anteriores: int = 30
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta movimientos en la cuenta de sobrantes (279510020) en días anteriores al arqueo.
        
        Esta consulta se usa para la regla de ARQUEO y DIARIO con la misma diferencia (FALTANTE)
        cuando no se encuentra movimiento el día del arqueo.
        
        Args:
            codigo_cajero: Código del cajero a buscar (filtro por NIT)
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            valor_descuadre: Valor del descuadre (faltante, positivo)
            cuenta: Número de cuenta de sobrantes (default: 279510020)
            codofi_excluir: Código de oficina a excluir (default: 976)
            dias_anteriores: Número de días anteriores a buscar (default: 30)
        
        Returns:
            Diccionario con los datos encontrados o None
        """
        if not self.admin_bd:
            logger.error("No se ha configurado el administrador de BD")
            return None
        
        try:
            # Formatear fecha del arqueo
            fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
            fecha_fin = int(fecha_obj.strftime('%Y%m%d'))
            
            # Calcular fecha inicio (días anteriores)
            fecha_inicio_obj = fecha_obj - timedelta(days=dias_anteriores)
            fecha_inicio = int(fecha_inicio_obj.strftime('%Y%m%d'))
            
            # Construir la consulta SQL
            consulta = f"""
            SELECT  ANOELB, 
                    MESELB, 
                    DIAELB, 
                    CODOFI, 
                    (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) AS CUENTA, 
                    NIT, 
                    NUMDOC, 
                    NROCMP, 
                    (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, 
                    VALOR 
            FROM gcolibranl.gcoffmvint 
            WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta})) 
              AND CODOFI <> {codofi_excluir}
              AND NIT = {codigo_cajero}
              AND (ANOELB*10000+MESELB*100+DIAELB) BETWEEN {fecha_inicio} AND {fecha_fin - 1}
            ORDER BY FECHA DESC
            """
            
            logger.debug(
                f"Consultando cuenta sobrantes {cuenta} días anteriores para cajero {codigo_cajero}, "
                f"fecha arqueo {fecha_arqueo}, valor descuadre {valor_descuadre}"
            )
            
            # Ejecutar consulta
            df = self.admin_bd.consultar(consulta)
            
            if df.empty:
                logger.debug(
                    f"No se encontraron movimientos en cuenta {cuenta} días anteriores para cajero {codigo_cajero}, "
                    f"fecha arqueo {fecha_arqueo}"
                )
                return None
            
            # Buscar el que coincida con el valor (exacto o por valor absoluto)
            coincidencia_valor = df[df['VALOR'].abs() == abs(valor_descuadre)]
            if len(coincidencia_valor) > 0:
                df = coincidencia_valor
            else:
                # Si no hay coincidencia exacta, retornar None
                logger.debug(
                    f"Movimiento encontrado en cuenta {cuenta} días anteriores pero valor no coincide: "
                    f"cajero={codigo_cajero}, fecha_arqueo={fecha_arqueo}, "
                    f"valor_buscado={valor_descuadre}, valores_encontrados={df['VALOR'].tolist()}"
                )
                return None
            
            # Convertir el primer resultado a diccionario (el más reciente)
            resultado = df.iloc[0].to_dict()
            
            logger.info(
                f"Movimiento encontrado en cuenta sobrantes {cuenta} días anteriores: cajero={codigo_cajero}, "
                f"fecha_movimiento={resultado.get('FECHA')}, valor={resultado.get('VALOR')}"
            )
            
            return resultado
        
        except Exception as e:
            logger.error(f"Error al consultar cuenta sobrantes días anteriores en BD: {e}")
            return None
    
    def consultar_sobrantes_negativos_suman_faltante(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        valor_faltante: float,
        cuenta: int = 279510020,
        codofi_excluir: int = 976,
        dias_anteriores: int = 30
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta movimientos en la cuenta de sobrantes (279510020) días anteriores al arqueo,
        buscando registros con signo negativo que sumen el valor del faltante.
        La búsqueda se detiene cuando encuentra un valor positivo.
        
        Esta consulta se usa para la regla de ARQUEO sin DIARIO con FALTANTE cuando
        arqueo_fisico/saldo_contadores está en 0.
        
        Args:
            codigo_cajero: Código del cajero a buscar (filtro por NIT)
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            valor_faltante: Valor del faltante (positivo)
            cuenta: Número de cuenta de sobrantes (default: 279510020)
            codofi_excluir: Código de oficina a excluir (default: 976)
            dias_anteriores: Número de días anteriores a buscar (default: 30)
        
        Returns:
            Diccionario con información de los movimientos encontrados si la suma coincide,
            None si no se encuentra o la suma no coincide
        """
        if not self.admin_bd:
            logger.error("No se ha configurado el administrador de BD")
            return None
        
        try:
            # Formatear fecha del arqueo
            fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
            fecha_fin = int(fecha_obj.strftime('%Y%m%d'))
            
            # Calcular fecha inicio (días anteriores)
            fecha_inicio_obj = fecha_obj - timedelta(days=dias_anteriores)
            fecha_inicio = int(fecha_inicio_obj.strftime('%Y%m%d'))
            
            # Construir la consulta SQL - buscar TODOS los movimientos (no solo negativos)
            consulta = f"""
            SELECT  ANOELB, 
                    MESELB, 
                    DIAELB, 
                    CODOFI, 
                    (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) AS CUENTA, 
                    NIT, 
                    NUMDOC, 
                    NROCMP, 
                    (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, 
                    VALOR 
            FROM gcolibranl.gcoffmvint 
            WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta})) 
              AND CODOFI <> {codofi_excluir}
              AND NIT = {codigo_cajero}
              AND (ANOELB*10000+MESELB*100+DIAELB) BETWEEN {fecha_inicio} AND {fecha_fin - 1}
            ORDER BY FECHA DESC
            """
            
            logger.debug(
                f"Consultando sobrantes negativos días anteriores para cajero {codigo_cajero}, "
                f"fecha arqueo {fecha_arqueo}, faltante {valor_faltante}"
            )
            
            # Ejecutar consulta
            df = self.admin_bd.consultar(consulta)
            
            if df.empty:
                logger.debug(
                    f"No se encontraron movimientos en cuenta {cuenta} días anteriores para cajero {codigo_cajero}, "
                    f"fecha arqueo {fecha_arqueo}"
                )
                return None
            
            # Procesar movimientos de más reciente a más antigua
            suma_negativos = 0.0
            movimientos_negativos = []
            
            for idx, row in df.iterrows():
                valor = row['VALOR']
                
                # Si encontramos un valor positivo, detener la búsqueda
                if valor > 0:
                    logger.debug(
                        f"Encontrado valor positivo ({valor}) en fecha {row['FECHA']}. "
                        f"Deteniendo búsqueda. Suma acumulada: {suma_negativos}"
                    )
                    break
                
                # Si el valor es negativo, sumarlo
                if valor < 0:
                    suma_negativos += abs(valor)  # Sumar el valor absoluto
                    movimientos_negativos.append(row.to_dict())
                    
                    logger.debug(
                        f"Movimiento negativo encontrado: fecha={row['FECHA']}, valor={valor}, "
                        f"suma_acumulada={suma_negativos}"
                    )
                    
                    # Si la suma alcanza o supera el faltante, verificar
                    if abs(suma_negativos - valor_faltante) < 0.01:
                        # La suma coincide con el faltante
                        logger.info(
                            f"Suma de sobrantes negativos coincide con faltante: "
                            f"suma={suma_negativos}, faltante={valor_faltante}, "
                            f"movimientos={len(movimientos_negativos)}"
                        )
                        return {
                            'encontrado': True,
                            'suma': suma_negativos,
                            'movimientos': movimientos_negativos,
                            'total_movimientos': len(movimientos_negativos)
                        }
                    elif suma_negativos > valor_faltante:
                        # La suma supera el faltante, no coincide
                        logger.debug(
                            f"Suma de sobrantes negativos ({suma_negativos}) supera el faltante ({valor_faltante})"
                        )
                        return None
            
            # Si llegamos aquí, la suma no coincide
            if suma_negativos > 0:
                logger.debug(
                    f"Suma de sobrantes negativos ({suma_negativos}) no coincide con faltante ({valor_faltante})"
                )
            else:
                logger.debug(
                    f"No se encontraron movimientos negativos en cuenta {cuenta} días anteriores"
                )
            
            return None
        
        except Exception as e:
            logger.error(f"Error al consultar sobrantes negativos días anteriores en BD: {e}")
            return None
    
    def consultar_cuenta_faltantes(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        valor_descuadre: float,
        cuenta: int = 168710093,
        codofi_excluir: int = 976
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta movimientos en la cuenta de faltantes (168710093) en la base de datos nacional
        para el mismo día del arqueo.
        
        Esta consulta se usa para la regla de ARQUEO y DIARIO con la misma diferencia (SOBRANTE).
        
        Args:
            codigo_cajero: Código del cajero a buscar (filtro por NIT)
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            valor_descuadre: Valor del descuadre (sobrante, negativo)
            cuenta: Número de cuenta de faltantes (default: 168710093)
            codofi_excluir: Código de oficina a excluir (default: 976)
        
        Returns:
            Diccionario con los datos encontrados o None
        """
        if not self.admin_bd:
            logger.error("No se ha configurado el administrador de BD")
            return None
        
        try:
            # Formatear fecha del arqueo
            fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
            fecha_formateada = int(fecha_obj.strftime('%Y%m%d'))
            
            # Construir la consulta SQL
            consulta = f"""
            SELECT  ANOELB, 
                    MESELB, 
                    DIAELB, 
                    CODOFI, 
                    (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) AS CUENTA, 
                    NIT, 
                    NUMDOC, 
                    NROCMP, 
                    (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, 
                    VALOR 
            FROM gcolibranl.gcoffmvint 
            WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta})) 
              AND CODOFI <> {codofi_excluir}
              AND NIT = {codigo_cajero}
              AND (ANOELB*10000+MESELB*100+DIAELB) = {fecha_formateada}
            ORDER BY FECHA DESC
            """
            
            logger.debug(
                f"Consultando cuenta faltantes {cuenta} para cajero {codigo_cajero}, "
                f"fecha arqueo {fecha_arqueo}, valor descuadre {valor_descuadre}"
            )
            
            # Ejecutar consulta
            df = self.admin_bd.consultar(consulta)
            
            if df.empty:
                logger.debug(
                    f"No se encontraron movimientos en cuenta {cuenta} para cajero {codigo_cajero}, "
                    f"fecha {fecha_arqueo}"
                )
                return None
            
            # Buscar el que coincida con el valor (exacto o por valor absoluto)
            # Para sobrantes, el valor_descuadre es negativo, pero en BD puede ser positivo o negativo
            coincidencia_valor = df[df['VALOR'].abs() == abs(valor_descuadre)]
            if len(coincidencia_valor) > 0:
                df = coincidencia_valor
            else:
                # Si no hay coincidencia exacta, retornar None
                logger.debug(
                    f"Movimiento encontrado en cuenta {cuenta} pero valor no coincide: "
                    f"cajero={codigo_cajero}, fecha={fecha_arqueo}, "
                    f"valor_buscado={valor_descuadre}, valores_encontrados={df['VALOR'].tolist()}"
                )
                return None
            
            # Convertir el primer resultado a diccionario
            resultado = df.iloc[0].to_dict()
            
            logger.info(
                f"Movimiento encontrado en cuenta faltantes {cuenta}: cajero={codigo_cajero}, "
                f"fecha={fecha_arqueo}, valor={resultado.get('VALOR')}"
            )
            
            return resultado
        
        except Exception as e:
            logger.error(f"Error al consultar cuenta faltantes en BD: {e}")
            return None
    
    def consultar_cuenta_faltantes_dias_anteriores(
        self,
        codigo_cajero: int,
        fecha_arqueo: str,
        valor_descuadre: float,
        cuenta: int = 168710093,
        codofi_excluir: int = 976,
        dias_anteriores: int = 30
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta movimientos en la cuenta de faltantes (168710093) en días anteriores al arqueo.
        
        Esta consulta se usa para la regla de ARQUEO y DIARIO con la misma diferencia (SOBRANTE)
        cuando no se encuentra movimiento el día del arqueo.
        
        Args:
            codigo_cajero: Código del cajero a buscar (filtro por NIT)
            fecha_arqueo: Fecha del arqueo en formato YYYY-MM-DD
            valor_descuadre: Valor del descuadre (sobrante, negativo)
            cuenta: Número de cuenta de faltantes (default: 168710093)
            codofi_excluir: Código de oficina a excluir (default: 976)
            dias_anteriores: Número de días anteriores a buscar (default: 30)
        
        Returns:
            Diccionario con los datos encontrados o None
        """
        if not self.admin_bd:
            logger.error("No se ha configurado el administrador de BD")
            return None
        
        try:
            # Formatear fecha del arqueo
            fecha_obj = datetime.strptime(fecha_arqueo, '%Y-%m-%d')
            fecha_fin = int(fecha_obj.strftime('%Y%m%d'))
            
            # Calcular fecha inicio (días anteriores)
            fecha_inicio_obj = fecha_obj - timedelta(days=dias_anteriores)
            fecha_inicio = int(fecha_inicio_obj.strftime('%Y%m%d'))
            
            # Construir la consulta SQL
            consulta = f"""
            SELECT  ANOELB, 
                    MESELB, 
                    DIAELB, 
                    CODOFI, 
                    (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) AS CUENTA, 
                    NIT, 
                    NUMDOC, 
                    NROCMP, 
                    (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, 
                    VALOR 
            FROM gcolibranl.gcoffmvint 
            WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta})) 
              AND CODOFI <> {codofi_excluir}
              AND NIT = {codigo_cajero}
              AND (ANOELB*10000+MESELB*100+DIAELB) BETWEEN {fecha_inicio} AND {fecha_fin - 1}
            ORDER BY FECHA DESC
            """
            
            logger.debug(
                f"Consultando cuenta faltantes {cuenta} días anteriores para cajero {codigo_cajero}, "
                f"fecha arqueo {fecha_arqueo}, valor descuadre {valor_descuadre}"
            )
            
            # Ejecutar consulta
            df = self.admin_bd.consultar(consulta)
            
            if df.empty:
                logger.debug(
                    f"No se encontraron movimientos en cuenta {cuenta} días anteriores para cajero {codigo_cajero}, "
                    f"fecha arqueo {fecha_arqueo}"
                )
                return None
            
            # Buscar el que coincida con el valor (exacto o por valor absoluto)
            coincidencia_valor = df[df['VALOR'].abs() == abs(valor_descuadre)]
            if len(coincidencia_valor) > 0:
                df = coincidencia_valor
            else:
                # Si no hay coincidencia exacta, retornar None
                logger.debug(
                    f"Movimiento encontrado en cuenta {cuenta} días anteriores pero valor no coincide: "
                    f"cajero={codigo_cajero}, fecha_arqueo={fecha_arqueo}, "
                    f"valor_buscado={valor_descuadre}, valores_encontrados={df['VALOR'].tolist()}"
                )
                return None
            
            # Convertir el primer resultado a diccionario (el más reciente)
            resultado = df.iloc[0].to_dict()
            
            logger.info(
                f"Movimiento encontrado en cuenta faltantes {cuenta} días anteriores: cajero={codigo_cajero}, "
                f"fecha_movimiento={resultado.get('FECHA')}, valor={resultado.get('VALOR')}"
            )
            
            return resultado
        
        except Exception as e:
            logger.error(f"Error al consultar cuenta faltantes días anteriores en BD: {e}")
            return None
    
    def consultar_documento_responsable(
        self,
        codigo_sucursal: int = 64,
        cuenta: int = 168710093,
        nrocmp: int = 770500,
        anio: int = None,
        mes_inicio: int = None,
        mes_fin: int = None
    ) -> Optional[str]:
        """
        Consulta el NIT (documento responsable) desde la cuenta de faltantes 168710093
        para un código de sucursal específico.
        
        Esta consulta se usa para la regla de SOBRANTE cuando se encuentra en cuenta de faltantes.
        
        Args:
            codigo_sucursal: Código de sucursal (default: 64)
            cuenta: Número de cuenta (default: 168710093)
            nrocmp: Número de comprobante (default: 770500)
            anio: Año de consulta (default: año actual)
            mes_inicio: Mes inicial del rango (default: 11)
            mes_fin: Mes final del rango (default: 12)
        
        Returns:
            NIT (documento responsable) o None
        """
        if not self.admin_bd:
            logger.error("No se ha configurado el administrador de BD")
            return None
        
        try:
            # Usar valores por defecto si no se proporcionan
            if anio is None:
                anio = datetime.now().year
            if mes_inicio is None:
                mes_inicio = 11
            if mes_fin is None:
                mes_fin = 12
            
            # Construir la consulta SQL con TOP 1 y ORDER BY NIT
            consulta = f"""
            SELECT TOP 1 NIT 
            FROM gcolibranl.gcoffmvint 
            WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC IN ({cuenta})) 
              AND ANOELB = {anio}
              AND MESELB BETWEEN {mes_inicio} AND {mes_fin}
              AND DIAELB BETWEEN 1 AND 31 
              AND CODOFI = {codigo_sucursal}
              AND NROCMP = {nrocmp}
            ORDER BY NIT
            """
            
            logger.debug(
                f"Consultando documento responsable para cuenta {cuenta}, "
                f"sucursal {codigo_sucursal}, nrocmp {nrocmp}"
            )
            
            # Ejecutar consulta
            df = self.admin_bd.consultar(consulta)
            
            if df.empty:
                logger.debug(
                    f"No se encontró documento responsable para cuenta {cuenta}, "
                    f"sucursal {codigo_sucursal}"
                )
                return None
            
            # Tomar el primer NIT encontrado
            nit = str(int(df.iloc[0]['NIT']))
            
            logger.info(
                f"Documento responsable encontrado: NIT={nit} para cuenta {cuenta}, "
                f"sucursal {codigo_sucursal}"
            )
            
            return nit
        
        except Exception as e:
            logger.error(f"Error al consultar documento responsable en BD: {e}")
            return None
    
    def desconectar(self):
        """
        Cierra la conexión a la base de datos.
        Nota: En CertificacionArqueo no hay método desconectar explícito,
        pero lo mantenemos para buena práctica.
        """
        if self.admin_bd and hasattr(self.admin_bd, 'conn') and self.admin_bd.conn:
            try:
                self.admin_bd.conn.close()
                self.admin_bd.conn = None
                logger.info("Conexión a BD cerrada")
            except Exception as e:
                logger.warning(f"Error al cerrar conexión: {e}")

