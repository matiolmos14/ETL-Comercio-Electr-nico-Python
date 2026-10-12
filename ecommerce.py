"""ETL de e-commerce: bronze -> silver -> gold sobre S3."""
import glob
import io
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

# No pisa variables ya definidas en el entorno (p. ej. las de docker-compose)
load_dotenv()

ESTADOS_VALIDOS = ['procesando', 'enviado', 'pendiente', 'cancelado', 'entregado']
ESTADO_DESCONOCIDO = 'desconocido'

# Errores de tipeo observados en origen y el estado al que corresponden. Cada uno es el estado válido con las letras
# cambiadas de lugar, así que no hay ambigüedad. Lo que no está acá ni es un estado válido queda como 'desconocido':
# agregar una variante nueva es una decisión explícita y no una coincidencia por parecido.
EQUIVALENCIAS_ESTADO = {
    'cacnelado': 'cancelado', 'cancealdo': 'cancelado', 'cancelaod': 'cancelado', 'cancleado': 'cancelado',
    'caneclado': 'cancelado', 'cnacelado': 'cancelado',
    'enivado': 'enviado', 'envaido': 'enviado', 'enviaod': 'enviado', 'envidao': 'enviado', 'evniado': 'enviado',
    'enrtegado': 'entregado', 'entergado': 'entregado', 'entregdao': 'entregado', 'entrgeado': 'entregado',
    'etnregado': 'entregado',
    'pendeinte': 'pendiente', 'pendietne': 'pendiente', 'penidente': 'pendiente', 'pnediente': 'pendiente',
    'porcesando': 'procesando', 'proceasndo': 'procesando', 'procesadno': 'procesando', 'procesanod': 'procesando',
    'procesnado': 'procesando', 'procseando': 'procesando',
}

VALORES_NULOS = ('', 'N/A', 'n/a')

COLUMNAS_NUMERICAS = ['total_amount', 'subtotal', 'unit_price', 'quantity', 'price', 'discount_percent']

# Clave primaria de cada tabla. Una fila sin clave no se puede cruzar y una clave repetida duplica filas al cruzar.
# orders y order_items tienen su propia regla de depuración.
CLAVES_PRIMARIAS = {
    'brands': 'brand_id', 'categories': 'category_id', 'customers': 'customer_id', 'inventory': 'inventory_id',
    'products': 'product_id', 'promotions': 'promotion_id', 'reviews': 'review_id', 'suppliers': 'supplier_id',
    'warehouses': 'warehouse_id',
}

# Columnas que usan la limpieza y los reportes. Sin ellas el pipeline falla más adelante con un KeyError de pandas.
TABLAS_REQUERIDAS = ['orders', 'customers', 'products', 'order_items']
COLUMNAS_REQUERIDAS = {
    'orders': ['order_id', 'customer_id', 'total_amount', 'order_date', 'status'],
    'customers': ['customer_id', 'first_name', 'last_name'],
    'products': ['product_id', 'product_name', 'category_id', 'price'],
    'order_items': ['order_id', 'product_id', 'quantity'],
    'inventory': ['product_id', 'warehouse_id', 'quantity'],
    'warehouses': ['warehouse_id', 'warehouse_name', 'capacity_units', 'location'],
}

# Formatos en que llegan las fechas. Con guion son día-mes-año y con barra mes-día-año: en los valores que no son
# ambiguos (día > 12) ningún guion trae el día en segundo lugar y ninguna barra lo trae en primero.
FORMATOS_FECHA = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d-%m-%Y', '%m/%d/%Y', '%Y/%m/%d', '%d %b %Y', '%b %d, %Y']
COLUMNA_FECHA = re.compile(r'(_date|_at)$|^last_login$')


class PipelineError(Exception):
    """Falla controlada del pipeline (configuración, origen o contrato de datos)."""


def configurar_logging() -> None:
    """Configura el log del proceso. Se llama desde el punto de entrada, no desde la clase."""
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # evita caracteres rotos en consolas Windows
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)]
    )


class EcommerceETL:
    """Pipeline bronze -> silver -> gold. Con LOCAL_MODE=true lee de data_dir y escribe en output_dir, sin usar S3."""

    def __init__(self, data_dir: str = 'data', output_dir: str = 'output',
                 local_mode: Optional[bool] = None, s3_client: Any = None,
                 as_of_date: Optional[str] = None) -> None:
        self.data_dir: str = data_dir
        self.output_dir: str = output_dir

        # Fecha de corrida: lo que es "futuro" se mide contra ella, así reprocesar el mismo origen da el mismo resultado.
        # Por defecto es hoy; para reprocesar una fecha pasada se define AS_OF_DATE (AAAA-MM-DD).
        fecha = as_of_date or os.getenv('AS_OF_DATE', '').strip()
        try:
            self.as_of_date: pd.Timestamp = pd.Timestamp(fecha) if fecha else pd.Timestamp.now()
        except ValueError as e:
            raise PipelineError(f"AS_OF_DATE inválida ('{fecha}'): se espera AAAA-MM-DD.") from e

        if local_mode is None:
            local_mode = os.getenv('LOCAL_MODE', 'false').strip().lower() in ('1', 'true', 'yes')
        self.local_mode: bool = local_mode

        self.bucket: str = os.getenv('S3_BUCKET', '').strip()
        self.bronze_prefix: str = self._prefijo('S3_BRONZE_PREFIX', 'bronze/')
        self.silver_prefix: str = self._prefijo('S3_SILVER_PREFIX', 'silver/')
        self.gold_prefix: str = self._prefijo('S3_GOLD_PREFIX', 'gold/')
        self.reject_threshold: float = float(os.getenv('REJECT_THRESHOLD', '0.05'))
        # Los reportes también salen en CSV porque quienes los consumen suelen abrirlos en Excel
        self.gold_csv: bool = os.getenv('GOLD_CSV', 'true').strip().lower() in ('1', 'true', 'yes')
        self._s3 = s3_client  # inyectable en tests; si es None se crea al primer uso

        self.datasets: Dict[str, pd.DataFrame] = {}
        self.reports: Dict[str, pd.DataFrame] = {}
        self.rejected: Dict[str, pd.DataFrame] = {}
        self._bytes_escritos: Dict[str, int] = {'silver': 0, 'gold': 0}

        self.logger = logging.getLogger("ETL_Pipeline")

    @staticmethod
    def _prefijo(variable: str, defecto: str) -> str:
        """Lee un prefijo de S3 del entorno y se asegura de que termine en '/'."""
        valor = os.getenv(variable, defecto).strip() or defecto
        return valor if valor.endswith('/') else valor + '/'

    def _get_s3(self) -> Any:
        if self._s3 is None:
            self._s3 = boto3.client('s3')
        return self._s3

    def _verificar_entorno(self) -> None:
        """Comprueba que el origen y el destino estén disponibles antes de procesar."""
        self.logger.info("Iniciando verificacion de infraestructura.")
        if self.local_mode:
            if not os.path.exists(self.data_dir):
                raise PipelineError(f"Directorio origen '{self.data_dir}/' inexistente.")
            os.makedirs(self.output_dir, exist_ok=True)
        else:
            if not self.bucket:
                raise PipelineError("Variable S3_BUCKET no definida. Configurar .env o usar LOCAL_MODE=true.")
            os.makedirs(self.data_dir, exist_ok=True)

    def _descargar_bronze(self) -> None:
        """Copia los JSON de bronze a data_dir. Solo lee de S3, nunca escribe en bronze."""
        self.logger.info(f"Descargando capa Bronze desde s3://{self.bucket}/{self.bronze_prefix}")
        descargados = 0
        try:
            s3 = self._get_s3()
            for pagina in s3.get_paginator('list_objects_v2').paginate(Bucket=self.bucket, Prefix=self.bronze_prefix):
                for obj in pagina.get('Contents', []):
                    nombre = os.path.basename(obj['Key'])
                    if nombre.startswith('ecommerce_') and nombre.endswith('.json'):
                        s3.download_file(self.bucket, obj['Key'], os.path.join(self.data_dir, nombre))
                        descargados += 1
        except (BotoCoreError, ClientError) as e:
            raise PipelineError(f"Falla al descargar Bronze desde S3: {e}") from e

        self.logger.info(f"Capa Bronze: {descargados} archivos descargados en '{self.data_dir}/'.")

    def extract(self) -> None:
        """Carga los JSON de origen en DataFrames y deja un perfil de cada tabla en el log."""
        self.logger.info("Fase de Extraccion: Iniciada.")

        if not self.local_mode:
            self._descargar_bronze()

        archivos: List[str] = glob.glob(os.path.join(self.data_dir, 'ecommerce_*.json'))
        if not archivos:
            raise PipelineError("No se detectaron archivos JSON para procesar.")

        nombres_tablas: List[str] = [
            'orders', 'order_items', 'customers', 'products', 'brands',
            'categories', 'inventory', 'promotions', 'reviews', 'suppliers', 'warehouses'
        ]

        for nombre in nombres_tablas:
            ruta = os.path.join(self.data_dir, f'ecommerce_{nombre}.json')
            if os.path.exists(ruta):
                try:
                    self.datasets[nombre] = pd.read_json(ruta)
                except ValueError as e:
                    self.logger.error(f"Error de parseo JSON en archivo '{ruta}': {e}")
            else:
                self.logger.warning(f"Archivo fuente omitido (No encontrado): {ruta}")

        self._perfilar()
        self.logger.info(f"Fase de Extraccion: Completada. {len(self.datasets)} datasets cargados.")

    def _perfilar(self) -> None:
        """Deja en el log el tamaño y los nulos de cada tabla, para revisar los datos antes de transformarlos."""
        for nombre, df in self.datasets.items():
            nulos = df.isna().sum()
            con_nulos = nulos[nulos > 0]
            detalle = ', '.join(f"{col} ({n})" for col, n in con_nulos.items()) or 'ninguna'
            self.logger.info(f"Perfil {nombre}: {len(df)} filas, {len(df.columns)} columnas. Columnas con nulos: {detalle}.")

    def _validate_schemas(self) -> None:
        """Verifica que existan las tablas y columnas que necesitan la limpieza y los reportes."""
        self.logger.info("Ejecutando validacion de esquemas.")

        problemas: List[str] = []
        for tabla in TABLAS_REQUERIDAS:
            if tabla not in self.datasets:
                problemas.append(f"Dataset crítico '{tabla}' no encontrado.")

        for tabla, requeridas in COLUMNAS_REQUERIDAS.items():
            if tabla in self.datasets:
                faltantes = [col for col in requeridas if col not in self.datasets[tabla].columns]
                if faltantes:
                    problemas.append(f"Faltan columnas críticas en '{tabla}': {faltantes}")

        if problemas:
            raise PipelineError("Falla de contrato: " + " | ".join(problemas))

        self.logger.info("Validacion de esquemas superada exitosamente.")

    @staticmethod
    def _limpiar_textos(df: pd.DataFrame) -> pd.DataFrame:
        """Quita espacios y saltos de línea al borde de las columnas de texto y convierte los vacíos en nulos."""
        for col in df.columns:
            # object (pandas 2) o str (pandas 3). Se usa map y no .str porque las columnas
            # mixtas (ids numéricos junto a '') perderían los valores que no son string.
            # No se usa df.replace(regex=True): falla en pandas si una columna de texto viene entera en nulo.
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                # El recorte va antes de detectar vacíos para que un texto de solo espacios también quede nulo
                df[col] = df[col].map(lambda v: (np.nan if v.strip() in VALORES_NULOS else v.strip()) if isinstance(v, str) else v)
        return df.infer_objects()

    @staticmethod
    def _parsear_fechas(serie: pd.Series) -> pd.Series:
        """Convierte fechas que llegan en varios formatos. Lo que no coincide con ninguno queda en NaT."""
        resultado = pd.Series(pd.NaT, index=serie.index, dtype='datetime64[ns]')
        for formato in FORMATOS_FECHA:
            pendiente = resultado.isna() & serie.notna()
            if not pendiente.any():
                break
            resultado[pendiente] = pd.to_datetime(serie[pendiente], format=formato, errors='coerce')
        return resultado

    def _convertir_fechas(self, df: pd.DataFrame, tabla: str) -> None:
        """Convierte a datetime las columnas de fecha. Avisa cuántos valores no tenían un formato reconocido."""
        for col in df.columns:
            if not COLUMNA_FECHA.search(col):
                continue
            if not (pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col])):
                continue
            antes = int(df[col].isna().sum())
            df[col] = self._parsear_fechas(df[col])
            sin_formato = int(df[col].isna().sum()) - antes
            if sin_formato:
                self.logger.warning(f"'{tabla}.{col}': {sin_formato} fechas con formato no reconocido quedaron nulas.")

    def _convertir_numericos(self, df: pd.DataFrame, tabla: str) -> None:
        """Convierte a número las columnas de importes y cantidades. Avisa cuántos valores quedaron nulos por no ser numéricos."""
        for col in [c for c in df.columns if c in COLUMNAS_NUMERICAS]:
            antes = int(df[col].isna().sum())
            df[col] = pd.to_numeric(df[col], errors='coerce')
            invalidos = int(df[col].isna().sum()) - antes
            if invalidos:
                self.logger.warning(f"'{tabla}.{col}': {invalidos} valores no numéricos quedaron nulos.")

    def _castear_ids(self, df: pd.DataFrame, tabla: str) -> None:
        """Guarda las columnas *_id como entero con nulos (Int64) para que los cruces entre tablas coincidan."""
        for col in [c for c in df.columns if c.endswith('_id')]:
            numerico = pd.to_numeric(df[col], errors='coerce')
            if numerico.isna().sum() != df[col].isna().sum():
                # Hay códigos de texto (ej. 'PROMO1'): castearlos los convertiría en nulos
                self.logger.warning(f"'{tabla}.{col}' contiene valores no numéricos: se mantiene sin castear.")
                continue
            try:
                df[col] = numerico.astype('Int64')
            except TypeError:
                self.logger.warning(f"'{tabla}.{col}' contiene valores no enteros: se mantiene sin castear.")

    def _depurar_claves(self, tabla: str, df: pd.DataFrame) -> pd.DataFrame:
        """Descarta filas sin clave primaria y se queda con la última de las repetidas."""
        clave = CLAVES_PRIMARIAS.get(tabla)
        if clave is None or clave not in df.columns:
            return df

        sin_clave = int(df[clave].isna().sum())
        df = df.dropna(subset=[clave])
        repetidos = int(df.duplicated(subset=[clave], keep='last').sum())
        df = df.drop_duplicates(subset=[clave], keep='last')
        if sin_clave or repetidos:
            self.logger.warning(f"{tabla}: {sin_clave} filas sin {clave} y {repetidos} repetidas por {clave} descartadas.")
        return df

    def _registrar_rechazos(self, tabla: str, rechazados: pd.DataFrame, total: int) -> None:
        """Guarda los registros apartados y avisa si la tasa de rechazo supera el umbral."""
        self.rejected[tabla] = rechazados.reset_index(drop=True)
        tasa = len(rechazados) / total
        detalle = ', '.join(f"{m}: {n}" for m, n in rechazados['motivo_rechazo'].value_counts().items())
        self.logger.warning(f"{tabla}: {len(rechazados)} de {total} registros ({tasa:.1%}) rechazados ({detalle}).")
        if tasa > self.reject_threshold:
            self.logger.warning(
                f"Tasa de rechazo {tasa:.1%} supera el umbral de {self.reject_threshold:.0%}: revisar la fuente de origen."
            )

    def _aislar_order_items_invalidos(self, ids_orders_origen: set) -> None:
        """Aparta las líneas sin pedido o con producto inexistente. Las de product_id nulo con pedido válido se conservan."""
        # Si el product_id es nulo pero el pedido es válido, la venta existe y no se rechaza.
        items = self.datasets['order_items']

        if 'order_item_id' in items.columns:
            antes = len(items)
            items = items.drop_duplicates(subset=['order_item_id'], keep='last')
            if antes != len(items):
                self.logger.warning(f"order_items: {antes - len(items)} duplicados por order_item_id eliminados.")

        if not {'order_id', 'product_id'} <= set(items.columns):
            self.datasets['order_items'] = items
            return

        motivo = pd.Series(pd.NA, index=items.index, dtype='object')
        motivo[items['order_id'].isna()] = 'order_id nulo'
        if 'orders' in self.datasets:
            # Se distingue el huérfano real del pedido que existía en origen y se depuró en silver
            fuera_de_silver = motivo.isna() & ~items['order_id'].isin(self.datasets['orders']['order_id'])
            motivo[fuera_de_silver & ~items['order_id'].isin(ids_orders_origen)] = 'order_id inexistente en orders'
            motivo[fuera_de_silver & items['order_id'].isin(ids_orders_origen)] = \
                'pedido descartado en orders (customer_id o total_amount nulo)'
        if 'products' in self.datasets:
            invalido = motivo.isna() & items['product_id'].notna() \
                & ~items['product_id'].isin(self.datasets['products']['product_id'])
            motivo[invalido] = 'product_id inexistente en products'

        rechazados = items[motivo.notna()].assign(motivo_rechazo=motivo[motivo.notna()])
        self.datasets['order_items'] = items[motivo.isna()]

        if rechazados.empty:
            return

        self._registrar_rechazos('order_items', rechazados, len(items))

    def _normalizar_estados(self, df: pd.DataFrame) -> None:
        """Lleva el estado del pedido a los 5 valores válidos. Nulos y valores no reconocidos pasan a 'desconocido'."""
        # En origen hay mayúsculas, sufijos (_v2) y errores de tipeo. El valor original se conserva en status_original.
        if 'status' not in df.columns:
            return

        def normalizar(valor: Any) -> str:
            if not isinstance(valor, str):
                return ESTADO_DESCONOCIDO
            limpio = re.sub(r'_v\d+$', '', valor.strip().lower())
            if limpio in ESTADOS_VALIDOS:
                return limpio
            return EQUIVALENCIAS_ESTADO.get(limpio, ESTADO_DESCONOCIDO)

        df['status_original'] = df['status']
        df['status'] = df['status'].map(normalizar)
        df['dq_estado_desconocido'] = df['status'] == ESTADO_DESCONOCIDO
        desconocidos = int(df['dq_estado_desconocido'].sum())
        if desconocidos:
            self.logger.warning(
                f"orders: {desconocidos} de {len(df)} pedidos con estado nulo o no reconocido; "
                f"quedan como '{ESTADO_DESCONOCIDO}' y se tratan como no cancelados."
            )

    def _nombrar_depositos(self) -> None:
        """Da nombre a los depósitos que no lo tienen: sin nombre quedarían fuera del agrupado de los reportes."""
        depositos = self.datasets['warehouses'].copy()
        if {'warehouse_id', 'warehouse_name'} <= set(depositos.columns):
            sin_nombre = depositos['warehouse_name'].isna()
            depositos.loc[sin_nombre, 'warehouse_name'] = 'Sin nombre (id ' + depositos.loc[sin_nombre, 'warehouse_id'].astype(str) + ')'
        self.datasets['warehouses'] = depositos.reset_index(drop=True)

    def _aislar_inventario_huerfano(self) -> None:
        """Aparta el stock que no se puede asignar a un depósito conocido: no entra a los reportes pero queda registrado."""
        inventario = self.datasets['inventory']
        if 'warehouse_id' not in inventario.columns or 'warehouses' not in self.datasets:
            return

        motivo = pd.Series(pd.NA, index=inventario.index, dtype='object')
        motivo[inventario['warehouse_id'].isna()] = 'warehouse_id nulo'
        motivo[motivo.isna() & ~inventario['warehouse_id'].isin(self.datasets['warehouses']['warehouse_id'])] = \
            'warehouse_id inexistente en warehouses'

        rechazados = inventario[motivo.notna()].assign(motivo_rechazo=motivo[motivo.notna()])
        self.datasets['inventory'] = inventario[motivo.isna()]
        if not rechazados.empty:
            self._registrar_rechazos('inventory', rechazados, len(inventario))

    def _marcar_calidad(self) -> None:
        """Marca con columnas dq_* los registros sospechosos y avisa cuántos son.

        Son reglas blandas: un monto atípico o una fecha futura pueden ser un error, pero también un caso real que el
        negocio todavía no confirmó. Por eso se marcan y se conservan; lo que se haga con ellos se decide en gold.
        """
        orders = self.datasets.get('orders')
        if orders is not None and 'total_amount' in orders.columns and not orders.empty:
            orders = orders.copy()
            orders['dq_monto_no_positivo'] = orders['total_amount'] <= 0

            # Atípico: más de 3 rangos intercuartílicos por encima del tercer cuartil
            q1, q3 = orders['total_amount'].quantile([0.25, 0.75])
            tope = q3 + 3 * (q3 - q1)
            orders['dq_monto_atipico'] = orders['total_amount'] > tope

            if 'order_date' in orders.columns and pd.api.types.is_datetime64_any_dtype(orders['order_date']):
                # Un pedido del día de la corrida no es futuro, aunque traiga hora
                orders['dq_fecha_futura'] = orders['order_date'] >= self.as_of_date.normalize() + pd.Timedelta(days=1)
            self.datasets['orders'] = orders

            if orders['dq_monto_no_positivo'].any():
                self.logger.warning(f"orders: {int(orders['dq_monto_no_positivo'].sum())} pedidos con total_amount menor o igual a 0.")
            total = orders['total_amount'].sum()
            atipicos = orders[orders['dq_monto_atipico']]
            if not atipicos.empty and total > 0:
                self.logger.warning(
                    f"orders: {len(atipicos)} pedidos con monto mayor a {tope:,.0f} (Q3 + 3 x IQR) concentran el "
                    f"{atipicos['total_amount'].sum() / total:.0%} del monto total. Se mantienen: validar con el negocio."
                )
            if 'dq_fecha_futura' in orders.columns and orders['dq_fecha_futura'].any():
                self.logger.warning(
                    f"orders: {int(orders['dq_fecha_futura'].sum())} pedidos con fecha posterior a la fecha de corrida "
                    f"({self.as_of_date:%Y-%m-%d}): no se asignan a ningún mes."
                )

        items = self.datasets.get('order_items')
        if items is not None:
            items = items.copy()
            reglas = [('quantity', 'dq_cantidad_no_positiva', lambda s: s <= 0, 'menor o igual a 0'),
                      ('unit_price', 'dq_precio_negativo', lambda s: s < 0, 'negativo')]
            for col, marca, condicion, texto in reglas:
                if col in items.columns:
                    items[marca] = condicion(items[col])
                    if items[marca].any():
                        self.logger.warning(f"order_items: {int(items[marca].sum())} líneas con {col} {texto}.")
            self.datasets['order_items'] = items

    def transform_cleaning(self) -> None:
        """Limpia cada tabla (textos, vacíos, tipos, claves) y aplica las reglas de negocio de orders, products y order_items."""
        self.logger.info("Fase de Transformacion (Data Quality): Iniciada.")

        for nombre, df in self.datasets.items():
            df = self._limpiar_textos(df)
            self._convertir_numericos(df, nombre)
            self._convertir_fechas(df, nombre)
            self._castear_ids(df, nombre)
            self.datasets[nombre] = self._depurar_claves(nombre, df)

        # Se guardan antes de depurar orders para clasificar los items huérfanos
        ids_orders_origen = set(self.datasets['orders']['order_id'].dropna()) if 'orders' in self.datasets else set()

        if 'orders' in self.datasets:
            df_orders = self.datasets['orders'].copy()

            # Criterio propuesto, pendiente de validar con el negocio: sin cliente la venta no se puede atribuir
            # y sin monto no entra a ningún reporte, así que se apartan a rechazados (no se borran).
            sin_cliente = df_orders['customer_id'].isna()
            sin_monto = df_orders['total_amount'].isna()
            # Si el pedido llegó repetido y una de las versiones es válida, esa queda y no se rechaza el pedido
            sin_version_valida = (sin_cliente | sin_monto) & ~df_orders['order_id'].isin(df_orders.loc[~(sin_cliente | sin_monto), 'order_id'])
            if sin_version_valida.any():
                rechazados = df_orders[sin_version_valida].assign(motivo_rechazo=np.select(
                    [sin_cliente[sin_version_valida] & sin_monto[sin_version_valida], sin_cliente[sin_version_valida]],
                    ['customer_id y total_amount nulos', 'customer_id nulo'],
                    default='total_amount nulo'))
                self._registrar_rechazos('orders', rechazados.drop_duplicates(subset=['order_id'], keep='last'),
                                         df_orders['order_id'].nunique())
            df_orders = df_orders.dropna(subset=['customer_id', 'total_amount'])

            # Pasa por object para soportar ids Int64 y códigos de texto
            df_orders["promotion_id"] = df_orders["promotion_id"].astype(object).fillna("Sin codigo").astype(str)
            df_orders["notes"] = df_orders["notes"].fillna("Sin notas")

            # Si un pedido llega repetido se conserva la versión más reciente. El orden estable hace que, ante la
            # misma fecha, quede la última en el orden de origen; los pedidos sin fecha pierden contra los que la tienen.
            df_orders = df_orders.sort_values('order_date', kind='stable', na_position='first') \
                .drop_duplicates(subset=['order_id'], keep='last')
            self._normalizar_estados(df_orders)
            self.datasets['orders'] = df_orders

        if 'products' in self.datasets:
            df_products = self.datasets['products'].copy()
            # Los precios nulos se imputan con la media de su categoría
            df_products['price'] = df_products.groupby('category_id')['price'].transform(lambda x: x.fillna(x.mean()))
            self.datasets['products'] = df_products

        if 'order_items' in self.datasets:
            # Criterio propuesto, pendiente de validar: una línea sin cantidad se asume de 1 unidad porque la venta existió
            self.datasets['order_items']['quantity'] = self.datasets['order_items']['quantity'].fillna(1)
            self._aislar_order_items_invalidos(ids_orders_origen)
        if 'warehouses' in self.datasets:
            self._nombrar_depositos()
        if 'inventory' in self.datasets:
            # Criterio propuesto, pendiente de validar: sin cantidad registrada se asume que no hay stock
            self.datasets['inventory']['quantity'] = self.datasets['inventory']['quantity'].fillna(0)
            self._aislar_inventario_huerfano()

        self._marcar_calidad()
        self.logger.info("Fase de Transformacion (Data Quality): Completada.")

    def transform_metrics(self) -> None:
        """Calcula los 5 reportes de gold. Los pedidos cancelados no cuentan en ninguno."""
        self.logger.info("Fase de Transformacion (Agregaciones OLAP): Iniciada.")

        ordenes = self.datasets['orders']
        df_ventas_validas = ordenes[ordenes['status'] != 'cancelado'].copy()

        # Los reportes por producto usan la misma regla: las líneas de pedidos cancelados no cuentan
        items_vigentes = self.datasets['order_items']
        if 'order_id' in items_vigentes.columns:
            cancelados = ordenes.loc[ordenes['status'] == 'cancelado', 'order_id']
            items_vigentes = items_vigentes[~items_vigentes['order_id'].isin(cancelados)]

        clientes = self.datasets['customers'][['customer_id', 'first_name', 'last_name']]
        sin_cliente = int((~df_ventas_validas['customer_id'].isin(clientes['customer_id'])).sum())
        if sin_cliente:
            self.logger.warning(f"{sin_cliente} pedidos de clientes que no existen en customers: figuran en el ranking sin nombre.")

        # dropna=False para que un cliente sin nombre cargado no desaparezca del ranking
        self.reports['reporte_clientes'] = pd.merge(df_ventas_validas, clientes, on="customer_id", how="left") \
            .groupby(["customer_id", "first_name", "last_name"], dropna=False) \
            .agg(total_amount=("total_amount", "sum"), cantidad_ordenes=("order_id", "count")) \
            .sort_values("total_amount", ascending=False).head(5).reset_index()

        # Las líneas sin product_id no entran al ranking: agruparlas armaría un "producto" desconocido
        productos = self.datasets['products'][['product_id', 'product_name']]
        self.reports['reporte_producto_mas_vendido'] = pd.merge(items_vigentes.dropna(subset=['product_id']), productos, on="product_id", how="left") \
            .groupby(["product_id", "product_name"], dropna=False)["quantity"].sum() \
            .sort_values(ascending=False).head(1).reset_index()

        # Un pedido sin fecha, o con fecha posterior a la corrida, no se puede ubicar en ningún mes
        fecha_invalida = df_ventas_validas["order_date"].isna()
        if "dq_fecha_futura" in df_ventas_validas.columns:
            fecha_invalida |= df_ventas_validas["dq_fecha_futura"].fillna(False).astype(bool)
        sin_fecha = df_ventas_validas[fecha_invalida]
        if not sin_fecha.empty:
            self.logger.warning(
                f"Ventas mensuales: {len(sin_fecha)} pedidos sin fecha válida ({sin_fecha['total_amount'].sum():,.0f}) no se asignan a ningún mes."
            )
        con_fecha = df_ventas_validas[~fecha_invalida].copy()
        con_fecha["mes"] = con_fecha["order_date"].dt.to_period("M").astype(str)
        self.reports['reporte_ventas_mensual'] = con_fecha.groupby("mes")["total_amount"].sum().reset_index()

        if all(k in self.datasets for k in ('inventory', 'warehouses', 'order_items')):
            df_stock_full = pd.merge(self.datasets['inventory'], self.datasets['warehouses'], on="warehouse_id", how="left")
            reporte_logistica = df_stock_full.groupby("warehouse_name").agg({
                'quantity': 'sum', 'capacity_units': 'first', 'location': 'first'
            }).reset_index()
            reporte_logistica['%_ocupacion'] = (reporte_logistica['quantity'] / reporte_logistica['capacity_units']) * 100
            saturados = reporte_logistica.loc[reporte_logistica['%_ocupacion'] > 100, 'warehouse_name']
            if not saturados.empty:
                self.logger.warning(
                    f"Ocupación superior a 100% en: {', '.join(saturados)}. Revisar stock o capacidad en origen."
                )
            self.reports['reporte_logistica_depositos'] = reporte_logistica

            ventas_totales_prod = items_vigentes.groupby("product_id")["quantity"].sum().reset_index()
            df_rotacion = pd.merge(self.datasets['inventory'], ventas_totales_prod, on="product_id", how="left").fillna(0)
            df_rotacion = pd.merge(df_rotacion, self.datasets['warehouses'], on="warehouse_id", how="left")

            reporte_rotacion = df_rotacion.groupby("warehouse_name").agg({
                'quantity_y': 'sum', 'quantity_x': 'sum', 'location': 'first'
            }).rename(columns={'quantity_y': 'total_vendido', 'quantity_x': 'stock_actual'}).reset_index()

            reporte_rotacion['indice_rotacion'] = np.where(
                reporte_rotacion['stock_actual'] > 0,
                (reporte_rotacion['total_vendido'] / reporte_rotacion['stock_actual']).round(2),
                0
            )
            self.reports['reporte_rotacion_inventario'] = reporte_rotacion

        self.logger.info("Fase de Transformacion (Agregaciones OLAP): Completada.")

    def _guardar_parquet(self, df: pd.DataFrame, capa: str, nombre: str) -> None:
        """Guarda un DataFrame como Parquet en output/ (modo local) o en S3, y suma los bytes escritos."""
        if df.empty:
            return

        if self.local_mode:
            ruta = os.path.join(self.output_dir, capa, f'{nombre}.parquet')  # `nombre` puede traer subcarpeta (rejected/...)
            os.makedirs(os.path.dirname(ruta), exist_ok=True)
            df.to_parquet(ruta, index=False, engine='pyarrow')
            self._bytes_escritos[capa] += os.path.getsize(ruta)
        else:
            prefijo = self.silver_prefix if capa == 'silver' else self.gold_prefix
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            contenido = buffer.getvalue()
            self._get_s3().put_object(Bucket=self.bucket, Key=f'{prefijo}{nombre}.parquet', Body=contenido)
            self._bytes_escritos[capa] += len(contenido)

    def _guardar_csv_gold(self, df: pd.DataFrame, nombre: str) -> None:
        """Guarda un reporte de gold también como CSV (UTF-8 con BOM para que Excel respete los acentos)."""
        if df.empty:
            return

        if self.local_mode:
            ruta = os.path.join(self.output_dir, 'gold', f'{nombre}.csv')
            os.makedirs(os.path.dirname(ruta), exist_ok=True)
            df.to_csv(ruta, index=False, encoding='utf-8-sig')
        else:
            self._get_s3().put_object(
                Bucket=self.bucket, Key=f'{self.gold_prefix}{nombre}.csv',
                Body=df.to_csv(index=False).encode('utf-8-sig'), ContentType='text/csv'
            )

    def _tamanio_bronze(self) -> int:
        """Suma el tamaño en bytes de los JSON de origen."""
        return sum(os.path.getsize(f) for f in glob.glob(os.path.join(self.data_dir, 'ecommerce_*.json')))

    def load(self) -> None:
        """Escribe silver, los rechazados y gold, y deja en el log cuánto pesan frente al JSON de origen."""
        destino = f"'{self.output_dir}/'" if self.local_mode else f"s3://{self.bucket}/"
        self.logger.info(f"Fase de Carga (Silver y Gold): Iniciada. Destino: {destino}")

        try:
            for nombre, df in self.datasets.items():
                self._guardar_parquet(df, 'silver', nombre)

            for nombre, df_rechazado in self.rejected.items():
                self._guardar_parquet(df_rechazado, 'silver', f'rejected/{nombre}')

            for nombre_reporte, df_reporte in self.reports.items():
                self._guardar_parquet(df_reporte, 'gold', nombre_reporte)
                if self.gold_csv:
                    self._guardar_csv_gold(df_reporte, nombre_reporte)
        except (OSError, BotoCoreError, ClientError) as e:
            raise PipelineError(f"Falla de I/O al persistir Silver/Gold: {e}") from e

        self.logger.info(
            f"Fase de Carga: Completada. Silver: {len(self.datasets)} tablas, "
            f"rechazados: {len(self.rejected)}, Gold: {len(self.reports)} reportes."
        )
        silver = self._bytes_escritos['silver']
        bronze = self._tamanio_bronze()
        if silver and bronze:
            self.logger.info(
                f"Tamaño: bronze JSON {bronze / 1024:.1f} KB, silver Parquet {silver / 1024:.1f} KB "
                f"({bronze / silver:.1f}x más chico), gold Parquet {self._bytes_escritos['gold'] / 1024:.1f} KB."
            )

    def run(self) -> None:
        """Ejecuta el pipeline completo. Ante una falla lanza la excepción; el punto de entrada decide cómo salir."""
        start_time = time.time()
        self.logger.info(f"--- Iniciando ciclo de ejecución del Pipeline (fecha de corrida: {self.as_of_date:%Y-%m-%d}) ---")

        self._verificar_entorno()
        self.extract()
        self._validate_schemas()
        self.transform_cleaning()
        self.transform_metrics()
        self.load()

        if 'orders' in self.datasets:
            self.logger.info(f"Registros transaccionales procesados: {len(self.datasets['orders'])}")
        self.logger.info(f"--- Pipeline finalizado exitosamente en {time.time() - start_time:.2f} segundos ---")


def main() -> int:
    """Punto de entrada: ejecuta el pipeline y devuelve el código de salida del proceso."""
    configurar_logging()
    logger = logging.getLogger("ETL_Pipeline")
    inicio = time.time()
    try:
        EcommerceETL().run()
    except PipelineError as e:
        logger.critical(f"El proceso falló tras {time.time() - inicio:.2f}s: {e}")
        return 1
    except Exception as e:
        logger.critical(f"El proceso falló tras {time.time() - inicio:.2f}s por un error no controlado: {e}", exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
