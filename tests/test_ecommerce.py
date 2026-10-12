import io
import os
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from ecommerce import EcommerceETL, PipelineError, main


@pytest.fixture
def etl():
    """Pipeline en modo local, sin S3."""
    return EcommerceETL(data_dir="dummy_data", output_dir="dummy_output", local_mode=True)


@pytest.fixture
def etl_s3(tmp_path, monkeypatch):
    """Pipeline en modo S3 con un cliente simulado: ningún test toca AWS."""
    monkeypatch.setenv('S3_BUCKET', 'bucket-test')
    cliente = MagicMock()
    pipeline = EcommerceETL(data_dir=str(tmp_path / "data"), output_dir=str(tmp_path / "output"),
                            local_mode=False, s3_client=cliente)
    return pipeline, cliente


# --- Validación de esquemas ---

def test_validate_schemas_aborts_on_missing_critical_column(etl):
    df_incompleto = pd.DataFrame({
        'order_id': [1, 2],
        'customer_id': [101, 102],
        'order_date': ['2026-05-10', '2026-05-11'] 
    })
    etl.datasets['orders'] = df_incompleto

    with pytest.raises(PipelineError, match="Falla de contrato.*Faltan columnas críticas en 'orders'"):
        etl._validate_schemas()


# --- Data Quality & Cleaning ---

def test_transform_cleaning_handles_nulls_and_types(etl):
    """Valida el casteo de tipos y la limpieza de strings vacíos/nulos."""
    etl.datasets['orders'] = pd.DataFrame({
        'order_id': [1, 2, 3],
        'customer_id': [101, np.nan, 102],
        'total_amount': ["150.5", "200", 300.5],
        'order_date': ['2026-05-10', '2026-05-11', '2026-05-12'],
        'promotion_id': ["", "PROMO1", np.nan],
        'notes': [np.nan, "Urgente", np.nan],
        'discount_percent': [0, 10, np.nan],
        'status': ['entregado', 'entregado', 'entregado']
    })

    etl.transform_cleaning()
    df_clean = etl.datasets['orders']

    assert len(df_clean) == 2, "No se eliminaron los registros con customer_id nulo."
    assert pd.api.types.is_float_dtype(df_clean['total_amount']), "total_amount no se casteó a Float."
    assert df_clean.iloc[0]['promotion_id'] == "Sin codigo", "No se imputaron los strings vacíos."

def test_transform_cleaning_resolves_duplicates(etl):
    """Valida la idempotencia conservando el evento transaccional más reciente."""
    etl.datasets['orders'] = pd.DataFrame({
        'order_id': [99, 99],
        'customer_id': [101, 101],
        'total_amount': [100.0, 150.0],
        'order_date': ['2026-05-10 10:00:00', '2026-05-10 15:00:00'],
        'promotion_id': [np.nan, np.nan],
        'notes': [np.nan, np.nan],
        'discount_percent': [0, 0],
        'status': ['procesando', 'entregado']
    })

    etl.transform_cleaning()
    df_clean = etl.datasets['orders']

    assert len(df_clean) == 1, "No se eliminó el duplicado."
    assert df_clean.iloc[0]['total_amount'] == 150.0, "No se conservó el estado más reciente."

def test_transform_cleaning_imputes_product_prices(etl):
    """Regla de negocio: Imputación de precios nulos usando la media por categoría."""
    etl.datasets['products'] = pd.DataFrame({
        'product_id': [1, 2, 3, 4],
        'category_id': [10, 10, 10, 20],
        'price': [100.0, 200.0, np.nan, 50.0] 
        # La media de la cat 10 es 150.0. El prod_id 3 debe heredar ese valor.
    })

    etl.transform_cleaning()
    df_clean = etl.datasets['products']

    precio_imputado = df_clean[df_clean['product_id'] == 3]['price'].iloc[0]
    assert precio_imputado == 150.0, f"Se esperaba 150.0, se obtuvo {precio_imputado}"
    assert not df_clean['price'].isnull().any(), "Aún quedan precios nulos tras la limpieza."


def test_transform_cleaning_strips_text_columns(etl):
    etl.datasets['customers'] = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'first_name': ['  Martín  ', 'Camila\n', '   '],
        'email': ['  ana@email.com  ', 'luis@email.com\n', 'eva@email.com'],
    })

    etl.transform_cleaning()
    df = etl.datasets['customers']

    assert df['first_name'].iloc[0] == 'Martín'
    assert df['first_name'].iloc[1] == 'Camila'
    assert pd.isna(df['first_name'].iloc[2])
    assert list(df['email']) == ['ana@email.com', 'luis@email.com', 'eva@email.com']

def test_transform_cleaning_casts_ids_to_nullable_int(etl):
    etl.datasets['inventory'] = pd.DataFrame({
        'inventory_id': [1, 2, 3],
        'product_id': [26, '', None],
        'warehouse_id': [1, 1, 2],
        'quantity': [5, 5, 5],
    })

    etl.transform_cleaning()
    df = etl.datasets['inventory']

    assert str(df['product_id'].dtype) == 'Int64'
    assert df['product_id'].iloc[0] == 26
    assert df['product_id'].isna().sum() == 2

def test_transform_cleaning_keeps_non_numeric_id_codes(etl):
    etl.datasets['promotions'] = pd.DataFrame({'promotion_id': ['PROMO1', 'PROMO2', None]})

    etl.transform_cleaning()

    assert list(etl.datasets['promotions']['promotion_id'].dropna()) == ['PROMO1', 'PROMO2']


# --- Integridad de order_items (registros rechazados) ---

def _dataset_con_order_items_sucios(etl):
    """Orden 1 y 2 válidas, producto 10 válido; items con cada tipo de problema."""
    etl.datasets['orders'] = pd.DataFrame({
        'order_id': [1, 2],
        'customer_id': [101, 102],
        'total_amount': [100.0, 200.0],
        'order_date': ['2026-05-10', '2026-05-11'],
        'promotion_id': [np.nan, np.nan],
        'notes': [np.nan, np.nan],
        'status': ['entregado', 'entregado'],
    })
    etl.datasets['products'] = pd.DataFrame({'product_id': [10], 'category_id': [1], 'price': [50.0]})
    etl.datasets['order_items'] = pd.DataFrame({
        'order_item_id': [1, 1, 2, 3, 4, 5, 6],
        'order_id':      [1, 1, 1, None, 99, 2, 2],
        'product_id':    [10, 10, None, 10, 10, 77, 10],
        'quantity':      [1, 1, 2, 3, 4, 5, 6],
    })

def test_order_items_rejects_invalid_rows(etl):
    _dataset_con_order_items_sucios(etl)

    etl.transform_cleaning()

    validos = etl.datasets['order_items']
    rechazados = etl.rejected['order_items']

    # item 1 (duplicado, queda uno), item 2 (producto nulo pero pedido válido) y item 6
    assert sorted(validos['order_item_id']) == [1, 2, 6]
    assert dict(zip(rechazados['order_item_id'], rechazados['motivo_rechazo'])) == {
        3: 'order_id nulo',
        4: 'order_id inexistente en orders',
        5: 'product_id inexistente en products',
    }

def test_order_items_distinguishes_discarded_orders_from_true_orphans(etl):
    _dataset_con_order_items_sucios(etl)
    etl.datasets['orders'] = pd.concat([etl.datasets['orders'], pd.DataFrame({
        'order_id': [3], 'customer_id': [np.nan], 'total_amount': [50.0],  # QA: sin cliente
        'order_date': ['2026-05-12'], 'promotion_id': [np.nan], 'notes': [np.nan], 'status': ['entregado'],
    })], ignore_index=True)
    etl.datasets['order_items'] = pd.DataFrame({
        'order_item_id': [1, 2], 'order_id': [3, 99], 'product_id': [10, 10], 'quantity': [1, 1],
    })

    etl.transform_cleaning()
    rechazados = etl.rejected['order_items']

    assert dict(zip(rechazados['order_item_id'], rechazados['motivo_rechazo'])) == {
        1: 'pedido descartado en orders (customer_id o total_amount nulo)',
        2: 'order_id inexistente en orders',
    }

def test_order_items_keeps_sale_without_product_id(etl):
    _dataset_con_order_items_sucios(etl)

    etl.transform_cleaning()
    validos = etl.datasets['order_items']

    assert validos[validos['order_item_id'] == 2]['product_id'].isna().all()

def test_order_items_warns_when_rejection_rate_exceeds_threshold(etl, caplog):
    _dataset_con_order_items_sucios(etl)

    etl.transform_cleaning()

    assert "supera el umbral" in caplog.text

def test_order_items_without_problems_has_no_rejected(etl):
    _dataset_con_order_items_sucios(etl)
    etl.datasets['order_items'] = pd.DataFrame({
        'order_item_id': [1], 'order_id': [1], 'product_id': [10], 'quantity': [1],
    })

    etl.transform_cleaning()

    assert etl.rejected == {}
    assert len(etl.datasets['order_items']) == 1


# --- Normalización de estados de pedido ---

def _orders_con_estados(estados):
    n = len(estados)
    return pd.DataFrame({
        'order_id': list(range(1, n + 1)),
        'customer_id': [101] * n,
        'total_amount': [10.0] * n,
        'order_date': ['2026-05-10'] * n,
        'promotion_id': [np.nan] * n,
        'notes': [np.nan] * n,
        'status': estados,
    })

def test_transform_cleaning_normalizes_order_status(etl):
    etl.datasets['orders'] = _orders_con_estados(
        ['Cancelado', 'cancelado_v2', 'cancleado', ' ENVIADO ', 'evniado', 'entregado', 'pendiente_v2'])

    etl.transform_cleaning()

    assert list(etl.datasets['orders']['status']) == [
        'cancelado', 'cancelado', 'cancelado', 'enviado', 'enviado', 'entregado', 'pendiente']

def test_transform_cleaning_marks_unrecognized_status_as_unknown_and_keeps_original(etl, caplog):
    etl.datasets['orders'] = _orders_con_estados(['entregado', 'xyz', None])

    etl.transform_cleaning()
    pedidos = etl.datasets['orders'].sort_values('order_id')

    assert list(pedidos['status']) == ['entregado', 'desconocido', 'desconocido']
    assert list(pedidos['dq_estado_desconocido']) == [False, True, True]
    assert list(pedidos['status_original'].iloc[:2]) == ['entregado', 'xyz']
    assert "2 de 3 pedidos con estado nulo o no reconocido" in caplog.text

def test_status_typos_use_explicit_table_not_similarity(etl):
    # 'cancleado' está en la tabla; 'canceladoo' se parece mucho a 'cancelado' pero no está: queda como desconocido
    etl.datasets['orders'] = _orders_con_estados(['cancleado', 'canceladoo'])

    etl.transform_cleaning()

    assert list(etl.datasets['orders'].sort_values('order_id')['status']) == ['cancelado', 'desconocido']


# --- Depósitos e inventario ---

def test_transform_cleaning_dedups_warehouses_and_names_unnamed(etl):
    etl.datasets['warehouses'] = pd.DataFrame({
        'warehouse_id': [1, 4, 4, 5],
        'warehouse_name': ['Norte', 'Este', 'Este', None],
        'capacity_units': [100, 50, 50, 70],
    })

    etl.transform_cleaning()
    depositos = etl.datasets['warehouses'].set_index('warehouse_id')

    assert list(depositos.index) == [1, 4, 5]
    assert depositos.loc[5, 'warehouse_name'] == 'Sin nombre (id 5)'

def test_transform_cleaning_rejects_inventory_without_known_warehouse(etl):
    etl.datasets['warehouses'] = pd.DataFrame({'warehouse_id': [1], 'warehouse_name': ['Norte']})
    etl.datasets['inventory'] = pd.DataFrame({
        'inventory_id': [1, 2, 3],
        'product_id': [10, 10, 10],
        'warehouse_id': [1, None, 100000],
        'quantity': [5, 5, 5],
    })

    etl.transform_cleaning()
    rechazados = etl.rejected['inventory']

    assert list(etl.datasets['inventory']['inventory_id']) == [1]
    assert dict(zip(rechazados['inventory_id'], rechazados['motivo_rechazo'])) == {
        2: 'warehouse_id nulo',
        3: 'warehouse_id inexistente en warehouses',
    }


# --- Business Logic (Métricas OLAP) ---

def test_transform_metrics_ignores_cancelled_orders(etl):
    """Garantiza que las órdenes canceladas no distorsionen los reportes financieros."""
    etl.datasets['orders'] = pd.DataFrame({
        'order_id': [1, 2, 3],
        'customer_id': [101, 101, 101],
        'total_amount': [100.0, 200.0, 500.0],
        'order_date': pd.to_datetime(['2026-05-01', '2026-05-02', '2026-05-03']),
        'status': ['entregado', 'entregado', 'cancelado'] 
    })
    
    etl.datasets['customers'] = pd.DataFrame({
        'customer_id': [101],
        'first_name': ['Juan'],
        'last_name': ['Perez']
    })
    
    # Mocks mínimos para satisfacer las dependencias de los demás reportes
    etl.datasets['order_items'] = pd.DataFrame({'product_id': [1], 'quantity': [2]})
    etl.datasets['products'] = pd.DataFrame({'product_id': [1], 'product_name': ['TV']})
    etl.datasets['inventory'] = pd.DataFrame({'product_id': [1], 'warehouse_id': [1], 'quantity': [10]})
    etl.datasets['warehouses'] = pd.DataFrame({'warehouse_id': [1], 'warehouse_name': ['Norte'], 'capacity_units': [100], 'location': ['BA']})

    etl.transform_metrics()
    reporte = etl.reports['reporte_clientes']

    assert len(reporte) == 1
    monto_calculado = reporte[reporte['customer_id'] == 101]['total_amount'].iloc[0]
    
    assert monto_calculado == 300.0, "Se sumaron órdenes canceladas al reporte."


# --- Arquitectura Medallion sobre S3 (Bronze -> Silver -> Gold) ---

def test_verificar_entorno_aborts_without_bucket_in_s3_mode(tmp_path, monkeypatch):
    monkeypatch.delenv('S3_BUCKET', raising=False)
    pipeline = EcommerceETL(data_dir=str(tmp_path / "data"), local_mode=False, s3_client=MagicMock())

    with pytest.raises(PipelineError, match="S3_BUCKET no definida"):
        pipeline._verificar_entorno()

def test_descargar_bronze_downloads_only_ecommerce_json(etl_s3):
    pipeline, cliente = etl_s3
    cliente.get_paginator.return_value.paginate.return_value = [
        {'Contents': [
            {'Key': 'bronze/'},
            {'Key': 'bronze/ecommerce_orders.json'},
            {'Key': 'bronze/notas.txt'},
        ]},
        {'Contents': [{'Key': 'bronze/ecommerce_customers.json'}]},
    ]
    pipeline._verificar_entorno()
    pipeline._descargar_bronze()

    cliente.get_paginator.return_value.paginate.assert_called_once_with(Bucket='bucket-test', Prefix='bronze/')
    descargas = sorted(c.args[1] for c in cliente.download_file.call_args_list)
    assert descargas == ['bronze/ecommerce_customers.json', 'bronze/ecommerce_orders.json']

def test_load_local_mode_writes_silver_and_gold_parquet(tmp_path):
    pipeline = EcommerceETL(data_dir=str(tmp_path / "data"), output_dir=str(tmp_path / "output"), local_mode=True)
    pipeline.datasets['orders'] = pd.DataFrame({'order_id': [1], 'total_amount': [10.0]})
    pipeline.reports['reporte_ventas_mensual'] = pd.DataFrame({'mes': ['2026-05'], 'total_amount': [10.0]})

    pipeline.load()

    assert (tmp_path / "output" / "silver" / "orders.parquet").exists()
    assert (tmp_path / "output" / "gold" / "reporte_ventas_mensual.parquet").exists()
    # El CSV de gold es solo para los reportes: silver no se publica en CSV
    assert [f.name for f in (tmp_path / "output").rglob("*.csv")] == ['reporte_ventas_mensual.csv']

def test_load_writes_gold_csv_readable_by_excel(tmp_path):
    pipeline = EcommerceETL(data_dir=str(tmp_path / "data"), output_dir=str(tmp_path / "output"), local_mode=True)
    pipeline.reports['reporte_clientes'] = pd.DataFrame({'first_name': ['María'], 'total_amount': [10.5]})

    pipeline.load()

    ruta = tmp_path / "output" / "gold" / "reporte_clientes.csv"
    assert ruta.read_bytes().startswith(b'\xef\xbb\xbf')  # BOM: Excel respeta los acentos
    assert pd.read_csv(ruta, encoding='utf-8-sig')['first_name'].iloc[0] == 'María'

def test_load_skips_gold_csv_when_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv('GOLD_CSV', 'false')
    pipeline = EcommerceETL(data_dir=str(tmp_path / "data"), output_dir=str(tmp_path / "output"), local_mode=True)
    pipeline.reports['reporte_clientes'] = pd.DataFrame({'total_amount': [10.5]})

    pipeline.load()

    assert (tmp_path / "output" / "gold" / "reporte_clientes.parquet").exists()
    assert not list((tmp_path / "output").rglob("*.csv"))

def test_load_s3_mode_uploads_to_silver_and_gold_prefixes(etl_s3):
    pipeline, cliente = etl_s3
    pipeline.datasets['orders'] = pd.DataFrame({'order_id': [1, 2], 'total_amount': [10.0, 20.0]})
    pipeline.reports['reporte_ventas_mensual'] = pd.DataFrame({'mes': ['2026-05'], 'total_amount': [30.0]})

    pipeline.load()

    subidas = {c.kwargs['Key']: c.kwargs for c in cliente.put_object.call_args_list}
    assert set(subidas) == {'silver/orders.parquet', 'gold/reporte_ventas_mensual.parquet',
                            'gold/reporte_ventas_mensual.csv'}
    assert subidas['gold/reporte_ventas_mensual.csv']['ContentType'] == 'text/csv'
    assert all(s['Bucket'] == 'bucket-test' for s in subidas.values())

    df_subido = pd.read_parquet(io.BytesIO(subidas['silver/orders.parquet']['Body']))
    assert df_subido['total_amount'].sum() == 30.0
    assert not os.path.exists(pipeline.output_dir)

def test_load_persists_rejected_under_silver_rejected(etl_s3, tmp_path):
    pipeline, cliente = etl_s3
    pipeline.rejected['order_items'] = pd.DataFrame({'order_item_id': [3], 'motivo_rechazo': ['order_id nulo']})

    pipeline.load()
    claves = [c.kwargs['Key'] for c in cliente.put_object.call_args_list]
    assert claves == ['silver/rejected/order_items.parquet']

    local = EcommerceETL(data_dir=str(tmp_path / "d"), output_dir=str(tmp_path / "o"), local_mode=True)
    local.rejected['order_items'] = pipeline.rejected['order_items']
    local.load()
    assert (tmp_path / "o" / "silver" / "rejected" / "order_items.parquet").exists()

def test_load_skips_empty_dataframes(etl_s3):
    pipeline, cliente = etl_s3
    pipeline.datasets['orders'] = pd.DataFrame()

    pipeline.load()

    cliente.put_object.assert_not_called()


# --- Métricas: ventas mensuales, ocupación y rotación ---

def _cargar_datasets_para_metricas(etl):
    # Ventas: p1 = 6 u., p2 = 4 u. Stock: Norte 40 de 100, Sur 5 de 50, Este 0
    etl.datasets['orders'] = pd.DataFrame({
        'order_id': [1, 2, 3, 4],
        'customer_id': [101, 101, 101, 101],
        'total_amount': [100.0, 50.0, 200.0, 999.0],
        'order_date': pd.to_datetime(['2026-05-01', '2026-05-20', '2026-06-02', '2026-06-03']),
        'status': ['entregado', 'entregado', 'entregado', 'cancelado'],
    })
    etl.datasets['customers'] = pd.DataFrame({'customer_id': [101], 'first_name': ['Juan'], 'last_name': ['Perez']})
    etl.datasets['products'] = pd.DataFrame({'product_id': [1, 2, 3], 'product_name': ['TV', 'Radio', 'Cable']})
    etl.datasets['order_items'] = pd.DataFrame({
        'order_id': [1, 1, 2], 'product_id': [1, 1, 2], 'quantity': [2, 4, 4],
    })
    etl.datasets['inventory'] = pd.DataFrame({
        'product_id': [1, 2, 3, 4], 'warehouse_id': [1, 1, 2, 3], 'quantity': [10, 30, 5, 0],
    })
    etl.datasets['warehouses'] = pd.DataFrame({
        'warehouse_id': [1, 2, 3],
        'warehouse_name': ['Norte', 'Sur', 'Este'],
        'capacity_units': [100, 50, 10],
        'location': ['BA', 'CBA', 'MZA'],
    })


def test_transform_metrics_monthly_sales_excludes_cancelled(etl):
    _cargar_datasets_para_metricas(etl)

    etl.transform_metrics()
    ventas = etl.reports['reporte_ventas_mensual'].set_index('mes')['total_amount']

    assert ventas.to_dict() == {'2026-05': 150.0, '2026-06': 200.0}


def test_transform_metrics_warehouse_occupancy(etl):
    _cargar_datasets_para_metricas(etl)

    etl.transform_metrics()
    ocupacion = etl.reports['reporte_logistica_depositos'].set_index('warehouse_name')['%_ocupacion']

    assert ocupacion.to_dict() == {'Norte': 40.0, 'Sur': 10.0, 'Este': 0.0}


def test_transform_metrics_inventory_turnover_handles_zero_stock(etl):
    _cargar_datasets_para_metricas(etl)

    etl.transform_metrics()
    rotacion = etl.reports['reporte_rotacion_inventario'].set_index('warehouse_name')['indice_rotacion']

    assert rotacion.to_dict() == {'Norte': 0.25, 'Sur': 0.0, 'Este': 0.0}


def test_transform_metrics_product_reports_exclude_cancelled_orders(etl):
    _cargar_datasets_para_metricas(etl)
    # El pedido 4 está cancelado: sus 100 unidades de p2 no deben contar
    etl.datasets['order_items'] = pd.DataFrame({
        'order_id': [1, 1, 2, 4], 'product_id': [1, 1, 2, 2], 'quantity': [2, 4, 4, 100],
    })

    etl.transform_metrics()

    top = etl.reports['reporte_producto_mas_vendido']
    assert list(top['product_name']) == ['TV'] and list(top['quantity']) == [6]
    rotacion = etl.reports['reporte_rotacion_inventario'].set_index('warehouse_name')['indice_rotacion']
    assert rotacion['Norte'] == 0.25  # (6 + 4) / 40, sin las 100 unidades canceladas


def test_transform_metrics_warns_when_occupancy_exceeds_capacity(etl, caplog):
    _cargar_datasets_para_metricas(etl)
    etl.datasets['inventory'].loc[0, 'quantity'] = 500

    etl.transform_metrics()

    assert "Ocupación superior a 100% en: Norte" in caplog.text


# --- Pipeline completo en modo local ---

def test_run_local_mode_end_to_end(tmp_path):
    data_dir, output_dir = tmp_path / "data", tmp_path / "output"
    data_dir.mkdir()

    tablas = {
        'orders': [
            {'order_id': 1, 'customer_id': 101, 'total_amount': 100.0, 'order_date': '2026-05-10',
             'promotion_id': None, 'notes': None, 'status': 'entregado'},
            {'order_id': 2, 'customer_id': 102, 'total_amount': 200.0, 'order_date': '2026-05-11',
             'promotion_id': None, 'notes': None, 'status': 'entregado'},
            {'order_id': 3, 'customer_id': None, 'total_amount': 50.0, 'order_date': '2026-05-12',
             'promotion_id': None, 'notes': None, 'status': 'entregado'},
            {'order_id': 4, 'customer_id': 101, 'total_amount': 300.0, 'order_date': '2026-05-13',
             'promotion_id': None, 'notes': None, 'status': 'cancelado'},
        ],
        'customers': [
            {'customer_id': 101, 'first_name': 'Juan\n', 'last_name': 'Perez'},
            {'customer_id': 102, 'first_name': '  Ana ', 'last_name': 'Gomez'},
        ],
        'products': [
            {'product_id': 10, 'product_name': 'TV', 'category_id': 1, 'price': 500.0},
            {'product_id': 11, 'product_name': 'Radio', 'category_id': 1, 'price': None},
        ],
        'order_items': [
            {'order_item_id': 1, 'order_id': 1, 'product_id': 10, 'quantity': 2},
            {'order_item_id': 2, 'order_id': 2, 'product_id': 11, 'quantity': 1},
            {'order_item_id': 3, 'order_id': None, 'product_id': 10, 'quantity': 1},  # sin pedido
            {'order_item_id': 4, 'order_id': 3, 'product_id': 10, 'quantity': 1},     # pedido descartado
        ],
        'inventory': [{'inventory_id': 1, 'product_id': 10, 'warehouse_id': 1, 'quantity': 20}],
        'warehouses': [{'warehouse_id': 1, 'warehouse_name': 'Norte', 'capacity_units': 100, 'location': 'BA'}],
    }
    for nombre, filas in tablas.items():
        pd.DataFrame(filas).to_json(data_dir / f'ecommerce_{nombre}.json', orient='records')

    pipeline = EcommerceETL(data_dir=str(data_dir), output_dir=str(output_dir), local_mode=True)
    pipeline.run()

    silver, gold = output_dir / 'silver', output_dir / 'gold'

    orders = pd.read_parquet(silver / 'orders.parquet')
    assert sorted(orders['order_id']) == [1, 2, 4]  # el pedido sin cliente se apartó

    pedidos_rechazados = pd.read_parquet(silver / 'rejected' / 'orders.parquet')
    assert dict(zip(pedidos_rechazados['order_id'], pedidos_rechazados['motivo_rechazo'])) == {3: 'customer_id nulo'}

    customers = pd.read_parquet(silver / 'customers.parquet')
    assert sorted(customers['first_name']) == ['Ana', 'Juan']

    products = pd.read_parquet(silver / 'products.parquet')
    assert products[products['product_id'] == 11]['price'].iloc[0] == 500.0  # media de su categoría

    items = pd.read_parquet(silver / 'order_items.parquet')
    assert sorted(items['order_item_id']) == [1, 2]

    rechazados = pd.read_parquet(silver / 'rejected' / 'order_items.parquet')
    assert dict(zip(rechazados['order_item_id'], rechazados['motivo_rechazo'])) == {
        3: 'order_id nulo',
        4: 'pedido descartado en orders (customer_id o total_amount nulo)',
    }

    top_clientes = pd.read_parquet(gold / 'reporte_clientes.parquet')
    assert list(top_clientes['customer_id']) == [102, 101]
    assert list(top_clientes['total_amount']) == [200.0, 100.0]

    assert pd.read_parquet(gold / 'reporte_producto_mas_vendido.parquet')['product_name'].iloc[0] == 'TV'
    assert pd.read_parquet(gold / 'reporte_ventas_mensual.parquet')['total_amount'].iloc[0] == 300.0
    assert pd.read_parquet(gold / 'reporte_logistica_depositos.parquet')['%_ocupacion'].iloc[0] == 20.0


# --- Fechas en varios formatos, claves y alertas de rango ---

def test_transform_cleaning_parses_dates_in_mixed_formats(etl):
    etl.datasets['orders'] = _orders_con_estados(['entregado'] * 7)
    etl.datasets['orders']['order_date'] = [
        '2026-05-10', '2026-05-10 10:30:00', '30-06-2026', '06/30/2026', '2026/06/30', '04 Aug 2024', 'Jul 26, 2024']

    etl.transform_cleaning()
    fechas = etl.datasets['orders'].sort_values('order_id')['order_date']

    assert [f.strftime('%Y-%m-%d') for f in fechas] == [
        '2026-05-10', '2026-05-10', '2026-06-30', '2026-06-30', '2026-06-30', '2024-08-04', '2024-07-26']

def test_transform_cleaning_warns_about_unrecognized_date_format(etl, caplog):
    etl.datasets['orders'] = _orders_con_estados(['entregado', 'entregado'])
    etl.datasets['orders']['order_date'] = ['2026-05-10', 'ayer']

    etl.transform_cleaning()

    assert "'orders.order_date': 1 fechas con formato no reconocido" in caplog.text

def test_transform_cleaning_warns_about_non_numeric_amounts(etl, caplog):
    etl.datasets['orders'] = _orders_con_estados(['entregado', 'entregado'])
    etl.datasets['orders']['total_amount'] = ['100', 'abc']

    etl.transform_cleaning()

    assert "'orders.total_amount': 1 valores no numéricos" in caplog.text
    assert len(etl.datasets['orders']) == 1  # el monto inválido quedó nulo y el pedido se descartó

def test_transform_cleaning_dedups_tables_by_primary_key(etl):
    etl.datasets['products'] = pd.DataFrame({
        'product_id': [1, 1, 2, None], 'product_name': ['TV viejo', 'TV', 'Radio', 'Fantasma'],
        'category_id': [10, 10, 10, 10], 'price': [100.0, 120.0, 80.0, 50.0],
    })
    etl.datasets['reviews'] = pd.DataFrame({'review_id': [1, 1, 2], 'rating': [3, 5, 4]})

    etl.transform_cleaning()
    productos = etl.datasets['products'].set_index('product_id')

    assert list(productos.index) == [1, 2]
    assert productos.loc[1, 'product_name'] == 'TV'  # queda la última
    assert list(etl.datasets['reviews']['rating']) == [5, 4]

def test_transform_cleaning_keeps_latest_order_when_dates_tie(etl):
    etl.datasets['orders'] = _orders_con_estados(['pendiente', 'entregado', 'enviado'])
    etl.datasets['orders']['order_id'] = [7, 7, 7]
    etl.datasets['orders']['total_amount'] = [1.0, 2.0, 3.0]

    etl.transform_cleaning()

    assert list(etl.datasets['orders']['total_amount']) == [3.0]  # la última del orden de origen

def test_transform_cleaning_alerts_on_impossible_and_outlier_values(etl, caplog):
    etl.datasets['orders'] = _orders_con_estados(['entregado'] * 12)
    etl.datasets['orders']['total_amount'] = [100.0] * 10 + [-5.0, 900000.0]
    etl.datasets['orders']['order_date'] = ['2026-05-10'] * 11 + ['2099-01-01']
    etl.datasets['order_items'] = pd.DataFrame({
        'order_item_id': [1, 2], 'order_id': [1, 2], 'product_id': [None, None],
        'quantity': [0, 2], 'unit_price': [-1.0, 5.0],
    })

    etl.transform_cleaning()

    assert "1 pedidos con total_amount menor o igual a 0" in caplog.text
    assert "1 pedidos con monto mayor a" in caplog.text
    assert "1 pedidos con fecha posterior a la fecha de corrida" in caplog.text
    assert "1 líneas con quantity menor o igual a 0" in caplog.text
    assert "1 líneas con unit_price negativo" in caplog.text
    assert len(etl.datasets['orders']) == 12  # marcar no descarta nada

    pedidos = etl.datasets['orders'].sort_values('order_id')
    assert pedidos['dq_monto_no_positivo'].sum() == 1 and pedidos['dq_monto_atipico'].sum() == 1
    assert pedidos['dq_fecha_futura'].sum() == 1 and pedidos['dq_fecha_futura'].iloc[-1]
    items = etl.datasets['order_items'].sort_values('order_item_id')
    assert list(items['dq_cantidad_no_positiva']) == [True, False]
    assert list(items['dq_precio_negativo']) == [True, False]


# --- Cuarentena de pedidos, fecha de corrida ---

def test_orders_without_customer_or_amount_go_to_rejected_with_reason(etl):
    etl.datasets['orders'] = _orders_con_estados(['entregado'] * 4)
    etl.datasets['orders']['customer_id'] = [101, None, 101, None]
    etl.datasets['orders']['total_amount'] = [10.0, 10.0, None, None]

    etl.transform_cleaning()
    rechazados = etl.rejected['orders']

    assert list(etl.datasets['orders']['order_id']) == [1]
    assert dict(zip(rechazados['order_id'], rechazados['motivo_rechazo'])) == {
        2: 'customer_id nulo', 3: 'total_amount nulo', 4: 'customer_id y total_amount nulos'}

def test_repeated_order_with_a_valid_version_is_not_rejected(etl):
    etl.datasets['orders'] = _orders_con_estados(['entregado', 'entregado'])
    etl.datasets['orders']['order_id'] = [5, 5]
    etl.datasets['orders']['total_amount'] = [None, 80.0]

    etl.transform_cleaning()

    assert list(etl.datasets['orders']['total_amount']) == [80.0]
    assert 'orders' not in etl.rejected

def test_valid_orders_produce_no_rejected_orders(etl):
    etl.datasets['orders'] = _orders_con_estados(['entregado'] * 2)

    etl.transform_cleaning()

    assert 'orders' not in etl.rejected

def test_future_dates_are_measured_against_the_run_date(etl):
    etl.as_of_date = pd.Timestamp('2026-05-10')
    etl.datasets['orders'] = _orders_con_estados(['entregado'] * 3)
    etl.datasets['orders']['order_date'] = ['2026-05-10 18:00:00', '2026-05-11', '2026-05-09']

    etl.transform_cleaning()
    pedidos = etl.datasets['orders'].sort_values('order_id')

    # El mismo día con hora no es futuro; el día siguiente sí
    assert list(pedidos['dq_fecha_futura']) == [False, True, False]

def test_as_of_date_comes_from_parameter_or_environment(monkeypatch):
    monkeypatch.setenv('AS_OF_DATE', '2026-01-31')
    assert EcommerceETL(local_mode=True).as_of_date == pd.Timestamp('2026-01-31')
    assert EcommerceETL(local_mode=True, as_of_date='2025-12-01').as_of_date == pd.Timestamp('2025-12-01')

def test_invalid_as_of_date_fails_with_clear_message(monkeypatch):
    monkeypatch.setenv('AS_OF_DATE', 'mañana')
    with pytest.raises(PipelineError, match="AS_OF_DATE inválida"):
        EcommerceETL(local_mode=True)

def test_monthly_report_leaves_out_future_dated_orders(etl, caplog):
    _datasets_minimos_para_reportes(etl)
    etl.datasets['orders']['dq_fecha_futura'] = [False, False, False, False]
    etl.datasets['orders'].loc[1, 'dq_fecha_futura'] = True  # el pedido 2 (50.0) es de fecha futura

    etl.transform_metrics()

    ventas = etl.reports['reporte_ventas_mensual']
    assert ventas['total_amount'].iloc[0] == 130.0  # 100 + 30; sin el pedido futuro ni el que no tiene fecha
    assert "2 pedidos sin fecha válida" in caplog.text


def test_validate_schemas_reports_every_missing_table_and_column(etl):
    etl.datasets['orders'] = pd.DataFrame({'order_id': [1]})
    etl.datasets['inventory'] = pd.DataFrame({'product_id': [1]})

    with pytest.raises(PipelineError) as error:
        etl._validate_schemas()

    mensaje = str(error.value)
    assert "Dataset crítico 'customers' no encontrado" in mensaje
    assert "Faltan columnas críticas en 'orders'" in mensaje
    assert "Faltan columnas críticas en 'inventory'" in mensaje

def test_extract_logs_a_profile_of_each_table(etl, tmp_path, caplog):
    pd.DataFrame({'order_id': [1, 2], 'customer_id': [1, None]}).to_json(tmp_path / 'ecommerce_orders.json')
    etl.data_dir = str(tmp_path)

    with caplog.at_level('INFO'):
        etl.extract()

    assert "Perfil orders: 2 filas, 2 columnas. Columnas con nulos: customer_id (1)." in caplog.text


# --- Reportes: casos que antes se perdían ---

def _datasets_minimos_para_reportes(etl):
    etl.datasets['orders'] = pd.DataFrame({
        'order_id': [1, 2, 3, 4],
        'customer_id': [101, 102, 999, 101],
        'total_amount': [100.0, 50.0, 30.0, 20.0],
        'order_date': pd.to_datetime(['2026-05-01', '2026-05-02', '2026-05-03', None]),
        'status': ['entregado'] * 4,
    })
    etl.datasets['customers'] = pd.DataFrame({
        'customer_id': [101, 102], 'first_name': ['Juan', None], 'last_name': ['Perez', None],
    })
    etl.datasets['products'] = pd.DataFrame({'product_id': [1, 2], 'product_name': ['TV', 'TV']})
    etl.datasets['order_items'] = pd.DataFrame({
        'order_id': [1, 2, 3, 4], 'product_id': [1, 2, 2, None], 'quantity': [3, 4, 4, 50],
    })

def test_customer_report_keeps_customers_without_name_and_counts_orders(etl, caplog):
    _datasets_minimos_para_reportes(etl)

    etl.transform_metrics()
    reporte = etl.reports['reporte_clientes'].set_index('customer_id')

    assert list(reporte.index) == [101, 102, 999]  # 102 no tiene nombre y 999 no existe en customers
    assert reporte.loc[101, 'total_amount'] == 120.0 and reporte.loc[101, 'cantidad_ordenes'] == 2
    assert "1 pedidos de clientes que no existen en customers" in caplog.text

def test_product_report_separates_products_with_same_name_and_ignores_null_product(etl):
    _datasets_minimos_para_reportes(etl)

    etl.transform_metrics()
    top = etl.reports['reporte_producto_mas_vendido']

    # Los dos "TV" son productos distintos: el 2 vende 8 unidades y el 1 vende 3. Las 50 sin product_id no cuentan.
    assert list(top['product_id']) == [2] and list(top['quantity']) == [8]

def test_monthly_report_has_no_fake_month_for_orders_without_date(etl, caplog):
    _datasets_minimos_para_reportes(etl)

    etl.transform_metrics()
    ventas = etl.reports['reporte_ventas_mensual']

    assert list(ventas['mes']) == ['2026-05']
    assert ventas['total_amount'].iloc[0] == 180.0
    assert "1 pedidos sin fecha" in caplog.text


# --- Punto de entrada ---

def test_main_returns_1_and_logs_when_pipeline_fails(monkeypatch, tmp_path, caplog):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('LOCAL_MODE', 'true')  # no existe ./data

    assert main() == 1
    assert "Directorio origen 'data/' inexistente" in caplog.text

def test_main_returns_0_on_success(monkeypatch, tmp_path):
    datos = tmp_path / 'data'
    datos.mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('LOCAL_MODE', 'true')
    tablas = {
        'orders': {'order_id': 1, 'customer_id': 1, 'total_amount': 5.0, 'order_date': '2026-05-01',
                   'promotion_id': None, 'notes': None, 'status': 'entregado'},
        'customers': {'customer_id': 1, 'first_name': 'A', 'last_name': 'B'},
        'products': {'product_id': 1, 'product_name': 'TV', 'category_id': 1, 'price': 1.0},
        'order_items': {'order_item_id': 1, 'order_id': 1, 'product_id': 1, 'quantity': 1},
    }
    for nombre, fila in tablas.items():
        pd.DataFrame([fila]).to_json(datos / f'ecommerce_{nombre}.json', orient='records')

    assert main() == 0
