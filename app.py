import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import bson # Importante para manejar el tipo Decimal128 de MongoDB y otros tipos BSON

# --- Configuración de la Conexión a MongoDB ---
client = MongoClient('mongodb+srv://clasesunivalle001:PoDcHnBh13FC7vTI@cluster0.2eo714p.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client['ventas']

# --- Función auxiliar para limpiar y convertir tipos ---
def clean_and_convert(doc, schema):
    """
    Limpia y convierte tipos de datos en un documento.
    schema: dict donde la clave es el nombre de la columna y el valor es el tipo deseado (str, int, float).
    """
    new_doc = {}
    for key, value in doc.items():
        if key == "_id": # Asegurarse de manejar el _id por si acaso
            new_doc[key] = str(value)
            continue

        if key in schema:
            target_type = schema[key]
            if value is None:
                new_doc[key] = None
            elif isinstance(value, bson.Decimal128):
                new_doc[key] = float(str(value))
            elif isinstance(value, str) and target_type in (int, float):
                try:
                    new_doc[key] = target_type(value)
                except ValueError:
                    new_doc[key] = None
            else:
                try:
                    new_doc[key] = target_type(value)
                except (TypeError, ValueError):
                    new_doc[key] = None
        elif isinstance(value, bson.Decimal128):
            new_doc[key] = float(str(value))
        elif isinstance(value, bson.ObjectId): # Para cualquier ObjectId que no sea el principal _id
            new_doc[key] = str(value)
        elif isinstance(value, (dict, list)): # Otros objetos complejos, convertirlos a string si es necesario
             new_doc[key] = str(value)
        else:
            new_doc[key] = value
    return new_doc


# --- Definición de Funciones para Cada Consulta ---

def consulta1():
    pipeline = [
        {"$match": {"estado": "Entregado"}},
        {"$lookup": {
            "from": "vendedor",
            "localField": "idvendedor",
            "foreignField": "idvendedor",
            "as": "vendedor"
        }},
        {"$unwind": {"path": "$vendedor", "preserveNullAndEmptyArrays": False}},
        {"$match": {"vendedor.nombrevendedor": "Marta"}},
        {"$project": {
            "_id": 0,
            "idpedido": 1,
            "fecha": 1,
            "estado": 1,
            "nropedido": 1,
            "nombrevendedor": "$vendedor.nombrevendedor",
            "apellidopaterno": "$vendedor.apellidopaterno",
            "apellidomaterno": "$vendedor.apellidomaterno"
        }}
    ]
    schema = {
        "idpedido": str, "fecha": str, "estado": str, "nropedido": str,
        "nombrevendedor": str, "apellidopaterno": str, "apellidomaterno": str
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.pedido.aggregate(pipeline)]
    return resultados

def consulta2():
    pipeline = [
        {"$match": {"cantidad": {"$gt": 5}}},
        {"$lookup": {
            "from": "producto",
            "localField": "idproducto",
            "foreignField": "idproducto",
            "as": "producto"
        }},
        {"$unwind": {"path": "$producto", "preserveNullAndEmptyArrays": False}},
        {"$project": {
            "_id": 0,
            "iddetallepedido": 1,
            "cantidad": 1,
            "idpedido": 1,
            "numerolinea": 1,
            "nombre": "$producto.nombre",
            "descripcion": "$producto.descripcion",
            "precioventa": "$producto.precioventa"
        }}
    ]
    schema = {
        "iddetallepedido": str, "cantidad": float, "idpedido": str,
        "numerolinea": int, "nombre": str, "descripcion": str, "precioventa": float
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.detallepedido.aggregate(pipeline)]
    return resultados

def consulta3():
    pipeline = [
        {"$match": {"estado": "Cancelado"}},
        {"$group": {
            "_id": "$idvendedor",
            "total_cancelados": {"$sum": 1}
        }},
        {"$lookup": {
            "from": "vendedor",
            "localField": "_id",
            "foreignField": "idvendedor",
            "as": "vendedor"
        }},
        {"$unwind": {"path": "$vendedor", "preserveNullAndEmptyArrays": False}},
        {"$project": {
            "_id": 0,
            "idvendedor": "$_id",
            "nombrevendedor": "$vendedor.nombrevendedor",
            "apellidopaterno": "$vendedor.apellidopaterno",
            "apellidomaterno": "$vendedor.apellidomaterno",
            "total_cancelados": 1
        }}
    ]
    schema = {
        "idvendedor": str, "nombrevendedor": str, "apellidopaterno": str,
        "apellidomaterno": str, "total_cancelados": int
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.pedido.aggregate(pipeline)]
    return resultados

def consulta4():
    pipeline = [
        {"$match": {"cantidadstock": 50}},
        {"$project": {
            "_id": 0,
            "idproducto": 1,
            "nombre": 1,
            "descripcion": 1,
            "cantidadstock": 1,
            "precioventa": 1,
            "codigoProducto": 1
        }}
    ]
    schema = {
        "idproducto": str, "nombre": str, "descripcion": str,
        "cantidadstock": int, "precioventa": float, "codigoProducto": str
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.producto.aggregate(pipeline)]
    return resultados

def consulta5():
    pipeline = [
        {"$lookup": {
            "from": "pedido",
            "localField": "idcliente",
            "foreignField": "idcliente",
            "as": "pedidos"
        }},
        {"$unwind": {"path": "$pedidos", "preserveNullAndEmptyArrays": False}},
        {"$match": {"pedidos.estado": {"$ne": "Entregado"}}},
        {"$project": {
            "_id": 0,
            "idcliente": 1,
            "nombre": 1,
            "apellidopaterno": 1,
            "apellidomaterno": 1,
            "ciudad": 1,
            "ci": 1,
            "pedido_estado": "$pedidos.estado",
            "pedido_id": "$pedidos.idpedido"
        }}
    ]
    schema = {
        "idcliente": str, "nombre": str, "apellidopaterno": str,
        "apellidomaterno": str, "ciudad": str, "ci": str,
        "pedido_estado": str, "pedido_id": str
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.cliente.aggregate(pipeline)]
    return resultados

def consulta6():
    pipeline = [
        {"$group": {
            "_id": "$idcliente",
            "totalPedidos": {"$sum": 1}
        }},
        {"$lookup": {
            "from": "cliente",
            "localField": "_id",
            "foreignField": "idcliente",
            "as": "cliente"
        }},
        {"$unwind": {"path": "$cliente", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "idcliente": "$_id",
            "nombre": "$cliente.nombre",
            "apellidos": {"$concat": ["$cliente.apellidopaterno", " ", "$cliente.apellidomaterno"]},
            "totalPedidos": 1
        }}
    ]
    schema = {
        "idcliente": str, "nombre": str, "apellidos": str, "totalPedidos": int
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.pedido.aggregate(pipeline)]
    return resultados

def consulta7():
    pipeline = [
        {"$lookup": {
            "from": "detallepedido",
            "localField": "idproducto",
            "foreignField": "idproducto",
            "as": "ventas_detalles"
        }},
        {"$unwind": {
            "path": "$ventas_detalles",
            "preserveNullAndEmptyArrays": False
        }},
        {"$addFields": {
            "subtotal_venta_linea": { "$multiply": ["$ventas_detalles.cantidad", "$precioventa"] }
        }},
        {"$group": {
            "_id": "$nombre",
            "promedio_valor_por_venta_linea": {"$avg": "$subtotal_venta_linea"}
        }},
        {"$match": {"promedio_valor_por_venta_linea": {"$gt": 20}}},
        {"$project": {
            "_id": 0,
            "nombreproducto": "$_id",
            "promedio_venta": "$promedio_valor_por_venta_linea"
        }}
    ]
    schema = {
        "nombreproducto": str, "promedio_venta": float
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.producto.aggregate(pipeline)]
    return resultados

def consulta8():
    pipeline = [
        {"$lookup": {
            "from": "cliente",
            "localField": "idcliente",
            "foreignField": "idcliente",
            "as": "cliente"
        }},
        {"$unwind": {"path": "$cliente", "preserveNullAndEmptyArrays": True}},
        {"$match": {
            "estado": "Entregado",
            "fecha": {
                "$gte": pd.to_datetime("2023-12-01"),
                "$lte": pd.to_datetime("2023-12-31")
            }
        }},
        {"$project": {
            "_id": 0,
            "idpedido": 1,
            "fecha": 1,
            "estado": 1,
            "nombrecliente": "$cliente.nombre"
        }}
    ]
    schema = {
        "idpedido": str, "fecha": str, "estado": str, "nombrecliente": str
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.pedido.aggregate(pipeline)]
    return resultados

def consulta9():
    pipeline = [
        {"$group": {
            "_id": "$idpedido",
            "totalProductos": {"$sum": 1}
        }},
        {"$match": {"totalProductos": {"$gt": 3}}},
        {"$lookup": {
            "from": "pedido",
            "localField": "_id",
            "foreignField": "idpedido",
            "as": "pedido"
        }},
        {"$unwind": {"path": "$pedido", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "idpedido": "$_id",
            "totalProductos": 1,
            "fecha": "$pedido.fecha",
            "estado": "$pedido.estado"
        }}
    ]
    schema = {
        "idpedido": str, "totalProductos": int, "fecha": str, "estado": str
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.detallepedido.aggregate(pipeline)]
    return resultados

def consulta10():
    pipeline = [
        {"$match": {"precioventa": {"$gt": 10}}},
        {"$project": {
            "_id": 0,
            "nombre": 1,
            "precioventa": 1
        }}
    ]
    schema = {
        "nombre": str, "precioventa": float
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.producto.aggregate(pipeline)]
    return resultados

def consulta11():
    pipeline = [
        {"$lookup": {
            "from": "vendedor",
            "localField": "idvendedor",
            "foreignField": "idvendedor",
            "as": "pedidos"
        }},
        {"$match": {"pedidos.estado": "Pendiente"}},
        {"$project": {
            "_id": 0,
            "idvendedor": 1,
            "nombrevendedor": 1,
            "ciudad": 1,
            "pedidos_nropedido": "$pedidos.nropedido",
            "pedidos_estado": "$pedidos.estado"
        }}
    ]
    schema = {
        "idvendedor": str, "nombrevendedor": str, "ciudad": str,
        "pedidos_nropedido": str, "pedidos_estado": str
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.vendedor.aggregate(pipeline)]
    return resultados

def consulta12():
    pipeline = [
        {"$lookup": {
            "from": "producto",
            "localField": "idproducto",
            "foreignField": "idproducto",
            "as": "producto"
        }},
        {"$unwind": "$producto"},
        {"$group": {
            "_id": "$producto.idproducto",
            "nombreProducto": {"$first": "$producto.nombre"},
            "totalVentas": {
                "$sum": {
                    "$multiply": ["$cantidad", "$producto.precioventa"]
                }
            }
        }},
        {"$project": {
            "_id": 0,
            "Producto": "$nombreProducto",
            "VentasTotales": "$totalVentas" # ¡CORREGIDO!
        }}
    ]
    schema = {
        "Producto": str, "VentasTotales": float
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.detallepedido.aggregate(pipeline)]
    return resultados

def consulta13():
    pipeline = [
        {"$lookup": {
            "from": "cliente",
            "localField": "idcliente",
            "foreignField": "idcliente",
            "as": "cliente_info"
        }},
        {"$unwind": "$cliente_info"},
        {"$group": {
            "_id": "$cliente_info.ciudad",
            "cantidadPedidos": {"$sum": 1}
        }},
        {"$project": {
            "_id": 0,
            "Ciudad": "$_id",
            "CantidadPedidos": "$cantidadPedidos" # ¡CORREGIDO!
        }}
    ]
    schema = {
        "Ciudad": str, "CantidadPedidos": int
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.pedido.aggregate(pipeline)]
    return resultados

def consulta14():
    pipeline = [
        {"$lookup": {
            "from": "cliente",
            "localField": "idcliente",
            "foreignField": "idcliente",
            "as": "cliente_info"
        }},
        {"$unwind": "$cliente_info"},
        {"$lookup": {
            "from": "detallepedido",
            "localField": "idpedido",
            "foreignField": "idpedido",
            "as": "detalles"
        }},
        {"$unwind": "$detalles"},
        {"$lookup": {
            "from": "producto",
            "localField": "detalles.idproducto",
            "foreignField": "idproducto",
            "as": "producto_info"
        }},
        {"$unwind": "$producto_info"},
        {"$group": {
            "_id": "$idpedido",
            "nombreCliente": {"$first": "$cliente_info.nombre"},
            "cantidadProductos": {"$sum": "$detalles.cantidad"},
            "totalPagar": {
                "$sum": {
                    "$multiply": ["$detalles.cantidad", "$producto_info.precioventa"]
                }
            }
        }},
        {"$project": {
            "_id": 0,
            "Pedido": "$_id",
            "Cliente": "$nombreCliente",
            "CantidadTotalProductos": "$cantidadProductos",
            "TotalAPagar": 1
        }}
    ]
    schema = {
        "Pedido": str, "Cliente": str, "CantidadTotalProductos": float, "TotalAPagar": float
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.pedido.aggregate(pipeline)]
    return resultados


def consulta15():
    pipeline = [
        {"$lookup": {
            "from": "vendedor",
            "localField": "idvendedor",
            "foreignField": "idvendedor",
            "as": "vendedor"
        }},
        {"$unwind": "$vendedor"},
        {"$lookup": {
            "from": "detallepedido",
            "localField": "idpedido",
            "foreignField": "idpedido",
            "as": "detalle"
        }},
        {"$unwind": "$detalle"},
        {"$lookup": {
            "from": "producto",
            "localField": "detalle.idproducto",
            "foreignField": "idproducto",
            "as": "producto"
        }},
        {"$unwind": "$producto"},
        {"$project": {
            "nombreVendedor": "$vendedor.nombrevendedor",
            "comision": "$vendedor.comision",
            "ingreso": {
                "$multiply": [
                    "$producto.precioventa",
                    "$detalle.cantidad",
                    "$vendedor.comision"
                ]
            }
        }},
        {"$group": {
            "_id": "$nombreVendedor",
            "totalComision": {"$sum": "$ingreso"}
        }},
        {"$sort": {"totalComision": -1}},
        {"$limit": 1},
        {"$project": {
            "_id": 0,
            "nombreVendedor": "$_id",
            "totalComision": 1
        }}
    ]
    schema = {
        "nombreVendedor": str, "totalComision": float
    }
    resultados = [clean_and_convert(doc, schema) for doc in db.pedido.aggregate(pipeline)]
    return resultados


# --- Diccionario para mapear nombres de consulta a funciones ---
consultas = {
    "Consulta 1: Pedidos entregados por Marta": consulta1,
    "Consulta 2: Detalles de pedidos con cantidad mayor a 5": consulta2,
    "Consulta 3: Total de pedidos cancelados por vendedor": consulta3,
    "Consulta 4: Productos con 50 unidades en stock": consulta4,
    "Consulta 5: Clientes con pedidos no entregados": consulta5,
    "Consulta 6: Total de pedidos por cliente": consulta6,
    "Consulta 7: Productos con precio de venta promedio mayor a 20": consulta7,
    "Consulta 8: Pedidos entregados en diciembre de 2023": consulta8,
    "Consulta 9: Pedidos con más de 3 productos": consulta9,
    "Consulta 10: Productos con precio de venta mayor a 10": consulta10,
    "Consulta 11: Vendedores con pedidos pendientes": consulta11,
    "Consulta 12: Ventas totales por producto": consulta12,
    "Consulta 13: Cantidad de pedidos por ciudad": consulta13,
    "Consulta 14: Detalles de pedidos y el total a pagar por cliente": consulta14,
    "Consulta 15: Vendedor con la mayor comisión": consulta15
}

# --- Interfaz de Usuario de Streamlit ---
st.set_page_config(layout="wide")
st.title("📊 Dashboard de Análisis de Ventas (MongoDB)")

st.sidebar.header("Opciones de Consulta")
consulta_seleccionada = st.sidebar.selectbox("Elige la consulta a ejecutar:", list(consultas.keys()))

st.write(f"### Resultados para: **{consulta_seleccionada}**")

if st.button("Ejecutar Consulta"):
    try:
        # Ejecutar la función de consulta seleccionada
        resultados = consultas[consulta_seleccionada]()

        # --- Punto de Depuración 1: Mostrar los resultados crudos de MongoDB ---
        st.subheader("Datos Crudos de MongoDB (para depuración):")
        if resultados:
            st.json(resultados[:10]) # Muestra los primeros 10 documentos
            if len(resultados) > 10:
                st.info(f"Mostrando solo los primeros 10 de {len(resultados)} documentos para depuración.")
        else:
            st.info("La consulta no retornó resultados.")

        if resultados:
            df = pd.DataFrame(resultados)
        else:
            df = pd.DataFrame()

        # --- Punto de Depuración 2: Mostrar el DataFrame y sus tipos de datos ---
        st.subheader("DataFrame después de pd.DataFrame(resultados) (para depuración):")
        if not df.empty:
            st.dataframe(df, use_container_width=True)
            st.write("Tipos de datos del DataFrame:")
            st.write(df.dtypes)
        else:
            st.info("El DataFrame está vacío.")


        # --- Procesamiento específico para el gráfico de Consulta 12 ---
        if consulta_seleccionada == "Consulta 12: Ventas totales por producto":
            if 'VentasTotales' in df.columns:
                df['VentasTotales'] = pd.to_numeric(df['VentasTotales'], errors='coerce')
                df.dropna(subset=['VentasTotales'], inplace=True)
            if 'Producto' in df.columns:
                df['Producto'] = df['Producto'].astype(str)

            if not df.empty and 'Producto' in df.columns and 'VentasTotales' in df.columns and not df['VentasTotales'].isnull().all():
                df_sorted = df.sort_values(by='VentasTotales', ascending=False).head(10)
                st.subheader("Gráfico de Ventas Totales por Producto (Top 10)")
                fig = px.bar(df_sorted, x="Producto", y="VentasTotales",
                             title="Ventas Totales por Producto (Top 10)",
                             labels={"Producto": "Producto", "VentasTotales": "Ventas Totales ($)"},
                             color="VentasTotales")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hay datos numéricos válidos o columnas esperadas para generar el gráfico de ventas totales por producto después de la limpieza.")


        # --- Procesamiento específico para el gráfico de Consulta 13 ---
        elif consulta_seleccionada == "Consulta 13: Cantidad de pedidos por ciudad":
            if 'CantidadPedidos' in df.columns:
                df['CantidadPedidos'] = pd.to_numeric(df['CantidadPedidos'], errors='coerce')
                df.dropna(subset=['CantidadPedidos'], inplace=True)
            if 'Ciudad' in df.columns:
                df['Ciudad'] = df['Ciudad'].astype(str)

            if not df.empty and 'Ciudad' in df.columns and 'CantidadPedidos' in df.columns and not df['CantidadPedidos'].isnull().all():
                st.subheader("Gráfico de Cantidad de Pedidos por Ciudad")
                fig = px.pie(df, values="CantidadPedidos", names="Ciudad",
                             title="Distribución de Pedidos por Ciudad")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hay datos válidos o columnas esperadas para generar el gráfico de cantidad de pedidos por ciudad.")


        # --- Generación de Gráficos Específicos (otras consultas) ---
        elif consulta_seleccionada == "Consulta 3: Total de pedidos cancelados por vendedor":
            st.subheader("Gráfico de Pedidos Cancelados por Vendedor")
            if 'total_cancelados' in df.columns and 'nombrevendedor' in df.columns:
                df['total_cancelados'] = pd.to_numeric(df['total_cancelados'], errors='coerce')
                df.dropna(subset=['total_cancelados'], inplace=True)
                if not df.empty and not df['total_cancelados'].isnull().all():
                    fig = px.bar(df, x="nombrevendedor", y="total_cancelados",
                                 title="Pedidos Cancelados por Vendedor",
                                 labels={"nombrevendedor": "Vendedor", "total_cancelados": "Pedidos Cancelados"},
                                 color="nombrevendedor")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No hay datos numéricos válidos para generar el gráfico de pedidos cancelados por vendedor.")
            else:
                 st.info("Columnas 'total_cancelados' o 'nombrevendedor' no encontradas para el gráfico.")


        elif consulta_seleccionada == "Consulta 6: Total de pedidos por cliente":
            st.subheader("Gráfico de Total de Pedidos por Cliente (Top 10)")
            if 'nombre' in df.columns and 'apellidos' in df.columns and 'totalPedidos' in df.columns:
                df['Cliente Completo'] = df['nombre'].fillna('') + ' ' + df['apellidos'].fillna('')
                df['totalPedidos'] = pd.to_numeric(df['totalPedidos'], errors='coerce')
                df.dropna(subset=['totalPedidos'], inplace=True)
                if not df.empty and not df['totalPedidos'].isnull().all():
                    df_sorted = df.sort_values(by='totalPedidos', ascending=False).head(10)
                    fig = px.bar(df_sorted, x="Cliente Completo", y="totalPedidos",
                                 title="Total de Pedidos por Cliente (Top 10)",
                                 labels={"Cliente Completo": "Cliente", "totalPedidos": "Cantidad de Pedidos"},
                                 color="totalPedidos")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No hay datos numéricos válidos para generar el gráfico de total de pedidos por cliente.")
            else:
                st.info("Columnas 'nombre', 'apellidos' o 'totalPedidos' no encontradas para el gráfico.")


        elif consulta_seleccionada == "Consulta 7: Productos con precio de venta promedio mayor a 20":
            st.subheader("Gráfico de Promedio de Valor por Línea de Venta de Producto")
            if 'promedio_venta' in df.columns and 'nombreproducto' in df.columns:
                df['promedio_venta'] = pd.to_numeric(df['promedio_venta'], errors='coerce')
                df.dropna(subset=['promedio_venta'], inplace=True)
                if not df.empty and not df['promedio_venta'].isnull().all():
                    fig = px.bar(df, x="nombreproducto", y="promedio_venta",
                                 title="Promedio de Valor por Línea de Venta de Producto (> 20)",
                                 labels={"nombreproducto": "Producto", "promedio_venta": "Promedio de Valor por Línea de Venta"},
                                 color="promedio_venta")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No hay datos numéricos válidos para generar el gráfico de promedio de venta por producto.")
            else:
                st.info("Columnas 'promedio_venta' o 'nombreproducto' no encontradas para el gráfico.")

    except Exception as e:
        st.error(f"Ocurrió un error al ejecutar la consulta: {e}")
        st.warning("Verifica tu conexión a MongoDB, la estructura de tus datos o los filtros de la consulta.")