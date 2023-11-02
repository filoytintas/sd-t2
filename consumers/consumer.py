from kafka import KafkaConsumer
import json
from psycopg2 import connect
import datetime

def init_db():
    conn = connect(
        dbname='t2',
        user='postgres',
        password='postgres',
        host='db',
    )
    return conn

servidores_bootstrap = 'kafka:9092'

# Tópicos a consumir
topics = ['suscripcion', 'ventas', 'stock']

# Configurar los nombres de los grupos de consumidores
grupo_consumidores = 'grupo_consumidores'

# Configurar el consumidor con el group_id y añadir deserializador de json
consumer = KafkaConsumer(
    *topics,
    group_id=grupo_consumidores,
    bootstrap_servers=[servidores_bootstrap],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Inicializar diccionario de contadores por correo electrónico
contadores = {}

# Consumir mensajes de los topics

conn = init_db()
for msg in consumer:
    email = msg.value.get("email")
    t_s = datetime.datetime.fromtimestamp(msg.value["timestamp"])
    if email not in contadores:
        contadores[email] = {"ventas_totales": 0, "ganancias_totales": 0}
    
    cur = conn.cursor()    
    if msg.topic == 'suscripcion':
        insert_query = """INSERT INTO solicitudes_suscripcion(ts, email_vendedor, tipo_suscripcion)
             VALUES(%s, %s, %s);"""
        insert_query_values = (t_s, msg.value["email_vendedor"], msg.value["tipo_suscripcion"])
        cur.execute(insert_query, insert_query_values)
        conn.commit()
        if msg.value["tipo_suscripcion"] == "paid":
            print("Procesando solicitud de suscripción paga:", msg.value)
        elif msg.value["tipo_suscripcion"] == "free":
            print("Procesando solicitud de suscripción gratuita:", msg.value)
    elif msg.topic == 'ventas':
        insert_query = """INSERT INTO reporte_ventas(ts, email_vendedor, tipo_suscripcion, n_venta, cantidad_vendida, valor_venta)
             VALUES(%s, %s, %s, %s, %s, %s);"""
        insert_query_values = (t_s, msg.value["email_vendedor"], msg.value["tipo_suscripcion"], msg.value["n_venta"], msg.value["cantidad_vendida"], msg.value["valor_venta"])
        cur.execute(insert_query, insert_query_values)
        conn.commit()
        # Actualizar el contador del productor correspondiente
        contadores[email]["ventas_totales"] += msg.value["cantidad_vendida"]
        contadores[email]["ganancias_totales"] += msg.value["valor_venta"]
        print(f"Ventas totales para {msg.value['email_vendedor']}: {contadores[email]['ventas_totales']}") 
        print(f"Ganancias totales para {msg.value['email_vendedor']}: ${contadores[email]['ganancias_totales']}")

    elif msg.topic == 'stock':
        insert_query = """INSERT INTO solicitudes_stock(ts, email_vendedor, tipo_suscripcion, mensaje)
             VALUES(%s, %s, %s, %s);"""
        insert_query_values = (t_s, msg.value["email_vendedor"], msg.value["tipo_suscripcion"], msg.value["mensaje"])
        cur.execute(insert_query, insert_query_values)
        conn.commit()
        if msg.value["tipo_suscripcion"] == "paid":
            print("Procesando solicitud de stock en suscripción paga:", msg.value)
        elif msg.value["tipo_suscripcion"] == "free":
            print("Procesando solicitud de stock en suscripción gratuita:", msg.value)
