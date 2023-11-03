from kafka import KafkaConsumer
import json
from psycopg2 import connect
import datetime
import random
import string

from email.message import EmailMessage
import ssl
import smtplib


def init_db():
    conn = connect(
        dbname='t2',
        user='postgres',
        password='postgres',
        host='db',
    )
    return conn

def generar_contrasena():
    caracteres = string.ascii_letters + string.digits + string.punctuation
    contrasena = ''.join(random.choice(caracteres) for i in range(16))
    return contrasena

servidores_bootstrap = 'kafka:9092'

# Tópicos a consumir
topics = ['suscripcion', 'ventas', 'stock']

# Configurar los nombres de los grupos de consumidores
grupo_consumidores = 'grupo_consumidores'

email_sender = "kafkarandom@gmail.com"
password = "nvkz jdoa mlhs mkms" 

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
        email_reciver = msg.value["email_vendedor"]


        contrasena = generar_contrasena()

        em = EmailMessage()
        subject = "Gremio Mote Huesillero"
        body =  """ 
        Bienvenido al gremio Mote Huesillero, has sido aceptado. Tus credenciales de acceso son: 
    """
        body = body + str(email_reciver) + "\n" + str(contrasena) + "\n"
        em["From"] = email_sender
        em["To"] = email_reciver
        em["Subject"] = subject
        em.set_content(body)

        context = ssl.create_default_context()

        with smtplib.SMTP_SSL("smtp.gmail.com",465,context = context) as smtp:
            smtp.login(email_sender,password)
            smtp.sendmail(email_sender,email_reciver,em.as_string())

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