from kafka import KafkaProducer
from json import dumps
import time
import argparse
import random

servidores_bootstrap = 'kafka:9092'

productor = KafkaProducer(
    bootstrap_servers=[servidores_bootstrap],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

def enviar_mail_suscripcion(tipo_suscripcion, email):
    topic = 'suscripcion'
    print(f"Te hemos enviado un correo con las credenciales a la dirección: {email}")
    print(f"Tu tipo de suscripción es: {tipo_suscripcion}")
    print("*******************************************")

    mensaje = {
        "timestamp": int(time.time()),
        "email_vendedor": email,
        "tipo_suscripcion": tipo_suscripcion,
    }
    productor.send(topic, value=mensaje, key=tipo_suscripcion.encode('utf-8'))
    print('Enviando JSON a Kafka (topic:suscripcion): ', mensaje)
    print("*******************************************")

def reportar_ventas(tipo_suscripcion, email):
    topic = 'ventas'
    for x in range(10):
        cantidad_vendida = random.randint(1, 4)
        valor_venta = cantidad_vendida * 1500
        n_venta = x+1
        mensaje = {
            "timestamp": int(time.time()),
            "email_vendedor": email,
            "tipo_suscripcion": tipo_suscripcion,
            "n_venta": n_venta,
            "cantidad_vendida": cantidad_vendida,
            "valor_venta": valor_venta
        }
        productor.send(topic, value=mensaje, key=tipo_suscripcion.encode('utf-8'))
        print('Enviando JSON a Kafka (topic:ventas): ', mensaje)
        print("*******************************************")
        time.sleep(random.randint(5, 7))
    print("Stock agotado.")
    print("*******************************************")

def solicitar_stock(tipo_suscripcion, email):
    topic = 'stock'
    print("Stock solicitado.")
    print("*******************************************")
    mensaje = {
        "timestamp": int(time.time()),
        "email_vendedor": email,
        "tipo_suscripcion": tipo_suscripcion,
        "mensaje": 'solicitud de stock',
    }
    productor.send(topic, value=mensaje, key=tipo_suscripcion.encode('utf-8'))
    print('Enviando JSON a Kafka (topic:stock): ', mensaje)
    print("*******************************************")

def mostrar_menu(n):
    if n == 0:
        print("1. Suscribirte al servicio de mensajería")
        print("2. Salir")
    elif n == 1:
        print("1. Iniciar reporte de ventas")
        print("2. Salir")
    elif n == 2:
        print("1. Solicitar stock")
        print("2. Salir")
    print("*******************************************")

def menu(tipo_suscripcion):
    print("*******************************************")
    print(f"Menu para suscripción {tipo_suscripcion}")
    print("*******************************************")
    while True:
        mostrar_menu(0)
        opcion = input("Elige una opción: ")
        print("*******************************************")
        if opcion == '1':
            email = input("Por favor, ingresa tu dirección de correo electrónico para hacerte llegar las credenciales: ")
            print("*******************************************")
            enviar_mail_suscripcion(tipo_suscripcion, email)
            while True:
                mostrar_menu(1)
                opcion = input("Elige una opción: ")
                print("*******************************************")
                if opcion == '1':
                    reportar_ventas(tipo_suscripcion, email)
                    while True:
                        mostrar_menu(2)
                        opcion = input("Elige una opción: ")
                        print("*******************************************")
                        if opcion == '1':
                            solicitar_stock(tipo_suscripcion, email)
                            break
                        elif opcion == '2':
                            print("Saliendo...")
                            exit(0)
                        else:
                            print("Opción inválida. Por favor, elige una opción válida.")
                            print("*******************************************")
                elif opcion == '2':
                    print("Saliendo...")
                    exit(0)
                else:
                    print("Opción inválida. Por favor, elige una opción válida.")
                    print("*******************************************")
        elif opcion == '2':
            print("Saliendo...")
            exit(0)
        else:
            print("Opción inválida. Por favor, elige una opción válida.")
            print("*******************************************")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("tipo_suscripcion", type=str, help="Tipo de suscripción a utilizar")
    args = parser.parse_args()

    if args.tipo_suscripcion not in ("paid", "free"):
        print("Por favor, ingresa un tipo de suscripción válida")
        print("*******************************************")
        exit(0)
    tipo_suscripcion = args.tipo_suscripcion
    menu(tipo_suscripcion)