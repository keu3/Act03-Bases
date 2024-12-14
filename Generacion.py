import random
from pymongo import MongoClient

def generar_datos_aleatorios():
    nombres = ["Carlos", "María", "Pedro", "Luisa", "Juan", "Camila", "Miguel", "Sofía", "Andrés", "Valentina"]
    ciudades = ["Bogota", "Cali", "Yopal", "Bucaramanga", "Barranquilla", "Medellin", "Arauca", "Santamarta", "Pereira"]
    ubicaciones = ["Sur", "Norte"]

    # Generar una lista de 100 documentos aleatorios
    datos = [
        {
            "Nombre": random.choice(nombres),
            "Ciudad": random.choice(ciudades),
            "Ubicacion": random.choice(ubicaciones)
        }
        for _ in range(100)
    ]

    return datos

def insertar_en_mongodb():
    # Configurar la conexión a MongoDB
    cliente = MongoClient("mongodb://localhost:27017/")
    db = cliente["Usuarios"]  # Cambia el nombre de la base de datos
    coleccion = db["Clientes"]    # Cambia el nombre de la colección

    # Generar datos aleatorios
    datos = generar_datos_aleatorios()

    # Insertar los datos en la colección
    resultado = coleccion.insert_many(datos)
    print(f"Se han insertado {len(resultado.inserted_ids)} documentos correctamente.")

# Llamar a la función
if __name__ == "__main__":
    insertar_en_mongodb()
