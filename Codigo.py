import pymongo
from pymongo import MongoClient

# Configuración de MongoDB
MONGO_HOST = "localhost"
MONGO_PUERTO = "27017"
MONGO_URL = f"mongodb://{MONGO_HOST}:{MONGO_PUERTO}/"
MONGO_BASEDATOS = "Usuarios"

# Conexión a MongoDB
try:
    cliente = MongoClient(MONGO_URL, serverSelectionTimeoutMS=2000)
    cliente.server_info()  # Verifica conexión
    print("Conexión exitosa a MongoDB")
except pymongo.errors.ServerSelectionTimeoutError as error_tiempo:
    print("Error de conexión: Tiempo excedido")
    exit()
except pymongo.errors.ConnectionFailure as error_conexion:
    print("Error de conexión: Fallo al conectarse")
    exit()

# Seleccionar la base de datos
db = cliente[MONGO_BASEDATOS]

# Leer datos de la colección grande
clientes = db.Clientes.find()
total_documentos = db.Clientes.count_documents({})
procesados = 0

for cliente in clientes:
    ubicacion = cliente.get("Ubicacion")
    try:
        if ubicacion == "Sur":
            db.clientes_sur.insert_one(cliente)
        elif ubicacion == "Norte":
            db.clientes_norte.insert_one(cliente)
        else:
            print(f"Documento ignorado: Ubicación no válida - {cliente}")
    except pymongo.errors.PyMongoError as e:
        print(f"Error al insertar documento: {cliente}, Error: {e}")
    
    procesados += 1
    if procesados % 100 == 0:
        print(f"{procesados}/{total_documentos} documentos procesados.")

# Confirmar y eliminar la colección grande
conteo_sur = db.clientes_sur.count_documents({})
print(conteo_sur)
conteo_norte = db.clientes_norte.count_documents({})
print(conteo_norte)
if procesados == total_documentos:
    db.Clientes.drop()
    print("Colección grande eliminada exitosamente.")
else:
    print("Los documentos no coinciden; no se eliminó la colección grande.")

