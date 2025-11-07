import os
import time
from flask import Flask, render_template, request, jsonify
from pymongo import MongoClient
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime


# Carga el archivo data.env para ejecución local (fuera de Docker)
# En Docker, las variables son inyectadas por docker-compose
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

app = Flask(__name__)

# --- Variables de Entorno (Tomadas desde data.env / Docker Compose) ---
# ATLAS_URI es la conexión externa (SRV)
ATLAS_URI = os.getenv('MONGODB_ATLAS_URI') 
# LOCAL_URI es la conexión interna a la red de Docker (al servicio mongo_db)
LOCAL_URI = os.getenv('MONGODB_LOCAL_URI') 

# --- Clientes de Conexión Globales ---
client_atlas = None
client_local = None
db_atlas = None
db_local = None
sensor1_collection = None

def init_mongodb_connection():
    """Inicializa la conexión a Atlas (externa) y a la base de datos local (interna de Docker)."""
    global client_atlas, client_local, db_atlas, db_local
    
    # Parámetros comunes de conexión para Atlas
    ATLAS_CONNECTION_OPTS = {'serverSelectionTimeoutMS': 5000, 'uuidRepresentation': 'standard'}
    
    # --- Conexión 1: MongoDB Atlas (Usa la URI SRV) ---
    if ATLAS_URI:
        try:
            # Aquí es donde se resuelve la conexión a Atlas.
            client_atlas = MongoClient(ATLAS_URI, **ATLAS_CONNECTION_OPTS)
            client_atlas.admin.command('ping') # Prueba la conexión
            # Nota: Cambia "ClusterPeoyect" por el nombre de tu base de datos principal en Atlas
            db_atlas = client_atlas.get_database("ClusterP1") 
            sensor1_collection = client_atlas.db.p1 
            print(f"✅ Conexión ATLAS exitosa. Base de datos: {db_atlas.name}")
        except Exception as e:
            print(f"❌ Error de conexión a MongoDB ATLAS: {e}")

    # --- Conexión 2: MongoDB Local (Usa la URI interna de Docker) ---
    # LOCAL_URI se construye en docker-compose con la dirección del servicio 'mongo_db'
    if LOCAL_URI:
        try:
            client_local = MongoClient(LOCAL_URI, serverSelectionTimeoutMS=5000)
            client_local.admin.command('ping') # Prueba la conexión
            # Nombre de la BD local
            db_local = client_local.get_database("localDB") 
            print(f"✅ Conexión LOCAL exitosa. Base de datos: {db_local.name}")
        except Exception as e:
            print(f"❌ Error de conexión a MongoDB LOCAL: {e}")

# Iniciar las conexiones al cargar la aplicación
init_mongodb_connection()

# --- Rutas de la API ---

@app.route('/')
def hola():
    return 'Hola mundo'

@app.route('/status', methods=['GET'])
def check_db_status():
    """Verifica el estado de ambas conexiones."""
    status = {}
    
    # Revisar Atlas
    if db_atlas is not None:
        try:
            db_atlas.command('ping')
            status['atlas'] = {"status": "ok", "db": db_atlas.name}
        except Exception:
            status['atlas'] = {"status": "error", "message": "Conexión Atlas perdida. (Verificar IP y credenciales)"}
    else:
        status['atlas'] = {"status": "error", "message": "Atlas no inicializado."}

    # Revisar Local
    if db_local:
        try:
            db_local.command('ping')
            status['local'] = {"status": "ok", "db": db_local.name}
        except Exception:
            status['local'] = {"status": "error", "message": "Conexión Local perdida. (Verificar servicio 'mongo_db')"}
    else:
        status['local'] = {"status": "error", "message": "Local no inicializado."}

    
    return jsonify({
        "status_general": "OK" if status['atlas']['status'] == 'ok' and status['local']['status'] == 'ok' else "FALLO",
        "conexiones": status
    }), 200

# Ejemplo de ruta que realiza operaciones en ambas bases de datos
@app.route('/vamos')
def vamos():
    message_atlas = "No conectado"
    message_local = "No conectado"
    
    if db_atlas:
        db_atlas.log_collection.insert_one({"message": "Operación Flask en Atlas", "timestamp": time.time()})
        message_atlas = "Documento insertado en Atlas"
    
    if db_local:
        # Recuperar y actualizar un documento de ejemplo en la BD local
        db_local.local_data.update_one(
            {"_id": "contador"},
            {"$inc": {"count": 1}, "$set": {"last_update": time.time()}},
            upsert=True
        )
        data = db_local.local_data.find_one({"_id": "contador"})
        message_local = f"Contador local actualizado: {data.get('count', 0)}"
        
    return jsonify({
        "atlas_operation": message_atlas,
        "local_operation": message_local
    })

@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/insertar')
def insertar():
    datos = {
        'sensor':'proximidad',
        'valor': 123,
        'timestamp': time.time()

    }
    resultado = db_atlas.p1.insert_one(datos)
    return jsonify({"insertado_id": str(resultado.inserted_id)})

@app.route('/receive_sensor_data', methods=['POST'])
def receive_sensor_data():
    if sensor1_collection is None:
        
        return jsonify({"error": "La conexión a la base de datos no está establecida."}), 503

    try:
        # Obtener los datos JSON
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No se proporcionó un payload JSON"}), 400

        
        sensor_type = data.get('sensor_type')
        value = data.get('value')
        unit = data.get('unit', 'N/A') 

        if sensor_type is None or value is None:
            return jsonify({"error": "Faltan campos obligatorios: 'sensor_type' o 'value'"}), 400

        
        doc_to_insert = {
            "sensor": sensor_type,
            "valor": value,
            "unidad": unit,
            "timestamp": datetime.now() 
        }

        
        result = sensor1_collection.insert_one(doc_to_insert)


        return jsonify({
            "status": "success",
            "message": "Dato de sensor recibido y guardado exitosamente.",
            "id_mongo": str(result.inserted_id),
            "data_received": doc_to_insert
        }), 201
    except Exception as e:
        print(f"Error al procesar los datos del sensor: {e}")
        return jsonify({"status": "error", "message": f"Error interno del servidor: {e}"}), 500

if __name__ == '__main__':
    # Usar el puerto 5000 (mapeado a 5000 por docker-compose) y host='0.0.0.0'
    app.run(host='0.0.0.0', port=5001, debug=True)