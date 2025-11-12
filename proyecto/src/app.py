import os
import time
from flask import Flask, render_template, request, jsonify
from pymongo import MongoClient
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime


# Carga el archivo .env
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

app = Flask(__name__)

# --- Variables de Entorno ---
ATLAS_URI = os.getenv('MONGODB_ATLAS_URI')
LOCAL_URI = os.getenv('MONGODB_LOCAL_URI')

# --- Variables globales ---
client_atlas = None
client_local = None
db_atlas = None
db_local = None
sensor1_collection = None
sensor2_collection = None
sensor3_collection = None


def init_mongodb_connection():
    """Inicializa la conexión a MongoDB Atlas y Local."""
    global client_atlas, client_local, db_atlas, db_local
    global sensor1_collection, sensor2_collection, sensor3_collection

    ATLAS_CONNECTION_OPTS = {'serverSelectionTimeoutMS': 5000, 'uuidRepresentation': 'standard'}

    # --- Conexión a Atlas ---
    if ATLAS_URI:
        try:
            client_atlas = MongoClient(ATLAS_URI, **ATLAS_CONNECTION_OPTS)
            client_atlas.admin.command('ping')
            db_atlas = client_atlas.get_database("DatosSensores")

            # Colecciones específicas
            sensor1_collection = db_atlas["Sensor_1"]
            sensor2_collection = db_atlas["Sensor_2"]
            sensor3_collection = db_atlas["Sensor_3"]

            print(f"✅ Conexión ATLAS exitosa. Base de datos: {db_atlas.name}")
            print(f"   ├─ Colección 1: {sensor1_collection.name}")
            print(f"   ├─ Colección 2: {sensor2_collection.name}")
            print(f"   └─ Colección 3: {sensor3_collection.name}")

        except Exception as e:
            print(f"❌ Error de conexión a MongoDB ATLAS: {e}")

    # --- Conexión Local ---
    if LOCAL_URI:
        try:
            client_local = MongoClient(LOCAL_URI, serverSelectionTimeoutMS=5000)
            client_local.admin.command('ping')
            db_local = client_local.get_database("localDB")
            print(f"✅ Conexión LOCAL exitosa. Base de datos: {db_local.name}")
        except Exception as e:
            print(f"❌ Error de conexión a MongoDB LOCAL: {e}")


# Inicializar al cargar la app
init_mongodb_connection()



# --- Ruta de prueba: realiza operaciones en Atlas y en la BD local ---
@app.route('/vamos')
def vamos():
    message_atlas = "❌ No conectado a Atlas"
    message_local = "❌ No conectado a la BD local"

    try:
        # --- Operación en MongoDB Atlas ---
        if db_atlas:
            log_doc = {
                "message": "Operación Flask en MongoDB Atlas (DatosSensores)",
                "timestamp": datetime.now().isoformat()
            }
            db_atlas["Log_Operaciones"].insert_one(log_doc)
            message_atlas = f"✅ Documento insertado en {db_atlas.name}.Colección: Log_Operaciones"

        # --- Operación en MongoDB Local ---
        if db_local:
            db_local.local_data.update_one(
                {"_id": "contador"},
                {"$inc": {"count": 1}, "$set": {"last_update": datetime.now().isoformat()}},
                upsert=True
            )
            data = db_local.local_data.find_one({"_id": "contador"})
            count = data.get("count", 0)
            message_local = f"✅ Contador local actualizado correctamente: {count}"

        return jsonify({
            "status": "success",
            "atlas_operation": message_atlas,
            "local_operation": message_local
        }), 200

    except Exception as e:
        print(f"❌ Error en /vamos: {e}")
        return jsonify({
            "status": "error",
            "message": f"Error interno: {e}"
        }), 500


@app.route('/index')
def index():
    return render_template('index.html')



@app.route('/TestInsert')
def test_insert():
    try:
        # Simulación de datos de prueba
        sensores = [
            {"codigosensor": 1, "estado": 1, "TipoEjecucion": datetime.now().isoformat()},
            {"codigosensor": 2, "estado": 0, "TipoEjecucion": datetime.now().isoformat()},
            {"codigosensor": 3, "estado": 1, "TipoEjecucion": datetime.now().isoformat()},
        ]

        colecciones = {
            1: db_atlas.Sensor_1,
            2: db_atlas.Sensor_2,
            3: db_atlas.Sensor_3
        }

        insertados = []

        for sensor in sensores:
            resultado = colecciones[sensor["codigosensor"]].insert_one(sensor)
            insertados.append({
                "coleccion": colecciones[sensor["codigosensor"]].name,
                "insertado_id": str(resultado.inserted_id),
                "documento": sensor
            })

        return jsonify({
            "status": "ok",
            "mensaje": "Documentos insertados correctamente en Atlas",
            "detalles": insertados
        }), 201

    except Exception as e:
        print(f"❌ Error al insertar: {e}")
        return jsonify({"status": "error", "mensaje": str(e)}), 500


@app.route('/receive_sensor_data', methods=['POST'])
def receive_sensor_data():
    sensor1_collection = db_atlas.p1

    if sensor1_collection is None:
        return jsonify({"error": "La conexión a la base de datos no está establecida."}), 503

    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No se proporcionó un payload JSON"}), 400

        # 🔍 Log detallado de datos recibidos y sus tipos
        print("📦 Datos recibidos del ESP32:")
        for key, value in data.items():
            print(f"   ➜ {key}: {value} (tipo: {type(value).__name__})")

        sensor_type = data.get('sensor_type')
        value = data.get('value')
        unit = data.get('unit', 'N/A')

        if sensor_type is None or value is None:
            return jsonify({"error": "Faltan campos obligatorios: 'sensor_type' o 'value'"}), 400

        # Documento a insertar
        doc_to_insert = {
            "sensor": sensor_type,
            "valor": value,
            "unidad": unit,
            "timestamp": datetime.now()
        }

        # Inserción en MongoDB
        sensor1_collection.insert_one(doc_to_insert)
        print("✅ Documento guardado exitosamente en MongoDB.")

        if "_id" in doc_to_insert:
            del doc_to_insert["_id"]


        # Convertir datetime a ISO para que sea legible en JSON
        safe_doc = {
            k: (v.isoformat() if isinstance(v, datetime) else v)
            for k, v in doc_to_insert.items()
        }

        return jsonify({
            "status": "success",
            "message": "Dato de sensor recibido y guardado exitosamente.",
            "data_received": safe_doc
        }), 201

    except Exception as e:
        print(f"❌ Error al procesar los datos del sensor: {e}")
        return jsonify({"status": "error", "message": f"Error interno del servidor: {e}"}), 500


    
@app.route('/tabla')
def tabla():
    usuarios = list(db_atlas.p1.find({}, {"_id": 0}))
    return render_template('tabla.html', usuarios=usuarios)

if __name__ == '__main__':
    # Usar el puerto 5000 (mapeado a 5000 por docker-compose) y host='0.0.0.0'
    app.run(host='0.0.0.0', port=6001, debug=True)