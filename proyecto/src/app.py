import json
import os
import random
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from bson.objectid import ObjectId
from dateutil import parser
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request
from pymongo import MongoClient



# --- Serializador JSON personalizado para manejar ObjectId y datetime ---
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            # Convierte datetime a string legible
            return o.isoformat()
        return super().default(o)


# Carga el archivo .env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

app = Flask(__name__)
# Aplicar el encoder personalizado a la aplicación Flask
app.json_encoder = JSONEncoder


# --- Variables de Entorno ---
ATLAS_URI = os.getenv('MONGODB_ATLAS_URI')
LOCAL_URI = os.getenv('MONGODB_LOCAL_URI')

# --- Variables globales de Conexión y Colecciones ---
client_atlas = None
client_local = None
db_atlas = None
db_local = None
sensor1_collection = None
sensor2_collection = None
sensor3_collection = None
COLECCIONES_MAP = {} # Mapa para Grafana


def init_mongodb_connection():
    """Inicializa la conexión a MongoDB Atlas y Local."""
    global client_atlas, client_local, db_atlas, db_local
    global sensor1_collection, sensor2_collection, sensor3_collection
    global COLECCIONES_MAP

    ATLAS_CONNECTION_OPTS = {'serverSelectionTimeoutMS': 5000, 'uuidRepresentation': 'standard'}

    # --- Conexión a Atlas ---
    if ATLAS_URI:
        try:
            client_atlas = MongoClient(ATLAS_URI, **ATLAS_CONNECTION_OPTS)
            client_atlas.admin.command('ping')
            db_atlas = client_atlas.get_database("DatosSensores")

            # Colecciones específicas de Grafana/Sensores
            sensor1_collection = db_atlas["Sensor_1"]
            sensor2_collection = db_atlas["Sensor_2"]
            sensor3_collection = db_atlas["Sensor_3"]

            # Llenar el mapa para Grafana
            COLECCIONES_MAP = {
                "Sensor_1": sensor1_collection,
                "Sensor_2": sensor2_collection,
                "Sensor_3": sensor3_collection,
            }

            print(f"✅ Conexión ATLAS exitosa. Base de datos: {db_atlas.name}")
            print(f"   ├─ Colección 1: {sensor1_collection.name}")
            print(f"   ├─ Colección 2: {sensor2_collection.name}")
            print(f"   └─ Colección 3: {sensor3_collection.name}")

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


# --------------------------------------------------------
#               RUTAS DE PRUEBA Y DATOS RAW
# --------------------------------------------------------

@app.route('/vamos')
def vamos():
    """Ruta de prueba de operaciones en Atlas y Local."""
    message_atlas = "❌ No conectado a Atlas"
    message_local = "❌ No conectado a la BD local"

    try:
        # --- Operación en MongoDB Atlas ---
        if db_atlas is not None:
            log_doc = {
                "message": "Operación Flask en MongoDB Atlas (DatosSensores)",
                "timestamp": datetime.now().isoformat()
            }
            db_atlas["Log_Operaciones"].insert_one(log_doc)
            message_atlas = f"✅ Documento insertado en {db_atlas.name}.Colección: Log_Operaciones"

        # --- Operación en MongoDB Local (si está conectada) ---
        if db_local is not None:
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
def template_index():
    return render_template('index.html')


@app.route('/TestInsert')
def test_insert():
    """Inserta datos de prueba en las colecciones de sensores."""
    try:
        if db_atlas is None:

            return jsonify({"status": "error", "mensaje": "No hay conexión a Atlas"}), 500

        sensores = [
            # Se usa 'estado' para coincidir con la lógica de /receive_sensor_data y /query
            {"codigosensor": 1, "estado": 1, "TipoEjecucion": datetime.now()},
            {"codigosensor": 2, "estado": 0, "TipoEjecucion": datetime.now()},
            {"codigosensor": 3, "estado": 1, "TipoEjecucion": datetime.now()},
        ]

        colecciones_insert = {
            1: db_atlas.Sensor_1,
            2: db_atlas.Sensor_2,
            3: db_atlas.Sensor_3
        }

        insertados = []

        for sensor in sensores:
            resultado = colecciones_insert[sensor["codigosensor"]].insert_one(sensor)

            insertados.append({
                "coleccion": colecciones_insert[sensor["codigosensor"]].name,
                "insertado_id": resultado.inserted_id,
                "documento": sensor
            })

        respuesta = {
            "status": "ok",
            "mensaje": "Documentos insertados correctamente en Atlas",
            "detalles": insertados
        }

        return Response(
            json.dumps(respuesta, cls=JSONEncoder),
            mimetype="application/json",
            status=201
        )

    except Exception as e:
        print(f"❌ Error al insertar: {e}")
        return Response(
            json.dumps({"status": "error", "mensaje": str(e)}, cls=JSONEncoder),
            mimetype="application/json",
            status=500
        )


def serialize_mongo_doc(doc):
    """
    Convierte ObjectId y datetime a tipos serializables por JSON.
    """
    safe_doc = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            safe_doc[k] = str(v)
        elif isinstance(v, datetime):
            safe_doc[k] = v.isoformat()
        else:
            safe_doc[k] = v
    return safe_doc

@app.route('/receive_sensor_data', methods=['POST'])
def receive_sensor_data():
    """
    Ruta para recibir datos JSON desde un ESP32 o dispositivo IoT.
    Se asegura de que el estado de ocupación se guarde como 'estado' (0 o 1).
    Acepta 'simulated_timestamp_ms' para guardar datos históricos.
    """
    try:
        if db_atlas is None:
            return jsonify({"status": "error", "mensaje": "No hay conexión a Atlas"}), 500

        data = request.get_json()

        if not data or "codigosensor" not in data:
            return jsonify({"status": "error", "mensaje": "Falta el campo 'codigosensor'"}), 400

        codigosensor = int(data["codigosensor"])
        
        # 1. Extraer el estado de ocupación. Se busca en 'estado' o 'value' (0 o 1).
        estado_raw = data.get("estado", data.get("value"))
        
        # 2. Manejar el tiempo: Usar el tiempo simulado o el tiempo real del servidor.
        simulated_time_ms = data.get("simulated_timestamp_ms")
        if simulated_time_ms is not None:
            # Convertir milisegundos de Unix Epoch (desde Wokwi) a objeto datetime
            # El tiempo de Python se basa en segundos, por lo que dividimos por 1000.
            # Se usa timezone.utc para asegurar que la marca de tiempo es correcta.
            timestamp_to_use = datetime.fromtimestamp(simulated_time_ms / 1000, tz=timezone.utc)
            print(f"⏰ Usando tiempo simulado: {timestamp_to_use.isoformat()}")
        else:
            # Fallback: usar el tiempo real del servidor (como antes)
            timestamp_to_use = datetime.now() 
            print("🕒 Usando tiempo real del servidor.")

        # 3. Validar y convertir a entero (0 o 1)
        try:
            estado = int(estado_raw)
            if estado not in [0, 1]:
                 raise ValueError("El estado debe ser 0 (libre) o 1 (ocupado).")
        except (ValueError, TypeError):
            return jsonify({
                "status": "error", 
                "mensaje": "El campo 'estado' o 'value' es inválido o falta. Debe ser 0 o 1."
            }), 400

        # Seleccionar colección
        if codigosensor == 1:
            collection = db_atlas["Sensor_1"]
        elif codigosensor == 2:
            collection = db_atlas["Sensor_2"]
        elif codigosensor == 3:
            collection = db_atlas["Sensor_3"]
        else:
            return jsonify({"status": "error", "mensaje": "Código de sensor no válido"}), 400

        print(f"📡 Recibido desde ESP32: Sensor={codigosensor}, Estado={estado}")

        # 4. Crear documento a insertar (usando el timestamp_to_use)
        doc_to_insert = {
            "codigosensor": codigosensor,
            "estado": estado, 
            "timestamp": timestamp_to_use # <-- USANDO TIEMPO SIMULADO O REAL
        }

        # Insertar en MongoDB
        resultado = collection.insert_one(doc_to_insert)

        # Añadir el ID al documento para devolverlo
        doc_to_insert["_id"] = resultado.inserted_id

        # Serializar campos no compatibles (ObjectId, datetime)
        safe_doc = serialize_mongo_doc(doc_to_insert)

        print(f"✅ Insertado en {collection.name} con ID {safe_doc['_id']}")

        return jsonify({
            "status": "ok",
            "mensaje": "Dato insertado correctamente",
            "coleccion": collection.name,
            "documento": safe_doc
        }), 201

    except Exception as e:
        print(f"❌ Error al procesar datos: {e}")
        return jsonify({
            "status": "error",
            "mensaje": str(e)
        }), 500
        
# 🎯 ENDPOINT 1: PRUEBA DE CONEXIÓN DE GRAFANA
# ----------------------------------------------------------------------
@app.route('/', methods=['GET'])
def test_connection():
    """Endpoint de prueba para que Grafana (Infinity) verifique la conexión."""
    return 'OK', 200

# ----------------------------------------------------------------------
# 🎯 ENDPOINT 2: CONSULTA DE DATOS AGREGADOS PARA GRAFANA (DATOS REALES)
# ----------------------------------------------------------------------

@app.route('/query', methods=['GET'])
def park_query():
    """
    Consulta los datos crudos (RAW) de las colecciones de sensores para
    devolverlos a Grafana Infinity, filtrados por rango de tiempo.
    El resultado usa el formato plano requerido por Grafana Infinity.
    """
    
    # Obtener el rango de tiempo de Grafana
    start_str = request.args.get('from', (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat())
    end_str = request.args.get('to', datetime.now(timezone.utc).isoformat())
    
    
    if db_atlas is not None:
        try:
            # Convertir las cadenas de tiempo de Grafana a objetos datetime de MongoDB
            start_time = parser.parse(start_str)
            end_time = parser.parse(end_str)
            
            # Lista de colecciones para consultar
            collections_to_query = [db_atlas["Sensor_1"], db_atlas["Sensor_2"], db_atlas["Sensor_3"]]
            all_results = []
            
            # Criterio de filtrado (match)
            time_filter = {"timestamp": {"$gte": start_time, "$lte": end_time}}
            
            # 1. ITERAR Y OBTENER LOS DATOS CRUDOS DE CADA COLECCIÓN
            for collection in collections_to_query:
                # Ejecutar la consulta simple (sin agregación)
                cursor = collection.find(time_filter)
                
                # 2. MAPEAR al formato plano requerido por Grafana Infinity
                for doc in cursor:
                    # Convertir el timestamp a milisegundos de Unix Epoch (formato requerido por Grafana)
                    # El timestamp se convierte a UTC, luego a epoch time, y finalmente a milisegundos.
                    time_in_ms = int(doc['timestamp'].replace(tzinfo=timezone.utc).timestamp() * 1000)
                    
                    all_results.append({
                        # 'time' debe ser el timestamp en milisegundos
                        "time": time_in_ms, 
                        # 'value' es el estado del sensor (0 o 1)
                        "value": doc['estado'], 
                        # 'metric' es el nombre de la colección/sensor (usado para diferenciar series)
                        "metric": collection.name,
                        # Campos adicionales que pueden ser útiles en Tablas/Logs de Grafana
                        "codigosensor": doc['codigosensor'],
                    })
            
            # 3. ORDENAR todos los resultados por tiempo (aunque no es estrictamente necesario, es buena práctica)
            all_results.sort(key=lambda x: x['time'])

            print(f"✅ Consulta MongoDB: {len(all_results)} documentos crudos obtenidos y mapeados.")
            return jsonify(all_results)
            
        except Exception as e:
            error_msg = f"❌ Error crítico al consultar MongoDB: {e}"
            print(error_msg)
            # Devolver un 500 en caso de error de conexión/consulta
            return jsonify({"status": "error", "message": error_msg, "results": []}), 500
            
    # Si no hay conexión Atlas (db_atlas es None), devolvemos un 503
    return jsonify({"status": "error", "message": "🚫 No hay conexión a MongoDB Atlas. Imposible obtener datos reales.", "results": []}), 503

if __name__ == '__main__':
    # Usar host='0.0.0.0' para que sea accesible externamente (como por Docker/Grafana)
    app.run(host='0.0.0.0', port=6001, debug=True)