import os
import json
import time
from pathlib import Path
from datetime import datetime,timezone# Se añadió 'timezone' para usar en la ruta /query
from bson import ObjectId
from dateutil import parser
from pymongo import MongoClient
from dotenv import load_dotenv
from flask import request, jsonify,Flask, render_template,Response


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
    """Ruta para recibir datos JSON desde un ESP32 o dispositivo IoT."""
    try:
        if db_atlas is None:

            return jsonify({"status": "error", "mensaje": "No hay conexión a Atlas"}), 500

        data = request.get_json()

        if not data or "codigosensor" not in data:
            return jsonify({"status": "error", "mensaje": "Falta el campo 'codigosensor'"}), 400

        codigosensor = int(data["codigosensor"])

        # Usamos el mapa COLECCIONES_MAP ya que contiene las referencias correctas
        if codigosensor == 1:
            collection = COLECCIONES_MAP.get("Sensor_1")
        elif codigosensor == 2:
            collection = COLECCIONES_MAP.get("Sensor_2")
        elif codigosensor == 3:
            collection = COLECCIONES_MAP.get("Sensor_3")
        else:
            return jsonify({"status": "error", "mensaje": "Código de sensor no válido"}), 400

        print(f"📡 Recibido desde ESP32: {data}")

        # Crear documento a insertar
        doc_to_insert = {
            "codigosensor": codigosensor,
            "sensor_type": data.get("sensor_type", "Desconocido"),
            "valor": data.get("value"),
            "unidad": data.get("unit", "N/A"),
            "timestamp": datetime.now()
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


@app.route('/tabla')
def tabla():
    """Muestra una tabla (asumiendo que db_atlas.p1 existe)."""
    # Nota: Asegúrate de que db_atlas.p1 exista
    if db_atlas is None:
         return "Error: No conectado a Atlas para la ruta /tabla", 503
    usuarios = list(db_atlas.p1.find({}, {"_id": 0}))
    return render_template('tabla.html', usuarios=usuarios)


# --------------------------------------------------------
#              RUTAS GRAFANA JSON DATA SOURCE
# --------------------------------------------------------

# 1. Health Check (Ruta de prueba de conexión)
@app.route('/', methods=['GET'])
def grafana_health_check():
    """Ruta de verificación de salud de Grafana."""
    # Verificamos si al menos la conexión a Atlas fue exitosa
    if client_atlas is not None:
        return "OK", 200
    else:
        return "Database Connection Error", 503

# 2. Search (Descubrimiento de métricas)
@app.route('/search', methods=['POST'])
def search():
    """Devuelve la lista de métricas disponibles para la consulta."""
    # Devuelve las claves del mapa (los nombres de los sensores)
    return jsonify(list(COLECCIONES_MAP.keys()))

# 3. Query (Consulta de datos)
@app.route('/query', methods=['POST'])
def query_json_api_format():
    try:
        # Nota: El plugin JSON API puede o no enviar 'range' y 'targets' de la misma manera.
        # Asumiremos que aún quieres filtrar por tiempo.
        req = request.get_json(silent=True)
        
        # --- (Manejo de Rango de Tiempo y Colecciones, si es necesario) ---
        # ... (Tu lógica para obtener time_from y time_to) ...
        
        colecciones = {
            "Sensor_1": db_atlas.Sensor_1,
            "Sensor_2": db_atlas.Sensor_2,
            "Sensor_3": db_atlas.Sensor_3
        }

        respuesta_tabular = [] # <-- La respuesta ahora será una lista simple de todos los documentos

        # En el plugin JSON API, a menudo se ignora 'targets' o se usa para un solo endpoint.
        # Iremos a buscar TODOS los datos de los sensores para el rango de tiempo (si lo usas).
        
        # Iterar sobre las colecciones y obtener datos filtrados por tiempo (si aplica)
        for nombre_sensor, col in colecciones.items():
            
            # Ejemplo de filtro (ajusta según la necesidad del plugin JSON API)
            # docs = col.find(query_filter, ...).sort("Fecha", 1) 
            docs = col.find({}, {"Valor": 1, "Fecha": 1, "_id": 0}) # Consulta sin filtro de tiempo simple

            for d in docs:
                valor = d.get("Valor")
                fecha = d.get("Fecha")
                
                # Conversión de Valor
                try:
                    valor_float = float(valor) if valor is not None else 0.0
                except ValueError:
                    valor_float = 0.0

                # Conversión de Fecha a formato ISO 8601 (string)
                time_str = None
                if isinstance(fecha, datetime):
                    # Aseguramos que la fecha esté en UTC y sea ISO 8601
                    time_str = fecha.astimezone(timezone.utc).isoformat()
                elif isinstance(fecha, str):
                    try:
                        # Parsear y luego convertir a ISO 8601
                        dt = parser.parse(fecha)
                        time_str = dt.astimezone(timezone.utc).isoformat()
                    except: # noqa: E722
                        continue

                # ⚠️ Agregar el documento al array en formato tabular
                if time_str:
                    respuesta_tabular.append({
                        "sensor_name": nombre_sensor, # Clave para la etiqueta de la serie
                        "time": time_str,            # Clave para el eje X
                        "value": valor_float         # Clave para el eje Y
                    })

        return jsonify(respuesta_tabular)

    except Exception as e:
        print(f"Error en el endpoint /query (JSON API Format): {e}")
        return jsonify({"error": str(e)}), 500



if __name__ == '__main__':
    # Usar host='0.0.0.0' para que sea accesible externamente (como por Docker/Grafana)
    app.run(host='0.0.0.0', port=6001, debug=True)