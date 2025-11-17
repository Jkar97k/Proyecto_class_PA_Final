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
        
        # 2. Validar y convertir a entero (0 o 1)
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

        # 3. Crear documento a insertar (Solo campos clave para la visualización)
        doc_to_insert = {
            "codigosensor": codigosensor,
            "estado": estado, # <-- CAMPO CLAVE PARA GRAFANA
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
        
# ----------------------------------------------------------------------
# 🎯 ENDPOINT 1: PRUEBA DE CONEXIÓN DE GRAFANA
# ----------------------------------------------------------------------
@app.route('/', methods=['GET'])
def test_connection():
    """Endpoint de prueba para que Grafana (Infinity) verifique la conexión."""
    return 'OK', 200

# ----------------------------------------------------------------------
# 🎯 ENDPOINT 2: CONSULTA DE DATOS AGREGADOS PARA GRAFANA (NUEVO)
# ----------------------------------------------------------------------
@app.route('/query', methods=['GET'])
def park_query():
    """
    Simula o ejecuta la consulta de agregación de datos de MongoDB para
    mostrar la ocupación total del parking a lo largo del tiempo.
    El resultado usa el formato plano requerido por Grafana Infinity.
    """
    
    # Grafana proporciona el rango de tiempo en milisegundos ISO (e.g., "2023-11-15T10:00:00.000Z")
    # Usaremos estas claves para simular la filtración de datos históricos.
    start_str = request.args.get('from', (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat())
    end_str = request.args.get('to', datetime.now(timezone.utc).isoformat())
    
    # -------------------------------------------------------------------
    # ⚠️ LÓGICA DE AGREGACIÓN REAL EN MONGODB 
    # -------------------------------------------------------------------
    if db_atlas is not None:
        try:
            # Convertir las cadenas de tiempo de Grafana a objetos datetime de MongoDB
            start_time = parser.parse(start_str)
            end_time = parser.parse(end_str)
            
            # NOTA: Para este pipeline, la agregación debe ocurrir sobre una colección 
            # que contenga los datos de TODOS los sensores para obtener la ocupación TOTAL.
            # Como los datos están separados en Sensor_1, Sensor_2, Sensor_3, usamos $unionWith.
            
            # Se usa Sensor_1 como colección inicial para el pipeline
            collection = db_atlas["Sensor_1"] 

            # Agregación: 
            # 1. Combinar todas las colecciones (Sensor_1, 2, 3)
            # 2. Filtrar por rango de tiempo.
            # 3. Agrupar por intervalo (ej. 5 minutos) y calcular la ocupación total.
            pipeline = [
                # Combinar datos de Sensor_2 y Sensor_3
                {"$unionWith": {"coll": "Sensor_2"}},
                {"$unionWith": {"coll": "Sensor_3"}},
                {
                    "$match": {
                        # Filtra solo los documentos dentro del rango de tiempo de Grafana
                        "timestamp": {"$gte": start_time, "$lte": end_time} 
                    }
                },
                {
                    "$group": {
                        # Agrupa los documentos en intervalos de 5 minutos
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "month": {"$month": "$timestamp"},
                            "day": {"$dayOfMonth": "$timestamp"},
                            "hour": {"$hour": "$timestamp"},
                            "minute": {"$subtract": [
                                {"$minute": "$timestamp"},
                                {"$mod": [{"$minute": "$timestamp"}, 5]} # Agrupa cada 5 minutos
                            ]}
                        },
                        # Calcula la ocupación total: suma los estados (1=ocupado, 0=libre)
                        "total_occupied_count": {"$sum": "$estado"}, 
                        "timestamp_first": {"$min": "$timestamp"} # Obtenemos un timestamp para Grafana
                    }
                },
                {
                    "$project": {
                        # Reformatea la salida al formato plano de Grafana
                        "_id": 0,
                        "time": {"$toLong": "$timestamp_first"}, # Timestamp en milisegundos
                        "value": "$total_occupied_count",
                        "metric": "Ocupación Total"
                    }
                },
                {"$sort": {"time": 1}} # Ordenar cronológicamente
            ]
            
            # Ejecutamos el pipeline en la colección Sensor_1, que ahora incluye las otras dos
            results = list(collection.aggregate(pipeline))
            print(f"✅ Consulta MongoDB: {len(results)} puntos agregados usando $unionWith.")
            return jsonify(results)
            
        except Exception as e:
            print(f"❌ Error al consultar MongoDB para Grafana: {e}")
            # Si hay un error real en la BD, se simula para no romper el dashboard
            pass 
            
    # -------------------------------------------------------------------
    # 🧑‍💻 SIMULACIÓN DE DATOS AGREGADOS (Para cuando db_atlas es None o falla)
    # -------------------------------------------------------------------
    
    # Usaremos los argumentos de Grafana para simular el rango de tiempo
    try:
        start_dt = parser.parse(start_str)
        end_dt = parser.parse(end_str)
    except:
        # Fallback si el parser falla
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=6)

    # El número de sensores definidos es 3
    TOTAL_SPOTS = 3 
    
    response_data = []
    
    # Simulación de puntos cada 10 minutos
    time_increment = timedelta(minutes=10)
    current_time = start_dt

    while current_time < end_dt:
        # Simula la ocupación total (entre 0 y 3)
        occupied_count = random.randint(0, TOTAL_SPOTS)
        
        response_data.append({
            # Grafana Infinity espera el timestamp en milisegundos
            "time": int(current_time.timestamp() * 1000), 
            "value": occupied_count,
            "metric": "Ocupación Total (Simulada)"
        })
        current_time += time_increment

    print(f"✨ Simulación de datos para Grafana: {len(response_data)} puntos generados.")
    return jsonify(response_data)