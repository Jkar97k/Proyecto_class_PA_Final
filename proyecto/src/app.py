import os
import time
from flask import Flask, render_template, request, jsonify,Response
from pymongo import MongoClient
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
from bson import ObjectId
import json


# --- Serializador JSON personalizado para manejar ObjectId de MongoDB ---
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()  # convierte datetime a string legible
        return super().default(o)


# Carga el archivo .env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

app = Flask(__name__)
app.json_encoder = JSONEncoder


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
        # Datos simulados con datetime directo (lo manejamos en JSONEncoder)
        sensores = [
            {"codigosensor": 1, "estado": 1, "TipoEjecucion": datetime.now()},
            {"codigosensor": 2, "estado": 0, "TipoEjecucion": datetime.now()},
            {"codigosensor": 3, "estado": 1, "TipoEjecucion": datetime.now()},
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
                "insertado_id": resultado.inserted_id,
                "documento": sensor
            })

        # ✅ Serialización 100 % segura
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

#------------------------wokwi----------------------------

# --- Colecciones simuladas (ajusta según tus nombres reales) ---
colecciones = {
    1: db_atlas.Sensor_1,
    2: db_atlas.Sensor_2,
    3: db_atlas.Sensor_3
}

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
    try:
        data = request.get_json()

        if not data or "codigosensor" not in data:
            return jsonify({"status": "error", "mensaje": "Falta el campo 'codigosensor'"}), 400

        codigosensor = int(data["codigosensor"])

        # Verificar que el código de sensor sea válido
        if codigosensor not in colecciones:
            return jsonify({"status": "error", "mensaje": "Código de sensor no válido"}), 400

        collection = colecciones[codigosensor]

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

#grafana 

@app.route('/LeerSensores', methods=['GET'])
def obtener_datos_sensores():
    try:
        # Accedemos a las colecciones de los tres sensores
        colecciones = [db_atlas.Sensor_1, db_atlas.Sensor_2, db_atlas.Sensor_3]

        datos_totales = []
        for col in colecciones:
            # Tomar los últimos 10 registros de cada sensor
            docs = list(col.find().sort("_id", -1).limit(10))

            # Serializar los ObjectId y datetime
            for d in docs:
                d["_id"] = str(d["_id"])
                if "TipoEjecucion" in d and hasattr(d["TipoEjecucion"], "isoformat"):
                    d["TipoEjecucion"] = d["TipoEjecucion"].isoformat()

            datos_totales.extend(docs)

        return jsonify({
            "status": "ok",
            "total_registros": len(datos_totales),
            "datos": datos_totales
        }), 200

    except Exception as e:
        print(f"❌ Error al obtener datos: {e}")
        return jsonify({"status": "error", "mensaje": str(e)}), 500


@app.route('/tabla')
def tabla():
    usuarios = list(db_atlas.p1.find({}, {"_id": 0}))
    return render_template('tabla.html', usuarios=usuarios)

if __name__ == '__main__':
    # Usar el puerto 5000 (mapeado a 5000 por docker-compose) y host='0.0.0.0'
    app.run(host='0.0.0.0', port=6001, debug=True)