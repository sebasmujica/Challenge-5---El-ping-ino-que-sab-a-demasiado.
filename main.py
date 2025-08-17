from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

def iso_now():
    pass

def val_token():
    pass

def validar_log(item):
    pass

def get_conn():
    pass

def parse_iso():
    pass


#Ruta para comprobar que el servidor esta operativo
@app.route("/health", methods=["GET"])
def health():
    return jsonify({'status': 'ok', 'time': iso_now()})


#Ruta /log con metodo GET
@app.route("/log",methods=['POST'])
def recibir_logs():
    
    token = val_token()
    if not token:
        return jsonify({"error": "inavalid token"}), 400
    
    try:
        data = request.get_json(force = True, silent= False)
    except:
        return jsonify({"error": "invalid JSON"}), 400
    
    if isinstance(data, dict):
        batch = [data]
    elif isinstance(data, list):
        batch = data
    else:
        return jsonify({"error":"El cuerpo debe ser un obejeto o lista de objetos JSON"}), 400
    
    to_insert = []
    recv_time = iso_now()

    try:
        for item in batch:
            ts, service, severity, mensaje = validar_log(item)
            to_insert.append((ts,recv_time,service,mensaje,token))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    
    conn = get_conn()
    try:
        with conn:
            conn.executemany()
    finally:
        conn.close()
    return jsonify({"inserted": len(to_insert)}), 201



@app.route("/logs", methods=["GET"])
def listar_logs():
    params = []
    where = []

    ts_start = request.args.get("timestamp_start")
    ts_end = request.args.get("timestamp_end")


#Filtros por rango (timestamp del evento)
    if ts_start:
        try:
            ts_start = parse_iso(ts_start)
            pass
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        where.append("timestamp >= ?")
        params.append(ts_start)
    
    if ts_end:
        try:
            ts_end = parse_iso(ts_end)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        where.append("timestamp <= ?")
        params.append(ts_end)

#Filtro por rango (received_at)
    ra_star = request.args.get("received_at_start")
    ra_end = request.args.get("received_at_end")

    if ra_star:
        try:
            ra_star = parse_iso(ra_star)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        where.append("received_at >= ?")
        params.append(ra_star)
    if ra_end:
        try:
            ra_end = parse_iso(ra_end)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        where.append("received_at <= ?")
        params.append(ra_end)

