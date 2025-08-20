from flask import Flask, jsonify, request
import sqlite3
from datetime import datetime, timezone,UTC
from typing import Dict, Any, Tuple

app = Flask(__name__)

db = "logs.db"

tokens_validos = {
    "ABC123",
    "DEF456",
    "GHI789",
}

SEVERITIES = {"DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"}

def iso_now():
    # YYYY-MM-DDTHH-MM-SS+00:00 ---> YYYY-MM-DDTHH-MM-SSZ
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")


def val_token() -> str: 

    auth = request.headers.get("Authorization","")

    if not auth.startswith("Token "):
        return ""

    token = auth.split(" ",1)[1].strip()

    if token not in tokens_validos:
        return ""

    return token


def validar_log(item: Dict[str,Any]) -> Tuple[str, str, str, str]:

    for f in ("timestamp","service","severity","message"):
        if f not in item:
            raise ValueError(f"Falta campo: {f}")

    ts = parse_iso(str(item["timestamp"]))
    service = str(item["service"]).strip()
    severity = str(item["severity"]).strip().upper()
    message = str(item["message"]).strip()

    if not service:
        raise ValueError("'service' ---> empty")
    elif severity not in SEVERITIES:
        raise ValueError(f"severity invalido.")
    elif not message:
        raise ValueError("'mensaje' ---> empty")

    return ts, service, severity, message

def get_conn():
    conn = sqlite3.connect(db)
    # row_factory permite que las filas de SQLite se devuelvan como sqlite3.Row,
    # lo que permite acceder a las columnas tanto por índice (row[0]) como por nombre (row["columna"]),
    # y facilita convertir las filas directamente en diccionarios (dict(row)) para usarlas en JSON.
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            received_at TEXT NO NULL,
            service TEXT NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            token TEXT NOT NULL
        )
        '''
)
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_logs_received_at ON logs(received_at)",
        "CREATE INDEX IF NOT EXISTS idx_logs_service ON logs(service)",
        "CREATE INDEX IF NOT EXISTS idx_logs_severity ON logs(severity)"
    ]
    for indice in indices:
        cur.execute(indice)

    conn.commit()
    conn.close()

#Funcion que permite verificar que el ts se encuentre en el formato ISO8601
def parse_iso(ts: str) -> str: #Devuelve en formato YYYY-MM-DDTHH-MM-SSZ
    raw = ts.strip() # strip() ---> elimina espacios o caracteres no desados 

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(raw) #el ts debe poseer el +HH:MM
    except ValueError:
        raise ValueError("timestamp invalido")
    
    if dt.tzinfo is None: #Si el ts no traia el +HH:MM o la "Z" al final(la cual se deberia haber modificado mas arriba)
        raise ValueError("timestamp sin zona horaria especificada")
    
    dt_utc = dt.astimezone(UTC).replace(microsecond=0)
    return dt_utc.isoformat().replace("+00:00", "Z") # YYYY-MM-DDTHH-MM-SS+00:00 ---> YYYY-MM-DDTHH-MM-SSZ



#Ruta para comprobar que el servidor esta operativo
@app.get("/health")
def health():
    return jsonify({'status': 'ok', 'time': iso_now()})


#Ruta /log con metodo GET
@app.post("/log")
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
            to_insert.append((ts,recv_time,service,severity,mensaje,token))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    
    conn = get_conn()
    try:
        with conn:
            conn.executemany(
                "INSERT INTO logs (timestamp, received_at, service, severity, message, token) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                to_insert,
            )
    finally:
        conn.close()
    return jsonify({"inserted": len(to_insert)}), 201



@app.get("/logs")
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

#Otros filtros 
    service = request.args.get("service")
    severity = request.args.get("severity")

    if service:
        where.append("service = ?")
        params.append(service)
    if severity:
        sev = severity.upper()
        if sev not in SEVERITIES:
            return jsonify({"error": f"severity invalido. Usa uno de: {', '.join(sorted(SEVERITIES))}"}),400
        where.append("severity = ?")
        params.append(sev)

    limit = request.args.get("limit", default="100")
    offset = request.args.get("offset",default="0")

    try:
        limit_i = max(1,min(int(limit), 1000))
        offset_i = max(0,int(offset))
    except ValueError:
        return jsonify({"error":"limit/offset invalidos"})

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    sql = f'''
        SELECT id, timestamp, received_at, service, severity, message, token
        FROM logs
        {where_sql}
        ORDER BY received_at DESC, id DESC
        LIMIT ? OFFSET ?
    '''

    params_ext = params + [limit_i, offset_i]

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql,params_ext)
    rows = cur.fetchall()
    conn.close()
    return jsonify(
        {
            "count": len(rows),
            "items": [dict(r) for r in rows]
        }
    )

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8000, debug= True)