import requests
import random
import time
from datetime import datetime, UTC
from typing import Dict, List, Tuple
import threading
server = "http://127.0.0.1:8000"
tokens: List[str]= [
    "ABC123",
    "DEF456",
    "GHI789",
    "NOT000"
]
pesos_tokens :List[int] = [31,31,31,7]
services :Dict[str,int] = {
    "Pedidos_YA": 60,
    "UBER": 60,
    "Banco":60
}
severities: List[str] = ["DEBUG","INFO","WARNING","ERROR","CRITICAL"]
pesos: List[int] =    [   50,    30,     10,       7,       3]

def iso_now():
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")

def send_log(service: str,cant_logs: int, delay_envio: Tuple[float,float]=(0.5,2.0)):
    for i in range(1,cant_logs +1 ):

        headers: Dict[str,str] = {
        "Authorization": "Token "+random.choices(tokens,weights=pesos_tokens,k=1)[0]
        }

        payload = {
            "timestamp":iso_now(),
            "service":service,
            "severity": random.choices(severities,weights=pesos,k=1)[0],
            "message":f"log #{i} - mensaje de prueba {random.randint(1000,9999)}"
        }

        try:
            r = requests.post(
                f"{server}/logs",
                headers= headers,
                json= payload,
                timeout= 10, #espera la respuesta del server por 10 segundos, sino lanza un timeout error
            )
            print(f"[{service}] {i}/{cant_logs} - HTTP -> {r.status_code} - {r.json()}")
            r.raise_for_status()
        except requests.exceptions.Timeout:
            print(f"[{service}] {i}/{cant_logs} - La solicitud tardo demasiado y fue cancelada")
            time.sleep(0.2)
            continue
        except requests.exceptions.RequestException as e:
            print(f"[{service}] {i}/{cant_logs} - Error de red/HTTP: {e}")
            time.sleep(0.2)
            continue

        print(f"[{service}] {i}/{cant_logs} Codigo de respuesta:",r.status_code)
        try:
            print(f"[{service}] {i}/{cant_logs} - Respuesta del server:", r.json())
        except ValueError:
            print(f"[{service}] {i}/{cant_logs} - Respuesta del server (no JSON):",r.text[:500])

        time.sleep(random.uniform(*delay_envio))

if __name__ == "__main__":

    r = requests.get(f"{server}/health")

    #Chequeo si es ok
    print("Codigo HTTP:",r.status_code)

    #Capturar JSON
    try:
        data = r.json()
        print(f"status: {data['status']} - time: {data['time']}")
    except ValueError:
        print("La respuesta no era un JSON")
    except requests.exceptions.RequestException as e:
        print(f"Error : {e}")

    threads :List[threading.Thread] = []

    for service, cant_logs in services.items():
        t = threading.Thread(target=send_log, args=(service, cant_logs), daemon= False)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    
    print("Todos los servicios terminaron sus envios")




