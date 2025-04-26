import io
import azure.functions as func
import datetime
import json
import logging
import requests
from dotenv import load_dotenv 
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient 
import random
import concurrent.futures

app = func.FunctionApp()
load_dotenv()

logging.basicConfig( level=logging.INFO )
logger = logging.getLogger(__name__)

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORA_ACCOUNT_NAME = os.getenv("STORA_ACCOUNT_NAME")


@app.queue_trigger(
     arg_name="azqueue", 
    queue_name="requests",
    connection="QueueAzureWebJobsStorage")  


def QueueTriggerPokeRequest(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]

    update_request(id, "inprogress")
    
    request = get_request(id)
    pokemons = get_pokemons(request["type"])

    sample_size = request.get("SampleSize")
    if sample_size and sample_size < len(pokemons):
        pokemons = random.sample(pokemons, sample_size)

    pokemons_bytes = generate_csv_to_blob(pokemons)
    blob_name = f"poke_report_{id}.csv"
    
    upload_csv_to_blob( blob_name=blob_name, csv_data=pokemons_bytes )
    logging.info(f"{blob_name} file uploaded successfully")

    full_url = f"https://{STORA_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"

    update_request(id, "completed", full_url)


def update_request(id: int, status: str, url: str = None) -> dict:
    payload = {
        "status": status,
        "id": id
    }

    if url:
        payload["url"] = url

    response = requests.put(f"{ DOMAIN }/api/request", json=payload)
    return response.json()

def get_request(id: int) -> dict:
    # logging.info(f"domain: {DOMAIN}")
    response = requests.get(f"{ DOMAIN }/api/request/{id}")
    # logging.info(f"Response from get_request: {response.json()}")
    return response.json()[0]


def get_pokemons(type: str) -> dict:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"

    response = requests.get(pokeapi_url, timeout=50)
    data = response.json()
    poke_entries = data.get("pokemon", [])

    pokemons = [p["pokemon"] for p in poke_entries]

    pokemons_with_details = get_pokemon_details(pokemons)
    return pokemons_with_details


def generate_csv_to_blob(pokemons_list: list) -> bytes:
    df = pd.DataFrame( pokemons_list) 

    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()

    return csv_bytes

def upload_csv_to_blob(blob_name: str, csv_data: bytes):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client( container=BLOB_CONTAINER_NAME, blob=blob_name )
        blob_client.upload_blob( csv_data, overwrite = True )

    except Exception as e:
        logging.error(f"Error uploading CSV to blob: {e}")
        raise


def get_pokemon_details(pokemons: list) -> dict:
    def fetch_pokemon(p):
        try:
            response = requests.get(p["url"], timeout=10)
            data = response.json()

            stats = {s["stat"]["name"]: s["base_stat"] for s in data["stats"]}
            abilities = ", ".join([a["ability"]["name"] for a in data["abilities"]])

            return {
                "name": p["name"],
                "url": p["url"],
                "hp": stats.get("hp"),
                "attack": stats.get("attack"),
                "defense": stats.get("defense"),
                "special-attack": stats.get("special-attack"),
                "special-defense": stats.get("special-defense"),
                "speed": stats.get("speed"),
                "abilities": abilities,
            }
        
        except Exception as e:
            logging.error(f"Error getting details for {p['name']}: {e}")
            return None
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(fetch_pokemon, pokemons))

        
    return [result for result in results if result is not None]

