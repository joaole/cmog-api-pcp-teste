import dotenv
import pandas as pd
import requests

dotenv.load_dotenv()

PUBLIC_KEY = "e341e0f251537883e3bbc9bd9f597585"

# Processos Abertos data dd/mm/yyyy

url = "https://apipcp.portaldecompraspublicas.com.br/publico/processosAbertos/?"

params = {
    "publicKey": PUBLIC_KEY,
    "dataInicio": "08/04/2025",
    "dataFim": "10/04/2025",
}

response = requests.get(url, params=params)

if response.status_code == 200:
    print(f"Response: {response.status_code}")
    print(f"Response: {response.text}")
    print(f"Response: {response.json()}")
    print(f"Response: {response.json()['data']}")
    data = response.json()
    df = pd.DataFrame(data["data"])
    df.to_csv("processos_abertos.csv", index=False)
    print("Data saved to processos_abertos.csv")
else:
    print(f"Error: {response.status_code}")
    print(response.text)
# The code above fetches open processes from the Portal de Compras Públicas API
# and saves the data to a CSV file.
