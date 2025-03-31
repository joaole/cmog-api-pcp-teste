import requests
import dotenv
import os
import pandas as pd

dotenv.load_dotenv()

PUBLIC_KEY = os.getenv("PUBLIC_KEY")

url = "https://apipcp.portaldecompraspublicas.com.br/publico/processosAbertos/?"

params = {
    "publicKey": PUBLIC_KEY,
    "dataInicio": "2025-03-31",
    "dataFim": "2025-04-01",
}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data['data'])
    df.to_csv('processos_abertos.csv', index=False)
    print("Data saved to processos_abertos.csv")
else:
    print(f"Error: {response.status_code}")
    print(response.text)
# The code above fetches open processes from the Portal de Compras Públicas API
# and saves the data to a CSV file.
