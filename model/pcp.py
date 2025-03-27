import os
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

# Carrega variáveis do .env.local
load_dotenv(dotenv_path=".env.local")


class PCPClient:
    BASE_URL = "https://apipcp.portaldecompraspublicas.com.br/publico/"

    def __init__(self, dias_futuro=14):
        self.public_key = os.getenv("PUBLIC_KEY")
        if not self.public_key:
            raise ValueError("A chave PUBLIC_KEY não foi encontrada no .env.local")
        self.dias_futuro = dias_futuro

    def _formatar_datas(self):
        hoje = datetime.today()
        futura = hoje + timedelta(days=self.dias_futuro)
        return hoje.strftime("%d/%m/%Y"), futura.strftime("%d/%m/%Y")

    def _montar_url(self, endpoint):
        return f"{self.BASE_URL}{endpoint}"

    def requisitar(self, endpoint, params_adicionais=None):
        data_inicio, data_fim = self._formatar_datas()

        params = {
            "publicKey": self.public_key,
            "dataInicio": data_inicio,
            "dataFim": data_fim,
        }

        if params_adicionais:
            params.update(params_adicionais)

        url = self._montar_url(endpoint)

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            print(f"✅ Sucesso: {endpoint}")
            return response.json()
        except requests.RequestException as e:
            print(f"❌ Erro na requisição ({endpoint}): {e}")
            return None
