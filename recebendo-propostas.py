import os

import dotenv
import requests
from supabase import Client, create_client

from constantes.estados_list import estados

dotenv.load_dotenv()

PUBLIC_KEY = os.getenv("PUBLIC_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE = os.getenv("SERVICE_ROLE")

print(f"PUBLIC_KEY: {PUBLIC_KEY}")
print(f"SUPABASE_URL: {SUPABASE_URL}")
print(f"SERVICE_ROLE: {SERVICE_ROLE}")
# Criação do cliente do Supabase
supabase: Client = create_client(SUPABASE_URL, SERVICE_ROLE)

# URL da API
url = "https://apipcp.portaldecompraspublicas.com.br/publico/obterProcessosPropostas/?"

# Listas para armazenar dados
municipios_data = []
licitacoes_data = []
itens_data = []
grupos_materiais_data = []
classes_materiais_data = []
cnaes_data = []

# Loop para cada estado
for estado in estados:
    print(f"Buscando dados para o estado: {estado}")
    # Parâmetros da requisição
    params = {
        "publicKey": PUBLIC_KEY,
        "uf": estado,
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()

        for licitacao in data["dadosLicitacoes"]:
            # Inserir dados do município (caso não existam)
            municipio_data = {
                "idMunicipio": licitacao["municipio"]["codigoIBGE"],
                # Usando o codigoIBGE como idMunicipio
                "codigoIBGE": licitacao["municipio"]["codigoIBGE"],
                "nomeMunicipio": licitacao["municipio"]["nomeMunicipio"],
                "ufMunicipio": licitacao["municipio"]["ufMunicipio"],
            }
            if municipio_data not in municipios_data:
                municipios_data.append(municipio_data)

            # Inserir dados da licitação com a `idLicitacao` da API
            licitacao_data = {
                "idLicitacao": licitacao[
                    "idLicitacao"
                ],  # Usando o idLicitacao diretamente da resposta da API
                "numero": licitacao["numero"],
                "dataAberturaPropostas": licitacao["dataAberturaPropostas"],
                "horaAberturaPropostas": licitacao["horaAberturaPropostas"],
                "tipoLicitacao": licitacao["tipoLicitacao"],
                "comprador": licitacao["comprador"],
                "url": licitacao["url"],
                "idMunicipio": licitacao["municipio"][
                    "codigoIBGE"
                ],  # Usando o codigoIBGE como idMunicipio
            }
            licitacoes_data.append(licitacao_data)

            # Inserir itens com o idLicitacao diretamente da API
            for item in licitacao["itens"]:
                item_data = {
                    "idItem": item["idItem"],
                    "NR_ITEM": item["NR_ITEM"],
                    "DS_ITEM": item["DS_ITEM"],
                    "QT_ITENS": item["QT_ITENS"],
                    "VL_UNITARIO_ESTIMADO": item["VL_UNITARIO_ESTIMADO"],
                    "idLicitacao": licitacao[
                        "idLicitacao"
                    ],  # Usando o idLicitacao diretamente da API
                }
                itens_data.append(item_data)

            # Inserir grupos de materiais com o idLicitacao diretamente da API
            for grupo in licitacao["gruposMateriais"]:
                grupo_data = {
                    "idGrupoMaterial": grupo["idGrupoMaterial"],
                    "nomeGrupoMaterial": grupo["nomeGrupoMaterial"],
                    "idLicitacao": licitacao[
                        "idLicitacao"
                    ],  # Usando o idLicitacao diretamente da API
                }
                grupos_materiais_data.append(grupo_data)

                # Inserir classes de materiais com o idGrupoMaterial da API
                for classe in grupo["classesMateriais"]:
                    classe_data = {
                        "idClasseMaterial": classe["idClasseMaterial"],
                        "nomeClasseMaterial": classe["nomeClasseMaterial"],
                        "idGrupoMaterial": grupo["idGrupoMaterial"],
                    }
                    classes_materiais_data.append(classe_data)

            # Inserir CNAEs com o idLicitacao diretamente da API
            for cnae in licitacao["cnaes"]:
                cnae_data = {
                    "cnae": cnae["cnae"],
                    "descricao": cnae["descricao"],
                    "idLicitacao": licitacao[
                        "idLicitacao"
                    ],  # Usando o idLicitacao diretamente da API
                }
                cnaes_data.append(cnae_data)

    else:
        print(f"Error: {response.status_code} for estado {estado}")
        print(response.text)

# Inserir os dados no Supabase
supabase.table("municipios").upsert(municipios_data)
supabase.table("licitacoes").upsert(licitacoes_data)
supabase.table("itens").upsert(itens_data)
supabase.table("grupos_materiais").upsert(grupos_materiais_data)
supabase.table("classes_materiais").upsert(classes_materiais_data)
supabase.table("cnaes").upsert(cnaes_data)

print("Dados inseridos com sucesso no Supabase!")
