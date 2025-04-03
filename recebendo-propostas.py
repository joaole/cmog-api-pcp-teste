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
                # Usando o codigoIBGE como idMunicipio
                "codigo_ibge": licitacao["municipio"]["codigoIBGE"],
                "nome_municipio": licitacao["municipio"]["nomeMunicipio"],
                "uf_municipio": licitacao["municipio"]["ufMunicipio"],
            }
            if municipio_data not in municipios_data:
                municipios_data.append(municipio_data)

            # Inserir dados da licitação com a `idLicitacao` da API
            licitacao_data = {
                "id_licitacao": licitacao[
                    "idLicitacao"
                ],  # Usando o idLicitacao diretamente da resposta da API
                "numero": licitacao["numero"],
                "data_abertura_propostas": licitacao["dataAberturaPropostas"],
                "hora_abertura_propostas": licitacao["horaAberturaPropostas"],
                "tipo_licitacao": licitacao["tipoLicitacao"],
                "comprador": licitacao["comprador"],
                "url": licitacao["url"],
                "id_municipio": licitacao["municipio"][
                    "codigoIBGE"
                ],  # Usando o codigoIBGE como idMunicipio
            }
            licitacoes_data.append(licitacao_data)

            # Inserir itens com o idLicitacao diretamente da API
            for item in licitacao["itens"]:
                item_data = {
                    "id_item": item["idItem"],
                    "nr_item": str(item["NR_ITEM"]),
                    "ds_item": str(item["DS_ITEM"]),
                    "qt_itens": str(item["QT_ITENS"]),
                    "vl_unitario_estimado": str(item["VL_UNITARIO_ESTIMADO"]),
                    "id_licitacao": licitacao[
                        "idLicitacao"
                    ],  # Usando o idLicitacao diretamente da API
                }
                if item_data["id_item"] not in [i["id_item"] for i in itens_data]:
                    itens_data.append(item_data)

            # Inserir grupos de materiais com o idLicitacao diretamente da API
            for grupo in licitacao["gruposMateriais"]:
                grupo_data = {
                    "id_grupo_material": grupo["idGrupoMaterial"],
                    "nome_grupo_material": grupo["nomeGrupoMaterial"],
                    "id_licitacao": licitacao[
                        "idLicitacao"
                    ],  # Usando o idLicitacao diretamente da API
                }
                if grupo_data["id_grupo_material"] not in [
                    g["id_grupo_material"] for g in grupos_materiais_data
                ]:
                    grupos_materiais_data.append(grupo_data)

                # Inserir classes de materiais com o idGrupoMaterial da API
                for classe in grupo["classesMateriais"]:
                    classe_data = {
                        "id_classe_material": classe["idClasseMaterial"],
                        "nome_classe_material": classe["nomeClasseMaterial"],
                        "id_grupo_material": grupo["idGrupoMaterial"],
                    }
                    if classe_data["id_classe_material"] not in [
                        c["id_classe_material"] for c in classes_materiais_data
                    ]:
                        classes_materiais_data.append(classe_data)

            # Inserir CNAEs com o idLicitacao diretamente da API
            for cnae in licitacao["cnaes"]:
                cnae_data = {
                    "cnae": cnae["cnae"],
                    "descricao": cnae["descricao"],
                    "id_licitacao": licitacao[
                        "idLicitacao"
                    ],  # Usando o idLicitacao diretamente da API
                }
                if cnae_data["cnae"] not in [c["cnae"] for c in cnaes_data]:
                    cnaes_data.append(cnae_data)

    else:
        print(f"Error: {response.status_code} for estado {estado}")
        print(response.text)

# Inserir os dados no Supabase
supabase.table("municipios").upsert(municipios_data).execute()
supabase.table("licitacoes").upsert(licitacoes_data).execute()
supabase.table("itens").upsert(itens_data).execute()
supabase.table("grupos_materiais").upsert(grupos_materiais_data).execute()
supabase.table("classes_materiais").upsert(classes_materiais_data).execute()
supabase.table("cnaes").upsert(cnaes_data).execute()

print("Dados inseridos com sucesso no Supabase!")
