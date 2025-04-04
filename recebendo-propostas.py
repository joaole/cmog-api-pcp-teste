import os
import dotenv
import requests
from supabase import Client, create_client
from constantes.estados_list import estados
from datetime import datetime

# Carregar variáveis de ambiente
dotenv.load_dotenv()

# Configuração do Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE = os.getenv("SERVICE_ROLE")
PUBLIC_KEY = os.getenv("PUBLIC_KEY")

# Criar cliente do Supabase
supabase: Client = create_client(SUPABASE_URL, SERVICE_ROLE)

# Estados a serem consultados

url = "https://apipcp.portaldecompraspublicas.com.br/publico/obterProcessosPropostas/?"

# Listas para armazenar os dados
municipios_data = []
licitacoes_data = []
itens_data = []
grupos_materiais_data = []
classes_materiais_data = []
cnaes_data = []






for estado in estados:
    print(f" Buscando dados para o estado: {estado}")
    response = requests.get(url, params={"publicKey": PUBLIC_KEY, "uf": estado})

    if response.status_code == 200:
        data = response.json()

        for licitacao in data.get("dadosLicitacoes", []):
            # Adicionar município
            municipio_data = {
                "codigo_ibge": licitacao["municipio"]["codigoIBGE"],
                "nome_municipio": licitacao["municipio"]["nomeMunicipio"],
                "uf_municipio": licitacao["municipio"]["ufMunicipio"],
            }
            if municipio_data not in municipios_data:
                municipios_data.append(municipio_data)

            # Adicionar licitação
            licitacao_data = {
                "id_licitacao": licitacao["idLicitacao"],
                "numero": licitacao["numero"],
                "data_abertura_propostas": licitacao["dataAberturaPropostas"],
                "hora_abertura_propostas": licitacao["horaAberturaPropostas"],
                "tipo_licitacao": licitacao["tipoLicitacao"],
                "comprador": licitacao["comprador"],
                "url": licitacao["url"],
                "id_municipio": licitacao["municipio"]["codigoIBGE"],
            }
            licitacoes_data.append(licitacao_data)

            # Adicionar itens
            ids_itens = {item["id_item"] for item in itens_data}
            for item in licitacao.get("itens", []):
                item_data = {
                    "id_item": item["idItem"],
                    "nr_item": str(item["NR_ITEM"]),
                    "ds_item": str(item["DS_ITEM"]),
                    "qt_itens": str(item["QT_ITENS"]),
                    "vl_unitario_estimado": str(item["VL_UNITARIO_ESTIMADO"]),
                    "id_licitacao": licitacao["idLicitacao"],
                }
                if item_data["id_item"] not in ids_itens:
                    itens_data.append(item_data)

            # Adicionar grupos de materiais e classes
            for grupo in licitacao.get("gruposMateriais", []):
                grupo_data = {
                    "id_grupo_material": grupo["idGrupoMaterial"],
                    "nome_grupo_material": grupo["nomeGrupoMaterial"],
                    "id_licitacao": licitacao["idLicitacao"],
                }
                if grupo_data not in grupos_materiais_data:
                    grupos_materiais_data.append(grupo_data)

                for classe in grupo.get("classesMateriais", []):
                    classe_data = {
                        "id_classe_material": classe["idClasseMaterial"],
                        "nome_classe_material": classe["nomeClasseMaterial"],
                        "id_grupo_material": grupo["idGrupoMaterial"],
                    }
                    if classe_data not in classes_materiais_data:
                        classes_materiais_data.append(classe_data)

            # Adicionar CNAEs
            for cnae in licitacao.get("cnaes", []):
                cnae_data = {
                    "cnae": cnae["cnae"],
                    "descricao": cnae["descricao"],
                    "id_licitacao": licitacao["idLicitacao"],
                }
                if cnae_data not in cnaes_data:
                    cnaes_data.append(cnae_data)

    else:
        print(f" Erro {response.status_code} ao buscar dados para {estado}")
        print(response.text)

for licitacao in licitacoes_data:
    if licitacao["data_abertura_propostas"] < datetime.now().isoformat():
        licitacao_data.remove(licitacao)
        
def selecionar_tabela(nome_tabela):
    try:
        response = supabase.table(nome_tabela).select("*").execute()
        print(f" Dados da tabela {nome_tabela} selecionados com sucesso!")
        return response.data
    except Exception as e:
        print(f" Erro ao acessar a tabela {nome_tabela}: {e}")
        return []

licitacoes_existentes = selecionar_tabela("licitacoes")
id_antigas = {lic["id_licitacao"] for lic in licitacoes_existentes}

# Filtrar novas licitações e dados relacionados
novas_licitacoes = [lic for lic in licitacoes_data if lic["id_licitacao"] not in id_antigas]
novos_itens = [item for item in itens_data if item["id_licitacao"] in {l["id_licitacao"] for l in novas_licitacoes}]
novos_grupos_materiais = [grupo for grupo in grupos_materiais_data if grupo["id_licitacao"] in {l["id_licitacao"] for l in novas_licitacoes}]
novas_classes_materiais = [classe for classe in classes_materiais_data if classe["id_grupo_material"] in {g["id_grupo_material"] for g in novos_grupos_materiais}]
novos_cnaes = [cnae for cnae in cnaes_data if cnae["id_licitacao"] in {l["id_licitacao"] for l in novas_licitacoes}]

def remover_duplicatas(lista, chave):
    seen = set()
    nova_lista = []
    for item in lista:
        valor = item[chave]
        if valor not in seen:
            nova_lista.append(item)
            seen.add(valor)
    return nova_lista



municipios_data = remover_duplicatas(municipios_data, "codigo_ibge")
novas_licitacoes = remover_duplicatas(novas_licitacoes, "id_licitacao")
novos_itens = remover_duplicatas(novos_itens, "id_item")
novos_grupos_materiais = remover_duplicatas(novos_grupos_materiais, "id_grupo_material")
novas_classes_materiais = remover_duplicatas(novas_classes_materiais, "id_classe_material")
novos_cnaes = remover_duplicatas(novos_cnaes, "cnae")
# 🔹 Exibir totais corrigidos

print(f" Total de novas licitações: {len(novas_licitacoes)}")
print(f" Total de novos itens: {len(novos_itens)}")
print(f" Total de novos grupos de materiais: {len(novos_grupos_materiais)}")
print(f" Total de novas classes de materiais: {len(novas_classes_materiais)}")
print(f" Total de novos CNAEs: {len(novos_cnaes)}")

# 🔹 Inserir novos dados no Supabase com verificação de listas vazias
if municipios_data:
    supabase.table("municipios").upsert(municipios_data).execute()
    print(" Municípios inseridos!")

if novas_licitacoes:
    supabase.table("licitacoes").upsert(novas_licitacoes).execute()
    print(" Licitações inseridas!")

if novos_itens:
    supabase.table("itens").upsert(novos_itens).execute()
    print(" Itens inseridos!")

if novos_grupos_materiais:
    supabase.table("grupos_materiais").upsert(novos_grupos_materiais).execute()
    print(" Grupos de materiais inseridos!")

if novas_classes_materiais:
    supabase.table("classes_materiais").upsert(novas_classes_materiais).execute()
    print(" Classes de materiais inseridas!")

if novos_cnaes:
    supabase.table("cnaes").upsert(novos_cnaes).execute()
    print(" CNAEs inseridos!")

print(" Dados inseridos com sucesso no Supabase!")
