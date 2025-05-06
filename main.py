import os

import dotenv
import requests
from supabase import Client, create_client

from constantes.estados_list import estados

dotenv.load_dotenv()

PUBLIC_KEY = os.getenv("PUBLIC_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE = os.getenv("SERVICE_ROLE")

# Supabase client
supabase: Client = create_client(SUPABASE_URL, SERVICE_ROLE)

# API URL
url = "https://apipcp.portaldecompraspublicas.com.br/publico/obterProcessosPropostas/?"

# Dados locais
municipios_data = []
licitacoes_data = []
itens_data = []


# IDs para controle de atualizações
licitacoes_ids_atuais = set()

print("📦 Iniciando busca dos dados...")

for estado in estados:
    print(f"🔍 Buscando dados para o estado: {estado}")
    params = {"publicKey": PUBLIC_KEY, "uf": estado}
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()

        for licitacao in data["dadosLicitacoes"]:
            licitacoes_ids_atuais.add(licitacao["idLicitacao"])

            # MUNICÍPIO
            municipio_data = {
                "codigo_ibge": licitacao["municipio"]["codigoIBGE"],
                "nome_municipio": licitacao["municipio"]["nomeMunicipio"],
                "uf_municipio": licitacao["municipio"]["ufMunicipio"],
            }
            if municipio_data not in municipios_data:
                municipios_data.append(municipio_data)

            # ITENS
            for item in licitacao["itens"]:
                item_data = {
                    "id_item": item["idItem"],
                    "nr_item": str(item["NR_ITEM"]),
                    "ds_item": str(item["DS_ITEM"]),
                    "qt_itens": str(item["QT_ITENS"]),
                    "vl_unitario_estimado": str(item["VL_UNITARIO_ESTIMADO"]),
                    "id_licitacao": licitacao["idLicitacao"],
                }
                if item_data["id_item"] not in [i["id_item"] for i in itens_data]:
                    itens_data.append(item_data)

            # LICITAÇÃO com objeto preenchido
            descricao_itens = []

            for item in licitacao["itens"]:
                if item.get("DS_ITEM"):
                    descricao_itens.append(str(item["DS_ITEM"]).strip())

            for grupo in licitacao["gruposMateriais"]:
                nome_grupo = grupo.get("nomeGrupoMaterial")
                if nome_grupo:
                    descricao_itens.append(str(nome_grupo).strip())
                for classe in grupo.get("classesMateriais"):
                    nome_classe = classe.get("nomeClasseMaterial")
                    if nome_classe:
                        descricao_itens.append(str(nome_classe).strip())

            objeto = " | ".join(descricao_itens).strip()

            # LICITAÇÃO
            licitacao_data = {
                "id_licitacao": licitacao["idLicitacao"],
                "numero": licitacao["numero"],
                "data_abertura_proposta": licitacao["dataAberturaPropostas"],
                "hora_abertura_proposta": licitacao["horaAberturaPropostas"],
                "tipo_licitacao": licitacao["tipoLicitacao"],
                "comprador": licitacao["comprador"],
                "url": licitacao["url"],
                "id_municipio": licitacao["municipio"]["codigoIBGE"],
                "objeto": objeto,
            }
            licitacoes_data.append(licitacao_data)
    else:
        print(f"⚠️ Erro {response.status_code} ao buscar dados de {estado}")

# =============================
# 🔁 Inserindo no Supabase
# =============================
print("📤 Enviando dados para o Supabase...")


def log_upsert(table, data):
    if not data:
        print(f"⚪ Tabela {table}: Nenhum dado a inserir.")
        return
    supabase.table(table).upsert(data).execute()
    count = len(data)
    print(f"🟢 Tabela {table}: {count} registros upsertados.")


log_upsert("municipios", municipios_data)
log_upsert("licitacoes", licitacoes_data)
log_upsert("itens", itens_data)

# =============================
# ❌ Deletando licitações antigas (opcional)
# =============================
print("🧹 Verificando dados obsoletos...")

# Buscar IDs já existentes no banco
existing_ids_response = supabase.table("licitacoes").select("id_licitacao").execute()
existing_ids = set(row["id_licitacao"] for row in existing_ids_response.data)

# Detectar IDs que sumiram
to_delete_ids = existing_ids - licitacoes_ids_atuais

if to_delete_ids:
    print(f"🗑️ Licitações removidas: {len(to_delete_ids)}")
    for id_del in to_delete_ids:
        supabase.table("licitacoes").delete().eq("id_licitacao", id_del).execute()
else:
    print("✅ Nenhuma licitação antiga para excluir.")

# =============================
# ✅ Resumo final
# =============================
print("\n📊 Resumo da execução:")
print(f"• Municípios inseridos: {len(municipios_data)}")
print(f"• Licitações atualizadas/inseridas: {len(licitacoes_data)}")
print(f"• Itens processados: {len(itens_data)}")
print(f"• Licitações excluídas: {len(to_delete_ids)}")
print("✅ Dados atualizados com sucesso!")

"""
Este script coleta dados de licitações públicas de uma API externa, organiza e insere
os dados em um banco de dados Supabase.
Além disso, remove registros antigos que não estão mais presentes na API.

### Funcionalidades principais:
1. **Coleta de dados**:
    - Faz requisições para uma API pública para obter informações de licitações,
    municípios e itens.
    - Processa e organiza os dados coletados.

2. **Inserção no banco de dados**:
    - Insere ou atualiza os dados no banco de dados Supabase usando a operação `upsert`.

3. **Remoção de dados obsoletos**:
    - Identifica e remove registros de licitações que não estão mais presentes na API.

### Estrutura de dados:
- **Municípios**: Contém informações como código IBGE, nome e UF.
- **Licitações**: Inclui detalhes como ID, número, data de abertura, tipo, comprador e
descrição do objeto.
- **Itens**: Lista de itens associados às licitações, com informações como ID,
descrição, quantidade e valor estimado.

### Resumo da execução:
Ao final, o script exibe um resumo com a quantidade de registros inseridos/atualizados
e excluídos.
"""
