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
grupos_materiais_data = []
classes_materiais_data = []
cnaes_data = []

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

            # LICITAÇÃO
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

            # GRUPOS MATERIAIS E CLASSES
            for grupo in licitacao["gruposMateriais"]:
                grupo_data = {
                    "id_grupo_material": grupo["idGrupoMaterial"],
                    "nome_grupo_material": grupo["nomeGrupoMaterial"],
                    "id_licitacao": licitacao["idLicitacao"],
                }
                if grupo_data["id_grupo_material"] not in [
                    g["id_grupo_material"] for g in grupos_materiais_data
                ]:
                    grupos_materiais_data.append(grupo_data)

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

            # CNAEs
            for cnae in licitacao["cnaes"]:
                cnae_data = {
                    "cnae": cnae["cnae"],
                    "descricao": cnae["descricao"],
                    "id_licitacao": licitacao["idLicitacao"],
                }
                if cnae_data["cnae"] not in [c["cnae"] for c in cnaes_data]:
                    cnaes_data.append(cnae_data)
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
log_upsert("grupos_materiais", grupos_materiais_data)
log_upsert("classes_materiais", classes_materiais_data)
log_upsert("cnaes", cnaes_data)

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
print(f"• Grupos de materiais: {len(grupos_materiais_data)}")
print(f"• Classes de materiais: {len(classes_materiais_data)}")
print(f"• CNAEs processados: {len(cnaes_data)}")
print(f"• Licitações excluídas: {len(to_delete_ids)}")
print("✅ Dados atualizados com sucesso!")
