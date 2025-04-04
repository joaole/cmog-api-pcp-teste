import os

import dotenv
from supabase import Client, create_client


dotenv.load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE = os.getenv("SERVICE_ROLE")

print(f"SUPABASE_URL: {SUPABASE_URL}")
print(f"SERVICE_ROLE: {SERVICE_ROLE}")

if not SUPABASE_URL or not SERVICE_ROLE:
    raise ValueError("SUPABASE_URL and SERVICE_ROLE must be set in the environment variables.")
supabase: Client = create_client(SUPABASE_URL, SERVICE_ROLE)


def selecionar_tabela(nome_tabela):
    try:
        response = supabase.table(nome_tabela).select("*").execute()
        print(f"Dados da tabela {nome_tabela} selecionados com sucesso!")
        return response.data
    except Exception as e:
        print(f"Erro ao acessar a tabela {nome_tabela}: {e}")

licitacoes = selecionar_tabela("licitacoes")

id_antigas = licitacoes['id_licitacao']
    
# Filtrar apenas as novas licitações que não estão no banco de dados
licitacoes_data = [l for l in licitacoes_data if l['id_licitacao'] not in id_antigas]

# Filtrar os itens, grupos de materiais, classes de materiais e CNAEs relacionados às novas licitações
itens_data = [i for i in itens_data if i['id_licitacao'] in [l['id_licitacao'] for l in licitacoes_data]]
grupos_materiais_data = [g for g in grupos_materiais_data if g['id_licitacao'] in [l['id_licitacao'] for l in licitacoes_data]]
classes_materiais_data = [c for c in classes_materiais_data if c['id_grupo_material'] in [g['id_grupo_material'] for g in grupos_materiais_data]]
cnaes_data = [c for c in cnaes_data if c['id_licitacao'] in [l['id_licitacao'] for l in licitacoes_data]]   
    