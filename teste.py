from Pipeline import GetTagByProduct, CompareTag, GetRepresentantesPlastico, GetBlankTag
from Pipeline import GetMachineTypo
from DbConection import create_connection
import pandas as pd
import numpy as np

SECRET_DB_PASSWORD = "3xpdOcTa0HlIjG"
con = create_connection('CRM.db', SECRET_DB_PASSWORD)
df_final = pd.read_sql('SELECT * FROM crm_powerbi', con)

# Cria uma coluna temporária com as tags (ja tem a tag de ACESSÓRIOS devido a funcao GetMachineType)
df_final['tag_periferico'] = df_final['listtags'].apply(GetMachineTypo)

# Verifica se a negociação não possui tag, e muda conforme o representante -> cria uma coluna temporária
df_final['tag_rep_injecao'] = df_final['listnomeresponsaveis'].apply(GetRepresentantesPlastico) 
df_final['tag_rep_injecao_final'] = GetBlankTag(df_final, 'tag_rep_injecao', 'listtags')

# Junta as negociacoes tageadas com as sem tag -> cria uma coluna temporária: nova_tag
df_final['tag_rep_injecao_periferico_final'] = CompareTag(df_final,'tag_periferico', 'tag_rep_injecao_final')

# Verifica se existe Produto, nesse caso a tag não importa
df_final['tag_produto'] = df_final['modelo'].apply(GetTagByProduct)

# Cria a coluna com todas as condicionais 
df_final['nova_tag'] = CompareTag(df_final, 'tag_rep_injecao_periferico_final', 'tag_produto')

# Remove as colunas temporárias
df_final = df_final.drop(['tag_rep_injecao_final','tag_rep_injecao','tag_rep_injecao_periferico_final',
                            'tag_produto','tag_periferico'], axis=1)

print(df_final['nova_tag'].value_counts())


