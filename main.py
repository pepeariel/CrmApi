import DbConection
from Pipeline import AsyncIO, GetMachineTypo, FiltraDatas, GetDictKey, GetQNTD
import asyncio
import pandas as pd
import time
import os
from dotenv import load_dotenv, find_dotenv
import warnings
warnings.filterwarnings('ignore')

# Inciar as variáveis de ambiente
load_dotenv(find_dotenv())
SECRET_DB_PASSWORD = os.environ.get('SECRET_DB_PASSWORD')
SECRET_API_KEY = os.environ.get('SECRET_API_KEY')

if __name__ == '__main__':
    # Inicializa a classe de requisição assíncorna
    id = AsyncIO(SECRET_API_KEY)

    # -> Pega a lista com todos os Ids das negociações e armazena em DataFrame
    asyncio.run(id.GetIdOfNegociacoesGanhas())

    df_id = id.ConvertToDataframe()

    print('Aguardando requisição na API...')
    time.sleep(5)

    asyncio.run(id.GetlistofNegociacoes())  # -> Pega as negociações atreladas a lista de ids e armazena em Dataframe

    df_final = id.JsonToDataFrame()

    df_final['ConcluidaEm'] = pd.to_datetime(df_final['ConcluidaEm'],
                                             dayfirst=True,
                                             format='%Y-%m-%d %H:%M:%S').astype(str)

    df_final['CriadaEm'] = pd.to_datetime(df_final['CriadaEm'],
                                          dayfirst=True,
                                          format='%Y-%m-%d %H:%M:%S').astype(str)

    df_final['ListTags'] = df_final['ListTags'].apply(GetDictKey, chave='Descricao')

    df_final['NOVA_TAG'] = df_final['ListTags'].apply(GetMachineTypo, intersecao='Nan')

    replace_representantes = {
        'Edison Boscaini Griti': 'ÉDISON BOSCAINI',
        'Joel Sprenger': 'JOEL SPRENGER',
        'Odirlei Costa': 'ODIRLEI',
        'Adriano Ossani': 'ADRIANO OSSANI',
        'Gustavo Castelhano': 'GUSTAVO CASTELHANO',
        'Reginaldo Maia': 'REGINALDO MAIA',
        'Vanderlei Bedin': 'VANDERLEI BEDIN',
        'Alexandre Wanzuita': 'ALEXANDRE',
        'Henrique Albuquerque': 'HENRIQUE ALBUQUERQUE',
        'Guilherme Simas Spotti': 'GUILHERME SPOTTI',
        'GUSTAVO HENRIQUE DE ALBUQUQUERQUE': 'GUSTAVO ALBUQUERQUE',
        'Rafael- Industria 4.0': 'RAFAEL',
        'Maurus Joenk': 'MAURUS JOENK',
        'Edivaldo Dallepiane': 'EDIVALDO DALLEPIANE',
        'Sergio Miwa': 'SERGIO MARQUES MIWA',
        'Carlos Leandro': 'CARLOS',
        'Herberte Piassi': 'HERBERTE'
    }

    df_final['NomeUsuarioConclusao'] = df_final['NomeUsuarioConclusao'].replace(replace_representantes)
    df_final['MODELO_AUX'] = df_final['ListProduto'].apply(GetDictKey, chave='Produto')
    df_final['MODELO'] = df_final['MODELO_AUX'].apply(GetDictKey, chave='Descricao')
    df_final = df_final.drop(['MODELO_AUX'], 1)

    df_final['QNTD_VENDIDA'] = df_final['ListProduto'].apply(GetDictKey, chave='Quantidade')
    df_final['QNTD'] = df_final['QNTD_VENDIDA'].apply(GetQNTD, valor=1)
    df_final = df_final.drop(['QNTD_VENDIDA'], 1)

    new_index = pd.Series([i for i in range(len(df_final))])
    df_final.insert(0, 'indice', new_index)

    df_final['ListProduto'] = df_final['ListProduto'].astype(str)
    df_final['ListCampoUsuario'] = df_final['ListCampoUsuario'].astype(str)
    df_final['ListIdResponsaveis'] = df_final['ListIdResponsaveis'].astype(str)
    df_final['ListNomeResponsaveis'] = df_final['ListNomeResponsaveis'].astype(str)
    df_final['ListIdExternoResponsaveis'] = df_final['ListIdExternoResponsaveis'].astype(str)
    df_final['ListTags'] = df_final['ListTags'].astype(str)
    df_final['MODELO'] = df_final['MODELO'].astype(str)

    # Cria a conexão com o banco de dados
    con = DbConection.create_connection('CRM.db', SECRET_DB_PASSWORD)
    cursor = con.cursor()

    # Pega o nome das colunas no banco
    df_crm_banco = df = pd.read_sql("SELECT * FROM CRM_POWERBI LIMIT 1", con)

    # Seleciona e lineariza as colunas do tabela criada pela API (coloca todas as letras em minusculo e remove pontos)
    colunas_linearizadas = [str(n).lower().replace('.', '') for n in df_final.columns]

    # Colunas diferentes entre as do Dataframe final e colunas do banco AWS
    dif = list(set(colunas_linearizadas) - set(df_crm_banco.columns))

    # Cria um dicionário com chave as colunas antigas e valores com as colunas novas -- para mapear o dataframe final
    # Exemplo: {CategoriaNegociacao: Categorianegociacao}
    novas_colunas = {colunas_antigas: colunas_novas for colunas_antigas, colunas_novas in zip(df_final.columns, colunas_linearizadas)}

    # Mapeaia o dataframe final com o nome das colunas corretos
    df_final = df_final.rename(columns=novas_colunas)
    df_final = df_final.drop(labels=dif, axis=1)
    df_final['indice'] = [index for index in range(len(df_final))]

    # Pega apenas as colunas necessárias para envio ao banco de dados
    df_final = df_final.loc[(df_final['funilnegociacao'] == 'Negociações Máquinas') | (df_final['funilnegociacao'] == 'Negociações ROBÓTICA')]

    # Transforma o Dataframe final em tupla -> formato para inserção no banco
    sql = [tuple(i) for i in df_final.to_numpy()]

    # Deleta o banco de dados atual
    DbConection.delete_query(con, cursor)

    # Atualiza o banco de dados
    DbConection.insert_query(con, sql, cursor)










