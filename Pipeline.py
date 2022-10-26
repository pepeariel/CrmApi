import asyncio
import aiohttp
import pandas as pd
import numpy as np

class AsyncIO:

    ''' Classe para requisição assíncrona de dados na API do sistema CRM simples'''

    def __init__(self, SECRET_API_KEY):
        self.results = [] # Lista com os ids unicos de cada negociação
        self.neg = [] # Lista com as negociações - é uma lista de requisições Json
        self.pag = [i for i in range(1,135)] # numero de paginas a serem requisitadas - cada pag possui 100 negociações
        self.dataInicial = '2019/01/01'
        self.filter = {'statusNegociacao' : 'Ganha|Perdida|Pendente'}
        self.idExterno = str()
        self.headers = { 'token': SECRET_API_KEY,
                         'Content-Type': 'application/json'}
        self.df_id = pd.DataFrame() # Dataframe normalizado com os ids unicos de cada negociação
        self.df_negociacoes = pd.DataFrame() # Dataframe normalizado final com todas as negociações

    def get_tasks(self, session):
        tasks = []
        for pagina in self.pag:
            tasks.append(
                session.get(url=f'https://api.crmsimples.com.br/API?method=getListNegociacao&pagina={pagina}&dataInicial={self.dataInicial}&filter={self.filter}',
                            headers=self.headers,
                            ssl=False))
        return tasks

    def taskNegociacao(self, session):
        tasks = []
        for idInterno in self.df_id.iloc[:,0]:
            tasks.append(
                session.get(url=f'https://api.crmsimples.com.br/API?method=getNegociacao&idExterno={self.idExterno}&idInterno={idInterno}',
                            headers=self.headers,
                            ssl=False))
        return tasks

    async def GetIdOfNegociacoesGanhas(self):
        async with aiohttp.ClientSession() as session:
            tasks = AsyncIO.get_tasks(self, session)
            responses = await asyncio.gather(*tasks)
            for r in responses:
                requested_id = await r.json()
                if len(requested_id['ListIdInterno']) > 0: # Verifica se existe algum registro para "apendar"
                    self.results.append(requested_id)

    async def GetlistofNegociacoes(self):
        async with aiohttp.ClientSession() as session:
            tasks = AsyncIO.taskNegociacao(self, session)
            responses = await asyncio.gather(*tasks)
            for r in responses:
                requested_id = await r.json()
                self.neg.append(requested_id)

    def ConvertToDataframe(self):
        for r in self.results:
            list_id_atual = pd.Series((r['ListIdInterno']))
            self.df_id = pd.concat([self.df_id, list_id_atual])
            self.df_id = self.df_id.applymap(lambda x: int(x))
        return self.df_id

    def JsonToDataFrame(self):
        for n in self.neg:
            n = pd.json_normalize(n)
            self.df_negociacoes = pd.concat([self.df_negociacoes, n])
        return self.df_negociacoes

def GetMachineTypo (series):
    intersecao = 'OUTROS'

    if 'PERIFÉRICOS' in series:
        intersecao = 'ACESSÓRIOS'
            
    elif ('INJETORA' in series) or ('INJEÇÃO' in series):
        intersecao = 'INJETORA'    
            
    elif ('CENTRO' in series):
        intersecao = 'CENTRO'       
            
    elif ('SOPRADORA' in series) or ('EXTRUSÃO' in series) or ('SOPRO' in series) or ('EXTRUSORA' in series):
        intersecao = 'EXTRUSÃO'
            
    elif ('TORNO' in series):
        intersecao = 'TORNO'
            
    elif ('ROBO' in series) or ('ROBÔ' in series) or ('ROBÓTICA' in series):
        intersecao = 'ROBÔ'   
            
    elif ('CELULA' in series) or ('CÉLULA' in series):
        intersecao = 'CÉLULA ROBOTIZADA'    
            
    elif ('LASER' in series):
        intersecao = 'LASER'
            
    elif ('AUTOMAÇÃO' in series):
        intersecao = 'CÉLULA ROBOTIZADA'
            
    return intersecao

def FiltraDatas(base, coluna_data, data_inicial, data_final):
    base[coluna_data] = pd.to_datetime(base[coluna_data], errors='coerce')
    base = base.loc[(base[coluna_data] >= data_inicial) & (base[coluna_data] < data_final)]
    return base

def GetDictKey(series, chave):
    key = [dic[chave] for dic in series]
    return key

def GetQNTD(series, valor):
    if len(series) == 0:
        valor = 1
    elif len(series) == 1:
        valor = int(series[0])
    else:
        mais_de_um_valor = []
        for valores in series:
            mais_de_um_valor.append(int(valores))
            valor = sum(mais_de_um_valor)
    return valor

def GetTagByProduct(tag_atual):
    # Coloca tudo em maiscula
    tag_atual = tag_atual.upper()
    # Chaves a serem buscadas na coluna modelo, deve retornar alguma das tags abaixo, caso contrário mantem a tag original
    dic_produtos = {
    'USINAGEM': 'CENTRO', 'TORNO': 'TORNO', 'TORNEAMENTO': 'TORNO',
    'LASER': 'LASER','DOBRADEIRA':'DOBRADEIRA',
    'INJETORA': 'INJETORA','EXTRUSÃO': 'EXTRUSORA','SOPRO': 'SOPRADORA'}
    # Suposição inicial q não tem TAG
    tag_final = 'SEM TAG'
    for chave, valor in dic_produtos.items():
        if chave in tag_atual:
            tag_final = valor
            break

    return tag_final

# A tag gerada pelo produto sobrescreve a tag antiga, se não tiver produto a tag se mantem
def CompareTag(df_final, nova_tag, tag_produto):
    condition = [df_final[tag_produto] == 'SEM TAG']
    choice = [df_final[nova_tag]]
    tag = np.select(condition, choice, default = df_final[tag_produto])
    return tag

# Coluna temporaria para definir as negociações sem tag e que são dos representantes de plástico
def GetRepresentantesPlastico(series):
    series = series.upper()
    intersecao = 'SEM TAG'
    representantes_plastico = ['REGINALDO', 'MAURUS','EDIVALDO','CARLOS', 'SPOTTI']
    for representante in representantes_plastico:
        if representante in series:
            intersecao = 'INJETORA'
            break
    return intersecao

def GetBlankTag(df_final, tag_rep, tag_atual):
    condition = [(df_final[tag_rep] == 'INJETORA') & (df_final[tag_atual] == '[]')]
    choice = [df_final[tag_rep]]
    tag = np.select(condition, choice, default = 'SEM TAG')
    return tag



