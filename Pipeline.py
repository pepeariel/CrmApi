import asyncio
import aiohttp
import pandas as pd


class AsyncIO:

    ''' Classe para requisição assíncrona de dados na API do sistema CRM simples'''

    def __init__(self, SECRET_API_KEY):
        self.results = [] # Lista com os ids unicos de cada negociação
        self.neg = [] # Lista com as negociações - é uma lista de requisições Json
        self.pag = [i for i in range(1,100)] # numero de paginas a serem requisitadas - cada pag possui 100 negociações
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
            tasks = AsyncIO.get_tasks(self,session)
            responses = await asyncio.gather(*tasks)
            for r in responses:
                requested_id = await r.json()
                if requested_id['HasMore'] == True:
                    self.results.append(requested_id)

    async def GetlistofNegociacoes(self):
        async with aiohttp.ClientSession() as session:
            tasks = AsyncIO.taskNegociacao(self,session)
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

def GetMachineTypo (series, intersecao):
    for palavra in series:
        if ('INJETORA' in palavra) or ('INJEÇÃO' in palavra):
            intersecao = 'INJETORA'
            break
        elif ('CENTRO' in palavra):
            intersecao = 'CENTRO'
            break
        elif ('TORNO' in palavra):
            intersecao = 'TORNO'
            break
        elif ('ROBO' in palavra) or ('ROBÔ' in palavra):
            intersecao = 'ROBÔ'
            break
        elif ('CELULA' in palavra) or ('CÉLULA' in palavra):
            intersecao = 'CÉLULA ROBOTIZADA'
            break
        elif ('LASER' in palavra):
            intersecao = 'LASER'
            break
        elif ('AUTOMAÇÃO' in palavra):
            intersecao = 'CÉLULA ROBOTIZADA'
            break
        else:
            intersecao = 'OUTROS'

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




