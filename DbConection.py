import time
import psycopg2

def create_connection(db_file, SECRET_DB_PASSWORD):
    try:
        con = psycopg2.connect(
            host='database-1.c7xrnorluyxo.us-east-1.rds.amazonaws.com',
            port='5432',
            user='postgres',
            password=SECRET_DB_PASSWORD
        )

    except Exception as e:
        print(e)

    return con

def insert_query(con, sql, cursor):

    # Comando para inserir novos dados no banco
    query = '''INSERT INTO CRM_POWERBI (indice, CategoriaNegociacao,FunilNegociacao,EtapaNegociacao,StatusNegociacao,
                                        NomeUsuarioConclusao,ConcluidaEm,PrevisaoFechamento,
                                        MotivoGanhoPerda,SubmotivoGanhoPerda,Valor,ValorOutros,Observacoes,
                                        NomeUsuarioInclusao,NomeUsuarioAtualizacao,
                                        ListProduto,ListNomeResponsaveis,ListTags,IdInterno, 
                                        Nome,Descricao,CriadaEm,AtualizadaEm,ContatoIdInterno,ContatoNome,
                                        ContatoCnpjCpf,OrganizacaoNome,Organizacao,
                                        NOVA_TAG, MODELO,QNTD)
            
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                '''
    # Inserir dados da tupla sql no banco AWS
    print('Inserindo dados no banco...')
    try:
        cursor.executemany(query, sql)
        con.commit()
        print('Operação concluída com sucesso!')
    except Exception as e:
        print('Erro devido a:',e)

    finally:
        time.sleep(2)
        con.close()


def delete_query(con, cursor):
    query = 'DELETE FROM CRM_POWERBI;'
    cursor.execute(query)

    return print('Dados do Banco Deletados com Sucesso!')


