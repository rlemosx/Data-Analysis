    # -*- coding: utf-8 -*-
"""
Created on Sun Dec 08 22:12:12 2019

@author: roberto.lemos
"""

import pandas as pd
import numpy as np
import os as os

## Diretório para ler a função de conectar no banco
os.chdir('C:/Users/roberto.lemos/iCloudDrive/Documentos Kroton/13. Python/02. Funções')
import conecta_banco 

## Diretório para salvar os dados 
os.chdir('C:/Users/roberto.lemos/Desktop/Outputs Colação')

#Padrão de código:

################### Usar para título de grandes módulos
################### Usar para subtítulo de grandes módulos

## usar como Título de pequenos módulos
# usar como subtítulo de pequenos módulos

###############################################################################################################
###############################################################################################################

################### IMPORTAR DADOS NA TABELA COLAÇÃO
################### Buscar alunos elegíveis e fazer carga no sistema de colação

## Início Consulta Olimpo
conn =  conecta_banco.conecta_bd('OLIMPO')

pres_201 = pd.read_sql("""select from where""", conn)
                               
conn.close()
## Fim consulta Olimpo

## Início Consulta Colaborar
conn =  conecta_banco.conecta_bd('COLABORAR')

ead_201 = pd.read_sql("""
                      select from where 
                      """, conn)

ead_192 = pd.read_sql(""" select from where """, conn)

conn.close()
## Fim Consulta Colaborar 


## Início Consulta Colação
conn = conecta_banco.conecta_bd('COLACAO')

cursor = conn.cursor()
# Puxa versão atualizada da tabela
cadastradas = pd.read_sql("""select from where """,conn)

conn.close()
## Fim Consulta Colaborar 


## Prepara bases para formato da Colação
pres_201 = pres_201[['CODIGOALUNO','RA','NOME','CPF','CODIGOCURSO','CURSO','UNIDADE','ENDERECOUNIDADE','CNPJ','CODEMPRESA','MARCA', 'DIRETOR','SECRETARIOACADEMICO', 'LOCAL', 'SISTEMA', 'GRAU', 'DATACOLACAO', 'ATIVO', 'PERIODO', 'DATAIMPORTACAO', 'MAT_SN']]
ead_201 = ead_201[['CODIGOALUNO','RA','NOME','CPF','CODIGOCURSO','CURSO','UNIDADE','ENDERECOUNIDADE','CNPJ','CODEMPRESA','MARCA', 'DIRETOR','SECRETARIOACADEMICO', 'LOCAL', 'SISTEMA', 'GRAU', 'DATACOLACAO', 'ATIVO', 'PERIODO', 'DATAIMPORTACAO', 'MAT_SN']]
ead_192 = ead_192[['CODIGOALUNO','RA','NOME','CPF','CODIGOCURSO','CURSO','UNIDADE','ENDERECOUNIDADE','CNPJ','CODEMPRESA','MARCA', 'DIRETOR','SECRETARIOACADEMICO', 'LOCAL', 'SISTEMA', 'GRAU', 'DATACOLACAO', 'ATIVO', 'PERIODO', 'DATAIMPORTACAO', 'MAT_SN']]

# Empilha base em uma única estrutura
formandos = pd.concat([pres_201, ead_192, ead_201]).drop_duplicates()

#Corrige nomes de curso ead
formandos['CURSO'] = np.where(formandos.CURSO == 'ENFERMAGEM'
                            ,' de ENGERMAGEM'
                            ,np.where(formandos.CURSO == 'ENGENHARIA CIVIL'
                                      , 'de ENGENHARIA CIVIL'
                                      , np.where(formandos.CURSO == 'ENGENHARIA DE COMPUTAÇÃO'
                                                , 'de ENGENHARIA DE COMPUTAÇÃO'
                                                ,np.where( formandos.CURSO == 'ENGENHARIA DE PRODUÇÃO'
                                                          ,'de ENGENHARIA DE PRODUÇÃO'
                                                          , formandos.CURSO))))

#Limpa DF's obsoletos
del pres_201, ead_201, ead_192



## Valida para fazer carga somente de novos alunos
importar = pd.merge(formandos, cadastradas, how = 'left', on = 'RA', copy = False, indicator = True)
importar = importar[importar['_merge'] == 'left_only']


## Valida se tem alguma empresa sem secretário
importar = importar[importar.SECRETARIOACADEMICO.isna() == False]


## Início Consulta Colação
conn = conecta_banco.conecta_bd('COLACAO')

cursor = conn.cursor()

# Insert DataFrame to Table
for row in importar.itertuples():
    cursor.execute("""
                INSERT INTO aluno (  column1
                                   , column2
                                   , column3
                                   , column4
                                   , column5
                                   , column6
                                   , column7
                                   , column8
                                   , column9
                                   , column10
                                   , column11
                                   , column12
                                   , column13
                                   , column14
                                   , column15
                                   , column16
                                   , column17
                                   , column18
                                   , column19 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,  row.column1
                , row.column2
                , row.column3
                , row.column4
                , row.column5
                , row.column6
                , row.column7
                , row.column8
                , row.column9
                , row.column10
                , row.column11
                , row.column12
                , row.column13
                , row.column14
                , row.column15
                , row.column16
                , row.column17
                , row.column18 
                , row.column19)
conn.commit()
conn.close()


## Cria base auxiliar para importação na SN
carga = importar[['CPF','RA','MAT_SN','SISTEMA']]

# Limpa DF's Obsoletos
del row, importar, cadastradas

################### IMPORTAR DADOS NA SERVICENOW
################### Buscar alunos já cadastrados e fazer carga no sistema da SN
from datetime import datetime, date
from pandas.io.json import json_normalize
import pysnow

## Conecta na SN      
conn = pysnow.Client(user='user', password= 'senha', instance='instance')

conn.parameters.display_value = True
conn.parameters.exclude_reference_link  = True
        
# Gera arquivo tabela        
table = conn.resource(chunk_size=81920, api_path='/table/tablename')        
        
# Gera querie na SN
last_extract_date = datetime(2020,1,1)
qb = (pysnow.QueryBuilder().field('sys_created_on').greater_than(last_extract_date))
        
# Configura início e tamanho máximo de Extração da SN        
offset = 0
limit = 10000

# Cria estrutura de chamad
response = table.get(offset=offset, limit=limit, stream=True)


# Checa tamanho máximo de dados
response_headers = response.headers
# Cria estrutura de loop baseado no limite
total_count = int(response_headers["X-Total-Count"])
loop_iterations = int(total_count) // limit +1
aux = pd.DataFrame()
colacao = pd.DataFrame()

# Início captura de dados
if total_count > 10000:
    print("Pagination Required")
    pag_flag=1
else:
    print("Pagination not required")
    pag_flag=0

if pag_flag==1:
    for i in range(loop_iterations): #Here I added +1 to just to get until the end
        print(i)
        response = table.get(offset=offset, limit=limit)
        aux = pd.DataFrame.from_dict(json_normalize(response.all()),orient = 'columns')
        colacao = colacao.append(aux,sort = False, ignore_index = True)
        offset += limit


else:
    aux = pd.DataFrame.from_dict(json_normalize(response.all()),orient = 'columns')
    
# Cria base auxiliar para importação na SN
colacao = colacao[['u_cpf','u_ra','u_data_colacao_digital','sys_id']]

# Limpa DF's obsoletos
del  aux, last_extract_date, limit, offset, pag_flag, total_count, loop_iterations, i

# Criar output para importação
importar_sn = pd.merge(carga, colacao, how = 'left', left_on = 'MAT_SN', right_on = 'u_ra', copy = False, indicator = True)
importar_sn = importar_sn[importar_sn['_merge'] == 'left_only']
importar_sn = importar_sn[importar_sn.u_data_colacao_digital.isna()]
importar_sn = importar_sn[['MAT_SN','CPF','SISTEMA']]


importar_sn.rename(columns={'MAT_SN':'RA'}, inplace=True)
importar_sn.to_excel('importar_sn_'+date.today().strftime('%Y-%m-%d')+'.xlsx', index = False)
## Fim leitura de dados


################### DELETAR DADOS NA SERVICENOW
################### Buscar alunos que já não deveriam estar cadastrados e fazer delete no sistema da SN

## Pega base com os registros para deletar
deletar_sn = pd.merge(formandos, colacao, how = 'outer', left_on = 'MAT_SN', right_on = 'u_ra', copy = False, indicator = True)
deletar_sn = deletar_sn[deletar_sn['_merge'] == 'right_only']
deletar_sn = deletar_sn[deletar_sn.u_data_colacao_digital == '']

deletar_sn.to_excel('deletado_sn_'+date.today().strftime('%Y-%m-%d')+'.xlsx', index = False)

deletar_sn_id = deletar_sn[['sys_id']]


for row in deletar_sn_id.itertuples():
    print(row.sys_id)    
    table.delete('sys_id='+row.sys_id)
    
print('fim das exclusões')

del deletar_sn_id, row


################### ATUALIZAR DADOS NO PORTAL COLAÇÃO
################### PEGAR DADOS DOS ALUNOS QUE FORAM ATUALIZADOS NA SN E LIBERAR COLACAO NA MÃO

## Lembrar que já tem um servidor rodando esta parte do código
## Início Consulta Colação
conn = conecta_banco.conecta_bd('COLACAO')

cursor = conn.cursor()
# Puxa versão atualizada da tabela
cadastradas = pd.read_sql("""select  from where   """,conn)

#cadastrada = pd.read_sql(""" select from where""", conn)
conn.close()
## Fim Consulta Colação 




atualizar = pd.merge(cadastradas, colacao, how = 'left', left_on = 'CPF', right_on = 'u_cpf', copy = False, indicator = True)
atualizar = atualizar[atualizar['_merge'] == 'both']
atualizar = atualizar[atualizar.u_data_colacao_digital != '']
atualizar['Ativo'] = 1


## Início Atualiza liberação de ata
conn = conecta_banco.conecta_bd('COLACAO')

cursor = conn.cursor()

for row in atualizar.itertuples():
    
    cursor.execute("UPDATE aluno SET Column1 = ? WHERE Column2 = ?", row.Column1, row.Column2)

conn.commit()

conn.close()

atualizar.to_excel('ata_liberada_pda_'+date.today().strftime('%Y-%m-%d')+'.xlsx', index = False)

## Fim código

colacao.to_excel('base_colacao_'+date.today().strftime('%Y-%m-%d')+'.xlsx', index = False)
