    # -*- coding: utf-8 -*-
"""
Created on Fri May 01 12:12:12 2020

@author: roberto.lemos
"""

#Padrão de código:

################### Usar para título de grandes módulos
################### Usar para subtítulo de grandes módulos

## usar como Título de pequenos módulos
# usar como subtítulo de pequenos módulos

import pandas as pd
import numpy as np
import os as os
import io
import boto3
import pyarrow.parquet as pq


#Diretório para ler a função de conectar no banco
os.chdir('C:/Users/roberto.lemos/iCloudDrive/Documentos Kroton/13. Python/02. Funções')
import conecta_banco 


################### Primeira parte - pegar todas as bases que precisamos
################### Vamos buscar base aluno (vai ser a consolidada), chamadas, convenio sit fin. e as unidades

## Buscar base de chamadas
conn = conecta_banco.conecta_bd('WEON')

cursor = conn.cursor()

chamadas = pd.read_sql("""SELECT FROM WHERE """,conn)

conn.close()
## Fecha resumo para obter dados por querie WeOn

## Buscar bases no olimpo
conn = conecta_banco.conecta_bd('OLIMPO')

cursor = conn.cursor()
# Consolida situação do aluno
alunos = pd.read_sql("""  SELECT FROM WHERE """, conn)

# Consolida percentual de bolsa do aluno
convenio = pd.read_sql(""" SELECT FROM WHERE """, conn)
  
# Consolida situação financeira do aluno (Adimplência e bolsa_prioridade)  
sit_financeira = pd.read_sql("""SELECT FROM WHERE """, conn)

# Traz as informações das unidades já cruzadas com MKT, trzendo consultor e amostra de NPS válido
unidades = pd.read_sql(""" SELECT FROM WHERE """, conn)

conn.close()

## Busca informação de engajamento da Aws da X

#Credenciais S3 X
s3 = boto3.resource(service_name = 'WKEY',
                      region_name='WKEY',
                      aws_access_key_id='WKEY',
                      aws_secret_access_key='WKEY')
bucket = s3.Bucket('BUCKET')


buffer = io.BytesIO()
s3_object =  s3.Object('PATHt')
s3_object.download_fileobj(buffer)

engajamento_ac = pq.read_table(buffer).to_pandas()



##Fecha resumo para obter dados por querie Olimpo
## Fim da busca de dados

################### Segunda parte - criar todas as bases nível aluno
################### Vamos criar todas as bases auxiliares que precisamos para consolidar no final

## Prepara base de chamadas por cpf
chamadas_aluno = chamadas.groupby(['contact_identifier']).apply(lambda x: 
                                                            pd.Series(dict(qtd_chamadas = (x.contact_identifier).count()))).reset_index()

# limpa eventuais sujeiras que tiver na base
chamadas_aluno = chamadas_aluno[chamadas_aluno['contact_identifier'] != '']    
chamadas_aluno['contact_identifier'] = chamadas_aluno['contact_identifier'].astype(str)

## Prepara base perc bolsa 
convenio_aluno = convenio.groupby(['SEMESTRE_BOLSA','ALUCOD','ESPCOD']).apply(lambda x:
                                                                        pd.Series(dict (percentual_bolsa = ((x.CMEPERCENTUAL).sum()/((x.MENMES).drop_duplicates()).count() )))).reset_index()

# Cria coluna com status final
convenio_aluno['sit_bolsa'] = np.where(convenio_aluno['percentual_bolsa'] >= 100 , '100%Bolsista', 'Pagante')

## Prepara base sit_financeira adimplencia por aluno
sit_financeira['aux_mens_valido'] = np.where(sit_financeira['CAIXA'] == 'NÃO'
                                                    , np.where(sit_financeira['BASE'] == 'CONVMENS'
                                                               , 1
                                                               , 0)
                                                    , 1)                                                    


sit_financeira['sit_adimplencia'] = np.where(sit_financeira['aux_mens_valido'] == 1
                                               , np.where( sit_financeira['TEMPO_PAGAMENTO'] == 'Pago até o vencimento'
                                                              , 1
                                                              , np.where(sit_financeira['TEMPO_PAGAMENTO'] == 'Pago mes'
                                                                         , 1
                                                                         , np.where(sit_financeira['TEMPO_PAGAMENTO'] == 'Isento'
                                                                                    , 1
                                                                                    , 0)))
                                               , 0 )

# Prepara base sit_financeira inadimplencia por aluno
sit_financeira['sit_inadimplencia'] = np.where(sit_financeira['aux_mens_valido'] == 1
                                               , np.where(sit_financeira['sit_adimplencia'] == 1
                                                          , 0 
                                                          , 1)
                                               , 0)

# Filtra só até abril dado do período do estudo (início de maio, não faria sentido trazer)
                                               ## Tomar cuidado par sempre validar o que for período
sit_financeira_aluno = sit_financeira[sit_financeira['MES_VENC'] == 7]

# Cria coluna com valor mensalidade correto

sit_financeira_aluno['valor_mens_limpo'] = np.where(sit_financeira_aluno['aux_mens_valido'] == 1
                                                    , sit_financeira_aluno['VALOR_MENS']
                                                    , 0)
                                                    
                                                    

# Cria tabela por aluno com informações simplificadas para consolidação                                                  
sit_financeira_aluno = sit_financeira_aluno.groupby(['ALUCOD','ESPCOD','BOLSA_PRIORIDADE']).apply(lambda x:
                                                                                            pd.Series(dict ( ticket_semestral = (x.valor_mens_limpo).sum()
                                                                                                             , ticket_medio = (x.valor_mens_limpo).sum()/(x.aux_mens_valido).sum()
                                                                                                             , mens_adimplente = (x.sit_adimplencia).sum()
                                                                                                             , mens_inadimplente = (x.sit_inadimplencia).sum()
                                                                                                             , mens_total = (x.aux_mens_valido).sum() ))).reset_index()

# Cria status final da situação do aluno    
sit_financeira_aluno['sit_fin'] = np.where(sit_financeira_aluno['mens_adimplente']/sit_financeira_aluno['mens_total'] >= 1
                                           ,'Adimplente'
                                           ,np.where(sit_financeira_aluno['mens_inadimplente']/sit_financeira_aluno['mens_total'] >= 1
                                                     ,'Inadimplente'
                                                     ,'Parcialmente inadimplente'))

temp = sit_financeira_aluno.head(50)
# Criar coluna Rank para garantir que vou manter apenas um registro por aluno, priorizando pelo ticket. limpando na

sit_financeira_aluno['ticket_medio'].fillna(0, inplace=True)

sit_financeira_aluno['rank'] = sit_financeira_aluno.groupby(['ALUCOD','ESPCOD'])['ticket_medio'].rank(ascending=False)

# Limpa linha duplicada q tenha menor valor para normalizar a base a 1 linha por aluno (matrícula, não cpf)
sit_financeira_aluno = sit_financeira_aluno[sit_financeira_aluno['rank'] == 1]
    

# Limpa base alunos de possíveis sujeiras
alunos = alunos[alunos['ALUCPF_WEON'] != '']
alunos['ALUCPF_WEON'] = alunos['ALUCPF_WEON'].astype(str)

## Prepara base da AC

#filtra só presencial
engajamento_ac_aluno = engajamento_ac[engajamento_ac['PRESENCIAL_EAD'] == 'Presencial']

# valida se tem dado replicado
engajamento_ac_aluno['rank'] = engajamento_ac_aluno.groupby(['ALUCOD','COD_CURSO'])['ALUCOD'].rank(ascending=False)

#Filtra rank limpo
engajamento_ac_aluno = engajamento_ac_aluno[engajamento_ac_aluno['rank'] <= 1]

# tira só as colunas que fazem sentido para o estudo
engajamento_ac_aluno = engajamento_ac_aluno[['ALUCOD'
                                       ,'COD_CURSO'
                                       , 'TIPO_OFERTA'
                                       , 'ALUNO_ATIVO'
                                       , 'REMATRICULADO'
                                       , 'HISTORICO_ACADEMICO_192'
                                       , 'CONFIRMOU_HORARIO'
                                       , 'ACESSO_AVA'
                                       , 'ENGAJAMENTO_AC'
                                       , 'ENGAJAMENTO_ACADEMICO']]

#Renomear coluna código de curso para espcod
engajamento_ac_aluno = engajamento_ac_aluno.rename(columns={'COD_CURSO': 'ESPCOD'})

## Fim da criação das bases auxiliares


################### Terceira parte - consolidar todas as visões aluno em uma só
################### Vamos consolidar todas as visões a nível aluno, centralizando na base alunos principal

## Primeiro, traz a informação de chamadas
alunos_consolidado = pd.merge(alunos, chamadas_aluno, how = 'left', left_on ='ALUCPF_WEON', right_on = 'contact_identifier')

## Segundo, traz a informação da situação de convênio do aluno
alunos_consolidado = pd.merge(alunos_consolidado, convenio_aluno, how = 'left', on = ['ALUCOD','ESPCOD'])

## Terceiro, traz a informação da situação financeira do aluno
alunos_consolidado = pd.merge(alunos_consolidado, sit_financeira_aluno, how = 'left', on = ['ALUCOD','ESPCOD'])

## Quarto, traz a informação da unidade pertencente aos alunos
alunos_consolidado = pd.merge(alunos_consolidado, unidades, how = 'left', on = ['IUNICODEMPRESA'])

## Quinto, traz a informação de engajamento acadêmico da AC e AVA

# Tive q fazer essa gambiarra para conseguir fazer join com a AC
alunos_consolidado['ESPCOD'] = alunos_consolidado['ESPCOD'].astype(str)
engajamento_ac_aluno['ESPCOD'] = engajamento_ac_aluno['ESPCOD'].astype(str)

# Consolida a base final
alunos_consolidado = pd.merge(alunos_consolidado, engajamento_ac_aluno, how = 'left' , on = ['ALUCOD','ESPCOD'])

## Faz os últimos ajustes para mudarmos a etapa

# Cria coluna de perfil aluno baseado no semestre
alunos_consolidado['fase_curso'] = np.where(alunos_consolidado['ETAPA_TURMA']/alunos_consolidado['ESPDURACAO'] <= 0.2
                                             , 'CALOURO'
                                             , np.where(alunos_consolidado['ETAPA_TURMA']/alunos_consolidado['ESPDURACAO'] >= 0.8
                                                        ,np.where(alunos_consolidado['ETAPA_TURMA']/alunos_consolidado['ESPDURACAO'] <= 1
                                                                  ,'FORMANDO'
                                                                  ,'FORMADO')
                                                        ,'VETERANO')) 

# Cria coluna de bolsa sugerida pelo Lucas
alunos_consolidado['bolsa_aluno'] = np.where(alunos_consolidado['sit_bolsa'] == '100%Bolsista'
                                              , '100%BOLSISTA'
                                              , np.where(alunos_consolidado['BOLSA_PRIORIDADE'] == 'FIES'
                                                         ,'FIES'
                                                         , np.where(alunos_consolidado['BOLSA_PRIORIDADE'].str.slice(0,3) == 'PEP'
                                                             ,'PEP'
                                                             ,'PAGANTE')))

# Salva base do alunos consolidado para não ter que ficar refazendo o cálculo
os.chdir('C:/Users/roberto.lemos/Desktop/Dimensionamento CRA/Dimensionamento Piloto/Piloto V2')
alunos_consolidado.to_excel('alunos_consolidado.xlsx')
# Ler aquivo para não ter que começar código de novo
alunos_consolidado = pd.read_excel('alunos_consolidado.xlsx')

################### Quarta parte - definir as unidades
################### com as informações consolidadas, agora vamos definir as unidades para o piloto

unidades_tipo = alunos_consolidado.groupby(['UNIDADE_MKT']).apply(lambda x: 
                                                            pd.Series(dict(qtd_alunos = (x['ALUCOD']).count()
                                                                          , TICKET_MEDIO = x['ticket_medio'].mean()
                                                                          , CR_UNIDADE = (x['qtd_chamadas'].sum()/x['ALUCOD'].count())
                                                                          , BOLSISTA = (x[x['bolsa_aluno'] == '100%BOLSISTA']['ALUCOD'].count())
                                                                          , FIES = (x[x['bolsa_aluno'] == 'FIES']['ALUCOD'].count())
                                                                          , PEP = (x[x['bolsa_aluno'] == 'PEP']['ALUCOD'].count())
                                                                          , PAGANTE = (x[x['bolsa_aluno'] == 'PAGANTE']['ALUCOD'].count())
                                                                          , PERC_BOLSISTA = (x[x['bolsa_aluno'] == '100%BOLSISTA']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_FIES = (x[x['bolsa_aluno'] == 'FIES']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_PEP = (x[x['bolsa_aluno'] == 'PEP']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_PAGANTE = (x[x['bolsa_aluno'] == 'PAGANTE']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , CALOURO = (x[x['fase_curso'] == 'CALOURO']['ALUCOD'].count())
                                                                          , VETERANO = (x[x['fase_curso'] == 'VETERANO']['ALUCOD'].count())
                                                                          , FORMANDO = (x[x['fase_curso'] == 'FORMANDO']['ALUCOD'].count())
                                                                          , FORMADO = (x[x['fase_curso'] == 'FORMADO']['ALUCOD'].count())
                                                                          , PERC_CALOURO = (x[x['fase_curso'] == 'CALOURO']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_VETERANO = (x[x['fase_curso'] == 'VETERANO']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_FORMANDO = (x[x['fase_curso'] == 'FORMANDO']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_FORMADO = (x[x['fase_curso'] == 'FORMADO']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_INA =  x['mens_adimplente'].sum()/x['mens_total'].sum()
                                                                          ))).reset_index()
    
    
# Salva base do unidades alvo e consolidada para não ter que ficar refazendo o cálculo
os.chdir('C:/Users/roberto.lemos/Desktop/Dimensionamento CRA/Dimensionamento Piloto/Piloto V2')
    
unidades_tipo.to_excel('unidades_tipo.xlsx')

################### Quinta parte - definir as carteiras por consultor
################### com as unidades definidas, vamos montar um algoritmo para criação de carteira;

# marca unidades escolhidas
unidades_alvo = unidades_tipo
unidades_alvo['FLAG_UNIDADE_ALVO'] = np.where(unidades_tipo['UNIDADE_MKT'] == '[Anhanguera] - JACAREI/SP' 
                                                   , 1 
                                                   , np.where (unidades_tipo['UNIDADE_MKT'] == '[Pitagoras] - BELO HORIZONTE/MG - BARREIRO' 
                                                               , 1
                                                               , 0))

unidades_alvo = unidades_alvo[unidades_alvo['FLAG_UNIDADE_ALVO'] == 1]

# marca alunos escolhidos
alunos_consolidado['FLAG_UNIDADE_ALVO'] = np.where(alunos_consolidado['UNIDADE_MKT'] == '[Anhanguera] - JACAREI/SP' 
                                                   , 1 
                                                   , np.where (alunos_consolidado['UNIDADE_MKT'] == '[Pitagoras] - BELO HORIZONTE/MG - BARREIRO' 
                                                               , 1
                                                               , 0))

# Filtra unidades alvo
alunos_alvo = alunos_consolidado[alunos_consolidado['FLAG_UNIDADE_ALVO'] == 1]





# Cria coluna aleatória para balizar o consultor
alunos_alvo['consultor'] = np.random.randint(1,4, size=len(alunos_alvo))

carteira = alunos_alvo.groupby(['UNIDADE_MKT','consultor']).apply(lambda x: 
                                                            pd.Series(dict(qtd_alunos = (x['ALUCOD']).count()
                                                                          , TICKET_MEDIO = x['ticket_medio'].mean()
                                                                          , CR_UNIDADE = (x['qtd_chamadas'].sum()/x['ALUCOD'].count())
                                                                          , BOLSISTA = (x[x['bolsa_aluno'] == '100%BOLSISTA']['ALUCOD'].count())
                                                                          , FIES = (x[x['bolsa_aluno'] == 'FIES']['ALUCOD'].count())
                                                                          , PEP = (x[x['bolsa_aluno'] == 'PEP']['ALUCOD'].count())
                                                                          , PAGANTE = (x[x['bolsa_aluno'] == 'PAGANTE']['ALUCOD'].count())
                                                                          , PERC_BOLSISTA = (x[x['bolsa_aluno'] == '100%BOLSISTA']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_FIES = (x[x['bolsa_aluno'] == 'FIES']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_PEP = (x[x['bolsa_aluno'] == 'PEP']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_PAGANTE = (x[x['bolsa_aluno'] == 'PAGANTE']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , CALOURO = (x[x['fase_curso'] == 'CALOURO']['ALUCOD'].count())
                                                                          , VETERANO = (x[x['fase_curso'] == 'VETERANO']['ALUCOD'].count())
                                                                          , FORMANDO = (x[x['fase_curso'] == 'FORMANDO']['ALUCOD'].count())
                                                                          , FORMADO = (x[x['fase_curso'] == 'FORMADO']['ALUCOD'].count())
                                                                          , PERC_CALOURO = (x[x['fase_curso'] == 'CALOURO']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_VETERANO = (x[x['fase_curso'] == 'VETERANO']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_FORMANDO = (x[x['fase_curso'] == 'FORMANDO']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_FORMADO = (x[x['fase_curso'] == 'FORMADO']['ALUCOD'].count())/(x['ALUCOD']).count()
                                                                          , PERC_INA =  x['mens_adimplente'].sum()/x['mens_total'].sum()
                                                                          ))).reset_index()


# Salva base de alunos alvo para não ter que ficar refazendo cálculo
os.chdir('C:/Users/roberto.lemos/Desktop/Dimensionamento CRA/Dimensionamento Piloto/Piloto V2')
alunos_alvo.to_excel('alunos_alvo.xlsx')
    
carteira.to_excel('carteira.xlsx')
    

################### Fim Script - Final mesmo, incluindo aleatorio



#Variaveis exploratorias
engajamento_ac.head(20)
temp = engajamento_ac.describe()
engajamento_ac_aluno.dtypes
convenio.dtypes
temp2 = engajamento_ac_aluno[engajamento_ac_aluno['ALUCOD'] == 40704]
temp3 = alunos_consolidado[alunos_consolidado['fase_curso'] == 'FORMADO']
len(convenio_aluno['ALUCOD'].unique())


temp3.to_excel('temp3.xlsx')

###Backup comando dos dados
#Gravar excel com os outputs
os.chdir('C:/Users/roberto.lemos/Desktop/Estudo Filtros PAD')
atendentes.to_excel('atendentes.xlsx')


#Fim leitura de dados
