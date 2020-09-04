
def conecta_bd(sistema):
    import psycopg2
    import cx_Oracle
    import pyodbc
    import pysnow

    #Fazer as conexões com o Postgress


    if sistema =='X':
    # Connect to an existing database
                     database_f = 'data_base_nome'
                     user_f = 'seu user'
                     password_f = 'sua senha'
                     host_f = 'seu host'
                     port_f = '5432'
                     
                     
     #Connect ORACLE SERVER
     
    elif sistema =='Y':
        conexao = 'user/senha@host:1521/bdprodexa'
        
    elif sistema == 'Z':
    # Connect to servicenow API
        user_f = 'user'
        password_f = 'senha'
        host_f = 'instancia'
        
    elif sistema == 'W':
    #Connect Microsoft SQL Server
        conexao = ('DRIVER='+'{SQL Server}'+';PORT=1433;SERVER='+'krt-ksk-sqlmi-br-prd.8074949fa3bc.database.windows.net'+';PORT=1443;DATABASE='+'COLACAODIGITAL-PRD'+';UID='+'webcolacaodigitalapi'+';PWD='+'#CkrY29#TghJ@')
            

    else:
        print('input de sistema errado')
        
    if sistema in ['X']:
        conn = psycopg2.connect(database=database_f, user=user_f, password=password_f,host=host_f, port=port_f)
    if sistema in ['Z']:
        
        conn = pysnow.Client(user = user_f , password = password_f, instance = host_f)
    if sistema in ['W']:
        
        conn = pyodbc.connect(conexao)
    else: 
        conn = cx_Oracle.connect(conexao)   
    return (conn)
