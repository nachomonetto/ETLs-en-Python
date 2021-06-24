#Importo cx_Oracle para realizar la conexión a la BD y Pandas
import cx_Oracle
import pandas as pd

#Datos de la conexión
ip = 'localhost'
port = 1521
SID = 'XE'
dsn_tns = cx_Oracle.makedsn(ip, port, SID)

connection = cx_Oracle.connect('SYSTEM', 'password', dsn_tns)

#Query origen
query_ora_PICKLIST_DATA = """SELECT
                             Trim(PD.OPTIONID) as OPTIONID,
                             Trim(Initcap(PD.ES_ES)) as ES_ES,
                             PD.EXTERNAL_CODE,
                             PD.PARENTOPTIONID
                             FROM PICKLIST_DATA PD
                             WHERE UPPER(PICKLISTID) = 'EC_ACTIVITY~1' and STATUS = 'ACTIVE'"""
           
df_ora_PICKLIST_DATA = pd.read_sql(query_ora_PICKLIST_DATA, con=connection)
df_ora_PICKLIST_DATA.head()

df_sistema_origen_cd_added=df_ora_PICKLIST_DATA.assign(SISTEMA_ORIGEN_CD='ONHR')


#Query para levantar D_SISTEMA_ORIGEN
query_ora_D_SISTEMA_ORIGEN = """SELECT SISTEMA_ORIGEN_CD, SISTEMA_ORIGEN_ID
                                FROM D_SISTEMA_ORIGEN"""         
df_ora_D_SISTEMA_ORIGEN = pd.read_sql(query_ora_D_SISTEMA_ORIGEN, con=connection)

#Hago el join entre el flujo principal y la D_SISTEMA_ORIGEN
left_sistema_origen=df_sistema_origen_cd_added.set_index(['SISTEMA_ORIGEN_CD'])
right_sistema_origen=df_ora_D_SISTEMA_ORIGEN.set_index(['SISTEMA_ORIGEN_CD'])

join_sistema_origen=left_sistema_origen.join(right_sistema_origen).reset_index().drop(columns=['SISTEMA_ORIGEN_CD'])

campos_renombrados=join_sistema_origen.rename(columns={'OPTIONID':'ACTIVIDAD_CD','ES_ES':'ACTIVIDAD_DE'})

campos_renombrados.head()

#Query para levantar D_PROCESO
query_ora_D_PROCESO = """SELECT CODIGO_EXTERNO_CD AS PARENTOPTIONID, PROCESO_ID
                                FROM D_PROCESO"""         
df_ora_D_PROCESO = pd.read_sql(query_ora_D_PROCESO, con=connection)

#Hago el join entre el flujo principal y la D_PROCESO
left_PROCESO=campos_renombrados.set_index(['PARENTOPTIONID'])
right_PROCESO=df_ora_D_PROCESO.set_index(['PARENTOPTIONID'])

join_PROCESO=left_PROCESO.join(right_PROCESO).reset_index().drop(columns=['PARENTOPTIONID'])
join_PROCESO.head()

#Query para traerme el máximo subrogado en D_ACTIVIDAD
query_ora_MAX_SKG_D_ACTIVIDAD = """SELECT 
                                            Coalesce(MAX(ACTIVIDAD_ID),0) AS MAX_SKG
                                            FROM 
                                            D_ACTIVIDAD
                                            WHERE
                                            ACTIVIDAD_ID >0"""         
df_ora_MAX_SKG_D_ACTIVIDAD = pd.read_sql(query_ora_MAX_SKG_D_ACTIVIDAD, con=connection)
df_ora_MAX_SKG_D_ACTIVIDAD.head()

max_skg=df_ora_MAX_SKG_D_ACTIVIDAD.iloc[0,0]
max_skg

#Query para traerme lo que existe en D_ACTIVIDAD
query_ora_D_ACTIVIDAD_Ref= """SELECT 
                                      ACTIVIDAD_ID as ACTIVIDAD_ID_Ref,
                                      ACTIVIDAD_CD,
                                      ACTIVIDAD_DE as ACTIVIDAD_DE_Ref,
                                      CODIGO_EXTERNO_CD as CODIGO_EXTERNO_CD_Ref,
                                      PROCESO_ID as PROCESO_ID_Ref,
                                      1 AS FLAG_EXISTE
                                      FROM D_ACTIVIDAD"""         
df_ora_D_ACTIVIDAD_Ref = pd.read_sql(query_ora_D_ACTIVIDAD_Ref, con=connection)
df_ora_D_ACTIVIDAD_Ref.head()

#Hago el left join para la detección de novedades y cambios
left_D_ACTIVIDAD_Ref=join_PROCESO.set_index(['ACTIVIDAD_CD'])
right_D_ACTIVIDAD_Ref=df_ora_D_ACTIVIDAD_Ref.set_index(['ACTIVIDAD_CD'])
df_output_join_ref=left_D_ACTIVIDAD_Ref.join(right_D_ACTIVIDAD_Ref).reset_index()
df_output_join_ref.head()

def procesoid(row):
    if pd.isna(row.PARENTOPTIONID):      
        row.PROCESO_ID = -2
    else: row.PROCESO_ID=row.PROCESO_ID       
    return row

df_proceso_id=df_output_join_ref.apply(procesoid, axis='columns')
df_proceso_id.head()

#Escenario insert
df_insert_flag_existe=df_proceso_id.loc[df_proceso_id.FLAG_EXISTE.isnull()]
df_insert_flag_existe['ACTIVIDAD_ID']=range(1,len(df_insert_flag_existe)+1,1)
df_insert_flag_existe['ACTIVIDAD_ID']=df_insert_flag_existe.ACTIVIDAD_ID.map(lambda s: s + max_skg)
df_insert_flag_existe['LOTE_CARGA']=1
df_insert_flag_existe['LOTE_ACTUALIZACION']=1
df_insert=df_insert_flag_existe[['ACTIVIDAD_ID','ACTIVIDAD_CD','ACTIVIDAD_DE','CODIGO_EXTERNO_CD','PROCESO_ID','SISTEMA_ORIGEN_ID','LOTE_CARGA','LOTE_ACTUALIZACION']]
df_insert.head()
#Escenario update
df_update_flag_existe=df_proceso_id.loc[(df_proceso_id.FLAG_EXISTE.notnull()) & ((df_proceso_id.ACTIVIDAD_DE!=df_proceso_id.ACTIVIDAD_DE_REF) | (df_proceso_id.CODIGO_EXTERNO_CD!=df_proceso_id.CODIGO_EXTERNO_CD_REF) | (df_proceso_id.PROCESO_ID!=df_proceso_id.PROCESO_ID_REF))]
df_update_flag_existe=df_update_flag_existe.rename(columns={'ACTIVIDAD_ID_REF':'ACTIVIDAD_ID'})
df_update_flag_existe['LOTE_ACTUALIZACION']=2
df_update=df_update_flag_existe[['ACTIVIDAD_DE','CODIGO_EXTERNO_CD','PROCESO_ID','LOTE_ACTUALIZACION','ACTIVIDAD_ID']]
df_update.head()

#Me conecto a la BD para hacer insert
if len(df_insert)>0:    
    cursor_insert = connection.cursor()

    sql_insert='insert into D_ACTIVIDAD (ACTIVIDAD_ID,ACTIVIDAD_CD,ACTIVIDAD_DE,SISTEMA_ORIGEN_ID,LOTE_CARGA,LOTE_ACTUALIZACION) values(:1,:2,:3,:4,:5,:6)'
    df_list_insert = df_insert.values.tolist()
    n = 0
    for i in df_insert.iterrows():
        cursor_insert.execute(sql_insert,df_list_insert[n])
        n += 1

    connection.commit()
print('Se insertaron: '+str(len(df_insert))+' registros.')
df_insert.head()

#Me conecto a la BD para hacer insert
if len(df_insert)>0:    
    cursor_insert = connection.cursor()

    sql_insert='insert into D_ACTIVIDAD (ACTIVIDAD_ID,ACTIVIDAD_CD,ACTIVIDAD_DE,CODIGO_EXTERNO_CD,PROCESO_ID,SISTEMA_ORIGEN_ID,LOTE_CARGA,LOTE_ACTUALIZACION) values(:1,:2,:3,:4,:5,:6,:7,:8)'
    df_list_insert = df_insert.values.tolist()
    n = 0
    for i in df_insert.iterrows():
        cursor_insert.execute(sql_insert,df_list_insert[n])
        n += 1

    connection.commit()
print('Se insertaron: '+str(len(df_insert))+' registros.')
df_insert.head()

#Me conecto a la BD para hacer update
if len(df_update)>0:    
    cursor_update = connection.cursor()

    sql_update='update D_ACTIVIDAD set ACTIVIDAD_DE=:1,CODIGO_EXTERNO_CD=:2,PROCESO_ID=:3,LOTE_ACTUALIZACION=:4 where ACTIVIDAD_ID=:5'
    df_list_update = df_update.values.tolist()
    n = 0
    for i in df_update.iterrows():
        cursor_update.execute(sql_update,df_list_update[n])
        n += 1

    connection.commit()
print('Se actualizaron: '+str(len(df_update))+' registros.')
df_update.head()