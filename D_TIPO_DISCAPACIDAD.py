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
query_ora_PICKLIST_DATA = """SELECT TRIM(OPTIONID) AS OPTIONID, TRIM(ES_ES) AS ES_ES, TRIM(EXTERNAL_CODE) AS EXTERNALCODE
                            FROM PICKLIST_DATA P
                            WHERE P.PICKLISTID = 'TipoDiscapacidad_ARG'
                            ORDER BY OPTIONID"""
           
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

join_sistema_origen=left_sistema_origen.join(right_sistema_origen).reset_index().drop(columns=['SISTEMA_ORIGEN_CD','EXTERNALCODE'])

campos_renombrados=join_sistema_origen.rename(columns={'OPTIONID':'TIPO_DISCAPACIDAD_CD','ES_ES':'TIPO_DISCAPACIDAD_DE'})

campos_renombrados.head()

#Query para traerme el máximo subrogado en D_TIPO_DISCAPACIDAD
query_ora_MAX_SKG_D_TIPO_DISCAPACIDAD = """SELECT 
                                            Coalesce(MAX(TIPO_DISCAPACIDAD_ID),0) AS MAX_SKG
                                            FROM 
                                            D_TIPO_DISCAPACIDAD
                                            WHERE
                                            TIPO_DISCAPACIDAD_ID >0"""         
df_ora_MAX_SKG_D_TIPO_DISCAPACIDAD = pd.read_sql(query_ora_MAX_SKG_D_TIPO_DISCAPACIDAD, con=connection)
df_ora_MAX_SKG_D_TIPO_DISCAPACIDAD.head()

max_skg=df_ora_MAX_SKG_D_TIPO_DISCAPACIDAD.iloc[0,0]

#Query para traerme lo que existe en D_TIPO_DISCAPACIDAD
query_ora_D_TIPO_DISCAPACIDAD_Ref= """SELECT 
                                      TIPO_DISCAPACIDAD_ID as TIPO_DISCAPACIDAD_ID_Ref,
                                      TIPO_DISCAPACIDAD_CD,
                                      TIPO_DISCAPACIDAD_DE as TIPO_DISCAPACIDAD_DE_Ref,
                                      1 AS FLAG_EXISTE
                                      FROM D_TIPO_DISCAPACIDAD"""         
df_ora_D_TIPO_DISCAPACIDAD_Ref = pd.read_sql(query_ora_D_TIPO_DISCAPACIDAD_Ref, con=connection)
df_ora_D_TIPO_DISCAPACIDAD_Ref.head()

#Hago el left join para la detección de novedades y cambios
left_D_TIPO_DISCAPACIDAD_Ref=campos_renombrados.set_index(['TIPO_DISCAPACIDAD_CD'])
right_D_TIPO_DISCAPACIDAD_Ref=df_ora_D_TIPO_DISCAPACIDAD_Ref.set_index(['TIPO_DISCAPACIDAD_CD'])
df_output_join_ref=left_D_TIPO_DISCAPACIDAD_Ref.join(right_D_TIPO_DISCAPACIDAD_Ref).reset_index()
df_output_join_ref.head()

#Escenario insert
df_insert_flag_existe=df_output_join_ref.loc[df_output_join_ref.FLAG_EXISTE.isnull()]
df_insert_flag_existe['TIPO_DISCAPACIDAD_ID']=range(1,len(df_insert_flag_existe)+1,1)
df_insert_flag_existe['TIPO_DISCAPACIDAD_ID']=df_insert_flag_existe.TIPO_DISCAPACIDAD_ID.map(lambda s: s + max_skg)
df_insert_flag_existe['LOTE_CARGA']=1
df_insert_flag_existe['LOTE_ACTUALIZACION']=1
df_insert=df_insert_flag_existe[['TIPO_DISCAPACIDAD_ID','TIPO_DISCAPACIDAD_CD','TIPO_DISCAPACIDAD_DE','SISTEMA_ORIGEN_ID','LOTE_CARGA','LOTE_ACTUALIZACION']]
#Escenario update
df_update_flag_existe=df_output_join_ref.loc[(df_output_join_ref.FLAG_EXISTE.notnull()) & (df_output_join_ref.TIPO_DISCAPACIDAD_DE!=df_output_join_ref.TIPO_DISCAPACIDAD_DE_REF)]
df_update_flag_existe=df_update_flag_existe.rename(columns={'TIPO_DISCAPACIDAD_ID_REF':'TIPO_DISCAPACIDAD_ID'})
df_update_flag_existe['LOTE_ACTUALIZACION']=2
df_update=df_update_flag_existe[['TIPO_DISCAPACIDAD_DE','LOTE_ACTUALIZACION','TIPO_DISCAPACIDAD_ID']]

#Me conecto a la BD para hacer insert
if len(df_insert)>0:    
    cursor_insert = connection.cursor()

    sql_insert='insert into D_TIPO_DISCAPACIDAD (TIPO_DISCAPACIDAD_ID,TIPO_DISCAPACIDAD_CD,TIPO_DISCAPACIDAD_DE,SISTEMA_ORIGEN_ID,LOTE_CARGA,LOTE_ACTUALIZACION) values(:1,:2,:3,:4,:5,:6)'
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

    sql_update='update D_TIPO_DISCAPACIDAD set TIPO_DISCAPACIDAD_DE=:1, LOTE_ACTUALIZACION=:2 where TIPO_DISCAPACIDAD_ID=:3'
    df_list_update = df_update.values.tolist()
    n = 0
    for i in df_update.iterrows():
        cursor_update.execute(sql_update,df_list_update[n])
        n += 1

    connection.commit()
print('Se actualizaron: '+str(len(df_update))+' registros.')
df_update.head()