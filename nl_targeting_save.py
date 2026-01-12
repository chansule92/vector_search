import os
import json
import mysql.connector
import pymssql
from openai import OpenAI
import seg_rcmn_utils as sru

DB_CONFIG = sru.DB_CONFIG
MSSQL_CONFIG = sru.mssql_DB_CONFIG

query = """SELECT A.QRY_META
     , B.PRMP_STRING
     , B.PRMP_JSON_INFO
     , CASE WHEN C.ML_DS_DIV_CD = 'DS' THEN C.DBMS_ID ELSE D.ML_DBMS_ID END AS DBMS_ID
     , CASE WHEN C.ML_DS_DIV_CD = 'DS' THEN C.DS_SQL ELSE D.ML_QRY END AS SQL_QRY
  FROM quadmax_sdz.t_xlig_query_list A
       INNER JOIN quadmax_sdz.t_xlig_query_prompt B
    ON A.QRY_ID = B.QRY_ID 
       LEFT OUTER JOIN quadmax_sdz.t_xlig_dimension_list C
    ON B.PRMP_KWD = C.PRMP_KWD
       LEFT OUTER JOIN 
       ( SELECT D0.ML_ID
              , D0.ML_DBMS_ID
              , D0.ML_QRY
           FROM quadmax_sdz.t_xlig_hierarchy_list D0
                INNER JOIN 
                ( SELECT ML_ID
                       , MAX(ML_SEQ) AS ML_SEQ
                    FROM quadmax_sdz.t_xlig_hierarchy_list 
                   GROUP BY ML_ID
                ) D1
             ON D0.ML_ID = D1.ML_ID
            AND D0.ML_SEQ = D1.ML_SEQ
       ) D
    ON C.DBMS_ID = D.ML_ID
 WHERE B.PRMP_JSON_INFO IS NOT NULL"""
db_conn = mysql.connector.connect(**DB_CONFIG)
cursor = db_conn.cursor(dictionary=True)
cursor.execute(query)
data = cursor.fetchall()
operator_list=['=','BETWEEN','IN','::op::']
result=[]

for i in data:
    table_list = sru.table_search(i['QRY_META'])
    json_info = json.loads(i['PRMP_JSON_INFO'])
    condition_name = json_info.get('label', {}).get('kr')
    type_name = json_info.get('input')
    if json_info.get('field'):
        column_name_base = json_info.get('field')
    else:
        column_name_base=i['PRMP_STRING']
        temp_value1=column_name_base.split(' ')
        for j in range(0,len(temp_value1)):
            if temp_value1[j] in operator_list:
                column_name_base = ' '.join(temp_value1[1:j])
    temp_table=[]
    for k in sru.column_search(column_name_base):
        temp_table.append(table_list[k[0]]+' '+k[0])
    temp_table2 = ','.join(temp_table)
    condition_value = json_info.get('values')
    if type_name == 'datePopup':
        condition_value=None
        result.append([condition_name, '', '', type_name, column_name_base, temp_table2 ])
    elif type_name == 'text':
        type_name = 'integer'
        condition_value=None
        result.append([condition_name, '', '', type_name, column_name_base, temp_table2 ])
    else :
        if len(condition_value)!=0 and all(condition_value):
            pass
        elif i['SQL_QRY']:
            if not (condition_value and all(condition_value)) and i['SQL_QRY']:
                condition_value = []
                refined_sql = sru.refine_query(i['SQL_QRY'])
                
                if i['DBMS_ID'] == 'QUADMAX_SDZ':
                    with db_conn.cursor(dictionary=True) as sub_cursor:
                        sub_cursor.execute(refined_sql)
                        for r in sub_cursor.fetchall():
                            vals = list(r.values())
                            condition_value.append({vals[0]: vals[1]})
                
                elif i['DBMS_ID'] == 'CRMDW':
                    with pymssql.connect(**MSSQL_CONFIG) as mssql_conn:
                        with mssql_conn.cursor(as_dict=True) as mssql_cursor:
                            mssql_cursor.execute(refined_sql)
                            for r in mssql_cursor:
                                vals = list(r.values())
                                condition_value.append({vals[0]: vals[1]})

            if condition_value:
                for k in condition_value:
                    items = list(k.items())
                    if items:
                        code, code_nm = items[0]
                        result.append([condition_name, code, code_nm, 'string', column_name_base, temp_table2])


final_rows = []
for res in result:
    sample_json = {'속성': res[0], '조건값': res[2]}
    gpt_result = sru.request_gpt(str(sample_json))
    
    for seq, desc in enumerate(gpt_result.split('!!!!'), 1):
        embedding = sru.request_embedding(desc)
        final_rows.append((
            f"{res[0]}_{res[1]}_{res[2]}_{seq}", res[0], res[1], res[2], 
            res[3], res[4], res[5], desc, str(embedding)
        ))

if final_rows:
    insert_query = """
    INSERT INTO quadmax_sdz.condition_vec
    (COND_ID, COND_NM, CODE, CODE_NM, COND_TYPE, COLUMN_NM, TABLE_NM, COND_DESC, COND_VEC)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, VEC_FromText(%s))
    """
	with db_conn.cursor() as save_cursor:
        save_cursor.executemany(insert_query, final_rows)
        db_conn.commit()

if db_conn and db_conn.is_connected():
	db_conn.close()