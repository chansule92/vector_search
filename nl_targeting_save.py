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
from openai import OpenAI
client = OpenAI(api_key=sru.api_key)
system_text = """
[프롬프트] 너는 데이터 검색 시스템의 검색어 생성기야. 아래 제공된 [속성]과 [값]을 가진 데이터를 찾고 싶을 때, 사용자들이 채팅창에 입력할 법한 자연스러운 검색 문장 3개를 만들어줘.

조건:

반드시 해당 [값]이 포함된 데이터를 찾는 상황이어야 함 (부정형 문장 금지).

조사가 틀리지 않도록 자연스러운 한국어 구어체로 작성할 것.

"찾아줘", "보여줘" 같은 명령형을 섞어 다양하게 만들 것.

[값]이 없다면 [속성]에 따라 적절한 기간이나 수치값을 넣을 것.

입력: {속성: 가입매장 ,값: 강남점}

출력 형식: 문장1!!!!문장2!!!!문장3
"""
for res in result:
    sample_json = {'속성': res[0], '조건값': res[2]}
	response = client.chat.completions.create(
		model = 'gpt-4o-mini',
		message=[
			{'role':'system','content':system_text},
			{'role':'user','content':str(sample_json)}
			 ]
	)
	gpt_result = response.choices[0].message.content
    
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
