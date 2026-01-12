import seg_rcmn_utils as sru
import mysql.connector
import numpy as np
import requests
DB_CONFIG = sru.DB_CONFIG
query = """
SELECT A.CAMP_ID AS 캠페인ID
     , A.CAMP_NM AS 캠페인명
     , C.LOOKUP_NM AS 캠페인유형
     , IFNULL(D.OFFER_NM,'없음') AS 오퍼명
     , IFNULL(E.CNTN_NM,'없음') AS 메시지제목
     , IFNULL(E.MSG_TEXT,'없음') AS 발송내용
  FROM quadmax_sdz.t_campaign A
       LEFT OUTER JOIN quadmax_sdz.t_lookup_value B
    ON A.CAMP_SUCCESS_TYPE_CD = B.LOOKUP_CODE
   AND B.LOOKUP_TYPE_ID = 'CAMP_SUCCESS_TYPE_CD'
       LEFT OUTER JOIN quadmax_sdz.t_lookup_value C
    ON A.CAMP_TYPE_CD = C.LOOKUP_CODE
   AND C.LOOKUP_TYPE_ID = 'CAMP_TYPE_CD'
       LEFT OUTER JOIN
       ( SELECT CAMP_ID
              , group_concat(B.offer_nm) AS OFFER_NM
           FROM quadmax_sdz.t_camp_cell_offer A
                INNER JOIN quadmax_sdz.t_offer B
             ON A.OFFER_ID = B.OFFER_ID 
          GROUP BY CAMP_ID 
       ) D
    ON A.CAMP_ID = D.CAMP_ID
       LEFT OUTER JOIN 
       ( SELECT CAMP_ID
              , CNTN_NM
              , MSG_TEXT
           FROM quadmax_sdz.t_camp_cell_content 
          GROUP BY CAMP_ID
              , CNTN_NM
              , MSG_TEXT
       ) E
    ON A.CAMP_ID = E.CAMP_ID
 WHERE A.CAMP_NM LIKE '[AI]%'
"""

db_conn = mysql.connector.connect(**DB_CONFIG)
cursor = db_conn.cursor(dictionary=True)
cursor.execute(query)
campaign_rows = cursor.fetchall()


result=[]
for i in campaign_rows:
    sentence_list=[]
    seq=0
    for j in i.keys():
        if seq!=0:
            sentence_list.append(f'{j}은 {i[j]} 입니다.')
        seq=seq+1
    result.append([i['캠페인ID'],sru.request_embedding(' '.join(sentence_list))])


for i in result:
    camp_id = i[0]
    camp_vec = i[1]
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        insert_query = f"""
        INSERT INTO quadmax_sdz.camp_summary_vec
        (CAMP_ID,CAMP_VEC)
        VALUES ('{camp_id}',VEC_FromText('{camp_vec}'));
        """
        cursor.execute(insert_query)
        conn.commit()
    except mysql.connector.Error as err:
        # ... (오류 처리 유지)
        print(f"❌ MariaDB 오류 발생: {err}")
    finally:
        # ... (연결 종료 유지)
        if conn and conn.is_connected():
            cursor.close()
            conn.close()