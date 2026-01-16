import seg_rcmn_utils as sru
import sys
import pandas as pd
import mysql.connector
import ast
from openai import OpenAI

DB_CONFIG = sru.DB_CONFIG
user_req_query = '신규매장 오픈 기념 캠페인' #사용자 요청
client = OpenAI(api_key=sru.api_key)
system_text = """
너는 캠페인 검색 시스템의 쿼리 확장 전문가이다.
사용자의 검색 쿼리가 입력되면, 너는 그 쿼리를 기반으로 사용자의 잠재적인 의도와 문맥을 파악해야 한다.
입력된 쿼리에 포함된 핵심 키워드와 연관성이 높으며, 구체적인 캠페인 속성(기간, 대상, 혜택, 유형 등)을 포함하는 5개의 새로운 검색 쿼리만을 생성해야 한다.

- 생성 규칙:
    1. 원본 쿼리를 포함하여 총 5개의 쿼리를 생성한다.
    2. 쿼리 구분은 반드시 (!!!!)으로만 한다.
    3. 쿼리는 최대한 구체적이어야 하며, 텍스트 임베딩 검색에 적합한 자연어 문장 형태를 포함해야 한다.
    4. 어떠한 설명, 부연, 인사말도 없이 5개의 쿼리 텍스트만 출력한다.

예시 (입력: 블랙프라이데이 캠페인):
블랙프라이데이 캠페인
11월 말 대규모 할인 행사
가장 큰 할인율이 적용된 프로모션
시즌 오프 상품 재고 판매 행사
광고 형식으로 진행된 블랙프라이데이 이벤트
---
"""
response = client.chat.completions.create(
	model = 'gpt-4o-mini',
	message=[
		{'role':'system','content':system_text},
		{'role':'user','content':user_req_query}
		 ]
)
sentence = response.choices[0].message.content
sentence = sru.request_gpt(user_req_query)  #LLM -> 사용자 요청을 문맥확장쿼리 5개 !!!! 구분자로 요청
sentence_list=sentence.split('!!!!')  #확장쿼리 list로 분리
all_query_results=[]
db_conn = mysql.connector.connect(**DB_CONFIG)  #벡터db연결
cursor = db_conn.cursor(dictionary=True)
for i in sentence_list:   #확장쿼리 5번 반복
	sentence_vec=sru.request_embedding(i)   #확장쿼리 임베딩 벡터화
	query = f"""SELECT A.CAMP_ID,B.CAMP_NM, 1 -VEC_DISTANCE_COSINE((SELECT VEC_FromText('{sentence_vec}')), A.CAMP_VEC) AS SIMIL_SCORE FROM quadmax_sdz.camp_summary_vec A INNER JOIN quadmax_sdz.t_campaign B ON A.CAMP_ID = B.CAMP_ID ORDER BY 3 DESC LIMIT 10"""
	cursor.execute(query)    #벡터화되어있는 기존 캠페인정보와 유사도 비교
	camp_simil=cursor.fetchall()
	all_query_results.append(camp_simil)
if cursor is not None:
	cursor.close()
if db_conn is not None and db_conn.is_connected():
	db_conn.close()
final_scores = {}
for result_list in all_query_results:   #5개의 확장쿼리의 유사도 비교 결과
    for item in result_list:
        camp_id = item['CAMP_ID']
        camp_nm = item['CAMP_NM']
        score = item['SIMIL_SCORE']
        
        if camp_id not in final_scores:
            final_scores[camp_id] = [score, camp_nm]
        else:
            final_scores[camp_id][0] += score
sorted_items = sorted(    #유사도순으로 정렬
    final_scores.items(),  
    key=lambda item: item[1][0], 
    reverse=True
)
top_5_items_raw = sorted_items[:5]  #유사 캠페인 상위5개 추출
top_5_list_result = []
for camp_id, value in top_5_items_raw:
    score = round(value[0]/5*100,2)
    camp_nm = value[1]
    top_5_list_result.append([camp_id, camp_nm, score])
temp1=[]
for i in top_5_list_result:
    temp1.append(i[0])
cond_str="','".join(temp1)
query=f"""SELECT CAMP_ID
               , GROUP_CONCAT(CONCAT(PRMP_DATA_TYPE,' %^ ',PRMP_NM,' %^ ',PRMP_OP,' %^ ',PRMP_VAL) ORDER BY PRMP_NM SEPARATOR ' !@#$ ' ) AS TARGET_CONDI 
            FROM quadmax_sdz.T_CAMP_TARGETING
           WHERE CAMP_ID IN ('{cond_str}')
           GROUP BY CAMP_ID"""
conn = mysql.connector.connect(**DB_CONFIG)  #유사 캠페인 상위5개 사용된 조건 추출
camp_cond=pd.read_sql(query,conn)
conn.close()
camp_simil_df = pd.DataFrame(top_5_list_result)
camp_simil_df.columns=['CAMP_ID','CAMP_NM','CAMP_SIMIL']
merged_df = pd.merge(  #사용된 조건 조인
    camp_cond,
    camp_simil_df,    
    on='CAMP_ID',     
    how='inner'       
)
integer_interval_events=[]
date_interval_events=[]
condition_simil=[]
absolute_score_threshold=merged_df['CAMP_SIMIL'].mean()+(merged_df['CAMP_SIMIL'].std()*0.5)  #조건 추출 기준 설정 평균 + (표준편차*0.5)
for index, row in merged_df.iterrows():
    camp_id = row['CAMP_ID']
    target_condi = row['TARGET_CONDI']
    camp_nm = row['CAMP_NM']
    camp_simil = row['CAMP_SIMIL']
    for i in target_condi.split('!@#$'):  #타겟팅조건 구분
        parts = [p.strip() for p in i.split('%^')]   #타겟팅조건 구분
        condition_form, feature_name, operator, value_str = parts
        if condition_form == 'string':    #string 조건 : ['A','B','C']
            for j in ast.literal_eval(value_str):
                condition_simil.append([feature_name,j,camp_simil])
        elif condition_form == 'integer':   #integer 조건 : [start,end] 최소값0, 최대값 9999999999
            value_str=ast.literal_eval(value_str)
            if type(value_str) is not list:
                value_str=[value_str]
            start = 0 if operator == 'less' else value_str[0]
            if operator == 'less':
                end = value_str[0]
            elif operator == 'greater':
                end = 9999999999
            else:
                end = value_str[1]
            integer_interval_events.append({'name': feature_name, 'point': float(start), 'weight': camp_simil, 'type': 'start'})
            integer_interval_events.append({'name': feature_name, 'point': float(end), 'weight': -camp_simil, 'type': 'end'})
        elif condition_form == 'datePopup':   #date 조건 : [start,end] 최소값20200101, 최대값 20301231
            value_str=ast.literal_eval(value_str)
            start = '20200101' if operator == 'less' else value_str[0]
            if operator == 'less':
                end = value_str[0]
            elif operator == 'greater':
                end = '20301231'
            else:
                end = value_str[1]
            date_interval_events.append({'name': feature_name, 'point': sru.Epoch_date(start), 'weight': camp_simil, 'type': 'start'})
            date_interval_events.append({'name': feature_name, 'point': sru.Epoch_date(end), 'weight': -camp_simil, 'type': 'end'})
df=pd.DataFrame(condition_simil)
df.columns=['name','value','simil']
group_df=df.groupby(['name','value']).sum()   #같은조건은 유사도값 sum
summed_df = group_df.reset_index()
filtered_df = summed_df[summed_df['simil'] > absolute_score_threshold]  #string 조건 기준점보다 높은 유사도값sum 을 가진 조건만 추출
filtered_list=filtered_df.values.tolist()
grouped_events = defaultdict(list)
for event in integer_interval_events:
    grouped_events[event['name']].append(event)
unique_names_list = list(grouped_events.keys())  #integer 조건 list
all_integer_scores=[]
final_integer_intervals={}
for i in unique_names_list:   #integer 조건 기준점보다 높은 구간 추출
    continuous_interval_scores={}
    grouped_events[i].sort(key=lambda x: (float(x['point']), 1 if x['type'] == 'end' else 0))
    current_weight = 0.0
    current_point = None
    for j in grouped_events[i]:
        name = j['name']
        point = float(j['point'])
        if current_point is not None and point > current_point:
            interval = (current_point, point)
            if current_weight > 0:
                continuous_interval_scores[interval] = current_weight
        current_weight += j['weight']
        current_point = point
    all_integer_scores.extend(continuous_interval_scores.values())
    final_integer_intervals[name] = continuous_interval_scores

grouped_events = defaultdict(list)
for event in date_interval_events:
    grouped_events[event['name']].append(event)
unique_names_list = list(grouped_events.keys())   #date 조건 list
all_date_scores=[]
final_date_intervals={}
for i in unique_names_list:   #date 조건 기준점보다 높은 구간 추출
    interval =(0,0)
    continuous_interval_scores={}
    grouped_events[i].sort(key=lambda x: (float(x['point']), 1 if x['type'] == 'end' else 0))
    current_weight = 0.0
    current_point = None
    for j in grouped_events[i]:
        name = j['name']
        point = float(j['point'])
        if interval[1] == current_point:
            current_point = current_point+1
        if current_point is not None and point > current_point:
            interval = (current_point, point)
            if current_weight > 0:
                continuous_interval_scores[interval] = current_weight
        current_weight += j['weight']
        current_point = point
    all_date_scores.extend(continuous_interval_scores.values())
    final_date_intervals[name] = continuous_interval_scores
for name, intervals in final_integer_intervals.items():   #integer 추출된 조건 표현될 형식으로 변환  ex) [0,100] ->  >100,  [200,300] -> 200~300
    selected_intervals_int=[]
    for (start, end), score in intervals.items():
        if score >= absolute_score_threshold:
            if int(start)==0:
                selected_intervals_int.append((f"<{int(end)}",score))
            elif int(end)==9999999999:
                selected_intervals_int.append((f">{int(start)}",score))
            else:
                selected_intervals_int.append((f"{int(start)}~{int(end)}",score))
    if selected_intervals_int:
        for sii in selected_intervals_int:
            filtered_list.append([name, sii[0], sii[1]])   #integer 추출된 조건 삽입
for name, intervals in final_date_intervals.items():   #date 추출된 조건 표현될 형식으로 변환  ex) [20200101,20200131] ->  >20200131,  [20240101,20241231] -> 20240101~20241231
    selected_intervals_date=[]
    for (start, end), score in intervals.items():
        if score >= absolute_score_threshold:
            if sru.date_from_epoch(int(start)) == '20200101':
                selected_intervals_date.append((f"<{sru.date_from_epoch(int(end))}",score))
            elif sru.date_from_epoch(int(end)) == '20301231':
                selected_intervals_date.append((f">{sru.date_from_epoch(int(start))}",score))
            else :
                selected_intervals_date.append((f"{sru.date_from_epoch(int(start))}~{sru.date_from_epoch(int(end))}",score))
    if selected_intervals_date:
        for sid in selected_intervals_date:
            filtered_list.append([name, sid[0],sid[1]])  #date 추출된 조건 삽입
result_dict={}
for item in filtered_list:
    key = item[0]
    value = item[1]
    if key not in result_dict:
        result_dict[key] = []
    result_dict[key].append(value)

query2=f"""SELECT A.BSNS_QRY_ID
     , A.PRMP_KWD
     , A.PRMP_NM
     , A.PRMP_OP
     , B.QRY_META 
  FROM quadmax_sdz.t_camp_targeting A
       INNER JOIN quadmax_sdz.t_xlig_query_list B
    ON A.BSNS_QRY_ID = B.QRY_ID 
 WHERE CAMP_ID IN ({cond_str})
 GROUP BY A.BSNS_QRY_ID
     , A.PRMP_KWD
     , A.PRMP_NM
     , A.PRMP_OP
     , B.QRY_META """  #사용된 조건 키워드, 프롬프트, 기호, 메타쿼리 가져오는 쿼리
cursor_2 = conn.cursor()
cursor_2.execute(query2, tuple(temp1))
meta_query = pd.DataFrame(cursor_2.fetchall(), columns=[desc[0] for desc in cursor_2.description])
cursor_2.close()

union_list=[]
for k in meta_query['BSNS_QRY_ID'].unique():  #meta쿼리 가져오기
    qry_meta=meta_query[meta_query['BSNS_QRY_ID']==k]['QRY_META'].head(1).iloc[0]
    cond_list={}
    for index, row in meta_query.iterrows():   #추출된 조건을 meta쿼리에 알맞게 분류 
        if row['BSNS_QRY_ID']==k:
            if row['PRMP_NM'] in result_dict:
                cond_list[row['PRMP_KWD']]=[row['PRMP_OP'],result_dict[row['PRMP_NM']]]
    union_list.append([qry_meta,cond_list])
union_query_list=[]
for h in union_list:
    sql=h[0].replace('\n',' ')   #meta쿼리 줄바꿈제거
    cond=h[1]
    for i in range(0,10):
        sql=sql.replace('  ',' ')  #meta쿼리 불필요 띄어쓰기 제거
    sql=sql.replace('@@SELECT_STRING@@','cust_id')
    no_comments_sql = re.sub(r'\/\*.*?\*\/', '', sql, flags=re.DOTALL)  #meta쿼리 코멘트 제거
    sample_parameters = re.findall(r'(\[.*?\])', no_comments_sql)  #where절 자리 찾기
    no_comments_sql = no_comments_sql.replace((sample_parameters[0]),'{use_cond} '+sample_parameters[0])  #사용파라미터 치환 자리 확보
    removed_parameters = re.findall(r'\[(.*?)\]', no_comments_sql)   #미사용파라미터 제거
    cleaned_sql = re.sub(r'\[.*?\]', '', no_comments_sql)   #미사용파라미터 제거
    cleaned_sql = re.sub(r'\@\@.*?\@\@', '', cleaned_sql, flags=re.DOTALL)
    use_cond_list=[]
    for j in removed_parameters:  #사용파라미터 쿼리 조건 만들기
        j=j.lstrip()
        kwd_nm = j.split(' ')[-1]
        if kwd_nm in cond.keys() :
            value_list="','".join(cond[kwd_nm][1])
            value_str=f"('{value_list}')"
            if cond[kwd_nm][0] == 'in':
                use_cond_list.append(j.replace('::op::',cond[kwd_nm][0]).replace(kwd_nm,value_str))
            elif cond[kwd_nm[0]] == 'equal':
                use_cond_list.append(j.replace('::op::','=').replace(kwd_nm,value_list))
            else:
                temp_op_list=[]
                for m in cond[kwd_nm][1]:
                    j=j.lstrip('AND')
                    if len(m.split(' AND ')) == 2 :
                        temp_op_list.append(f"({j.replace('::op::','BETWEEN').replace(kwd_nm,m)})")
                    else:
                        temp_op_list.append(f"({j.replace('::op::',' ').replace(kwd_nm,m)})")
                use_cond_list.append(f" AND ({' OR '.join(temp_op_list)})")
    use_cond=' '.join(use_cond_list)
    union_query_list.append(f"{cleaned_sql}".format(use_cond=use_cond))  #사용파라미터 치환
from_str=' UNION '.join(union_query_list)
last_query = f'SELECT COUNT(DISTINCT cust_id) FROM ({from_str}) TOTAL'  #타겟팅된 고객수 카운트
cust_cnt=[]
mssql_conn = pymssql.connect(sru.mssql_DB_CONFIG)
mssql_cursor = mssql_conn.cursor()
mssql_cursor.execute(last_query)
for r in mssql_cursor:
    cust_cnt.append(r)
mssql_conn.close()
print(cust_cnt[0][0])  #타겟팅된 고객수출력

