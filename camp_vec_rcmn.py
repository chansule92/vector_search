import seg_rcmn_utils as sru
import sys
import pandas as pd
import mysql.connector

DB_CONFIG = sru.DB_CONFIG
user_req_query = '신규매장 오픈 기념 캠페인'
sentence = sru.request_gpt(user_req_query)
sentence_list=sentence.split('!!!!')
all_query_results=[]
db_conn = mysql.connector.connect(**DB_CONFIG)
cursor = db_conn.cursor(dictionary=True)
for i in sentence_list:
	sentence_vec=sru.request_embedding(i)
	query = f"""SELECT A.CAMP_ID,B.CAMP_NM, 1 -VEC_DISTANCE_COSINE((SELECT VEC_FromText('{sentence_vec}')), A.CAMP_VEC) AS SIMIL_SCORE FROM quadmax_sdz.camp_summary_vec A INNER JOIN quadmax_sdz.t_campaign B ON A.CAMP_ID = B.CAMP_ID ORDER BY 3 DESC LIMIT 10"""
	cursor.execute(query)
	camp_simil=cursor.fetchall()
	all_query_results.append(camp_simil)
if cursor is not None:
	cursor.close()
if db_conn is not None and db_conn.is_connected():
	db_conn.close()
final_scores = {}
for result_list in all_query_results:
    for item in result_list:
        camp_id = item['CAMP_ID']
        camp_nm = item['CAMP_NM']
        score = item['SIMIL_SCORE']
        
        if camp_id not in final_scores:
            final_scores[camp_id] = [score, camp_nm]
        else:
            final_scores[camp_id][0] += score
sorted_items = sorted(
    final_scores.items(),  
    key=lambda item: item[1][0], 
    reverse=True
)
top_5_items_raw = sorted_items[:5]
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
conn = mysql.connector.connect(**DB_CONFIG)
camp_cond=pd.read_sql(query,conn)
conn.close()
camp_simil_df = pd.DataFrame(top_5_list_result)
camp_simil_df.columns=['CAMP_ID','CAMP_NM','CAMP_SIMIL']
merged_df = pd.merge( 
    camp_cond,
    camp_simil_df,    
    on='CAMP_ID',     
    how='inner'       
)
integer_interval_events=[]
date_interval_events=[]
condition_simil=[]
absolute_score_threshold=merged_df['CAMP_SIMIL'].mean()+merged_df['CAMP_SIMIL'].std()
for index, row in merged_df.iterrows():
    camp_id = row['CAMP_ID']
    target_condi = row['TARGET_CONDI']
    camp_nm = row['CAMP_NM']
    camp_simil = row['CAMP_SIMIL']
    for i in target_condi.split('!@#$'):
        parts = [p.strip() for p in i.split('%^')]
        condition_form, feature_name, operator, value_str = parts
        if condition_form == 'string':
            for j in ast.literal_eval(value_str):
                condition_simil.append([feature_name,j,camp_simil])
        elif condition_form == 'integer':
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
        elif condition_form == 'datePopup':
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
group_df=df.groupby(['name','value']).sum()
summed_df = group_df.reset_index()
filtered_df = summed_df[summed_df['simil'] > absolute_score_threshold]
filtered_list=filtered_df.values.tolist()
grouped_events = defaultdict(list)
for event in integer_interval_events:
    grouped_events[event['name']].append(event)
unique_names_list = list(grouped_events.keys())
all_integer_scores=[]
final_integer_intervals={}
for i in unique_names_list:
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
unique_names_list = list(grouped_events.keys())
all_date_scores=[]
final_date_intervals={}
for i in unique_names_list:
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
for name, intervals in final_integer_intervals.items():
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
            filtered_list.append([name, sii[0], sii[1]])
for name, intervals in final_date_intervals.items():
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
            filtered_list.append([name, sid[0],sid[1]])
result_dict={}
for item in filtered_list:
    key = item[0]
    value = item[1]
    if key not in result_dict:
        result_dict[key] = []
    result_dict[key].append(value)