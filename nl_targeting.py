import seg_rcmn_utils as sru
import sys
import mysql.connector
from openai import OpenAI

DB_CONFIG = sru.DB_CONFIG
systemp_prompt = """
당신은 데이터분석 및 마케팅전문가이다.

아래 **요청문장**에서 타겟팅이 가능한 키워드를 추출하라.

[출력 규칙]
1. 반드시 아래 출력형식만 사용하라.
2. 설명, 문장, 줄바꿈, 머리말을 절대 출력하지 마라.
3. 모든 띄어쓰기는 '_'로 변환하라.
4. 출력형식이 하나라도 어긋나면 잘못된 출력이다.
5. 키워드는 중요도 순으로 나열하라.

[출력형식]
[{속성:키워드,값:키워드값},{속성:키워드,값:키워드값}]

[예시]
입력: 30대 여성 피부 관리
출력:
[{속성:연령대,값:30대},{속성:성별,값:여성},{속성:관심사,값:피부관리}]
"""
client = OpenAI(
api_key=sru.api_key
)
input_query='강남에서 구매금액이 100만원 넘는 VIP 고객들' #사용자 요청
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": systemp_prompt},
        {"role": "user", "content": i['값']}
    ],
    temperature=0, 
    max_tokens=300 
)
request_conv = response.choices[0].message.content

db_conn = mysql.connector.connect(**DB_CONFIG)

def nl_targeting(request_conv, standard):
    grouped_results = {}
    for i in request_conv:
        query_sentence=f"""{i['속성']}이(가) {i['값']} 인것 찾아줘"""
        query_vec=sru.request_embedding(query_sentence)
        query = f"""SELECT COND_NM,CODE_NM,COND_TYPE, 1 -VEC_DISTANCE_COSINE((SELECT VEC_FromText('{query_vec}')), COND_VEC) AS SIMIL_SCORE FROM quadmax_sdz.condition_vec"""
        cursor = db_conn.cursor(dictionary=True)
        cursor.execute(query)
        camp_simil=cursor.fetchall()
        for item in camp_simil:
            if item['SIMIL_SCORE'] >= standard:
                cond = item['COND_NM']
                if item['CODE_NM'] == '':
                    systemp_prompt = ''
                    if item['COND_TYPE'] == 'integer':
                        systemp_prompt = '아래 값 숫자를 operator(>,>=,<,<=,=,BETWEEN)로 표시해줘. 숫자에는 천단위구분자 ,을 넣지말고. 딱 값 하나만 출력해. 만약 값이 날짜라면 아무것도 출력하지마'
                    elif item['COND_TYPE'] == 'datePopup':
                        systemp_prompt = '아래 값 날짜를 operator(>,>=,<,<=,=,BETWEEN)로 표시해줘. 표시형식은 YYYYMMDD. 딱 값 하나만 출력해. 만약 값이 날짜가 아니라면 아무것도 출력하지마'
                    else:
                        systemp_prompt = ''
                    response = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": systemp_prompt},
                            {"role": "user", "content": i['값']}
                        ],
                        temperature=0, 
                        max_tokens=300 
                    )
                    code = response.choices[0].message.content
                else:
                    code = item['CODE_NM']
                if cond not in grouped_results:
                    grouped_results[cond] = []
                if code not in grouped_results[cond]:
                    grouped_results[cond].append(code)
    return grouped_results

first_standard = 0.5
second_standard = 0.4
if step == 'first':
    grouped_results = nl_targeting(request_conv,first_standard)
    empty_delete = {k: [x for x in v if x.strip()] for k, v in grouped_results.items()}
    result = {k: v for k, v in empty_delete.items() if v}
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": 'Condition에서 Request에 맞는조건들만 남겨줘. Condition 형태는 그대로 유지하고 Condition만 출력해. Condition말고는 다른말은 절대 하지마'},
            {"role": "user", "content": f"Request : {str(request_conv)}. Conditiion : {str(result)}"}
        ],
        temperature=0, 
        max_tokens=300 
    )
    refine_result = response.choices[0].message.content
elif step == 'second':
    grouped_results = nl_targeting(request_conv,first_standard)
elif step == 'third':
    grouped_results = nl_targeting(request_conv,second_standard)

if cursor is not None:
  cursor.close()
if db_conn is not None and db_conn.is_connected():
  db_conn.close()
empty_delete = {k: [x for x in v if x.strip()] for k, v in grouped_results.items()}
result = {k: v for k, v in empty_delete.items() if v}

print(result)
