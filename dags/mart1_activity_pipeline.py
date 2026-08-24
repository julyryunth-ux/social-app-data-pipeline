from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sqlalchemy import create_engine
from itertools import product

# =============================================
# 기본 설정
# =============================================
DEFAULT_ARGS = {
    'owner': 'jungryun',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

ANALYSIS_START  = pd.Timestamp('2023-05-27')
ANALYSIS_END    = pd.Timestamp('2024-05-08')
MAX_PERIOD_DAYS = (ANALYSIS_END - ANALYSIS_START).days  # 348일
SUPER_USER_THRESHOLD = 507                              # 투표수 상위 1% 기준

# DB 연결 설정
from airflow.providers.mysql.hooks.mysql import MySqlHook

def get_dwh_engine():
    return MySqlHook(mysql_conn_id='local_warehouse').get_sqlalchemy_engine()

def get_mart_engine():
    return MySqlHook(mysql_conn_id='local_mart').get_sqlalchemy_engine()

def get_railway_engine():
    return MySqlHook(mysql_conn_id='railway_mart').get_sqlalchemy_engine()


# =============================================
# 공통 함수
# =============================================
# NOTE: 필터링 후 전량 로드. 데이터 증가 시 GROUP BY를 SQL로 내려 집계 결과만 수신하도록 전환 필요
def read_in_chunks(query, engine, chunksize=100000):
    chunks = pd.read_sql(query, engine, chunksize=chunksize)
    return pd.concat(chunks, ignore_index=True)


def get_clean_user():
    query = """
        SELECT id, gender, point, is_staff, is_superuser,
               pending_chat, friend_id_list, created_at
        FROM raw_accounts_user
        WHERE is_staff = 0
          AND is_superuser = 0
          AND point < 100000000
          AND pending_chat >= 0
    """
    df_user = read_in_chunks(query, get_dwh_engine())
    df_user_clean = df_user.dropna().rename(columns={'id': 'user_id'})
    # 기존 .loc[lambda df: ...] 4줄은 WHERE로 옮겼으니 삭제
    import ast
    df_user_clean['friend_cnt'] = (
        df_user_clean['friend_id_list']
        .apply(lambda x: len(ast.literal_eval(x)) if isinstance(x, str) and x.startswith('[') else 0)
    )
    df_user_clean['created_at'] = pd.to_datetime(df_user_clean['created_at'])
    return df_user_clean.reset_index(drop=True)


def get_questionrecord():
    query = """
        SELECT user_id, created_at
        FROM raw_accounts_userquestionrecord
        WHERE created_at BETWEEN '2023-05-27' AND '2024-05-08'
    """
    df = read_in_chunks(query, get_dwh_engine())
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df


def get_paymenthistory():
    query = """
        SELECT user_id, created_at, item_price
        FROM raw_accounts_paymenthistory
        WHERE created_at BETWEEN '2023-05-27' AND '2024-05-08'
    """
    df = read_in_chunks(query, get_dwh_engine())
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df


def get_pointhistory():
    query = """
        SELECT user_id, created_at, delta_point, user_question_record_id
        FROM raw_accounts_pointhistory
        WHERE created_at BETWEEN '2023-05-27' AND '2024-05-08'
    """
    df = read_in_chunks(query, get_dwh_engine())
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df


def get_blockrecord():
    query = """
        SELECT user_id, created_at
        FROM raw_accounts_blockrecord
        WHERE created_at BETWEEN '2023-05-27' AND '2024-05-08'
    """
    df = read_in_chunks(query, get_dwh_engine())
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df


def get_attendance():
    import ast
    df = read_in_chunks("SELECT * FROM raw_accounts_attendance", get_dwh_engine())
    df['attendance_date_list'] = df['attendance_date_list'].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else []
    )
    df_exploded = df.explode('attendance_date_list').rename(columns={'attendance_date_list': 'attendance_date'})
    df_exploded['attendance_date'] = pd.to_datetime(df_exploded['attendance_date'])
    return df_exploded[['user_id', 'attendance_date']]


# =============================================
# Task 1. activity_super_user_df + activity_cluster_df 적재
# =============================================
def load_cluster(**context):

    df_user_clean     = get_clean_user()
    df_questionrecord = get_questionrecord()
    df_paymenthistory = get_paymenthistory()
    df_pointhistory   = get_pointhistory()
    df_blockrecord    = get_blockrecord()

    # ── RFM 집계 ──────────────────────────────────
    df_vote_summary = (
        df_questionrecord[df_questionrecord['created_at'].between(ANALYSIS_START, ANALYSIS_END)]
        .groupby('user_id')
        .agg(last_vote=('created_at', 'max'), frequency_F=('created_at', 'count'))
        .reset_index()
    )
    df_vote_summary['recency_R'] = (ANALYSIS_END - df_vote_summary['last_vote']).dt.days

    df_pay_summary = (
        df_paymenthistory[df_paymenthistory['created_at'].between(ANALYSIS_START, ANALYSIS_END)]
        .groupby('user_id').size().reset_index(name='monetary_M')
    )

    df_block_summary = (
        df_blockrecord[df_blockrecord['created_at'].between(ANALYSIS_START, ANALYSIS_END)]
        .groupby('user_id').size().reset_index(name='block_cnt')
    )

    df_point_summary = (
        df_pointhistory[df_pointhistory['created_at'].between(ANALYSIS_START, ANALYSIS_END)]
        .groupby('user_id').agg(point_flow=('delta_point', 'sum'))
        .reset_index()
    )

    # ── 마스터 테이블 병합 ─────────────────────────
    cluster_df = df_user_clean[['user_id', 'gender', 'friend_cnt', 'created_at']].copy()
    cluster_df['user_id'] = pd.to_numeric(cluster_df['user_id'], errors='coerce')
    cluster_df = (
        cluster_df
        .merge(df_vote_summary[['user_id', 'recency_R', 'frequency_F']], on='user_id', how='left')
        .merge(df_pay_summary,    on='user_id', how='left')
        .merge(df_block_summary,  on='user_id', how='left')
        .merge(df_point_summary,  on='user_id', how='left')
    )

    # ── Recency 보정 ───────────────────────────────
    cluster_df['recency_R_filled'] = (ANALYSIS_END - cluster_df['created_at']).dt.days.clip(upper=MAX_PERIOD_DAYS)
    cluster_df['recency (R)']      = cluster_df['recency_R'].fillna(cluster_df['recency_R_filled']).clip(lower=0)

    # ── 결측치 처리 ────────────────────────────────
    cluster_df.fillna({'frequency_F': 0, 'monetary_M': 0, 'block_cnt': 0, 'point_flow': 0, 'point': 0}, inplace=True)

    # ── 컬럼명 정리 ────────────────────────────────
    cluster_df = cluster_df.rename(columns={
        'frequency_F': 'frequency (F)',
        'monetary_M' : 'monetary (M)',
    })

    # ── Super 유저 분리 (투표수 507회 이상, 상위 1%) ─
    cluster_df['user_type'] = cluster_df['frequency (F)'].apply(
        lambda x: 'Super' if x >= SUPER_USER_THRESHOLD else 'General'
    )

    super_user_df   = cluster_df[cluster_df['user_type'] == 'Super'].copy()
    general_user_df = cluster_df[cluster_df['user_type'] == 'General'].copy()
    print(f"Super 유저 수: {len(super_user_df)}명 / General 유저 수: {len(general_user_df)}명")

    # ── Super 유저 적재 ────────────────────────────
    super_result_df = super_user_df[[
        'user_id', 'user_type', 'gender', 'friend_cnt', 'point_flow',
        'recency (R)', 'frequency (F)', 'monetary (M)', 'block_cnt'
    ]].rename(columns={
        'recency (R)'  : 'recency',
        'frequency (F)': 'frequency',
        'monetary (M)' : 'monetary',
        'point_flow'   : 'point',
    })

    existing_super = pd.read_sql("SELECT user_id FROM activity_super_user_df", get_mart_engine())
    new_super = super_result_df[~super_result_df['user_id'].isin(existing_super['user_id'])]
    if not new_super.empty:
        new_super.to_sql('activity_super_user_df', get_mart_engine(), if_exists='append', index=False)
        print(f"activity_super_user_df : {len(new_super)}건 적재 완료")
    else:
        print("activity_super_user_df : 신규 데이터 없음")

    # ── General 유저 클러스터링 전처리 ───────────────
    cluster_final_df = general_user_df.drop(columns=['gender', 'point', 'user_type', 'point_flow'], errors='ignore')

    # 로그 변환
    log_features = ['friend_cnt', 'frequency (F)', 'monetary (M)', 'block_cnt']
    for col in log_features:
        cluster_final_df[f'{col}_log'] = np.log1p(cluster_final_df[col])

    # 스케일링
    clustering_features = [
        'recency (R)',
        'friend_cnt_log',
        'frequency (F)_log',
        'monetary (M)_log',
        'block_cnt_log'
    ]
    scaler    = StandardScaler()
    df_scaled = pd.DataFrame(
        scaler.fit_transform(cluster_final_df[clustering_features]),
        columns=clustering_features
    )

    # ── K-means 클러스터링 (K=6) ───────────────────
    kmeans = KMeans(n_clusters=6, init='k-means++', n_init=10, random_state=42)
    labels = kmeans.fit_predict(df_scaled)
    cluster_final_df = cluster_final_df.copy()
    cluster_final_df['cluster'] = labels

    # ── General 유저 적재 ──────────────────────────
    general_result_df = cluster_final_df[[
        'user_id', 'friend_cnt', 'recency (R)', 'frequency (F)', 'monetary (M)', 'block_cnt', 'cluster'
    ]].rename(columns={
        'recency (R)'  : 'recency',
        'frequency (F)': 'frequency',
        'monetary (M)' : 'monetary',
    })

    existing_cluster = pd.read_sql("SELECT user_id FROM activity_cluster_df", get_mart_engine())
    new_cluster = general_result_df[~general_result_df['user_id'].isin(existing_cluster['user_id'])]
    if not new_cluster.empty:
        new_cluster.to_sql('activity_cluster_df', get_mart_engine(), if_exists='append', index=False)
        print(f"activity_cluster_df : {len(new_cluster)}건 적재 완료")
    else:
        print("activity_cluster_df : 신규 데이터 없음")


# =============================================
# Task 2. activity_timeseries_df 적재 (로컬 마트)
# =============================================
def load_timeseries(**context):

    df_questionrecord = get_questionrecord()
    df_paymenthistory = get_paymenthistory()
    df_pointhistory   = get_pointhistory()
    df_attendance     = get_attendance()
    df_user_clean     = get_clean_user()

    # 가격 매핑
    PRICE_MAP = {
        'heart.200' : 900,
        'heart.777' : 1900,
        'heart.1000': 2900,
        'heart.4000': 9900,
    }
    df_paymenthistory['item_price'] = df_paymenthistory['productId'].map(PRICE_MAP).fillna(0)

    # 로컬 마트에서 cluster 매핑 조회 (General 유저만)
    cluster_map = pd.read_sql(
        "SELECT user_id, cluster FROM activity_cluster_df WHERE cluster IN (1, 4, 5)",
        get_mart_engine()
    )

    target_clusters = [1, 4, 5]

    # ── 주차 프레임 생성 ───────────────────────────
    all_weeks = pd.date_range(start=ANALYSIS_START, end=ANALYSIS_END, freq='W-MON')
    base_df   = pd.DataFrame(list(product(all_weeks, target_clusters)), columns=['week_start', 'cluster'])

    week_mapping = {date: f"W{i+1}" for i, date in enumerate(all_weeks)}
    base_df['week_label'] = base_df['week_start'].apply(
        lambda d: f"{d.strftime('%Y-%m-%d')} ({week_mapping[d]})"
    )

    # ── cum_users: 주차별 누적 가입자 수 산출 ────────
    df_user_clean['created_at'] = pd.to_datetime(df_user_clean['created_at'])
    user_cluster = df_user_clean[['user_id', 'created_at']].merge(cluster_map, on='user_id', how='inner')

    cum_users_list = []
    for cluster in target_clusters:
        cluster_users = user_cluster[user_cluster['cluster'] == cluster]
        for week in all_weeks:
            cnt = cluster_users[cluster_users['created_at'] <= week + pd.Timedelta(days=6)]['user_id'].nunique()
            cum_users_list.append({'week_start': week, 'cluster': cluster, 'cum_users': cnt})
    cum_users_df = pd.DataFrame(cum_users_list)

    # ── cluster 매핑 및 주차 단위 집계 공통 함수 ───
    def map_cluster_and_week(df, date_col):
        df = df.merge(cluster_map, on='user_id', how='inner')
        df = df[df['cluster'].isin(target_clusters)]
        df = df[(df[date_col] >= ANALYSIS_START) & (df[date_col] <= ANALYSIS_END)]
        df['week_start'] = df[date_col].dt.to_period('W').dt.start_time
        return df

    # revenue
    pay_df     = map_cluster_and_week(df_paymenthistory, 'created_at')
    revenue_df = pay_df.groupby(['week_start', 'cluster'])['item_price'].sum().reset_index(name='revenue')

    # vote_cnt / vote_user_cnt (이상치 100표 이상 제거)
    vote_df       = map_cluster_and_week(df_questionrecord, 'created_at')
    vote_per_user = vote_df.groupby(['week_start', 'cluster', 'user_id']).size().reset_index(name='user_vote_cnt')

    # vpu용 원본 (이상치 포함)
    vpu_agg = vote_per_user.groupby(['week_start', 'cluster']).agg(
        vote_cnt_raw=('user_vote_cnt', 'sum')
    ).reset_index()

    # 이상치 제거 (주간 100표 이상)
    vote_filtered = vote_per_user[vote_per_user['user_vote_cnt'] < 100]
    vote_agg = vote_filtered.groupby(['week_start', 'cluster']).agg(
        vote_cnt     =('user_vote_cnt', 'sum'),
        vote_user_cnt=('user_id',       'nunique')
    ).reset_index()

    # point_burn / net_flow
    point_df  = map_cluster_and_week(df_pointhistory, 'created_at')
    point_df['point_burn'] = point_df['delta_point'].apply(lambda x: abs(x) if x < 0 else 0)
    point_df['net_flow']   = point_df['delta_point']
    point_agg = point_df.groupby(['week_start', 'cluster']).agg(
        point_burn=('point_burn', 'sum'),
        net_flow  =('net_flow',   'sum')
    ).reset_index()

    # active_users (출석 OR 투표, 중복 제거)
    attend_merged = map_cluster_and_week(df_attendance, 'attendance_date')
    vote_merged   = map_cluster_and_week(df_questionrecord, 'created_at')

    act_attendance = attend_merged[['week_start', 'cluster', 'user_id']]
    act_voting     = vote_merged[['week_start', 'cluster', 'user_id']]
    combined       = pd.concat([act_attendance, act_voting]).drop_duplicates()
    active_agg     = combined.groupby(['week_start', 'cluster'])['user_id'].nunique().reset_index(name='active_users')

    # last_act: 해당 주차 내 최종 활동 시점
    combined_with_date = pd.concat([
        attend_merged[['week_start', 'cluster', 'user_id', 'attendance_date']].rename(columns={'attendance_date': 'act_date'}),
        vote_merged[['week_start', 'cluster', 'user_id', 'created_at']].rename(columns={'created_at': 'act_date'})
    ])
    last_act_df = combined_with_date.groupby(['week_start', 'cluster'])['act_date'].max().reset_index(name='last_act')

    # ── 전체 병합 ──────────────────────────────────
    ts_df = (
        base_df
        .merge(revenue_df,                                          on=['week_start', 'cluster'], how='left')
        .merge(vote_agg,                                            on=['week_start', 'cluster'], how='left')
        .merge(vpu_agg,                                             on=['week_start', 'cluster'], how='left')
        .merge(point_agg,                                           on=['week_start', 'cluster'], how='left')
        .merge(active_agg,                                          on=['week_start', 'cluster'], how='left')
        .merge(last_act_df,                                         on=['week_start', 'cluster'], how='left')
        .merge(cum_users_df,                                        on=['week_start', 'cluster'], how='left')
    )

    # last_act는 NULL 허용, 나머지는 0으로 채움
    fill_cols = ['revenue', 'vote_cnt', 'vote_user_cnt', 'vote_cnt_raw',
                 'point_burn', 'net_flow', 'active_users', 'cum_users']
    ts_df[fill_cols] = ts_df[fill_cols].fillna(0)

    # ── 파생 지표 생성 ─────────────────────────────
    ts_df = ts_df.sort_values(['cluster', 'week_start'])

    # 기본 KPI
    ts_df['war']                 = ts_df.apply(lambda r: (r['active_users'] / r['cum_users']) * 100 if r['cum_users'] > 0 else 0, axis=1)
    ts_df['arpu']                = ts_df.apply(lambda r: r['revenue'] / r['active_users'] if r['active_users'] > 0 else 0, axis=1)
    ts_df['votes_per_user']      = ts_df.apply(lambda r: r['vote_cnt'] / r['active_users'] if r['active_users'] > 0 else 0, axis=1)
    ts_df['vpu']                 = ts_df.apply(lambda r: r['vote_cnt_raw'] / r['active_users'] if r['active_users'] > 0 else 0, axis=1)
    ts_df['vote_conversion_rate']= ts_df.apply(lambda r: (r['vote_user_cnt'] / r['active_users']) * 100 if r['active_users'] > 0 else 0, axis=1)
    ts_df['pure_attendance_cnt'] = ts_df['active_users'] - ts_df['vote_user_cnt']
    ts_df['pure_attendance_rate']= ts_df.apply(lambda r: (r['pure_attendance_cnt'] / r['active_users']) * 100 if r['active_users'] > 0 else 0, axis=1)

    # 누적 지표
    ts_df['cumulative_net_flow'] = ts_df.groupby('cluster')['net_flow'].cumsum()
    ts_df['cumulative_burn']     = ts_df.groupby('cluster')['point_burn'].cumsum()

    # 상태 라벨
    ts_df['war_status']  = ts_df['war'].apply(lambda x: 'Safe' if x >= 20 else ('Warning' if x >= 5 else 'Exit'))
    ts_df['arpu_status'] = ts_df['arpu'].apply(lambda x: 'Safe' if x >= 3000 else ('Warning' if x >= 1000 else 'Exit'))
    ts_df['vpu_status']  = ts_df['votes_per_user'].apply(lambda x: 'Safe' if x >= 15 else ('Warning' if x >= 5 else 'Exit'))

    # ── 최종 컬럼 정리 ─────────────────────────────
    ts_df = ts_df[[
        'week_start', 'cluster', 'week_label',
        'revenue', 'active_users', 'cum_users',
        'point_burn', 'net_flow', 'last_act',
        'vote_cnt', 'vote_user_cnt',
        'war', 'arpu', 'votes_per_user', 'vote_conversion_rate',
        'war_status', 'arpu_status', 'vpu_status',
        'pure_attendance_cnt', 'pure_attendance_rate',
        'cumulative_net_flow', 'cumulative_burn',
        'vpu'
    ]]

    # ── 증분 적재 (로컬 마트) ──────────────────────
    existing = pd.read_sql("SELECT week_label, cluster FROM activity_timeseries_df", get_mart_engine())
    existing['key'] = existing['week_label'] + '_' + existing['cluster'].astype(str)
    ts_df['key']    = ts_df['week_label']    + '_' + ts_df['cluster'].astype(str)
    new_rows = ts_df[~ts_df['key'].isin(existing['key'])].drop(columns='key')

    if not new_rows.empty:
        new_rows.to_sql('activity_timeseries_df', get_mart_engine(), if_exists='append', index=False)
        print(f"activity_timeseries_df : {len(new_rows)}건 적재 완료")
    else:
        print("activity_timeseries_df : 신규 데이터 없음")


# =============================================
# Task 3. Railway 동기화 (마트 테이블만)
# =============================================
def sync_railway(**context):
    mart_engine    = get_mart_engine()
    railway_engine = get_railway_engine()

    for table in ['activity_super_user_df', 'activity_cluster_df', 'activity_timeseries_df']:

        local_df   = pd.read_sql(f"SELECT * FROM {table}", mart_engine)
        railway_df = pd.read_sql(f"SELECT * FROM {table}", railway_engine)

        if table in ['activity_super_user_df', 'activity_cluster_df']:
            new_rows = local_df[~local_df['user_id'].isin(railway_df['user_id'])]

        elif table == 'activity_timeseries_df':
            railway_df['key'] = railway_df['week_label'] + '_' + railway_df['cluster'].astype(str)
            local_df['key']   = local_df['week_label']   + '_' + local_df['cluster'].astype(str)
            new_rows = local_df[~local_df['key'].isin(railway_df['key'])].drop(columns='key')

        else:
            continue

        if not new_rows.empty:
            new_rows.to_sql(table, railway_engine, if_exists='append', index=False)
            print(f"{table} : {len(new_rows)}건 Railway 동기화 완료")
        else:
            print(f"{table} : 신규 데이터 없음")
            
            
# =============================================
# DAG 정의
# =============================================
with DAG(
    dag_id='mart1_activity_pipeline',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 5, 8),
    schedule_interval='@weekly',
    catchup=False,
    tags=['mart1', 'activity'],
) as dag:

    t_cluster      = PythonOperator(task_id='load_cluster',    python_callable=load_cluster)
    t_timeseries   = PythonOperator(task_id='load_timeseries', python_callable=load_timeseries)
    t_sync_railway = PythonOperator(task_id='sync_railway',    python_callable=sync_railway)

    t_cluster >> t_timeseries >> t_sync_railway