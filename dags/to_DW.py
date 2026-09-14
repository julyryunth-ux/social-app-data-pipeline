"""
📦 RAW DB → Data Warehouse 적재 파이프라인
- 최초 실행: 전체 데이터 풀 로드 (FULL LOAD)
- 이후 매일: 증분 데이터만 업데이트 (INCREMENTAL LOAD)
- 메타데이터 테이블(pipeline_sync_log)로 마지막 동기화 시점 추적
- SSDictCursor(서버사이드 커서)로 대용량 테이블 메모리 안전 처리

🔧 최종 수정 사항 (2024 데이터 명세서 기반):
- accounts_attendance: created_at 없음 → None (전체 재적재)
- accounts_user_contacts: created_at 없음 → None (전체 재적재)
- hackle_properties: created_at 없음 → None (전체 재적재)
- device_properties: created_at 없음 → None (전체 재적재)
- user_properties: created_at 없음 → None (전체 재적재)
- hackle_events: event_datetime 사용 (created_at 대신)
"""
import os
import gc
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pymysql
import pymysql.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# -------------------------
# 로거 설정
# -------------------------
logger = logging.getLogger(__name__)

# -------------------------
# DB 연결 정보
# -------------------------
WH_DB_URL  = os.environ["WH_DB_URL"]

# -------------------------
# 테이블별 설정
#
# incremental_col : 증분 기준 컬럼 (None이면 매일 전체 재적재)
# pk              : 중복 제거용 기본 키 컬럼 (None이면 중복 제거 생략)
# chunk_size      : 청크 단위 행 수
# preprocessor    : 추가 전처리 함수명 (없으면 None)
# timeout_hours   : Task 실행 제한 시간 (zombie 방지)
# -------------------------
TABLE_CONFIG = {
    # ── 레퍼런스성 소형 테이블 (증분 컬럼 없음 → 매일 전체 재적재) ──
    "accounts_group":              {"incremental_col": None,         "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "accounts_school":             {"incremental_col": None,         "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "accounts_nearbyschool":       {"incremental_col": None,         "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "polls_question":              {"incremental_col": None,         "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "polls_questionset":           {"incremental_col": None,         "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "polls_questionpiece":         {"incremental_col": None,         "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},

    # ── 증분 적재 가능한 테이블 (created_at 존재) ──
    "accounts_user":               {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": "preprocess_accounts_user",         "timeout_hours": 2},
    "accounts_userquestionrecord": {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 2},
    "polls_usercandidate":         {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "polls_questionreport":        {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "accounts_blockrecord":        {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "accounts_timelinereport":     {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "accounts_userwithdraw":       {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "accounts_paymenthistory":     {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "accounts_failpaymenthistory": {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "event_receipts":              {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "events":                      {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "accounts_pointhistory":       {"incremental_col": "created_at", "pk": "id", "chunk_size": 10_000, "preprocessor": "preprocess_accounts_pointhistory",  "timeout_hours": 2},
    "accounts_friendrequest":      {"incremental_col": "created_at", "pk": "id", "chunk_size": 5_000,  "preprocessor": None,                               "timeout_hours": 6},

    # ── created_at 없는 테이블 → 전체 재적재 ──
    "accounts_attendance":         {"incremental_col": None,         "pk": "id", "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "accounts_user_contacts":      {"incremental_col": None,         "pk": None, "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 1},
    "hackle_properties":           {"incremental_col": None,         "pk": None, "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 2},
    "device_properties":           {"incremental_col": None,         "pk": None, "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 2},
    "user_properties":             {"incremental_col": None,         "pk": None, "chunk_size": 10_000, "preprocessor": None,                               "timeout_hours": 2},

    # ── hackle_events: event_datetime으로 증분 가능 (대용량) ──
    "hackle_events":               {"incremental_col": "event_datetime", "pk": None, "chunk_size": 5_000,  "preprocessor": None,                           "timeout_hours": 6},
}

WH_TABLE_PREFIX = "raw_"
SYNC_LOG_TABLE  = "pipeline_sync_log"


# =========================================================
# 전처리 함수
# =========================================================

def preprocess_accounts_user(df: pd.DataFrame) -> pd.DataFrame:
    """
    accounts_user 테이블 전처리
    - gender, group_id NULL 제거
    - point 상한선 체크 (100M 이하)
    - pending_chat 음수 제거
    """
    if {"gender", "group_id"}.issubset(df.columns):
        df = df.dropna(subset=["gender", "group_id"])
    if "point" in df.columns:
        df = df[df["point"] <= 100_000_000]
    if "pending_chat" in df.columns:
        df = df[df["pending_chat"] >= 0]
    return df


def preprocess_accounts_pointhistory(df: pd.DataFrame) -> pd.DataFrame:
    """
    accounts_pointhistory 테이블 전처리
    - user_question_record_id NULL 제거
    - created_at에서 dt(날짜) 컬럼 추출
    """
    if "user_question_record_id" in df.columns:
        df = df.dropna(subset=["user_question_record_id"])
    if "created_at" in df.columns:
        df["dt"] = pd.to_datetime(df["created_at"]).dt.date
    return df


PREPROCESSORS = {
    "preprocess_accounts_user":         preprocess_accounts_user,
    "preprocess_accounts_pointhistory": preprocess_accounts_pointhistory,
}


# =========================================================
# SSDictCursor 스트리밍 헬퍼
# ─────────────────────────────────────────────────────────
# pd.read_sql + chunksize는 MySQL에서 전체 결과를 서버 메모리에
# 올린 뒤 잘라서 전달하므로 대용량 테이블에서 OOM 발생.
# SSDictCursor는 MySQL 서버가 행을 한 줄씩 전송 → 메모리 안전.
# =========================================================

def _raw_conn():
    """pymysql SSDictCursor 전용 커넥션"""
    return pymysql.connect(
        host        = os.environ["RAW_DB_HOST"],
        user        = os.environ["RAW_DB_USER"],
        password    = os.environ["RAW_DB_PASSWORD"],
        database    = os.environ["RAW_DB_NAME"],
        port        = int(os.environ.get("RAW_DB_PORT", 3306)),
        cursorclass = pymysql.cursors.SSDictCursor,
        connect_timeout = 30,
    )


def stream_query_chunks(query: str, chunk_size: int):
    """SSDictCursor로 쿼리 결과를 chunk_size 단위 DataFrame으로 yield"""
    conn = _raw_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                yield pd.DataFrame(rows)
    finally:
        conn.close()


# =========================================================
# 메타데이터 유틸
# =========================================================

def get_wh_engine():
    return create_engine(WH_DB_URL, pool_pre_ping=True)


def ensure_sync_log_table(engine):
    """동기화 메타데이터 테이블 생성"""
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SYNC_LOG_TABLE} (
                table_name      VARCHAR(128) PRIMARY KEY,
                last_synced_at  DATETIME     NOT NULL,
                row_count       BIGINT       DEFAULT 0,
                updated_at      DATETIME     NOT NULL
            )
        """))


def get_last_synced_at(engine, table_name: str):
    """마지막 동기화 시점 조회"""
    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT last_synced_at FROM {SYNC_LOG_TABLE} WHERE table_name = :t"),
            {"t": table_name}
        ).fetchone()
    return row[0] if row else None


def update_sync_log(engine, table_name: str, last_synced_at: datetime, row_count: int):
    """동기화 메타데이터 업데이트"""
    now = datetime.utcnow()
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {SYNC_LOG_TABLE} (table_name, last_synced_at, row_count, updated_at)
            VALUES (:t, :lsa, :rc, :now)
            ON DUPLICATE KEY UPDATE
                last_synced_at = VALUES(last_synced_at),
                row_count      = VALUES(row_count),
                updated_at     = VALUES(updated_at)
        """), {"t": table_name, "lsa": last_synced_at, "rc": row_count, "now": now})


def wh_table_exists(engine, table_name: str) -> bool:
    """DW 테이블 존재 여부 확인"""
    with engine.connect() as conn:
        return conn.execute(text("SHOW TABLES LIKE :t"), {"t": table_name}).fetchone() is not None


def _get_existing_ids(engine, table_name: str, pk_col: str) -> set:
    """증분 적재 시 기존 PK 목록 조회 (중복 방지용)"""
    if not wh_table_exists(engine, table_name):
        return set()
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT {pk_col} FROM {table_name}")).fetchall()
    return {r[0] for r in rows}


# =========================================================
# 핵심 적재 함수
# =========================================================

def load_table(table_name: str, sync_start: datetime):
    """
    테이블 적재 메인 로직
    - FULL LOAD: incremental_col이 None이거나 최초 실행
    - INCREMENTAL LOAD: incremental_col 기준으로 신규 데이터만 적재
    """
    cfg          = TABLE_CONFIG[table_name]
    inc_col      = cfg["incremental_col"]
    pk           = cfg["pk"]
    chunk_size   = cfg["chunk_size"]
    preprocessor = PREPROCESSORS.get(cfg["preprocessor"]) if cfg["preprocessor"] else None
    wh_table     = f"{WH_TABLE_PREFIX}{table_name}"

    wh_engine = get_wh_engine()
    ensure_sync_log_table(wh_engine)
    last_synced_at = get_last_synced_at(wh_engine, table_name)

    # ── 모드 결정 ──
    is_full_load = (last_synced_at is None) or (inc_col is None)

    if is_full_load:
        query     = f"SELECT * FROM {table_name}"
        if_exists = "replace"
        logger.info(f"[FULL LOAD] {table_name}")
    else:
        query     = f"SELECT * FROM {table_name} WHERE {inc_col} > '{last_synced_at}'"
        if_exists = "append"
        logger.info(f"[INCREMENTAL] {table_name} | since {last_synced_at}")

    # 증분 APPEND 시 기존 PK 미리 수집
    existing_ids = set()
    if not is_full_load and pk:
        existing_ids = _get_existing_ids(wh_engine, wh_table, pk)

    # ── SSDictCursor 스트리밍 청크 처리 ──
    total_rows  = 0
    first_chunk = True

    for chunk in stream_query_chunks(query, chunk_size):
        if chunk.empty:
            continue

        if preprocessor:
            chunk = preprocessor(chunk)

        # 증분 중복 방지
        if not is_full_load and pk and pk in chunk.columns:
            chunk = chunk[~chunk[pk].isin(existing_ids)]
            if chunk.empty:
                continue

        chunk.to_sql(
            name      = wh_table,
            con       = wh_engine,
            if_exists = if_exists if first_chunk else "append",
            index     = False,
            chunksize = 1_000,
            method    = "multi",
        )
        total_rows += len(chunk)
        first_chunk = False

        # 청크 처리 후 메모리 명시적 해제
        del chunk
        gc.collect()

    update_sync_log(wh_engine, table_name, sync_start, total_rows)
    logger.info(f"✅ {table_name} 완료 | rows={total_rows} | mode={'FULL' if is_full_load else 'INCREMENTAL'}")
    wh_engine.dispose()


# =========================================================
# Airflow Task 래퍼
# =========================================================

def make_task_callable(table_name: str):
    def _task(**context):
        sync_start = context["data_interval_end"]
        load_table(table_name, sync_start)
    return _task


# =========================================================
# DAG 정의
# =========================================================

REFERENCE_TABLES = [
    "accounts_group", "accounts_school", "accounts_nearbyschool",
    "polls_question", "polls_questionset", "polls_questionpiece",
]

INCREMENTAL_GROUP_1 = [
    "accounts_user", "accounts_userquestionrecord", "polls_usercandidate",
    "polls_questionreport", "accounts_blockrecord",
    "accounts_timelinereport", "accounts_userwithdraw",
    "accounts_paymenthistory", "accounts_failpaymenthistory", "event_receipts",
    "events", "accounts_pointhistory",
]

# created_at 없는 테이블들 (매일 전체 재적재)
FULL_RELOAD_TABLES = [
    "accounts_attendance",
    "accounts_user_contacts",
    "hackle_properties",
    "device_properties",
    "user_properties",
]

# 대용량 테이블은 순차 실행 (병렬 시 DB 부하 집중 방지)
INCREMENTAL_GROUP_2 = [
    "accounts_friendrequest",
    "hackle_events",  # event_datetime으로 증분
]

with DAG(
    dag_id            = "load_to_warehouse",
    description       = "RAW DB → DW 전체/증분 적재 (25 tables, SSDictCursor, 데이터명세서 반영)",
    start_date        = datetime(2024, 1, 1),
    schedule_interval = "0 3 * * *",   # 매일 오전 3시 (UTC)
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["warehouse", "incremental", "fixed"],
    default_args      = {
        "owner": 'jungryun',
        "retries":     1,                    # zombie 방지: 재시도 1회만
        "retry_delay": timedelta(minutes=10),
    },
) as dag:

    def create_task(table_name: str):
        return PythonOperator(
            task_id           = f"load__{table_name}",
            python_callable   = make_task_callable(table_name),
            execution_timeout = timedelta(hours=TABLE_CONFIG[table_name]["timeout_hours"]),
        )

    ref_tasks      = [create_task(t) for t in REFERENCE_TABLES]
    inc1_tasks     = [create_task(t) for t in INCREMENTAL_GROUP_1]
    reload_tasks   = [create_task(t) for t in FULL_RELOAD_TABLES]
    inc2_tasks     = [create_task(t) for t in INCREMENTAL_GROUP_2]

    # ── 의존성 ──
    # [레퍼런스 6개] (병렬)
    #       ↓
    # [일반 증분 12개] (병렬)
    #       ↓
    # [전체 재적재 5개] (병렬) - 작은 테이블이므로 병렬 OK
    #       ↓
    # [대용량 2개] accounts_friendrequest → hackle_events (순차)

    # 레퍼런스 → 증분1
    
    # 단계 사이를 EmptyOperator 게이트로 연결해 의존성 엣지 수를 축소
    # (직접 연결 시 6x12 + 12x5 + 5x1 = 137개 → 게이트 경유 시 약 50개)

    gate_ref    = EmptyOperator(task_id='reference_done')
    gate_inc1   = EmptyOperator(task_id='incremental_done')
    gate_reload = EmptyOperator(task_id='full_reload_done')

    ref_tasks    >> gate_ref    >> inc1_tasks
    inc1_tasks   >> gate_inc1   >> reload_tasks
    reload_tasks >> gate_reload >> inc2_tasks[0]
    inc2_tasks[0] >> inc2_tasks[1]
