-- 로컬 서버에 있는 데이터 웨어하우스에서 로컬 마트로 ETL 후 공유 서버(Railway) 적재
-- 유저 활동 분석용 마트 DDL

CREATE DATABASE railway;
USE railway;

# MART_ACTIVITY DDL
-- 1. Super 유저 테이블 추가
CREATE TABLE activity_super_user_df (
    user_id         BIGINT          NOT NULL            COMMENT '유저 식별자',
    user_type       VARCHAR(10)     NOT NULL            COMMENT '유저 타입 (Super)',
    gender          VARCHAR(5)      NULL                COMMENT '성별',
    friend_cnt      INT             NOT NULL DEFAULT 0  COMMENT '보유 친구 수',
    point           FLOAT           NOT NULL DEFAULT 0  COMMENT '포인트',
    recency         FLOAT           NOT NULL            COMMENT '마지막 투표 경과일',
    frequency       FLOAT           NOT NULL DEFAULT 0  COMMENT '총 투표 참여 횟수',
    monetary        FLOAT           NOT NULL DEFAULT 0  COMMENT '유료 결제 발생 횟수',
    block_cnt       INT             NOT NULL DEFAULT 0  COMMENT '차단 횟수',
    PRIMARY KEY (user_id)
) COMMENT = 'Super 유저 VIP 테이블 (투표수 상위 1%, 507회 이상)'
ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- 2. activity_cluster_df 생성 (user_type 컬럼 추가)
CREATE TABLE activity_cluster_df (
    user_id         BIGINT          NOT NULL            COMMENT '유저 식별자',
    friend_cnt      INT             NOT NULL DEFAULT 0  COMMENT '보유 친구 수',
    recency         FLOAT           NOT NULL            COMMENT '마지막 투표 경과일 (최대 348일)',
    frequency       FLOAT           NOT NULL DEFAULT 0  COMMENT '총 투표 참여 횟수',
    monetary        FLOAT           NOT NULL DEFAULT 0  COMMENT '유료 결제 발생 횟수',
    block_cnt       INT             NOT NULL DEFAULT 0  COMMENT '차단 횟수',
    cluster         TINYINT         NOT NULL            COMMENT '군집 번호 (0~5, K=6)',
    PRIMARY KEY (user_id),
    INDEX idx_cluster (cluster)
) COMMENT = 'General 유저 클러스터링 테이블'
ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;


-- 3. activity_timeseries 생성
CREATE TABLE activity_timeseries_df (
    -- 기본 정보
    week_start              DATE            NOT NULL                COMMENT '주차 시작일 (YYYY-MM-DD)',
    cluster                 TINYINT         NOT NULL                COMMENT '군집 번호 (1, 4, 5)',
    week_label              VARCHAR(20)     NOT NULL                COMMENT '주차 레이블 (예: 2023-05-29 (W1))',

    -- 기초 수치
    revenue                 BIGINT          NOT NULL DEFAULT 0      COMMENT '주간 총 매출 (원)',
    active_users            INT             NOT NULL DEFAULT 0      COMMENT '주간 활성 유저 수 (출석 OR 투표 Unique)',
    cum_users               INT             NOT NULL DEFAULT 0      COMMENT '누적 가입자 수 (WAR 분모)',
    point_burn              FLOAT           NOT NULL DEFAULT 0      COMMENT '주간 포인트 소진량 (절댓값)',
    net_flow                FLOAT           NOT NULL DEFAULT 0      COMMENT '주간 포인트 순증감 (적립 - 소진)',
    last_act                DATETIME        NULL                    COMMENT '해당 주차 내 최종 활동 시점',
    vote_cnt                INT             NOT NULL DEFAULT 0      COMMENT '정제된 총 투표수 (100표 이상 제거)',
    vote_user_cnt           INT             NOT NULL DEFAULT 0      COMMENT '정제된 투표자 수 (Unique Count)',
    pure_attendance_cnt     INT             NOT NULL DEFAULT 0      COMMENT '단순 출석 유저 수 (active_users - vote_user_cnt)',

    -- 핵심 KPI
    war                     FLOAT           NOT NULL DEFAULT 0      COMMENT '주간 활동률 (active_users / cum_users * 100)',
    arpu                    FLOAT           NOT NULL DEFAULT 0      COMMENT '활동 유저 인당 매출 (revenue / active_users)',
    votes_per_user          FLOAT           NOT NULL DEFAULT 0      COMMENT '정제된 인당 투표수 (vote_cnt / active_users)',
    vote_conversion_rate    FLOAT           NOT NULL DEFAULT 0      COMMENT '투표 전환율 (vote_user_cnt / active_users * 100)',
    pure_attendance_rate    FLOAT           NOT NULL DEFAULT 0      COMMENT '단순 출석률 (pure_attendance_cnt / active_users * 100)',

    -- 진단 데이터
    war_status              VARCHAR(10)     NOT NULL DEFAULT ''     COMMENT '활동률 상태 (Safe/Warning/Exit)',
    arpu_status             VARCHAR(10)     NOT NULL DEFAULT ''     COMMENT '매출 상태 (Safe/Warning/Exit)',
    vpu_status              VARCHAR(10)     NOT NULL DEFAULT ''     COMMENT '투표 상태 (Safe/Warning/Exit)',

    -- 시계열 KPI
    cumulative_net_flow     FLOAT           NOT NULL DEFAULT 0      COMMENT '누적 포인트 순증감 (net_flow 누적합)',
    cumulative_burn         FLOAT           NOT NULL DEFAULT 0      COMMENT '누적 포인트 소진액 (point_burn 누적합)',

    -- 참조
    vpu                     FLOAT           NOT NULL DEFAULT 0      COMMENT '원본 인당 투표수 (이상치 포함)',

    PRIMARY KEY (week_start, cluster),
    INDEX idx_cluster (cluster),
    INDEX idx_week_label (week_label)
) COMMENT = '시계열 분석용 테이블 (df_ts_ms_final) - 군집 1·4·5 대상'
ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;


