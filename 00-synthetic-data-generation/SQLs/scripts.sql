
CREATE SCHEMA BANKING;

CREATE TABLE BANKING.user_profiles (
    user_id         VARCHAR(20)      NOT NULL,
    full_name       NVARCHAR(200)    NOT NULL,
    email           NVARCHAR(200)    NOT NULL,
    phone           VARCHAR(25)          NULL,
    date_of_birth   DATE                 NULL,
    nationality     VARCHAR(5)           NULL,
    gender          VARCHAR(5)           NULL,
    address         NVARCHAR(500)        NULL,
    city            NVARCHAR(100)        NULL,
    state           NVARCHAR(100)        NULL,
    is_pep          BIT              NOT NULL  DEFAULT 0,
    is_sanctioned   BIT              NOT NULL  DEFAULT 0,
    account_count   TINYINT          NOT NULL  DEFAULT 1,
    customer_since  DATE                 NULL,
    status          VARCHAR(20)      NOT NULL,
    created_at      DATETIME2(6)     NOT NULL,
    updated_at      DATETIME2(6)     NOT NULL,

    CONSTRAINT PK_user_profiles PRIMARY KEY (user_id)
);


CREATE TABLE BANKING.historical_transactions (
    transaction_id  VARCHAR(36)      NOT NULL,
    legacy_txn_id   VARCHAR(50)          NULL,
    account_id      VARCHAR(20)      NOT NULL,
    txn_type        VARCHAR(20)          NULL,
    channel         VARCHAR(30)          NULL,
    amount          DECIMAL(18, 2)       NULL,    -- nullable: legacy NULL_AMOUNT rows
    currency        VARCHAR(10)          NULL,    -- may contain XXX / YYY / ---
    status          VARCHAR(30)          NULL,    -- legacy codes: APPROVED, AUTH, NSF…
    city            NVARCHAR(100)        NULL,
    source_system   VARCHAR(50)          NULL,
    _source         VARCHAR(30)          NULL,
    _ingest_time    DATETIME2(3)         NULL,
    timestamp       DATETIME2(3)     NOT NULL,

    CONSTRAINT PK_historical_transactions PRIMARY KEY (transaction_id)
    -- No FK on account_id: legacy data may have MISSING_ACCOUNT rows
);

CREATE TABLE BANKING.accounts (
    account_id      VARCHAR(20)      NOT NULL,
    user_id         VARCHAR(20)      NOT NULL,
    account_type    VARCHAR(20)      NOT NULL,
    currency        VARCHAR(10)      NOT NULL,
    status          VARCHAR(20)     NULL,
    opened_on       DATE             NOT NULL,
    closed_on       DATE                 NULL,
    updated_on      DATETIME2(3)     NOT NULL,

    CONSTRAINT PK_accounts           PRIMARY KEY (account_id),
    CONSTRAINT FK_accounts_user      FOREIGN KEY (user_id)
        REFERENCES APPLICATION_A.user_profiles (user_id)
);

