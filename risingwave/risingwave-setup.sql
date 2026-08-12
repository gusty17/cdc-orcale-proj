-- RisingWave - consume the Debezium topic straight into a live table.

-- Ordered DROP block, dependents first. Without this, re-running this
-- file is not idempotent: DROP TABLE lookup_metadata (below) fails once
-- either flattening MV already exists, because that MV still depends on
-- it - a second `setup.ps1 -SqlOnly` used to fail here.
DROP MATERIALIZED VIEW IF EXISTS t24_account_blob_before;
DROP MATERIALIZED VIEW IF EXISTS t24_account_before;
DROP MATERIALIZED VIEW IF EXISTS t24_account_blob_columns;
DROP MATERIALIZED VIEW IF EXISTS t24_account_columns;
DROP VIEW IF EXISTS t24_account_blob_text;

--set the barrier interval to 1 second.
ALTER SYSTEM SET barrier_interval_ms = 1000;

-- CREATE TABLE t24_account when adding primary key it upserts data not appends
CREATE TABLE IF NOT EXISTS t24_account (
    recid     VARCHAR,
    xmlrecord VARCHAR,
    PRIMARY KEY (recid)
) WITH (
    connector = 'kafka',
    topic = 't24.T24.ACCOUNT',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE JSON;

-- Audit log - every change, not just current state. 
CREATE TABLE IF NOT EXISTS t24_account_events (
    op       VARCHAR,
    "before" JSONB,
    "after"  JSONB,
    source   JSONB,
    ts_ms    BIGINT
) WITH (
    connector = 'kafka',
    topic = 't24.T24.ACCOUNT',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- blob table 
CREATE TABLE IF NOT EXISTS t24_account_blob (
    recid      VARCHAR,
    blobrecord VARCHAR,
    PRIMARY KEY (recid)
) WITH (
    connector = 'kafka',
    topic = 't24.T24.ACCOUNT_BLOB',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE JSON;

CREATE TABLE IF NOT EXISTS t24_account_blob_events (
    op       VARCHAR,
    "before" JSONB,
    "after"  JSONB,
    source   JSONB,
    ts_ms    BIGINT
) WITH (
    connector = 'kafka',
    topic = 't24.T24.ACCOUNT_BLOB',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Create a lookup table to resolve the XML tags into real column names.
--
-- Emptied and refilled, NOT dropped and recreated. DROP TABLE fails
-- whenever anything still depends on this table, and benchmarks/
-- 01-trace-setup.sql's t24_trace_parsed does - it lives in a different
-- file, so the ordered DROP block at the top of this one cannot know
-- about it. The old DROP/CREATE pair failed silently in sequence
-- (DROP -> "used by 1 other objects", CREATE -> "already exists") while
-- the INSERTs below still succeeded, appending a second full copy of
-- every row. Three `-SqlOnly` runs had left 867 rows for 289 keys,
-- fanning out both flattening MVs' double LEFT JOIN 9x and inflating
-- t24_trace_parsed's columns_parsed count.
CREATE TABLE IF NOT EXISTS lookup_metadata (
    field_index      VARCHAR,
    m_index          VARCHAR,
    resolved_name_en VARCHAR,
    is_multivalue    BOOLEAN
);
DELETE FROM lookup_metadata;
INSERT INTO lookup_metadata (field_index, m_index, resolved_name_en, is_multivalue) VALUES
    ('c20', '18', 'ac_amt_loc', FALSE),
    ('c182', NULL, 'acc_deb_limit', FALSE),
    ('c0', NULL, 'account_number', FALSE),
    ('c11', NULL, 'account_officer', FALSE),
    ('c3', NULL, 'account_title_1', FALSE),
    ('c201', NULL, 'accounting_company', FALSE),
    ('c54', NULL, 'accr_chg_amount', FALSE),
    ('c52', NULL, 'accr_chg_categ', FALSE),
    ('c53', NULL, 'accr_chg_trans', FALSE),
    ('c60', NULL, 'accr_cr2_categ', FALSE),
    ('c57', NULL, 'accr_cr_trans', FALSE),
    ('c70', NULL, 'accr_dr2_amount', FALSE),
    ('c68', NULL, 'accr_dr2_categ', FALSE),
    ('c71', NULL, 'accr_dr2_susp', FALSE),
    ('c66', NULL, 'accr_dr_amount', FALSE),
    ('c65', NULL, 'accr_dr_trans', FALSE),
    ('c87', NULL, 'acct_credit_int', FALSE),
    ('c88', NULL, 'acct_debit_int', FALSE),
    ('c108', NULL, 'allow_netting', FALSE),
    ('c100', NULL, 'alt_acct_id', TRUE),
    ('c99', NULL, 'alt_acct_type', TRUE),
    ('c29', NULL, 'amnt_last_cr_cust', FALSE),
    ('c41', NULL, 'amnt_last_dr_auto', FALSE),
    ('c44', NULL, 'amnt_last_dr_bank', FALSE),
    ('c20', '19', 'approval_date', FALSE),
    ('c20', '4', 'arabic_title', FALSE),
    ('c151', NULL, 'av_auth_db_mvmt', FALSE),
    ('c152', NULL, 'av_nau_db_mvmt', FALSE),
    ('c158', NULL, 'available_bal_upd', FALSE),
    ('c46', NULL, 'cap_date_charge', TRUE),
    ('c47', NULL, 'cap_date_cr_int', TRUE),
    ('c102', NULL, 'cap_date_prm', FALSE),
    ('c2', NULL, 'category', FALSE),
    ('c92', NULL, 'charge_account', FALSE),
    ('c93', NULL, 'charge_ccy', FALSE),
    ('c163', NULL, 'closed_online', FALSE),
    ('c90', NULL, 'closure_date', FALSE),
    ('c205', NULL, 'closure_notes', FALSE),
    ('c204', NULL, 'closure_reason', FALSE),
    ('c20', '12', 'collateral_code_for_reports', FALSE),
    ('c97', NULL, 'con_charge_accr', FALSE),
    ('c72', NULL, 'consol_key', FALSE),
    ('c159', NULL, 'consolidate_ent', FALSE),
    ('c83', NULL, 'contingent_bal_cr', FALSE),
    ('c143', NULL, 'contingent_int', FALSE),
    ('c157', NULL, 'credit_check', FALSE),
    ('c213', NULL, 'credit_chk_txn_type', FALSE),
    ('c80', NULL, 'credit_movement', FALSE),
    ('c248', NULL, 'curr_no', FALSE),
    ('c8', NULL, 'currency', FALSE),
    ('c31', NULL, 'date_last_cr_auto', FALSE),
    ('c43', NULL, 'date_last_dr_bank', FALSE),
    ('c37', NULL, 'date_last_dr_cust', FALSE),
    ('c167', NULL, 'date_last_update', FALSE),
    ('c250', NULL, 'date_time', TRUE),
    ('c81', NULL, 'debit_movement', FALSE),
    ('c20', '3', 'debit_or_credit', FALSE),
    ('c124', NULL, 'dispo_exempt', FALSE),
    ('c123', NULL, 'dispo_officer', FALSE),
    ('c187', NULL, 'dr2_adj_amount', FALSE),
    ('c186', NULL, 'dr_adj_amount', FALSE),
    ('c20', '17', 'ebc_auth_no', FALSE),
    ('c20', '23', 'email_address', FALSE),
    ('c220', NULL, 'emergency_block', FALSE),
    ('c20', '14', 'end_nab', FALSE),
    ('c147', NULL, 'ep_balance', FALSE),
    ('c146', NULL, 'er_balance', FALSE),
    ('c145', NULL, 'er_value_date', FALSE),
    ('c190', NULL, 'event', FALSE),
    ('c169', NULL, 'exposure_dates', FALSE),
    ('c209', NULL, 'external_posting', FALSE),
    ('c173', NULL, 'first_af_date', FALSE),
    ('c156', NULL, 'forward_mvmts', FALSE),
    ('c20', '15', 'ft_ref_nab', FALSE),
    ('c20', '5', 'hold_date', FALSE),
    ('c20', '8', 'hold_details', FALSE),
    ('c20', '10', 'hold_reason', FALSE),
    ('c135', NULL, 'ica_add_remove', FALSE),
    ('c136', NULL, 'ica_back_value', FALSE),
    ('c137', NULL, 'ica_main_acct', FALSE),
    ('c138', NULL, 'ica_main_date', FALSE),
    ('c132', NULL, 'ica_main_ratio', FALSE),
    ('c22', NULL, 'inactiv_marker', FALSE),
    ('c75', NULL, 'int_liq_ccy', FALSE),
    ('c96', NULL, 'interest_mkt', FALSE),
    ('c107', NULL, 'joint_notes', FALSE),
    ('c176', NULL, 'last_com_chg_date', FALSE),
    ('c10', NULL, 'limit_ref', FALSE),
    ('c89', NULL, 'link_to_limit', FALSE),
    ('c162', NULL, 'lock_inc_this_mvmt', FALSE),
    ('c91', NULL, 'locked_with_limit', FALSE),
    ('c183', NULL, 'mandate_appl', FALSE),
    ('c185', NULL, 'mandate_record', FALSE),
    ('c184', NULL, 'mandate_reg', FALSE),
    ('c6', NULL, 'mnemonic', FALSE),
    ('c224', NULL, 'multi_currency_parent', FALSE),
    ('c199', NULL, 'mv_alert_res1', FALSE),
    ('c198', NULL, 'mv_alert_res2', FALSE),
    ('c197', NULL, 'mv_alert_res3', FALSE),
    ('c165', NULL, 'next_acct_cap', FALSE),
    ('c164', NULL, 'next_af_date', FALSE),
    ('c166', NULL, 'next_exp_date', FALSE),
    ('c168', NULL, 'next_stmt_date', FALSE),
    ('c23', NULL, 'open_actual_bal', FALSE),
    ('c175', NULL, 'open_asset_type', FALSE),
    ('c78', NULL, 'opening_date', FALSE),
    ('c120', NULL, 'original_acct', FALSE),
    ('c210', NULL, 'parent_bv_date', FALSE),
    ('c76', NULL, 'passbook', FALSE),
    ('c170', NULL, 'portfolio_no', FALSE),
    ('c7', NULL, 'position_type', FALSE),
    ('c13', NULL, 'posting_restrict', FALSE),
    ('c103', NULL, 'premium_freq', FALSE),
    ('c112', NULL, 'reco_tolerance', FALSE),
    ('c18', NULL, 'referal_code', FALSE),
    ('c200', NULL, 'request_id', FALSE),
    ('c234', NULL, 'reserved_12', FALSE),
    ('c233', NULL, 'reserved_13', FALSE),
    ('c232', NULL, 'reserved_14', FALSE),
    ('c230', NULL, 'reserved_16', FALSE),
    ('c229', NULL, 'reserved_17', FALSE),
    ('c228', NULL, 'reserved_18', FALSE),
    ('c227', NULL, 'reserved_19', FALSE),
    ('c226', NULL, 'reserved_20', FALSE),
    ('c243', NULL, 'reserved_3', FALSE),
    ('c242', NULL, 'reserved_4', FALSE),
    ('c241', NULL, 'reserved_5', FALSE),
    ('c240', NULL, 'reserved_6', FALSE),
    ('c239', NULL, 'reserved_7', FALSE),
    ('c238', NULL, 'reserved_8', FALSE),
    ('c20', '30', 'revaluation', FALSE),
    ('c148', NULL, 'sb_group_id', FALSE),
    ('c212', NULL, 'secondary_limit_amt', FALSE),
    ('c116', NULL, 'serial_no_format', FALSE),
    ('c171', NULL, 'shadow_account', FALSE),
    ('c5', NULL, 'short_title', FALSE),
    ('c77', NULL, 'start_year_bal', FALSE),
    ('c33', NULL, 'tran_last_cr_auto', FALSE),
    ('c36', NULL, 'tran_last_cr_bank', FALSE),
    ('c30', NULL, 'tran_last_cr_cust', FALSE),
    ('c42', NULL, 'tran_last_dr_auto', FALSE),
    ('c45', NULL, 'tran_last_dr_bank', FALSE),
    ('c39', NULL, 'tran_last_dr_cust', FALSE),
    ('c193', NULL, 'value', FALSE),
    ('c79', NULL, 'value_date', FALSE),
    ('c19', NULL, 'waive_ledger_fee', FALSE),
    ('c20', '33', 'welcome_pack', FALSE),
    ('c20', '34', 'account_purpose', FALSE),
    ('c4', NULL, 'account_title_2', FALSE),
    ('c223', NULL, 'account_type', FALSE),
    ('c55', NULL, 'accr_chg_susp', FALSE),
    ('c62', NULL, 'accr_cr2_amount', FALSE),
    ('c63', NULL, 'accr_cr2_susp', FALSE),
    ('c61', NULL, 'accr_cr2_trans', FALSE),
    ('c58', NULL, 'accr_cr_amount', FALSE),
    ('c56', NULL, 'accr_cr_categ', FALSE),
    ('c59', NULL, 'accr_cr_susp', FALSE),
    ('c69', NULL, 'accr_dr2_trans', FALSE),
    ('c64', NULL, 'accr_dr_categ', FALSE),
    ('c67', NULL, 'accr_dr_susp', FALSE),
    ('c20', '22', 'accroual_amount', FALSE),
    ('c20', '1', 'activyty', FALSE),
    ('c144', NULL, 'all_in_one_product', FALSE),
    ('c211', NULL, 'allowed_bv_date', FALSE),
    ('c32', NULL, 'amnt_last_cr_auto', FALSE),
    ('c35', NULL, 'amnt_last_cr_bank', FALSE),
    ('c38', NULL, 'amnt_last_dr_cust', FALSE),
    ('c104', NULL, 'apr', FALSE),
    ('c181', NULL, 'arrangement_id', FALSE),
    ('c255', NULL, 'audit_date_time', FALSE),
    ('c254', NULL, 'auditor_code', FALSE),
    ('c251', NULL, 'authoriser', FALSE),
    ('c117', NULL, 'auto_pay_acct', FALSE),
    ('c119', NULL, 'auto_rec_ccy', FALSE),
    ('c153', NULL, 'av_auth_cr_mvmt', FALSE),
    ('c154', NULL, 'av_nau_cr_mvmt', FALSE),
    ('c155', NULL, 'available_bal', FALSE),
    ('c150', NULL, 'available_date', FALSE),
    ('c215', NULL, 'balance_conversion_mkt', FALSE),
    ('c51', NULL, 'cap_back_value', FALSE),
    ('c48', NULL, 'cap_date_c2_int', TRUE),
    ('c50', NULL, 'cap_date_d2_int', TRUE),
    ('c49', NULL, 'cap_date_dr_int', TRUE),
    ('c174', NULL, 'cash_pool_group', FALSE),
    ('c94', NULL, 'charge_mkt', FALSE),
    ('c252', NULL, 'co_code', FALSE),
    ('c20', '31', 'comp_code', FALSE),
    ('c98', NULL, 'con_interest_accr', FALSE),
    ('c21', NULL, 'condition_group', FALSE),
    ('c84', NULL, 'contingent_bal_dr', FALSE),
    ('c20', '26', 'cost_bonds', FALSE),
    ('c189', NULL, 'cr2_adj_amount', FALSE),
    ('c188', NULL, 'cr_adj_amount', FALSE),
    ('c214', NULL, 'credit_chk_condition', FALSE),
    ('c9', NULL, 'currency_market', FALSE),
    ('c1', NULL, 'customer', FALSE),
    ('c34', NULL, 'date_last_cr_bank', FALSE),
    ('c28', NULL, 'date_last_cr_cust', FALSE),
    ('c40', NULL, 'date_last_dr_auto', FALSE),
    ('c253', NULL, 'dept_code', FALSE),
    ('c221', NULL, 'emergency_reason', FALSE),
    ('c20', '29', 'eval_bal', FALSE),
    ('c20', '6', 'expiry_date', FALSE),
    ('c219', NULL, 'fa_status', FALSE),
    ('c191', NULL, 'field', FALSE),
    ('c121', NULL, 'from_date', FALSE),
    ('c172', NULL, 'fwd_entry_hold', FALSE),
    ('c20', '7', 'hold_amount', FALSE),
    ('c20', '9', 'hold_user', FALSE),
    ('c141', NULL, 'hvt_flag', FALSE),
    ('c177', NULL, 'ic_charge_id', FALSE),
    ('c180', NULL, 'ic_lst_prod_cap', FALSE),
    ('c178', NULL, 'ic_next_cap_date', FALSE),
    ('c179', NULL, 'ic_product', FALSE),
    ('c128', NULL, 'ica_distrib_ratio', FALSE),
    ('c130', NULL, 'ica_distrib_type', FALSE),
    ('c127', NULL, 'ica_main_account', FALSE),
    ('c129', NULL, 'ica_main_acct_ind', FALSE),
    ('c133', NULL, 'ica_new_main_acc', FALSE),
    ('c131', NULL, 'ica_post_interest', FALSE),
    ('c134', NULL, 'ica_start_date', FALSE),
    ('c249', NULL, 'inputter', TRUE),
    ('c74', NULL, 'int_liqu_acct', FALSE),
    ('c73', NULL, 'int_liqu_type', FALSE),
    ('c17', NULL, 'int_no_booking', FALSE),
    ('c95', NULL, 'interest_ccy', FALSE),
    ('c16', NULL, 'interest_comp_acct', FALSE),
    ('c15', NULL, 'interest_liqu_acct', FALSE),
    ('c105', NULL, 'joint_holder', FALSE),
    ('c109', NULL, 'ledg_reco_with', FALSE),
    ('c216', NULL, 'limit_key', FALSE),
    ('c218', NULL, 'limit_proc_type', FALSE),
    ('c139', NULL, 'liquidation_mode', FALSE),
    ('c122', NULL, 'locked_amount', FALSE),
    ('c161', NULL, 'master_account', FALSE),
    ('c20', '25', 'matur_date', FALSE),
    ('c160', NULL, 'max_sub_account', FALSE),
    ('c208', NULL, 'multi_currency', FALSE),
    ('c196', NULL, 'mv_alert_res4', FALSE),
    ('c195', NULL, 'mv_alert_res5', FALSE),
    ('c194', NULL, 'mv_alert_res6', FALSE),
    ('c20', '32', 'nab_flag', FALSE),
    ('c20', '24', 'no_of_bonds', FALSE),
    ('c20', '11', 'old_account', FALSE),
    ('c25', NULL, 'online_actual_bal', FALSE),
    ('c26', NULL, 'online_cleared_bal', FALSE),
    ('c149', NULL, 'open_available_bal', FALSE),
    ('c85', NULL, 'open_category', FALSE),
    ('c24', NULL, 'open_cleared_bal', FALSE),
    ('c86', NULL, 'open_val_dated_bal', FALSE),
    ('c192', NULL, 'operand', FALSE),
    ('c118', NULL, 'orig_ccy_payment', FALSE),
    ('c12', NULL, 'other_officer', FALSE),
    ('c111', NULL, 'our_ext_acct_no', FALSE),
    ('c140', NULL, 'overdue_status', FALSE),
    ('c246', NULL, 'override', FALSE),
    ('c207', NULL, 'parent_account', FALSE),
    ('c113', NULL, 'pending_id', FALSE),
    ('c101', NULL, 'premium_type', FALSE),
    ('c20', '27', 'prev_bal', FALSE),
    ('c20', '28', 'price_eval', FALSE),
    ('c14', NULL, 'reconcile_acct', FALSE),
    ('c247', NULL, 'record_status', FALSE),
    ('c217', NULL, 'reducing_limit', FALSE),
    ('c202', NULL, 'ref_data_item', FALSE),
    ('c203', NULL, 'ref_data_value', FALSE),
    ('c106', NULL, 'relation_code', FALSE),
    ('c245', NULL, 'reserved_1', FALSE),
    ('c236', NULL, 'reserved_10', FALSE),
    ('c235', NULL, 'reserved_11', FALSE),
    ('c231', NULL, 'reserved_15', FALSE),
    ('c244', NULL, 'reserved_2', FALSE),
    ('c225', NULL, 'reserved_21', FALSE),
    ('c237', NULL, 'reserved_9', FALSE),
    ('c222', NULL, 'risk_stage', FALSE),
    ('c206', NULL, 'sam_id_hist', FALSE),
    ('c142', NULL, 'single_limit', FALSE),
    ('c20', '13', 'start_nab', FALSE),
    ('c20', '16', 'stmt_date', FALSE),
    ('c110', NULL, 'stmt_reco_with', FALSE),
    ('c115', NULL, 'stock_control_type', FALSE),
    ('c126', NULL, 'tax_at_settle', FALSE),
    ('c125', NULL, 'tax_suspend', FALSE),
    ('c114', NULL, 'total_pending', FALSE),
    ('c20', '21', 'under_age_interest_date', FALSE),
    ('c20', '20', 'under_age_no_of_months', FALSE),
    ('c82', NULL, 'value_dated_bal', FALSE),
    ('c20', '2', 'version_name', FALSE),
    ('c27', NULL, 'working_balance', FALSE);

FLUSH;

-- Decodes t24_account_blob's base64 text back to the original XML
CREATE VIEW t24_account_blob_text AS
SELECT recid,
       convert_from(decode(blobrecord, 'base64'), 'UTF8') AS xmlrecord
  FROM t24_account_blob
 WHERE blobrecord IS NOT NULL
   AND blobrecord NOT IN ('__debezium_unavailable_value',
                          'X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ==');

-- Flattens XMLRECORD into named columns via regex - RisingWave has no
-- XML functions. Three stages:
--   unpivoted - one row per XML tag: (recid, field, position, value)
--   resolved  - joins lookup_metadata to get each tag's real column name
--   final SELECT - pivots back to wide columns via MAX(CASE WHEN ...)
-- MATERIALIZED so it stays live (a VIEW would re-run this every query).
CREATE MATERIALIZED VIEW t24_account_columns AS
WITH unpivoted AS (
    SELECT recid, m[1] AS field, m[2] AS position, m[3] AS value
      FROM t24_account,
           LATERAL (SELECT regexp_matches(xmlrecord, '<(c\d+)(?:\s+m="(\d+)")?>([^<]*)</\1>', 'g') AS m) AS t
),
resolved AS (
    SELECT
        u.recid,
        COALESCE(
            exact.resolved_name_en,
            CASE WHEN base.is_multivalue
                 THEN base.resolved_name_en || '_' || LPAD(COALESCE(u.position, '1'), 2, '0')
                 ELSE base.resolved_name_en
            END
        ) AS column_name,
        u.value
    FROM unpivoted u
    LEFT JOIN lookup_metadata exact
           ON exact.field_index = u.field AND exact.m_index = u.position
    LEFT JOIN lookup_metadata base
           ON base.field_index = u.field AND base.m_index IS NULL
)
SELECT
    recid,
    MAX(CASE WHEN column_name = 'customer' THEN value END) AS customer,
    MAX(CASE WHEN column_name = 'category' THEN value END) AS category,
    MAX(CASE WHEN column_name = 'account_title_1' THEN value END) AS account_title_1,
    MAX(CASE WHEN column_name = 'short_title' THEN value END) AS short_title,
    MAX(CASE WHEN column_name = 'position_type' THEN value END) AS position_type,
    MAX(CASE WHEN column_name = 'currency' THEN value END) AS currency,
    MAX(CASE WHEN column_name = 'currency_market' THEN value END) AS currency_market,
    MAX(CASE WHEN column_name = 'account_officer' THEN value END) AS account_officer,
    MAX(CASE WHEN column_name = 'activyty' THEN value END) AS activyty,
    MAX(CASE WHEN column_name = 'version_name' THEN value END) AS version_name,
    MAX(CASE WHEN column_name = 'debit_or_credit' THEN value END) AS debit_or_credit,
    MAX(CASE WHEN column_name = 'arabic_title' THEN value END) AS arabic_title,
    MAX(CASE WHEN column_name = 'nab_flag' THEN value END) AS nab_flag,
    MAX(CASE WHEN column_name = 'condition_group' THEN value END) AS condition_group,
    MAX(CASE WHEN column_name = 'open_actual_bal' THEN value END) AS open_actual_bal,
    MAX(CASE WHEN column_name = 'open_cleared_bal' THEN value END) AS open_cleared_bal,
    MAX(CASE WHEN column_name = 'online_actual_bal' THEN value END) AS online_actual_bal,
    MAX(CASE WHEN column_name = 'online_cleared_bal' THEN value END) AS online_cleared_bal,
    MAX(CASE WHEN column_name = 'working_balance' THEN value END) AS working_balance,
    MAX(CASE WHEN column_name = 'date_last_cr_cust' THEN value END) AS date_last_cr_cust,
    MAX(CASE WHEN column_name = 'amnt_last_cr_cust' THEN value END) AS amnt_last_cr_cust,
    MAX(CASE WHEN column_name = 'tran_last_cr_cust' THEN value END) AS tran_last_cr_cust,
    MAX(CASE WHEN column_name = 'date_last_cr_auto' THEN value END) AS date_last_cr_auto,
    MAX(CASE WHEN column_name = 'amnt_last_cr_auto' THEN value END) AS amnt_last_cr_auto,
    MAX(CASE WHEN column_name = 'tran_last_cr_auto' THEN value END) AS tran_last_cr_auto,
    MAX(CASE WHEN column_name = 'date_last_cr_bank' THEN value END) AS date_last_cr_bank,
    MAX(CASE WHEN column_name = 'amnt_last_cr_bank' THEN value END) AS amnt_last_cr_bank,
    MAX(CASE WHEN column_name = 'tran_last_cr_bank' THEN value END) AS tran_last_cr_bank,
    MAX(CASE WHEN column_name = 'date_last_dr_cust' THEN value END) AS date_last_dr_cust,
    MAX(CASE WHEN column_name = 'amnt_last_dr_cust' THEN value END) AS amnt_last_dr_cust,
    MAX(CASE WHEN column_name = 'tran_last_dr_cust' THEN value END) AS tran_last_dr_cust,
    MAX(CASE WHEN column_name = 'date_last_dr_auto' THEN value END) AS date_last_dr_auto,
    MAX(CASE WHEN column_name = 'amnt_last_dr_auto' THEN value END) AS amnt_last_dr_auto,
    MAX(CASE WHEN column_name = 'tran_last_dr_auto' THEN value END) AS tran_last_dr_auto,
    MAX(CASE WHEN column_name = 'date_last_dr_bank' THEN value END) AS date_last_dr_bank,
    MAX(CASE WHEN column_name = 'amnt_last_dr_bank' THEN value END) AS amnt_last_dr_bank,
    MAX(CASE WHEN column_name = 'tran_last_dr_bank' THEN value END) AS tran_last_dr_bank,
    MAX(CASE WHEN column_name = 'cap_date_charge_01' THEN value END) AS cap_date_charge_01,
    MAX(CASE WHEN column_name = 'cap_date_charge_02' THEN value END) AS cap_date_charge_02,
    MAX(CASE WHEN column_name = 'cap_date_charge_03' THEN value END) AS cap_date_charge_03,
    MAX(CASE WHEN column_name = 'cap_date_charge_04' THEN value END) AS cap_date_charge_04,
    MAX(CASE WHEN column_name = 'cap_date_charge_05' THEN value END) AS cap_date_charge_05,
    MAX(CASE WHEN column_name = 'cap_date_charge_06' THEN value END) AS cap_date_charge_06,
    MAX(CASE WHEN column_name = 'cap_date_charge_07' THEN value END) AS cap_date_charge_07,
    MAX(CASE WHEN column_name = 'cap_date_charge_08' THEN value END) AS cap_date_charge_08,
    MAX(CASE WHEN column_name = 'cap_date_charge_09' THEN value END) AS cap_date_charge_09,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_01' THEN value END) AS cap_date_cr_int_01,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_02' THEN value END) AS cap_date_cr_int_02,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_03' THEN value END) AS cap_date_cr_int_03,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_04' THEN value END) AS cap_date_cr_int_04,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_05' THEN value END) AS cap_date_cr_int_05,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_06' THEN value END) AS cap_date_cr_int_06,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_07' THEN value END) AS cap_date_cr_int_07,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_08' THEN value END) AS cap_date_cr_int_08,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_09' THEN value END) AS cap_date_cr_int_09,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_10' THEN value END) AS cap_date_cr_int_10,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_11' THEN value END) AS cap_date_cr_int_11,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_12' THEN value END) AS cap_date_cr_int_12,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_13' THEN value END) AS cap_date_cr_int_13,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_14' THEN value END) AS cap_date_cr_int_14,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_15' THEN value END) AS cap_date_cr_int_15,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_16' THEN value END) AS cap_date_cr_int_16,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_17' THEN value END) AS cap_date_cr_int_17,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_18' THEN value END) AS cap_date_cr_int_18,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_19' THEN value END) AS cap_date_cr_int_19,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_20' THEN value END) AS cap_date_cr_int_20,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_21' THEN value END) AS cap_date_cr_int_21,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_22' THEN value END) AS cap_date_cr_int_22,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_23' THEN value END) AS cap_date_cr_int_23,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_01' THEN value END) AS cap_date_c2_int_01,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_02' THEN value END) AS cap_date_c2_int_02,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_03' THEN value END) AS cap_date_c2_int_03,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_04' THEN value END) AS cap_date_c2_int_04,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_05' THEN value END) AS cap_date_c2_int_05,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_06' THEN value END) AS cap_date_c2_int_06,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_07' THEN value END) AS cap_date_c2_int_07,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_08' THEN value END) AS cap_date_c2_int_08,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_09' THEN value END) AS cap_date_c2_int_09,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_10' THEN value END) AS cap_date_c2_int_10,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_11' THEN value END) AS cap_date_c2_int_11,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_12' THEN value END) AS cap_date_c2_int_12,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_13' THEN value END) AS cap_date_c2_int_13,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_14' THEN value END) AS cap_date_c2_int_14,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_15' THEN value END) AS cap_date_c2_int_15,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_16' THEN value END) AS cap_date_c2_int_16,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_17' THEN value END) AS cap_date_c2_int_17,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_18' THEN value END) AS cap_date_c2_int_18,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_19' THEN value END) AS cap_date_c2_int_19,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_20' THEN value END) AS cap_date_c2_int_20,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_21' THEN value END) AS cap_date_c2_int_21,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_22' THEN value END) AS cap_date_c2_int_22,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_23' THEN value END) AS cap_date_c2_int_23,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_01' THEN value END) AS cap_date_dr_int_01,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_02' THEN value END) AS cap_date_dr_int_02,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_03' THEN value END) AS cap_date_dr_int_03,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_04' THEN value END) AS cap_date_dr_int_04,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_05' THEN value END) AS cap_date_dr_int_05,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_06' THEN value END) AS cap_date_dr_int_06,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_07' THEN value END) AS cap_date_dr_int_07,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_08' THEN value END) AS cap_date_dr_int_08,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_09' THEN value END) AS cap_date_dr_int_09,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_10' THEN value END) AS cap_date_dr_int_10,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_11' THEN value END) AS cap_date_dr_int_11,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_12' THEN value END) AS cap_date_dr_int_12,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_13' THEN value END) AS cap_date_dr_int_13,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_14' THEN value END) AS cap_date_dr_int_14,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_15' THEN value END) AS cap_date_dr_int_15,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_16' THEN value END) AS cap_date_dr_int_16,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_17' THEN value END) AS cap_date_dr_int_17,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_18' THEN value END) AS cap_date_dr_int_18,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_19' THEN value END) AS cap_date_dr_int_19,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_20' THEN value END) AS cap_date_dr_int_20,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_21' THEN value END) AS cap_date_dr_int_21,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_22' THEN value END) AS cap_date_dr_int_22,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_23' THEN value END) AS cap_date_dr_int_23,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_01' THEN value END) AS cap_date_d2_int_01,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_02' THEN value END) AS cap_date_d2_int_02,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_03' THEN value END) AS cap_date_d2_int_03,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_04' THEN value END) AS cap_date_d2_int_04,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_05' THEN value END) AS cap_date_d2_int_05,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_06' THEN value END) AS cap_date_d2_int_06,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_07' THEN value END) AS cap_date_d2_int_07,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_08' THEN value END) AS cap_date_d2_int_08,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_09' THEN value END) AS cap_date_d2_int_09,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_10' THEN value END) AS cap_date_d2_int_10,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_11' THEN value END) AS cap_date_d2_int_11,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_12' THEN value END) AS cap_date_d2_int_12,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_13' THEN value END) AS cap_date_d2_int_13,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_14' THEN value END) AS cap_date_d2_int_14,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_15' THEN value END) AS cap_date_d2_int_15,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_16' THEN value END) AS cap_date_d2_int_16,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_17' THEN value END) AS cap_date_d2_int_17,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_18' THEN value END) AS cap_date_d2_int_18,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_19' THEN value END) AS cap_date_d2_int_19,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_20' THEN value END) AS cap_date_d2_int_20,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_21' THEN value END) AS cap_date_d2_int_21,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_22' THEN value END) AS cap_date_d2_int_22,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_23' THEN value END) AS cap_date_d2_int_23,
    MAX(CASE WHEN column_name = 'passbook' THEN value END) AS passbook,
    MAX(CASE WHEN column_name = 'start_year_bal' THEN value END) AS start_year_bal,
    MAX(CASE WHEN column_name = 'opening_date' THEN value END) AS opening_date,
    MAX(CASE WHEN column_name = 'open_category' THEN value END) AS open_category,
    MAX(CASE WHEN column_name = 'charge_ccy' THEN value END) AS charge_ccy,
    MAX(CASE WHEN column_name = 'charge_mkt' THEN value END) AS charge_mkt,
    MAX(CASE WHEN column_name = 'interest_ccy' THEN value END) AS interest_ccy,
    MAX(CASE WHEN column_name = 'interest_mkt' THEN value END) AS interest_mkt,
    MAX(CASE WHEN column_name = 'alt_acct_type_01' THEN value END) AS alt_acct_type_01,
    MAX(CASE WHEN column_name = 'alt_acct_type_02' THEN value END) AS alt_acct_type_02,
    MAX(CASE WHEN column_name = 'alt_acct_type_03' THEN value END) AS alt_acct_type_03,
    MAX(CASE WHEN column_name = 'alt_acct_id_01' THEN value END) AS alt_acct_id_01,
    MAX(CASE WHEN column_name = 'alt_acct_id_02' THEN value END) AS alt_acct_id_02,
    MAX(CASE WHEN column_name = 'allow_netting' THEN value END) AS allow_netting,
    MAX(CASE WHEN column_name = 'from_date' THEN value END) AS from_date,
    MAX(CASE WHEN column_name = 'locked_amount' THEN value END) AS locked_amount,
    MAX(CASE WHEN column_name = 'hvt_flag' THEN value END) AS hvt_flag,
    MAX(CASE WHEN column_name = 'open_available_bal' THEN value END) AS open_available_bal,
    MAX(CASE WHEN column_name = 'date_last_update' THEN value END) AS date_last_update,
    MAX(CASE WHEN column_name = 'curr_no' THEN value END) AS curr_no,
    MAX(CASE WHEN column_name = 'inputter_01' THEN value END) AS inputter_01,
    MAX(CASE WHEN column_name = 'inputter_02' THEN value END) AS inputter_02,
    MAX(CASE WHEN column_name = 'date_time_01' THEN value END) AS date_time_01,
    MAX(CASE WHEN column_name = 'date_time_02' THEN value END) AS date_time_02,
    MAX(CASE WHEN column_name = 'authoriser' THEN value END) AS authoriser,
    MAX(CASE WHEN column_name = 'co_code' THEN value END) AS co_code,
    MAX(CASE WHEN column_name = 'dept_code' THEN value END) AS dept_code
FROM resolved
GROUP BY recid;


-- ===========================================================================
-- t24_account_blob_columns - the BLOB-path twin of t24_account_columns above.
-- Generated programmatically (2 substitutions: MV name, FROM source) from
-- the block starting at "CREATE MATERIALIZED VIEW t24_account_columns AS" -
-- NOT hand-transcribed, to guarantee the 166 pivot lines can never drift
-- from their XMLTYPE-path counterpart by copy error. Regenerate this
-- block the same way whenever the XMLTYPE MV's field mappings change.
-- ===========================================================================
CREATE MATERIALIZED VIEW t24_account_blob_columns AS
WITH unpivoted AS (
    SELECT recid, m[1] AS field, m[2] AS position, m[3] AS value
      FROM t24_account_blob_text,
           LATERAL (SELECT regexp_matches(xmlrecord, '<(c\d+)(?:\s+m="(\d+)")?>([^<]*)</\1>', 'g') AS m) AS t
),
resolved AS (
    SELECT
        u.recid,
        COALESCE(
            exact.resolved_name_en,
            CASE WHEN base.is_multivalue
                 THEN base.resolved_name_en || '_' || LPAD(COALESCE(u.position, '1'), 2, '0')
                 ELSE base.resolved_name_en
            END
        ) AS column_name,
        u.value
    FROM unpivoted u
    LEFT JOIN lookup_metadata exact
           ON exact.field_index = u.field AND exact.m_index = u.position
    LEFT JOIN lookup_metadata base
           ON base.field_index = u.field AND base.m_index IS NULL
)
SELECT
    recid,
    MAX(CASE WHEN column_name = 'customer' THEN value END) AS customer,
    MAX(CASE WHEN column_name = 'category' THEN value END) AS category,
    MAX(CASE WHEN column_name = 'account_title_1' THEN value END) AS account_title_1,
    MAX(CASE WHEN column_name = 'short_title' THEN value END) AS short_title,
    MAX(CASE WHEN column_name = 'position_type' THEN value END) AS position_type,
    MAX(CASE WHEN column_name = 'currency' THEN value END) AS currency,
    MAX(CASE WHEN column_name = 'currency_market' THEN value END) AS currency_market,
    MAX(CASE WHEN column_name = 'account_officer' THEN value END) AS account_officer,
    MAX(CASE WHEN column_name = 'activyty' THEN value END) AS activyty,
    MAX(CASE WHEN column_name = 'version_name' THEN value END) AS version_name,
    MAX(CASE WHEN column_name = 'debit_or_credit' THEN value END) AS debit_or_credit,
    MAX(CASE WHEN column_name = 'arabic_title' THEN value END) AS arabic_title,
    MAX(CASE WHEN column_name = 'nab_flag' THEN value END) AS nab_flag,
    MAX(CASE WHEN column_name = 'condition_group' THEN value END) AS condition_group,
    MAX(CASE WHEN column_name = 'open_actual_bal' THEN value END) AS open_actual_bal,
    MAX(CASE WHEN column_name = 'open_cleared_bal' THEN value END) AS open_cleared_bal,
    MAX(CASE WHEN column_name = 'online_actual_bal' THEN value END) AS online_actual_bal,
    MAX(CASE WHEN column_name = 'online_cleared_bal' THEN value END) AS online_cleared_bal,
    MAX(CASE WHEN column_name = 'working_balance' THEN value END) AS working_balance,
    MAX(CASE WHEN column_name = 'date_last_cr_cust' THEN value END) AS date_last_cr_cust,
    MAX(CASE WHEN column_name = 'amnt_last_cr_cust' THEN value END) AS amnt_last_cr_cust,
    MAX(CASE WHEN column_name = 'tran_last_cr_cust' THEN value END) AS tran_last_cr_cust,
    MAX(CASE WHEN column_name = 'date_last_cr_auto' THEN value END) AS date_last_cr_auto,
    MAX(CASE WHEN column_name = 'amnt_last_cr_auto' THEN value END) AS amnt_last_cr_auto,
    MAX(CASE WHEN column_name = 'tran_last_cr_auto' THEN value END) AS tran_last_cr_auto,
    MAX(CASE WHEN column_name = 'date_last_cr_bank' THEN value END) AS date_last_cr_bank,
    MAX(CASE WHEN column_name = 'amnt_last_cr_bank' THEN value END) AS amnt_last_cr_bank,
    MAX(CASE WHEN column_name = 'tran_last_cr_bank' THEN value END) AS tran_last_cr_bank,
    MAX(CASE WHEN column_name = 'date_last_dr_cust' THEN value END) AS date_last_dr_cust,
    MAX(CASE WHEN column_name = 'amnt_last_dr_cust' THEN value END) AS amnt_last_dr_cust,
    MAX(CASE WHEN column_name = 'tran_last_dr_cust' THEN value END) AS tran_last_dr_cust,
    MAX(CASE WHEN column_name = 'date_last_dr_auto' THEN value END) AS date_last_dr_auto,
    MAX(CASE WHEN column_name = 'amnt_last_dr_auto' THEN value END) AS amnt_last_dr_auto,
    MAX(CASE WHEN column_name = 'tran_last_dr_auto' THEN value END) AS tran_last_dr_auto,
    MAX(CASE WHEN column_name = 'date_last_dr_bank' THEN value END) AS date_last_dr_bank,
    MAX(CASE WHEN column_name = 'amnt_last_dr_bank' THEN value END) AS amnt_last_dr_bank,
    MAX(CASE WHEN column_name = 'tran_last_dr_bank' THEN value END) AS tran_last_dr_bank,
    MAX(CASE WHEN column_name = 'cap_date_charge_01' THEN value END) AS cap_date_charge_01,
    MAX(CASE WHEN column_name = 'cap_date_charge_02' THEN value END) AS cap_date_charge_02,
    MAX(CASE WHEN column_name = 'cap_date_charge_03' THEN value END) AS cap_date_charge_03,
    MAX(CASE WHEN column_name = 'cap_date_charge_04' THEN value END) AS cap_date_charge_04,
    MAX(CASE WHEN column_name = 'cap_date_charge_05' THEN value END) AS cap_date_charge_05,
    MAX(CASE WHEN column_name = 'cap_date_charge_06' THEN value END) AS cap_date_charge_06,
    MAX(CASE WHEN column_name = 'cap_date_charge_07' THEN value END) AS cap_date_charge_07,
    MAX(CASE WHEN column_name = 'cap_date_charge_08' THEN value END) AS cap_date_charge_08,
    MAX(CASE WHEN column_name = 'cap_date_charge_09' THEN value END) AS cap_date_charge_09,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_01' THEN value END) AS cap_date_cr_int_01,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_02' THEN value END) AS cap_date_cr_int_02,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_03' THEN value END) AS cap_date_cr_int_03,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_04' THEN value END) AS cap_date_cr_int_04,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_05' THEN value END) AS cap_date_cr_int_05,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_06' THEN value END) AS cap_date_cr_int_06,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_07' THEN value END) AS cap_date_cr_int_07,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_08' THEN value END) AS cap_date_cr_int_08,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_09' THEN value END) AS cap_date_cr_int_09,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_10' THEN value END) AS cap_date_cr_int_10,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_11' THEN value END) AS cap_date_cr_int_11,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_12' THEN value END) AS cap_date_cr_int_12,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_13' THEN value END) AS cap_date_cr_int_13,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_14' THEN value END) AS cap_date_cr_int_14,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_15' THEN value END) AS cap_date_cr_int_15,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_16' THEN value END) AS cap_date_cr_int_16,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_17' THEN value END) AS cap_date_cr_int_17,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_18' THEN value END) AS cap_date_cr_int_18,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_19' THEN value END) AS cap_date_cr_int_19,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_20' THEN value END) AS cap_date_cr_int_20,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_21' THEN value END) AS cap_date_cr_int_21,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_22' THEN value END) AS cap_date_cr_int_22,
    MAX(CASE WHEN column_name = 'cap_date_cr_int_23' THEN value END) AS cap_date_cr_int_23,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_01' THEN value END) AS cap_date_c2_int_01,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_02' THEN value END) AS cap_date_c2_int_02,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_03' THEN value END) AS cap_date_c2_int_03,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_04' THEN value END) AS cap_date_c2_int_04,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_05' THEN value END) AS cap_date_c2_int_05,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_06' THEN value END) AS cap_date_c2_int_06,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_07' THEN value END) AS cap_date_c2_int_07,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_08' THEN value END) AS cap_date_c2_int_08,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_09' THEN value END) AS cap_date_c2_int_09,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_10' THEN value END) AS cap_date_c2_int_10,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_11' THEN value END) AS cap_date_c2_int_11,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_12' THEN value END) AS cap_date_c2_int_12,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_13' THEN value END) AS cap_date_c2_int_13,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_14' THEN value END) AS cap_date_c2_int_14,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_15' THEN value END) AS cap_date_c2_int_15,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_16' THEN value END) AS cap_date_c2_int_16,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_17' THEN value END) AS cap_date_c2_int_17,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_18' THEN value END) AS cap_date_c2_int_18,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_19' THEN value END) AS cap_date_c2_int_19,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_20' THEN value END) AS cap_date_c2_int_20,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_21' THEN value END) AS cap_date_c2_int_21,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_22' THEN value END) AS cap_date_c2_int_22,
    MAX(CASE WHEN column_name = 'cap_date_c2_int_23' THEN value END) AS cap_date_c2_int_23,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_01' THEN value END) AS cap_date_dr_int_01,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_02' THEN value END) AS cap_date_dr_int_02,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_03' THEN value END) AS cap_date_dr_int_03,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_04' THEN value END) AS cap_date_dr_int_04,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_05' THEN value END) AS cap_date_dr_int_05,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_06' THEN value END) AS cap_date_dr_int_06,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_07' THEN value END) AS cap_date_dr_int_07,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_08' THEN value END) AS cap_date_dr_int_08,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_09' THEN value END) AS cap_date_dr_int_09,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_10' THEN value END) AS cap_date_dr_int_10,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_11' THEN value END) AS cap_date_dr_int_11,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_12' THEN value END) AS cap_date_dr_int_12,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_13' THEN value END) AS cap_date_dr_int_13,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_14' THEN value END) AS cap_date_dr_int_14,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_15' THEN value END) AS cap_date_dr_int_15,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_16' THEN value END) AS cap_date_dr_int_16,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_17' THEN value END) AS cap_date_dr_int_17,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_18' THEN value END) AS cap_date_dr_int_18,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_19' THEN value END) AS cap_date_dr_int_19,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_20' THEN value END) AS cap_date_dr_int_20,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_21' THEN value END) AS cap_date_dr_int_21,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_22' THEN value END) AS cap_date_dr_int_22,
    MAX(CASE WHEN column_name = 'cap_date_dr_int_23' THEN value END) AS cap_date_dr_int_23,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_01' THEN value END) AS cap_date_d2_int_01,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_02' THEN value END) AS cap_date_d2_int_02,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_03' THEN value END) AS cap_date_d2_int_03,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_04' THEN value END) AS cap_date_d2_int_04,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_05' THEN value END) AS cap_date_d2_int_05,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_06' THEN value END) AS cap_date_d2_int_06,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_07' THEN value END) AS cap_date_d2_int_07,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_08' THEN value END) AS cap_date_d2_int_08,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_09' THEN value END) AS cap_date_d2_int_09,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_10' THEN value END) AS cap_date_d2_int_10,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_11' THEN value END) AS cap_date_d2_int_11,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_12' THEN value END) AS cap_date_d2_int_12,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_13' THEN value END) AS cap_date_d2_int_13,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_14' THEN value END) AS cap_date_d2_int_14,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_15' THEN value END) AS cap_date_d2_int_15,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_16' THEN value END) AS cap_date_d2_int_16,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_17' THEN value END) AS cap_date_d2_int_17,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_18' THEN value END) AS cap_date_d2_int_18,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_19' THEN value END) AS cap_date_d2_int_19,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_20' THEN value END) AS cap_date_d2_int_20,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_21' THEN value END) AS cap_date_d2_int_21,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_22' THEN value END) AS cap_date_d2_int_22,
    MAX(CASE WHEN column_name = 'cap_date_d2_int_23' THEN value END) AS cap_date_d2_int_23,
    MAX(CASE WHEN column_name = 'passbook' THEN value END) AS passbook,
    MAX(CASE WHEN column_name = 'start_year_bal' THEN value END) AS start_year_bal,
    MAX(CASE WHEN column_name = 'opening_date' THEN value END) AS opening_date,
    MAX(CASE WHEN column_name = 'open_category' THEN value END) AS open_category,
    MAX(CASE WHEN column_name = 'charge_ccy' THEN value END) AS charge_ccy,
    MAX(CASE WHEN column_name = 'charge_mkt' THEN value END) AS charge_mkt,
    MAX(CASE WHEN column_name = 'interest_ccy' THEN value END) AS interest_ccy,
    MAX(CASE WHEN column_name = 'interest_mkt' THEN value END) AS interest_mkt,
    MAX(CASE WHEN column_name = 'alt_acct_type_01' THEN value END) AS alt_acct_type_01,
    MAX(CASE WHEN column_name = 'alt_acct_type_02' THEN value END) AS alt_acct_type_02,
    MAX(CASE WHEN column_name = 'alt_acct_type_03' THEN value END) AS alt_acct_type_03,
    MAX(CASE WHEN column_name = 'alt_acct_id_01' THEN value END) AS alt_acct_id_01,
    MAX(CASE WHEN column_name = 'alt_acct_id_02' THEN value END) AS alt_acct_id_02,
    MAX(CASE WHEN column_name = 'allow_netting' THEN value END) AS allow_netting,
    MAX(CASE WHEN column_name = 'from_date' THEN value END) AS from_date,
    MAX(CASE WHEN column_name = 'locked_amount' THEN value END) AS locked_amount,
    MAX(CASE WHEN column_name = 'hvt_flag' THEN value END) AS hvt_flag,
    MAX(CASE WHEN column_name = 'open_available_bal' THEN value END) AS open_available_bal,
    MAX(CASE WHEN column_name = 'date_last_update' THEN value END) AS date_last_update,
    MAX(CASE WHEN column_name = 'curr_no' THEN value END) AS curr_no,
    MAX(CASE WHEN column_name = 'inputter_01' THEN value END) AS inputter_01,
    MAX(CASE WHEN column_name = 'inputter_02' THEN value END) AS inputter_02,
    MAX(CASE WHEN column_name = 'date_time_01' THEN value END) AS date_time_01,
    MAX(CASE WHEN column_name = 'date_time_02' THEN value END) AS date_time_02,
    MAX(CASE WHEN column_name = 'authoriser' THEN value END) AS authoriser,
    MAX(CASE WHEN column_name = 'co_code' THEN value END) AS co_code,
    MAX(CASE WHEN column_name = 'dept_code' THEN value END) AS dept_code
FROM resolved
GROUP BY recid;



-- ===========================================================================
-- Before-images - reconstructed, because Oracle cannot supply them.
-- ===========================================================================

CREATE MATERIALIZED VIEW t24_account_before AS
WITH ev AS (
    SELECT
        (source->>'scn')::BIGINT AS scn,
        COALESCE("after"->>'RECID', "before"->>'RECID') AS recid,
        op,
        -- Guarded: on an UPDATE that doesn't touch the LOB, Debezium emits
        -- the placeholder in `after` too - never let it become a before_xml.
        CASE WHEN "after"->>'XMLRECORD' <> '__debezium_unavailable_value'
             THEN "after"->>'XMLRECORD' END AS after_xml
    FROM t24_account_events
    WHERE op IS NOT NULL   -- skip tombstones
)
SELECT scn, recid, op, after_xml,
       lag(after_xml) OVER (PARTITION BY recid ORDER BY scn) AS before_xml
FROM ev;

-- The BLOB twin. Identical shape; the only difference is that BLOBRECORD
-- arrives base64-encoded, so both sides decode - same guard and decode as
-- t24_account_blob_text above, applied BEFORE lag() so the placeholder can
-- never reach decode() ('_' is not valid base64 and would error).
CREATE MATERIALIZED VIEW t24_account_blob_before AS
WITH ev AS (
    SELECT
        (source->>'scn')::BIGINT AS scn,
        COALESCE("after"->>'RECID', "before"->>'RECID') AS recid,
        op,
        CASE WHEN "after"->>'BLOBRECORD' NOT IN
                  ('__debezium_unavailable_value',
                   'X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ==')
             THEN "after"->>'BLOBRECORD' END AS after_b64
    FROM t24_account_blob_events
    WHERE op IS NOT NULL
),
lagged AS (
    SELECT scn, recid, op, after_b64,
           lag(after_b64) OVER (PARTITION BY recid ORDER BY scn) AS prev_b64
    FROM ev
)
SELECT scn, recid, op,
       CASE WHEN after_b64 IS NOT NULL
            THEN convert_from(decode(after_b64, 'base64'), 'UTF8') END AS after_xml,
       CASE WHEN prev_b64 IS NOT NULL
            THEN convert_from(decode(prev_b64, 'base64'), 'UTF8') END AS before_xml
FROM lagged;
