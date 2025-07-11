-- Процедура заполнения витрины 101 формы
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_records_processed INTEGER := 0;
    v_status VARCHAR(20) := 'completed';
    v_error_message TEXT := NULL;
    v_report_start_date DATE;
    v_report_end_date DATE;
    v_log_key VARCHAR(100);
BEGIN
    -- Определяем отчетный период (предыдущий месяц)
    v_report_start_date := DATE_TRUNC('month', i_OnDate - INTERVAL '1 month')::DATE;
    v_report_end_date := (DATE_TRUNC('month', i_OnDate) - INTERVAL '1 day')::DATE;
    
    -- Создаем уникальный ключ для лога
    v_log_key := 'period_' || v_report_start_date || '_' || v_report_end_date || '_run_' || TO_CHAR(v_start_time, 'YYYYMMDD_HH24MISS');
    
    -- Удаляем старые логи для этого отчетного периода
    DELETE FROM logs.etl_logs
    WHERE table_name = 'dm.dm_f101_round_f'
      AND error_message LIKE 'period_' || v_report_start_date || '_' || v_report_end_date || '%';
    
    -- Записываем новый лог
    INSERT INTO logs.etl_logs (
        table_name,
        start_time,
        status,
        error_message
    ) VALUES (
        'dm.dm_f101_round_f',
        v_start_time,
        'started',
        v_log_key
    );
    
    -- Удаляем старые данные за этот отчетный период
    DELETE FROM dm.dm_f101_round_f 
    WHERE from_date = v_report_start_date 
      AND to_date = v_report_end_date;
    
    BEGIN
        -- Вставляем новые данные
        INSERT INTO dm.dm_f101_round_f (
            from_date,
            to_date,
            chapter,
            ledger_account,
            characteristic,
            balance_in_rub,
            balance_in_val,
            balance_in_total,
            turn_deb_rub,
            turn_deb_val,
            turn_deb_total,
            turn_cre_rub,
            turn_cre_val,
            turn_cre_total,
            balance_out_rub,
            balance_out_val,
            balance_out_total
        )
        WITH 
        -- Получаем балансовые счета второго порядка (первые 5 символов)
        ledger_accounts AS (
            SELECT 
                LEFT(a.account_number::text, 5) AS ledger_account,
                a.char_type,
                a.currency_code,
                a.account_rk
            FROM ds.md_account_d a
            WHERE (v_report_start_date BETWEEN a.data_actual_date AND COALESCE(a.data_actual_end_date, '9999-12-31'))
                OR (v_report_end_date BETWEEN a.data_actual_date AND COALESCE(a.data_actual_end_date, '9999-12-31'))
        ),
        -- Получаем информацию о главах из справочника балансовых счетов
        ledger_info AS (
            SELECT 
                LEFT(las.ledger_account::text, 5) AS ledger_account,
                las.chapter,
                las.characteristic
            FROM ds.md_ledger_account_s las
            WHERE (las.start_date <= v_report_end_date AND (las.end_date IS NULL OR las.end_date >= v_report_start_date))
        ),
        -- Входящие остатки (на дату перед началом отчетного периода)
        balance_in AS (
            SELECT 
                la.ledger_account,
                SUM(CASE WHEN la.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_in_rub,
                SUM(CASE WHEN la.currency_code NOT IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_in_val,
                SUM(b.balance_out_rub) AS balance_in_total
            FROM ledger_accounts la
            JOIN dm.dm_account_balance_f b ON la.account_rk = b.account_rk
            WHERE b.on_date = v_report_start_date - INTERVAL '1 day'
            GROUP BY la.ledger_account
        ),
        -- Исходящие остатки (на последний день отчетного периода)
        balance_out AS (
            SELECT 
                la.ledger_account,
                SUM(CASE WHEN la.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_out_rub,
                SUM(CASE WHEN la.currency_code NOT IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_out_val,
                SUM(b.balance_out_rub) AS balance_out_total
            FROM ledger_accounts la
            JOIN dm.dm_account_balance_f b ON la.account_rk = b.account_rk
            WHERE b.on_date = v_report_end_date
            GROUP BY la.ledger_account
        ),
        -- Обороты за отчетный период
        turnovers AS (
            SELECT 
                la.ledger_account,
                SUM(CASE WHEN la.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
                SUM(CASE WHEN la.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
                SUM(t.debet_amount_rub) AS turn_deb_total,
                SUM(CASE WHEN la.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
                SUM(CASE WHEN la.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val,
                SUM(t.credit_amount_rub) AS turn_cre_total
            FROM ledger_accounts la
            JOIN dm.dm_account_turnover_f t ON la.account_rk = t.account_rk
            WHERE t.on_date BETWEEN v_report_start_date AND v_report_end_date
            GROUP BY la.ledger_account
        )
        SELECT
            v_report_start_date AS from_date,
            v_report_end_date AS to_date,
            li.chapter,
            li.ledger_account,
            li.characteristic,
            COALESCE(bi.balance_in_rub, 0) AS balance_in_rub,
            COALESCE(bi.balance_in_val, 0) AS balance_in_val,
            COALESCE(bi.balance_in_total, 0) AS balance_in_total,
            COALESCE(t.turn_deb_rub, 0) AS turn_deb_rub,
            COALESCE(t.turn_deb_val, 0) AS turn_deb_val,
            COALESCE(t.turn_deb_total, 0) AS turn_deb_total,
            COALESCE(t.turn_cre_rub, 0) AS turn_cre_rub,
            COALESCE(t.turn_cre_val, 0) AS turn_cre_val,
            COALESCE(t.turn_cre_total, 0) AS turn_cre_total,
            COALESCE(bo.balance_out_rub, 0) AS balance_out_rub,
            COALESCE(bo.balance_out_val, 0) AS balance_out_val,
            COALESCE(bo.balance_out_total, 0) AS balance_out_total
        FROM ledger_info li
        LEFT JOIN balance_in bi ON li.ledger_account = bi.ledger_account
        LEFT JOIN balance_out bo ON li.ledger_account = bo.ledger_account
        LEFT JOIN turnovers t ON li.ledger_account = t.ledger_account;
        
        GET DIAGNOSTICS v_records_processed = ROW_COUNT;
    EXCEPTION WHEN OTHERS THEN
        v_status := 'failed';
        v_error_message := v_log_key || '; Error: ' || SQLERRM;
        RAISE NOTICE 'Ошибка при заполнении формы 101: %', SQLERRM;
    END;
    
    -- Обновляем лог
    UPDATE logs.etl_logs
    SET
        end_time = CURRENT_TIMESTAMP,
        status = v_status,
        records_processed = v_records_processed,
        error_message = CASE 
            WHEN v_status = 'failed' THEN v_error_message
            ELSE v_log_key
        END
    WHERE
        table_name = 'dm.dm_f101_round_f' AND
        error_message = v_log_key AND
        start_time = v_start_time;
    
    RAISE NOTICE 'Обработано % записей за отчетный период с % по %', 
        v_records_processed, v_report_start_date, v_report_end_date;
END;
$$;