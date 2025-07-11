-- Расчет формы 101 за январь 2018 (передаем 1 февраля как отчетную дату)
CALL dm.fill_f101_round_f('2018-02-01');

-- Проверка результатов
SELECT * FROM dm.dm_f101_round_f 
WHERE from_date = '2018-01-01' AND to_date = '2018-01-31'
LIMIT 10;

-- Проверка логов
SELECT * FROM logs.etl_logs 
WHERE table_name = 'dm.dm_f101_round_f'
ORDER BY start_time DESC
LIMIT 5;