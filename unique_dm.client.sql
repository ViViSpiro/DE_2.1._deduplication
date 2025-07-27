-----------------------------------------------------------------------------------------
-- Подготовка к созданию запросов:
-- Обнаружение дубликатов в витрине dm.client
SELECT 
    client_rk,
    effective_from_date,
    COUNT(1) as duplicate_count
FROM 
    dm.client
GROUP BY 
    client_rk, effective_from_date
HAVING 
    COUNT(1) > 1
ORDER BY 
    client_rk, effective_from_date;






-- Подсчет уникальных строк и строк, которые будут удалены
SELECT 
    COUNT(DISTINCT (client_rk, effective_from_date)) as rows_after_deduplication,
    COUNT(1) - COUNT(DISTINCT (client_rk, effective_from_date)) as rows_to_delete
FROM 
    dm.client;


---------------------------------------------------------------------------------------------------------------------------
-- Тест запросов для видео:
-- Создаем копии таблицы, чтобы показать работу запросов на видео
CREATE TABLE dm.client1 AS SELECT * FROM dm.client;
CREATE TABLE dm.client2 AS SELECT * FROM dm.client;






-- Запрос для удаления дубликатов из витрины dm.client1 при доступном чтении из таблицы
DO $$
DECLARE
    v_total_before BIGINT;
    v_should_remain BIGINT;
    v_total_after BIGINT;
    v_unique_after BIGINT;
    v_deleted_rows BIGINT;
BEGIN
    -- Блокировка таблицы (EXCLUSIVE позволяет читать, но не изменять)
    LOCK TABLE dm.client1 IN EXCLUSIVE MODE;
    -- Сбор статистики ДО изменений
    SELECT
        COUNT(*),
        COUNT(DISTINCT (client_rk, effective_from_date)),
        COUNT(*) - COUNT(DISTINCT (client_rk, effective_from_date))
    INTO
        v_total_before,
        v_should_remain,
        v_deleted_rows
    FROM dm.client1;
    RAISE NOTICE 'Статистика до удаления:';
    RAISE NOTICE '  Всего строк: %', v_total_before;
    RAISE NOTICE '  Уникальных комбинаций: %', v_should_remain;
    RAISE NOTICE '  Дубликатов для удаления: %', v_deleted_rows;
    -- Создаем временную таблицу с уникальными записями
    CREATE TEMP TABLE temp_unique_clients AS
    SELECT DISTINCT ON (client_rk, effective_from_date) *
    FROM dm.client1;
    -- Заменяем данные (используем TRUNCATE + INSERT для атомарности)
    TRUNCATE dm.client1;
    INSERT INTO dm.client1 SELECT * FROM temp_unique_clients;
    DROP TABLE temp_unique_clients;
    -- Проверка результата
    SELECT
        COUNT(*),
        COUNT(DISTINCT (client_rk, effective_from_date))
    INTO
        v_total_after,
        v_unique_after
    FROM dm.client1;
    RAISE NOTICE 'Результат после удаления:';
    RAISE NOTICE '  Осталось строк: %', v_total_after;
    RAISE NOTICE '  Уникальных комбинаций: %', v_unique_after;
    -- Верификация
    IF v_total_after != v_should_remain OR v_unique_after != v_should_remain THEN
        RAISE EXCEPTION 'ОШИБКА ВЕРИФИКАЦИИ: Ожидалось % строк. Получено % строк (уникальных: %)',
                      v_should_remain, v_total_after, v_unique_after;
    END IF;
    RAISE NOTICE 'Операция успешно завершена. Удалено % дубликатов', v_deleted_rows;
EXCEPTION
    WHEN OTHERS THEN
        -- Автоматическая очистка при ошибке
        IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'temp_unique_clients') THEN
            DROP TABLE temp_unique_clients;
        END IF;
        RAISE EXCEPTION 'ОШИБКА: %. Все изменения отменены (автоматический ROLLBACK)', SQLERRM;
END $$;






-- Запрос для удаления дубликатов из витрины dm.client2 с полной блокировкой таблицы
DO $$
DECLARE
    v_total_before BIGINT;
    v_should_remain BIGINT;
    v_total_after BIGINT;
    v_unique_after BIGINT;
    v_deleted_count BIGINT;
BEGIN
    -- Блокировка таблицы (ACCESS EXCLUSIVE - полная блокировка)
    LOCK TABLE dm.client2 IN ACCESS EXCLUSIVE MODE;
    -- Сбор статистики до изменений
    SELECT
        COUNT(*),
        COUNT(DISTINCT (client_rk, effective_from_date))
    INTO
        v_total_before,
        v_should_remain
    FROM dm.client2;
    RAISE NOTICE 'Начало обработки:';
    RAISE NOTICE '  Всего строк: %', v_total_before;
    RAISE NOTICE '  Уникальных комбинаций: %', v_should_remain;
    RAISE NOTICE '  Дубликатов для удаления: %', v_total_before - v_should_remain;
    -- Удаление дубликатов с сохранением количества удаленных строк
    WITH duplicates AS (
        SELECT
            ctid,
            ROW_NUMBER() OVER (PARTITION BY client_rk, effective_from_date ORDER BY ctid) AS rn
        FROM dm.client2
    ),
    deleted AS (
        DELETE FROM dm.client2
        WHERE ctid IN (SELECT ctid FROM duplicates WHERE rn > 1)
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_count FROM deleted;
    -- Проверка результата
    SELECT
        COUNT(*),
        COUNT(DISTINCT (client_rk, effective_from_date))
    INTO
        v_total_after,
        v_unique_after
    FROM dm.client2;
    RAISE NOTICE 'Результат:';
    RAISE NOTICE '  Удалено строк: %', v_deleted_count;
    RAISE NOTICE '  Осталось строк: %', v_total_after;
    RAISE NOTICE '  Уникальных комбинаций: %', v_unique_after;
    -- Верификация
    IF v_total_after != v_should_remain OR v_unique_after != v_should_remain THEN
        RAISE EXCEPTION 'ОШИБКА ВЕРИФИКАЦИИ: Ожидалось % строк, получилось % (уникальных: %)',
                        v_should_remain, v_total_after, v_unique_after;
    END IF;
    RAISE NOTICE 'Операция успешно завершена';
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'ОШИБКА: %. Все изменения отменены (автоматический ROLLBACK)', SQLERRM;
END $$;




-----------------------------------------------------------------------------------------------------------
-- Запросы для поиска и удаления дубликатов в оригинальной таблице:
-- Запрос для удаления дубликатов из витрины dm.client при доступном чтении из таблицы
DO $$
DECLARE
    v_total_before BIGINT;
    v_should_remain BIGINT;
    v_total_after BIGINT;
    v_unique_after BIGINT;
    v_deleted_rows BIGINT;
BEGIN
    -- Блокировка таблицы (EXCLUSIVE позволяет читать, но не изменять)
    LOCK TABLE dm.client IN EXCLUSIVE MODE;
    -- Сбор статистики ДО изменений
    SELECT
        COUNT(*),
        COUNT(DISTINCT (client_rk, effective_from_date)),
        COUNT(*) - COUNT(DISTINCT (client_rk, effective_from_date))
    INTO
        v_total_before,
        v_should_remain,
        v_deleted_rows
    FROM dm.client;
    RAISE NOTICE 'Статистика до удаления:';
    RAISE NOTICE '  Всего строк: %', v_total_before;
    RAISE NOTICE '  Уникальных комбинаций: %', v_should_remain;
    RAISE NOTICE '  Дубликатов для удаления: %', v_deleted_rows;
    -- Создаем временную таблицу с уникальными записями
    CREATE TEMP TABLE temp_unique_clients AS
    SELECT DISTINCT ON (client_rk, effective_from_date) *
    FROM dm.client;
    -- Заменяем данные (используем TRUNCATE + INSERT для атомарности)
    TRUNCATE dm.client;
    INSERT INTO dm.client SELECT * FROM temp_unique_clients;
    DROP TABLE temp_unique_clients;
    -- Проверка результата
    SELECT
        COUNT(*),
        COUNT(DISTINCT (client_rk, effective_from_date))
    INTO
        v_total_after,
        v_unique_after
    FROM dm.client;
    RAISE NOTICE 'Результат после удаления:';
    RAISE NOTICE '  Осталось строк: %', v_total_after;
    RAISE NOTICE '  Уникальных комбинаций: %', v_unique_after;
    -- Верификация
    IF v_total_after != v_should_remain OR v_unique_after != v_should_remain THEN
        RAISE EXCEPTION 'ОШИБКА ВЕРИФИКАЦИИ: Ожидалось % строк. Получено % строк (уникальных: %)',
                      v_should_remain, v_total_after, v_unique_after;
    END IF;
    RAISE NOTICE 'Операция успешно завершена. Удалено % дубликатов', v_deleted_rows;
EXCEPTION
    WHEN OTHERS THEN
        -- Автоматическая очистка при ошибке
        IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'temp_unique_clients') THEN
            DROP TABLE temp_unique_clients;
        END IF;
        RAISE EXCEPTION 'ОШИБКА: %. Все изменения отменены (автоматический ROLLBACK)', SQLERRM;
END $$;






-- Запрос для удаления дубликатов из витрины dm.client с полной блокировкой таблицы
DO $$
DECLARE
    v_total_before BIGINT;
    v_should_remain BIGINT;
    v_total_after BIGINT;
    v_unique_after BIGINT;
    v_deleted_count BIGINT;
BEGIN
    -- Блокировка таблицы (ACCESS EXCLUSIVE - полная блокировка)
    LOCK TABLE dm.client IN ACCESS EXCLUSIVE MODE;
    -- Сбор статистики до изменений
    SELECT
        COUNT(*),
        COUNT(DISTINCT (client_rk, effective_from_date))
    INTO
        v_total_before,
        v_should_remain
    FROM dm.client;
    RAISE NOTICE 'Начало обработки:';
    RAISE NOTICE '  Всего строк: %', v_total_before;
    RAISE NOTICE '  Уникальных комбинаций: %', v_should_remain;
    RAISE NOTICE '  Дубликатов для удаления: %', v_total_before - v_should_remain;
    -- Удаление дубликатов с сохранением количества удаленных строк
    WITH duplicates AS (
        SELECT
            ctid,
            ROW_NUMBER() OVER (PARTITION BY client_rk, effective_from_date ORDER BY ctid) AS rn
        FROM dm.client
    ),
    deleted AS (
        DELETE FROM dm.client
        WHERE ctid IN (SELECT ctid FROM duplicates WHERE rn > 1)
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_count FROM deleted;
    -- Проверка результата
    SELECT
        COUNT(*),
        COUNT(DISTINCT (client_rk, effective_from_date))
    INTO
        v_total_after,
        v_unique_after
    FROM dm.client;
    RAISE NOTICE 'Результат:';
    RAISE NOTICE '  Удалено строк: %', v_deleted_count;
    RAISE NOTICE '  Осталось строк: %', v_total_after;
    RAISE NOTICE '  Уникальных комбинаций: %', v_unique_after;
    -- Верификация
    IF v_total_after != v_should_remain OR v_unique_after != v_should_remain THEN
        RAISE EXCEPTION 'ОШИБКА ВЕРИФИКАЦИИ: Ожидалось % строк, получилось % (уникальных: %)',
                        v_should_remain, v_total_after, v_unique_after;
    END IF;
    RAISE NOTICE 'Операция успешно завершена';
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'ОШИБКА: %. Все изменения отменены (автоматический ROLLBACK)', SQLERRM;
END $$;
