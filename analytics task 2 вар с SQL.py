import pandas as pd
import sqlite3

# Подключение к базе данных SQLite
conn = sqlite3.connect('analytics.db')

# Чтение данных из Excel-файла
try:
    df_report1 = pd.read_excel('задание на аналитика.xlsx', sheet_name='отчет 1')
    df_report2 = pd.read_excel('задание на аналитика.xlsx', sheet_name='отчет 2')
    df_cession = pd.read_excel('задание на аналитика.xlsx', sheet_name='цессия')
except Exception as e:
    print(f"Ошибка чтения Excel-файла: {e}")
    exit()

# Переименование столбцов для устранения проблем с символом `№` и другими кириллическими именами
df_report1 = df_report1.rename(columns={
    '№': 'Number',
    'Номер контракта': 'ContractNumber',
    'дата выдачи': 'IssueDate',
    'Сумма выдачи': 'LoanAmount',
    'Задолженность по ОД': 'PrincipalDebt',
    'Задолженность по %%': 'InterestDebt',
    'Кол-во дней просрочки, фактическое': 'OverdueDays'
})

df_report2 = df_report2.rename(columns={
    'NumContract': 'ContractNumber',
    'ContractID': 'ContractID',
    'SubdivisionName': 'Region',
    'Status': 'Status',
    'DateStatus': 'LastPaymentDate',
    'SumLastPay': 'LastPaymentAmount'
})

df_cession = df_cession.rename(columns={
    'ContractID': 'ContractID'
})

# Загрузка данных в SQLite
df_report1.to_sql('report1', conn, if_exists='replace', index=False)
df_report2.to_sql('report2', conn, if_exists='replace', index=False)
df_cession.to_sql('cession', conn, if_exists='replace', index=False)

# SQL-запрос для реестра
query_registry = """
SELECT 
    r1.Number AS `Номер`,
    r1.ContractNumber AS `Номер контракта`,
    r2.ContractID AS `ID договора`,
    r2.Region AS `Регион выдачи`,
    r1.IssueDate AS `Дата выдачи`,
    r1.LoanAmount AS `Сумма выдачи`,
    r2.Status AS `Статус`,
    r1.PrincipalDebt AS `Задолженность по ОД`,
    r1.InterestDebt AS `Задолженность по %%`,
    r1.OverdueDays AS `Кол-во дней просрочки, фактическое`,
    r2.LastPaymentDate AS `Дата последнего платежа`,
    r2.LastPaymentAmount AS `Сумма последнего платежа`
FROM report1 r1
LEFT JOIN report2 r2
ON LTRIM(r1.ContractNumber, '0') = CAST(r2.ContractNumber AS TEXT)
WHERE r2.ContractID NOT IN (SELECT ContractID FROM cession)
AND r2.Status NOT IN ('BANKRUPTCY', 'REPAID', 'EARLY_REPAID')
"""

# SQL-запрос для сводной таблицы
query_pivot = """
SELECT 
    `Регион выдачи`,
    COUNT(*) AS Количество_клиентов,
    SUM(`Сумма выдачи`) AS Сумма_выданных_займов
FROM (
    SELECT 
        r1.Number AS `Номер`,
        r1.ContractNumber AS `Номер контракта`,
        r2.ContractID AS `ID договора`,
        r2.Region AS `Регион выдачи`,
        r1.IssueDate AS `Дата выдачи`,
        r1.LoanAmount AS `Сумма выдачи`,
        r2.Status AS `Статус`,
        r1.PrincipalDebt AS `Задолженность по ОД`,
        r1.InterestDebt AS `Задолженность по %%`,
        r1.OverdueDays AS `Кол-во дней просрочки, фактическое`,
        r2.LastPaymentDate AS `Дата последнего платежа`,
        r2.LastPaymentAmount AS `Сумма последнего платежа`
    FROM report1 r1
    LEFT JOIN report2 r2
    ON LTRIM(r1.ContractNumber, '0') = CAST(r2.ContractNumber AS TEXT)
    WHERE r2.ContractID NOT IN (SELECT ContractID FROM cession)
    AND r2.Status NOT IN ('BANKRUPTCY', 'REPAID', 'EARLY_REPAID')
) AS filtered
GROUP BY `Регион выдачи`
"""

# Выполнение запросов
try:
    df_registry = pd.read_sql_query(query_registry, conn)
    df_pivot = pd.read_sql_query(query_pivot, conn)
except Exception as e:
    print(f"Ошибка выполнения SQL-запроса: {e}")
    conn.close()
    exit()

# Закрытие соединения
conn.close()

# Вывод результатов
print("Реестр:")
print(df_registry)
print("\nСводная таблица:")
print(df_pivot)

# Сохранение результатов в CSV (опционально, для отправки)
df_registry.to_csv('registry.csv', index=False, encoding='utf-8-sig')
df_pivot.to_csv('pivot_table.csv', index=False, encoding='utf-8-sig')