import pandas as pd

# Загрузка данных
file_path = 'задание на аналитика.xlsx'

df_report1 = pd.read_excel(file_path, sheet_name='отчет 1')
df_report2 = pd.read_excel(file_path, sheet_name='отчет 2')
df_cession = pd.read_excel(file_path, sheet_name='цессия')

# Нормализация номера контракта
df_report1['ContractNum'] = df_report1['Номер контракта'].astype(str).str.lstrip('0')
df_report2['ContractNum'] = df_report2['NumContract'].astype(str)

# Объединение данных
df_merged = pd.merge(df_report1, df_report2, on='ContractNum', how='left')

# Формирование реестра
registry = df_merged[[
    '№', 'Номер контракта', 'ContractID', 'SubdivisionName', 'дата выдачи',
    'Сумма выдачи', 'Status', 'Задолженность по ОД', 'Задолженность по %%',
    'Кол-во дней просрочки, фактическое', 'DateStatus', 'SumLastPay'
]].rename(columns={
    'ContractID': 'ID договора',
    'SubdivisionName': 'Регион выдачи',
    'дата выдачи': 'Дата выдачи',
    'Status': 'Статус',
    'DateStatus': 'Дата последнего платежа',
    'SumLastPay': 'Сумма последнего платежа'
})

# Исключение клиентов
cession_ids = df_cession['ContractID'].unique()
filtered_registry = registry[
    (~registry['ID договора'].isin(cession_ids)) &
    (~registry['Статус'].isin(['BANKRUPTCY', 'REPAID', 'EARLY_REPAID']))
]

# Построение сводной таблицы
pivot_table = filtered_registry.groupby('Регион выдачи').agg(
    Количество_клиентов=('№', 'count'),
    Сумма_выданных_займов=('Сумма выдачи', 'sum')
).reset_index()

# Вывод результатов 
print("Реестр:")
print(filtered_registry)
print("\nСводная таблица:")
print(pivot_table)