#!/usr/bin/python3

#Выбираем библиотеки
import pandas as pd
from datetime import datetime
from py_scripts import cfg as c
from py_scripts import scripts as s
import os
import psycopg2
import sys
import re
#----------------------------------------------------------------------------------------------#

#Отмечаем дату начала скрипта
start = datetime.now()
print(f'Запуск скрипта:{start}')

#----------------------------------------------------------------------------------------------#

# Подключение к базе данных формирование курсоров отключение autocommit
# Создание подключения курсоров к учебной бд (def connect_to_db: Bd,host,User,Password,Port)

try:
	conn_edu,cursor_edu = s.connect_to_db(
						c.bd_e,
						c.host,
						c.user_e,
						c.password_e,
						c.port)
# Создание подключения курсоров к базе bank (def connect_to_db: Bd,host,User,Password,Port)
	conn_bank,cursor_bank = s.connect_to_db(
						c.bd_b,
						c.host,
						c.user_b,
						c.password_b,
						c.port)
except(Exception,psycopg2.DatabaseError,SystemExit) as Err:
	print(f'Sys.test.py: {Err}')
	sys.exit(1)

#----------------------------------------------------------#

#Проверка наличия таблиц(Отсутвие = создание.)
s.great_table(c.pj,cursor_edu)
conn_edu.commit()

#Очищаем staging table
s.clear_stg_table(cursor_edu)

#Проверяем наличие файлов для загрузки (в функции стоит sys.exit(1))
s.val_files()

#----------------------------------------------------------

# Начинаем наполнять staging table (Тут не совсем понятно что делать с NaN обработаем в 0)
#///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////#

# Таблица STG_Terminals
try:
	last_terminals_date = s.find_last_date('terminals', cursor_edu)
	file_terminals,date_file_terminals = s.sort_file('terminals_', last_terminals_date)

	with open(f'{c.files}{file_terminals}', mode='rb') as t:
		terminals = pd.read_excel(t, header=0,sheet_name='terminals')
		terminals = terminals.fillna(0) #dropna не хочу использовать отсею в sql
		terminals['update_dt'] = date_file_terminals.strftime('%Y-%m-%d')

		cursor_edu.executemany('''INSERT INTO de12.alpa_stg_terminals(	terminal_id,
										terminal_type,
										terminal_city,
										terminal_address,
										update_dt )
					VALUES( %s, %s, %s, %s,%s)''', terminals.values.tolist( ))

	print (f'Файл:{file_terminals} от {date_file_terminals} записан.')

except (Exception,psycopg2.DatabaseError,SystemExit) as Err:
	print(f'Sys.load_terminals: {Err}')
	conn_edu.rollback()
finally:
	s.archive(file_terminals)
	t.close()
# Таблица STG_passport_black_list
try:
	last_passport_date_date = s.find_last_date('passports_blacklist', cursor_edu)
	file_passports,date_file_passports = s.sort_file('passport_blacklist_', last_terminals_date)

	with open(f'{c.files}/{file_passports}', mode='rb') as t:
		passports_blacklist = pd.read_excel(t)
		passports_blacklist = passports_blacklist.fillna(0) #dropna не хочу использовать отсею в sql
		passports_blacklist.rename(columns={'date':'entry_dt','passport':'passport_num'}, inplace=True)

		cursor_edu.executemany('''INSERT INTO de12.alpa_stg_passport_blacklist( entry_dt,
											passport_num)
					VALUES( %s, %s )''',passports_blacklist.values.tolist( ))

	print (f'Файл:{file_passports} от {date_file_passports} записан.')

except (Exception,psycopg2.DatabaseError,SystemExit) as Err:
	print(f'Sys.load_passports: {Err}')
	conn_edu.rollback()
finally:
	s.archive(file_passports)
	t.close()
# Таблица STG_Transactions
try:
	last_transactions_date = s.find_last_date('transactions', cursor_edu)
	file_transactions,date_file_transactions = s.sort_file('transactions_', last_transactions_date)

	with open(f'{c.files}{file_transactions}') as t:
		transactions = pd.read_csv(t,sep=';')
		transactions.rename(columns={'transaction_id':'trans_id','transaction_date':'trans_date','amount':'amt'},inplace=True)
		transactions['trans_id'] = transactions['trans_id'].astype('object')
		transactions['amt'] = transactions['amt'].astype(str).str.replace(',', '.').astype('float')
		transactions['trans_date'] = pd.to_datetime(transactions['trans_date'])
		transactions['update_dt'] = date_file_transactions.strftime('%Y-%m-%d')
		cursor_edu.executemany('''INSERT INTO de12.alpa_stg_transactions(	trans_id,
											trans_date,
											amt,
											card_num,
											oper_type,
											oper_result,
											terminal,
											update_dt )
					VALUES( %s, %s, %s, %s, %s, %s, %s,%s )''',transactions.values.tolist( ))

	print (f'Файл:{file_transactions} от {date_file_transactions} записан.')

except (Exception, psycopg2.DatabaseError) as Err:
	print(f'Sys.load_transactions: {Err}')
	conn_edu.rollback()
finally:
	s.archive(file_transactions)
	t.close()

#Таблица STG Accounts
try:
	last_accounts_date = s.find_last_date('accounts', cursor_edu)

	cursor_bank.execute(f'''SELECT	account,
					valid_to,
					client,
					create_dt,
					update_dt
				FROM info.accounts
				WHERE COALESCE(update_dt, create_dt) > (to_timestamp('{last_accounts_date}','YYYY-MM-DD HH24:MI:SS'))
					or create_dt = to_timestamp('1900-01-01','YYYY-MM-DD HH24:MI:SS') ''' )

	records = cursor_bank.fetchall()
	names = [x[0] for x in cursor_bank.description]
	accounts = pd.DataFrame(records,columns = names)

	cursor_edu.executemany('''INSERT INTO de12.alpa_stg_accounts(	account_num,
									valid_to,
									client,
									create_dt,
									update_dt
						) VALUES( %s, %s, %s, %s, %s) ''', accounts.values.tolist() )

except (Exception,psycopg2.DatabaseError,SystemExit) as Err:
	print(f'Sys.load_stg_account: {Err}')
	conn_edu.rollback( )

finally:
	print ('Данные из bank.accounts записаны.')

#Таблица STG_Cards

try:
	last_cards_date = s.find_last_date('cards', cursor_edu)

	cursor_bank.execute(f'''SELECT card_num,
					account,
					create_dt,
					update_dt
				FROM info.cards
				WHERE COALESCE(update_dt, create_dt) > (to_timestamp('{last_cards_date}','YYYY-MM-DD HH24:MI:SS'))''')

	records = cursor_bank.fetchall()
	names = [x[0] for x in cursor_bank.description]
	cards = pd.DataFrame(records,columns = names)
	cursor_edu.executemany('''INSERT INTO de12.alpa_stg_cards( card_num,
								account_num,
								create_dt,
								update_dt
				) VALUES(rtrim(%s), %s, %s, %s) ''',  cards.values.tolist())
except (Exception,psycopg2.DatabaseError,SystemExit) as Err: 
	print(f'Sys.load_stg_cards: {Err}')
	conn_edu.rollback()

finally:
	print ('Данные из bank.cards записаны.')

#Таблица STG_clients
try:
	last_clients_date = s.find_last_date('clients', cursor_edu)

	cursor_bank.execute (f'''SELECT client_id,
					last_name,
					first_name,
					patronymic,
					date_of_birth,
					passport_num,
					passport_valid_to,
					phone,
					create_dt,
					update_dt
				FROM info.clients
				WHERE COALESCE(update_dt, create_dt) > (to_timestamp('{last_clients_date}','YYYY-MM-DD HH24:MI:SS'))
					or create_dt = to_timestamp('1900-01-01','YYYY-MM-DD HH24:MI:SS') ''' )

	records = cursor_bank.fetchall()
	names = [x[0] for x in cursor_bank.description]
	clients = pd.DataFrame(records,columns = names)

	cursor_edu.executemany('''INSERT INTO de12.alpa_stg_clients (	client_id,
									last_name,
									first_name,
									patronymic,
									date_of_birth,
									passport_num,
									passport_valid_to,
									phone,
									create_dt,
									update_dt
					) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) ''',clients.values.tolist())


except (Exception,psycopg2.DatabaseError,SystemExit) as Err:
	print(f'Sys.load_stg_clients: {Err}')
	conn_edu.rollback()

finally:
	print ('Данные из bank.clients записаны.')

#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////#

#-------------------------------------------------------------------------------------------------------------------------#
#Наполние таблиц для проверки на удаление полным срезом и sourse или файлов()

try:
	s.into_del(cursor_edu,'terminals','terminal_id',terminals[['terminal_id']])
	s.into_del_table(cursor_bank,cursor_edu,'accounts','account')
	s.into_del_table(cursor_bank,cursor_edu,'clients','client_id')
	s.into_del_table(cursor_bank,cursor_edu,'cards','card_num')

except (Exception, psycopg2.DatabaseError) as Err:
	print(f'Sys.table_del_inf: {Err}')

#-------------------------------------------------------------------------------------------------------------------------#
#Выполнение скриптов для загрузки Таблицы фактов и измерений

try:
	sql_list = ['accounts_load','cards_load','clients_load','transactions_load','passports_load','terminals_load']
	for file in sql_list:
		query = open(f'{c.sql}{file}.sql', 'r', encoding='utf-8').read( )
		cursor_edu.execute(query)
		print(f'Выполнено: {file}')

except (Exception, psycopg2.DatabaseError) as Err:
	print(f'Sys.sql_scripts:Файл: {file}: Errors: {Err}')
	conn_edu.rollback()
	sys.exit(1)

#-------------------------------------------------------------------------------------------------------------------------#
#Выполение скрипта загрузки отчета
#(перенес в конец чтоб выполение было после заполнения всех таблиц фактов и измерений)
#Почему-то не доверяю последовательности выполнения, лучше сам поставлю так спокойнее
#Будет время попробовать подставку в execute (пока оставляем подзапрос)
try:
	query = open(f'{c.sql}rep_fraud_load.sql', 'r', encoding='utf-8').read( )
	cursor_edu.execute(query)

	print (f'Выполнено: rep_fraud_load.sql')
	print ('Отчёт построен' )
except (Exception, psycopg2.DatabaseError) as Err:
      print(f'Sys.sql_scripts: Rep_fraud: {Err}')

#-------------------------------------------------------------------------------------------------------------------------#
#Конечная. Подтверждение, Закрытие подключения и курсоров

conn_edu.commit()
conn_edu.close()
conn_bank.close()
cursor_edu.close()
cursor_bank.close()

#-------------------------------------------------------------------------------------------------------------------------#
# Добавим информативности
close = datetime.now()
time = close.timestamp() - start.timestamp()

print(f'''Выполнение началось:{start} 
Закончили: {close}
Длительность: {time}
Очёт отправлен в телеграмм
Good day and Good luck !''')

