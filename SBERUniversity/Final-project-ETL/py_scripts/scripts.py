import psycopg2
import sys
import os
import re
import shutil
import pandas as pd
from datetime import datetime
from py_scripts import cfg as c
import requests


#///////////////////////////////////////////////////////#
def connect_to_db(db, host, user, password, port):
# connect to the PostgreSQL server
	try:
		conn = psycopg2.connect(database = db,
                                                    host = host,
                                                    user = user,
                                                    password = password,
                                                    port = port)
# Отключение автокоммита

		conn.autocommit = False

# Создание курсора

		cursor = conn.cursor()
	except(Exception,psycopg2.DatabaseError,SystemExit) as Err:
		print(f'Sys.py_scripts: {Err}')
		sys.exit(1)
	finally:
		print(f'Подключение {db} успешно!')
		print(f'autocommit: {conn.autocommit}')
		return conn, cursor

#Очищаем  staging table информацию цепляем запросом из функций БД , запрос в cfg.py
def clear_stg_table(cursor):
	cursor.execute(c.stg_tab)
	for k in cursor.fetchall():
		cursor.execute(f'''DELETE FROM de12.{''.join(k)};''')
		print (f'Таблица {list(k)} очищена')

#Проверка наличия таблиц (проверка стоит через количество таблиц ( Эх добавить бы еще название в таблицу like работал бы лучше))
# Проверка идет через количество если меньше то идем к файлу main.dll и создаём все которых нет (там использован if not exists)
#Запуск создания если количество таблиц меньше 18
def great_table(path,cursor):
	cursor.execute(c.gre_tab)
	val = len(cursor.fetchall())
	if not  val == 18:
		path1 = path.join(['main.ddl'])
		file = open(path1)
		sql = file.read()
		cursor.execute(sql)
		print('Таблицы отсутсвуют: Создаю! ')
	print('Таблицы созданы: Продолжаем.')

#Забираем дату последнего обновления из alpa_meta_data
def find_last_date(table,cursor):
	cursor.execute(f""" select max_update_dt
				from de12.alpa_meta_data
				where schema_name ='de12'
				and table_name = '{table}'""")
	last_date = cursor.fetchone()[0]
	return last_date

#Провекра на наличие файлов если меньше 3 отмена выполнения(Все данные в бд за предыдущие числа)
def val_files():
	val = []
	for i in sorted(os.listdir(c.files)):
		val += re.findall('\S+', i)
	if len(val) < 3:
		print ("Нет файлов для загрузки! Отчеты построены по старым данным")
		print (f'Файлы в директории: {val}')
		sys.exit(0)

#Находим файл с минимальной датой (из функции last_date), смотрим чтоб файлы не были меньше последней даты обновления
def sort_file(file_name,last_date):
	for i in sorted(os.listdir(c.files)):
		if i.startswith(file_name):
			date_file = ''.join([n for n in i if n.isdigit()])
			file_dt = datetime.strptime(date_file,'%d%m%Y')
			if file_dt <= last_date:
				print (f'Файлы пришли равные или меньше последней даты! {file_dt} Нужна проверка!')
				break
			if file_dt  > last_date:
				file = i
				break
	return file,file_dt

#архивация файлов после обработки
def archive(file_name):
	shutil.move(f'{c.files + file_name}',f'{c.archive + file_name}.backup')
	print(f'Файл:{file_name} -> перенесён в архив')


#Вынимаем данные полным срезом из файлов для исторических таблиц на проверку удаленных
def into_del(cursor,table,field,datf):
	cursor.executemany(f'''INSERT INTO de12.alpa_stg_{table}_del ({field}) VALUES(%s)''',datf.values.tolist())


#Вынимаем данные полным срезом из таблиц для исторических таблиц на проверку удаленных
def into_del_table(cursor_one,cursor_two,table,field):
	cursor_one.execute(f'''select {field} from info.{table}''')
	records = cursor_one.fetchall()
	cursor_two.executemany(f'insert into de12.alpa_stg_{table}_del ({field}) values(rtrim(%s))',records)

#Отправление cообщения в телеграмм
def send_mes():
	TOKEN = "5804101843:AAHS41OjKt_fcJIKM9hHhYxaSEks3mF2At8"
	chat_id = "1071137776" 
	f = open('/home/de12/alpa/project/rep.txt','r')
	report = f.read()
	message = report
	url =f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
	f.close
	requests.get(url).json()
	print('Сообшение отправлено в чат Telegram  @Aaaaaaaaaaa69bot')
