#Создание переменых подключения
bd_b="bank"
bd_e="edu"
host="de-edu-db.chronosavant.ru"
port="5432"
user_e="de12"
password_e="sarumanthewhite"
user_b="bank_etl"
password_b="bank_etl_password"

#Создание путей
files="/home/de12/alpa/project/data/"
archive="/home/de12/alpa/project/archive/"
sql="/home/de12/alpa/project/sql_scripts/"
py="/home/de12/alpa/project/py_scripts/"
pj="/home/de12/alpa/project/"

#Создание запросов

stg_tab = ''' SELECT tablename
		FROM pg_tables
		WHERE schemaname='de12'and (tablename like 'alpa_stg%') '''
gre_tab = '''select table_name
                                from information_schema.tables
                                where table_name in (SELECT tablename
                    						FROM pg_tables
								WHERE schemaname='de12' and (tablename like 'alpa_%'))'''
