--############################################################################################
--## Файл создания таблиц и наполнения метаданных стартовыми значениями (de12.alpa)       ###
--## Поля были поменяны местами для удобства, при загрузке буду направлять по полям      ###
--#########################################################################################

--##########################################################################################
--# Staging_table (6 таблиц)(terminal,black_passport,transactions,accounts,cards,client) ##
--########################################################################################

--#1.Terminals
create table if not exists de12.alpa_stg_terminals( 
	terminal_id varchar(10),
	terminal_type varchar(10),
	terminal_city varchar(30),
	terminal_address varchar(100),
	update_dt date
);

--#2.Passport_blacklist
create table if not exists de12.alpa_stg_passport_blacklist(
	entry_dt date,
	passport_num varchar(15)
);

--#3.Transactions
create table if not exists de12.alpa_stg_transactions( 
	trans_id varchar(20),
	trans_date timestamp(0),
	amt decimal(14,2),
	card_num varchar(25),
	oper_type varchar(20), 
	oper_result varchar(20),
	terminal varchar(20),
	update_dt date
);

--#4.Accounts
create table if not exists de12.alpa_stg_accounts( 
	account_num varchar(40),
	valid_to date,
	client varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

--#5.Cards
create table if not exists de12.alpa_stg_cards( 
	card_num varchar(25),
	account_num varchar(40),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

--#6.Client
create table if not exists de12.alpa_stg_clients(
	client_id varchar(20),
	last_name varchar(40),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0)
);


--#######################################################################################
--# Создание Del table(фиксация удаления) (4 таблицы)(Client,Cards,Accounts,Terminals) #
--#####################################################################################

--#1.Clients
create table if not exists de12.alpa_stg_clients_del (
    client_id varchar(20)
);
--#2.Cards	
create table if not exists de12.alpa_stg_cards_del (
    card_num varchar(25)
);
--#3.Accounts
create table if not exists de12.alpa_stg_accounts_del (
    account varchar(40)
);
--#4.Terminals
create table if not exists de12.alpa_stg_terminals_del (
    terminal_id char(5)
);


--###############################################################################
--# Создание таблицы измерений (4 таблицы) (Accounts,Cards,Clients,Terminals)  #
--#############################################################################

--#1.Accounts
create table if not exists de12.alpa_dwh_dim_accounts_hist( 
	account_num varchar(40),
	valid_to date,
	client varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg int2
);

--#2.Cards
create table if not exists de12.alpa_dwh_dim_cards_hist( 
	card_num varchar(25),
	account_num varchar(40),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg int2
);

--#3.Clients
create table if not exists de12.alpa_dwh_dim_clients_hist(
	client_id varchar(20),
	last_name varchar(40),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg int2
);

--4.Terminals
create table if not exists de12.alpa_dwh_dim_terminals_hist(
	terminal_id varchar(10),
	terminal_type varchar(10),
	terminal_city varchar(30),
	terminal_address varchar(100),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg int2
);


--#########################################################################
--# Создание таблицы фактов (2 таблицы)(Transactions,Passport_blacklist) #
--#######################################################################

--#1.Transactions
create table if not exists de12.alpa_dwh_fact_transactions( 
	trans_id varchar(20),
	trans_date timestamp(0),
	amt decimal(14,2),
	card_num varchar(25),
	oper_type varchar(20), 
	oper_result varchar(20),
	terminal varchar(20),
	update_dt date
);
--#2.Passport_blacklist
create table if not exists de12.alpa_dwh_fact_passport_blacklist( 
	entry_dt date,
	passport_num varchar(15)
);


--#############################
--# Создание таблицы отчетов #
--###########################

--#1.rep_fraud
create table if not exists de12.alpa_rep_fraud(
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar(100),
	phone varchar(20),
	event_type varchar(10),
	report_dt date
);


--################################
--#Создание таблицы метаданных. #
--##############################

--#1.Meta_data
create table if not exists de12.alpa_meta_data(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);


--##################################################
--#Наполнение стартовыми данными таблицы metadata #	
--################################################

--#Clients
insert into de12.alpa_meta_data( schema_name, table_name, max_update_dt )
	values( 'de12','clients', to_timestamp('1900-01-01','YYYY-MM-DD'));

--#Accounts
insert into de12.alpa_meta_data( schema_name, table_name, max_update_dt )
	values( 'de12','accounts', to_timestamp('1900-01-01','YYYY-MM-DD'));

--#Cards
insert into de12.alpa_meta_data( schema_name, table_name, max_update_dt )
	values( 'de12','cards', to_timestamp('1900-01-01','YYYY-MM-DD'));

--#Terminals
insert into de12.alpa_meta_data( schema_name, table_name, max_update_dt )
	values( 'de12','terminals', to_timestamp('1900-01-01','YYYY-MM-DD'));

--#Transactions
insert into de12.alpa_meta_data( schema_name, table_name, max_update_dt )
	values( 'de12','transactions', to_timestamp('1900-01-01','YYYY-MM-DD'));

--#Passport_blacklist
insert into de12.alpa_meta_data( schema_name, table_name, max_update_dt )
	values( 'de12','passports_blacklist', to_timestamp('1900-01-01','YYYY-MM-DD'));

--#Report_table	
insert into de12.alpa_meta_data( schema_name, table_name, max_update_dt )
        values( 'de12','rep_fraud', to_timestamp('1900-01-01','YYYY-MM-DD'));



