
--insert into de12.alpa_rep_fraud (event_dt, passport, fio,  phone, event_type, report_dt)
select  t4.trans_date,
		t1.passport_num,
		t1.last_name || ' ' || t1.first_name  || ' ' || t1.patronymic as fio,
		t1.phone as phone,
		t5.terminal_city,
		t3.card_num,
		lag(t5.terminal_city) over bag as lag_city,
        lag(t4.trans_date) over bag as lag_date,
		case when ( t1.passport_valid_to is not null 
					and t1.passport_valid_to + interval '1 day' < t4.trans_date
					or t1.passport_num in (select passport_num from de12.alpa_dwh_fact_passport_blacklist)) then '1'
			when (t2.valid_to + interval '1 day' < t4.trans_date ) then '2'
			when (lag(t5.terminal_city) over bag <> terminal_city and trans_date - interval '1 hour' < lag(t4.trans_date) over bag ) then '3'
		end  as event_type,
		(select max(max_update_dt)  from de12.alpa_meta_data where table_name in ('transactions','passports_blacklist','terminals'))  report_dt
from de12.alpa_dwh_dim_clients_hist t1
		left join de12.alpa_dwh_dim_accounts_hist t2
		on t1.client_id = t2.client 
		left join de12.alpa_dwh_dim_cards_hist t3
		on t2.account_num = t3.account_num  
		left join de12.alpa_dwh_fact_transactions t4
		on t3.card_num = t4.card_num
		left join de12.alpa_dwh_dim_terminals_hist t5
		on t5.terminal_id = t4.terminal 
where t4.trans_date >= (select max_update_dt from de12.alpa_meta_data where table_name ='rep_fraud') 
window bag as (partition by t4.card_num  order by t4.trans_date)
*/




--1.Совершение операции при просроченном или заблокированном паспорте
insert into de12.alpa_rep_fraud (event_dt, passport, fio,  phone, event_type, report_dt)
select  t4.trans_date,
		t1.passport_num,
		t1.last_name || ' ' || t1.first_name  || ' ' || t1.patronymic as fio,
		t1.phone as phone,
		'1' as event_dt,
		(select max(max_update_dt)  from de12.alpa_meta_data where table_name in ('transactions','passports_blacklist','terminals'))  report_dt
from de12.alpa_dwh_dim_clients_hist t1
		inner join de12.alpa_dwh_dim_accounts_hist t2
		on t1.client_id = t2.client 
		inner join de12.alpa_dwh_dim_cards_hist t3
		on t2.account_num = t3.account_num  
		inner join de12.alpa_dwh_fact_transactions t4
		on t3.card_num = t4.card_num
		and t4.trans_date > (select max_update_dt from de12.alpa_meta_data where table_name ='rep_fraud')
where   t1.passport_valid_to is not null 
		and t1.passport_valid_to + interval '1 day' < t4.trans_date
		or (t1.deleted_flg = '1' and t1.effective_to = to_date('9999-12-31','YYYY-MM-DD'))-- мне кажется это тоже является формой отмены 
		or t1.passport_num in (select passport_num from de12.alpa_dwh_fact_passport_blacklist)
;


--2 Махинация.Cовершение операции при недействующем договоре
insert into de12.alpa_rep_fraud (event_dt, passport, fio,  phone, event_type, report_dt)
select  
		t4.trans_date,
		t1.passport_num,
		t1.last_name || ' ' || t1.first_name  || ' ' || t1.patronymic as fio,
		t1.phone as phone,
		'2' as event_dt,
		(select max(max_update_dt)  from de12.alpa_meta_data where table_name in ('transactions','passports_blacklist','terminals'))  report_dt
from de12.alpa_dwh_dim_clients_hist t1
		inner join de12.alpa_dwh_dim_accounts_hist t2
		on t1.client_id = t2.client 
		inner join de12.alpa_dwh_dim_cards_hist t3
		on t2.account_num = t3.account_num  
		inner join de12.alpa_dwh_fact_transactions t4
		on t3.card_num = t4.card_num
		and t4.trans_date > (select max_update_dt from de12.alpa_meta_data where table_name ='rep_fraud')
		and t4.oper_result = 'SUCCESS'
where 1=0
		or t2.valid_to + interval '1 day' < t4.trans_date
 		or (t2.deleted_flg = '1' and t2.effective_to = to_date('9999-12-31','YYYY-MM-DD'))
;



--3 Махинация. Совершение операций в разных городах в течение одного часа
insert into de12.alpa_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select 
		trans_date as event_dt,
		passport,
		fio,
		phone,
		'3' as event_type,
		(select max(max_update_dt)  from de12.alpa_meta_data where table_name in ('transactions','passports_blacklist','terminals'))  report_dt
from ( select 
				lead(t5.terminal_city) over (partition by t4.card_num order by t4.trans_date) as next_city,
				lag(t5.terminal_city) over (partition by t4.card_num order by t4.trans_date) as prev_city,
				lead(t4.trans_date) over (partition by t4.card_num order by t4.trans_date) as next_date,
				t4.trans_date,
				t4.card_num,
				t4.amt,
				t4.oper_result,
				t5.terminal_city,
				t1.passport_num as passport,
				t1.last_name || ' ' || t1.first_name  || ' ' || t1.patronymic as fio,
				t1.phone
		from de12.alpa_dwh_dim_clients_hist t1
				left join de12.alpa_dwh_dim_accounts_hist t2
				on t1.client_id = t2.client 
				left join de12.alpa_dwh_dim_cards_hist t3
				on t2.account_num = t3.account_num  
				left join de12.alpa_dwh_fact_transactions t4
				on t3.card_num = t4.card_num
				left join de12.alpa_dwh_dim_terminals_hist t5
				on t5.terminal_id = t4.terminal
				and t4.trans_date > (select max_update_dt from de12.alpa_meta_data where table_name ='rep_fraud')
		where t4.oper_result = 'SUCCESS') as tmp
where tmp.prev_city is not null and	(tmp.prev_city <> tmp.terminal_city)
		and extract(epoch from next_date - tmp.trans_date) / 60 < 60
;
;


--4 Махинация.Подбор суммы с уменьшением в течении 20 минут.
insert into de12.alpa_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select 
		trans_date as event_dt,
		passport,
		fio,
		phone,
		'4' as event_type,
		(select max(max_update_dt)  from de12.alpa_meta_data where table_name in ('transactions','passports_blacklist','terminals'))  report_dt        
from (
		select 
				t4.trans_date,
				t4.card_num,
				t4.amt,
				t4.oper_result,
				t1.passport_num as passport,
				t1.last_name || ' ' || t1.first_name  || ' ' || t1.patronymic as fio,
				t1.phone,
				lag(t4.trans_date, 3, '1900-01-01') over (partition by t4.card_num order by t4.trans_date) as lag_3_date,
				lag(t4.oper_result, 1, 'NONE') over (partition by t4.card_num order by t4.trans_date) as lag_result,
				lag(t4.oper_result, 2, 'NONE') over (partition by t4.card_num order by t4.trans_date) as lag_2_result,
				lag(t4.oper_result, 3, 'NONE') over (partition by t4.card_num order by t4.trans_date) as lag_3_result,
				lag(t4.amt, 1, 0) over (partition by t4.card_num order by t4.trans_date) as lag_amt,
				lag(t4.amt, 2, 0) over (partition by t4.card_num order by t4.trans_date) as lag_2_amt,
				lag(t4.amt, 3, 0) over (partition by t4.card_num order by t4.trans_date) as lag_3_amt
		from de12.alpa_dwh_dim_clients_hist t1
				inner join de12.alpa_dwh_dim_accounts_hist t2
				on t1.client_id = t2.client 
				inner join de12.alpa_dwh_dim_cards_hist t3
				on t2.account_num = t3.account_num  
				inner join de12.alpa_dwh_fact_transactions t4
				on t3.card_num = t4.card_num
				inner join de12.alpa_dwh_dim_terminals_hist t5
				on t5.terminal_id = t4.terminal) as  tmp
		where 1 = 1
				and tmp.trans_date > (select max_update_dt  from de12.alpa_meta_data where table_name ='rep_fraud')
				and lag_3_result = 'REJECT'
				and lag_2_result = 'REJECT'
				and lag_result = 'REJECT'
				and oper_result = 'SUCCESS'
				and lag_3_amt > lag_2_amt
				and lag_2_amt > lag_amt
				and lag_amt > amt
				and tmp.trans_date - interval '20 minutes' < lag_3_date;

update de12.alpa_meta_data
set max_update_dt = coalesce(( select max(trans_date) from de12.alpa_dwh_fact_transactions ),max_update_dt)
where schema_name='de12' and table_name = 'rep_fraud';


