
-- Обновление метаданных.

update de12.alpa_meta_data
set max_update_dt = coalesce(( select max( update_dt) from de12.alpa_stg_clients ),max_update_dt)
where schema_name='de12' and table_name = 'clients';

--Загрузка в хранилище

insert into de12.alpa_dwh_dim_clients_hist(client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone,effective_from,effective_to,deleted_flg)
select 	
			stg.client_id,
			stg.last_name,
			stg.first_name,
			stg.patronymic,
			stg.date_of_birth,
			stg.passport_num,
			stg.passport_valid_to,
			stg.phone,
			stg.create_dt as effective_from, 
			to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
			0 as deleted_flg 
from de12.alpa_stg_clients stg
			left join de12.alpa_dwh_dim_clients_hist tgt
				on stg.client_id = tgt.client_id
where tgt.client_id is null
	and stg.last_name is not null
	and stg.date_of_birth is not null
	and stg.passport_num is not null
	and stg.phone is not null;


-- Обновление в хранилище записей с изменением 

update de12.alpa_dwh_dim_clients_hist
set effective_to = tmp.update_dt - interval '1 second'
from (select 
					stg.client_id, 
					stg.update_dt
		from de12.alpa_stg_clients stg
				inner join de12.alpa_dwh_dim_clients_hist tgt
					on stg.client_id = tgt.client_id
						and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
		where 1=0
					or stg.client_id <> tgt.client_id
					or stg.last_name <> tgt.last_name 
					or stg.first_name <> tgt.first_name
					or stg.patronymic <> tgt.patronymic
					or stg.passport_num <> tgt.passport_num
					or stg.passport_valid_to <> tgt.passport_valid_to 
					or stg.phone <> tgt.phone
					or ( stg.client_id is null and tgt.client_id is not null ) 	or ( stg.client_id is not null and tgt.client_id is null )
					or ( stg.last_name is null and tgt.last_name is not null ) 	or ( stg.last_name is not null and tgt.last_name is null )
					or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )
					or ( stg.patronymic is null and tgt.patronymic is not null ) 	or ( stg.patronymic is not null and tgt.patronymic is null )
					or ( stg.passport_num is null and tgt.passport_num is not null ) 	or ( stg.passport_num is not null and tgt.passport_num is null )
					or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
					or ( stg.phone is null and tgt.phone is not null ) 	or ( stg.phone is not null and tgt.phone is null )) as tmp
where de12.alpa_dwh_dim_clients_hist.client_id = tmp.client_id
			and de12.alpa_dwh_dim_clients_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ); 

-- Добавление измененной записи с новой effective_to после изменения

insert into de12.alpa_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone,effective_from,effective_to,deleted_flg)
select 	
			stg.client_id,
			stg.last_name,
			stg.first_name,
			stg.patronymic,
			stg.date_of_birth,
			stg.passport_num,
			stg.passport_valid_to,
			stg.phone,
			stg.update_dt as effective_from, 
			to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
			0 as deleted_flg 
from de12.alpa_stg_clients stg
			inner join de12.alpa_dwh_dim_clients_hist tgt
						on stg.client_id = tgt.client_id
							and tgt.effective_to = stg.update_dt - interval '1 second'
where 1=0
			or stg.client_id <> tgt.client_id
			or stg.last_name <> tgt.last_name 
			or stg.first_name <> tgt.first_name
			or stg.patronymic <> tgt.patronymic
			or stg.passport_num <> tgt.passport_num
			or stg.passport_valid_to <> tgt.passport_valid_to 
			or stg.phone <> tgt.phone
			or ( stg.client_id is null and tgt.client_id is not null ) 	or ( stg.client_id is not null and tgt.client_id is null )
			or ( stg.last_name is null and tgt.last_name is not null ) 	or ( stg.last_name is not null and tgt.last_name is null )
			or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )
			or ( stg.patronymic is null and tgt.patronymic is not null ) 	or ( stg.patronymic is not null and tgt.patronymic is null )
			or ( stg.passport_num is null and tgt.passport_num is not null ) 	or ( stg.passport_num is not null and tgt.passport_num is null )
			or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
			or ( stg.phone is null and tgt.phone is not null ) 	or ( stg.phone is not null and tgt.phone is null );
	
	
--Пометка в хранилище удаленных записей

insert into de12.alpa_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone,effective_from,effective_to,deleted_flg)
select 	
			tgt.client_id,
			tgt.last_name,
			tgt.first_name,
			tgt.patronymic,
			tgt.date_of_birth,
			tgt.passport_num,
			tgt.passport_valid_to,
			tgt.phone,
			(select max(g.max_update_dt) from (select max_update_dt from de12.alpa_meta_data where table_name = 'clients') as g limit 1) as effective_from,
			to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
			1 as deleted_flg 
from de12.alpa_dwh_dim_clients_hist tgt
			left join de12.alpa_stg_clients_del stg
				on stg.client_id = tgt.client_id
where stg.client_id is null
			and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
				and tgt.deleted_flg = '0';
		

--Обновление удаленной записи

update de12.alpa_dwh_dim_clients_hist
set effective_to = (select max(g.max_update_dt) - interval '1 second' from (select max_update_dt from de12.alpa_meta_data where table_name = 'clients') as g limit 1)
from  ( select 
					tgt.client_id
			from de12.alpa_dwh_dim_clients_hist tgt
						left join de12.alpa_stg_clients stg
							on stg.client_id = tgt.client_id
			where stg.client_id is null
						and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
						and tgt.deleted_flg = '0') as tmp
where 	de12.alpa_dwh_dim_clients_hist.client_id = tmp.client_id	
			and de12.alpa_dwh_dim_clients_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
				and de12.alpa_dwh_dim_clients_hist.deleted_flg = '0';




