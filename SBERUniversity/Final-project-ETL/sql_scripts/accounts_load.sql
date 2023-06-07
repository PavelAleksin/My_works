-- Обновление метаданных.

update de12.alpa_meta_data
set max_update_dt = coalesce(( select max( update_dt) from de12.alpa_stg_accounts ),max_update_dt)
where schema_name='de12' and table_name = 'accounts';

--Загрузка в приемник

insert into de12.alpa_dwh_dim_accounts_hist(account_num, valid_to, client,effective_from,effective_to,deleted_flg)
select	stg.account_num,
	stg.valid_to,
	stg.client,
	stg.create_dt as effective_from, 
	to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
	0 as deleted_flg
from de12.alpa_stg_accounts stg
	left join de12.alpa_dwh_dim_accounts_hist tgt
		on stg.account_num = tgt.account_num
where tgt.account_num is null
	and stg.valid_to is not null
	and stg.client is not null;

-- Обновление в хранилище записей с изменением 

update de12.alpa_dwh_dim_accounts_hist
set effective_to = tmp.update_dt - interval '1 second'
from (select 
					stg.account_num, 
					stg.update_dt
			from de12.alpa_stg_accounts stg
						inner join de12.alpa_dwh_dim_accounts_hist tgt
							on stg.account_num = tgt.account_num
								and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			where 1=0
						or stg.account_num <> tgt.account_num
						or stg.valid_to <> tgt.valid_to 
						or stg.client <> tgt.client
						or ( stg.account_num is null and tgt.account_num is not null ) 	or ( stg.account_num is not null and tgt.account_num is null )
						or ( stg.valid_to is null and tgt.valid_to is not null ) 	or ( stg.valid_to is not null and tgt.valid_to is null )
						or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null )) as tmp
where de12.alpa_dwh_dim_accounts_hist.account_num = tmp.account_num
			and de12.alpa_dwh_dim_accounts_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ); 

-- Добавление измененной записи с новой effective_to после изменения

insert into de12.alpa_dwh_dim_accounts_hist(account_num, valid_to, client,effective_from,effective_to,deleted_flg)
select 
		stg.account_num,
		stg.valid_to,
		stg.client,
		stg.update_dt as effective_from, 
		to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
		0 as deleted_flg 
from de12.alpa_stg_accounts stg
			inner join de12.alpa_dwh_dim_accounts_hist tgt
				on stg.account_num = tgt.account_num
					and tgt.effective_to = stg.update_dt - interval '1 second'
where  1=0
			or stg.account_num <> tgt.account_num
			or stg.valid_to <> tgt.valid_to 
			or stg.client <> tgt.client
			or ( stg.account_num is null and tgt.account_num is not null ) 	or ( stg.account_num is not null and tgt.account_num is null )
			or ( stg.valid_to is null and tgt.valid_to is not null ) 	or ( stg.valid_to is not null and tgt.valid_to is null )
			or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null );
	
	
--Пометка в хранилище удаленных записей

insert into de12.alpa_dwh_dim_accounts_hist(account_num, valid_to, client,effective_from,effective_to,deleted_flg)
select 
			tgt.account_num,
			tgt.valid_to,
			tgt.client,
			(select max(g.max_update_dt) from (select max_update_dt from de12.alpa_meta_data where table_name = 'accounts' ) as g limit 1)  as effective_from,
			to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
			1 as deleted_flg 
from de12.alpa_dwh_dim_accounts_hist tgt
			left join de12.alpa_stg_accounts_del stg
				on stg.account = tgt.account_num
where stg.account is null
			and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and tgt.deleted_flg = '0';


-- Обновление удаленной записи

update de12.alpa_dwh_dim_accounts_hist
set effective_to = (select max(g.max_update_dt)- interval '1 second' from (select max_update_dt from de12.alpa_meta_data where table_name = 'accounts' ) as g limit 1) 
from (Select 
					tgt.account_num
			from de12.alpa_dwh_dim_accounts_hist tgt
					left join de12.alpa_stg_accounts stg
						on stg.account_num = tgt.account_num
			where stg.account_num is null
						and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
						and tgt.deleted_flg = '0') as tmp
where de12.alpa_dwh_dim_accounts_hist.account_num = tmp.account_num
			and de12.alpa_dwh_dim_accounts_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
				and de12.alpa_dwh_dim_accounts_hist.deleted_flg = '0';



