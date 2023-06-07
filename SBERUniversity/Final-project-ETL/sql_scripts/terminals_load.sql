--Обновление метаданных

update de12.alpa_meta_data
set max_update_dt = coalesce(
    ( select max( update_dt ) from de12.alpa_stg_terminals ),max_update_dt)
where schema_name='de12' and table_name = 'terminals';

--Загрузка в хранилище

insert into de12.alpa_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select
		stg.terminal_id,
		stg.terminal_type,
		stg.terminal_city,
		stg.terminal_address,
		stg.update_dt as effective_from,
		to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
		0 as deleted_flg
from de12.alpa_stg_terminals stg
		left join de12.alpa_dwh_dim_terminals_hist tgt
			on stg.terminal_id = tgt.terminal_id
where tgt.terminal_id is null and stg.terminal_id <> '0' and stg.terminal_city <> '0' and stg.terminal_address <> '0';

-- Обновление в хранилище записей с изменением 

update de12.alpa_dwh_dim_terminals_hist
set effective_to = tmp.update_dt- interval '1 second'
from (select 
						stg.terminal_id,
						stg.update_dt
			from de12.alpa_stg_terminals stg
					inner join de12.alpa_dwh_dim_terminals_hist tgt
						on stg.terminal_id = tgt.terminal_id
							and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			where	1=0
						or stg.terminal_type <> tgt.terminal_type 
						or stg.terminal_city <> tgt.terminal_city 
						or stg.terminal_address <> tgt.terminal_address 
						or ( stg.terminal_type is null and tgt.terminal_type is not null ) 	or ( stg.terminal_type is not null and tgt.terminal_type is null )
						or ( stg.terminal_city is null and tgt.terminal_city is not null ) 	or ( stg.terminal_city is not null and tgt.terminal_city is null )
						or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )) as tmp
where de12.alpa_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
		and de12.alpa_dwh_dim_terminals_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ); 

 	
-- Добавление измененной записи с новой effective_to после изменения

insert into de12.alpa_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select 
			stg.terminal_id,
			stg.terminal_type,
			stg.terminal_city,
			stg.terminal_address,
			stg.update_dt as effective_from,
			to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
			0 as deleted_flg
from de12.alpa_stg_terminals stg
		inner join de12.alpa_dwh_dim_terminals_hist tgt
			on stg.terminal_id = tgt.terminal_id
				and tgt.effective_to = stg.update_dt - interval '1 second'
where 1=0
		or stg.terminal_type <> tgt.terminal_type 
		or stg.terminal_city <> tgt.terminal_city 
		or stg.terminal_address <> tgt.terminal_address 
		or ( stg.terminal_type is null and tgt.terminal_type is not null ) 	or ( stg.terminal_type is not null and tgt.terminal_type is null )
		or ( stg.terminal_city is null and tgt.terminal_city is not null ) 	or ( stg.terminal_city is not null and tgt.terminal_city is null )
		or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null );



--Пометка в хранилище удаленных записей
	
insert into de12.alpa_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select 
			tgt.terminal_id,
			tgt.terminal_type,
			tgt.terminal_city,
			tgt.terminal_address,
			(select max(g.max_update_dt) from (select max_update_dt from de12.alpa_meta_data where table_name = 'terminals' ) as g limit 1)  as effective_from,
			to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to,
			1 as deleted_flg
from de12.alpa_dwh_dim_terminals_hist tgt
			left join de12.alpa_stg_terminals_del stg
			on stg.terminal_id = tgt.terminal_id
where stg.terminal_id is null
			and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and tgt.deleted_flg = '0';


-- Обновление удаленной записи

update de12.alpa_dwh_dim_terminals_hist
set effective_to = (select max(g.max_update_dt) - interval '1 second' from (select max_update_dt from de12.alpa_meta_data where table_name = 'terminals') as g limit 1) 
from (select 
					tgt.terminal_id
			from de12.alpa_dwh_dim_terminals_hist tgt
					left join de12.alpa_stg_terminals_del stg
						on stg.terminal_id = tgt.terminal_id
			where stg.terminal_id is null
					and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
					and tgt.deleted_flg = '0') as tmp
where de12.alpa_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
			and  de12.alpa_dwh_dim_terminals_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
				and  de12.alpa_dwh_dim_terminals_hist.deleted_flg = '0';

--Обновление в хранилище удаленной записи если добавили туже запись без изменений
			
update de12.alpa_dwh_dim_terminals_hist
set effective_to = tmp.update_dt- interval '1 second'
from (select 
						stg.terminal_id,
						stg.update_dt
			from de12.alpa_stg_terminals stg
					inner join de12.alpa_dwh_dim_terminals_hist tgt
						on stg.terminal_id = tgt.terminal_id
							and stg.terminal_type = tgt.terminal_type 
								and stg.terminal_city = tgt.terminal_city 
									and stg.terminal_address = tgt.terminal_address
										and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			where	tgt.effective_from <> stg.update_dt and deleted_flg= '1'
						and stg.terminal_type = tgt.terminal_type
														and stg.terminal_city = tgt.terminal_city 
															and stg.terminal_address = tgt.terminal_address
						or ( stg.terminal_type is null and tgt.terminal_type is not null ) 	or ( stg.terminal_type is not null and tgt.terminal_type is null )
						or ( stg.terminal_city is null and tgt.terminal_city is not null ) 	or ( stg.terminal_city is not null and tgt.terminal_city is null )
						or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )) as tmp
where de12.alpa_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
		and de12.alpa_dwh_dim_terminals_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' );

--Добавление в хранилище записи с новой effective_to после удаления если добавили туже запись без изменений

insert into de12.alpa_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select 
			stg.terminal_id,
			stg.terminal_type,
			stg.terminal_city,
			stg.terminal_address,
			stg.update_dt as effective_from,
			to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
			0 as deleted_flg
from de12.alpa_stg_terminals stg
		inner join de12.alpa_dwh_dim_terminals_hist tgt
			on stg.terminal_id = tgt.terminal_id
				and tgt.effective_to = stg.update_dt - interval '1 second'
where 1=0
		or (tgt.effective_from <> stg.update_dt and deleted_flg= '1'
													and stg.terminal_type = tgt.terminal_type
														and stg.terminal_city = tgt.terminal_city 
															and stg.terminal_address = tgt.terminal_address)
		or ( stg.terminal_type is null and tgt.terminal_type is not null ) 	or ( stg.terminal_type is not null and tgt.terminal_type is null )
		or ( stg.terminal_city is null and tgt.terminal_city is not null ) 	or ( stg.terminal_city is not null and tgt.terminal_city is null )
		or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null );




