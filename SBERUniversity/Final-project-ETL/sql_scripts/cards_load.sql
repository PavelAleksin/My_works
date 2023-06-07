
-- Обновление метаданных.

update de12.alpa_meta_data
set max_update_dt = coalesce(( select max( update_dt) from de12.alpa_stg_cards ),max_update_dt)
where schema_name='de12' and table_name = 'cards';

--Загрузка в приемник 
insert into de12.alpa_dwh_dim_cards_hist( card_num,account_num,effective_from, effective_to, deleted_flg)
select 	
			stg.card_num,
			stg.account_num,
			stg.create_dt as effective_from, 
			to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
			0 as deleted_flg 
from de12.alpa_stg_cards stg
			left join de12.alpa_dwh_dim_cards_hist tgt
				on stg.card_num = tgt.card_num
where tgt.card_num is null
	and stg.account_num is not null;

-- Обновление в хранилище записей с изменением 

update de12.alpa_dwh_dim_cards_hist
set effective_to = tmp.update_dt - interval '1 second'
from (select 
					stg.card_num, 
					stg.update_dt
			from de12.alpa_stg_cards stg
				inner join de12.alpa_dwh_dim_cards_hist tgt
					on stg.card_num = tgt.card_num
						and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			where 1=0
						or stg.card_num <> tgt.card_num
						or stg.account_num <> tgt.account_num 
						or ( stg.account_num is null and tgt.account_num is not null ) 	or ( stg.account_num is not null and tgt.account_num is null )
						or ( stg.card_num is null and tgt.card_num is not null ) 	or ( stg.card_num is not null and tgt.card_num is null )) as  tmp
where de12.alpa_dwh_dim_cards_hist.card_num = tmp.card_num
			and de12.alpa_dwh_dim_cards_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ); 

-- Добавление измененной записи с новой effective_to после изменения

insert into de12.alpa_dwh_dim_cards_hist(card_num, account_num,effective_from,effective_to,deleted_flg)
select	
			stg.card_num,
			stg.account_num,
			stg.update_dt as effective_from, 
			to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
			'0' as deleted_flg 
from de12.alpa_stg_cards stg
			inner join de12.alpa_dwh_dim_cards_hist tgt
				on stg.card_num = tgt.card_num
					and tgt.effective_to = stg.update_dt - interval '1 second'
where 1=0
			or stg.card_num <> tgt.card_num
			or stg.account_num <> tgt.account_num 
			or ( stg.account_num is null and tgt.account_num is not null ) 	or ( stg.account_num is not null and tgt.account_num is null )
			or ( stg.card_num is null and tgt.card_num is not null ) 	or ( stg.card_num is not null and tgt.card_num is null );
	
	
--Пометка в хранилище удаленных записей

insert into de12.alpa_dwh_dim_cards_hist(card_num, account_num,effective_from,effective_to,deleted_flg)
select 	
			tgt.card_num,
			tgt.account_num,
			(select max(g.max_update_dt)  from (select max_update_dt from de12.alpa_meta_data where table_name = 'cards') as g limit 1) as effective_from,
			to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
			1 as deleted_flg 
from de12.alpa_dwh_dim_cards_hist tgt
		left join de12.alpa_stg_cards_del stg
			on stg.card_num = tgt.card_num
where stg.card_num is null
		and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and tgt.deleted_flg = '0';

--Обновление удаленной записи

update de12.alpa_dwh_dim_cards_hist
set effective_to = (select max(g.max_update_dt) - interval '1 second' from (select max_update_dt from de12.alpa_meta_data where table_name = 'cards') as g limit 1)
from (	select tgt.card_num
		from de12.alpa_dwh_dim_cards_hist tgt
					left join de12.alpa_stg_cards stg
						on stg.card_num = tgt.card_num
		where stg.card_num is null
					and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
					and tgt.deleted_flg = '0') as tmp
where 	de12.alpa_dwh_dim_cards_hist.	card_num=tmp.card_num
			and de12.alpa_dwh_dim_cards_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
					and de12.alpa_dwh_dim_cards_hist.deleted_flg = '0';


