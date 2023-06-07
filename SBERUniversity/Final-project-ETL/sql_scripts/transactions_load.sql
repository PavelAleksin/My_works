--Загрузка в хранилище

insert into de12.alpa_dwh_fact_transactions(trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal, update_dt)
select 	
			stg.trans_id,
			stg.trans_date,
			stg.card_num,
			stg.oper_type, 
			stg.amt,
			stg.oper_result,
			stg.terminal,
			stg.update_dt 
from de12.alpa_stg_transactions stg
			left join de12.alpa_dwh_fact_transactions tgt
				on stg.trans_id = tgt.trans_id
where tgt.trans_id is null 
	and stg.trans_date is not null
	and stg.card_num is not null
	and stg.oper_type is not null
	and stg.amt is not null
	and stg.oper_result is not null
	and stg.terminal is not null;


--Обновление метаданных

update de12.alpa_meta_data
set max_update_dt = coalesce(( select max( update_dt) from de12.alpa_stg_transactions ),max_update_dt)
where schema_name='de12' and table_name = 'transactions';
