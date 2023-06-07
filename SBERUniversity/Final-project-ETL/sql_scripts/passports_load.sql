--Загрузка в хранилище

insert into de12.alpa_dwh_fact_passport_blacklist( passport_num, entry_dt )
select 	
			stg.passport_num,
			stg.entry_dt 
from de12.alpa_stg_passport_blacklist stg
			left join de12.alpa_dwh_fact_passport_blacklist tgt
				on stg.passport_num = tgt.passport_num
where tgt.passport_num is null and stg.passport_num <> '0';


--Обновление метаданных

update de12.alpa_meta_data
set max_update_dt = coalesce(( select max( entry_dt ) from de12.alpa_stg_passport_blacklist ),max_update_dt)
where schema_name='de12' and table_name = 'passports_blacklist';
