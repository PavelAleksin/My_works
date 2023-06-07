1.Создать таблицу "заказы" заказы (код int, заказчик text, товар text, цена int, количество int)
2.Создать таблицу "удаленные_заказы" (код int, заказчик text, товар text, цена int, количество int, время timestamp, пользователь text)
3.Наполнить таблицу "заказы" несколькими записями
4.создать триггер, который при удалении записи в таблице заказы будет копировать удаленную запрись
  в таблицу "удаленные_заказы" с указанием времени удаления и пользователя, выполнившего удаление

drop schema if exists job_№10 cascade;

create schema job_№10;

create table job_№10.заказы(
					код int,
					заказчик text,
					товар text,
					цена int,
					количество int
					);
			

create table  job_№10.удаленные_заказы (
								код int,
								заказчик text,
								товар text, 
								цена int, 
								количество int, 
								время timestamp, 
								пользователь text);

insert into job_№10.заказы(код,заказчик,товар,цена,количество)
values
		(1,'Иванов','Куртка',1500,1),
		(2,'Петров','Штаны',700,2),
		(3,'Сидоров','Футболка',500,4);


create or replace function Check_zakaz_dele()
returns trigger
as
$code$
begin
	insert into job_№10.удаленные_заказы
	values
			(old.код,
			old.заказчик,
			old.товар,
			old.цена,
			old.количество,
			current_timestamp,
			current_user);
return old;
end;
$code$ 
language plpgsql;


create or replace trigger Удален_заказ before delete on job_№10.заказы
for each row execute procedure Check_zakaz_dele();


delete from job_№10.заказы                        --проверка работоспособности--
where код = 3;


