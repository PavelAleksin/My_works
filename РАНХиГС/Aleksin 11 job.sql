1.В дополнение к таблицам задания 10 создать таблицы:
"поступления"(код int,
	       товар text,
	       количество int)
"требования"(код int,
	      товар text,
	      сообщение text)
2.Для таблицы "заказы" создать триггер, проверяющий по совокупности поступлений и
  заказов остаток на складе, и создающий новый заказ только в случае достаточного для
  заказа остатка на складе. В случае недостатка товаров запись заказа не создавать, а в
  таблице "требования" сделать запись с требованием закупи недостающих товаров в
  нужном количестве.																			--Очищаем бд--

drop schema if exists job_№11 cascade;
																			--Создаем схему--
create schema job_№11;
																			--Указываем схему--
set search_path to job_№11;
																			--Создаем таблицу заказы--
create table job_№11.заказы(
					код int primary key,
					заказчик text,
					товар text,
					цена int,
					количество int
					);
			
																			--Создаем таблицу удаленные заказы--
create table  job_№11.удаленные_заказы (
								код int primary key ,
								заказчик text,
								товар text, 
								цена int, 
								количество int, 
								время timestamp, 
								пользователь text);
																			--Создаем таблицу поступления--
create table job_№11.поступления(
								код int primary key,
								товар text,
								количество int);
																			--Создаем таблицу требования--
create table job_№11.требования(
								код int,
								товар text,
								сообщение text);

																			--Заносим данные в таблицу заказы--
insert into job_№11.заказы(код,заказчик,товар,цена,количество)
values
		(1,'Иванов','Куртка',1500,1),
		(2,'Петров','Штаны',700,2),
		(3,'Сидоров','Футболка',500,4);
																			--Заносим данные в таблицу поступления--
insert into job_№11.поступления(код,товар,количество)
values
		(1,'Куртка',2),
		(2,'Штаны',3),
		(3,'Футболка',5);

																			--Создаем функцию проверки удаления заказов--
create or replace function Check_zakaz_dele()
returns trigger
as
$code$
begin
	insert into job_№11.удаленные_заказы
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

																			--Создаем функцию проверки количества товара-
create or replace function Check_kolvo_tovar()
returns trigger
as
$code$
DECLARE
	Заказ int;
	Наличие int;
BEGIN 
	SELECT sum(количество) 
						FROM заказы 
									WHERE товар = new.товар INTO Заказ ;
	SELECT sum(количество) 
							FROM поступления 
									WHERE товар = new.товар INTO Наличие ;
				IF  new.количество > ( Наличие - Заказ) 
				THEN  INSERT INTO требования
				VALUES (new.код,
						new.товар,
						'Купить товар '||new.товар||' в количестве = '||(new.количество - (Наличие - Заказ))); 
			RETURN NULL;
		ELSE
	RETURN new;    
	END IF;	
END
$code$ 
language plpgsql;


																			--создаем триггер удаления заказов--
create or replace trigger Удален_заказ before delete on job_№11.заказы
for each row execute procedure Check_zakaz_dele();

																			--создаем триггер проверки количества товара--
create or replace trigger Проверка_наличия before insert on job_№11.заказы
for each row execute procedure Check_kolvo_tovar();



																			--проверка работоспособности 10 задания--
delete from job_№11.заказы                      							   
where код = 3;
																			--проверка работоспособности 11 задания--
insert into job_№11.заказы(код,заказчик,товар,цена,количество)
values
		(4,'Петров','Куртка',1500,4);
	
	
	
	
	

