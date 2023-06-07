1.Написать SQL скрипт для создния таблицы "STUDENTS" с полями
	firstname - имя
	lastname - фамилия
	date_of_birth - дата рождения
2.Написать SQL скрипт для создания таблицы "FACULTIES" с полями name
3.Написать SQL скрипт, создающий в обеих таблицах основной (PRIMARY) ключ
4.Написать SQL скрипт, создающий в в таблице STUDENTS поле FACULTY и внешний ключ для связи этого поля с таблицей

--создаем схему УНИВЕР--

create schema УНИВЕР
;

--Добавил выбор схемы
set search_path to УНИВЕР;

--создаем таблицу STUDENTS--
create table STUDENTS(
	firstname  varchar,
	lastname   varchar,
	date_of_birth date
	)
;

--создаем таблицу FACULTIES--
create table FACULTIES
	(Name varchar
	)
;

--Создаем Первичный ключ(Primary rey) для обеих таблиц--
alter table students
add id int primary key
;

alter table faculties
add constraint univer_pkey_name primary key (name)
;

--Добавляем поле FACULTY--
alter table students 
add faculty varchar
;

--добавляем внешний ключ для поля FACULTY на связь с таблицей--
alter table students 
add constraint foreign_key1
foreign key (faculty) 
references faculties(name)
;
