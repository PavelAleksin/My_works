1.�������� SQL ������ ��� ������� ������� "STUDENTS" � ������
	firstname - ���
	lastname - �������
	date_of_birth - ���� ��������
2.�������� SQL ������ ��� �������� ������� "FACULTIES" � ������ name
3.�������� SQL ������, ��������� � ����� �������� �������� (PRIMARY) ����
4.�������� SQL ������, ��������� � � ������� STUDENTS ���� FACULTY � ������� ���� ��� ����� ����� ���� � ��������

--������� ����� ������--

create schema ������
;

--������� ����� �����
set search_path to ������;

--������� ������� STUDENTS--
create table STUDENTS(
	firstname  varchar,
	lastname   varchar,
	date_of_birth date
	)
;

--������� ������� FACULTIES--
create table FACULTIES
	(Name varchar
	)
;

--������� ��������� ����(Primary rey) ��� ����� ������--
alter table students
add id int primary key
;

alter table faculties
add constraint univer_pkey_name primary key (name)
;

--��������� ���� FACULTY--
alter table students 
add faculty varchar
;

--��������� ������� ���� ��� ���� FACULTY �� ����� � ��������--
alter table students 
add constraint foreign_key1
foreign key (faculty) 
references faculties(name)
;
