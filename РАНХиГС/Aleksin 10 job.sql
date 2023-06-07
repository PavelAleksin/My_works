1.������� ������� "������" ������ (��� int, �������� text, ����� text, ���� int, ���������� int)
2.������� ������� "���������_������" (��� int, �������� text, ����� text, ���� int, ���������� int, ����� timestamp, ������������ text)
3.��������� ������� "������" ����������� ��������
4.������� �������, ������� ��� �������� ������ � ������� ������ ����� ���������� ��������� �������
  � ������� "���������_������" � ��������� ������� �������� � ������������, ������������ ��������

drop schema if exists job_�10 cascade;

create schema job_�10;

create table job_�10.������(
					��� int,
					�������� text,
					����� text,
					���� int,
					���������� int
					);
			

create table  job_�10.���������_������ (
								��� int,
								�������� text,
								����� text, 
								���� int, 
								���������� int, 
								����� timestamp, 
								������������ text);

insert into job_�10.������(���,��������,�����,����,����������)
values
		(1,'������','������',1500,1),
		(2,'������','�����',700,2),
		(3,'�������','��������',500,4);


create or replace function Check_zakaz_dele()
returns trigger
as
$code$
begin
	insert into job_�10.���������_������
	values
			(old.���,
			old.��������,
			old.�����,
			old.����,
			old.����������,
			current_timestamp,
			current_user);
return old;
end;
$code$ 
language plpgsql;


create or replace trigger ������_����� before delete on job_�10.������
for each row execute procedure Check_zakaz_dele();


delete from job_�10.������                        --�������� �����������������--
where ��� = 3;


