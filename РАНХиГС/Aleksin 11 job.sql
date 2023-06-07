1.� ���������� � �������� ������� 10 ������� �������:
"�����������"(��� int,
	       ����� text,
	       ���������� int)
"����������"(��� int,
	      ����� text,
	      ��������� text)
2.��� ������� "������" ������� �������, ����������� �� ������������ ����������� �
  ������� ������� �� ������, � ��������� ����� ����� ������ � ������ ������������ ���
  ������ ������� �� ������. � ������ ���������� ������� ������ ������ �� ���������, � �
  ������� "����������" ������� ������ � ����������� ������ ����������� ������� �
  ������ ����������.																			--������� ��--

drop schema if exists job_�11 cascade;
																			--������� �����--
create schema job_�11;
																			--��������� �����--
set search_path to job_�11;
																			--������� ������� ������--
create table job_�11.������(
					��� int primary key,
					�������� text,
					����� text,
					���� int,
					���������� int
					);
			
																			--������� ������� ��������� ������--
create table  job_�11.���������_������ (
								��� int primary key ,
								�������� text,
								����� text, 
								���� int, 
								���������� int, 
								����� timestamp, 
								������������ text);
																			--������� ������� �����������--
create table job_�11.�����������(
								��� int primary key,
								����� text,
								���������� int);
																			--������� ������� ����������--
create table job_�11.����������(
								��� int,
								����� text,
								��������� text);

																			--������� ������ � ������� ������--
insert into job_�11.������(���,��������,�����,����,����������)
values
		(1,'������','������',1500,1),
		(2,'������','�����',700,2),
		(3,'�������','��������',500,4);
																			--������� ������ � ������� �����������--
insert into job_�11.�����������(���,�����,����������)
values
		(1,'������',2),
		(2,'�����',3),
		(3,'��������',5);

																			--������� ������� �������� �������� �������--
create or replace function Check_zakaz_dele()
returns trigger
as
$code$
begin
	insert into job_�11.���������_������
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

																			--������� ������� �������� ���������� ������-
create or replace function Check_kolvo_tovar()
returns trigger
as
$code$
DECLARE
	����� int;
	������� int;
BEGIN 
	SELECT sum(����������) 
						FROM ������ 
									WHERE ����� = new.����� INTO ����� ;
	SELECT sum(����������) 
							FROM ����������� 
									WHERE ����� = new.����� INTO ������� ;
				IF  new.���������� > ( ������� - �����) 
				THEN  INSERT INTO ����������
				VALUES (new.���,
						new.�����,
						'������ ����� '||new.�����||' � ���������� = '||(new.���������� - (������� - �����))); 
			RETURN NULL;
		ELSE
	RETURN new;    
	END IF;	
END
$code$ 
language plpgsql;


																			--������� ������� �������� �������--
create or replace trigger ������_����� before delete on job_�11.������
for each row execute procedure Check_zakaz_dele();

																			--������� ������� �������� ���������� ������--
create or replace trigger ��������_������� before insert on job_�11.������
for each row execute procedure Check_kolvo_tovar();



																			--�������� ����������������� 10 �������--
delete from job_�11.������                      							   
where ��� = 3;
																			--�������� ����������������� 11 �������--
insert into job_�11.������(���,��������,�����,����,����������)
values
		(4,'������','������',1500,4);
	
	
	
	
	

