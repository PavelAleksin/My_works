������� �������, �������� �������� N-�� ����� ���� ���������

--�������, �������� �������� N-�� ����� ���� ���������--

create or replace function ����_����_���(����� int)
returns int
as 
$code$
	begin
		if ����� = 1 then
         	return 0;
		end if;
     	if ����� <= 3 then
         	return 1;
		end if;
	return ����_����_���(�����-1) + ����_����_���(�����-2);
end;
$code$
language plpgsql;

--������ ��������� ������� �������� �������� N-�� ����� ���� ���������--

SELECT ����_����_���(10);









