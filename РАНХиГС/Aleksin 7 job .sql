1.�������� SQL ������ ��� �� "demo" ��� ��������� ������ ������ � ��������� ���
  ������ ����� �������, � ��� ������� ������ - ����� ���������, ������ ������, ������
  �����, ������� ������, ���������� ����� � �������, ������� � ���� � ���� ������.
2.������������� �� ������ �����, ����� ���������, ������ ������, �����, �������
  ������

select
	t.book_ref �����_�����,
	t.ticket_no �����_������,
	passenger_name ���_���������,
	tf.ticket_no �����_������,
	flight_no �����_�����, 
	scheduled_departure �����_������,
	a.airport_name ��������_������,
	a2.airport_name ��������_�������, 
	f.scheduled_arrival - f.scheduled_departure �����_�_������,
	tf.amount ���������_������
from
	flights f, 
	tickets t,
	bookings b,
	ticket_flights tf,
	airports a,
	airports a2
where 
		t.book_ref = b.book_ref
	and tf.ticket_no = t.ticket_no
	and f.flight_id = tf.flight_id
	and f.departure_airport = a.airport_code
	and f.arrival_airport = a2.airport_code
order by 
		t.book_ref,
		t.passenger_name,
		tf.ticket_no,
		flight_no,
		scheduled_departure
;
