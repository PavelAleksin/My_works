1.Написать SQL скрипт для БД "demo" для получения списка броней с указанием для
  каждой брони билетов, а для каждого билета - имени пассажира, номера билета, номера
  рейса, времени вылета, аэропортов улета и прилета, времени в пути и цены билета.
2.Отсортировать по номеру брони, имени пассажира, номеру билета, рейса, времени
  вылета

select
	t.book_ref Номер_брони,
	t.ticket_no Номер_билета,
	passenger_name Имя_пассажира,
	tf.ticket_no Номер_билета,
	flight_no Номер_рейса, 
	scheduled_departure Время_вылета,
	a.airport_name Аэропорт_вылета,
	a2.airport_name Аэропорт_прилета, 
	f.scheduled_arrival - f.scheduled_departure Время_в_полете,
	tf.amount Стоимость_билета
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
