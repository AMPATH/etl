
drop temporary table if exists appointments;
create temporary table appointments
(select patient_id, encounter_datetime, e.encounter_id, value_datetime as next_appointment, e.location_id
from encounter e
left outer join obs o on e.encounter_id = o.encounter_id
where o.concept_id = 5096
and e.voided = 0 and e.encounter_type in (1,2,14,17) and o.voided=0 
);

drop table if exists follow_up;
create table follow_up
(select l.name as clinic, 
a.*, 
e.encounter_datetime as enc_on_exp_return_date, 
e.encounter_id as exp_return_enc_id, 
e2.encounter_id as enc_within_14_enc_id, 
e2.encounter_datetime as enc_within_14_days

from appointments a
left outer join encounter e on a.next_appointment = e.encounter_datetime and e.patient_id = a.patient_id and e.encounter_type in (1,2,14,17) and e.voided=0 
left outer join encounter e2 on e2.patient_id = a.patient_id and abs(timestampdiff(DAY,a.next_appointment,e2.encounter_datetime)) < 14 and e2.encounter_id <> a.encounter_id and e2.encounter_type in (1,2,14,17) and e2.voided=0
inner join location l on a.location_id = l.location_id
order by a.patient_id, encounter_datetime asc
);



drop table if exists lost_to_follow_up;
create table lost_to_follow_up
(select timestampdiff(day,next_appointment,curdate()) as days_since_exp_return_date, f1.*, f2.max_date, f3.max_next_date
from follow_up f1
left outer join (select patient_id,max(encounter_datetime) as max_date from follow_up group by patient_id) f2 on f1.patient_id= f2.patient_id
left outer join (select patient_id,max(next_appointment) as max_next_date from follow_up group by patient_id) f3 on f1.patient_id= f3.patient_id
where f1.encounter_datetime = f2.max_date and f1.next_appointment = f3.max_next_date
and timestampdiff(day,next_appointment,curdate()) > 60
order by clinic, patient_id);

select clinic, count(*) from lost_to_follow_up group by clinic


(select f1.*, f2.max_date, f3.max_next_date
from follow_up f1
left outer join (select patient_id,max(encounter_datetime) as max_date from follow_up group by patient_id) f2 on f1.patient_id= f2.patient_id
left outer join (select patient_id,max(next_appointment) as max_next_date from follow_up group by patient_id) f3 on f1.patient_id= f3.patient_id
where f1.encounter_datetime = f2.max_date and f1.next_appointment = f3.max_next_date
and timestampdiff(day,next_appointment,curdate()) > 60
order by clinic, patient_id);


select a.clinic, made_appt, did_not_make_appt, round(made_appt/(made_appt + did_not_make_appt)*100,1) as percent, lost_to_follow_up from
(select clinic, count(*) as made_appt from follow_up where enc_within_14_days is not null group by clinic) as a
left outer join (select clinic, count(*) as lost_to_follow_up from lost_to_follow_up group by clinic) as b on a.clinic=b.clinic
left outer join (select clinic, count(*) as did_not_make_appt from follow_up where enc_within_14_days is null group by clinic) as c on a.clinic=c.clinic
