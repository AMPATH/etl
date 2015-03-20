
use amrs_local;
drop temporary table if exists appointments;
create temporary table appointments
(select patient_id, encounter_datetime, e.encounter_id, value_datetime as next_appointment, e.location_id

from encounter e
inner join obs o on e.encounter_id = o.encounter_id
where o.concept_id = 5096
and e.voided = 0
)



drop table if exists follow_up;
create table follow_up
(select l.name as clinic, 
a.*, 
e.encounter_datetime as enc_on_exp_return_date, 
e.encounter_id as exp_return_enc_id, 
e2.encounter_id as enc_within_14_enc_id, 
e2.encounter_datetime as enc_within_14_days

from appointments a
left outer join encounter e on a.next_appointment = e.encounter_datetime and e.patient_id = a.patient_id and e.encounter_type in 
left outer join encounter e2 on e2.patient_id = a.patient_id and abs(timestampdiff(DAY,a.next_appointment,e2.encounter_datetime)) < 30 and e2.encounter_id <> a.encounter_id
inner join location l on a.location_id = l.location_id
order by a.patient_id, encounter_datetime asc
);

# patiets who have recently missed their appointments
select * from follow_up
(select *, count(enc_within_14_enc_id) as num_appts from follow_up 
where next_appointment < '2013-09-20' and next_appointment > '2013-09-01'
group by encounter_id
order by patient_id, encounter_datetime
) t
where num_appts = 0
order by clinic, next_appointment desc

# patients who have not come to clinic with 60 days of their next appointment date.
select f1.*, max(f2.encounter_datetime)
from follow_up f1.*
inner join follow_up f2 on f1.patient_id=f2.patient_id and 
where timestampdiff(day,next_appointment, curdate()) > 60
and encounter_id 
order by patient_id, encounter_datetime desc

select * from follow_up

select count(*) from follow_up where enc_within_7_days is null and next_appointment < '2013-09-20'

select 
sum(if(next_appointment >= '2013-02-01' and next_appointment <= '2013-02-31' and enc_within_7_days is null,1,null)) as February_missed_appt,
sum(if(next_appointment >= '2013-03-01' and next_appointment <= '2013-03-31' and enc_within_7_days is null,1,null)) as March_missed_appt,
sum(if(next_appointment >= '2013-04-01' and next_appointment <= '2013-04-31' and enc_within_7_days is null,1,null)) as April_missed_appt,
sum(if(next_appointment >= '2013-05-01' and next_appointment <= '2013-05-31' and enc_within_7_days is null,1,null)) as May_missed_appt,
sum(if(next_appointment >= '2013-06-01' and next_appointment <= '2013-06-31' and enc_within_7_days is null,1,null)) as June_missed_appt,
sum(if(next_appointment >= '2013-07-01' and next_appointment <= '2013-07-31' and enc_within_7_days is null,1,null)) as July_missed_appt,
sum(if(next_appointment >= '2013-08-01' and next_appointment <= '2013-08-31' and enc_within_7_days is null,1,null)) as August_missed_appt,
sum(if(next_appointment >= '2013-09-01' and next_appointment <= '2013-09-31' and enc_within_7_days is null,1,null)) as September_missed_appt,
sum(if(next_appointment >= '2013-10-01' and next_appointment <= '2013-10-31' and enc_within_7_days is null,1,null)) as October_missed_appt

from follow_up


# Query to determine patients who missed their appointments within the last four days. Note that this is heavily influenced by data entry
select f.*, if(o.value_coded = 1065,'yes','no') as on_arvs 
from follow_up f
left outer join obs o on o.encounter_id = f.encounter_id and o.concept_id =1192 and o.value_coded=1065
where timestampdiff(DAY,next_appointment,curdate()) < 30
and timestampdiff(DAY,next_appointment,curdate()) > 10
order by patient_id
and enc_on_exp_return_date is null
and enc_within_7_days is null


drop temporary table if exists appointment;
create temporary table appointment
(select patient_id,
sum(if(encounter_datetime >= '2013-01-01' and encounter_datetime <= '2013-01-31',1,null)) as January,
sum(if(encounter_datetime >= '2013-02-01' and encounter_datetime <= '2013-02-31',1,null)) as February,
sum(if(encounter_datetime >= '2013-03-01' and encounter_datetime <= '2013-03-31',1,null)) as March,
sum(if(encounter_datetime >= '2013-04-01' and encounter_datetime <= '2013-04-31',1,null)) as April,
sum(if(encounter_datetime >= '2013-05-01' and encounter_datetime <= '2013-05-31',1,null)) as May,
sum(if(encounter_datetime >= '2013-06-01' and encounter_datetime <= '2013-06-31',1,null)) as June,
sum(if(encounter_datetime >= '2013-07-01' and encounter_datetime <= '2013-07-31',1,null)) as July,
sum(if(encounter_datetime >= '2013-08-01' and encounter_datetime <= '2013-08-31',1,null)) as August,
sum(if(encounter_datetime >= '2013-09-01' and encounter_datetime <= '2013-09-31',1,null)) as September,
sum(if(encounter_datetime >= '2013-10-01' and encounter_datetime <= '2013-10-31',1,null)) as October

from encounter e1
where voided=0
group by patient_id
order by patient_id
);

drop temporary table if exists next_appointment;
create temporary table next_appointment
(
select person_id,
sum(if(concept_id = 5096 and value_datetime >= '2013-01-01' and value_datetime <= '2013-01-31',1,null)) as January,
sum(if(concept_id = 5096 and value_datetime >= '2013-02-01' and value_datetime <= '2013-02-31',1,null)) as February,
sum(if(concept_id = 5096 and value_datetime >= '2013-03-01' and value_datetime <= '2013-03-31',1,null)) as March,
sum(if(concept_id = 5096 and value_datetime >= '2013-04-01' and value_datetime <= '2013-04-31',1,null)) as April,
sum(if(concept_id = 5096 and value_datetime >= '2013-05-01' and value_datetime <= '2013-05-31',1,null)) as May,
sum(if(concept_id = 5096 and value_datetime >= '2013-06-01' and value_datetime <= '2013-06-31',1,null)) as June,
sum(if(concept_id = 5096 and value_datetime >= '2013-07-01' and value_datetime <= '2013-07-31',1,null)) as July,
sum(if(concept_id = 5096 and value_datetime >= '2013-08-01' and value_datetime <= '2013-08-31',1,null)) as August,
sum(if(concept_id = 5096 and value_datetime >= '2013-09-01' and value_datetime <= '2013-09-31',1,null)) as September,
sum(if(concept_id = 5096 and value_datetime >= '2013-10-01' and value_datetime <= '2013-10-31',1,null)) as October

from obs
where voided=0
group by person_id
order by person_id
);


drop temporary table if exists missed_appts_by_patient;
create temporary table missed_appts_by_patient
(
select person_id,
(b.February - a.January) as Feb_missed_appts,
(b.March- a.February) as March_missed_appts,
(b.April - a.March) as April_missed_appts,
(b.May - a.April) as May_missed_appts,
(b.June - a.May) as June_missed_appts,
(b.July - a.June) as July_missed_appts,
(b.August - a.July) as August_missed_appts,
(b.September - a.August) as September_missed_appts,
(b.October- a.September) as October_missed_appts

from appointment a
left outer join next_appointment b on a.patient_id = b.person_id
);


# Missed appointments by month summed over all patients
select 
sum(Feb_missed_appts) as Feb_missed_appts,
sum(March_missed_appts) as March_missed_appts,
sum(April_missed_appts) as April_missed_appts,
sum(May_missed_appts) as May_missed_appts,
sum(June_missed_appts) as June_missed_appts,
sum(July_missed_appts) as July_missed_appts,
sum(August_missed_appts) as August_missed_appts,
sum(September_missed_appts) as September_missed_appts,
sum(October_missed_appts) as October_missed_appts
from missed_appts_by_patient


# Missed appointments by month summed over all patients, but only tracking missed appointments, i.e. extra unscheduled appointments don't cancel out missed apppts
select 
sum(if(Feb_missed_appts>0,Feb_missed_appts,0)) as Feb_missed_appts,
sum(if(March_missed_appts>0,March_missed_appts,0)) as March_missed_appts,
sum(if(April_missed_appts>0,April_missed_appts,0)) as April_missed_appts,
sum(if(May_missed_appts>0,May_missed_appts,0)) as May_missed_appts,
sum(if(June_missed_appts>0,June_missed_appts,0)) as June_missed_appts,
sum(if(July_missed_appts>0,July_missed_appts,0)) as July_missed_appts,
sum(if(August_missed_appts>0,August_missed_appts,0)) as August_missed_appts,
sum(if(September_missed_appts>0,September_missed_appts,0)) as September_missed_appts,
sum(if(October_missed_appts>0,October_missed_appts,0)) as October_missed_appts
from missed_appts_by_patient
