

DATA return;
SET returnp;
if person_id=25391 then identifier='9451MT-8';
if person_id=19707 then location='MTRH                ';
if (index(identifier, 'Am')>0 or index(identifier, 'AM')>0 or index(identifier, 'am')>0 or index(identifier, 'aM')>0)  then LOCATION='Amukura      ';
else if (index(identifier, 'Bf')>0 or index(identifier, 'BF')>0 or index(identifier, 'bf')>0 or index(identifier, 'bF')>0) then LOCATION= 'BurntForest';
else if (index(identifier, 'Bs')>0 or index(identifier, 'BS')>0 or index(identifier, 'bs')>0 or index(identifier, 'bS')>0) then LOCATION= 'Busia';
else if (index(identifier, 'Ch')>0 or index(identifier, 'CH')>0 or index(identifier, 'ch')>0 or index(identifier, 'cH')>0) then LOCATION= 'Chulaimbo';
else if (index(identifier, 'Te')>0 or index(identifier, 'TE')>0 or index(identifier, 'te')>0 or index(identifier, 'tE')>0) then LOCATION= 'Iten';
else if (index(identifier, 'Kb')>0 or index(identifier, 'KB')>0 or index(identifier, 'kb')>0 or index(identifier, 'kB')>0) then LOCATION= 'Kabarnet';
else if (index(identifier, 'Kp')>0 or index(identifier, 'KP')>0 or index(identifier, 'kp')>0 or index(identifier, 'kP')>0) then LOCATION= 'Kapenguria';
else if (index(identifier, 'Kh')>0 or index(identifier, 'KH')>0 or index(identifier, 'kh')>0 or index(identifier, 'kH')>0) then LOCATION= 'Khuyangu';
else if (index(identifier, 'Kt')>0 or index(identifier, 'KT')>0 or index(identifier, 'kt')>0 or index(identifier, 'kT')>0) then LOCATION= 'Kitale';
else if (index(identifier, 'Mt')>0 or index(identifier, 'MT')>0 or index(identifier, 'mt')>0 or index(identifier, 'mT')>0) then LOCATION= 'MTRH';
else if (index(identifier, 'Mp')>0 or index(identifier, 'MP')>0 or index(identifier, 'mp')>0 or index(identifier, 'mP')>0) then LOCATION= 'MTRH4';
else if (index(identifier, 'Eg')>0 or index(identifier, 'EG')>0 or index(identifier, 'eg')>0 or index(identifier, 'eG')>0) then LOCATION= 'MtElgon';
else if (index(identifier, 'Nt')>0 or index(identifier, 'NT')>0 or index(identifier, 'nt')>0 or index(identifier, 'nT')>0) then LOCATION= 'Naitiri';
else if (index(identifier, 'Pv')>0 or index(identifier, 'PV')>0 or index(identifier, 'pv')>0 or index(identifier, 'pV')>0) then LOCATION= 'PortVictoria';
else if (index(identifier, 'Ts')>0 or index(identifier, 'TS')>0 or index(identifier, 'ts')>0 or index(identifier, 'tS')>0) then LOCATION= 'Teso';
else if (index(identifier, 'Tu')>0 or index(identifier, 'TU')>0 or index(identifier, 'tu')>0 or index(identifier, 'tU')>0)then LOCATION= 'Turbo';
else if (index(identifier, 'Wb')>0 or index(identifier, 'WB')>0 or index(identifier, 'wb')>0 or index(identifier, 'wB')>0) then LOCATION= 'Webuye';
else if (index(identifier, 'Ad')>0 or index(identifier, 'AD')>0 or index(identifier, 'ad')>0 or index(identifier, 'aD')>0) then LOCATION= 'Anderson';
else if (index(identifier, 'Dh')>0 or index(identifier, 'DH')>0 or index(identifier, 'dh')>0 or index(identifier, 'dH')>0) then LOCATION= 'DH';
else if (index(identifier, 'Zw')>0 or index(identifier, 'ZW')>0 or index(identifier, 'zw')>0 or index(identifier, 'zW')>0) then LOCATION= 'Ziwa';
else if (index(identifier, 'Bm')>0 or index(identifier, 'BM')>0 or index(identifier, 'bm')>0 or index(identifier, 'bM')>0) then LOCATION= 'Busia';
else if (index(identifier, 'Mo')>0 or index(identifier, 'MO')>0 or index(identifier, 'mo')>0 or index(identifier, 'mO')>0) then LOCATION= 'Mosoriot';
else if (index(identifier, '6-')>0 or index(identifier, '7-')>0 or index(identifier, '8-')>0 or index(identifier, '9-')>0 or
index(identifier, '0-')>0 or index(identifier, '1-')>0 or index(identifier, '2-')>0 or index(identifier, '3-')>0 or index(identifier, '4-')>0 
or index(identifier, '5-')>0) then LOCATION= 'Mosoriot';
RUN;


