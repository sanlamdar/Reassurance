

-----script pour sp tranche edp


--Table garantie et risque
delete from  ACTUARY.PN_GARANTIE_RISQUE 
--where TO_CHAR (DATECOMP,'yyyymm')=month_id
;

insert into actuary.PN_GARANTIE_RISQUE  (

drop table actuary.PN_GARANTIE_RISQUE;

create table actuary.PN_GARANTIE_RISQUE as (
select 
  TYPEMOUV,
   GENRMOUV,
   CODTYPIN,
   i.NUMEQUIT,
   CODTYPQU,
   LIBTYPIN,
   i.CODEINTE,
   RAISOCIN,
   CODEASSU,
   NOM_ASSU,
   CODEBRAN,
   LIBEBRAN,
   CODECATE,
   LIBECATE,
   i.NUMEPOLI,
   CODERISQ,
   CODEGARA,
   --NVL(NUMEAVEN,0) NUMEAVEN,
   NUMEAVEN_G,
   DATEEFFE,
   DATEECHE,
   DATECOMP,
   PRIMNETT PRIMNETT_POLICE,
   PRINETCO PRINETCO_POLICE,
   CHIFAFFA CHIFAFFA_POLICE,
   decode (TYPEMOUV,'ANNULATIONS',-primnette,primnette) primnett,
   case when nvl(PRIMNETT,0)=0 then decode (TYPEMOUV,'ANNULATIONS',-primnette,primnette) 
           else nvl(PRINETCO,0)/nvl(PRIMNETT,0)*decode (TYPEMOUV,'ANNULATIONS',-primnette,primnette) end primnetco --,
 
      
 from 
(select* 
 from  orass_v6.v_ch_affaire
 where   TO_CHAR (DATECOMP,'yyyymm')>='1990'
  -- and codebran=4
 and  codeinte  not in (9999,9998,9995) and genrmouv<>'C' and codebran not in (4,81,82,83,89))i,
 (
select
  CODEINTE, 
  NUMEPOLI,
  NUMEQUIT,
  NVL(NUMEAVEN,0) NUMEAVEN_G,
  CODERISQ,
  CODECATE CODECATE_G,
  CODEGARA,
  sum(primnett) primnette
  from orass_v6.prime_garantie
  where
  CODEINTE not in (9999,9998,9995) 
  group by
  CODEINTE, 
  NUMEPOLI,
  NUMEQUIT,
  NVL(NUMEAVEN,0) ,
  CODERISQ,
  CODECATE,
  CODEGARA
 --AND NUMEPOLI = 4000000271 and 
)j

where i.CODEINTE=j.CODEINTE(+) 
and   i.NUMEPOLI=j.NUMEPOLI(+) 
and   i.NUMEQUIT=j.NUMEQUIT(+) 
);
  

delete from  ACTUARY.LAST_RISQ;
insert into  ACTUARY.LAST_RISQ
create table ACTUARY.LAST_RISQ as (
select
   i.CODEINTE,
   i.NUMEPOLI,
   i.CODERISQ,
   avenant_max,
   LIBERISQ,
   MARQVEHI,
   TYPEVEHI,
   NUMEIMMA,
   DATE_MEC,
   CARRVEHI,
   CODGENAU,
   CODEZONE,
   TYPEMOTE,
   NUMECHAS,
   PUISVEHI,
   POIDVEHI,
   CYLIVEHI,
   VITEVEHI,
   NOMBPLAC,
   CAPRIS01,
   CAPRIS02,
  NOMPLAIN,
   DATEENTR,
   DATESORT,
   CREE__LE,
   MODI_PAR,
   MODI__LE,
   CATERISQ

from
(select 
        CODEINTE,
        NUMEPOLI,
        CODERISQ,
      nvl( max(avenmodi),0) avenant_max
   from hist_risque
   
group by CODEINTE,
        NUMEPOLI,
        CODERISQ)i,   
(select *
 from hist_risque)j
        
 where i.CODEINTE=j.CODEINTE(+) 
and    i.NUMEPOLI=j.NUMEPOLI(+) 
and    i.CODERISQ=j.CODERISQ(+) 
and    i.avenant_max=nvl(j.avenmodi(+),0)

);  


delete from  actuary.PN_GAR_RISQUE;
drop table  actuary.PN_GAR_RISQUE;
insert into actuary.PN_GAR_RISQUE  

---create table PN_GAR_RISQUE as (select *from actuary.PN_AUTO_RISQUE)
create table actuary.PN_GAR_RISQUE as (select 

   i.TYPEMOUV,
   GENRMOUV,
   CODTYPIN,
   i.NUMEQUIT,
   CODTYPQU,
   LIBTYPIN,
   i.CODEINTE,
   RAISOCIN,
   CODEASSU,
   NOM_ASSU,
   CODEBRAN,
   LIBEBRAN,
   CODECATE,
   LIBECATE,
   i.NUMEPOLI,
   i.CODERISQ,
   CODEGARA,
   NUMEAVEN_G NUMEAVEN,
   avenmodi,
   avenant_max,
  -- NUMEAVEN_G NUMEAVEN,
   DATEEFFE,
   DATEECHE,
   DATECOMP,
   PRIMNETT_POLICE,
   PRINETCO_POLICE,
   CHIFAFFA_POLICE, 
   primnett,
   primnetco,
  case when nvl(primnett1,0)=0 then PRIMNETT_POLICE
      else nvl(primnett,0)/nvl(primnett1,0)*  PRIMNETT_POLICE end primnett2, 
   case when nvl(primnetco1,0)=0 then PRINETCO_POLICE
      else nvl (primnetco,0)/nvl(primnetco1,0)*  PRINETCO_POLICE end primnetco2, 
   case when nvl(primnett1,0)=0 then CHIFAFFA_POLICE
      else nvl(primnett,0)/nvl(primnett1,0)*  CHIFAFFA_POLICE end CHIFAFFA,   
   case when  avenmodi is null then k.LIBERISQ   else j.LIBERISQ    end LIBERISQ, 
  case when  avenmodi is null  then k.MARQVEHI	 else j.MARQVEHI  end   MARQVEHI,
  case when  avenmodi is null  then k.TYPEVEHI	 else j.TYPEVEHI  end   TYPEVEHI,
  case when  avenmodi is null  then k.NUMEIMMA	 else j.NUMEIMMA  end   NUMEIMMA,
  case when  avenmodi is null  then k.DATE_MEC	 else j.DATE_MEC  end   DATE_MEC,
  case when  avenmodi is null  then k.CARRVEHI	 else j.CARRVEHI  end   CARRVEHI,
  case when  avenmodi is null  then k.CODGENAU	 else j.CODGENAU  end   CODGENAU,
  case when  avenmodi is null  then k.CODEZONE	 else j.CODEZONE  end   CODEZONE,
  case when  avenmodi is null  then k.TYPEMOTE	 else j.TYPEMOTE  end   TYPEMOTE,
  case when  avenmodi is null  then k.NUMECHAS	 else j.NUMECHAS  end   NUMECHAS,
  case when  avenmodi is null  then k.PUISVEHI	 else j.PUISVEHI  end   PUISVEHI,
  case when  avenmodi is null  then k.POIDVEHI	 else j.POIDVEHI  end   POIDVEHI,
  case when  avenmodi is null  then k.CYLIVEHI	 else j.CYLIVEHI  end   CYLIVEHI,
  case when  avenmodi is null  then k.VITEVEHI	 else j.VITEVEHI  end   VITEVEHI,
  case when  avenmodi is null  then k.NOMBPLAC	 else j.NOMBPLAC  end   NOMBPLAC,
  case when  avenmodi is null  then k.CAPRIS01	 else j.CAPRIS01  end   CAPRIS01,
  case when  avenmodi is null  then k.CAPRIS02	 else j.CAPRIS02  end   CAPRIS02,
  case when  avenmodi is null  then k.NOMPLAIN	 else j.NOMPLAIN  end   NOMPLAIN,
  case when  avenmodi is null  then k.DATEENTR	 else j.DATEENTR  end   DATEENTR,
  
  case when  avenmodi is null  then k.DATESORT	 else j.DATESORT  end   DATESORT,
  case when  avenmodi is null  then k.CREE__LE	 else j.CREE__LE  end   CREE__LE,
  case when  avenmodi is null  then k.MODI_PAR	 else j.MODI_PAR  end   MODI_PAR,
  case when  avenmodi is null  then k.MODI__LE	 else j.MODI__LE  end   MODI__LE,
  case when  avenmodi is null  then k.CATERISQ	 else j.CATERISQ  end   CATERISQ
--J.CODUSAAU
from

(select* 
from actuary.PN_GARANTIE_RISQUE) i,
(select CODEINTE,
NUMEPOLI,
CODERISQ,
nvl(avenmodi,0) avenmodi,
LIBERISQ,
MARQVEHI,
TYPEVEHI,
NUMEIMMA,
DATE_MEC,
CARRVEHI,
CODGENAU,
CODEZONE,
TYPEMOTE,
NUMECHAS,
PUISVEHI,
POIDVEHI,
CYLIVEHI,
VITEVEHI,
NOMBPLAC,
CAPRIS01,
CAPRIS02,
NOMPLAIN,
DATEENTR,
DATESORT,
CREE__LE,
MODI_PAR,
MODI__LE,
--CODEASSU,
CODUSAAU,
CATERISQ

from hist_risque) j,
(select CODEINTE,NUMEPOLI, CODERISQ,avenant_max,
LIBERISQ,
MARQVEHI,
TYPEVEHI,
NUMEIMMA,
DATE_MEC,
CARRVEHI,
CODGENAU,
CODEZONE,
TYPEMOTE,
NUMECHAS,
PUISVEHI,
POIDVEHI,
CYLIVEHI,
VITEVEHI,
NOMBPLAC,
CAPRIS01,
CAPRIS02,
NOMPLAIN,
DATEENTR,
DATESORT,
--CODUSAAU,
CREE__LE,
MODI_PAR,
MODI__LE,
CATERISQ

from  actuary.LAST_RISQ) k,

(select
   *from actuary.pn_all)h


where i.CODEINTE=j.CODEINTE(+) 
and   i.NUMEPOLI=j.NUMEPOLI(+)
and   i.CODERISQ=j.CODERISQ(+)
and   i.NUMEAVEN_G=j.avenmodi(+)

and   i.CODEINTE=k.CODEINTE(+) 
and   i.NUMEPOLI=k.NUMEPOLI(+)
and   i.CODERISQ=k.CODERISQ(+)


and   i.CODEINTE=h.CODEINTE(+) 
and   i.NUMEPOLI=h.NUMEPOLI(+) 
and   i.NUMEQUIT=h.NUMEQUIT(+) 
and   i.TYPEMOUV=h.TYPEMOUV(+) 
)
 
;




drop table actuary.PN_GAR_RISQUE_1;

create table actuary.PN_GAR_RISQUE_1

as

(
select

i.*

,case when LIBEL_BRAN_RE='Incendie' and MONT_SMP <=1000000000 then '[0-1 000 M] INC'
      when LIBEL_BRAN_RE='Incendie' and MONT_SMP <=3000000000 then ']1 000 M -3 000 M ] INC'
     when LIBEL_BRAN_RE='Incendie' and MONT_SMP <=7000000000 then ']3 000 M -7 000 M] INC'
     when LIBEL_BRAN_RE='Incendie' and MONT_SMP <=14000000000 then ']7 000 M -14 000 M] INC'
      when LIBEL_BRAN_RE='Incendie' and MONT_SMP >14000000000 then 'sup à 14 000 M INC'
     
       when LIBEL_BRAN_RE='BDM - TRI' and MONT_SMP <=500000000 then '[0-500 M] BDM - TRI'
      when LIBEL_BRAN_RE='BDM - TRI' and MONT_SMP <=2000000000 then ']500 M -2 000 M] BDM - TRI'
      when LIBEL_BRAN_RE='BDM - TRI' and MONT_SMP <=4000000000 then ']2 000 M -4 000 M] BDM - TRI'
      when LIBEL_BRAN_RE='BDM - TRI' and MONT_SMP >4000000000 then 'sup à 4 000 M BDM - TRI'
      
      when LIBEL_BRAN_RE='TRM - TRC' and MONT_SMP <=1000000000 then '[0-1 000 M] TRM - TRC'
       when LIBEL_BRAN_RE='TRM - TRC' and MONT_SMP <=4000000000 then ']1 000 M -4 000 M] TRM - TRC'
       when LIBEL_BRAN_RE='TRM - TRC' and MONT_SMP <=8000000000 then ']4 000 M -8 000 M] TRM - TRC'
       when LIBEL_BRAN_RE='TRM - TRC' and MONT_SMP >8000000000 then 'sup à 8 000 M TRM - TRC'
       
       when LIBEL_BRAN_RE='Risques Divers DDE- BDG -VOL' and MONT_SMP <=250000000 then '[0-250 M] RD'
        when LIBEL_BRAN_RE='Risques Divers DDE- BDG -VOL' and MONT_SMP <=1000000000 then ']250 M -1 000 M] RD'
        when LIBEL_BRAN_RE='Risques Divers DDE- BDG -VOL' and MONT_SMP <=2000000000 then ']1 000 M -2 000 M] RD'
        when LIBEL_BRAN_RE='Risques Divers DDE- BDG -VOL' and MONT_SMP >2000000000 then 'sup à 2 000 M RD'

      else 'NONE'
      end Tranche_Retention
      

    
from


(
select
i.CODEINTE,i.CODEBRAN,i.NUMEPOLI,i.NUMEAVEN,i.CODEGARA,i.CODERISQ,DATEEFFE,DATEECHE,DATECOMP,i.CODE_BRAN_RE,PRIMNETCO2,CHIFAFFA
,MONT_SMP,LIBEL_BRAN_RE

from
(
select

i.CODEINTE,i.CODEBRAN,i.NUMEPOLI,i.NUMEAVEN,i.CODEGARA,i.CODERISQ,DATEEFFE,DATEECHE,DATECOMP,CODE_BRAN_RE,PRIMNETCO2,CHIFAFFA
,MONT_SMP
--,CODBRARE

from

(
select *from actuary.PN_GAR_RISQUE where  codebran not in (4,81,82,83,89)
)i,


(select CODEGARA, CODBRARE CODE_BRAN_RE from reass_db.TB_CODEGARA_CODBRANRE)k,

(select  distinct CODEINTE,NUMEPOLI,nvl(NUMEAVEN,0) NUMEAVEN ,CODERISQ
,max(MONT_SMP) MONT_SMP
--,MONT_SMP 
from ORASS_V6.SMP_RISQUE

group by
  CODEINTE,NUMEPOLI,nvl(NUMEAVEN,0)  ,CODERISQ
)j


where

      i.CODEGARA=k.CODEGARA(+)
and   i.CODEINTE=  j.CODEINTE(+) and
      I.NUMEPOLI=J.NUMEPOLI(+) and
      I.NUMEAVEN=J.NUMEAVEN(+) and
      I.CODERISQ=J.CODERISQ(+) 
 )i,     
      (select distinct CODE_BRAN_RE,LIBEL_BRAN_RE from REASS_DB.PRIME_CEDEE_ANT2021 where CODE_BRAN_RE is not null ) t

      where i.CODE_BRAN_RE=t.CODE_BRAN_RE(+)

)i


);







delete from actuary.tb_prime_ref_gar;
drop table actuary.tb_prime_ref_gar; ---create table tb_prime_ref_gar as (select  *from tb_prime_ref_auto)
insert into actuary.tb_prime_ref_gar  (

create table actuary.tb_prime_ref_gar as (
select 

       nvl(CODEBRAN,   0) CODEBRAN,
       nvl(BRANCHE1,   0) BRANCHE1,
       nvl(BRANCHE2,   0) BRANCHE2,
       nvl(NUM_POLICE, 0) NUM_POLICE, 
       nvl(NUMEPOLI,   0) NUMEPOLI,
       nvl(CODEINTE,   0) CODEINTE,
      -- nvl(NUMEAVEN,   0) NUMEAVEN,
       nvl(CODERISQ, 0) CODERISQ, 
       nvl(CODE_BRAN_RE,0) CODE_BRAN_RE,
       nvl(LIBEL_BRAN_RE,0) LIBEL_BRAN_RE,
        IDPOLICE,MIN_YEAR
       , Tranche_Retention,
       DATEECHE,
       min(DATEEFFE)    DATEEFFE,
       min(datecomp)  datecomp,
       (trunc(DATEECHE)- trunc(min(DATEEFFE))+1) /365.25 EXPO,          
       --sum(PRIMNETT2)    PRIMNETT_ALL,
       sum(PRIMNETCO2) PRIMNET,
       sum(CHIFAFFA)    CHIFAFFA
       --sum(COMMISSI)    COMMISSI,
      -- sum(REGLCOMM)     REGLCOMM,
     --  sum(COMMISS_CEDEE)         COMMISS_CEDEE,
     --  sum(COMISS_NETTE_DE_COASS) COMISS_NETTE_DE_COASS,
     --  sum(COMMISSION_APPORTEUR)  COMMISSION_APPORTEUR,
      -- sum(COMMGEST) COMMGEST
from

(
select
CODEBRAN,
BRANCHE1,
BRANCHE2,
CODEINTE,
NUMEPOLI,
nvl(CODEINTE, 0)||nvl(NUMEPOLI, 0) NUM_POLICE,CODEGARA,
--nvl(NUMEAVEN,   0) NUMEAVEN, 
CODERISQ,
       nvl(CODE_BRAN_RE,0) CODE_BRAN_RE,
       nvl(LIBEL_BRAN_RE,0) LIBEL_BRAN_RE
       , Tranche_Retention,
 nvl(CODEINTE, 0)||nvl(NUMEPOLI, 0)||nvl(CODERISQ,0)||nvl(to_char(DATEECHE),'0') IDPOLICE,
 trunc(DATEECHE) DATEECHE,
 trunc(DATEEFFE) DATEEFFE,
  trunc(DATECOMP) DATECOMP,
PRIMNETCO2,
--PRIME_CEDEE,
CHIFAFFA,
--,
--COMMISSI,
-- REGLCOMM,
--COMMISS_CEDEE,
--COMISS_NETTE_DE_COASS,
--COMMISSION_APPORTEUR,
--COMMGEST,
MIN_YEAR
from
(select *from actuary.PN_GAR_RISQUE_1 where  PRIMNETCO2<>0  )i,

--(select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto)h,
(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j,
(select EXTRACT(YEAR FROM min(DATEEFFE)) MIN_YEAR from actuary.PN_GAR_RISQUE_1 )k
where i.CODEBRAN=j.CBR(+) 
---and nvl(i.CODEGARA,0)=h.VAR(+)

)
group by
 nvl(CODEBRAN,   0)       ,
       nvl(BRANCHE1,   0) ,
       nvl(BRANCHE2,   0) ,
       nvl(NUM_POLICE, 0) , 
       nvl(NUMEPOLI,   0) ,
       nvl(CODEINTE,   0) ,
      -- nvl(NUMEAVEN,   0) NUMEAVEN,
       nvl(CODERISQ, 0)    ,
       nvl(CODE_BRAN_RE,0) ,
       nvl(LIBEL_BRAN_RE,0),
        IDPOLICE,MIN_YEAR
       , Tranche_Retention,
       DATEECHE
       
 ); 


---Prime et expo par annee d'acquisition sur l'auto


delete from  actuary.tb_prime_acq_agg_auto;
drop table actuary.tb_prime_acq_agg_gar;
insert into actuary.tb_prime_acq_agg_auto 

create table actuary.tb_prime_acq_agg_gar as
(
select i.ANNEE, i.Annee_acquisition,

   i.CODEBRAN,
   i.CODE_BRAN_RE,
   i.LIBEL_BRAN_RE,
   i.Tranche_Retention,
   i.BRANCHE1,
   i.BRANCHE2,
   i.Acquisition,
   i.NbrePolice,
   j.Prime_acquise,
   j.Pnette_acquise
from

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
 --  CODEGARA,
   CODEBRAN,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
   --CODEINTE,
   --NUMEPOLI,
  -- NUMEAVEN,
 --  CODERISQ,
 --  CODEBRAN,
     BRANCHE1,
    BRANCHE2,
   --PRIME_CEDEE,
   sum(Acquisition) Acquisition,--tb_pn_risque_ref
   sum(NbrePolice) NbrePolice --tb_pn_risque_ref
from 
(select
   CODEBRAN,
   --CODEGARA,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
   BRANCHE1,
   BRANCHE2,
   NUM_POLICE,
   NUMEPOLI,
   CODEINTE,
  -- NUMEAVEN,
   IDPOLICE,
   DATECOMP,
   DATEEFFE,
   DATEECHE,
   PRIMNET,
   CODERISQ,
   --PRIME_CEDEE,
   CHIFAFFA,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992)) Acquisition,
   actuary.Exposition(DATEEFFE,DATEECHE) Expo,
   case when actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992))>0 then 1 else 0 end kount,
   (case when actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992))>0 then 1 else 0 end) *1 NbrePolice,
   1992 Annee_acquisition

from  actuary.tb_prime_ref_gar)
group by to_char(datecomp,'YYYY'), Annee_acquisition,
 CODEBRAN,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
   --CODEINTE,
   --NUMEPOLI,
  -- NUMEAVEN,
 --  CODERISQ,
 --  CODEBRAN,
     BRANCHE1,
    BRANCHE2

) i,

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
    CODEBRAN,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
   --BRANCHE1,
   --BRANCHE2,
   sum(Prime_acquise) Prime_acquise,--pn_risque1
   sum(Pnette_acquise) Pnette_acquise--pn_risque1
from
(select
   datecomp,
  CODEGARA,
   CODEINTE,
   NUMEPOLI,
   NUMEAVEN,
   CODERISQ,
    CODEBRAN,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
  -- BRANCHE1,
  -- BRANCHE2,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else CHIFAFFA *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992))/Exposition(DATEEFFE,DATEECHE) end Prime_acquise,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else PRIMNETCO2 *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992))/Exposition(DATEEFFE,DATEECHE) end Pnette_acquise,
    1992 Annee_acquisition

from  actuary.PN_GAR_RISQUE_1 i
--, (select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto )h
---where nvl(i.CODEGARA,0)=h.VAR(+)
)
group by to_char(datecomp,'YYYY'), Annee_acquisition, 
   CODEBRAN,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention
   --,
  -- BRANCHE1,
  -- BRANCHE2
   ) j
where i.ANNEE=j.ANNEE(+) and i.Annee_acquisition=j.Annee_acquisition(+) and
     i.CODEBRAN=j.CODEBRAN(+)
and  i.CODE_BRAN_RE=j.CODE_BRAN_RE(+)
and  i.LIBEL_BRAN_RE=j.LIBEL_BRAN_RE(+)
and  i.Tranche_Retention=j.Tranche_Retention(+)
--and  i.RETENTION_BDM_TRI=j.RETENTION_BDM_TRI(+)
--and  i.RETENTION_TRM_TRC=j.RETENTION_TRM_TRC(+)
--and  i.RETENTION_RISQUES_DIVERS=j.RETENTION_RISQUES_DIVERS(+)


);

---drop table actuary.tb_prime_acq_agg_gar;

Begin
 FOR an IN (1993)..2030   loop

delete from  actuary.tb_prime_acq_agg_gar where annee_acquisition=an;
insert into  actuary.tb_prime_acq_agg_gar

 ( 
 
 select i.ANNEE, i.Annee_acquisition,

   i.CODEBRAN,
   i.CODE_BRAN_RE,
   i.LIBEL_BRAN_RE,
   i.Tranche_Retention,
   i.BRANCHE1,
   i.BRANCHE2,
   i.Acquisition,
   i.NbrePolice,
   j.Prime_acquise,
   j.Pnette_acquise
from

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
 --  CODEGARA,
   CODEBRAN,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
  Tranche_Retention,
   --CODEINTE,
   --NUMEPOLI,
  -- NUMEAVEN,
 --  CODERISQ,
 --  CODEBRAN,
     BRANCHE1,
    BRANCHE2,
   --PRIME_CEDEE,
   sum(Acquisition) Acquisition,--tb_pn_risque_ref
   sum(NbrePolice) NbrePolice --tb_pn_risque_ref
from 
(select
   CODEBRAN,
   --CODEGARA,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
   BRANCHE1,
   BRANCHE2,
   NUM_POLICE,
   NUMEPOLI,
   CODEINTE,
  -- NUMEAVEN,
   IDPOLICE,
   DATECOMP,
   DATEEFFE,
   DATEECHE,
   PRIMNET,
   CODERISQ,
   --PRIME_CEDEE,
   CHIFAFFA,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an)) Acquisition,
   actuary.Exposition(DATEEFFE,DATEECHE) Expo,
   case when actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))>0 then 1 else 0 end kount,
   (case when actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))>0 then 1 else 0 end) *1 NbrePolice,
   an Annee_acquisition

from  actuary.tb_prime_ref_gar)
group by to_char(datecomp,'YYYY'), Annee_acquisition,
 CODEBRAN,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
   --CODEINTE,
   --NUMEPOLI,
  -- NUMEAVEN,
 --  CODERISQ,
 --  CODEBRAN,
     BRANCHE1,
    BRANCHE2

) i,

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
    CODEBRAN,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
   --BRANCHE1,
   --BRANCHE2,
   sum(Prime_acquise) Prime_acquise,--pn_risque1
   sum(Pnette_acquise) Pnette_acquise--pn_risque1
from
(select
   datecomp,
  CODEGARA,
   CODEINTE,
   NUMEPOLI,
   NUMEAVEN,
   CODERISQ,
    CODEBRAN,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
  Tranche_Retention,
  -- BRANCHE1,
  -- BRANCHE2,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else CHIFAFFA *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end Prime_acquise,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else PRIMNETCO2 *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end Pnette_acquise,
    an Annee_acquisition

from  actuary.PN_GAR_RISQUE_1 i
--, (select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto )h
---where nvl(i.CODEGARA,0)=h.VAR(+)
)
group by to_char(datecomp,'YYYY'), Annee_acquisition, 
   CODEBRAN,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention
   --,
  -- BRANCHE1,
  -- BRANCHE2
   ) j
where i.ANNEE=j.ANNEE(+) and i.Annee_acquisition=j.Annee_acquisition(+) and
     i.CODEBRAN=j.CODEBRAN(+)
and  i.CODE_BRAN_RE=j.CODE_BRAN_RE(+)
and  i.LIBEL_BRAN_RE=j.LIBEL_BRAN_RE(+)
and  i.Tranche_Retention=j.Tranche_Retention(+)
---and  i.RETENTION_BDM_TRI=j.RETENTION_BDM_TRI(+)
---and  i.RETENTION_TRM_TRC=j.RETENTION_TRM_TRC(+)
---and  i.RETENTION_RISQUES_DIVERS=j.RETENTION_RISQUES_DIVERS(+)


   
   
 );

--group by  i.ANNEE, i.Annee_acquisition,j.CODEBRAN, 
 --  i.BRANCHE1,
 --  i.BRANCHE2
END LOOP;
end;

--select * from tb_prime_ref_gar
 delete from  actuary.tb_reglement;
   
 insert into actuary.tb_reglement
   
   SELECT c.CODEBRAN,b.LIBEBRAN,
       m.caterisq  ,c.libecate ,
       m.codegara , 
       d.LIBEGARA ,
       S.CODERISQ ,
       m.codeinte ,
       i.raisocin ,
       m.exersini ,
       m.numesini ,
       decode(m.natusini,'S','Maladie','M','Materiel','C','Corporel','D','Mixte (Mat. et Corp.)') natusini,         --m.natusini Nature, a faire  
       m.dateeval ,
       s.datesurv ,
       s.datedecl ,
       s.numepoli ,
       s.numeaven ,
       s.codeassu ,
       a.raissoci nom,
       -m.montprin montprin,
       -m.monthono monthono ,
       -(m.montprin + m.monthono) total_reglement ,ss.CODTYPSO ,TS.LIBTYPSO
  from v_mouvement_sinistre m,
  REFERENCE_GARANTIE d,
       sinistre s,
       categorie c,
       intermediaire i,
       assure a ,orass_v6.branche b 
        , sort_sinistre ss, type_sort ts
 where m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
   and m.CODEGARA=d.codegara
   and m.caterisq = c.codecate
   and m.codeinte = i.codeinte
   and s.codeassu = a.codeassu
   and m.natusini = s.natusini
   and m.typemouvement = 'REGLE'--renc
   and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
   and c.CODEBRAN=b.CODEBRAN
   and ss.datsorsi = (select max(ss2.datsorsi)
                        from sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                          and to_char(trunc(ss2.datsorsi),'YYYY') <=substr(month_id,1,4) )
   and ss.codtypso = ts.codtypso
   and to_char(m.dateeval,'YYYY') between  '2000' and substr(month_id,1,4)
  -- and  lower(a.raissoci) like '%;%'
 -- and m.caterisq between 400 and 412
 --  and (m.numepoli,m.intepoli) in (select police,inter from coassurance_detailles ) 
 --and s.codeassu in ()
 order by m.caterisq,
          m.exersini,
          m.codeinte,
          m.numesini,
          m.dateeval
          ;
delete from  actuary.tb_recours;
   
 insert into actuary.tb_recours
   
   SELECT c.CODEBRAN,b.LIBEBRAN,
       m.caterisq  ,c.libecate ,
       m.codegara , 
       d.LIBEGARA ,
       S.CODERISQ ,
       m.codeinte ,
       i.raisocin ,
       m.exersini ,
       m.numesini ,
       decode(m.natusini,'S','Maladie','M','Materiel','C','Corporel','D','Mixte (Mat. et Corp.)') natusini,         --m.natusini Nature, a faire  
       m.dateeval ,
       s.datesurv ,
       s.datedecl ,
       s.numepoli ,
       s.numeaven ,
       s.codeassu ,
       a.raissoci nom,
       -m.montprin montprin,
       -m.monthono monthono ,
       -(m.montprin + m.monthono) total_reglement ,ss.CODTYPSO ,TS.LIBTYPSO
  from v_mouvement_sinistre m,
  REFERENCE_GARANTIE d,
       sinistre s,
       categorie c,
       intermediaire i,
       assure a ,orass_v6.branche b 
        , sort_sinistre ss, type_sort ts
 where m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
   and m.CODEGARA=d.codegara
   and m.caterisq = c.codecate
   and m.codeinte = i.codeinte
   and s.codeassu = a.codeassu
   and m.natusini = s.natusini
   and m.typemouvement = 'RENC'--renc
   and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
   and c.CODEBRAN=b.CODEBRAN
   and ss.datsorsi = (select max(ss2.datsorsi)
                        from sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                          and to_char(trunc(ss2.datsorsi),'YYYY') <=substr(month_id,1,4) )
   and ss.codtypso = ts.codtypso
   and to_char(m.dateeval,'YYYY') between  '2000' and substr(month_id,1,4)
  -- and  lower(a.raissoci) like '%;%'
 -- and m.caterisq between 400 and 412
 --  and (m.numepoli,m.intepoli) in (select police,inter from coassurance_detailles ) 
 --and s.codeassu in ()
 order by m.caterisq,
          m.exersini,
          m.codeinte,
          m.numesini,
          m.dateeval
          ;



delete from  actuary.tb_sinistre_ref_GAR;  ---create table tb_sinistre_ref_GAR as (select  *from tb_sinistre_ref_auto)
---insert into actuary.tb_sinistre_ref_GAR  (

drop table actuary.tb_sinistre_ref_GAR ;

create table actuary.tb_sinistre_ref_GAR as (
select
CODEBRAN,
BRANCHE1,
BRANCHE2,
ANNEE,
--CODEINTE,
--NUMESINI,
CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
count(*) NBSIN,
 sum(TOT_REGLEMENT) TOT_REGLEMENT,

sum(TOT_REGLEMENT_NET)  TOT_REGLEMENT_NET,
sum(RECOURS)  RECOURS,
sum(SAP) SAP,
sum(SAP_NET) SAP_NET,
sum(CHARGE) CHARGE, 
sum(CHARGE_NET_RECOURS)  CHARGE_NET_RECOURS,

sum(decode(graves,1,1,0)) NBSIN_GRAVE,
 sum(TOT_REGLEMENT*graves) TOT_REGLEMENT_GRAVES,

sum(TOT_REGLEMENT_NET*graves)  TOT_REGLEMENT_NET_GRAVES,
sum(RECOURS*graves)  RECOURS_GRAVES,
sum(SAP*graves) SAP_GRAVES,
sum(SAP_NET*graves) SAP_NET_GRAVES,
sum(CHARGE*graves) CHARGE_GRAVES, 
sum(CHARGE_NET_RECOURS*graves)  CHARGE_NET_RECOURS_GRAVES

from 
(

select  
CODEBRAN,
BRANCHE1,
BRANCHE2,
ANNEE,
--CODEINTE,
--NUMESINI,
CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
case 
     when BRANCHE1='AUTO' and CHARGE_NET_RECOURS>=10000000 then 1
     when (BRANCHE1='DAB' or BRANCHE1='INCENDIE')  and CHARGE_NET_RECOURS>=50000000 then 1 
     when BRANCHE1='RC' and CHARGE_NET_RECOURS>=30000000 then 1
     when BRANCHE1='TRANSPORT' and CHARGE_NET_RECOURS>=20000000 then 1 else 0 END graves,
TOT_REGLEMENT,
TOT_REGLEMENT_NET,
 RECOURS,
SAP,
SAP_NET,
CHARGE, 
CHARGE_NET_RECOURS


from

(select 
CODEBRAN,
BRANCHE1,
BRANCHE2,
ANNEE,
CODEINTE,
NUMESINI,
CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention,
--VARRAPPORTACTUA 
---CODEGARA,
sum(SINPAY)+sum(SINPAY_ANT)  TOT_REGLEMENT,

sum(SINPAY)+sum(SINPAY_ANT) -sum(RECENC)-sum(RECENC_ANT)  TOT_REGLEMENT_NET,
sum(RECENC)+sum(RECENC_ANT)  RECOURS,
sum(SAP) SAP,
sum(SAP) -sum(AREC) SAP_NET,
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) CHARGE, 
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) - (sum(RECENC)+sum(RECENC_ANT)+sum(AREC))  CHARGE_NET_RECOURS


from

(
select
   i.*,
    BRANCHE1,
    BRANCHE2,
   CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   
   Tranche_Retention
   --,
   --SINPAY+SINPAY_ANT+SAP CHARGE_BR_REC,
   --COUTS_SINISTRES       CHARGE_NT_REC

from

(select*
from ACTUARY.CHARGE_SINISTRE where (SINPAY+SINPAY_ANT+SAP<>0 or SINPAY+SINPAY_ANT+SAP-RECENC-RECENC_ANT-AREC<>0)
and 
CODTYPSO in ('OU','RE','TR') and CODEINTE<>9999 and codebran not in (4,81,82,83,89))i,

(
select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)

j,

(select distinct CODEINTE c,NUMEPOLI n,NUMEAVEN nu,CODERISQ co,CODEGARA gar,
         CODE_BRAN_RE,
         LIBEL_BRAN_RE,
         Tranche_Retention
   from actuary.PN_GAR_RISQUE_1)k

where 
    i.CODEINTE=nvl(k.c(+),0)
and i.NUMEPOLI=nvl(k.n(+)    ,0)
and i.NUMEAVEN=nvl(k.nu(+)   ,0)
and i.CODERISQ=nvl(k.co(+)   ,0)
and i.CODEGARA=nvl(k.gar(+)  ,0)
and i.CODEBRAN=J.CBR(+)        
)




---,(select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto)h

 ---and nvl(i.codegara,0)=h.VAR(+)

group by 
CODEBRAN,
BRANCHE1,
BRANCHE2,
ANNEE,
CODEINTE,
NUMESINI,
CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention
)

)
group by 
CODEBRAN,
BRANCHE1,
BRANCHE2,
ANNEE,
--CODEINTE,
--NUMESINI,
CODE_BRAN_RE,
   LIBEL_BRAN_RE,
   Tranche_Retention
);





delete from actuary.tb_analyse_sp_gar;   ---create table tb_analyse_sp_gar as (select  *from tb_analyse_sp_auto)
insert into actuary.tb_analyse_sp_gar  ;
--drop table actuary.tb_analyse_sp_gar 
create table  actuary.tb_analyse_sp_gar as
(



select

i.*,
nvl(ACQUISITION            ,0)  ACQUISITION    ,
nvl(nbrepolice             ,0)  nbrepolice     ,
nvl(PRIME_ACQUISE          ,0)  PRIME_ACQUISE  ,
nvl(PNETTE_ACQUISE         ,0)  PNETTE_ACQUISE ,
nvl( NBSIN,                   0)   NBSIN ,
 nvl( TOT_REGLEMENT,           0)   TOT_REGLEMENT ,
 nvl( TOT_REGLEMENT_NET,       0)   TOT_REGLEMENT_NET  ,
 nvl( RECOURS,                 0)    RECOURS ,
 nvl( SAP,                     0)   SAP  ,
 nvl(SAP_NET,                  0)   SAP_NET  ,
 nvl(CHARGE,                   0)   CHARGE  ,
 nvl( CHARGE_NET_RECOURS,      0)   CHARGE_NET_RECOURS  ,
 nvl( NBSIN_GRAVE,             0)   NBSIN_GRAVE  ,
 nvl(TOT_REGLEMENT_GRAVES,     0)   TOT_REGLEMENT_GRAVES  ,
 nvl(TOT_REGLEMENT_NET_GRAVES, 0)   TOT_REGLEMENT_NET_GRAVES  ,
 nvl(RECOURS_GRAVES,           0)    RECOURS_GRAVES ,
 nvl( SAP_GRAVES,              0)   SAP_GRAVES  ,
 nvl(SAP_NET_GRAVES,           0)   SAP_NET_GRAVES  ,
 nvl(CHARGE_GRAVES,            0)    CHARGE_GRAVES ,
 nvl(CHARGE_NET_RECOURS_GRAVES,0)    CHARGE_NET_RECOURS_GRAVES 
from
(
select
distinct*
from
(
select distinct 
to_char(ANNEE_ACQUISITION)  ANNEE,
nvl(CODEBRAN                 ,0)  CODEBRAN                   ,
nvl(CODE_BRAN_RE             ,0)  CODE_BRAN_RE               ,
nvl(LIBEL_BRAN_RE            ,0)  LIBEL_BRAN_RE              ,
nvl(Tranche_Retention,0)  Tranche_Retention   ,
nvl(BRANCHE1                 ,0)  BRANCHE1                   ,
nvl(BRANCHE2                 , 0) BRANCHE2

from actuary.tb_prime_acq_agg_gar


union all

select
distinct
 ANNEE,
nvl(CODEBRAN                 ,0)  CODEBRAN                   ,
nvl(CODE_BRAN_RE             ,0)  CODE_BRAN_RE               ,
nvl(LIBEL_BRAN_RE            ,0)  LIBEL_BRAN_RE              ,
nvl(Tranche_Retention,0)  Tranche_Retention   ,
nvl(BRANCHE1                 ,0)  BRANCHE1                   ,
nvl(BRANCHE2                 , 0) BRANCHE2

from  actuary.tb_sinistre_ref_GAR

)

)i,

(
select 
ANNEE_ACQUISITION  ANNEE,
CODEBRAN                 ,
CODE_BRAN_RE             ,
LIBEL_BRAN_RE            ,
Tranche_Retention ,
BRANCHE1                 ,
BRANCHE2                 ,

sum(ACQUISITION)    ACQUISITION,
sum(NBREPOLICE)     nbrepolice,
sum(PRIME_ACQUISE)  PRIME_ACQUISE,
sum(PNETTE_ACQUISE) PNETTE_ACQUISE
from  actuary.tb_prime_acq_agg_gar
group by 
ANNEE_ACQUISITION,
CODEBRAN                 ,
CODE_BRAN_RE             ,
LIBEL_BRAN_RE            ,
Tranche_Retention ,
BRANCHE1                 ,
BRANCHE2               

)j,

(select*from  actuary.tb_sinistre_ref_GAR)k


where
i.ANNEE                          =nvl(j.ANNEE  (+)                         ,0)
and i.CODEBRAN                   =nvl(j.CODEBRAN  (+)                      ,0)
and i.CODE_BRAN_RE               =nvl(j.CODE_BRAN_RE  (+)                  ,0)
and i.LIBEL_BRAN_RE              =nvl(j.LIBEL_BRAN_RE  (+)                 ,0)
and i.Tranche_Retention  =nvl(j.Tranche_Retention  (+)      ,0)
and i.BRANCHE1                   =nvl(j.BRANCHE1  (+)                      ,0)
and i.BRANCHE2                   =nvl(j.BRANCHE2  (+)                      ,0)

and i.ANNEE                      =nvl(k.ANNEE  (+)                     ,0)
and i.CODEBRAN                   =nvl(k.CODEBRAN  (+)                  ,0)
and i.CODE_BRAN_RE               =nvl(k.CODE_BRAN_RE  (+)              ,0)
and i.LIBEL_BRAN_RE              =nvl(k.LIBEL_BRAN_RE  (+)             ,0)
and i.Tranche_Retention   =nvl(k.Tranche_Retention  (+)  ,0)
and i.BRANCHE1                   =nvl(k.BRANCHE1  (+)                  ,0)
and i.BRANCHE2                   =nvl(k.BRANCHE2  (+)                  ,0)

);
commit;
 ---
 ---end;
 
 
 
/