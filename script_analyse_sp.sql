--CREATE OR REPLACE PROCEDURE ACTUARY.proc_RA('202207' in varCHAR2,'202112' in varCHAR2,itt in int, fin in int) as
--
--begin

---update table pn risque sans les garanties, NUM_POLICE et CODERISQ , PRIMNET
delete from  ACTUARY.PN_RISQUE 
--where TO_CHAR (DATECOMP,'yyyymm')='202207'
;

insert into  actuary.PN_RISQUE  (
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
  -- CODEGARA,
   NVL(NUMEAVEN,0) NUMEAVEN,
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
 where  TO_CHAR (DATECOMP,'yyyy')>='1990'
  -- and codebran=4
 and  codeinte  not in (9999,9998,9995) and genrmouv<>'C')i,
 (
select
  CODEINTE, 
  NUMEPOLI,
  NUMEQUIT,
  NVL(NUMEAVEN,0) NUMEAVEN_G,
  CODERISQ,
  CODECATE CODECATE_G,
 -- CODEGARA,
  sum(primnett) primnette
  from prime_garantie
  where
  CODEINTE not in (9999,9998,9995) 
  group by
  CODEINTE, 
  NUMEPOLI,
  NUMEQUIT,
  NVL(NUMEAVEN,0) ,
  CODERISQ,
  CODECATE--,
--  CODEGARA
 --AND NUMEPOLI = 4000000271 and 
)j

where i.CODEINTE=j.CODEINTE(+) 
and   i.NUMEPOLI=j.NUMEPOLI(+) 
and   i.NUMEQUIT=j.NUMEQUIT(+) 
 
);
 

delete from actuary.pn_all;

insert into actuary.pn_all  
(select
   CODEINTE ,TYPEMOUV,
   NUMEPOLI,NUMEQUIT,
   sum(primnett) primnett1,
   sum(primnetco) primnetco1
  from  actuary.PN_RISQUE

group by CODEINTE,TYPEMOUV,
   NUMEPOLI,NUMEQUIT)
 ; 


delete from actuary.PN_RISQUE1;

insert into  actuary.PN_RISQUE1 
 (
 select
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
   CODERISQ,
   NUMEAVEN,
   NUMEAVEN_G,
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
      else nvl(primnett,0)/nvl(primnett1,0)*  CHIFAFFA_POLICE end CHIFAFFA   
 from
  
(select *from PN_RISQUE)i,
(select
   *from pn_all)j

where i.CODEINTE=j.CODEINTE(+) 
and   i.NUMEPOLI=j.NUMEPOLI(+) 
and   i.NUMEQUIT=j.NUMEQUIT(+) 
and   i.TYPEMOUV=j.TYPEMOUV(+) 
);

--create table actuary.branche as( select* from orass_v6.tb_branche);
---table prime avec id :num_police coderisque et date echeance


delete from actuary.tb_pn_risque_ref;
insert into actuary.tb_pn_risque_ref  (

select nvl(CODEBRAN,   0) CODEBRAN,
       nvl(BRANCHE1,   0) BRANCHE1,
       nvl(BRANCHE2,   0) BRANCHE2,
       nvl(NUM_POLICE, 0) NUM_POLICE, 
       nvl(NUMEPOLI,   0) NUMEPOLI,
       nvl(CODEINTE,   0) CODEINTE,
       nvl(CODERISQ, 0) CODERISQ, 
        IDPOLICE,MIN_YEAR,
       min(DATEEFFE)    DATEEFFE,
       min(datecomp)  datecomp,
                        DATEECHE,
       --sum(PRIMNETT2)    PRIMNETT_ALL,
       sum(PRIMNETCO2) PRIMNET,
       sum(CHIFAFFA)    CHIFAFFA,
       (trunc(DATEECHE)- trunc(min(DATEEFFE))+1) /365.25 EXPO
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
nvl(CODEINTE, 0)||nvl(NUMEPOLI, 0) NUM_POLICE,
CODERISQ,
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
(select *from actuary.pn_risque1 where PRIMNETCO2<>0  )i,

(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j,
(select EXTRACT(YEAR FROM min(DATEEFFE)) MIN_YEAR from actuary.pn_risque1 where '202111'>= to_char(datecomp,'YYYYMM') )k
where i.CODEBRAN=j.CBR(+) 

)
group by
nvl(CODEBRAN,   0)        ,
       nvl(BRANCHE1,   0) ,
       nvl(BRANCHE2,   0) ,
       nvl(NUM_POLICE, 0) , 
       nvl(NUMEPOLI,   0) ,
       nvl(CODEINTE,   0) ,
       nvl(CODERISQ, 0),
       IDPOLICE,
       DATEECHE,MIN_YEAR
       
 ); 
  


---Prime et expo par annee d'acquisition

FOR an IN (itt+1)..fin   loop

delete from actuary.tb_pn_risque_acq_agg where annee_acquisition=an;
insert into actuary.tb_pn_risque_acq_agg
(
   select i.ANNEE, i.Annee_acquisition,
   j.CODEBRAN,
   i.BRANCHE1,
   i.BRANCHE2,
   i.Acquisition,
   i.NbrePolice,
   j.Prime_acquise,
   j.Pnette_acquise
from

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEBRAN,
   BRANCHE1,
   BRANCHE2,
   --PRIME_CEDEE,
   sum(Acquisition) Acquisition,--tb_pn_risque_ref
   sum(NbrePolice) NbrePolice --tb_pn_risque_ref
from 
(select
   CODEBRAN,
   BRANCHE1,
   BRANCHE2,
   NUM_POLICE,
   NUMEPOLI,
   CODEINTE,
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
   AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an)) Acquisition,
   Exposition(DATEEFFE,DATEECHE) Expo,
   case when AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))>0 then 1 else 0 end kount,
   (case when AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))>0 then 1 else 0 end) *1 NbrePolice,
   an Annee_acquisition

from  actuary.tb_pn_risque_ref)
group by to_char(datecomp,'YYYY'), Annee_acquisition,
   CODEBRAN,
   BRANCHE1,
   BRANCHE2

) i,

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEBRAN,
   --BRANCHE1,
   --BRANCHE2,
   sum(Prime_acquise) Prime_acquise,--pn_risque1
   sum(Pnette_acquise) Pnette_acquise--pn_risque1
from
(select
   datecomp,
   CODERISQ,
   CODEBRAN,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   case when Exposition(DATEEFFE,DATEECHE)=0 then 0 else CHIFAFFA *AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end Prime_acquise,
   case when Exposition(DATEEFFE,DATEECHE)=0 then 0 else PRIMNETCO2 *AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end Pnette_acquise,
    an Annee_acquisition

from  actuary.pn_risque1)
group by to_char(datecomp,'YYYY'), Annee_acquisition, CODEBRAN) j
where i.ANNEE=j.ANNEE(+) and i.Annee_acquisition=j.Annee_acquisition(+) and
   i.CODEBRAN=j.CODEBRAN(+)
   );
END LOOP;



delete from  ACTUARY.charge_sinistre ;

insert into ACTUARY.charge_sinistre 

drop table   ACTUARY.charge_sinistre ;


create table  ACTUARY.charge_sinistre  as 


select to_char(x.datesurv,'yyyy') Annee,
       '202207' deal_date,
        x.codeinte ,
       i.raisocin ,
       x.numesini ,
       x.refeinte ,
       x.NUMEPOLI ,
       x.NUMEAVEN,
       decode(k.dateeffe,NULL,decode(l.dateeffe, NULL,m.dateeffe,l.dateeffe),k.dateeffe) dateeffe,
       decode(k.dateeche,NULL,decode(l.dateeche, NULL,m.dateeche,l.dateeche),k.dateeche) dateeche,   
       
       --decode(k.dateeffe,NULL,l.dateeffe, k.dateeffe) dateeffe,
       --decode(k.dateeche,NULL,l.dateeche,k.dateeche) dateeche,   
       
       x.codeassu ,
       x.nom_assu nom,
       c.codebran , B.LIBEBRAN,
       x.CODECATE  ,
       x.caterisq  ,
       substr(c.libecate,1, 40) libecate,
        x.codegara,x.CODERISQ,
       decode(x.natusini,'S','Maladie','M','Materiel','C','Corporel','D','Mat et Corporel')  natusini,
       g.libegara  ,
       x.datesurv  ,
       x.datedecl   ,
--      x.dateeval Date eval/reglt/Recours,
        sum(sinpay) sinpay,
        sum(sinpay_ant)sinpay_ant,   
        sum(recenc) recenc,
        sum(recenc_ant) recenc_ant,  
        sum(sinpay) - sum(recenc) Solde_paiements,
        sum(eval) eval,
        sum(sap) sap,  
        sum(arec) arec , 
       
(sum(sap) + sum(sinpay_ant)+sum(sinpay)) - (sum(arec)+ sum(recenc_ant)+ sum(recenc)) Couts_sinistres 
       ,x.CODTYPSO 
       ,x.LIBTYPSO
     
  from
(
         --************  Reglements sinistres  et recours encaissé de la periode    **********************---------------
select m.typemouvement,
       m.codeinte,
       m.numesini,
       m.nom_assu,
       m.codeassu,
       m.codecate,
       m.codegara,
       m.caterisq,S.CODERISQ,
       m.datesurv,
       m.datedecl,
       m.dateeval,
       m.refeinte,
       m.NUMEPOLI,
       nvl(m.NUMEAVEN,0)  NUMEAVEN,
       m.natusini, 
       sum(decode(m.typemouvement,'REGLE',-m.monteval,0)) sinpay,
       0 sinpay_ant, 
       sum(decode(m.typemouvement,'RENC', -m.monteval,0)) recenc,
       0 recenc_ant, 
	   0 Eval,
       0 sap, 
       0 arec ,ss.CODTYPSO ,ts.LIBTYPSO 
   from orass_v6.v_mouvement_sinistre m , orass_v6.sort_sinistre ss, orass_v6.type_sort ts ,orass_v6.sinistre s  
  -- to_char(trunc(ss2.datsorsi),'YYYYMM')
   where to_char(trunc(m.dateeval),'YYYYMM') between substr('202207',1,4)||'01'  and '202207'
   and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
   and m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
     and ss.datsorsi = (select max(ss2.datsorsi)
                        from orass_v6.sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                         and to_char(trunc(ss2.datsorsi),'YYYYMM')  <= '202207')
   and ss.codtypso = ts.codtypso
  -- and ts.natusort in ('OU','RO')   
--  and  m.codeinte=3044 and m.numesini=500232 and to_char(m.datesurv,'yyyy')=2017  
--and  m.codeinte=3023 and m.numesini=300278 and to_char(M.datesurv,'yyyy')=2016
 
 --and  m.codeinte=1021 and m.numesini=8214 and to_char(m.datesurv,'yyyy')=1990
 group by m.typemouvement,ss.CODTYPSO ,ts.LIBTYPSO,m.natusini,m.dateeval,m.codecate,m.refeinte,m.caterisq,S.CODERISQ,m.codeinte,m.numesini,m.nom_assu,m.codeassu,m.datesurv,m.datedecl,m.codegara ,m.NUMEPOLI, nvl(m.NUMEAVEN,0) 
union all
    --************  Reglements sinistres  et recours encaissé de la periode passée jusqu'a l'origine du sinistres    **********************---------------
select m.typemouvement,
       m.codeinte,
       m.numesini,
       m.nom_assu,
       m.codeassu,
       m.codecate,
        m.codegara,
       m.caterisq,S.CODERISQ,
       m.datesurv,
       m.datedecl,
       m.dateeval,
       m.refeinte,
       m.NUMEPOLI,
       nvl(m.NUMEAVEN,0)  NUMEAVEN,
       m.natusini, 
       0 sinpay,
       sum(decode(m.typemouvement,'REGLE',-m.monteval,0)) sinpay_ant,
       
       0 recenc,
       sum(decode(m.typemouvement,'RENC', -m.monteval,0)) recenc_ant,
       0 Eval,
       0 sap, 
       0 arec,ss.CODTYPSO ,ts.LIBTYPSO
       
  from orass_v6.v_mouvement_sinistre m , orass_v6.sort_sinistre ss, orass_v6.type_sort ts,orass_v6.sinistre s
 where to_char(trunc(m.dateeval),'YYYYMM') <='202112'
 and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
    and m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
   and ss.datsorsi = (select max(ss2.datsorsi)
                        from orass_v6.sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                         and to_char(trunc(ss2.datsorsi),'YYYYMM') <= '202207')
   and ss.codtypso = ts.codtypso
    --and ts.natusort in ('OU','RO')          
 
 group by m.typemouvement,ss.CODTYPSO ,ts.LIBTYPSO,m.natusini,m.dateeval,m.codecate,m.refeinte,m.caterisq,S.CODERISQ,m.codeinte,m.numesini,m.nom_assu,m.codeassu,m.datesurv,m.datedecl,m.codegara ,m.NUMEPOLI, nvl(m.NUMEAVEN,0) 

union all

------------------------------********************* Eval total et SAP et recours a encaisser *********************-------------
select m.typemouvement,
       m.codeinte,
       m.numesini,
       m.nom_assu,
       m.codeassu,
       m.codecate,
        m.codegara,
       m.caterisq,
       S.CODERISQ,
       m.datesurv,
       m.datedecl,
       m.dateeval,
       m.refeinte,
       m.NUMEPOLI,
       nvl(m.NUMEAVEN,0)  NUMEAVEN,
       m.natusini, 
       0 sinpay,
       0 sinpay_ant, 
	   
       0 recenc, 
       0 recenc_ant,
       sum(decode(m.typemouvement,'EVAL', m.monteval,0)) eval,
       sum(decode(m.typemouvement,'EVAL', m.monteval,0)) + sum(decode(m.typemouvement,'REGLE',m.monteval,0)) sap,
        
       sum(decode(m.typemouvement,'ESTR', m.monteval,0)) + sum(decode(m.typemouvement,'RENC', m.monteval,0)) arec 
	   ,ss.CODTYPSO ,ts.LIBTYPSO
       
  from orass_v6.v_mouvement_sinistre m, orass_v6.sort_sinistre ss, orass_v6.type_sort ts,orass_v6.sinistre s
 where to_char(trunc(m.dateeval),'YYYYMM') <= '202207'
   and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
    and m.codeinte = s.codeinte
   and m.exersini  = s.exersini
   and m.numesini  = s.numesini
   and ss.datsorsi = (select max(ss2.datsorsi)
                        from orass_v6.sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                         and to_char(trunc(ss2.datsorsi),'YYYYMM') <= '202207')
   and ss.codtypso = ts.codtypso
   and ts.natusort in ('OU','RO')
 group by  m.NUMEPOLI, nvl(m.NUMEAVEN,0), m.typemouvement,ss.CODTYPSO ,ts.LIBTYPSO,m.natusini,m.dateeval,m.codecate, m.codegara ,m.refeinte, m.caterisq,S.CODERISQ,m.codeinte,m.numesini,m.nom_assu,m.codeassu,m.datesurv,m.datedecl

) x, orass_v6.categorie c,orass_v6.intermediaire i  ,orass_v6.reference_garantie g  ,orass_v6.branche b , 

(select numepoli,codeinte, nvl(numeaven,0) numeaven, dateffav dateeffe, datechav dateeche from orass_v6.avenant) k,
(select numepoli,codeinte, nvl(numeaven,0) numeaven,  min(dateeffe) dateeffe,  min(dateeche) dateeche
 from orass_v6.v_ch_affaire where typemouv='EMISSIONS'  and genrmouv<>'C' --and nvl(numeaven,0)=0
 group by numepoli,codeinte, nvl(numeaven,0)) l,
 
(select numepoli,codeinte, nvl(avenmodi,0) numeaven,  min(dateeffe) dateeffe, min(dateeche) dateeche from orass_v6.hist_police where  nvl(avenmodi,0)=0
group by numepoli,codeinte, nvl(avenmodi,0)) m

where c.codecate = x.caterisq

and x.numepoli=k.numepoli(+)
and x.numeaven=k.numeaven (+)
and x.codeinte=k.codeinte (+)

and x.numepoli=l.numepoli(+)
and x.numeaven=l.numeaven (+)
and x.codeinte=l.codeinte (+)
--and x.codecate=l.codecate (+)
--and x.codeassu=l.codeassu (+)

and x.numepoli=m.numepoli(+)
and x.numeaven=m.numeaven (+)
and x.codeinte=m.codeinte (+)

and x.codeinte=i.codeinte
 and x.codegara=g.codegara  
 and c.codebran = b.codebran
 and   x.codeinte !=9999
--and  c.codebran = 81

--and  X.codeinte=3023 and x.numesini=300278 and to_char(x.datesurv,'yyyy')=2016
--and  x.codeinte=1021 and x.numesini=8214 and to_char(x.datesurv,'yyyy')=1990
--and  x.codeinte=3002 and x.numesini=13731 and to_char(x.datesurv,'yyyy')=1997
--and  x.codeinte=3044 and x.numesini=500232 and to_char(x.datesurv,'yyyy')=2017
 --and  x.codeinte=1001 and x.numesini=239 and to_char(x.datesurv,'yyyy')=1977

-- and x.codeassu in ( 10010028960,-30021122633,-30021104360 )
--and x.codeassu in (2850,410010016210)
 -- and c.codecate between   '400' and '412'
 --and  lower(x.nom_assu) like '%filivoire%'  
 --and x.codeinte|| x.NUMEPOLI in (3003||7010000001,3002||71263,3003||7010000110,)
group by 
'202207',
         x.codeinte,
        x.caterisq,
         c.codebran,
         i.raisocin,
         x.numesini,
         x.nom_assu,
         x.codeassu,
         c.libecate,
         x.caterisq,
         x.datesurv,
         x.refeinte,
         x.codegara,   
         x.datedecl,    
         g.libegara,  x.CODERISQ, 
         x.natusini,
          
         x.CODECATE,
         x.NUMEPOLI ,x.CODTYPSO ,x.LIBTYPSO , B.LIBEBRAN,x.NUMEAVEN,
         decode(k.dateeffe,NULL,decode(l.dateeffe, NULL,m.dateeffe,l.dateeffe),k.dateeffe) ,
         decode(k.dateeche,NULL,decode(l.dateeche, NULL,m.dateeche,l.dateeche),k.dateeche) 
        
         --decode(k.dateeffe,NULL,l.dateeffe, k.dateeffe) ,
        --decode(k.dateeche,NULL,l.dateeche,k.dateeche)  
--having sum(sap) > 0
order by 1,2,3

;



delete from actuary.TB_SINISTRE where substr(deal_date,1,4)=substr('202207',1,4);

insert into actuary.TB_SINISTRE  (select*from actuary.charge_sinistre);

---Correction anomalie

delete from  actuary.charge_sinistre_detect0;

insert into  actuary.charge_sinistre_detect0
--create table  actuary.charge_sinistre_detect0 as 
(
select*from
(
select
ANNEE            ,         
DEAL_DATE        ,
CODEINTE         ,
RAISOCIN         ,
NUMESINI         ,
REFEINTE         ,
NUMEPOLI         ,
NUMEAVEN         ,
DATEEFFE         ,
DATEECHE         ,
CODEASSU         ,
NOM              ,
CODEBRAN         ,
LIBEBRAN         ,
CODECATE         ,
CATERISQ         ,
LIBECATE         ,
CODEGARA         ,
CODERISQ         ,
NATUSINI         ,
LIBEGARA         ,
DATESURV         ,
DATEDECL         ,
SINPAY           ,
SINPAY_ANT       ,
RECENC           ,
RECENC_ANT       ,
SOLDE_PAIEMENTS  ,
EVAL             ,
SAP              ,
AREC             ,
COUTS_SINISTRES  ,
CODTYPSO         ,
LIBTYPSO         ,

ANNEE||'_'||CODEINTE||'_'||NUMESINI  ID_SINI,
ANNEE||'_'||CODEINTE||'_'||NUMESINI||'_'||CODEGARA   ID_SINI_GAR,
ANNEE||'_'||CODEINTE||'_'||NUMESINI||'_'||CODEGARA||'_'||CODECATE   ID_SINI_GAR_CODECATE,

CODECATE  CODECATE2,
ANNEE ANNEESURV,
nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0)  PAYEALLNET,
nvl(SAP,0)-nvl(AREC,0)  SAPNET,
nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0) +nvl(SAP,0)-nvl(AREC,0) CHARGENET,

case 
  when  nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0) +nvl(SAP,0)-nvl(AREC,0) < 0 then 1
  when  nvl(SAP,0)-nvl(AREC,0) < 0 then 1
  when  nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0) < 0 then 1
  when  (nvl(SAP,0)-nvl(AREC,0)< 0) and (nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0)) < 0 then 1
  when  nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0)=0 AND nvl(SAP,0)-nvl(AREC,0)=0 then 1
   when  nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0) +nvl(SAP,0)-nvl(AREC,0)=0
         AND (nvl(SAP,0)-nvl(AREC,0))<>0 then 1
  when  nvl(RECENC,0)+nvl(RECENC_ANT,0) < 0 then 1 else  0 end  ANOMALIE,

case 
  when nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0) +nvl(SAP,0)-nvl(AREC,0) < 0 then 'CHARGE NET NEGATIF'
  when nvl(SAP,0)-nvl(AREC,0) < 0 then 'SAP NEGATIF'
  when nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0) < 0 then 'PAYE NET NEGATIF'
  when (nvl(SAP,0)-nvl(AREC,0)< 0) AND (nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0)< 0)  then 'PAYE NET et SAP NET NEGATIF'
  when nvl(RECENC,0)+nvl(RECENC_ANT,0) < 0 then 'RECOURS NET NEGATIF' else 'AUCUN'  end  MOTIF_ANOMAL,


case 
when nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0)=0 AND nvl(SAP,0)-nvl(AREC,0)=0 then 1 else 0 end SANS_FLUX,

case
when
nvl(SINPAY,0)+nvl(SINPAY_ANT,0)-nvl(RECENC,0)-nvl(RECENC_ANT,0) +nvl(SAP,0)-nvl(AREC,0)=0
         AND (nvl(SAP,0)-nvl(AREC,0))<>0 then 1 else 0 end CHARGE_ANNULE,
         
 decode(CODTYPSO,'TR',1,0) CLOS,
 
 case 
     when CODTYPSO in ('OU','RE','TR') then 1 else 0 end INCLUS_ANALYSE,
     
  1 NBSIN
  
  from actuary.charge_sinistre )i, 
  (select

varc,n, 1 DOUBLON
from(
select
ANNEE||'_'||CODEINTE||'_'||NUMESINI||'_'||CODEGARA varc, count(*) n
from actuary.charge_sinistre  group by ANNEE||'_'||CODEINTE||'_'||NUMESINI||'_'||CODEGARA)
where n>=2) j
  
 where  i.ID_SINI_GAR=j.varc(+)
);  
 
delete from  actuary.charge_sinistre_detect;

insert into  actuary.charge_sinistre_detect
--create table actuary.charge_sinistre_detect as
(select 

ANNEE                         ,            
DEAL_DATE                     ,
CODEINTE                      ,
RAISOCIN                      ,
NUMESINI                      ,
REFEINTE                      ,
NUMEPOLI                      ,
NUMEAVEN                      ,
DATEEFFE                      ,
DATEECHE                      ,
CODEASSU                      ,
NOM                           ,
CODEBRAN                      ,
LIBEBRAN                      ,
CODECATE                      ,
CATERISQ                      ,
LIBECATE                      ,
CODEGARA                      ,
CODERISQ                      ,
NATUSINI                      ,
LIBEGARA                      ,
DATESURV                      ,
DATEDECL                      ,
SINPAY                        ,
SINPAY_ANT                    ,
RECENC                        ,
RECENC_ANT                    ,
SOLDE_PAIEMENTS               ,
EVAL                          ,
SAP                           ,
AREC                          ,
COUTS_SINISTRES               ,
CODTYPSO                      ,
LIBTYPSO                      ,
ID_SINI                       ,
ID_SINI_GAR                   ,
ID_SINI_GAR_CODECATE          ,
CODECATE2                     ,
ANNEESURV                     ,
PAYEALLNET                    ,
SAPNET                        ,
CHARGENET                     ,

decode(nvl(doublon,0),1,1,ANOMALIE) ANOMALIE,

 Case 
    when  nvl(doublon,0)=1 then 'Doublon Numero de Sin et Gar'||','||MOTIF_ANOMAL 
    when  SANS_FLUX=1 then 'SANS FLUX'||','||MOTIF_ANOMAL
    when  CHARGE_ANNULE=1 then 'CHARGE ANNULEE'||','||MOTIF_ANOMAL  else MOTIF_ANOMAL  end MOTIF_ANORMAL,
SANS_FLUX                     ,
CHARGE_ANNULE                 ,
CLOS                          ,
INCLUS_ANALYSE                ,
NBSIN,
decode(INCLUS_ANALYSE,1,0,1) CLASSE_SANS_SUITE

from actuary.charge_sinistre_detect0
);


---delete from  actuary.tb_sinistre_detect;

--insert into  actuary.tb_sinistre_detect

--(
--select 

--from






--)


delete from actuary.tb_sinistre_grave;
insert into actuary.tb_sinistre_grave 

(select  
CODEBRAN,BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
  NUMESINI,

TOT_REGLEMENT,
TOT_REGLEMENT_NET,
 RECOURS,
SAP,
SAP_NET,
CHARGE, 
CHARGE_NET_RECOURS,
case 
     when BRANCHE1='AUTO' and CHARGE_NET_RECOURS>=10000000 then 1
     when (BRANCHE1='DAB' or BRANCHE1='INCENDIE')  and CHARGE_NET_RECOURS>=50000000 then 1 
     when BRANCHE1='RC' and CHARGE_NET_RECOURS>=30000000 then 1
     when BRANCHE1='TRANSPORT' and CHARGE_NET_RECOURS>=20000000 then 1 else 0 END graves,
CODTYPSO
from

(select 
i.CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
NUMESINI,
CODTYPSO,

sum(SINPAY)+sum(SINPAY_ANT)  TOT_REGLEMENT,

sum(SINPAY)+sum(SINPAY_ANT) -sum(RECENC)-sum(RECENC_ANT)  TOT_REGLEMENT_NET,
sum(RECENC)+sum(RECENC_ANT)  RECOURS,
sum(SAP) SAP,
sum(SAP) -sum(AREC) SAP_NET,
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) CHARGE, 
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) - (sum(RECENC)+sum(RECENC_ANT)+sum(AREC))  CHARGE_NET_RECOURS


from
(select*
from actuary.charge_sinistre)i,

(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j

where i.CODEBRAN=j.CBR(+)

group by 
CODTYPSO,
i.CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
NUMESINI
)
 where ((CODTYPSO='TR' and CHARGE_NET_RECOURS<>0)
 or CODTYPSO in ('OU','RE')) and CODEINTE<>9999
)
;

delete from actuary.tb_sinistre_ref;
insert into ACTUARY.tb_sinistre_ref  (
select 
CODEBRAN,
BRANCHE1,
BRANCHE2,
ANNEE,
count( distinct ANNEE||CODEINTE||NUMESINI ) NBSIN,
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
 k.ANNEE,
 k.CODEINTE,
 k.NUMESINI,
 TOT_REGLEMENT,
 TOT_REGLEMENT_NET,
 RECOURS,
 SAP,
 SAP_NET,
 CHARGE, 
 CHARGE_NET_RECOURS,
 GRAVES,
 CODTYPSO


from

(select 

i.CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
NUMESINI,
sum(SINPAY)+sum(SINPAY_ANT)  TOT_REGLEMENT,

sum(SINPAY)+sum(SINPAY_ANT) -sum(RECENC)-sum(RECENC_ANT)  TOT_REGLEMENT_NET,
sum(RECENC)+sum(RECENC_ANT)  RECOURS,
sum(SAP) SAP,
sum(SAP) -sum(AREC) SAP_NET,
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) CHARGE, 
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) - (sum(RECENC)+sum(RECENC_ANT)+sum(AREC))  CHARGE_NET_RECOURS,
CODTYPSO
from

(select*
from actuary.charge_sinistre)i,

(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j

where i.CODEBRAN=j.CBR(+)

group by 
CODTYPSO,
i.CODEBRAN,
BRANCHE1,
BRANCHE2,
ANNEE,
CODEINTE,
NUMESINI
)k,

(select
distinct ANNEE,CODEINTE,NUMESINI, graves

from actuary.tb_sinistre_grave where graves=1)l

where k.ANNEE=l.ANNEE(+) 
and  k.CODEINTE=l.CODEINTE(+)
and  k.NUMESINI=l.NUMESINI(+)
and  CHARGE_NET_RECOURS<>0
 and CODTYPSO in ('TR','OU','RE')  and k.CODEINTE<>9999



)
group by 
CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE
);
--select annee, codeinte, numesini , count(*) from tb_sinistre_ref group by annee, codeinte, numesini 
--select * from tb_sinistre_ref

----table analyse sp

delete from actuary.tb_analyse_sp;
insert into ACTUARY.tb_analyse_sp  
(
select
i.CODEBRAN, i.BRANCHE1,i.BRANCHE2,
ANNEE_ACQUISITION ANNEE,
 nvl(acquisition ,             0)   acquisition   ,
 nvl(prime_acquise,            0)   prime_acquise  ,
 nvl(I.pnette_acquise,         0)   pnette_acquise  ,
 nvl(nbrepolice,               0)   nbrerisque  ,
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
(select 
ANNEE_ACQUISITION,CODEBRAN,BRANCHE1,BRANCHE2,
sum(ACQUISITION) ACQUISITION,
sum(NBREPOLICE) nbrepolice,
sum(PRIME_ACQUISE) PRIME_ACQUISE,
sum(PNETTE_ACQUISE) PNETTE_ACQUISE
from actuary.tb_pn_risque_acq_agg
group by ANNEE_ACQUISITION,CODEBRAN,BRANCHE1,BRANCHE2)i,
(select*from actuary.tb_sinistre_ref)j

where i.CODEBRAN=j.CODEBRAN(+) 
      and i.ANNEE_ACQUISITION=j.ANNEE(+) 
     
);



---AUTOMOBILE



--Table garantie et risque
delete from  ACTUARY.PN_GARANTIE_RISQUE 
--where TO_CHAR (DATECOMP,'yyyymm')='202207'
;

insert into actuary.PN_GARANTIE_RISQUE  (
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
   and codebran=4
 and  codeinte  not in (9999,9998,9995) and genrmouv<>'C')i,
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
(
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


delete from  PN_AUTO_RISQUE;
insert into PN_AUTO_RISQUE  
(select 

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



delete from tb_prime_ref_auto;
insert into tb_prime_ref_auto  (


select nvl(CODEBRAN,   0) CODEBRAN,
       nvl(BRANCHE1,   0) BRANCHE1,
       nvl(BRANCHE2,   0) BRANCHE2,
       nvl(NUM_POLICE, 0) NUM_POLICE, 
       nvl(NUMEPOLI,   0) NUMEPOLI,
       nvl(CODEINTE,   0) CODEINTE,
       nvl(CODERISQ, 0) CODERISQ, 
       nvl(VARRAPPORTACTUA,0) CODEGARA,
        IDPOLICE,MIN_YEAR,
       min(DATEEFFE)    DATEEFFE,
       min(datecomp)  datecomp,
                        DATEECHE,
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
nvl(CODEINTE, 0)||nvl(NUMEPOLI, 0) NUM_POLICE,VARRAPPORTACTUA,
CODERISQ,
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
(select *from actuary.PN_AUTO_RISQUE where  PRIMNETCO2<>0  )i,

(select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto)h,
(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j,
(select EXTRACT(YEAR FROM min(DATEEFFE)) MIN_YEAR from actuary.PN_AUTO_RISQUE )k
where i.CODEBRAN=j.CBR(+) and nvl(i.CODEGARA,0)=h.VAR(+)

)
group by
nvl(CODEBRAN,   0)        ,
       nvl(BRANCHE1,   0) ,
       nvl(BRANCHE2,   0) ,
       nvl(NUM_POLICE, 0) , 
       nvl(NUMEPOLI,   0) ,
       nvl(CODEINTE,   0) ,
       nvl(CODERISQ, 0), nvl(VARRAPPORTACTUA,0),
       IDPOLICE,
       DATEECHE,MIN_YEAR
       
 ); 


---Prime et expo par annee d'acquisition sur l'auto


delete from  tb_prime_acq_agg_auto;
insert into tb_prime_acq_agg_auto 
(
select i.ANNEE, i.Annee_acquisition,
   j.CODEGARA,
   i.BRANCHE1,
   i.BRANCHE2,
   i.Acquisition,
   i.NbrePolice,
   j.Prime_acquise,
   j.Pnette_acquise
from

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEGARA,
   BRANCHE1,
   BRANCHE2,
   --PRIME_CEDEE,
   sum(Acquisition) Acquisition,--tb_pn_risque_ref
   sum(NbrePolice) NbrePolice --tb_pn_risque_ref
from 
(select
   CODEGARA,
   BRANCHE1,
   BRANCHE2,
   NUM_POLICE,
   NUMEPOLI,
   CODEINTE,
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

from  actuary.TB_PRIME_REF_AUTO)
group by to_char(datecomp,'YYYY'), Annee_acquisition,
   CODEGARA,
   BRANCHE1,
   BRANCHE2

) i,

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEGARA,
   --BRANCHE1,
   --BRANCHE2,
   sum(Prime_acquise) Prime_acquise,--pn_risque1
   sum(Pnette_acquise) Pnette_acquise--pn_risque1
from
(select
   datecomp,
   CODERISQ,
   VARRAPPORTACTUA CODEGARA,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else CHIFAFFA *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992))/Exposition(DATEEFFE,DATEECHE) end Prime_acquise,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else PRIMNETCO2 *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992))/Exposition(DATEEFFE,DATEECHE) end Pnette_acquise,
    1992 Annee_acquisition

from  actuary.pn_auto_risque i, (select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto )h
where nvl(i.CODEGARA,0)=h.VAR(+))
group by to_char(datecomp,'YYYY'), Annee_acquisition, CODEGARA) j
where i.ANNEE=j.ANNEE(+) and i.Annee_acquisition=j.Annee_acquisition(+) and
   i.CODEGARA=j.CODEGARA(+)

);




 FOR an IN (itt+1)..fin   loop

delete from  actuary.tb_prime_acq_agg_auto where annee_acquisition=an;
insert into  actuary.tb_prime_acq_agg_auto

   select i.ANNEE, i.Annee_acquisition,
   j.CODEGARA,
   i.BRANCHE1,
   i.BRANCHE2,
   i.Acquisition,
   i.NbrePolice,
   j.Prime_acquise,
   j.Pnette_acquise
from

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEGARA,
   BRANCHE1,
   BRANCHE2,
   --PRIME_CEDEE,
   sum(Acquisition) Acquisition,--tb_pn_risque_ref
   sum(NbrePolice) NbrePolice --tb_pn_risque_ref
from 
(select
   CODEGARA,
   BRANCHE1,
   BRANCHE2,
   NUM_POLICE,
   NUMEPOLI,
   CODEINTE,
   IDPOLICE,
   DATECOMP,
   DATEEFFE,
   DATEECHE,
   PRIMNET,
   --CODERISQ,
   --PRIME_CEDEE,
   --CHIFAFFA,
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

from  actuary.TB_PRIME_REF_AUTO)
group by to_char(datecomp,'YYYY'), Annee_acquisition,
   CODEGARA,
   BRANCHE1,
   BRANCHE2

) i,

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEGARA,
   --BRANCHE1,
   --BRANCHE2,
   sum(Prime_acquise) Prime_acquise,--pn_risque1
   sum(Pnette_acquise) Pnette_acquise--pn_risque1
from
(select
   datecomp,
   CODERISQ,
   VARRAPPORTACTUA CODEGARA,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else CHIFAFFA *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end Prime_acquise,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else PRIMNETCO2 *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end Pnette_acquise,
    an Annee_acquisition

from  actuary.pn_auto_risque i, (select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto )h
where nvl(i.CODEGARA,0)=h.VAR(+))
group by to_char(datecomp,'YYYY'), Annee_acquisition, CODEGARA) j
where i.ANNEE=j.ANNEE(+) and i.Annee_acquisition=j.Annee_acquisition(+) and
   i.CODEGARA=j.CODEGARA(+);

--group by  i.ANNEE, i.Annee_acquisition,j.CODEBRAN, 
 --  i.BRANCHE1,
 --  i.BRANCHE2
END LOOP;

--select * from tb_prime_ref_auto
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
                          and to_char(trunc(ss2.datsorsi),'YYYY') <=substr('202207',1,4) )
   and ss.codtypso = ts.codtypso
   and to_char(m.dateeval,'YYYY') between  '2000' and substr('202207',1,4)
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
                          and to_char(trunc(ss2.datsorsi),'YYYY') <=substr('202207',1,4) )
   and ss.codtypso = ts.codtypso
   and to_char(m.dateeval,'YYYY') between  '2000' and substr('202207',1,4)
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




delete from  actuary.tb_sinistre_ref_auto;
 insert into actuary.tb_sinistre_ref_auto  (
select
CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEGARA,
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
CODEBRAN,BRANCHE1,
BRANCHE2,
  ANNEE,
CODEGARA,
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
i.CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
NUMESINI,
VARRAPPORTACTUA CODEGARA,
sum(SINPAY)+sum(SINPAY_ANT)  TOT_REGLEMENT,

sum(SINPAY)+sum(SINPAY_ANT) -sum(RECENC)-sum(RECENC_ANT)  TOT_REGLEMENT_NET,
sum(RECENC)+sum(RECENC_ANT)  RECOURS,
sum(SAP) SAP,
sum(SAP) -sum(AREC) SAP_NET,
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) CHARGE, 
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) - (sum(RECENC)+sum(RECENC_ANT)+sum(AREC))  CHARGE_NET_RECOURS


from
(select*
from ACTUARY.CHARGE_SINISTRE where (SINPAY+SINPAY_ANT+SAP<>0 or SINPAY+SINPAY_ANT+SAP-RECENC-RECENC_ANT-AREC<>0)
and 
CODTYPSO in ('OU','RE','TR') and CODEINTE<>9999 and CODEBRAN=4)i,

(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j,
(select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto)h

where i.CODEBRAN=j.CBR(+) and nvl(i.codegara,0)=h.VAR(+)

group by 
i.CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
NUMESINI,
VARRAPPORTACTUA
))
group by 
CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEGARA
);

delete from actuary.tb_analyse_sp_auto;
insert into actuary.tb_analyse_sp_auto  
(
select
i.BRANCHE1,i.BRANCHE2,i.CODEGARA,
ANNEE_ACQUISITION ANNEE,
 nvl(acquisition ,             0)  acquisition   ,
 nvl(prime_acquise,            0)   prime_acquise  ,
 --nvl(cions,                    0)   cions  ,
 nvl(nbrepolice,               0)  nbrerisque   ,
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
( select 
ANNEE_ACQUISITION,CODEGARA,BRANCHE1,BRANCHE2,
sum(ACQUISITION) ACQUISITION,
sum(NBREPOLICE) nbrepolice,
sum(PRIME_ACQUISE) PRIME_ACQUISE,
sum(PNETTE_ACQUISE) PNETTE_ACQUISE
from  actuary.tb_prime_acq_agg_auto
group by ANNEE_ACQUISITION,CODEGARA,BRANCHE1,BRANCHE2 )i,
(select*from  actuary.tb_sinistre_ref_auto)j

where i.CODEGARA=j.CODEGARA(+)
      and i.ANNEE_ACQUISITION=j.ANNEE(+) 
     
);




commit;

 --end;
/
