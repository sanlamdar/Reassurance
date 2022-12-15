--CREATE OR REPLACE procedure REASS_DB.proc_prime_sin_cedee(itt in int, fin in int) as
--
--begin

delete from reass_db.PRIME_CEDEE_ANT2021;
insert into reass_db.PRIME_CEDEE_ANT2021
--create table reass_db.PRIME_CEDEE_ANT2021 as
(
select  codeinte , numepoli , numeaven ,numetrai ,numetran, DATEEFFE , datecomp , DATEECHE , codecate ,
 libecate , codebran , libebran , CODEGARA , liberisq , coderisq , codtyptr , REASSUR , 
        CODE_REASR , CODE_BRAN_RE , LIBEL_BRAN_RE , desitrai , 
        tauxreas ,tauxprim,
        decode(nvl(primbrut,0),0,0,decode(nvl(montcess,0),0,0,nvl(montcomm,0)*100/montcess)) TX_COM ,
        MONTCESS  ,MONTCOMM   , MONTCESS- MONTCOMM  CESS_NT,montprim,primbrut,valeplei, nompletr

from (

select r.codereas CODE_REASR,rr.raisocre REASSUR, r.tauxreas ,v.numepoli ,v.numetrai ,v.numetran ,v.codeinte ,nvl(v.numeaven,0) numeaven ,v.dateffqu DATEEFFE,v.datetrai datecomp, v.datechqu DATEECHE,
       v.codegara, v.codecate ,c.libecate libecate,br.codebran codebran, br.libebran  libebran,v.codbrare CODE_BRAN_RE, v.libbrare LIBEL_BRAN_RE, LIBEL_RISQ liberisq,
        v.coderisq coderisq,v.codtyptr codtyptr,tr.desitrai desitrai,v.tauxprim,v.valeplei, v.nompletr,
         sum(v.ajuscom) ajuscom,
        sum(v.primbrut) primbrut, sum(v.montprim) montprim, max(v.mont_smp) mont_smp, max(v.capibrut) capibrut, max(v.montcapi) montcapi,
        sum(v.montacce) montacce, max(v.accebrut) accebrut, sum(v.moncomaj) moncomaj, sum(nvl(v.montprim,0)*nvl(r.tauxreas,0))/100 montcess,
         sum(nvl(v.montprim,0)*nvl(r.tauxreas,0)*nvl(r.tauxcomm,0))/10000 montcomm                     
from(
select  statproj, pr.numequit, pr.numetrai, pr.exertrai, pr.dateeffe, pr.codtyptr, pr.ordrappl, pr.numesect, pr.numetran, pr.coderisq, pr.codecate, pr.codeinte, pr.codegara, pr.primbrut, pr.montprim, pr.mont_smp, pr.capibrut, pr.montcapi, pr.montcomm, pr.flaggene, pr.flagannu, pr.flagvali, pr.annetrai, pr.peritrai, pr.tauxprim, nvl(pr.montacce,0) montacce, nvl(pr.accebrut,0) accebrut, nvl(pr.moncomaj,0) moncomaj, pr.taux_smp, pr.catereas, pr.catecomp, pr.codclari, pr.anneproj, pr.numeproj, pr.ajuscom, pr.numepoli, nvl(pr.numeaven,0) numeaven, pr.dateffqu, pr.datechqu, pr.valeplei, pr.refeinte, pr.nomplere, pr.nompletr, pr.datetrai, pr.nume_pmd, pr.datecomp, pr.modetrai,
        (select liberisq from  orass_v6.risque r where 
        r.codeinte=pr.codeinte and r.numepoli=pr.numepoli and r.coderisq=pr.coderisq) LIBEL_RISQ,
        (select max(libbrare) from reass.ref_categorie_reassurance r1, orass_v6.branche_reass b1
                          where r1.typcatre = 'R'
                          and   pr.catereas = r1.catereas(+)
                          and   nvl(r1.codbrare,-1)=b1.codbrare(+)) libbrare,
        (select max(r1.libcatre) from reass.ref_categorie_reassurance r1
                          where r1.typcatre = 'R'
                          and   pr.catereas = r1.catereas) libcatre_c,
        (select max(r1.libcatre) from reass.ref_categorie_reassurance r1
                          where r1.typcatre='R'
                          and   pr.catereas=r1.catereas) libcatre_r,
        (select max(codbrare) from reass.ref_categorie_reassurance r1
                          where r1.typcatre = 'R'
                          and   pr.catereas = r1.catereas(+)
                          ) codbrare
from reass.vv_prime_reassurance pr  where  datetrai between '01/01/2010' and '31/12/2021' and codtyptr in ('XP','FQ','CL') --'03/08/1975'
) v, reass.projet_cession_prime p,   orass_v6.categorie c, orass_v6.branche br,  orass_v6.police pp, reass.traite tr,  orass_v6.assure a,reass.reassureur_tranche r, reass.reassureur rr
where v.anneproj  = p.anneproj
  and v.numeproj  = p.numeproj
  and p.statproj  = v.statproj
  and v.codecate  = c.codecate
  and br.codebran = c.codebran
  and v.codeinte  = pp.codeinte(+)
  and v.numepoli  = pp.numepoli(+)
  and v.numetrai  = tr.numetrai
  and v.exertrai  = tr.exertrai
  and v.dateeffe  = tr.dateeffe
  and nvl(pp.codeassu,999999999) = a.codeassu(+)
  and    r.numetrai = v.numetrai
  and    r.exertrai = v.exertrai
  and    r.dateeffe = v.dateeffe
  and    nvl(r.numesect,0) = nvl(v.numesect,0)
  and    nvl(r.numetran,0) = nvl(v.numetran,0)
  and rr.codereas=r.codereas
  
group by  r.codereas ,rr.raisocre , r.tauxreas,v.numepoli,v.numetrai ,v.numetran ,v.codeinte ,nvl(v.numeaven,0) ,v.dateffqu,v.datetrai , v.datechqu ,
       v.codegara, v.codecate ,c.libecate ,br.codebran ,br.libebran  ,v.codbrare , v.libbrare , 
        v.coderisq ,v.codtyptr ,tr.desitrai,LIBEL_RISQ,v.tauxprim,v.valeplei, v.nompletr
)
);
---------------------------------------------------------ref-----------------------


delete from reass_db.PRIME_CEDEE_ANT2021_REF;
--drop table reass_db.PRIME_CEDEES_ANT2021_REF;
insert into reass_db.PRIME_CEDEE_ANT2021_REF

--create table reass_db.PRIME_CEDEE_ANT2021_REF as
(
select  codeinte , 
        numepoli , 
      --  numeaven ,
        min(DATEEFFE) dateeffe , 
        min(datecomp) datecomp , 
        DATEECHE , 
        CODEBRAN,codtyptr,desitrai,CODE_BRAN_RE, 
        libebran , 
        liberisq , 
        coderisq , 
        (trunc(DATEECHE)- trunc(min(DATEEFFE))+1) /365.25 EXPO 
from reass_db.PRIME_CEDEE_ANT2021 where codeinte  not in (9999,9998,9995) 
group by codeinte , 
         numepoli , 
      --   numeaven ,
         DATEECHE ,
        CODEBRAN,codtyptr,desitrai,CODE_BRAN_RE , 
        libebran ,
        liberisq , 
        coderisq 
        
        ); 
        
        
--------------Calcul des acquisitions-----------------------------
---TRUNCATE TABLE reass_db.PRIME_CEDEES_ANT2021_acq_agg;
delete from reass_db.PRIME_CEDEES_ANT2021_acq_agg;

begin
FOR an IN (2010)..2025   loop

delete from reass_db.PRIME_CEDEES_ANT2021_acq_agg where annee_acquisition=an;
insert into reass_db.PRIME_CEDEES_ANT2021_acq_agg
(
   select i.ANNEE, i.Annee_acquisition,
   j.codebran , j.codtyptr,j.desitrai,j.CODE_BRAN_RE,
 --  i.BRANCHE1,
  -- i.BRANCHE2,
   i.Acquisition,
   i.NbrePolice,
   j.Prime_cess_acquise,
   j.Pnette_acquise,
   j.montcomm_acquise,
   j.Cess_nt_acquise,
   j.Primbrut_acquise

from

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEBRAN,codtyptr,desitrai,CODE_BRAN_RE,
  -- BRANCHE1,
  -- BRANCHE2,
   --PRIME_CEDEE,
   sum(Acquisition) Acquisition,--tb_pn_risque_ref
   sum(NbrePolice) NbrePolice --tb_pn_risque_ref
from 
(select
   codeinte , 
   numepoli , 
  -- numeaven ,
   codebran , codtyptr,desitrai,CODE_BRAN_RE,
   --libebran ,
   --liberisq , 
   coderisq ,
   DATECOMP,
   DATEEFFE,
   DATEECHE,
   nvl(CODEINTE, 0)||nvl(NUMEPOLI, 0)||nvl(CODERISQ,0)||nvl(to_char(DATEECHE),'0') IDPOLICE,
   
   --MONTPRIM,

   reass_db.AnneePoliceAcquisition(trunc(DATEEFFE) ,trunc(DATEECHE) ,to_date('01/01/'||an) ,to_date('31/12/'||an)) Acquisition,
   EXPO,
   case when reass_db.AnneePoliceAcquisition(trunc(DATEEFFE) ,trunc(DATEECHE) ,to_date('01/01/'||an) ,to_date('31/12/'||an))>0 then 1 else 0 end kount,
   (case when reass_db.AnneePoliceAcquisition(trunc(DATEEFFE) ,trunc(DATEECHE) ,to_date('01/01/'||an) ,to_date('31/12/'||an))>0 then 1 else 0 end) *1 NbrePolice,
   an Annee_acquisition

from  reass_db.PRIME_CEDEE_ANT2021_REF)
group by to_char(datecomp,'YYYY'), Annee_acquisition,
  -- codeinte , 
   --numepoli , 
  -- numeaven ,
   --trunc(DATEECHE) ,
   codebran , codtyptr,desitrai,CODE_BRAN_RE--,
   --libebran ,
   --liberisq , 
   --coderisq 

) i,

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEBRAN,codtyptr,desitrai,CODE_BRAN_RE,
   --BRANCHE1,
   --BRANCHE2,
   sum(Prime_cess_acquise) Prime_cess_acquise,--pn_risque1
   sum(Pnette_acquise) Pnette_acquise,
   sum(montcomm_acquise) montcomm_acquise,
   sum(Cess_nt_acquise) Cess_nt_acquise,
   sum(Primbrut_acquise) Primbrut_acquise--pn_risque1
from
(select
   datecomp,
   CODERISQ,
    codebran , codtyptr,
    desitrai,
    CODE_BRAN_RE,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   case when reass_db.Exposition(trunc(DATEEFFE),trunc(DATEECHE))=0 then 0 else MONTCESS *reass_db.AnneePoliceAcquisition(trunc(DATEEFFE) ,trunc(DATEECHE) ,to_date('01/01/'||an) ,to_date('31/12/'||an))/reass_db.Exposition(trunc(DATEEFFE),trunc(DATEECHE)) end Prime_cess_acquise,
   case when reass_db.Exposition(trunc(DATEEFFE),trunc(DATEECHE))=0 then 0 else MONTPRIM *reass_db.AnneePoliceAcquisition(trunc(DATEEFFE) ,trunc(DATEECHE) ,to_date('01/01/'||an) ,to_date('31/12/'||an))/reass_db.Exposition(trunc(DATEEFFE),trunc(DATEECHE)) end Pnette_acquise,
   case when reass_db.Exposition(trunc(DATEEFFE),trunc(DATEECHE))=0 then 0 else MONTCOMM *reass_db.AnneePoliceAcquisition(trunc(DATEEFFE) ,trunc(DATEECHE) ,to_date('01/01/'||an) ,to_date('31/12/'||an))/reass_db.Exposition(trunc(DATEEFFE),trunc(DATEECHE)) end montcomm_acquise,
   case when reass_db.Exposition(trunc(DATEEFFE),trunc(DATEECHE))=0 then 0 else CESS_NT * reass_db.AnneePoliceAcquisition(trunc(DATEEFFE) ,trunc(DATEECHE) ,to_date('01/01/'||an) ,to_date('31/12/'||an))/reass_db.Exposition(trunc(DATEEFFE),trunc(DATEECHE)) end Cess_nt_acquise,
   case when reass_db.Exposition(trunc(DATEEFFE),trunc(DATEECHE))=0 then 0 else PRIMBRUT *reass_db.AnneePoliceAcquisition(trunc(DATEEFFE) ,trunc(DATEECHE) ,to_date('01/01/'||an) ,to_date('31/12/'||an))/(reass_db.Exposition(trunc(DATEEFFE),trunc(DATEECHE))) end Primbrut_acquise,
    an Annee_acquisition

from  REASS_DB.PRIME_CEDEE_ANT2021 where codeinte not in (9999,9998,9995) )
group by to_char(datecomp,'YYYY'), Annee_acquisition, CODEBRAN,codtyptr,desitrai,
CODE_BRAN_RE) j
where i.ANNEE=j.ANNEE(+) and i.Annee_acquisition=j.Annee_acquisition(+) and
   i.CODEBRAN=j.CODEBRAN(+)
   and  i.codtyptr=j.codtyptr(+)
    and  i.desitrai=j.desitrai(+)
     and  i.CODE_BRAN_RE=j.CODE_BRAN_RE(+)
   );
END LOOP;

end;

---drop table PRIME_CEDEES_ANT2021_acq_agg_f;

delete from PRIME_CEDEES_ANT2021_acq_agg_f;
insert into PRIME_CEDEES_ANT2021_acq_agg_f

---create table PRIME_CEDEES_ANT2021_acq_agg_f as
(

select
ANNEE                 ,
ANNEE_ACQUISITION     ,
CODEBRAN              ,
CODTYPTR    ,
DESITRAI              ,
CODE_BRAN_RE   CODE_BRAN_RE,
sum(ACQUISITION) ACQUISITION,
sum(NBREPOLICE) NBREPOLICE,
sum(PRIME_CESS_ACQUISE ) PRIME_CESS_ACQUISE,
sum(PNETTE_ACQUISE     ) PNETTE_ACQUISE   ,
sum(MONTCOMM_ACQUISE   ) MONTCOMM_ACQUISE   ,
sum(CESS_NT_ACQUISE    ) CESS_NT_ACQUISE   ,
sum(PRIMBRUT_ACQUISE   ) PRIMBRUT_ACQUISE  

from 
(
select distinct
ANNEE                 ,
ANNEE_ACQUISITION     ,
CODEBRAN              ,
CODTYPTR              ,
decode(codtyptr,'FQ','FQ',desitrai)  DESITRAI,
 DESITRAI   DESITRAI2           ,
CODE_BRAN_RE          ,
ACQUISITION           ,
NBREPOLICE            ,
PRIME_CESS_ACQUISE    ,
PNETTE_ACQUISE        ,
MONTCOMM_ACQUISE      ,
CESS_NT_ACQUISE       ,
PRIMBRUT_ACQUISE      

from PRIME_CEDEES_ANT2021_acq_agg 
)
group by
ANNEE                 ,
ANNEE_ACQUISITION     ,
CODEBRAN              ,
CODTYPTR    ,
DESITRAI ,
CODE_BRAN_RE
)              
;
 


delete from reass_db.TABLE_SINREASS ;

insert into reass_db.TABLE_SINREASS 
--drop table reass_db.TABLE_SINREASS ;
--create table  reass_db.TABLE_SINREASS as


(select distinct  * from
(

select
i.*,

decode(TAUXPRIM_DTV_AP,null,TAUXPRIM_UP,TAUXPRIM_DTV_AP)TX_PRIM

from

(select k.ID_GARANTIE  
       ,k.ANNEE          
       ,k.DEAL_DATE      
       ,k.CODEINTE       
       ,k.RAISOCIN       
       ,k.NUMESINI       
       ,k.REFEINTE       
       ,k.NUMEPOLI       
       ,k.NUMEAVEN       
       ,k.DATEEFFE       
       ,k.DATEECHE       
       ,k.CODEASSU       
       ,k.NOM            
       ,k.CODEBRAN 
       ,k.CODE_BRAN_RE      
       ,k.LIBEBRAN       
       ,k.CODECATE       
       ,k.CATERISQ       
       ,k.LIBECATE       
       ,k.CODEGARA       
       ,k.CODERISQ       
       ,k.NATUSINI       
       ,k.LIBEGARA       
       ,k.DATESURV       
       ,k.DATEDECL       
       ,k.SINPAY         
       ,k.SINPAY_ANT     
       ,k.RECENC         
       ,k.RECENC_ANT     
       ,k.SOLDE_PAIEMENTS
       ,k.EVAL           
       ,k.SAP            
       ,k.AREC           
       ,k.COUTS_SINISTRES
       
       
       ,(k.SINPAY/nvl(l.TAUXPART,1))           SINPAY_100        
       ,(k.SINPAY_ANT/nvl(l.TAUXPART,1))       SINPAY_ANT_100
       ,(k.RECENC/nvl(l.TAUXPART,1))           RECENC_100
       ,(k.RECENC_ANT/nvl(l.TAUXPART,1))       RECENC_ANT_100
       ,(k.SOLDE_PAIEMENTS/nvl(l.TAUXPART,1))  SOLDE_PAIEMENTS_100
       ,(k.EVAL/nvl(l.TAUXPART,1))             EVAL_100
       ,(k.SAP/nvl(l.TAUXPART,1))              SAP_100
       ,(k.AREC/nvl(l.TAUXPART,1))             AREC_100
       ,(k.COUTS_SINISTRES/nvl(l.TAUXPART,1))  COUTS_SINISTRES_100
       
       
       ,k.CODTYPSO       
       ,k.LIBTYPSO
       ,nvl(k.CODTYPTR,'NON TRAITE PROPOR') CODTYPTR
       ,K.DESITRAI
       ,k.TAUXPRIM
       ,case when CODTYPTR='CL' THEN q.TAUX_CL 
             when CODTYPTR='XP' THEN q.TAUX_EDP_UP
             when CODTYPTR='FQ' THEN q.TAUX_FAC_UP ELSE 0 END TAUXPRIM_UP
       ,case when CODTYPTR='CL' THEN Jl.TAUX_CL 
             when CODTYPTR='XP' THEN J.TAUX_EDP
             when CODTYPTR='FQ' THEN J.TAUX_FAC ELSE 0 END TAUXPRIM_DTV_AV
       , case when CODTYPTR='CL' THEN pl.TAUX_CL 
             when CODTYPTR='XP' THEN p.TAUX_EDP
             when CODTYPTR='FQ' THEN p.TAUX_FAC ELSE 0  END TAUXPRIM_DTV_AP
       , nvl(l.TAUXPART,1) TAUX_COASS    

  ---,case when (k.CODEINTE||'_'||k.NUMEPOLI||'_'||nvl(k.NUMEAVEN,0)||'_'||k.CODERISQ||'_'||k.CODEGARA=j.CODEINTE||'_'||j.NUMEPOLI||'_'||nvl(j.NUMEAVEN,0)||'_'||j.CODERISQ||'_'||j.CODEGARA and j.SOURCE='NOS CALCULS' and trunc(j.DATE_INS)='06/07/2022' and K.CODTYPTR='FQ') then j.TAUX_FAC else k.TAUXPRIM END TAUX_FAC_AVT  
  ---      ,case when (k.CODEINTE||'_'||k.NUMEPOLI||'_'||nvl(k.NUMEAVEN,0)=L.CODEINTE||'_'||L.NUMEPOLI||'_'||nvl(L.AVENMODI,0) and L.CODECOAS=0) then nvl(L.TAUXPART/100,1) else k.TAUXPRIM END TAUX_COASS
  ---      ,case when (k.CODEINTE||'_'||k.NUMEPOLI||'_'||nvl(k.NUMEAVEN,0)||'_'||k.CODERISQ||'_'||k.CODEGARA=j.CODEINTE||'_'||j.NUMEPOLI||'_'||nvl(j.NUMEAVEN,0)||'_'||j.CODERISQ||'_'||j.CODEGARA and j.SOURCE='NOS CALCULS' and trunc(j.DATE_INS)='06/07/2022' and K.CODTYPTR='CL') then j.TAUX_CL else k.TAUXPRIM END TAUX_CL_AVT
  ---      ,case when (k.CODEINTE||'_'||k.NUMEPOLI||'_'||nvl(k.NUMEAVEN,0)||'_'||k.CODERISQ||'_'||k.CODEGARA=j.CODEINTE||'_'||j.NUMEPOLI||'_'||nvl(j.NUMEAVEN,0)||'_'||j.CODERISQ||'_'||j.CODEGARA and j.SOURCE='NOS CALCULS' and trunc(j.DATE_INS)='06/07/2022' and K.CODTYPTR='XP') then j.TAUX_EDP else k.TAUXPRIM END TAUX_EDP_AVT
  ---      
  ---      ,case when (k.CODEINTE||'_'||k.NUMEPOLI||'_'||nvl(k.NUMEAVEN,0)||'_'||k.CODERISQ||'_'||k.CODEGARA=j.CODEINTE||'_'||j.NUMEPOLI||'_'||nvl(j.NUMEAVEN,0)||'_'||j.CODERISQ||'_'||j.CODEGARA and j.SOURCE='NOS CALCULS' and J.STATUT='ACTIF' and K.CODTYPTR='FQ') then j.TAUX_FAC else k.TAUXPRIM END TAUX_FAC_APR  
  ---      ,case when (k.CODEINTE||'_'||k.NUMEPOLI||'_'||nvl(k.NUMEAVEN,0)||'_'||k.CODERISQ||'_'||k.CODEGARA=j.CODEINTE||'_'||j.NUMEPOLI||'_'||nvl(j.NUMEAVEN,0)||'_'||j.CODERISQ||'_'||j.CODEGARA and j.SOURCE='NOS CALCULS' and J.STATUT='ACTIF' and K.CODTYPTR='CL') then j.TAUX_CL else k.TAUXPRIM END TAUX_CL_APR
  ---      ,case when (k.CODEINTE||'_'||k.NUMEPOLI||'_'||nvl(k.NUMEAVEN,0)||'_'||k.CODERISQ||'_'||k.CODEGARA=j.CODEINTE||'_'||j.NUMEPOLI||'_'||nvl(j.NUMEAVEN,0)||'_'||j.CODERISQ||'_'||j.CODEGARA and j.SOURCE='NOS CALCULS' and J.STATUT='ACTIF' and K.CODTYPTR='XP') then j.TAUX_EDP else k.TAUXPRIM END TAUX_EDP_APR

 from 
 
(select distinct * from 
(select i.*,
 reass_db.MD5(i.CODEINTE||'_'||i.NUMEPOLI||'_'||i.NUMEAVEN||'_'||i.CODERISQ||'_'||i.CODEGARA)ID_GARANTIE,
decode(J.CODTYPTR,null,u.codtyptr,J.CODTYPTR) CODTYPTR,
j.CODE_BRAN_RE,

decode(J.CODTYPTR,null,u.DESITRAI,decode(J.CODTYPTR,'FQ','FQ',J.DESITRAI)) DESITRAI,
decode(J.CODTYPTR,null,u.TAUXPRIM,nvl(J.TAUXPRIM/100,0)) TAUXPRIM
 from
(select * from REASS_DB.SINISTRE2021
where annee >= 2014 and CODTYPSO in ('RE','OU','TR')) i,

(select * from REASS_DB.PRIME_CEDEE_ANT2021 where to_char(dateeffe,'YYYY') >= 2014)j,

(select 
 distinct
CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA, 'CL' CODTYPTR,
'Cession précipitaire CICARE' DESITRAI,
TAUX_CL TAUXPRIM

 from REASS_DB.SAT_OUTPUT_REASS where STATUT='ACTIF' and TAUX_CL is not null
)u


where I.CODEINTE=j.CODEINTE(+)
  and I.NUMEPOLI=J.NUMEPOLI(+)
  and nvl(I.NUMEAVEN,0)=nvl(J.NUMEAVEN(+),0)
 and I.CODERISQ=J.CODERISQ(+)
  and I.CODEGARA=J.CODEGARA(+)
  
  and I.CODEINTE=u.CODEINTE(+)
  and I.NUMEPOLI=u.NUMEPOLI(+)
  and nvl(I.NUMEAVEN,0)=nvl(u.NUMEAVEN(+),0)

 )) k,

(select distinct ID_GARANTIE,TAUX_CL,TAUX_FAC,TAUX_EDP,TAUX_RETENTION,
  case when CODEBRAN in (2,3) and PRM_NT_100<>0 then PRIME_DECOUVERT/PRM_NT_100 else 0 end  TAUX_DECOUVERT
  from REASS_DB.SAT_OUTPUT_REASS where SOURCE='NOS CALCULS' and trunc(DATE_INS)='06/07/2022')  j,
  
(select distinct CODEINTE,NUMEPOLI,NUMEAVEN, max(TAUX_CL) TAUX_CL
  from REASS_DB.SAT_OUTPUT_REASS where SOURCE='NOS CALCULS' and trunc(DATE_INS)='06/07/2022'
  group by CODEINTE,NUMEPOLI,NUMEAVEN)  jl,  
  
  
(select distinct ID_GARANTIE,TAUX_CL,TAUX_FAC,TAUX_EDP,TAUX_RETENTION , 
case when CODEBRAN in (2,3) and PRM_NT_100<>0 then PRIME_DECOUVERT/PRM_NT_100 else 0 end  TAUX_DECOUVERT
from REASS_DB.SAT_OUTPUT_REASS where SOURCE='NOS CALCULS' and STATUT='ACTIF' )  p,

  
(select distinct CODEINTE,NUMEPOLI,NUMEAVEN, max(TAUX_CL) TAUX_CL
from REASS_DB.SAT_OUTPUT_REASS where SOURCE='NOS CALCULS' and STATUT='ACTIF' 
 group by CODEINTE,NUMEPOLI,NUMEAVEN)  pl,

(select distinct CODEINTE,NUMEPOLI, nvl(NUMEAVEN,0) NUMEAVEN, TAUXPART/100  TAUXPART from ORASS_V6.QUITTANCE_COASSURANCE where CODECOAS=0) l,

(select i.*,nvl(l.TAUXPART,1)  TAUXCOASS,
nvl(TAUX_FAC,0)*nvl(l.TAUXPART,1)*(1-nvl(TAUX_CL,0)) TAUX_FAC_UP,
TAUX_EDP TAUX_EDP_UP
from
(   
   select 
   distinct CODEINTE,NUMEPOLI,NUMEAVEN,
reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA) ID_GARANTIE,
max(case when CODTYPTR='FQ' then TAUXPRIM/100 else 0 end) TAUX_FAC,
max(case when CODTYPTR='CL' then TAUXPRIM/100 else 0 end) TAUX_CL,
max(case when CODTYPTR='XP' then TAUXPRIM/100 else 0 end )TAUX_EDP

FROM REASS_DB.PRIME_CEDEE_ANT2021 where to_char(dateeffe,'YYYY') >= 2014
group by CODEINTE,NUMEPOLI,NUMEAVEN,reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA) 

)i,

   
(select distinct CODEINTE,NUMEPOLI, NUMEAVEN, TAUXPART/100  TAUXPART from ORASS_V6.QUITTANCE_COASSURANCE where CODECOAS=0) l

where  i.CODEINTE=l.CODEINTE(+)
   and i.NUMEPOLI=l.NUMEPOLI(+)
   and i.NUMEAVEN=l.NUMEAVEN(+)
 
) q 
---,(select CODEGARA, CODBRARE CODE_BRAN_RE from TB_CODEGARA_CODBRANRE)b
   
 where K.ID_GARANTIE=J.ID_GARANTIE(+)
 and K.ID_GARANTIE=p.ID_GARANTIE(+)
  -- and K.NUMEPOLI=J.NUMEPOLI(+)
  -- and K.NUMEAVEN=J.NUMEAVEN(+)
  -- and K.CODERISQ=J.CODERISQ(+)
  -- and K.CODEGARA=J.CODEGARA(+)
   and K.CODEINTE=l.CODEINTE(+)
   and K.NUMEPOLI=l.NUMEPOLI(+)
   and k.NUMEAVEN=l.NUMEAVEN(+)
   
     and K.CODEINTE=jl.CODEINTE(+)
   and K.NUMEPOLI=jl.NUMEPOLI(+)
   and k.NUMEAVEN=jl.NUMEAVEN(+)
   
     and K.CODEINTE=pl.CODEINTE(+)
   and K.NUMEPOLI=pl.NUMEPOLI(+)
   and k.NUMEAVEN=pl.NUMEAVEN(+)
   
   and K.ID_GARANTIE=q.ID_GARANTIE(+)
 --  and K.CODEGARA=b.CODEGARA(+)
  -- and K.NUMEPOLI=b.NUMEPOLI(+)
  -- and k.NUMEAVEN=b.NUMEAVEN(+)
   )i
)
)
;









---drop table sinistre_cedes;
delete from sinistre_cedes;
    insert into sinistre_cedes

---create table sinistre_cedes as


SELECT

   ANNEE                        
   ,CODTYPTR                     
   ,DESITRAI                     
   ,CODEBRAN                     
   ,CODE_BRAN_RE                 
   ,SINPAY                       
   ,SINPAY_ANT                   
   ,RECENC                       
   ,RECENC_ANT                   
   ,EVAL                         
   ,SAP                          
   ,AREC                         
   ,COUTS_SINISTRES              
   ,SINPAY_100                   
   ,SINPAY_ANT_100               
   ,RECENC_100                   
   ,RECENC_ANT_100               
   ,EVAL_100                     
   ,SAP_100                      
   ,AREC_100                     
   ,COUTS_SINISTRES_100          
   ,SINPAY_GRAVES                
   ,SINPAY_ANT_GRAVES            
   ,RECENC_GRAVES                
   ,RECENC_ANT_GRAVES            
   ,EVAL_GRAVES                  
   ,SAP_GRAVES                   
   ,AREC_GRAVES                  
   ,COUTS_SINISTRES_GRAVES       
   ,SINPAY_GRAVES_100            
   ,SINPAY_ANT_GRAVES_100        
   ,RECENC_GRAVES_100            
   ,RECENC_ANT_GRAVES_100        
   ,EVAL_GRAVES_100              
   ,SAP_GRAVES_100               
   ,AREC_GRAVES_100              
   ,COUTS_SINISTRES_GRAVES_100   
   ,SINPAY_CESS                  
   ,SINPAY_ANT_CESS  
   ,SINPAY_CESS+ SINPAY_ANT_CESS as PAYES_BR_REC_CESS
              
   ,RECENC_CESS                  
   ,RECENC_ANT_CESS   
   
   ,SINPAY_CESS+ SINPAY_ANT_CESS-RECENC_CESS-RECENC_ANT_CESS as  PAYES_NT_REC_CESS
              
   ,EVAL_CESS                    
   ,SAP_CESS   as SAP_BR_REC_CESS                     
   ,AREC_CESS   
   
   ,SAP_CESS-AREC_CESS as  SAP_NT_REC_CESS
   
   ,SINPAY_CESS+ SINPAY_ANT_CESS+SAP_CESS  as COUTS_BR_REC_CESS
   
   ,COUTS_SINISTRES_CESS   as COUTS_NT_REC_CESS
   
   
        
   ,SINPAY_GRAVES_CESS           
   ,SINPAY_ANT_GRAVES_CESS  
   ,SINPAY_GRAVES_CESS+ SINPAY_ANT_GRAVES_CESS as PAYES_BR_REC_GRAVES_CESS   
     
   ,RECENC_GRAVES_CESS           
   ,RECENC_ANT_GRAVES_CESS   
   
   ,SINPAY_GRAVES_CESS+ SINPAY_ANT_GRAVES_CESS-RECENC_GRAVES_CESS-RECENC_ANT_GRAVES_CESS as  PAYES_NT_REC_GRAVES_CESS
    
   ,EVAL_GRAVES_CESS             
   ,SAP_GRAVES_CESS   as SAP_BR_REC_GRAVES_CESS           
   ,AREC_GRAVES_CESS 
   
   ,SAP_GRAVES_CESS- AREC_GRAVES_CESS as SAP_NT_REC_GRAVES_CESS
               
   ,SINPAY_GRAVES_CESS+ SINPAY_ANT_GRAVES_CESS+SAP_GRAVES_CESS  as COUTS_BR_REC_GRAVES_CESS
   ,COUTS_SINISTRES_GRAVES_CESS as COUTS_NT_REC_GRAVES_CESS 
   
   
   
FROM


(
select annee,CODTYPTR,DESITRAI,codebran,CODE_BRAN_RE,sum(sinpay) sinpay, sum(sinpay_ant) sinpay_ant, sum(recenc) recenc,
sum(recenc_ant) recenc_ant, sum(eval) eval, sum(sap) sap, sum(arec) arec,
sum(couts_sinistres) couts_sinistres,

  sum(sinpay_100)               sinpay_100, 
  sum(sinpay_ant_100)           sinpay_ant_100, 
  sum(recenc_100)               recenc_100,
  sum(recenc_ant_100)           recenc_ant_100, 
  sum(eval_100)                 eval_100, 
  sum(sap_100)                  sap_100, 
  sum(arec_100)                 arec_100,
  sum(couts_sinistres_100)      couts_sinistres_100,
  
  sum(sinpay*graves)              sinpay_graves,
  sum(sinpay_ant*graves)          sinpay_ant_graves,
  sum(recenc*graves)              recenc_graves,
  sum(recenc_ant*graves)          recenc_ant_graves, 
  sum(eval*graves)                eval_graves,
  sum(sap*graves)                 sap_graves, 
  sum(arec*graves)                arec_graves,
  sum(couts_sinistres*graves)     couts_sinistres_graves,
  
  sum(sinpay_100*graves)          sinpay_graves_100,
  sum(sinpay_ant_100*graves)      sinpay_ant_graves_100,
  sum(recenc_100*graves)          recenc_graves_100,
  sum(recenc_ant_100*graves)      recenc_ant_graves_100, 
  sum(eval_100*graves)            eval_graves_100,
  sum(sap_100*graves)             sap_graves_100, 
  sum(arec_100*graves)            arec_graves_100,
  sum(couts_sinistres_100*graves) couts_sinistres_graves_100,
  
  sum(case when  CODTYPTR='CL'  then  sinpay_100*TAUX_COASS*TX_PRIM 
       when  CODTYPTR='XP' then  sinpay_100*TX_PRIM 
       when  CODTYPTR='FQ' then  sinpay_100*TX_PRIM end ) sinpay_cess,

    sum(case when  CODTYPTR='CL'  then  sinpay_ant_100*TAUX_COASS*TX_PRIM 
       when  CODTYPTR='XP' then  sinpay_ant_100*TX_PRIM 
       when  CODTYPTR='FQ' then  sinpay_ant_100*TX_PRIM end ) sinpay_ant_cess,
       
      
     sum(case when  CODTYPTR='CL'  then  recenc_100*TAUX_COASS*TX_PRIM 
       when  CODTYPTR='XP' then  recenc_100*TX_PRIM 
       when  CODTYPTR='FQ' then  recenc_100*TX_PRIM end ) recenc_cess,
       
    sum(case when  CODTYPTR='CL'  then  recenc_ant_100*TAUX_COASS*TX_PRIM 
       when  CODTYPTR='XP' then  recenc_ant_100*TX_PRIM 
       when  CODTYPTR='FQ' then  recenc_ant_100*TX_PRIM end ) recenc_ant_cess,
       
       
     sum(case when  CODTYPTR='CL'  then  eval_100*TAUX_COASS*TX_PRIM 
       when  CODTYPTR='XP' then  eval_100*TX_PRIM 
       when  CODTYPTR='FQ' then  eval_100*TX_PRIM end ) eval_cess,
       
       
     sum(case when  CODTYPTR='CL'  then  sap_100*TAUX_COASS*TX_PRIM 
       when  CODTYPTR='XP' then  sap_100*TX_PRIM 
       when  CODTYPTR='FQ' then  sap_100*TX_PRIM end ) sap_cess,        

  sum(case when  CODTYPTR='CL'  then  arec_100*TAUX_COASS*TX_PRIM 
       when  CODTYPTR='XP' then  arec_100*TX_PRIM 
       when  CODTYPTR='FQ' then  arec_100*TX_PRIM end ) arec_cess,   
       
       
    sum(case when  CODTYPTR='CL'  then  couts_sinistres_100*TAUX_COASS*TX_PRIM 
       when  CODTYPTR='XP' then  couts_sinistres_100*TX_PRIM 
       when  CODTYPTR='FQ' then  couts_sinistres_100*TX_PRIM end ) couts_sinistres_cess, 
         
       
   sum(case when  CODTYPTR='CL'  then  sinpay_100*TAUX_COASS*TX_PRIM*graves  
       when  CODTYPTR='XP' then  sinpay_100*TX_PRIM*graves  
       when  CODTYPTR='FQ' then  sinpay_100*TX_PRIM*graves  end ) sinpay_graves_cess,

    sum(case when  CODTYPTR='CL'  then  sinpay_ant_100*TAUX_COASS*TX_PRIM*graves  
       when  CODTYPTR='XP' then  sinpay_ant_100*TX_PRIM*graves  
       when  CODTYPTR='FQ' then  sinpay_ant_100*TX_PRIM*graves  end ) sinpay_ant_graves_cess,
       
     sum(case when  CODTYPTR='CL'  then  recenc_100*TAUX_COASS*TX_PRIM*graves  
       when  CODTYPTR='XP' then  recenc_100*TX_PRIM*graves  
       when  CODTYPTR='FQ' then  recenc_100*TX_PRIM*graves  end ) recenc_graves_cess,
       
    sum(case when  CODTYPTR='CL'  then  recenc_ant_100*TAUX_COASS*TX_PRIM*graves  
       when  CODTYPTR='XP' then  recenc_ant_100*TX_PRIM*graves  
       when  CODTYPTR='FQ' then  recenc_ant_100*TX_PRIM*graves  end ) recenc_ant_graves_cess,
       
       
     sum(case when  CODTYPTR='CL'  then  eval_100*TAUX_COASS*TX_PRIM*graves  
       when  CODTYPTR='XP' then  eval_100*TX_PRIM*graves  
       when  CODTYPTR='FQ' then  eval_100*TX_PRIM*graves  end ) eval_graves_cess,
       
       
     sum(case when  CODTYPTR='CL'  then  sap_100*TAUX_COASS*TX_PRIM*graves  
       when  CODTYPTR='XP' then  sap_100*TX_PRIM*graves  
       when  CODTYPTR='FQ' then  sap_100*TX_PRIM*graves  end ) sap_graves_cess,        

  sum(case when  CODTYPTR='CL'  then  arec_100*TAUX_COASS*TX_PRIM*graves  
       when  CODTYPTR='XP' then  arec_100*TX_PRIM*graves  
       when  CODTYPTR='FQ' then  arec_100*TX_PRIM*graves  end ) arec_graves_cess,   
       
       
    sum(case when  CODTYPTR='CL'  then  couts_sinistres_100*TAUX_COASS*TX_PRIM*graves 
       when  CODTYPTR='XP' then  couts_sinistres_100*TX_PRIM*graves  
       when  CODTYPTR='FQ' then  couts_sinistres_100*TX_PRIM*graves  end ) couts_sinistres_graves_cess

from

--(
--ID_GARANTIE,
--ANNEE,CODEINTE,CODTYPTR,DESITRAI,codebran


 (
 select
 distinct
k.*,nvl(l.graves,0) graves,
nvl(TX_PRIM/TAUX_COASS,0) TX_PRIM_100


from

(
select distinct * from TABLE_SINREASS
)k,
  
  (select
distinct ANNEE,CODEINTE,NUMESINI, graves

from reass_db.tb_b0sinistre_grave where graves=1

)l

where K.CODEINTE=L.CODEINTE(+)
  and K.NUMESINI=L.NUMESINI(+)
  and K.ANNEE=L.ANNEE(+))
--)
  group by annee,CODTYPTR,codebran,DESITRAI,CODE_BRAN_RE

)
union all
select * from reass_db.sinistre_cedes_reass

;

---drop table tb_analyse_sp_reass;
delete from tb_analyse_sp_reass;
insert into tb_analyse_sp_reass

---create table tb_analyse_sp_reass as
(
select
 i.ANNEE,
 i.CODEBRAN      ,
 BRANCHE1, BRANCHE2,
 i.CODTYPTR      ,    
 i.DESITRAI      ,
 i.CODE_BRAN_RE  , 
nvl(ACQUISITION          ,0) ACQUISITION        ,
nvl(NBREPOLICE           ,0) NBREPOLICE         ,
nvl(PRIME_CESS_ACQUISE   ,0) PRIME_CESS_ACQUISE ,
---nvl(PNETTE_ACQUISE       ,0) PNETTE_ACQUISE     ,
nvl(MONTCOMM_ACQUISE     ,0) MONTCOMM_ACQUISE   --,
---nvl(CESS_NT_ACQUISE      ,0) CESS_NT_ACQUISE    ,
---nvl(PRIMBRUT_ACQUISE     ,0) PRIMBRUT_ACQUISE 
,
decode(i.codtyptr,'XS',px.NBSIN,p.NBSIN) NBSIN, 
SINPAY                            ,
SINPAY_ANT                        ,
RECENC                            ,
RECENC_ANT                        ,
EVAL                              ,
SAP                               ,
AREC                              ,
COUTS_SINISTRES                   ,

sinpay_100, 
sinpay_ant_100, 
recenc_100,
recenc_ant_100, 
eval_100, 
sap_100, 
arec_100,
couts_sinistres_100,

decode(i.codtyptr,'XS',qx.NBSIN_GRAVES,q.NBSIN_GRAVES) NBSIN_GRAVES,
SINPAY_GRAVES                     ,
SINPAY_ANT_GRAVES                 ,
RECENC_GRAVES                     ,
RECENC_ANT_GRAVES                 ,
EVAL_GRAVES                       ,
SAP_GRAVES                        ,
AREC_GRAVES                       ,
COUTS_SINISTRES_GRAVES            ,

sinpay_graves_100                 ,
sinpay_ant_graves_100             ,
recenc_graves_100                 ,
recenc_ant_graves_100             , 
eval_graves_100                   ,
sap_graves_100                     , 
arec_graves_100                    ,
couts_sinistres_graves_100           ,

SINPAY_CESS                  ,
SINPAY_ANT_CESS              ,
PAYES_BR_REC_CESS            ,
RECENC_CESS                  ,
RECENC_ANT_CESS              ,
PAYES_NT_REC_CESS            ,
EVAL_CESS                    ,
SAP_BR_REC_CESS              ,
AREC_CESS                    ,
SAP_NT_REC_CESS              ,
COUTS_BR_REC_CESS            ,
COUTS_NT_REC_CESS            ,
SINPAY_GRAVES_CESS           ,
SINPAY_ANT_GRAVES_CESS       ,
PAYES_BR_REC_GRAVES_CESS     ,
RECENC_GRAVES_CESS           ,
RECENC_ANT_GRAVES_CESS       ,
PAYES_NT_REC_GRAVES_CESS     ,
EVAL_GRAVES_CESS             ,
SAP_BR_REC_GRAVES_CESS       ,
AREC_GRAVES_CESS             ,
SAP_NT_REC_GRAVES_CESS       ,
COUTS_BR_REC_GRAVES_CESS     ,
COUTS_NT_REC_GRAVES_CESS   


from

(
select distinct *

from
(
select distinct to_char(ANNEE_ACQUISITION) ANNEE,nvl(CODEBRAN,0) CODEBRAN , nvl(CODTYPTR,'NON TRAITE PROPOR') CODTYPTR,
 nvl(DESITRAI,'NON TRAITE PROPOR') DESITRAI,nvl(CODE_BRAN_RE,1000) CODE_BRAN_RE from PRIME_CEDEES_ANT2021_acq_agg_f
union all
select distinct  ANNEE,nvl(CODEBRAN,0) CODEBRAN , nvl(CODTYPTR,'NON TRAITE PROPOR')  CODTYPTR,
 nvl(DESITRAI,'NON TRAITE PROPOR') DESITRAI,nvl(CODE_BRAN_RE,1000) CODE_BRAN_RE from sinistre_cedes

)
) i, 
 
(
select 

 ANNEE_ACQUISITION ,
 nvl(CODEBRAN,0) CODEBRAN      ,
 nvl(CODTYPTR,'NON TRAITE PROPOR') CODTYPTR      ,    
  nvl(DESITRAI,'NON TRAITE PROPOR')DESITRAI      ,
 nvl(CODE_BRAN_RE,1000) CODE_BRAN_RE  , 
sum(ACQUISITION ) ACQUISITION        ,
sum(NBREPOLICE         ) NBREPOLICE         ,
sum(PRIME_CESS_ACQUISE ) PRIME_CESS_ACQUISE ,
sum(PNETTE_ACQUISE     ) PNETTE_ACQUISE     ,
sum(MONTCOMM_ACQUISE   ) MONTCOMM_ACQUISE  

from PRIME_CEDEES_ANT2021_acq_agg_f where ANNEE_ACQUISITION>= 2014
group by
ANNEE_ACQUISITION ,
 nvl(CODEBRAN,0)       ,
 nvl(CODTYPTR,'NON TRAITE PROPOR')       ,    
  nvl(DESITRAI,'NON TRAITE PROPOR')      ,
 nvl(CODE_BRAN_RE,1000)
 ) a, 
 
 sinistre_cedes j,
 


(
select distinct
ANNEE,nvl(CODEBRAN,0) CODEBRAN,CODTYPTR,DESITRAI,CODE_BRAN_RE,
count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN 

from TABLE_SINREASS
group by
ANNEE,nvl(CODEBRAN,0) ,CODTYPTR,DESITRAI,CODE_BRAN_RE
)p,

(select distinct
ANNEE,nvl(CODEBRAN,0) CODEBRAN,CODTYPTR,DESITRAI,CODE_BRAN_RE,
count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN_GRAVES 
 from
(
select
distinct
k.*,nvl(l.graves,0) graves


from

(
select distinct * from TABLE_SINREASS
)k,
  
  (select
distinct ANNEE,CODEINTE,NUMESINI, graves

from reass_db.tb_sinistre_grave where graves=1

)l

where K.CODEINTE=L.CODEINTE(+)
  and K.NUMESINI=L.NUMESINI(+)
  and K.ANNEE=L.ANNEE(+)

) where graves=1
group by ANNEE,nvl(CODEBRAN,0),CODTYPTR,DESITRAI,CODE_BRAN_RE
)q,



(
select distinct
ANNEE,nvl(CODEBRAN,0) CODEBRAN, 'XS' CODTYPTR,DESITRAN DESITRAI,CODE_BRAN_RE,
count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN 

from TABLE_SINREASS_3
group by
ANNEE,nvl(CODEBRAN,0) ,'XS',DESITRAN,CODE_BRAN_RE
)px,

(
select distinct
ANNEE,nvl(CODEBRAN,0) CODEBRAN, 'XS' CODTYPTR, DESITRAN DESITRAI,CODE_BRAN_RE,
count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN_GRAVES 
 from TABLE_SINREASS_3 where graves =1

group by ANNEE,nvl(CODEBRAN,0),'XS',DESITRAN,CODE_BRAN_RE
)qx,





(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from reass_db.branche)k


where i.ANNEE =    j.ANNEE (+)
 and  i.CODEBRAN  = nvl(j.CODEBRAN (+) ,0)  
 and  i.CODTYPTR  = j.CODTYPTR (+)  
 and  i.DESITRAI  = nvl(j.DESITRAI(+),'NON TRAITE PROPOR')
  and i.CODE_BRAN_RE = nvl(j.CODE_BRAN_RE(+),1000)
  
 and  i.ANNEE =     a.ANNEE_ACQUISITION (+)
 and  i.CODEBRAN  = a.CODEBRAN (+)   
 and  i.CODTYPTR  = a.CODTYPTR (+)     
 and  i.DESITRAI  = a.DESITRAI(+)
 and  i.CODE_BRAN_RE  =a.CODE_BRAN_RE(+)
  
 and  i.ANNEE = p.ANNEE (+)
 and  i.CODEBRAN  = p.CODEBRAN (+)   
 and  i.CODTYPTR  =  nvl(p.CODTYPTR (+),'NON TRAITE PROPOR')     
 and  i.DESITRAI  = nvl(p.DESITRAI(+),'NON TRAITE PROPOR')
 and  i.CODE_BRAN_RE  = nvl(p.CODE_BRAN_RE(+),1000)

 
  and i.ANNEE = q.ANNEE (+)
 and  i.CODEBRAN  = q.CODEBRAN (+)   
 and  i.CODTYPTR  = nvl(q.CODTYPTR (+),'NON TRAITE PROPOR')        
 and  i.DESITRAI  = nvl(q.DESITRAI(+),'NON TRAITE PROPOR')
 and  i.CODE_BRAN_RE  = nvl(q.CODE_BRAN_RE(+),1000)
 
 and  i.ANNEE = px.ANNEE (+)
 and  i.CODEBRAN  = px.CODEBRAN (+)   
 and  i.CODTYPTR  =  nvl(px.CODTYPTR (+),'NON TRAITE PROPOR')     
 and  i.DESITRAI  = nvl(px.DESITRAI(+),'NON TRAITE PROPOR')
 and  i.CODE_BRAN_RE  = nvl(px.CODE_BRAN_RE(+),1000)

 
  and i.ANNEE = qx.ANNEE (+)
 and  i.CODEBRAN  = qx.CODEBRAN (+)   
 and  i.CODTYPTR  = nvl(qx.CODTYPTR (+),'NON TRAITE PROPOR')        
 and  i.DESITRAI  = nvl(qx.DESITRAI(+),'NON TRAITE PROPOR')
 and  i.CODE_BRAN_RE  = nvl(qx.CODE_BRAN_RE(+),1000)
 
 
 
 and  i.CODEBRAN  = k.CBR (+) 
);



----------------------------  COMMISSION------------------------------------------------------
--create table reass_db.for_check as
delete from reass_db.for_check;
insert into reass_db.for_check
select  i.ID_GARANTIE          
       ,i.ID_REASSURANCE_CL    
       ,i.ID_REASSURANCE_FAC   
       ,i.ID_REASSURANCE_EDP   
       ,i.DATE_INS             
       ,i.DATE_FIN             
       ,i.SOURCE               
       ,i.ID_SAT_OUTPUT_REASS  
       ,i.CODEINTE             
       ,i.NUMEPOLI             
       ,i.NUMEAVEN             
       ,i.DATEEFFE             
       ,i.DATEECHE             
       ,i.CODECATE             
       ,i.LIBECATE             
       ,i.CODEBRAN             
       ,i.LIBEBRAN             
       ,i.CODEGARA             
       ,i.LIBEL_BRAN_RE        
       ,i.LIBERISQ             
       ,i.CODERISQ             
       ,i.LIBECLASS            
       ,i.CLASS_RISQ           
       ,i.ACTIVITE             
       ,i.CODEASSU             
       ,i.NOM                  
       ,i.CAPITAUX_100         
       ,i.SMP_100              
       ,i.PRM_NT_100           
       ,i.TAUX_COASS       
       ,i.TAUX_CL                      
       ,i.PRIME_CEDEE_CL        
       ,i.TAUX_FAC    
       ,i.TAUX_EDP 
       ,i.TAUX_RETENTION
       ,0 TAUX_COMM_CL
       ,0 TAUX_COMM_EDP
       ,decode(SOURCE,'NOS CALCULS',decode(i.DATE_INS,to_date('06/07/2022'),j.TAUX_COM_AVT,j.TAUX_COM_APR),0) TAUX_COM_FAC
       ,i.STATUT               
 
from REASS_DB.SAT_OUTPUT_REASS i,
(select K.ID_REASSURANCE
        , sum(decode(k.DATE_INS,to_date('06/07/2022'),K.TAUX_COM_REASSURANCE*K.PART_REASSUREUR,0)) TAUX_COM_AVT
        ,sum(decode(K.STATUT,'ACTIF',K.TAUX_COM_REASSURANCE*K.PART_REASSUREUR,0)) TAUX_COM_APR 
from REASS_DB.SAT_LINK_REASS_REASSUREUR k 
group by K.ID_REASSURANCE)j
where I.ID_REASSURANCE_FAC=J.ID_REASSURANCE(+);

-------------------- SINISTRES GRAVES ---------------------------------
delete from reass_db.for_check;
insert into reass_db.for_check
select i.* from
(select * from REASS_DB.SINISTRE2021
where annee >= 2014 and CODTYPSO in ('RE','OU','TR')) i,
(select
distinct ANNEE,CODEINTE,NUMESINI, graves
from reass_db.tb_sinistre_grave where graves=1
)j
where i.CODEINTE=j.CODEINTE
  and i.NUMESINI=j.NUMESINI
  and i.ANNEE=j.ANNEE;



 commit;

 --end;
/
