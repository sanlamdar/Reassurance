CREATE OR REPLACE PROCEDURE REASS_DB.proc_insert_v2( src in varCHAR2, dte_ins in varchar2) as -- dte_ins pour marquer la date d'insertion exple: 06072022 

begin


----IMPUT GARANTIE A METTRE DANS LE STAGGING------------
--drop table REASS_DB.PIT_GARANTIES
delete from REASS_DB.BDR_GARANTIES;
insert into REASS_DB.BDR_GARANTIES
---create table REASS_DB.BDR_GARANTIES as
(

select distinct
    
     i.NUMEPOLI,i.CODEINTE,i.NUMEAVEN,i.CODERISQ,i.CODEGARA , nvl(j.NUMEAVEN_up,0) NUMEAVEN_up
     --case when nvl(j.NUMEAVEN_up,0)=0 then k.NUMEAVEN_up else  j.NUMEAVEN_up end NUMEAVEN_up
     from
     (select distinct NUMEPOLI,CODEINTE,NUMEAVEN,CODERISQ,CODEGARA from REASS_DB.GARANTIES)i,
     (select distinct NUMEPOLI,CODEINTE,AVENMODI NUMEAVEN_up,CODERISQ,CODEGARA from REASS_DB.TB_GARANTIE_REASS)j
     --,
   --  (select distinct NUMEPOLI,CODEINTE,CODERISQ,CODEGARA,min(AVENMODI) NUMEAVEN_up from REASS_DB.TB_GARANTIE_REASS
   --   group by NUMEPOLI,CODEINTE,CODERISQ,CODEGARA)k
     
     where i.NUMEPOLI=j.NUMEPOLI(+)
       and i.CODEINTE=j.CODEINTE(+)
       and i.CODERISQ=j.CODERISQ(+)
       and i.CODEGARA=j.CODEGARA(+)
       and i.NUMEAVEN=j.NUMEAVEN_up(+) 
    
    )
;
--delete from REASS_DB.GARANTIES1;
--drop table REASS_DB.TB_GARANTIE_REASS;
delete from REASS_DB.TB_GARANTIE_REASS;
insert into REASS_DB.TB_GARANTIE_REASS
--create table REASS_DB.TB_GARANTIE_REASS as
(
select
distinct
i.CODEINTE,i.NUMEPOLI,i.AVENMODI,i.NUMEAVEN,CODEBRAN,LIBEBRAN,CODEASSU,RAISOCIN,NOM_ASSU,CODERISQ,
i.CODEGARA,j.CODBRARE, LIBBRARE, CAPITAUX, CAPITAUX_RE, SMP SMP_RE

from
(select k.CODEINTE,k.NUMEPOLI,k.AVENMODI,k.NUMEAVEN,l.CODEBRAN,l.LIBEBRAN,l.CODEASSU,l.RAISOCIN,l.NOM_ASSU,k.CODERISQ,
k.CODEGARA,k.CODBRARE,k.CAPIASSU CAPITAUX,k.MONTCAPI CAPITAUX_RE
--,k.MONT_SMP SMP_RE
from
(select I.CODEINTE,I.NUMEPOLI,I.AVENMODI,I.CODERISQ,I.CAPIASSU,i.NUMEAVEN,
i.CODEGARA,j.CODBRARE,j.MONTCAPI
--,P.MONT_SMP
from
(select distinct CODEINTE,NUMEPOLI,AVENMODI,NUMEAVEN,CODERISQ,CAPIASSU,CODEGARA 
from ORASS_V6.IMAGE_GARANTIE_ACCORDEE)i,

(select distinct CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODBRARE,MONTCAPI
--,MONT_SMP 
from ORASS_V6.SMP_RISQUE)j

--,
where i.CODEINTE=j.CODEINTE(+) and
      I.NUMEPOLI=J.NUMEPOLI(+) and
      I.AVENMODI=J.NUMEAVEN(+) and
      I.CODERISQ=J.CODERISQ(+) --and
      
    
      )k,
(select distinct CODEINTE, NUMEPOLI, NUMEAVEN,CODEBRAN,LIBEBRAN,CODEASSU,RAISOCIN,NOM_ASSU 
from ORASS_V6.V_CH_AFFAIRE)l

where k.CODEINTE=l.CODEINTE(+) and
      k.NUMEPOLI=l.NUMEPOLI(+) and
      k.AVENMODI=l.NUMEAVEN(+)
)i,
(select distinct CODBRARE,LIBBRARE
from ORASS_V6.BRANCHE_REASS)m  ,

(
select distinct i.CODEGARA,decode(i.CODEGARA, '2PLQ',23,j.CODBRARE) CODBRARE
from
(
select  CODEGARA,  max (n) n from 
(select distinct CODEGARA, CODBRARE,
   count(*) n
FROM ORASS_V6.GARANTIE where CODBRARE is not null 
group by CODEGARA, CODBRARE 
 ) group by CODEGARA 
)i,

(select distinct CODEGARA, CODBRARE,
   count(*) n
FROM ORASS_V6.GARANTIE where CODBRARE is not null 
group by CODEGARA, CODBRARE  
)j
 where     i.CODEGARA=j.CODEGARA
 and       i.n=j.n
 )j,
 
 (select distinct CODEINTE,NUMEPOLI,NUMEAVEN,
 CODBRARE, MONT_SMP SMP
 from ORASS_V6.SMP_POLICE )p
  
  where  i.CODBRARE=m.CODBRARE(+)    
  and i.CODEGARA=j.CODEGARA
  and i.CODBRARE=j.CODBRARE
  
  and I.CODEINTE=p.CODEINTE(+) 
  and I.NUMEPOLI=p.NUMEPOLI(+) 
  and I.AVENMODI=p.NUMEAVEN(+) 
  and I.CODBRARE=p.CODBRARE(+) 
      );    
--drop table GARANTIES1

delete from
    REASS_DB.GARANTIES1;
insert into
    REASS_DB.GARANTIES1 
    -- create table REASS_DB.GARANTIES1 as 
    (
        select
            NUMEPOLI,
            CODEINTE,
            NUMEAVEN,
            DATECOMP,
            DATEEFFE,
            DATEECHE,
            CODECATE,
            LIBECATE,
            CODEBRAN,
            LIBEBRAN,
            CODEGARA,
            CODE_BRAN_RE,
            LIBEL_BRAN_RE,
            CODERISQ,
            LIBERISQ,
            CLASS_RISQ,
            LIBECLASS,
            case 
                when CAPITAUX is null then CAPITAUX_R 
                else CAPITAUX 
            end CAPITAUX,
            case 
                when SMP is null then SMP_R 
                else SMP 
            end SMP,
            PRM_NT,
            CODE_GARANTIE_REF
        from
            (
                select
                    i.NUMEPOLI,
                    i.CODEINTE,
                    i.NUMEAVEN,
                    i.DATECOMP,
                    i.DATEEFFE,
                    i.DATEECHE,
                    i.CODECATE,
                    i.LIBECATE,
                    i.CODEBRAN,
                    i.LIBEBRAN,
                    i.CODEGARA,
                    case 
                        when j.CODBRARE is null then q.CODBRARE 
                        else j.CODBRARE 
                    end CODE_BRAN_RE,
                    case 
                        when j.LIBBRARE is null then q.LIBBRARE 
                        else j.LIBBRARE 
                    end LIBEL_BRAN_RE,
                    i.CODERISQ,
                    i.LIBERISQ,
                    i.CLASS_RISQ,
                    i.LIBECLASS,
                    j.CAPITAUX CAPITAUX,
                    i.CAPITAUX CAPITAUX_R,
                    j.SMP_RE SMP,
                    i.SMP SMP_R,
                    --i.PRM_NT,
                    decode (p.PRIMNETT,NULL,a.PRIMNETT,p.PRIMNETT) PRM_NT,
                    i.CODE_GARANTIE_REF
                from
                    (
                        select
                            distinct *
                        from
                            REASS_DB.GARANTIES
                    ) i,
                    (
                        select
                            distinct NUMEPOLI,
                            CODEINTE,
                            AVENMODI,
                            CODERISQ,
                            CODEGARA,
                            CODBRARE,
                            LIBBRARE,
                            CAPITAUX,
                            SMP_RE
                        from
                            REASS_DB.TB_GARANTIE_REASS
                    ) j,
                    (
                        select
                            distinct 
                            CODEGARA,
                            max(CODBRARE) CODBRARE,
                            max(LIBBRARE) LIBBRARE
                        from
                            REASS_DB.TB_GARANTIE_REASS
                            group by CODEGARA
                    ) q,
                    
                    (select distinct CODEINTE,NUMEPOLI,CODERISQ,NUMEAVEN,CODEGARA,PRIMNETT
                    from 
                    (select distinct
                    CODEINTE,NUMEPOLI,CODERISQ,nvl(AVENMODI,0)NUMEAVEN,CODEGARA,
                    sum(MONTGARA) PRIMNETT
                    from ORASS_V6.GARANTIE_ACCORDEE
                    group by CODEINTE,NUMEPOLI,CODERISQ,nvl(AVENMODI,0),CODEGARA
                    
                    union all
                    select distinct
                    CODEINTE,NUMEPOLI,CODERISQ,nvl(AVENMODI,0)NUMEAVEN,CODEGARA,
                    sum(MONTGARA) PRIMNETT
                    from ORASS_V6.HIST_GARANTIE_ACCORDEE
                    group by CODEINTE,NUMEPOLI,CODERISQ,nvl(AVENMODI,0),CODEGARA
                    )
                    
                    )a,
                    
                    (select
                        distinct 
                         i.NUMEPOLI,
                        i.CODEINTE,
                        nvl(i.NUMEAVEN,0) NUMEAVEN,
                        CODERISQ,
                        CODEGARA,
                        
                        decode (j.SORTQUIT,2,0,PRIMNETT) PRIMNETT
                        from
                            ORASS_V6.PRIME_GARANTIE i,
                       (select distinct NUMEPOLI, CODEINTE,nvl(NUMEAVEN,0) NUMEAVEN,SORTQUIT from  ORASS_V6.QUITTANCE)j
                        
                       where
                              i.NUMEPOLI=j.NUMEPOLI(+)
                         and  i.CODEINTE=j.CODEINTE(+)
                         and  nvl(i.NUMEAVEN,0)=j.NUMEAVEN(+)

                    ) p                
                where
                    i.NUMEPOLI = j.NUMEPOLI(+)
                    and i.CODEINTE = j.CODEINTE(+)
                    and i.CODERISQ = j.CODERISQ(+)
                    and i.CODEGARA = j.CODEGARA(+)
                    and i.NUMEAVEN = j.AVENMODI(+)
                  --  and i.NUMEPOLI = q.NUMEPOLI(+)
                  --  and i.CODEINTE = q.CODEINTE(+)
                  --  and i.CODERISQ = q.CODERISQ(+)
                    and i.CODEGARA = q.CODEGARA(+)
                  --  and i.NUMEAVEN = q.NUMEAVEN(+)
                    and i.NUMEPOLI = p.NUMEPOLI(+)
                    and i.CODEINTE = p.CODEINTE(+)
                    and i.CODERISQ = p.CODERISQ(+)
                    and i.CODEGARA = p.CODEGARA(+)
                    and i.NUMEAVEN = p.NUMEAVEN(+)
                    and i.NUMEPOLI = a.NUMEPOLI(+)
                    and i.CODEINTE = a.CODEINTE(+)
                    and i.CODERISQ = a.CODERISQ(+)
                    and i.CODEGARA = a.CODEGARA(+)
                    and i.NUMEAVEN = a.NUMEAVEN(+)
                    -- and i.NUMEPOLI = 3010000002
                    -- AND i.CODEINTE = 2519
                    -- AND i.CODEGARA = '3INC'
                    -- and i.NUMEAVEN = 13
            )
    );

---------------------------------------------------------------
-------------Client-------------------------------
------------------------------------------------------------------
--------Hub_client------------
delete from REASS_DB.HUB_CLIENT;
insert into REASS_DB.HUB_CLIENT
(
select
     reass_db.MD5(i.CODEASSU||'_'||NOM_CLIENT) ID_CLIENT ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
     src SOURCE,
     CODEASSU

from 

(select   distinct CODEASSU,NOM_CLIENT from  REASS_DB.POLICE)i


)
;

------------Sat_client-------------------------------
--drop table REASS_DB.SAT_CLIENT;
delete from REASS_DB.SAT_CLIENT;
insert into REASS_DB.SAT_CLIENT
--create table REASS_DB.SAT_CLIENT as 
(select
     i.ID_CLIENT ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
   to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
    --- row_number() OVER (ORDER BY NOM_CLIENT ASC) 
    reass_db.MD5(j.CODEASSU||'_'||j.NOM_CLIENT||'_'||j.ACTIVITE||'_'||src)  ID_SAT_CLIENT,
     i.CODEASSU,
     NOM_CLIENT  NOM,
     NOM_CLIENT  PRENOMS,
     --CODECATEG CATEGORIE,
     ACTIVITE    ,
     'ACTIF' STATUT


from 

(select   ID_CLIENT,CODEASSU from  REASS_DB.HUB_CLIENT)i,
(select  distinct NOM_CLIENT,CODEASSU,
--categorie,
max(ACTIVITE) ACTIVITE
 from  REASS_DB.POLICE group by NOM_CLIENT,CODEASSU
 )j

where i.CODEASSU=j.CODEASSU(+)
);

commit;

---Hub intermediaire ------------------------

delete from REASS_DB.HUB_INTERMEDIAIRE;
insert into REASS_DB.HUB_INTERMEDIAIRE
(
select distinct
     reass_db.MD5(i.CODEINTE) ID_INTERMEDIAIRE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
     src SOURCE,
     CODEINTE
from 

(select   distinct CODEINTE from  REASS_DB.POLICE)i


);


----Sat Intermediaire ------------------------
delete from REASS_DB.SAT_INTERMEDIAIRE;
insert into REASS_DB.SAT_INTERMEDIAIRE
--create table REASS_DB.SAT_INTERMEDIAIRE as
(select
    distinct

     i.ID_INTERMEDIAIRE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
    -- row_number() OVER (ORDER BY i.CODEINTE ASC) 
    reass_db.MD5(j.CODEINTE||'_'||j.RAISSOIN||'_'||src)  ID_SAT_INTERMEDIAIRE,
     i.CODEINTE,
     'TYPE_INTER'  TYPE_INTER,
     RAISSOIN  RAISON

from 

(select  distinct ID_INTERMEDIAIRE,CODEINTE from  REASS_DB.HUB_INTERMEDIAIRE)i,
(select  distinct CODEINTE,RAISSOIN
 from  REASS_DB.POLICE)j  ---à modifier

where i.CODEINTE=j.CODEINTE(+)
);

commit;
---Hub Police Avenant ------------------------
--drop table REASS_DB.HUB_POLICE_AVENANT;
delete from REASS_DB.HUB_POLICE_AVENANT;
insert into REASS_DB.HUB_POLICE_AVENANT
--create table REASS_DB.HUB_POLICE_AVENANT as
(
select
distinct
     reass_db.MD5(to_char(i.CODEINTE)||'_'||to_char(i.NUMEPOLI)||'_'||to_char(i.NUMEAVEN)) ID_POLICE_AVENANT ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
     src SOURCE,
     i.CODEINTE,
     i.NUMEPOLI,
     i.NUMEAVEN--,
     --'ACTIF' STATUT 
from 

(select   distinct CODEINTE,NUMEPOLI,NUMEAVEN,DATEEFFE,DATEECHE from  REASS_DB.POLICE)i


);
--rollback;
--select count(*)from REASS_DB.SAT_POLICE_AVENANT;
----Sat Police ------------------------
--drop table REASS_DB.SAT_POLICE_AVENANT;
delete from REASS_DB.SAT_POLICE_AVENANT;
insert into REASS_DB.SAT_POLICE_AVENANT
--create table REASS_DB.SAT_POLICE_AVENANT as
(select           
distinct
     i.ID_POLICE_AVENANT ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
    --- row_number() OVER (ORDER BY i.CODEINTE ASC) 
    reass_db.MD5(j.CODEINTE||'_'||j.NUMEPOLI||'_'||j.NUMEAVEN||'_'||j.CODECATEG||'_'||j.DATEEFFE||'_'||j.DATEECHE||'_'||j.DATECOMP||'_'||src)   ID_SAT_POLICE_AVENANT,
      i.CODEINTE,
     i.NUMEPOLI,
     CODECATEG CODECATE,
     i.NUMEAVEN,
     DATEEFFE,
     DATEECHE,
     DATECOMP,
    -- 0 TAUX_CL,
     1 GARANTIE_IDENTIQUE_PAR_RISQUE,
     'ACTIF' STATUT   ---par defaut les garanties ne sont pas identiques par risque
from 

(select  distinct ID_POLICE_AVENANT,CODEINTE,NUMEPOLI,NUMEAVEN from  REASS_DB.HUB_POLICE_AVENANT)i,
(select  distinct CODEINTE,NUMEPOLI,NUMEAVEN,CODECATEG,DATEEFFE,DATEECHE,DATECOMP
 from  REASS_DB.POLICE)j  ---à modifier

where i.CODEINTE=j.CODEINTE(+) 
   and i.NUMEPOLI=j.NUMEPOLI(+)
   and i.NUMEAVEN=j.NUMEAVEN(+)

);





----link clent inter police--------------------

delete from REASS_DB.LINK_INTER_CLIENT_POLICE;
insert into  REASS_DB.LINK_INTER_CLIENT_POLICE 

(
select
  distinct
  i.ID_POLICE_AVENANT,   
  to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
  to_date('99990101','yyyymmdd')  DATE_FIN ,
  src SOURCE,   
  j.ID_CLIENT  , 
  k.ID_INTERMEDIAIRE ,
  reass_db.MD5(i.ID_POLICE_AVENANT||'_'|| j.ID_CLIENT ||'_'|| k.ID_INTERMEDIAIRE ) ID_LINK_IN_CL_POLICE
 ,            
     'ACTIF' STATUT

from 
( select distinct CODEINTE,NUMEPOLI,NUMEAVEN,CODEASSU from REASS_DB.POLICE) t,

(select distinct  ID_POLICE_AVENANT,CODEINTE,NUMEPOLI,NUMEAVEN from  REASS_DB.HUB_POLICE_AVENANT)i,

(select distinct ID_CLIENT,CODEASSU from  REASS_DB.HUB_CLIENT)j,

(select distinct ID_INTERMEDIAIRE,CODEINTE from  REASS_DB.HUB_INTERMEDIAIRE)k

where t.CODEINTE=i.CODEINTE(+)
and   t.NUMEPOLI=i.NUMEPOLI(+)
and   t.NUMEAVEN=i.NUMEAVEN(+)

and   t.CODEASSU=j.CODEASSU(+)
and   t.CODEINTE=k.CODEINTE(+)
);


--------------------------------------------------------------------------------
-------------Coassurance-------------------------------
--------------------------------------------------------------------------------
--------Hub_Coassurance------------
--drop table REASS_DB.HUB_COASSURANCE;
delete from REASS_DB.HUB_COASSURANCE;
insert into REASS_DB.HUB_COASSURANCE
--create table REASS_DB.HUB_COASSURANCE as
(
select distinct
     reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||NOM_COASR) ID_COASSURANCE,
     CODEINTE,NUMEPOLI,NUMEAVEN,NOM_COASR,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
     'MILLIARD' SOURCE --,
     --CODEASSU
from 

(select   distinct CODEINTE,NUMEPOLI,NUMEAVEN,NOM_COASR from  REASS_DB.COASSURANCES)i


);


------------Sat_Coassurance-------------------------------

--drop table REASS_DB.SAT_COASSURANCE;
delete from REASS_DB.SAT_COASSURANCE;
insert into REASS_DB.SAT_COASSURANCE
--create table REASS_DB.SAT_COASSURANCE as
(select distinct
     i.ID_COASSURANCE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
    to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
    -- row_number() OVER (ORDER BY NUMEPOLI ASC) 
     reass_db.MD5(j.NUMEPOLI||'_'||j.CODEINTE||'_'||j.NUMEAVEN||'_'||j.STATUT||'_'||j.DATEEFFE||'_'||j.DATEECHE||'_'||j.DATECOMP||'_'||j.NOM_COASR||'_'||j.TX_COASS||'_'||src)
     ID_SAT_COASSURANCE,
     NUMEPOLI     ,
     CODEINTE     ,
     NUMEAVEN     ,
     DATEEFFE     ,
     DATEECHE     ,
     DATECOMP     ,
     STATUT STATUT_COASS       ,
     NOM_COASR   NOM_COASSUREUR,
     TX_COASS  TAUX_COASS   ,
     'ACTIF' STATUT

from 

(select   ID_COASSURANCE from  REASS_DB.HUB_COASSURANCE)i,
(select  distinct reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||NOM_COASR) id ,
NUMEPOLI  ,
CODEINTE  ,
NUMEAVEN  ,
DATECOMP  ,
DATEEFFE  ,
DATEECHE  ,
STATUT   ,
NOM_COASR ,
TX_COASS  

--categorie,

 from  REASS_DB.COASSURANCES)j

where i.ID_COASSURANCE=j.id(+)
);


-------------------------------------------------------------------------------------------
-------------------------------Coassureur-------------------------------
---------------------------------------------------------------------------------------

delete from REASS_DB.HUB_COASSUREUR;----UNIQUEMENT EN CAS DE CESSION
insert into REASS_DB.HUB_COASSUREUR
(
select distinct
     reass_db.MD5(NOM_COASR) ID_COASSUREUR,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
     src      SOURCE ,
     i.NOM_COASR CODE_COASS
     --CODEASSU
from 

(select   distinct NOM_COASR from  REASS_DB.COASSURANCES)i


);


--------------------Sat_Coassureur-------------------------------

--drop table REASS_DB.SAT_COASSURANCE;
delete from REASS_DB.SAT_COASSUREUR;
insert into REASS_DB.SAT_COASSUREUR
--create table REASS_DB.SAT_COASSURANCE as
(select distinct
     i.ID_COASSUREUR ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
     --row_number() OVER (ORDER BY ID_COASSUREUR ASC) 
     reass_db.MD5(j.NOM_COASR||'_'||src) ID_SAT_COASSUREUR,
      i.CODE_COASS    ,
      NOM_COASR  NOM_COASSUREUR,    
'ACTIF' STATUT
from 

(select  distinct ID_COASSUREUR,CODE_COASS from  REASS_DB.HUB_COASSUREUR)i,
(select  distinct reass_db.MD5(NOM_COASR) id, NOM_COASR

--categorie,

 from  REASS_DB.COASSURANCES)j

where i.ID_COASSUREUR=j.id(+)
);



------------------------------------------------------------------------
-------------------LINK COASSURANCE_CESSION_COASSUREUR------------------
------------------------------------------------------------------------


delete from REASS_DB.LINK_COASS_CESSION_COASSUREUR;
insert into  REASS_DB.LINK_COASS_CESSION_COASSUREUR 
(
select distinct
 k.ID_COASSUREUR ,   ---uniquement pour les affaires en cession
   i.ID_COASSURANCE ,  
    j.ID_POLICE_AVENANT,
    reass_db.MD5( k.ID_COASSUREUR ||'_'|| i.ID_COASSURANCE ||'_'|| j.ID_POLICE_AVENANT)    ID_LINK_CESSION_COASSUREUR,
   to_date(dte_ins,'ddmmyyyy') DATE_INS ,
    to_date('99990101','yyyymmdd')  DATE_FIN , 
         
   src     SOURCE           
 ,            
     'ACTIF' STATUT

from 

(select  distinct reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||NOM_COASR) 
ID_COASSURANCE ,CODEINTE,NUMEPOLI,NUMEAVEN,NOM_COASR from REASS_DB.COASSURANCES
where STATUT='Apériteur' or (STATUT='Coassureur' and NOM_COASR  not like '%Sanlam%'))i,
--from

-----( select distinct * from REASS_DB.COASSURANCES where STATUT="Apériteur" ) i,

--( select distinct ID_COASSURANCE from REASS_DB.HUB_COASSURANCE) i,

--( select distinct ID_COASSURANCE, CODEINTE, NUMEPOLI, NUMEAVEN,NOM_COASSUREUR ) j
--
--where i.ID_COASSURANCE=j.ID_COASSURANCE(+)
--
--)i,

(select distinct  ID_COASSUREUR,NOM_COASSUREUR from REASS_DB.SAT_COASSUREUR) k,

(select distinct * from  REASS_DB.HUB_POLICE_AVENANT)j


where i.CODEINTE=j.CODEINTE(+)
and   i.NUMEPOLI=j.NUMEPOLI(+)
and   i.NUMEAVEN=j.NUMEAVEN(+)

and   i.NOM_COASR=k.NOM_COASSUREUR(+)

);

------------------------------------------------------------------------
-------------------SAT LINK COASSURANCE_CESSION_COASSUREUR------------------
--------------------------------------------------------------------------

--drop table REASS_DB.SAT_LINK_COASS_CES_COASSUREUR;

delete from REASS_DB.SAT_LINK_COASS_CES_COASSUREUR;
insert into REASS_DB.SAT_LINK_COASS_CES_COASSUREUR
--create table REASS_DB.SAT_LINK_COASS_CES_COASSUREUR as
(select distinct
   i.ID_COASSUREUR                                                 ,
   i.ID_COASSURANCE                                                ,
   i.ID_POLICE_AVENANT                                             ,
   to_date(dte_ins,'ddmmyyyy') DATE_INS                                                      ,
     to_date('99990101','yyyymmdd')  DATE_FIN ,
   src SOURCE                                                        ,
   ---row_number() OVER (ORDER BY CODEINTE ASC) 
   reass_db.MD5(j.NUMEPOLI||'_'||j.CODEINTE||'_'||j.NUMEAVEN||'_'||j.NOM_COASSUREUR||'_'||j.TAUX_COASS||'_'||src)  ID_SAT_LINK_COASS ,
   NOM_COASSUREUR                                                ,
   TAUX_COASS                                                    ,
    'ACTIF' STATUT            

from 

  (select distinct* from  REASS_DB.LINK_COASS_CESSION_COASSUREUR WHERE STATUT='ACTIF') i,
  (select distinct reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||NOM_COASR) ID_COASSURANCE ,CODEINTE,NUMEPOLI,NUMEAVEN,
  NOM_COASR NOM_COASSUREUR,  TX_COASS/100 TAUX_COASS  from  REASS_DB.COASSURANCES where STATUT='Apériteur' or (STATUT='Coassureur' and NOM_COASR  not like '%Sanlam%') ) j

  where i.ID_COASSURANCE= j.ID_COASSURANCE
);


----------------------------------------------------------------------------------------
-------------------LINK_COASSURANCE_ACCEPTATION_COASSUREUR------------------------------
----------------------------------------------------------------------------------------

delete from REASS_DB.LINK_COASS_ACCEPTATION;
insert into  REASS_DB.LINK_COASS_ACCEPTATION 
(
select distinct
--- k.ID_COASSUREUR ,   ---uniquement pour les affaires en cession
   i.ID_COASSURANCE ,  
    j.ID_POLICE_AVENANT,
    reass_db.MD5( i.ID_COASSURANCE ||'_'|| j.ID_POLICE_AVENANT)    ID_LINK_CESSION_COASSUREUR,

   to_date(dte_ins,'ddmmyyyy') DATE_INS ,  
   to_date('99990101','yyyymmdd')  DATE_FIN ,      
   src     SOURCE           
 ,            
     'ACTIF' STATUT

from 

(select  distinct reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||NOM_COASR) ID_COASSURANCE ,CODEINTE,NUMEPOLI,NUMEAVEN

 from  REASS_DB.COASSURANCES where STATUT='Coassureur' and NOM_COASR   like '%Sanlam%')i,

--from

-----( select distinct * from REASS_DB.COASSURANCES where STATUT="Apériteur" ) i,
--
--( select distinct ID_COASSURANCE from REASS_DB.HUB_COASSURANCE) i,
--
--( select distinct ID_COASSURANCE, CODEINTE, NUMEPOLI, NUMEAVEN,NOM_COASSUREUR ) j
--
--where i.ID_COASSURANCE=j.ID_COASSURANCE(+)
--
--)i,

--(select distinct  ID_COASSUREUR,NOM_COASSUREUR from REASS_DB.SAT_COASSUREUR) k,

(select distinct * from  REASS_DB.HUB_POLICE_AVENANT)j


where i.CODEINTE=j.CODEINTE(+)
and   i.NUMEPOLI=j.NUMEPOLI(+)
and   i.NUMEAVEN=j.NUMEAVEN(+)

---and   i.NOM_COASSUREUR=k.NOM_COASSUREUR(+)

);


-------------------------------------------------------------------------------------------
---------------------SAT LINK COASSURANCE_ACCEPTATION_COASSUREUR---------------------------
-------------------------------------------------------------------------------------------

--drop table REASS_DB.SAT_LINK_ACCEPT_COASSUREUR;
delete from REASS_DB.SAT_LINK_COASS_ACCEPTATION;
insert into REASS_DB.SAT_LINK_COASS_ACCEPTATION
--create table REASS_DB.SAT_LINK_ACCEPT_COASSUREUR as
(select distinct
 ---  i.ID_COASSUREUR                                                 ,
   i.ID_COASSURANCE                                                ,
   i.ID_POLICE_AVENANT                                             ,
   to_date(dte_ins,'ddmmyyyy') DATE_INS                                                      ,
     to_date('99990101','yyyymmdd')  DATE_FIN ,
   src SOURCE                                                        ,
  -- row_number() OVER (ORDER BY CODEINTE ASC) 
    reass_db.MD5(j.NUMEPOLI||'_'||j.CODEINTE||'_'||j.NUMEAVEN||'_'||j.NOM_COASR||'_'||j.TX_COASS||'_'||src)  ID_SAT_LINK_COASS ,
   NOM_COASR NOM_COASSUREUR                                                ,
   TX_COASS TAUX_COASS                                                    ,
    'ACTIF' STATUT            


from 

  (select distinct* from  REASS_DB.LINK_COASS_ACCEPTATION) i,

  (select  distinct reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||NOM_COASR) ID_COASSURANCE ,
    NUMEPOLI  ,
    CODEINTE  ,
    NUMEAVEN  ,
    DATECOMP  ,
    DATEEFFE  ,
    DATEECHE  ,
    STATUT   ,
    NOM_COASR ,
    TX_COASS/100  TX_COASS

--categorie,

 from  REASS_DB.COASSURANCES where STATUT='Coassureur' and NOM_COASR   like '%Sanlam%') j

  where i.ID_COASSURANCE= j.ID_COASSURANCE

);


-----------------------------------------------------------------------------------------
-----------------------------------------------------Risque-------------------------------
-------------------------------------------------------------------------------------

------hub risque-----------

--drop table REASS_DB.HUB_RISQUE;
delete from REASS_DB.HUB_RISQUE;
insert into REASS_DB.HUB_RISQUE
--create table REASS_DB.HUB_RISQUE as
(
select

     reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||CODERISQ)
    -- ||'_'||substr(CODE_RISQUE_REF,2,1888) 
     ID_RISQUE,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
     src SOURCE,
     --CODE_RISQUE_REF
     --,
     i.CODEINTE,i.NUMEPOLI,-- i.NUMEAVEN,
     i.CODERISQ
from 

(select   distinct * from  REASS_DB.RISQUE)i


);


----Sat Risque ------------------------
--drop table REASS_DB.SAT_RISQUE;
delete from REASS_DB.SAT_RISQUE;
insert into REASS_DB.SAT_RISQUE
--create table REASS_DB.SAT_RISQUE as
(select   distinct         
     i.ID_RISQUE ,
       to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
    --- row_number() OVER (ORDER BY CODEINTE ASC) 
     reass_db.MD5(j.CODEINTE||'_'||j.NUMEPOLI||'_'||j.CODERISQ||'_'||j.OBSECOMM||'_'||j.LIBERISQ||'_'||j.ADRERISQ||'_'||j.CLASS_RISQUE||'_'||j.LIBECLASS||'_'||src) ID_SAT_RISQUE,
     i.CODEINTE                  ,
     i.NUMEPOLI                  ,
     --NUMEAVEN                  ,
     i.CODERISQ                ,
     --DATEEFFE                  ,
     --DATEECHE                  ,
     OBSECOMM               ,
     ADRERISQ               ,
     LIBERISQ               ,
     CLASS_RISQUE           ,
     LIBECLASS              ,
  --  CODE_RISQUE_REF         ,
 --   PRCT_RED_CPCT ,
    'ACTIF' STATUT            
     --SMP        ,
     --PRM_NT            
from 

(select distinct ID_RISQUE, CODEINTE,NUMEPOLI,-- i.NUMEAVEN,
     CODERISQ from  REASS_DB.HUB_RISQUE)i,
(select  distinct  reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||CODERISQ) ID_RISQUE,
CODEINTE,
NUMEPOLI              ,
--NUMEAVEN              ,
CODERISQ              ,
--DATEEFFE              , 
--DATEECHE              ,
OBSECOMM,
ADRERISQ,
LIBERISQ              ,
CLASS_RISQUE            ,
LIBECLASS             ,
1 PRCT_RED_CPCT ---à modifier
--CAPITAUX              ,
--SMP                   ,
--PRM_NT               

 from  REASS_DB.RISQUE)j  ---à modifier

where i.ID_RISQUE=j.ID_RISQUE(+) 

);




----Sat Risque ------------------------
---drop table REASS_DB.SAT_RISQUE_GARANTIE_REASS;
--delete from REASS_DB.SAT_RISQUE_GARANTIE_REASS;
--insert into REASS_DB.SAT_RISQUE_GARANTIE_REASS
----create table REASS_DB.SAT_RISQUE_GARANTIE_REASS as
--(select           
--     i.ID_RISQUE_GAR_REASS ,
--     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
--     src SOURCE,
--     row_number() OVER (ORDER BY i.CODEINTE ASC) ID_SAT_RISQUE_GAR_REASS,
--     i.CODEINTE                  ,
--     i.NUMEPOLI                  ,
--     i.NUMEAVEN                  ,
--     i.CODERISQ                ,
--     DATEEFFE                  ,
--     DATEECHE                  ,
--     LIBERISQ                  ,
--     CODEGARA,
--     LIBEL_BRAN_RE,
--     CLASS_RISQ             ,
--     LIBECLASS     ,
--     CAPITAUX      ,
--     SMP           ,
--     PRM_NT            
--from 
--
--(select distinct  ID_RISQUE_GAR_REASS, CODEINTE,NUMEPOLI,NUMEAVEN, CODERISQ,LIBEL_BRAN_RE from  REASS_DB.HUB_RISQUE_GARANTIE_REASS)i,
--(select  distinct reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||LIBEL_BRAN_RE) ID_RISQUE_GAR_REASS,
--DATEEFFE    ,
--DATEECHE   ,
--LIBERISQ  ,
--CODEGARA   ,
----LIBEL_BRAN_RE  ,
--CLASS_RISQ   ,
--LIBECLASS     ,
--CAPITAUX    ,
--SMP        ,
--PRM_NT            
--
-- from  REASS_DB.GARANTIES1)j  ---à modifier
--
--where i.ID_RISQUE_GAR_REASS=j.ID_RISQUE_GAR_REASS(+) 
--
--);
--



-------------------------------------------------------------------------------------------
------------------------------Garantie (Reass)-------------------------------
---------------------------------------------------------------------------------------
--------------hub  Garantie -----------

--drop table REASS_DB.HUB_GARANTIE;
delete from REASS_DB.HUB_GARANTIE;
insert into REASS_DB.HUB_GARANTIE
--create table REASS_DB.HUB_GARANTIE as
(
select

     reass_db.MD5(i.CODEINTE||'_'||i.NUMEPOLI||'_'||i.NUMEAVEN||'_'||i.CODERISQ||'_'||i.CODEGARA) ID_GARANTIE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
     src SOURCE,
   -- CODE_GARANTIE_REF
    i.CODEINTE,i.NUMEPOLI, i.NUMEAVEN,
    i.CODERISQ,i.CODEGARA
from 

(select   distinct CODEINTE,NUMEPOLI, NUMEAVEN,
    CODERISQ,CODEGARA from  REASS_DB.GARANTIES1)i


);

commit;

----BKP GARANTIES
--drop  table  REASS_DB.GARANTIES_BKP;
delete from REASS_DB.GARANTIES_BKP;
insert into REASS_DB.GARANTIES_BKP
--create table  REASS_DB.GARANTIES_BKP as
    (select *from REASS_DB.GARANTIES1);



----Sat GARANTIE ------------------------
---drop table REASS_DB.SAT_GARANTIE;
delete from REASS_DB.SAT_GARANTIE;
insert into REASS_DB.SAT_GARANTIE
--create table REASS_DB.SAT_GARANTIE as
(select           
     i.ID_GARANTIE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
   ---  row_number() OVER (ORDER BY i.ID_GARANTIE ASC) 
      reass_db.MD5(j.CODEINTE||'_'||j.NUMEPOLI||'_'||j.NUMEAVEN||'_'||j.CODERISQ||'_'||j.CODEGARA||'_'||j.LIBEL_BRAN_RE||'_'||j.CAPITAUX||'_'||j.SMP||'_'||j.PRM_NT||'_'||src)   ID_SAT_GARANTIE,
   --  i.CODE_GARANTIE_REF ,
     i.CODEINTE      ,
     i.NUMEPOLI        ,
     i.NUMEAVEN      ,
     --   DATECOMP      ,
     --   DATEEFFE      ,
     --   DATEECHE      ,
     --   CODECATE      ,
     --   LIBECATE      ,
     --   CODEBRAN      ,
     --   LIBEBRAN      ,
        i.CODEGARA      ,
         LIBEL_BRAN_RE ,
     --   LIBERISQ      ,
        i.CODERISQ      ,
     --   LIBECLASS     ,
     --- CLASS_RISQ    ,
      -- PRCT_RED_CPCT ,
       CAPITAUX      ,
       SMP           ,
       PRM_NT        ,

     'ACTIF' STATUT    
from 

(select distinct  ID_GARANTIE, CODEINTE,NUMEPOLI, NUMEAVEN,
    CODERISQ,CODEGARA   from  REASS_DB.HUB_GARANTIE)i,
(select  distinct   reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA) ID_GARANTIE,  CODEINTE,NUMEPOLI, NUMEAVEN,
    CODERISQ,CODEGARA ,LIBEL_BRAN_RE,CAPITAUX,SMP,PRM_NT
from  REASS_DB.GARANTIES1)j  ---à modifier

where i.ID_GARANTIE=j.ID_GARANTIE(+) 

);


---------------------------------------------------------------------------------------
-----------------------------REASSUREUR------------------------------------------------
---------------------------------------------------------------------------------------

delete from REASS_DB.HUB_REASSUREUR;
insert into REASS_DB.HUB_REASSUREUR
(
select
     reass_db.MD5(REASSUR) ID_REASSUREUR,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
     src      SOURCE ,
     i.REASSUR CODE_REASSUREUR
     --CODEASSU
from 

(select   distinct REASSUR from  REASS_DB.T_FAC where REASSUR is not null)i


);


--------------------Sat_REASSUREUR-------------------------------

--drop table REASS_DB.SAT_REASSUREUR;
delete from REASS_DB.SAT_REASSUREUR;
insert into REASS_DB.SAT_REASSUREUR
--create table REASS_DB.SAT_REASSUREUR as
(select
     i.ID_REASSUREUR ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
     --row_number() OVER (ORDER BY ID_REASSUREUR ASC) 
      reass_db.MD5(j.REASSUR||'_'||src)    ID_SAT_REASSUREUR,
      i.CODE_REASSUREUR     ,
      REASSUR   LIBELLE_REASSUREUR ,
     'ACTIF' STATUT
from 

(select  distinct ID_REASSUREUR,CODE_REASSUREUR from  REASS_DB.HUB_REASSUREUR)i,
(select  distinct reass_db.MD5(REASSUR) id , REASSUR from  REASS_DB.T_FAC where REASSUR is not null)j
where i.ID_REASSUREUR=j.id(+)
);


delete from REASS_DB.LINK_AVENANT_RISQ_GARANTIE;

insert into  REASS_DB.LINK_AVENANT_RISQ_GARANTIE 
--create table REASS_DB.LINK_AVENANT_RISQ_GARANTIE  as
(
select 
distinct
   i.ID_GARANTIE,     
   j.ID_POLICE_AVENANT,
    reass_db.MD5(   i.ID_GARANTIE ||'_'|| j.ID_POLICE_AVENANT ||'_'|| m.ID_RISQUE )  ID_LINK_AVENANT_RISQ_GARANTIE,
   to_date(dte_ins,'ddmmyyyy') DATE_INS ,  
   to_date('99990101','yyyymmdd')  DATE_FIN ,      
   src     SOURCE,           
   m.ID_RISQUE  
 ,            
'ACTIF' STATUT

from 

(select distinct --CODE_GARANTIE_REF, 
CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA from  REASS_DB.GARANTIES1)t,

(select distinct ID_GARANTIE,CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA
--CODE_GARANTIE_REF 
from  REASS_DB.HUB_GARANTIE)i,

(select distinct  ID_POLICE_AVENANT,CODEINTE,NUMEPOLI,NUMEAVEN from  REASS_DB.HUB_POLICE_AVENANT)j,

(
select 
i.ID_RISQUE,CODEINTE,NUMEPOLI,CODERISQ

from 
(select distinct ID_RISQUE from  REASS_DB.HUB_RISQUE)i,

(select distinct ID_RISQUE,CODEINTE,NUMEPOLI,CODERISQ from  REASS_DB.SAT_RISQUE)k


where i.ID_RISQUE=k.ID_RISQUE(+)
)m


where       t.CODEGARA=i.CODEGARA(+)
     and   t.CODEINTE=i.CODEINTE(+)
     and   t.NUMEPOLI=i.NUMEPOLI(+)
     and   t.NUMEAVEN=i.NUMEAVEN(+)
     and   t.CODERISQ=i.CODERISQ(+)

and   t.CODEINTE=j.CODEINTE(+)
and   t.NUMEPOLI=j.NUMEPOLI(+)
and   t.NUMEAVEN=j.NUMEAVEN(+)

and   t.CODEINTE=m.CODEINTE(+)
and   t.NUMEPOLI=m.NUMEPOLI(+)
--and   t.NUMEAVEN=m.NUMEAVEN(+)
and   t.CODERISQ=m.CODERISQ(+)
);


commit;

-----Update de GARANTIES F REPLIQUE DE SAT_GARANTIE old ----

--drop table REASS_DB.GARANTIES1_F;
--delete from REASS_DB.GARANTIES1_F;
--delete from REASS_DB.GARANTIES1_F;
--insert into REASS_DB.GARANTIES1_F
----create table  REASS_DB.GARANTIES1_F as 
--(
--select distinct
--    i.ID_GARANTIE ,
--    ---trunc(SYSDATE)  DATE_INS ,
--    --src SOURCE,
--   --- row_number() OVER (ORDER 
--    i.CODE_GARANTIE_REF ,
--         NUMEPOLI        ,
--         CODEINTE      ,
--         NUMEAVEN      ,
--       --  DATECOMP      ,
--         DATEEFFE      ,
--         DATEECHE      ,
--         CODECATE      ,
--         LIBECATE      ,
--         CODEBRAN      ,
--         LIBEBRAN      ,
--         j.CODEGARA      ,
--        j.LIBEL_BRAN_RE ,
--         LIBERISQ      ,
--        CODERISQ      ,
--        LIBECLASS     ,
--        CLASS_RISQ    ,
--     -- PRCT_RED_CPCT ,
--      j.CAPITAUX      ,
--      j.SMP           ,
--      j.PRM_NT        
--
--from 
--(select distinct ID_GARANTIE,CODE_GARANTIE_REF from REASS_DB.HUB_GARANTIE) i,
--(select distinct ID_GARANTIE, CODEGARA,  LIBEL_BRAN_RE, CAPITAUX ,SMP, PRM_NT,CODE_GARANTIE_REF 
--from  reass_db.SAT_GARANTIE where STATUT='ACTIF')j,
--
--(
--select 
--distinct j.ID_GARANTIE,j.ID_POLICE_AVENANT,J.ID_RISQUE,j.CODEINTE,j.NUMEPOLI,j.NUMEAVEN ,
--j.LIBERISQ,j.CODERISQ ,j.LIBECLASS, j.CLASS_RISQ, 
----t.DATECOMP ,
--t.DATEEFFE,t.DATEECHE,t.CODECATE,t.LIBECATE,t.CODEBRAN,t.LIBEBRAN
--
--from
--(
--select distinct j.ID_GARANTIE,j.ID_POLICE_AVENANT,J.ID_RISQUE, CODEINTE,NUMEPOLI, NUMEAVEN,
--LIBERISQ,CODERISQ ,LIBECLASS, CLASS_RISQUE CLASS_RISQ
-- from
--(select distinct ID_GARANTIE,ID_POLICE_AVENANT,ID_RISQUE from reass_db.LINK_AVENANT_RISQ_GARANTIE where STATUT='ACTIF')j,
--(select distinct ID_POLICE_AVENANT, CODEINTE,NUMEPOLI, NUMEAVEN from  reass_db.SAT_POLICE_AVENANT where STATUT='ACTIF')k,
--(select distinct ID_RISQUE, LIBERISQ,CODERISQ ,LIBECLASS, CLASS_RISQUE from  reass_db.SAT_RISQUE where STATUT='ACTIF')t
--
--where j.ID_POLICE_AVENANT=k.ID_POLICE_AVENANT(+)  and j.ID_RISQUE=t.ID_RISQUE(+)
--)j,
--
--(select distinct NUMEPOLI,CODEINTE,NUMEAVEN --,DATECOMP 
--,DATEEFFE,DATEECHE,CODECATE,LIBECATE,CODEBRAN,LIBEBRAN 
--
-- from REASS_DB.GARANTIES1_BKP --where STATUT='ACTIF'
-- )t
--
-- where j.CODEINTE=t.CODEINTE(+)
--  and j.NUMEPOLI=t.NUMEPOLI(+)
--  and j.NUMEAVEN=t.NUMEAVEN(+)
--
--)h
-- where i.ID_GARANTIE=j.ID_GARANTIE(+) 
-- and i.ID_GARANTIE=h.ID_GARANTIE(+) 
--
--)     
--;



------------------------------------------------
---------------------HUB_BRANCHE_CIMA--------------------
--------------------------------------------------

delete from REASS_DB.HUB_BRANCHE_CIMA;
insert into REASS_DB.HUB_BRANCHE_CIMA
(
select
     reass_db.MD5(CODEBRAN) ID_BRANCHE,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
     src      SOURCE ,
     i.CODEBRAN
     --CODEASSU
from 

(select   distinct CODEBRAN from  REASS_DB.BRANCHE_CIMA)i

);

-------------------Sat_BRANCHE_CIMA-------------------------------

--drop table REASS_DB.SAT_BRANCHE_CIMA;
delete from REASS_DB.SAT_BRANCHE_CIMA;
insert into REASS_DB.SAT_BRANCHE_CIMA
--create table REASS_DB.SAT_BRANCHE_CIMA as
(select
     i.ID_BRANCHE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
     --row_number() OVER (ORDER BY ID_BRANCHE ASC) 
    reass_db.MD5(j.CODEBRAN||'_'||j.LIBSYS||'_'||src)  ID_SAT_BRANCHE,
     i.CODEBRAN     ,
     LIBSYS LIBEBRAN 

from 

(select  distinct ID_BRANCHE,CODEBRAN from  REASS_DB.HUB_BRANCHE_CIMA)i,
(select  distinct * from  REASS_DB.BRANCHE_CIMA)j
where i.CODEBRAN=j.CODEBRAN(+)
);



delete from REASS_DB.HUB_REASSURANCE;
insert into REASS_DB.HUB_REASSURANCE
(select
     reass_db.MD5(i.CODETRAITE) ID_REASSURANCE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
     src SOURCE,
     CODETRAITE CODEREASS

from 

(select   distinct CODETRAITE from  REASS_DB.T_FAC
union all
select distinct CODETRAITE from REASS_DB.T_EDP
union all
select distinct CODETRAITE from REASS_DB.T_XS
union all
select distinct CODETRAITE from REASS_DB.T_CL) i
);


------------Sat_reassurance-------------------------------
--drop table REASS_DB.SAT_REASSURANCE;
delete from REASS_DB.SAT_REASSURANCE;
insert into REASS_DB.SAT_REASSURANCE
--create table REASS_DB.SAT_REASSURANCE as 
(select
     i.ID_REASSURANCE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
     --row_number() OVER (ORDER BY ID_REASSURANCE ASC) 
       reass_db.MD5(j.CODEREASS||'_'||j.LIBEREASS||'_'||src)  ID_SAT_REASSURANCE,
     i.CODEREASS,
     j.LIBEREASS,
   case when  substr(to_char(i.CODEREASS),1,3) = 'FAC'   then 'FAC' else 'TRAITE' end  MODEREASS,
   case when  substr(to_char(i.CODEREASS),1,2) in ('FA','CL')   then 'QS' 
			when substr(to_char(i.CODEREASS),1,3) = 'EDP' then 'EDP'
			when substr(to_char(i.CODEREASS),1,2) = 'XS' then 'XS' else 'XL' end  TYPEREASS,
     'ACTIF' STATUT  


from 

(select   ID_REASSURANCE,CODEREASS from  REASS_DB.HUB_REASSURANCE)i,
(select
distinct
     ---trunc(SYSDATE)  DATE_INS ,
     --src SOURCE,
     CODETRAITE CODEREASS,
     LIBETRAITE LIBEREASS

from 

(select   distinct CODETRAITE, LIBETRAITE from  REASS_DB.T_FAC
union all
select distinct CODETRAITE, LIBETRAITE from REASS_DB.T_EDP
union all
select distinct CODETRAITE, LIBETRAITE from REASS_DB.T_XS
union all
select distinct CODETRAITE, LIBETRAITE from REASS_DB.T_CL))j

where i.CODEREASS=j.CODEREASS(+));

------------Sat_traite_reassurance-------------------------------

delete from REASS_DB.SAT_TRAITE_REASSURANCE;
insert into REASS_DB.SAT_TRAITE_REASSURANCE
(
select
     i.ID_REASSURANCE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
    -- row_number() OVER (ORDER BY ID_REASSURANCE ASC) 
      reass_db.MD5(j.CODETRAITE||'_'||j.LIBETRAITE||'_'||j.DATEEFFE||'_'||j.DATEECHE||'_'||src) ID_SAT_TRAITE_REASSURANCE,
     CODETRAITE,
     LIBETRAITE,
     DATEEFFE,
     DATEECHE,
     'ACTIF' STATUT

from 

(select   ID_REASSURANCE,CODEREASS from  REASS_DB.HUB_REASSURANCE)i,

(select distinct CODETRAITE,LIBETRAITE, DATEEFFE, DATEECHE from REASS_DB.T_EDP
union all
select distinct CODETRAITE,LIBETRAITE, DATEEFFE, DATEECHE from REASS_DB.T_XS
union all
select distinct CODETRAITE,LIBETRAITE, DATEEFFE, DATEECHE from REASS_DB.T_CL) j

where i.CODEREASS=j.CODETRAITE
)
;




------------Sat_EDP-------------------------------
delete from REASS_DB.SAT_TRAITE_EDP;
insert into REASS_DB.SAT_TRAITE_EDP
(
select
     i.ID_REASSURANCE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
    --row_number() OVER (ORDER BY ID_REASSURANCE ASC) 
      reass_db.MD5(j.LIBETRAITE||'_'||j.PLEIN_CONSERVATION||'_'||j.NB_PLEIN||'_'||j.ENGAGEMENT_REASSUREUR||'_'||j.CAPACITE_TRAITE||'_'||src)  ID_SAT_TRAITE_EDP,
     LIBETRAITE             ,
     PLEIN_CONSERVATION     ,
     NB_PLEIN               ,
     ENGAGEMENT_REASSUREUR  ,
     CAPACITE_TRAITE        ,            
     'ACTIF' STATUT

from 

(select   ID_REASSURANCE,CODEREASS from  REASS_DB.HUB_REASSURANCE where substr(to_char(CODEREASS),1,3) = 'EDP' )i,

(select distinct *from REASS_DB.T_EDP) j

where i.CODEREASS=j.CODETRAITE(+)
)
;
-------------------LINK_REASSURANCE_REASSUREUR--------------------
-------------------------------------------------------------------- --
--drop  table REASS_DB.LINK_REASSURANCE_REASSUREUR;
delete from REASS_DB.LINK_REASSURANCE_REASSUREUR;
insert into  REASS_DB.LINK_REASSURANCE_REASSUREUR 
--create table REASS_DB.LINK_REASSURANCE_REASSUREUR as
(
select
  i.ID_REASSURANCE,
  k.ID_REASSUREUR,
   reass_db.MD5(   i.ID_REASSURANCE ||'_'||  k.ID_REASSUREUR ) ID_LINK_REASSURANCE_REASSUREUR,
  i.CODEREASS,
  i.REASSUR,
  to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
   to_date('99990101','yyyymmdd')  DATE_FIN ,
  src SOURCE
 ,            
     'ACTIF' STATUT

from 

(
select

distinct ID_REASSURANCE,CODEREASS,REASSUR from 
(select   ID_REASSURANCE,CODEREASS from  REASS_DB.HUB_REASSURANCE where substr(to_char(CODEREASS),1,3) = 'FAC' )i,

(select  distinct CODETRAITE,REASSUR from  REASS_DB.T_FAC where REASSUR is not null)j

where i.CODEREASS=j.CODETRAITE(+)
)i,

(select 
i.ID_REASSUREUR,i.CODE_REASSUREUR,j.LIBELLE_REASSUREUR
from
(select  distinct ID_REASSUREUR,CODE_REASSUREUR from  REASS_DB.HUB_REASSUREUR )i,
(select   distinct * from  REASS_DB.SAT_REASSUREUR where STATUT='ACTIF')j
where i.ID_REASSUREUR=j.ID_REASSUREUR(+)
) k

where i.REASSUR=k.LIBELLE_REASSUREUR(+)
);

----------------------------------
------Sat link reassurance reassureur--------------------
---------------------------------------
delete from REASS_DB.SAT_LINK_REASS_REASSUREUR;
insert into  REASS_DB.SAT_LINK_REASS_REASSUREUR 

(
select

  i.ID_REASSURANCE,
  i.ID_REASSUREUR,
  to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
    to_date('99990101','yyyymmdd')  DATE_FIN ,
  src SOURCE,   

  --row_number() OVER (ORDER BY ID_REASSURANCE ASC) 
    reass_db.MD5(j.CODETRAITE||'_'||j.REASSUR||'_'||j.TX_COM||'_'||j.TX_FAC||'_'||src) ID_SAT_LINK_REASS_REASSUREUR,
 i.CODEREASS ,
  -- LIBEREASS,
  j.TX_COM TAUX_COM_REASSURANCE, 
  j.TX_FAC PART_REASSUREUR,
  'ACTIF' STATUT

from 
  (select *from REASS_DB.LINK_REASSURANCE_REASSUREUR) i,
  (select  distinct CODETRAITE,REASSUR, max(TX_COM) TX_COM, max(TX_FAC) TX_FAC
from REASS_DB.T_FAC group by CODETRAITE,REASSUR )j

  where i.CODEREASS=j.CODETRAITE(+) and i.REASSUR=j.REASSUR(+)


);


 ------------Sat_QS-------------------------------

delete from REASS_DB.SAT_QUOTASHARE;
insert into REASS_DB.SAT_QUOTASHARE
(
select
     ID_REASSURANCE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
    --- row_number() OVER (ORDER BY ID_REASSURANCE ASC) 
  reass_db.MD5(CODEREASS||'_'||LIBEREASS||'_'||TAUX_FAC||'_'||TAUX_COM||'_'||MODEREASS||'_'||TAUX_CESSION||'_'||src)
     ID_SAT_QUOTASHARE,
     CODEREASS,
     LIBEREASS,
     case when  MODEREASS='FAC' then TAUX_FAC 
          else  TAUX_CESSION end TAUX_CESSION,
     case when  MODEREASS='FAC' then TAUX_COM 
          else  0 end TAUX_COM ,     
     'ACTIF' STATUT

from 

(
select

    distinct i.ID_REASSURANCE,i.CODEREASS,y.LIBEREASS,TAUX_FAC,TAUX_COM, y.MODEREASS,j.TAUX_CESSION
from
(select  distinct  ID_REASSURANCE,CODEREASS from  REASS_DB.HUB_REASSURANCE where substr(to_char(CODEREASS),1,2) in ('FA','CL') )i,

(select*from REASS_DB.SAT_REASSURANCE where STATUT='ACTIF') y,
(select distinct* from REASS_DB.T_CL) j,

(select ID_REASSURANCE, sum(PART_REASSUREUR) TAUX_FAC ,
        sum(TAUX_COM_REASSURANCE*PART_REASSUREUR)  TAUX_COM
  from REASS_DB.SAT_LINK_REASS_REASSUREUR where STATUT='ACTIF'
  group by  ID_REASSURANCE) k

where i.CODEREASS=j.CODETRAITE(+)
      and i.ID_REASSURANCE=y.ID_REASSURANCE(+)
      and i.ID_REASSURANCE=k.ID_REASSURANCE(+)
)
);







------------Sat_XS-------------------------------
delete from REASS_DB.SAT_TRAITE_XS;
insert into REASS_DB.SAT_TRAITE_XS
(
select
     i.ID_REASSURANCE ,
     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
     src SOURCE,
    -- row_number() OVER (ORDER BY ID_REASSURANCE ASC) 
    reass_db.MD5(j.CODETRAITE||'_'||j.LIBETRAITE||'_'||j.PRIORITE||'_'||j.PORTEE||'_'||j.PLN_SCRPT||'_'||src) ID_SAT_TRAITE_XS,
     LIBETRAITE,
     PRIORITE  ,
     PORTEE    ,
     PLN_SCRPT ,         
     'ACTIF' STATUT

from 

(select   ID_REASSURANCE,CODEREASS from  REASS_DB.HUB_REASSURANCE where substr(to_char(CODEREASS),1,2) = 'XS' )i,

(select distinct *from REASS_DB.T_XS) j

where i.CODEREASS=j.CODETRAITE(+)
)
;
------------Sat_XL-------------------------------
--delete from REASS_DB.SAT_TRAITE_XL;
--insert into REASS_DB.SAT_TRAITE_XL
--(
--select
--     i.ID_REASSURANCE ,
--     to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
--     src SOURCE,
--     row_number() OVER (ORDER BY ID_REASSURANCE ASC) ID_SAT_TRAITE_XL,
--     --PRIORITE  ,
--     --PORTEE    ,
--     --PLN_SCRPT ,         
--     'ACTIF' STATUT
--      
--from 
--
--(select   ID_REASSURANCE,CODEREASS from  REASS_DB.HUB_REASSURANCE where substr(to_char(CODEREASS),1,2) = 'XL' )i,
--
--(select distinct *from REASS_DB.T_XL) j
--
--where i.CODEREASS=j.CODETRAITE(+)
--)
--;

-------------------------------------------------
---------------------LINK_REASSURANCE_GARANTIE --------------------
--------------------------------------------------

--drop table REASS_DB.LINK_REASSURANCE_GARANTIE;
delete from REASS_DB.LINK_REASSURANCE_GARANTIE;
insert into  REASS_DB.LINK_REASSURANCE_GARANTIE 
--create table REASS_DB.LINK_REASSURANCE_GARANTIE  as
(
select  distinct           
   ID_GARANTIE,     
   ID_REASSURANCE,
   ID_LINK_REASSURANCE_GARANTIE,
   DATE_INS ,
   DATE_FIN ,        
    SOURCE--,           
   --m.ID_RISQUE  
    ,            
    STATUT

from 


(
select distinct i.ID_GARANTIE,     
       j.ID_REASSURANCE, 
        reass_db.MD5( i.ID_GARANTIE ||'_'|| j.ID_REASSURANCE ) ID_LINK_REASSURANCE_GARANTIE,
       to_date(dte_ins,'ddmmyyyy') DATE_INS ,
       to_date('99990101','yyyymmdd')  DATE_FIN , 
       src     SOURCE,

     'ACTIF' STATUT
        from
(
select 
distinct 
i.ID_GARANTIE,
---i.CODE_GARANTIE_REF,
 i.CODEINTE,  i.NUMEPOLI,  i.NUMEAVEN,  i.CODERISQ,
i.CODEGARA,
i.ID_POLICE_AVENANT

from

(
  select distinct t.ID_GARANTIE,
  --t.CODE_GARANTIE_REF,
  t.CODEINTE,  t.NUMEPOLI,  t.NUMEAVEN,  t.CODERISQ,
  t.CODEGARA, j.ID_POLICE_AVENANT from

(select distinct * from  REASS_DB.HUB_GARANTIE)t,

--(select distinct  * from  REASS_DB.SAT_GARANTIE where STATUT='ACTIF' and CODEGARA is not null)k,

(select distinct  * from  REASS_DB.LINK_AVENANT_RISQ_GARANTIE)j

where   ----t.ID_GARANTIE=k.ID_GARANTIE (+) and
t.ID_GARANTIE=J.ID_GARANTIE(+) )i ,

(select distinct  * from REASS_DB.SAT_POLICE_AVENANT )p

where  i.ID_POLICE_AVENANT=p.ID_POLICE_AVENANT(+) 

)i,



(select distinct k.ID_REASSURANCE,k.LIBEREASS,k.CODEREASS, j.CODEGARA
 from

(select l.ID_REASSURANCE,m.LIBEREASS,l.CODEREASS from
(select distinct * from  REASS_DB.HUB_REASSURANCE)l,
(select distinct * from  REASS_DB.SAT_REASSURANCE where STATUT='ACTIF')m

where l.ID_REASSURANCE=m.ID_REASSURANCE(+) ) k,

(select distinct * from  REASS_DB.PARAM_GARANTIE_CATALOGUE where STATUT=1)j

WHERE k.LIBEREASS=j.LIBEREASS(+) 
) j

where i.CODEGARA=j.CODEGARA(+)
)

union all

(
select distinct i.ID_GARANTIE,     
       K.ID_REASSURANCE ,
         reass_db.MD5( i.ID_GARANTIE ||'_'|| k.ID_REASSURANCE ) ID_LINK_REASSURANCE_GARANTIE,
       to_date(dte_ins,'ddmmyyyy') DATE_INS ,
        to_date('99990101','yyyymmdd')  DATE_FIN , 
       src     SOURCE 
        ,            
     'ACTIF' STATUT
from

(select 
distinct 
ID_GARANTIE,--i.CODE_GARANTIE_REF,
CODEGARA,
CODEINTE,NUMEPOLI,CODERISQ,NUMEAVEN

from

--(
--  select distinct t.ID_GARANTIE,
  --t.CODE_GARANTIE_REF,
--  t.CODEGARA, t.NUMEPOLI,
--  t.CODEINTE,t.NUMEAVEN,t.CODERISQ from


(
select distinct ID_GARANTIE,
--CODE_GARANTIE_REF
 CODEGARA, NUMEPOLI,
CODEINTE,NUMEAVEN,CODERISQ 
from  REASS_DB.HUB_GARANTIE
)
--t
--,

--(select distinct  * from  REASS_DB.SAT_GARANTIE where STATUT='ACTIF' and CODEGARA is not null)k,

--(
--select distinct j.ID_GARANTIE,j.ID_POLICE_AVENANT,J.ID_RISQUE, CODEINTE,NUMEPOLI, NUMEAVEN,
--LIBERISQ,CODERISQ ,LIBECLASS, CLASS_RISQUE CLASS_RISQ
-- from
--(select distinct ID_GARANTIE,ID_POLICE_AVENANT,ID_RISQUE from REASS_DB.LINK_AVENANT_RISQ_GARANTIE where STATUT='ACTIF')j,
--(select distinct ID_POLICE_AVENANT, CODEINTE,NUMEPOLI, NUMEAVEN from REASS_DB.SAT_POLICE_AVENANT where STATUT='ACTIF')k,
--(select distinct ID_RISQUE, LIBERISQ,CODERISQ ,LIBECLASS, CLASS_RISQUE from REASS_DB.SAT_RISQUE where STATUT='ACTIF')t

--
--where j.ID_POLICE_AVENANT=k.ID_POLICE_AVENANT(+)  and j.ID_RISQUE=t.ID_RISQUE(+)
--)j

--where --t.ID_GARANTIE=k.ID_GARANTIE(+) and 
--t.ID_GARANTIE=j.ID_GARANTIE(+)

--)i

)i,

(select i.ID_REASSURANCE, i.CODEREASS, j.CODEINTE, j.NUMEPOLI,j.CODERISQ, j.NUMEAVEN,j.CODEGARA from
( select distinct ID_REASSURANCE, CODEREASS from REASS_DB.HUB_REASSURANCE  where substr(to_char(CODEREASS),1,3) = 'FAC')i,
(select  distinct CODEINTE,NUMEPOLI,CODERISQ,CODEGARA,NUMEAVEN, CODETRAITE from REASS_DB.T_FAC where REASSUR is not null) j

where i.CODEREASS=j.CODETRAITE(+)

)k

where     i.CODEINTE=k.CODEINTE
      and i.NUMEPOLI=k.NUMEPOLI
      and i.NUMEAVEN=k.NUMEAVEN
      and i.CODEGARA=k.CODEGARA
      and i.CODERISQ=k.CODERISQ
)
)

;



--drop table REASS_DB.SAT_LINK_REASS_GARA_CL;

delete from REASS_DB.SAT_LINK_REASS_GARA_CL;
  insert into  REASS_DB.SAT_LINK_REASS_GARA_CL 
--create table REASS_DB.SAT_LINK_REASS_GARA_CL  as
(
select

      distinct
      ID_GARANTIE,
      ID_REASSURANCE ,
      to_date(dte_ins,'ddmmyyyy')  DATE_INS ,
        to_date('99990101','yyyymmdd')  DATE_FIN ,
      src SOURCE,
     -- row_number() OVER (ORDER BY ID_GARANTIE ASC) 
      reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CAPITAUX||'_'||SMP||'_'||PRM_NT||'_'||TAUX_CESSION||'_'||ANNEECOMP||'_'||ANNEEFFE||'_'||ANNEEFFE_CL||'_'||ANNEECHE_CL||'_'||src)  ID_SAT_LINK_REASS_GARA_CL,
      CODEINTE,
      NUMEPOLI,
      NUMEAVEN,
      TAUX_CESSION ,
      'ACTIF' STATUT


      from
      (
      select distinct * from

      (select 
      distinct
        i.ID_GARANTIE,
        i.ID_REASSURANCE ,
        i.ID_POLICE_AVENANT,
        i.CODEINTE,
        i.NUMEPOLI,
        i.NUMEAVEN,
        j.CAPITAUX  ,
        j.SMP      ,
        j.PRM_NT   ,
        k.TAUX_CESSION ,
       -- t.TAUX_COASS  ,
        i.ANNEECOMP,
        i.ANNEEFFE,
        k.ANNEEFFE ANNEEFFE_CL,
        k.ANNEECHE ANNEECHE_CL

      from
      (select 
      i.ID_GARANTIE, i.ID_REASSURANCE ,i.ID_POLICE_AVENANT,j.CODEINTE,j.NUMEPOLI,j.NUMEAVEN,
      substr(j.DATEEFFE,1,4)  ANNEEFFE,substr(j.DATECOMP,1,4) ANNEECOMP

      from
      (
      select
      distinct i.ID_GARANTIE, i.ID_REASSURANCE ,g.ID_POLICE_AVENANT
      from

      (select distinct ID_GARANTIE, ID_REASSURANCE from REASS_DB.LINK_REASSURANCE_GARANTIE
       where   ID_REASSURANCE in (select distinct ID_REASSURANCE from REASS_DB.HUB_REASSURANCE where substr(to_char(CODEREASS),1,2) = 'CL'
     and STATUT='ACTIF')
      )i,
      (select DISTINCT *from REASS_DB.LINK_AVENANT_RISQ_GARANTIE)g

      where I.ID_GARANTIE=G.ID_GARANTIE(+)
      )i,

    (  
    select distinct*from  REASS_DB.SAT_POLICE_AVENANT where STATUT='ACTIF'
     )j

     where i.ID_POLICE_AVENANT=j.ID_POLICE_AVENANT(+))i, 

      (select distinct*from REASS_DB.SAT_GARANTIE where STATUT='ACTIF')j,
   --   (select distinct* from REASS_DB.SAT_COASSURANCE where STATUT='ACTIF')t,

      (
     select i.ID_REASSURANCE,i.CODEREASS,  to_char(j.DATEEFFE,'YYYY') ANNEEFFE, 
     to_char(j.DATEECHE,'YYYY') ANNEECHE ,i.TAUX_CESSION,i.TAUX_COM
       from
      (select distinct* from REASS_DB.SAT_QUOTASHARE where STATUT='ACTIF')i,
      (select distinct*from REASS_DB.SAT_TRAITE_REASSURANCE) j
      where i.ID_REASSURANCE=j.ID_REASSURANCE
      )k


      where i.ID_GARANTIE=j.ID_GARANTIE(+)
      and   i.ID_REASSURANCE=k.ID_REASSURANCE(+) 
  --    and   i.CODEINTE=t.CODEINTE(+)
    --  and   i.NUMEPOLI=t.NUMEPOLI(+)
   --   and   i.NUMEAVEN=t.NUMEAVEN(+)

      )

     where ANNEEFFE>=ANNEEFFE_CL and ANNEECHE_CL>=ANNEEFFE
      )

)
;
commit;
      --drop table REASS_DB.INPUT_CL;
      delete from REASS_DB.INPUT_CL;  
     insert into REASS_DB.INPUT_CL
     --create table REASS_DB.INPUT_CL as
     ( select

      distinct
      ID_GARANTIE,
      ID_REASSURANCE ,
      ID_POLICE_AVENANT,
      CODEINTE,
      NUMEPOLI,
      NUMEAVEN,
      CAPITAUX  ,
      SMP      ,
      PRM_NT   ,
      TAUX_CESSION ,
      case when TAUX_COASS  is null then 1 else  TAUX_COASS end TAUX_COASS,
      case when STATUT_COASS  is null then 'Apériteur' else  STATUT_COASS end STATUT_COASS

      from



      (select 
      distinct
        i.ID_GARANTIE,
        i.ID_REASSURANCE ,
        i.ID_POLICE_AVENANT,
        i.CODEINTE,
        i.NUMEPOLI,
        i.NUMEAVEN,
        j.CAPITAUX  ,
        j.SMP      ,
        j.PRM_NT   ,
        k.TAUX_CESSION ,
        t.STATUT_COASS,
        t.TAUX_COASS  ,
        i.ANNEECOMP,
        i.ANNEEFFE
        --,
        --k.ANNEEFFE ANNEEFFE_CL,
      --  k.ANNEECHE ANNEECHE_CL

      from
      (select 
      i.ID_GARANTIE, i.ID_REASSURANCE ,i.ID_POLICE_AVENANT,j.CODEINTE,j.NUMEPOLI,j.NUMEAVEN,
      substr(j.DATEEFFE,1,4)  ANNEEFFE,substr(j.DATECOMP,1,4) ANNEECOMP

      from
      (
      select
      distinct i.ID_GARANTIE, i.ID_REASSURANCE ,g.ID_POLICE_AVENANT
      from

      (select distinct ID_GARANTIE, ID_REASSURANCE from REASS_DB.LINK_REASSURANCE_GARANTIE
       where   ID_REASSURANCE in (select distinct ID_REASSURANCE from REASS_DB.HUB_REASSURANCE where substr(to_char(CODEREASS),1,2) = 'CL'
       and  STATUT='ACTIF')
      )i,
      (select distinct * from REASS_DB.LINK_AVENANT_RISQ_GARANTIE where STATUT='ACTIF')g

      where I.ID_GARANTIE=G.ID_GARANTIE(+)
      )i,

    (  
    select *from  REASS_DB.SAT_POLICE_AVENANT where STATUT='ACTIF'
     )j

     where i.ID_POLICE_AVENANT=j.ID_POLICE_AVENANT(+))i, 

      (select *from REASS_DB.SAT_GARANTIE where STATUT='ACTIF')j,

      (
         select 
         distinct
      --  ID_COASSURANCE,
        ID_POLICE_AVENANT,
        STATUT_COASS,
        TAUX_COASS

        from
    (select distinct ID_POLICE_AVENANT,'Coassureur' STATUT_COASS,TAUX_COASS from REASS_DB.SAT_LINK_COASS_ACCEPTATION    where STATUT='ACTIF') 
    union all
    (select distinct ID_POLICE_AVENANT,'Apériteur' STATUT_COASS,sum(TAUX_COASS) TAUX_COASS from REASS_DB.SAT_LINK_COASS_CES_COASSUREUR where STATUT='ACTIF' and NOM_COASSUREUR   in  ('Sanlam Assurance')
    group by ID_POLICE_AVENANT) 

   )t,

    --   (select * from REASS_DB.SAT_COASSURANCE where STATUT='ACTIF')t,
      (
      --select distinct i.ID_GARANTIE, TAUX_CESSION,DATEEFFE,DATEECHE from
     -- (
      select distinct ID_GARANTIE,ID_REASSURANCE, TAUX_CESSION from REASS_DB.SAT_LINK_REASS_GARA_CL where STATUT='ACTIF'
      --)i,
     -- (select distinct ID_GARANTIE, DATEEFFE,DATEECHE FROM GARANTIES_F)j
     -- where i.ID_GARANTIE=j.ID_GARANTIE(+)
      )k


      where i.ID_GARANTIE=j.ID_GARANTIE(+)
      and   i.ID_GARANTIE=k.ID_GARANTIE  and i.ID_REASSURANCE=k.ID_REASSURANCE 
      and   i.ID_POLICE_AVENANT=t.ID_POLICE_AVENANT(+)
    --  and   i.NUMEPOLI=t.NUMEPOLI(+)
    --  and   i.NUMEAVEN=t.NUMEAVEN(+)

      )
      )
     --where ANNEEFFE>=ANNEEFFE_CL and ANNEECHE_CL>=ANNEEFFE

      ;

     delete from REASS_DB.INPUT_FAC;

      insert into REASS_DB.INPUT_FAC
    ( 
      select distinct
        i.ID_GARANTIE,
        i.ID_REASSURANCE ,
        --i.ID_POLICE_AVENANT,
        f.CODEINTE,
        f.NUMEPOLI,
        f.NUMEAVEN,
        j.CAPITAUX  ,
        j.SMP      ,
        j.PRM_NT   ,
        k.TAUX_CESSION  TAUX_FAC ,
        k.TAUX_COM  TAUX_COM --,
       -- t.TAUX_COASS  

      from

      (select distinct ID_GARANTIE, ID_REASSURANCE from REASS_DB.LINK_REASSURANCE_GARANTIE
      where ID_REASSURANCE in 
      (select distinct ID_REASSURANCE from REASS_DB.HUB_REASSURANCE where substr(to_char(CODEREASS),1,3) = 'FAC' )
      and STATUT='ACTIF')i,
      (
select distinct j.ID_GARANTIE,j.ID_POLICE_AVENANT,J.ID_RISQUE, k.CODEINTE,k.NUMEPOLI, k.NUMEAVEN,
t.LIBERISQ,t.CODERISQ ,t.LIBECLASS, t.CLASS_RISQUE CLASS_RISQ
from 
(select distinct ID_GARANTIE,ID_POLICE_AVENANT,ID_RISQUE from REASS_DB.LINK_AVENANT_RISQ_GARANTIE where STATUT='ACTIF')j,
(select distinct ID_POLICE_AVENANT, CODEINTE,NUMEPOLI, NUMEAVEN from REASS_DB.SAT_POLICE_AVENANT where STATUT='ACTIF')k,
(select distinct ID_RISQUE, LIBERISQ,CODERISQ ,LIBECLASS, CLASS_RISQUE from REASS_DB.SAT_RISQUE where STATUT='ACTIF')t

where j.ID_POLICE_AVENANT=k.ID_POLICE_AVENANT(+)  and j.ID_RISQUE=t.ID_RISQUE(+)

)f,

      (select *from REASS_DB.SAT_GARANTIE where STATUT='ACTIF')j,

      (select ID_REASSURANCE, sum(PART_REASSUREUR) TAUX_CESSION ,
        sum(TAUX_COM_REASSURANCE*PART_REASSUREUR)  TAUX_COM
  from REASS_DB.SAT_LINK_REASS_REASSUREUR where STATUT='ACTIF'
  group by  ID_REASSURANCE) k

   --   (select * from REASS_DB.SAT_QUOTASHARE where STATUT='ACTIF')k --,
     -- (select * from REASS_DB.SAT_COASSURANCE where STATUT='ACTIF')t

      where i.ID_GARANTIE=j.ID_GARANTIE(+)
      and   i.ID_REASSURANCE=k.ID_REASSURANCE(+)

      and i.ID_GARANTIE=f.ID_GARANTIE(+)
      --and   i.CODEINTE=t.CODEINTE
      --and   i.NUMEPOLI=t.NUMEPOLI(+)
      --and   i.NUMEAVEN=t.NUMEAVEN(+)
)
      ;

      delete from REASS_DB.INPUT_EDP;

      insert into REASS_DB.INPUT_EDP
     (
       select 
      distinct
        i.ID_GARANTIE,
        i.ID_REASSURANCE ,
        i.ID_POLICE_AVENANT,
        i.CODEINTE,
        i.NUMEPOLI,
        i.NUMEAVEN,
        j.CAPITAUX ,
        j.SMP      ,
        j.PRM_NT   ,
        k.TAUX_CESSION TAUX_CL    ,
        case when p.TAUX_CESSION is null then 0 else p.TAUX_CESSION end   TAUX_FAC ,
        case when t.TAUX_COASS is null then 1 else  t.TAUX_COASS end TAUX_COASS,

        case when  t.STATUT_COASS in ('Coassureur') then i.PRCT_RED_CPCT_C 
          else  i.PRCT_RED_CPCT_A end  PRCT_RED_CPCT,

        f.PLEIN_CONSERVATION     ,
        f.NB_PLEIN               ,
        f.ENGAGEMENT_REASSUREUR  ,
        f.CAPACITE_TRAITE        


      from
      (select distinct
      i.ID_GARANTIE, i.ID_REASSURANCE ,i.ID_POLICE_AVENANT,i.ID_RISQUE,i.CODEINTE,i.NUMEPOLI,i.NUMEAVEN,i.CLASS_RISQ,
      p.PRCT_RED_CPCT_C, p.PRCT_RED_CPCT_A

      from 
      (select distinct 
      i.ID_GARANTIE, i.ID_REASSURANCE ,i.ID_POLICE_AVENANT,i.ID_RISQUE,j.CODEINTE,j.NUMEPOLI,j.NUMEAVEN,h.CLASS_RISQ

      from
      (
      select
      distinct i.ID_GARANTIE, i.ID_REASSURANCE ,g.ID_POLICE_AVENANT,g.ID_RISQUE
      from

     (
      select distinct ID_GARANTIE, ID_REASSURANCE from REASS_DB.LINK_REASSURANCE_GARANTIE
      where ID_REASSURANCE in (select distinct ID_REASSURANCE from REASS_DB.HUB_REASSURANCE 
      where  substr(to_char(CODEREASS),1,3) = 'EDP'
       )
      )i,

      (select*from REASS_DB.LINK_AVENANT_RISQ_GARANTIE)g

      where I.ID_GARANTIE=G.ID_GARANTIE(+)
      )i,

    (  
    select *from  REASS_DB.SAT_POLICE_AVENANT where STATUT='ACTIF'
     )j,

    (  
    select distinct j.ID_GARANTIE,j.ID_POLICE_AVENANT,J.ID_RISQUE, k.CODEINTE,k.NUMEPOLI, k.NUMEAVEN,
t.LIBERISQ,t.CODERISQ ,t.LIBECLASS, t.CLASS_RISQUE CLASS_RISQ
from 
(select distinct ID_GARANTIE,ID_POLICE_AVENANT,ID_RISQUE from REASS_DB.LINK_AVENANT_RISQ_GARANTIE where STATUT='ACTIF')j,
(select distinct ID_POLICE_AVENANT, CODEINTE,NUMEPOLI, NUMEAVEN from REASS_DB.SAT_POLICE_AVENANT where STATUT='ACTIF')k,
(select distinct ID_RISQUE, LIBERISQ,CODERISQ ,LIBECLASS, CLASS_RISQUE from REASS_DB.SAT_RISQUE where STATUT='ACTIF')t

where j.ID_POLICE_AVENANT=k.ID_POLICE_AVENANT(+)  and j.ID_RISQUE=t.ID_RISQUE(+)
    )h


     where i.ID_POLICE_AVENANT=j.ID_POLICE_AVENANT(+)
        and i.ID_GARANTIE=h.ID_GARANTIE(+)

     )i, 

     (
     select distinct  QUALITE_RISQUE CLASS_RISQ,
            APERITEUR   PRCT_RED_CPCT_A,  
            COASSUREUR  PRCT_RED_CPCT_C   from reass_db.TB_CLASS_RISQUE

     )p
     where  i.CLASS_RISQ=p.CLASS_RISQ(+)
)i,
      (select *from REASS_DB.SAT_GARANTIE where STATUT='ACTIF')j,

      (
   select i.ID_REASSURANCE,j.ID_GARANTIE, i.TAUX_CESSION,i.TAUX_COM 
    --
        from
    --   (select distinct * from REASS_DB.SAT_QUOTASHARE where STATUT='ACTIF' and substr(to_char(CODEREASS),1,3) = 'FAC')i,
    --   (select distinct * from REASS_DB.LINK_REASSURANCE_GARANTIE)j
    --   where i.ID_REASSURANCE=j.ID_REASSURANCE(+)

        (select distinct ID_REASSURANCE, sum(PART_REASSUREUR) TAUX_CESSION ,
        sum(TAUX_COM_REASSURANCE*PART_REASSUREUR)  TAUX_COM
        from REASS_DB.SAT_LINK_REASS_REASSUREUR where STATUT='ACTIF'
        group by  ID_REASSURANCE)i,
       (select distinct ID_REASSURANCE,ID_GARANTIE from REASS_DB.LINK_REASSURANCE_GARANTIE)j
        where i.ID_REASSURANCE=j.ID_REASSURANCE(+)


       )p,


    --  (select distinct *from 
    --  (select  distinct i.ID_REASSURANCE,j.ID_GARANTIE,d.ID_POLICE_AVENANT,i.TAUX_CESSION,i.TAUX_COM ,q.CODETRAITE,
    --  to_char(q.DATEEFFE,'YYYY') ANNEEFFE_CL,to_char(q.DATEECHE,'YYYY') ANNEECHE_CL, 
    --  substr(k.DATEEFFE,7,4)  ANNEEFFE,substr(k.DATECOMP,7,4) ANNEECOMP
    --  from
    --   (select distinct * from REASS_DB.SAT_QUOTASHARE where STATUT='ACTIF' and substr(to_char(CODEREASS),1,2) = 'CL')i,
    --   (select distinct * from REASS_DB.LINK_REASSURANCE_GARANTIE)j,
    --   (select*from SAT_TRAITE_REASSURANCE) q,
    --   (select distinct * from LINK_AVENANT_RISQ_GARANTIE) d,
    --   (select distinct * from SAT_POLICE_AVENANT) k
    --  
    --   where i.ID_REASSURANCE=j.ID_REASSURANCE and i.ID_REASSURANCE=q.ID_REASSURANCE 
    --   and j.ID_GARANTIE=d.ID_GARANTIE and d.ID_POLICE_AVENANT=k.ID_POLICE_AVENANT
    --   )
    --   
    --   where ANNEEFFE>=ANNEEFFE_CL and ANNEECHE_CL>=ANNEEFFE
    --   )

    (select distinct ID_GARANTIE,ID_REASSURANCE, TAUX_CESSION from REASS_DB.SAT_LINK_REASS_GARA_CL where STATUT='ACTIF')
       k,



      (    select 
         distinct
      --  ID_COASSURANCE,
        ID_POLICE_AVENANT,
        STATUT_COASS,
        TAUX_COASS

        from
    (select distinct ID_POLICE_AVENANT,'Coassureur' STATUT_COASS,TAUX_COASS from REASS_DB.SAT_LINK_COASS_ACCEPTATION    where STATUT='ACTIF') 
    union all
    (select distinct ID_POLICE_AVENANT,'Apériteur' STATUT_COASS, sum(TAUX_COASS) TAUX_COASS
     from REASS_DB.SAT_LINK_COASS_CES_COASSUREUR where STATUT='ACTIF' and NOM_COASSUREUR  in ('Sanlam Assurance')
    group by ID_POLICE_AVENANT,'Apériteur') 
     -- select * from REASS_DB.SAT_COASSURANCE where STATUT='ACTIF'
      )t,

       (select * from REASS_DB.SAT_TRAITE_EDP where STATUT='ACTIF')f

      where i.ID_GARANTIE=j.ID_GARANTIE(+)

      and   i.ID_GARANTIE=p.ID_GARANTIE(+)
       and   i.ID_GARANTIE=k.ID_GARANTIE(+)
        and   i.ID_REASSURANCE=f.ID_REASSURANCE(+)

      and   i.ID_POLICE_AVENANT=t.ID_POLICE_AVENANT(+)
      --and   i.NUMEPOLI=t.NUMEPOLI(+)
     -- and   i.NUMEAVEN=t.NUMEAVEN(+)
)
      ;










    delete from REASS_DB.INPUT_REASS;

   insert into REASS_DB.INPUT_REASS  (
      ---drop table  REASS_DB.INPUT_REASS 
  --    create table REASS_DB.INPUT_REASS  as (


          select distinct

                 i.ID_GARANTIE  ,
                 i.CODEINTE      ,
                 i.NUMEPOLI      ,
                 i.NUMEAVEN      ,
                 --f.DATECOMP,
                 f.DATEEFFE,
                 f.DATEECHE,
                 f.CODECATE,
                 f.LIBECATE,
                 f.CODEBRAN,
                 f.LIBEBRAN,
                 i.CODEGARA,
                 i.LIBEL_BRAN_RE,
                 f.LIBERISQ,
                 i.CODERISQ,
                 f.LIBECLASS,
                 f.CLASS_RISQ ,
                 i.CODEASSU,
                 i.NOM ,
                 i.ACTIVITE,
                 nvl(i.CAPITAUX      ,            0   )  CAPITAUX             ,
                 nvl(i.SMP           ,            0   )   SMP                 ,
                 nvl(i.PRM_NT        ,            0   ) PRM_NT                ,
                 nvl(i.TAUX_CESSION  ,            0   ) TAUX_CESSION          ,
                 nvl(i.TAUX_COASS    ,            1   ) TAUX_COASS            ,
                 nvl(i.TAUX_FAC          ,        0   ) TAUX_FAC              ,
                 nvl(i.TAUX_COM        ,          0   ) TAUX_COM              ,
                 nvl(i.PRCT_RED_CPCT           ,  0   ) PRCT_RED_CPCT         ,
                 nvl(i.PLEIN_CONSERVATION      ,  0   ) PLEIN_CONSERVATION    ,
                 nvl(i.NB_PLEIN                ,  0   ) NB_PLEIN              ,
                 nvl(i.ENGAGEMENT_REASSUREUR   ,  0   ) ENGAGEMENT_REASSUREUR ,
                 nvl(i.CAPACITE_TRAITE          ,  0  ) CAPACITE_TRAITE


     from


      (select 
      distinct t.ID_GARANTIE, 
      --P.CODE_GARANTIE_REF,
             t.CODEINTE      ,
             t.NUMEPOLI      ,
             t.NUMEAVEN      ,
             q.CODEASSU,
             t.CODERISQ,
             t.CODEGARA,t.LIBEL_BRAN_RE,
             q.NOM ,
             q.ACTIVITE,
             t.CAPITAUX      ,
             t.SMP           ,
             t.PRM_NT        ,
             i.TAUX_CESSION  ,
             l.TAUX_COASS    ,
              k.TAUX_FAC          ,
              k.TAUX_COM        ,
             h.PRCT_RED_CPCT           ,
             h.PLEIN_CONSERVATION      ,
             h.NB_PLEIN                ,
             h.ENGAGEMENT_REASSUREUR   ,
             h.CAPACITE_TRAITE    

from 

(
select distinct t.ID_GARANTIE,
            p.ID_Sat_GARANTIE,p.CODEINTE,p.NUMEPOLI,p.NUMEAVEN,p.CODEGARA,p.CODERISQ,p.LIBEL_BRAN_RE,p.CAPITAUX,p.SMP,p.PRM_NT,
            reass_db.MD5(p.CODEINTE||'_'||p.NUMEPOLI||'_'||p.NUMEAVEN) ID_POLICE_AVENANT
          from
(
select distinct ID_GARANTIE
from REASS_DB.LINK_REASSURANCE_GARANTIE  where STATUT='ACTIF'
)t,

  (
select distinct ID_GARANTIE,ID_Sat_GARANTIE,CODEINTE,NUMEPOLI,NUMEAVEN,CODEGARA,CODERISQ,LIBEL_BRAN_RE,CAPITAUX,SMP,PRM_NT
--CODE_GARANTIE_REF,

from REASS_DB.SAT_GARANTIE where STATUT='ACTIF'
)p
where t.ID_GARANTIE=p.ID_GARANTIE(+)
)t,


(
select distinct
    i.ID_POLICE_AVENANT,j.CODEASSU,j.NOM,j.ACTIVITE,p.ID_GARANTIE --d.CODEINTE,d.NUMEPOLI,d.NUMEAVEN
    from
(select distinct * 
from  REASS_DB.LINK_INTER_CLIENT_POLICE)i,

(select distinct * 
from  REASS_DB.LINK_AVENANT_RISQ_GARANTIE)p,

--(select distinct * from  REASS_DB.SAT_POLICE_AVENANT)k,

(select distinct * from REASS_DB.SAT_CLIENT  )j
---(select distinct * from REASS_DB.SAT_POLICE_AVENANT)d
where     i.ID_POLICE_AVENANT=p.ID_POLICE_AVENANT(+)
      and i.ID_CLIENT=j.ID_CLIENT(+)  
  --    and   i.ID_POLICE_AVENANT=d.ID_POLICE_AVENANT(+)

   --  and i.ID_POLICE_AVENANT=p.ID_POLICE_AVENANT(+)
)q
,

--(
--select j.ID_GARANTIE,
-- j.CODEINTE,  j.NUMEPOLI,  j.NUMEAVEN,  j.CODERISQ,
--j.CODEGARA,
--i.CODE_GARANTIE_REF,

--CAPITAUX   , SMP,PRM_NT
--from (
--select distinct 
-- CODEINTE,  NUMEPOLI,  NUMEAVEN,  CODERISQ,CODEGARA,
--CODE_GARANTIE_REF,
 --  CAPITAUX      ,
 --            SMP,
 --            PRM_NT  from REASS_DB.GARANTIES1 
 --)i,
-- (select distinct ID_GARANTIE,CODEINTE,  NUMEPOLI,  NUMEAVEN,  CODERISQ,CODEGARA
--  from REASS_DB.HUB_GARANTIE)j

-- where 
 --j.CODE_GARANTIE_REF=i.CODE_GARANTIE_REF(+)
--    j.CODEINTE=i.CODEINTE(+)
 --and j.NUMEPOLI=i.NUMEPOLI(+)
 --and j.NUMEAVEN=i.NUMEAVEN(+)
 --and j.CODERISQ=i.CODERISQ(+)
-- and j.CODEGARA=i.CODEGARA(+)


--)tb,

(
select distinct *from REASS_DB.INPUT_CL 
)i,

(
 select 
         distinct
      --  ID_COASSURANCE,
        ID_POLICE_AVENANT,
        STATUT_COASS,
        TAUX_COASS

        from
    (select distinct ID_POLICE_AVENANT,'Coassureur' STATUT_COASS,TAUX_COASS from REASS_DB.SAT_LINK_COASS_ACCEPTATION    where STATUT='ACTIF') 
    union all
    (select distinct ID_POLICE_AVENANT,'Apériteur' STATUT_COASS, sum(TAUX_COASS) TAUX_COASS
     from REASS_DB.SAT_LINK_COASS_CES_COASSUREUR where STATUT='ACTIF' and NOM_COASSUREUR  in ('Sanlam Assurance')
    group by ID_POLICE_AVENANT,'Apériteur') )l,


(

select distinct *from REASS_DB.INPUT_EDP 
)h,

(
select distinct *from REASS_DB.INPUT_FAC 
)k

where t.ID_POLICE_AVENANT=l.ID_POLICE_AVENANT(+)
and  t.ID_GARANTIE=h.ID_GARANTIE(+)
and  t.ID_GARANTIE=k.ID_GARANTIE(+)
 and t.ID_GARANTIE=q.ID_GARANTIE(+)
and t.ID_GARANTIE=i.ID_GARANTIE(+)
 --and t.ID_GARANTIE=tb.ID_GARANTIE(+)

)i,


---where ID_GARANTIE=592696777
---group by ID_GARANTIE;

(
select distinct j.ID_GARANTIE,j.ID_POLICE_AVENANT,J.ID_RISQUE, --k.CODEINTE,k.NUMEPOLI, k.NUMEAVEN,
t.LIBERISQ,t.CODERISQ ,t.LIBECLASS, t.CLASS_RISQUE CLASS_RISQ,--f.DATECOMP,
f.DATEEFFE,f.DATEECHE,f.CODECATE,f.LIBECATE,f.CODEBRAN,f.LIBEBRAN
 from
(select distinct ID_GARANTIE,ID_POLICE_AVENANT,ID_RISQUE from REASS_DB.LINK_AVENANT_RISQ_GARANTIE where STATUT='ACTIF')j,
---(select distinct ID_POLICE_AVENANT, CODEINTE,NUMEPOLI, NUMEAVEN from REASS_DB.SAT_POLICE_AVENANT where STATUT='ACTIF')k,
(select distinct ID_RISQUE, LIBERISQ,CODERISQ ,LIBECLASS, CLASS_RISQUE from REASS_DB.SAT_RISQUE where STATUT='ACTIF')t,
(select distinct reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA) id,
--NUMEPOLI,CODEINTE,NUMEAVEN,
       
--DATECOMP,
DATEEFFE,DATEECHE,CODECATE,LIBECATE,CODEBRAN,LIBEBRAN
,LIBERISQ,CODERISQ,LIBECLASS,CLASS_RISQ   from REASS_DB.GARANTIES1)f

where --j.ID_POLICE_AVENANT=k.ID_POLICE_AVENANT(+)  and 
j.ID_RISQUE=t.ID_RISQUE(+) and j.ID_GARANTIE=f.id(+)
--and k.CODEINTE=f.CODEINTE(+)
--and k.NUMEPOLI=f.NUMEPOLI(+)
--and k.NUMEAVEN=f.NUMEAVEN(+)
)f



where i.ID_GARANTIE=f.ID_GARANTIE(+)
 );




delete from REASS_DB.SAT_LINK_REASS_GARANTIE_CL where DATE_INS=to_date(dte_ins,'ddmmyyyy')  and SOURCE=src 
;
insert into REASS_DB.SAT_LINK_REASS_GARANTIE_CL

select
distinct
    reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA)   ID_GARANTIE   ,
    reass_db.MD5(CODETRAITE)  ID_REASSURANCE    ,
    to_date(dte_ins,'ddmmyyyy') DATE_INS               ,
   to_date('99990101','yyyymmdd')  DATE_FIN ,
    src SOURCE             ,
   -- row_number() OVER (ORDER BY ID_REASSURANCE ASC) 
   reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA||'_'||CODETRAITE||'_'||TAUXPRIM||'_'||MONTCESS||'_'||src)   ID_SAT_LINK_REASS_GARANTIE_CL  ,
    0 CAPITAUX_AVANT_CL     ,        
    0 SMP_AVANT_CL              ,   
    0 PRIME_AVANT_CL            ,
    TAUXPRIM/100 TAUX_CL                        ,
    0 CAPITAUX_CEDES_CL     ,          
    0 SMP_CEDE_CL           ,
    MONTCESS PRIME_CEDEE_CL                    ,
    0 CAPITAUX_APRES_CL       ,     
    0 SMP_APRES_CL            ,     
    0 PRIME_APRES_CL          ,
    'ACTIF' STATUT                       

 from (select CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA,CODETRAITE,
  MAX(TAUXPRIM) TAUXPRIM, sum(MONTCESS) MONTCESS
  from reass_db.TABLE_REASS WHERE CODTYPTR='CL'  
  group by  CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA,CODETRAITE
  ) ;


delete from REASS_DB.SAT_LINK_REASS_GARANTIE_FAC where DATE_INS=to_date(dte_ins,'ddmmyyyy')  and SOURCE=src
;
insert into REASS_DB.SAT_LINK_REASS_GARANTIE_FAC
    select
    reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA)   ID_GARANTIE   ,
    reass_db.MD5(CODETRAITE)  ID_REASSURANCE    ,
    to_date(dte_ins,'ddmmyyyy') DATE_INS                         ,
  to_date('99990101','yyyymmdd')  DATE_FIN ,
    src SOURCE                           ,
   reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA||'_'||CODETRAITE||'_'||TAUXPRIM||'_'||MONTCESS||'_'||src)   ID_SAT_LINK_REASS_GARANTIE_FAC   ,
    0  CAPITAUX_AVANT_FAC               ,
    0   SMP_AVANT_FAC                  ,
    0 PRIME_AVANT_FAC              ,
    TAUXPRIM/100 TAUX_FAC                     ,
    0 CAPITAUX_CEDES_FAC                   ,
    0 SMP_CEDE_FAC                   ,
    MONTCESS PRIME_CEDEE_FAC                      ,
    0 CAPITAUX_APRES_FAC               ,
    0 SMP_APRES_FAC      ,
    0 PRIME_APRES_FAC                  ,                      
    'ACTIF' STATUT                               

  from (select CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA,CODETRAITE,
  MAX(TAUXPRIM) TAUXPRIM, sum(MONTCESS) MONTCESS
  from reass_db.TABLE_REASS WHERE CODTYPTR='FQ'  
  group by  CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA,CODETRAITE
  )
  ;


delete from REASS_DB.SAT_LINK_REASS_GARANTIE_EDP where DATE_INS=to_date(dte_ins,'ddmmyyyy')  and SOURCE=src
;
insert into REASS_DB.SAT_LINK_REASS_GARANTIE_EDP

    select
    reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA)   ID_GARANTIE   ,
    reass_db.MD5(CODETRAITE)  ID_REASSURANCE    ,
    to_date(dte_ins,'ddmmyyyy') DATE_INS                       ,
   to_date('99990101','yyyymmdd')  DATE_FIN ,
    src SOURCE                             ,
   reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA||'_'||CODETRAITE||'_'||TAUXPRIM||'_'||MONTCESS||'_'||src)   ID_SAT_LINK_REASS_GARANTIE_EDP ,
    0 CAPITAUX_AVANT_EDP                   ,
    0 SMP_AVANT_EDP                   ,
    0 PRIME_AVANT_EDP                      ,
    0 PRCT_RED_CPCT                ,
    0 PLEIN_CONSERVATION           ,
    0 NB_PLEIN                     ,
    0 ENGAGEMENT_REASSUREUR        ,
    0 CAPACITE_TRAITE              ,
    
    TAUXPRIM/100 TAUX_EDP                         ,
    0 CAPITAUX_CEDE_EDP           ,
    0 SMP_CEDE_EDP                , 
    MONTCESS PRIME_CEDEE_EDP             , 
    0 CAPITAUX_APRES_EDP          , 
    0 SMP_APRES_EDP               , 
    0 PRIME_APRES_EDP             , 
    0 CAPITAUX_RETENTION          , 
    0 SMP_RETENTION               , 
    0 PRIME_RETENTION             , 
    0 TAUX_RETENTION              , 
    0 CAPITAUX_DECOUVERT          , 
    0 SMP_DECOUVERT               , 
    0 PRIME_DECOUVERT             ,
    'ACTIF' STATUT                               

  from (select CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA,CODETRAITE,
  MAX(TAUXPRIM) TAUXPRIM, sum(MONTCESS) MONTCESS
  from reass_db.TABLE_REASS WHERE CODTYPTR='XP'  
  group by  CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA,CODETRAITE)
  ;  

delete from REASS_DB.SAT_OUTPUT_REASS where DATE_INS=to_date(dte_ins,'ddmmyyyy')  and SOURCE=src 
;
insert into REASS_DB.SAT_OUTPUT_REASS
select
       reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA)  ID_GARANTIE   ,
       ID_REASSURANCE_CL,
        ID_REASSURANCE_FAC,
       ID_REASSURANCE_EDP,  
       to_date(dte_ins,'ddmmyyyy') DATE_INS                       ,
       to_date('99990101','yyyymmdd')  DATE_FIN ,
       src SOURCE     ,
        reass_db.MD5(CODEINTE||'_'||NUMEPOLI||'_'||NUMEAVEN||'_'||CODERISQ||'_'||CODEGARA||'_'||CODETRAITE||'_'||TAUX_CL||'_'||TAUX_FAC||'_'||TAUX_EDP||'_'||src)    ID_SAT_OUTPUT_REASS     ,
         CODEINTE                ,
         NUMEPOLI                ,
         NUMEAVEN                ,
         DATEEFFE                ,
         DATEECHE                ,
         CODECATE                ,
         LIBECATE                ,
         CODEBRAN                ,
         LIBEBRAN                ,
         CODEGARA                ,
         LIBEL_BRAN_RE           ,
         LIBERISQ                ,
         CODERISQ                ,
         '' LIBECLASS               ,
         '' CLASS_RISQ              ,
        '' ACTIVITE                ,
         '' CODEASSU                ,
         '' NOM                     ,
        0 CAPITAUX_100            ,
        0 SMP_100                 ,
        PRIMBRUT PRM_NT_100              ,
        0 TAUX_COASS              ,
        0 CAPITAUX_NET_COASS      ,
        0 SMP_NET_COASS           ,
        0 PRIME_NET_COASS         ,
        0 PRIME_REVERSEE_COASS    ,
        0 CAPITAUX_AVANT_CL     ,        
        0 SMP_AVANT_CL              ,   
        0 PRIME_AVANT_CL            ,
          TAUX_CL                        ,
        0 CAPITAUX_CEDES_CL     ,          
        0 SMP_CEDE_CL           ,
         PRIME_CEDEE_CL                    ,
        0 CAPITAUX_APRES_CL       ,     
        0 SMP_APRES_CL            ,     
        0 PRIME_APRES_CL          ,
        0  CAPITAUX_AVANT_FAC               ,
        0   PRIME_AVANT_FAC                  ,
        0 PRIME_AVANT_FAC              ,
           TAUX_FAC                     ,
        0 CAPITAUX_CEDES_FAC                   ,
        0 SMP_CEDE_FAC                   ,
          PRIME_CEDEE_FAC                      ,
        0 CAPITAUX_APRES_FAC               ,
        0 SMP_APRES_FAC      ,
        0 PRIME_APRES_FAC                  ,   
          
        0 CAPITAUX_AVANT_EDP      ,
        0 SMP_AVANT_EDP           ,
        0 PRIME_AVANT_EDP         ,
        0 PRCT_RED_CPCT           ,
        0 PLEIN_CONSERVATION      ,
        0 NB_PLEIN                ,
        0 ENGAGEMENT_REASSUR      ,
        0 CAPACITE_TRAITE         ,
          TAUX_EDP                ,
        0 CAPITAUX_CEDE_EDP       ,
        0 SMP_CEDE_EDP            ,
          PRIME_CEDEE_EDP         ,
        0 CAPITAUX_APRES_EDP      ,
        0 SMP_APRES_EDP           ,
        0 PRIME_APRES_EDP         ,
        0 CAPITAUX_RETENTION      ,
        0 SMP_RETENTION           ,
        0 PRIME_RETENTION         ,
        0 TAUX_RETENTION          ,
        0 CAPITAUX_DECOUVERT      ,
        0 SMP_DECOUVERT           ,
        0 PRIME_DECOUVERT         ,
        'ACTIF' STATUT

     from (
      
      select CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA,
      max(CODETRAITE) CODETRAITE,
      max( decode(CODTYPTR,'CL',reass_db.MD5(CODETRAITE) ,'')) ID_REASSURANCE_CL,
      max(  decode(CODTYPTR,'FQ',reass_db.MD5(CODETRAITE) ,'') )ID_REASSURANCE_FAC,
      max(  decode(CODTYPTR,'XP',reass_db.MD5(CODETRAITE) ,'') )ID_REASSURANCE_EDP,  
      max(DATEEFFE      ) DATEEFFE,
      max(DATEECHE      ) DATEECHE,
      max(CODECATE      ) CODECATE,
      max(LIBECATE      ) LIBECATE,
      max(CODEBRAN      ) CODEBRAN,
      max(LIBEBRAN     ) LIBEBRAN,
      max(LIBERISQ      ) LIBERISQ,
      max(LIBEL_BRAN_RE) LIBEL_BRAN_RE,
      
      
    MAX( decode(CODTYPTR,'XP',TAUXPRIM/100,0)) TAUX_EDP,
    MAX( decode(CODTYPTR,'FQ',TAUXPRIM/100,0)) TAUX_FAC,
    MAX( decode(CODTYPTR,'CL',TAUXPRIM/100,0)) TAUX_CL,
    sum( decode(CODTYPTR,'XP',MONTCESS,0)) PRIME_CEDEE_EDP,
    sum( decode(CODTYPTR,'FQ',MONTCESS,0)) PRIME_CEDEE_FAC,
    sum( decode(CODTYPTR,'CL',MONTCESS,0)) PRIME_CEDEE_CL,
    max(PRIMBRUT) PRIMBRUT

    from reass_db.TABLE_REASS WHERE CODTYPTR IN ('XP','FQ','CL')  
    group by  CODEINTE,NUMEPOLI,NUMEAVEN,CODERISQ,CODEGARA,CODETRAITE
  
  
  )

commit;
end;
/
