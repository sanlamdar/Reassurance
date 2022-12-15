create table reass_db.SINISTRE_REAS_CESS2021 AS
select b.CODEBRAN,
      categ ,
      x.CODECATE,
      categorie ,
      intermediaire, 
      nom_intermediaire,
      annee ,
      sinistre ,
      Nr_Reference,
      survenance ,
      declaration ,
      police ,
      avenant,
      assure, 
      nom_assure,     
      -- coderisq,
      -- codegara,
       TAUXPART,
       decode(flagcopr,NULL,'G',flagcopr) "FLAGPR",
       decode(flagcoho,NULL,'G',flagcoho) "FLAGH0",
       sum(montant_sap) montant_sap,
       sum(principal_sap) principal_sap,
       sum(honoraires_sap) honoraires_sap,

       sum(montant_regl) montant_regl,
       sum(principal_regl) principal_regl,
       sum(honoraires_regl) honoraires_regl,


       numetrai,
       exertrai,
       numesect,
       numetran,
      -- codtyptr,
       resebrut,
       totaregl,
       sum(monrestr) monrestr,
--       sum(decode()) ""conservation"",
       sum(decode(codtyptr,'CL',monrestr,0)) cession_legale_sap,
       sum(decode(codtyptr,'QP',monrestr,0)) cession_QP_sap,
       sum(decode(codtyptr,'XP',monrestr,0)) cession_XP_sap,
       sum(decode(codtyptr,'FQ',monrestr,'FX',monrestr, 'OQ',monrestr,'OP',monrestr, 'FO',monrestr,0)) cession_fac_sap,
       sum(decode(codtyptr,'XS',monrestr,0)) cession_XS_sap,

       sum(montregl)montregl,
       sum(decode(codtyptr,'CL',montregl,0)) cession_legale_regl,
       sum(decode(codtyptr,'QP',montregl,0)) cession_QP_regl,
       sum(decode(codtyptr,'XP',montregl,0)) cession_XP_regl,
       sum(decode(codtyptr,'FQ',montregl,  'FX',montregl,'OQ',montregl,'OP',montregl,'FO',montregl,0)) cession_fac_regl,
       sum(decode(codtyptr,'XS',montregl,0)) cession_XS_regl,

       frantran,
       porttran,
       CAPIBRUT,
       MONCAPTR,
       MONT_SMP,
     --  TAUXCESS,
       CATEREAS,
       VALEPLEI,
       NOMPLERE,
       NOMPLETR
from (

          --------------traitement des suspends  simpl       
Select 

m.caterisq categ,
m.CODECATE,
c.libecate categorie,
m.codeinte intermediaire,
i.raisocin nom_intermediaire,
m.exersini annee,
m.numesini sinistre,
          --m.dateeval ""date"",                         -------- regle   
s.refeinte Nr_Reference,
s.datesurv survenance,
s.datedecl declaration,
s.numepoli police,
s.numeaven avenant,
s.codeassu assure,
a.raissoci nom_assure,
s.CODERISQ,
m.CODEGARA,     -- suspend   
s.TAUXPART,
      ------m.NUMEREGL,                    -------- regle        
s.FLAGCOPR,
s.FLAGCOHO,
sum(m.monteval) MONTANT_sap,
sum(m.montprin) principal_sap,
sum(m.monthono) honoraires_sap,
null MONTANT_regl,
null principal_regl,
null honoraires_regl,
--FLAGCOPR,
--TAUXPART,
null NUMETRAI ,
null EXERTRAI,
null NUMESECT,
null NUMETRAN,
null CODTYPTR,
null  codegara,
null  coderisq,
null RESEBRUT,
null REGLBRUT ,                 -------- regle    
null TOTAREGL,
null MONRESTR,
null FRANTRAN,
null PORTTRAN,
null REGLANTE ,   --------------- regle 
null CAPIBRUT,
null MONTREGL ,   --------------- regle 
null MONCAPTR,
null MONT_SMP,
null MONTCAPI ,    --------------- regle 
null MONTRECO ,      --------------- regle 
null PRIMRECO ,   --------------- regle       
--NOMPLERE,
null TAUXCESS,
null CATEREAS,
null VALEPLEI,
null CODCLARI,   --------------- regle 
null NOMPLERE,
null NOMPLETR    

from 
v_mouvement_sinistre m,
       sinistre s,
       categorie c,
       intermediaire i,
       assure a,
       sort_sinistre ss,
       type_sort ts
 where m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
   and m.caterisq = c.codecate
   and m.codeinte = i.codeinte
   and s.codeassu = a.codeassu
   and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
   and ss.datsorsi = (select max(ss2.datsorsi)
                        from sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                       and ss2.exersini = m.exersini
                       and ss2.numesini = m.numesini
                       and trunc(ss2.datsorsi) <= to_date('31/12/2021','dd/mm/yyyy')) --:Date_fin
   and ss.codtypso = ts.codtypso
   and ts.natusort in ('OU','RO')
   and m.typemouvement in ('EVAL','REGLE')
   and trunc(m.dateeval) <= to_date('31/12/2021','dd/mm/yyyy')-- :Date_fin
   -- and m.numesini = 400874 and m.exersini = 2015  and m.codeinte = 3215    
   --  and m.numesini = 700041 and m.exersini = 2009  and m.codeinte = 3002  
  -- and m.numesini = 200001 and m.exersini = 2009  and m.codeinte = 3002 
  -- and m.numesini = 300352 and m.exersini = 2017  and m.codeinte = 3058 
   --and m.numesini = 400874 and m.exersini = 2015  and m.codeinte = 3215 
 -- and m.numesini = 200001 and m.exersini = 2009  and m.codeinte = 3002  
  -- and  m.CODECATE=400  


 group by
       m.caterisq,
       m.CODECATE,
       c.libecate,
       m.codeinte,
       i.raisocin,
       m.exersini,
       m.numesini,
       s.refeinte,
       s.datesurv,
       s.datedecl,
       s.numepoli,
       s.numeaven,
       s.codeassu,
       a.raissoci,
       s.coderisq,
       m.codegara,
       s.TAUXPART,
       s.flagcopr,
       s.flagcoho


  union all   
          --------------traitement des suspends  reass      
Select 
m.caterisq categ,
m.CODECATE,
c.libecate categorie,
m.codeinte intermediaire,
i.raisocin nom_intermediaire,
m.exersini annee,
m.numesini sinistre,
          --m.dateeval ""date"",                         -------- regle   
s.refeinte Nr_Reference,
s.datesurv survenance,
s.datedecl declaration,
s.numepoli police,
s.numeaven avenant,
s.codeassu assure,
a.raissoci nom_assure,
s.CODERISQ,
m.CODEGARA,     -- suspend   
s.TAUXPART,
      ------m.NUMEREGL,                    -------- regle        
s.FLAGCOPR,
s.FLAGCOHO,

null MONTANT_sap,
null principal_sap,
null honoraires_sap,
null MONTANT_regl,
null principal_regl,
null honoraires_regl,



--FLAGCOPR,
--TAUXPART,
  NUMETRAI ,
  EXERTRAI,
  NUMESECT,
  NUMETRAN,
  CODTYPTR,
    vv.codegara,
    vv.coderisq,
  RESEBRUT,
 null REGLBRUT ,                 -------- regle    
 null TOTAREGL,
 MONRESTR,
 FRANTRAN,
  PORTTRAN,
 null REGLANTE ,   --------------- regle 
 CAPIBRUT,
 null MONTREGL ,   --------------- regle 
 MONCAPTR,
 MONT_SMP,
 null MONTCAPI ,    --------------- regle 
 null MONTRECO ,      --------------- regle 
 null PRIMRECO ,   --------------- regle       
--NOMPLERE,
  TAUXCESS,
  CATEREAS,
  VALEPLEI,
  CODCLARI,   --------------- regle 
  NOMPLERE,
  NOMPLETR

from 
v_mouvement_sinistre m,
       sinistre s,
       categorie c,
       intermediaire i,
       assure a,
       sort_sinistre ss,
       type_sort ts,
       reass.vv_reserve_reassurance vv
 where m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
   and m.caterisq = c.codecate
   and m.codeinte = i.codeinte
   and s.codeassu = a.codeassu
   and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
   and ss.datsorsi = (select max(ss2.datsorsi)
                        from sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                         and trunc(ss2.datsorsi) <= to_date('31/12/2021','dd/mm/yyyy') --:Date_fin)
   and ss.codtypso = ts.codtypso
   and ts.natusort in ('OU','RO')
   and m.typemouvement in ('EVAL','REGLE')
   and trunc(m.dateeval) <= to_date('31/12/2021','dd/mm/yyyy')--:Date_fin
    and m.codeinte = vv.codeinte
   and m.exersini = vv.exersini
   and m.numesini = vv.numesini
   and s.coderisq = vv.coderisq
   and m.codegara = vv.codegara
   and vv.datecomp = to_date('01/01/2021','dd/mm/yyyy')) --:Date_fin_reass
  -- and m.numesini = 400874 and m.exersini = 2015  and m.codeinte = 3215    
   -- and m.numesini = 700041 and m.exersini = 2009  and m.codeinte = 3002  
   --and m.numesini = 200001 and m.exersini = 2009  and m.codeinte = 3002  
  --   and m.numesini = 400874 and m.exersini = 2015  and m.codeinte = 3215  
  -- and m.numesini = 300352 and m.exersini = 2017  and m.codeinte = 3058 



 group by
       m.caterisq,
       m.CODECATE,
       c.libecate,
       m.codeinte,
       i.raisocin,
       m.exersini,
       m.numesini,
       s.refeinte,
       s.datesurv,
       s.datedecl,
       s.numepoli,
       s.numeaven,
       s.codeassu,
       a.raissoci,
       s.coderisq,
       m.codegara,
       s.TAUXPART,
       s.flagcopr,
       s.flagcoho,
       NUMETRAI,
       EXERTRAI,
       NUMESECT,
       NUMETRAN,
       CODTYPTR,
       vv.codegara,
       vv.coderisq,
       RESEBRUT,
       TOTAREGL,
       MONRESTR,
       FRANTRAN,
       PORTTRAN,
       CAPIBRUT,
       MONCAPTR,
       MONT_SMP,
       TAUXCESS,
       CATEREAS,
       VALEPLEI,
       CODCLARI,   --------------- regle 
       NOMPLERE,
       NOMPLETR


union all        
       --------------traitement des reglement   simple 

       Select 
m.caterisq categ,
m.CODECATE,
c.libecate categorie,
m.codeinte intermediaire,
i.raisocin nom_intermediaire,
m.exersini annee,
m.numesini sinistre,
          --m.dateeval ""date"",                         -------- regle   
s.refeinte Nr_Reference,
s.datesurv survenance,
s.datedecl declaration,
s.numepoli police,
s.numeaven avenant,
s.codeassu assure,
a.raissoci nom_assure,
s.CODERISQ,
m.CODEGARA,     -- suspend   
s.TAUXPART,
      ------m.NUMEREGL,                    -------- regle        
s.FLAGCOPR,
s.FLAGCOHO,

  --  null  /* -m.monteval */ MONTANT,
 --   null /*  -m.montprin */ principal,
  --  null  /* -m.monthono */ honoraires,   
 null MONTANT_sap,
 null principal_sap,
 null honoraires_sap,

 -m.monteval MONTANT_regl,
 -m.montprin principal_regl,
 -m.monthono honoraires_regl,




--FLAGCOPR,
--TAUXPART,
null NUMETRAI ,
null EXERTRAI,
null NUMESECT,
null NUMETRAN,
null CODTYPTR,
null    codegara,
null   coderisq,
null RESEBRUT,
null REGLBRUT ,                 -------- regle    
null TOTAREGL,
null MONRESTR,
null FRANTRAN,
null PORTTRAN,
null REGLANTE ,   --------------- regle 
null CAPIBRUT,
null MONTREGL ,   --------------- regle 
null MONCAPTR,
null MONT_SMP,
null MONTCAPI ,    --------------- regle 
null MONTRECO ,      --------------- regle 
null PRIMRECO ,   --------------- regle       
--NOMPLERE,
null TAUXCESS,
null CATEREAS,
null VALEPLEI,
null CODCLARI,   --------------- regle 
null NOMPLERE,
null NOMPLETR

from 
v_mouvement_sinistre m,
     sinistre s,
       categorie c,
       intermediaire i,
       assure a
 where m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
   and m.caterisq = c.codecate
   and m.codeinte = i.codeinte
   and s.codeassu = a.codeassu
   and m.typemouvement = 'REGLE'
  and m.dateeval between to_date('01/01/2021','dd/mm/yyyy') and to_date('31/12/2021','dd/mm/yyyy')  -- :Date_debut   and :Date_fin
 -- and m.numesini = 400874 and m.exersini = 2015  and m.codeinte = 3215  
-- and m.numesini = 400874 and m.exersini = 2015  and m.codeinte = 3215 
--and m.numesini = 200001 and m.exersini = 2009  and m.codeinte = 3002   

  -- m.CODECATE=400


union all        
       --------------traitement des reglement   avk reass  

Select 
m.caterisq categ,
m.CODECATE,
c.libecate categorie,
m.codeinte intermediaire,
i.raisocin nom_intermediaire,
m.exersini annee,
m.numesini sinistre,
          --m.dateeval ""date"",                         -------- regle   
s.refeinte Nr_Reference,
s.datesurv survenance,
s.datedecl declaration,
s.numepoli police,
s.numeaven avenant,
s.codeassu assure,
a.raissoci nom_assure,
s.CODERISQ,
m.CODEGARA,     -- suspend   
s.TAUXPART,
      ------m.NUMEREGL,                    -------- regle        
s.FLAGCOPR,
s.FLAGCOHO,
/*
null MONTANT,
null principal,
null honoraires,
*/
 null MONTANT_sap,
 null principal_sap,
 null honoraires_sap,

 null MONTANT_regl,
 null principal_regl,
 null honoraires_regl,



--FLAGCOPR,
--TAUXPART,
  NUMETRAI ,
  EXERTRAI,
  NUMESECT,
  NUMETRAN,
  CODTYPTR,
    vr.codegara,
    vr.coderisq,
 null RESEBRUT,
 null REGLBRUT ,                 -------- regle    
 null TOTAREGL,
 null MONRESTR,
 FRANTRAN,
  PORTTRAN,
   REGLANTE ,   --------------- regle 
 CAPIBRUT,
  MONTREGL ,   --------------- regle 
 null MONCAPTR,
 MONT_SMP,
   MONTCAPI ,    --------------- regle 
   MONTRECO ,      --------------- regle 
   PRIMRECO ,   --------------- regle       
--NOMPLERE,
  TAUXCESS,
  CATEREAS,
  VALEPLEI,
  CODCLARI,   --------------- regle 
  NOMPLERE,
  NOMPLETR

from 
v_mouvement_sinistre m,
       sinistre s,
       categorie c,
       intermediaire i,
       assure a,
        reass.z#rea_reglement_reassurance vr
 where m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
   and m.caterisq = c.codecate
   and m.codeinte = i.codeinte
   and s.codeassu = a.codeassu
   and vr.intesini || vr.exersini || vr.numesini = s.codeinte || s.exersini || s.numesini
   and vr.numeregl = m.numeregl
--   and vv.numelign = m.numelign
   --and vv.reglbrut = -m.monteval
   and vr.codegara = m.codegara
   and vr.datetrai = m.dateeval
   and m.typemouvement = 'REGLE'
   and m.dateeval between to_date('01/01/2021','dd/mm/yyyy') and to_date('31/12/2021','dd/mm/yyyy')  -- :Date_debut  and :Date_fin
  -- and m.numesini = 400874 and m.exersini = 2015  and m.codeinte = 3215  
   -- and m.numesini = 200001 and m.exersini = 2009  and m.codeinte = 3002  
  ) x ,CATEGORIE c,branche b
  where  c.CODECATE=x.categ and c.CODEBRAN=b.CODEBRAN
  group by 
   categ ,
    x.CODECATE,
      categorie ,
      intermediaire ,
           nom_intermediaire,
           annee ,
           sinistre ,
           Nr_Reference,
            survenance ,
            declaration ,
            police ,
            avenant,
            assure,
           nom_assure,     
      -- coderisq,
      -- codegara,
       TAUXPART,
       decode(flagcopr,NULL,'G',flagcopr) ,
       decode(flagcoho,NULL,'G',flagcoho)  ,

       numetrai,
       exertrai,
       numesect,
       numetran,
       codtyptr,
       resebrut,
       totaregl,
       codtyptr, 
       frantran,
       porttran,
       CAPIBRUT,
       MONCAPTR,
       MONT_SMP,
    --   TAUXCESS,
       CATEREAS,
       VALEPLEI,
       NOMPLERE,
       NOMPLETR ,b.CODEBRAN
