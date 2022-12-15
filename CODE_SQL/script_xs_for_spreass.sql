
--delete from reass_db.TABLE_SINREASS_1 ;

---insert into reass_db.TABLE_SINREASS_1 
drop table reass_db.TABLE_SINREASS_1 ;
create table  reass_db.TABLE_SINREASS_1 as  --Table qui contient l'ensemble des taux millaird, datavault, avant et apres

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
       ,decode(k.CODE_BRAN_RE ,NULL, b.CODE_BRAN_RE,  k.CODE_BRAN_RE )   CODE_BRAN_RE      
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
       ,case when CODTYPTR='CL' THEN q.TAUX_CL    else 0  end  TAUX_CL_MILLIARD
       ,case when CODTYPTR='XP' THEN q.TAUX_EDP    else 0  end   TAUX_EDP_MILLIARD
       ,case when CODTYPTR='FQ' THEN q.TAUX_FAC_UP    else 0  end   TAUX_FAC_MILLIARD
       
       ,case when CODTYPTR='CL' THEN JL.TAUX_CL    else 0  end  TAUX_CL_DTV_AV
       ,case when CODTYPTR='XP' THEN j.TAUX_EDP    else 0  end   TAUX_EDP_DTV_AV
       ,case when CODTYPTR='FQ' THEN j.TAUX_FAC   else 0  end   TAUX_FAC_DTV_AV
        
       ,case when CODTYPTR='CL' THEN pL.TAUX_CL    else 0  end  TAUX_CL_DTV_AP
       ,case when CODTYPTR='XP' THEN p.TAUX_EDP    else 0  end   TAUX_EDP_DTV_AP
       ,case when CODTYPTR='FQ' THEN p.TAUX_FAC   else 0  end   TAUX_FAC_DTV_AP

       ,nvl(J.TAUX_DECOUVERT,0) TAUX_DECOUVERT_AV
      
       ,nvl(p.TAUX_DECOUVERT,0) TAUX_DECOUVERT_AP
       , nvl(l.TAUXPART,1) TAUX_COASS    

 from 
 
(select distinct * from 
(select i.*,
 reass_db.MD5(i.CODEINTE||'_'||i.NUMEPOLI||'_'||i.NUMEAVEN||'_'||i.CODERISQ||'_'||i.CODEGARA)ID_GARANTIE,
decode(J.CODTYPTR,null,u.codtyptr,J.CODTYPTR) CODTYPTR,
j.CODE_BRAN_RE,

decode(J.CODTYPTR,null,u.DESITRAI,decode(J.CODTYPTR,'FQ','FQ',J.DESITRAI)) DESITRAI,
decode(J.CODTYPTR,null,u.TAUXPRIM,nvl(J.TAUXPRIM/100,0)) TAUXPRIM
 from
(select * from actuary.charge_sinistre
where annee >= 2014 and CODTYPSO in ('RE','OU','TR')) i,

(select * from REASS_DB.PRIME_CEDEE_ANT2021 where to_char(dateeffe,'YYYY') >= 2014)j,

(select 
 distinct
CODEINTE,NUMEPOLI,NUMEAVEN,--CODERISQ,CODEGARA, 
'CL' CODTYPTR,
'Cession précipitaire CICARE' DESITRAI,
max(TAUX_CL) TAUXPRIM

 from REASS_DB.SAT_OUTPUT_REASS where STATUT='ACTIF' and TAUX_CL is not null
 
 group by CODEINTE,NUMEPOLI,NUMEAVEN,
'CL',
'Cession précipitaire CICARE'
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
nvl(TAUX_FAC,0)*nvl(l.TAUXPART,1)*(1-nvl(TAUX_CL,0)) TAUX_FAC_UP
 
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

   
(select distinct CODEINTE,NUMEPOLI, nvl(NUMEAVEN,0) NUMEAVEN, TAUXPART/100  TAUXPART from ORASS_V6.QUITTANCE_COASSURANCE where CODECOAS=0) l

where  i.CODEINTE=l.CODEINTE(+)
   and i.NUMEPOLI=l.NUMEPOLI(+)
   and i.NUMEAVEN=l.NUMEAVEN(+)
 
)
 q 
 ,(select CODEGARA, CODBRARE CODE_BRAN_RE from TB_CODEGARA_CODBRANRE)b
   
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
   and K.CODEGARA=b.CODEGARA(+)
  -- and K.NUMEPOLI=b.NUMEPOLI(+)
  -- and k.NUMEAVEN=b.NUMEAVEN(+)
  -- )i
)
;

drop table reass_db.TABLE_SINREASS_2;

--delete from reass_db.TABLE_SINREASS_2 ;

---insert into reass_db.TABLE_SINREASS_2 
--drop table reass_db.TABLE_SINREASS_2 ;
create table  reass_db.TABLE_SINREASS_2 as  -- table qui contien t les taux definitifs de cession et les differents couts de cession de sinitres
(
select 
*
from
( 
  select
    i.ID_GARANTIE 
   ,i.ID_GARANTIE_SINI
   ,i.ANNEE            
   ,i.DEAL_DATE        
   ,i.CODEINTE         
   ,i.RAISOCIN         
   ,i.NUMESINI         
   ,i.REFEINTE         
   ,i.NUMEPOLI         
   ,i.NUMEAVEN         
   ,i.DATEEFFE         
   ,i.DATEECHE         
   ,i.CODEASSU         
   ,i.NOM              
   ,i.CODEBRAN         
   ,i.CODE_BRAN_RE     
   ,i.LIBEBRAN         
   ,i.CODECATE         
   ,i.CATERISQ         
   ,i.LIBECATE         
   ,i.CODEGARA         
   ,i.CODERISQ         
   ,i.NATUSINI         
   ,i.LIBEGARA         
   ,i.DATESURV         
   ,i.DATEDECL         
   ,i.SINPAY           
   ,i.SINPAY_ANT       
   ,i.RECENC           
   ,i.RECENC_ANT       
   ,i.SOLDE_PAIEMENTS  
   ,i.EVAL             
   ,i.SAP              
   ,i.AREC             
   ,i.COUTS_SINISTRES  
   ,i.SINPAY_100       
   ,i.SINPAY_ANT_100   
   ,i.RECENC_100       
   ,i.RECENC_ANT_100   
   ,i.SOLDE_PAIEMENTS_100
   ,i.EVAL_100         
   ,i.SAP_100          
   ,i.AREC_100         
   ,i.COUTS_SINISTRES_100
   ,i.CODTYPSO         
   ,i.LIBTYPSO 
   ,i.TAUX_COASS
   ,nvl(decode(TAUX_CL_DTV_AP,null,TAUX_CL_MILLIARD, TAUX_CL_DTV_AP),  0)    TAUX_CL
   ,nvl(decode(TAUX_EDP_DTV_AV,null,TAUX_EDP_MILLIARD,TAUX_EDP_DTV_AV),0) TAUX_EDP
   ,nvl(decode(TAUX_FAC_DTV_AP,null,TAUX_FAC_MILLIARD,TAUX_FAC_DTV_AP),0) TAUX_FAC
   ,TAUX_DECOUVERT_AP TAUX_DECOUVERT
  
   ,(SINPAY_100+SINPAY_ANT_100+SAP_100)*TAUX_COASS*nvl(decode(TAUX_CL_DTV_AP,null,TAUX_CL_MILLIARD, TAUX_CL_DTV_AP), 0)  COUTS_BR_REC_CEDES_CL
   
   ,(SINPAY_100+SINPAY_ANT_100+SAP_100)*nvl(decode(TAUX_EDP_DTV_AV,null,TAUX_EDP_MILLIARD,TAUX_EDP_DTV_AV),0)  COUTS_BR_REC_CEDES_EDP
   
   ,(SINPAY_100+SINPAY_ANT_100+SAP_100)*nvl(decode(TAUX_FAC_DTV_AP,null,TAUX_FAC_MILLIARD,TAUX_FAC_DTV_AP),0)  COUTS_BR_REC_CEDES_FAC
   
   ,(SINPAY_100+SINPAY_ANT_100+SAP_100)*TAUX_DECOUVERT_AP  COUTS_BR_REC_CEDES_DECOUVERT
   
   ,(SINPAY_100+SINPAY_ANT_100+SAP_100)*TAUX_COASS-
   (SINPAY_100+SINPAY_ANT_100+SAP_100)*TAUX_COASS*nvl(decode(TAUX_CL_DTV_AP,null,TAUX_CL_MILLIARD, TAUX_CL_DTV_AP),0)-
   (SINPAY_100+SINPAY_ANT_100+SAP_100)*nvl(decode(TAUX_EDP_DTV_AV,null,TAUX_EDP_MILLIARD,TAUX_EDP_DTV_AV),0)-
   (SINPAY_100+SINPAY_ANT_100+SAP_100)*nvl(decode(TAUX_FAC_DTV_AP,null,TAUX_FAC_MILLIARD,TAUX_FAC_DTV_AP),0)-
   (SINPAY_100+SINPAY_ANT_100+SAP_100)*TAUX_DECOUVERT_AP    ASSIETE_COUTS_BR_REC_SINISTRE
   
   
    ,(COUTS_SINISTRES_100)*TAUX_COASS*nvl(decode(TAUX_CL_DTV_AP,null,TAUX_CL_MILLIARD, TAUX_CL_DTV_AP), 0)  COUTS_NT_REC_CEDES_CL
   
   ,(COUTS_SINISTRES_100)*nvl(decode(TAUX_EDP_DTV_AV,null,TAUX_EDP_MILLIARD,TAUX_EDP_DTV_AV),0)  COUTS_NT_REC_CEDES_EDP
   
   ,(COUTS_SINISTRES_100)*nvl(decode(TAUX_FAC_DTV_AP,null,TAUX_FAC_MILLIARD,TAUX_FAC_DTV_AP),0)  COUTS_NT_REC_CEDES_FAC
   
   ,(COUTS_SINISTRES_100)*TAUX_DECOUVERT_AP  COUTS_NT_REC_CEDES_DECOUVERT
   
   ,(COUTS_SINISTRES_100)*TAUX_COASS-
   (COUTS_SINISTRES_100)*TAUX_COASS*nvl(decode(TAUX_CL_DTV_AP,null,TAUX_CL_MILLIARD, TAUX_CL_DTV_AP),0)-
   (COUTS_SINISTRES_100)*nvl(decode(TAUX_EDP_DTV_AV,null,TAUX_EDP_MILLIARD,TAUX_EDP_DTV_AV),0)-
   (COUTS_SINISTRES_100)*nvl(decode(TAUX_FAC_DTV_AP,null,TAUX_FAC_MILLIARD,TAUX_FAC_DTV_AP),0)-
   (COUTS_SINISTRES_100)*TAUX_DECOUVERT_AP    ASSIETE_COUTS_NT_REC_SINISTRE
   
   
   
    ,(SINPAY_100+SINPAY_ANT_100)*TAUX_COASS*nvl(decode(TAUX_CL_DTV_AP,null,TAUX_CL_MILLIARD, TAUX_CL_DTV_AP), 0)  PAYES_BR_REC_CEDES_CL
   
   ,(SINPAY_100+SINPAY_ANT_100)*nvl(decode(TAUX_EDP_DTV_AV,null,TAUX_EDP_MILLIARD,TAUX_EDP_DTV_AV),0)  PAYES_BR_REC_CEDES_EDP
   
   ,(SINPAY_100+SINPAY_ANT_100)*nvl(decode(TAUX_FAC_DTV_AP,null,TAUX_FAC_MILLIARD,TAUX_FAC_DTV_AP),0)  PAYES_BR_REC_CEDES_FAC
   
   ,(SINPAY_100+SINPAY_ANT_100)*TAUX_DECOUVERT_AP  PAYES_BR_REC_CEDES_DECOUVERT
   
   ,(SINPAY_100+SINPAY_ANT_100)*TAUX_COASS-
   (SINPAY_100+SINPAY_ANT_100)*TAUX_COASS*nvl(decode(TAUX_CL_DTV_AP,null,TAUX_CL_MILLIARD, TAUX_CL_DTV_AP),0)-
   (SINPAY_100+SINPAY_ANT_100)*nvl(decode(TAUX_EDP_DTV_AV,null,TAUX_EDP_MILLIARD,TAUX_EDP_DTV_AV),0)-
   (SINPAY_100+SINPAY_ANT_100)*nvl(decode(TAUX_FAC_DTV_AP,null,TAUX_FAC_MILLIARD,TAUX_FAC_DTV_AP),0)-
   (SINPAY_100+SINPAY_ANT_100)*TAUX_DECOUVERT_AP    ASSIETE_PAYES_BR_REC_SINISTRE    
   
   
     ,(SINPAY_100+SINPAY_ANT_100-RECENC_100-RECENC_ANT_100)*TAUX_COASS*nvl(decode(TAUX_CL_DTV_AP,null,TAUX_CL_MILLIARD, TAUX_CL_DTV_AP), 0)  PAYES_NT_REC_CEDES_CL
   
   ,(SINPAY_100+SINPAY_ANT_100-RECENC_100-RECENC_ANT_100)*nvl(decode(TAUX_EDP_DTV_AV,null,TAUX_EDP_MILLIARD,TAUX_EDP_DTV_AV),0)  PAYES_NT_REC_CEDES_EDP
   
   ,(SINPAY_100+SINPAY_ANT_100-RECENC_100-RECENC_ANT_100)*nvl(decode(TAUX_FAC_DTV_AP,null,TAUX_FAC_MILLIARD,TAUX_FAC_DTV_AP),0)  PAYES_NT_REC_CEDES_FAC
   
   ,(SINPAY_100+SINPAY_ANT_100-RECENC_100-RECENC_ANT_100)*TAUX_DECOUVERT_AP  PAYES_NT_REC_CEDES_DECOUVERT
   
   ,(SINPAY_100+SINPAY_ANT_100-RECENC_100-RECENC_ANT_100)*TAUX_COASS-
   (SINPAY_100+SINPAY_ANT_100-RECENC_100-RECENC_ANT_100)*TAUX_COASS*nvl(decode(TAUX_CL_DTV_AP,null,TAUX_CL_MILLIARD, TAUX_CL_DTV_AP),0)-
   (SINPAY_100+SINPAY_ANT_100-RECENC_100-RECENC_ANT_100)*nvl(decode(TAUX_EDP_DTV_AV,null,TAUX_EDP_MILLIARD,TAUX_EDP_DTV_AV),0)-
   (SINPAY_100+SINPAY_ANT_100-RECENC_100-RECENC_ANT_100)*nvl(decode(TAUX_FAC_DTV_AP,null,TAUX_FAC_MILLIARD,TAUX_FAC_DTV_AP),0)-
   (SINPAY_100+SINPAY_ANT_100-RECENC_100-RECENC_ANT_100)*TAUX_DECOUVERT_AP    ASSIETE_PAYES_NT_REC_SINISTRE    
   
   
   ,graves
   
from

(

select
    ID_GARANTIE 
   ,reass_db.MD5(ID_GARANTIE||'_'||ANNEE||'_'||NUMESINI)   ID_GARANTIE_SINI
   ,ANNEE            
   ,DEAL_DATE        
   ,CODEINTE         
   ,RAISOCIN         
   ,NUMESINI         
   ,REFEINTE         
   ,NUMEPOLI         
   ,NUMEAVEN         
   ,DATEEFFE         
   ,DATEECHE         
   ,CODEASSU         
   ,NOM              
   ,CODEBRAN         
   ,CODE_BRAN_RE     
   ,LIBEBRAN         
   ,CODECATE         
   ,CATERISQ         
   ,LIBECATE         
   ,CODEGARA         
   ,CODERISQ         
   ,NATUSINI         
   ,LIBEGARA         
   ,DATESURV         
   ,DATEDECL         
   ,SINPAY           
   ,SINPAY_ANT       
   ,RECENC           
   ,RECENC_ANT       
   ,SOLDE_PAIEMENTS  
   ,EVAL             
   ,SAP              
   ,AREC             
   ,COUTS_SINISTRES  
   ,SINPAY_100       
   ,SINPAY_ANT_100   
   ,RECENC_100       
   ,RECENC_ANT_100   
   ,SOLDE_PAIEMENTS_100
   ,EVAL_100         
   ,SAP_100          
   ,AREC_100         
   ,COUTS_SINISTRES_100
   ,CODTYPSO         
   ,LIBTYPSO 
           
   --,CODTYPTR         
   --,DESITRAI         
   --,TAUXPRIM         
   ,max(decode (CODTYPTR,'CL',TAUX_CL_MILLIARD,NULL) ) TAUX_CL_MILLIARD
   ,max(decode (CODTYPTR,'XP',TAUX_EDP_MILLIARD,NULL)) TAUX_EDP_MILLIARD
   ,max(decode (CODTYPTR,'FQ',TAUX_FAC_MILLIARD,NULL)) TAUX_FAC_MILLIARD
   ,max(decode (CODTYPTR,'CL',TAUX_CL_DTV_AV   ,NULL)) TAUX_CL_DTV_AV
   ,max(decode (CODTYPTR,'XP',TAUX_EDP_DTV_AV  ,NULL)) TAUX_EDP_DTV_AV
   ,max(decode (CODTYPTR,'FQ',TAUX_FAC_DTV_AV  ,NULL)) TAUX_FAC_DTV_AV
   ,max(decode (CODTYPTR,'CL',TAUX_CL_DTV_AP   ,NULL)) TAUX_CL_DTV_AP
   ,max(decode (CODTYPTR,'XP',TAUX_EDP_DTV_AP  ,NULL)) TAUX_EDP_DTV_AP
   ,max(decode (CODTYPTR,'FQ',TAUX_FAC_DTV_AP  ,NULL)) TAUX_FAC_DTV_AP
   ,max(TAUX_DECOUVERT_AV  ) TAUX_DECOUVERT_AV
   ,max(TAUX_DECOUVERT_AP  ) TAUX_DECOUVERT_AP
   ,max(TAUX_COASS)         TAUX_COASS
    
    
    from
     TABLE_SINREASS_1 --where COUTS_SINISTRES>=10000000
     
     
   group by
   
    ID_GARANTIE 
   ,reass_db.MD5(ID_GARANTIE||'_'||ANNEE||'_'||NUMESINI) 
   ,ANNEE            
   ,DEAL_DATE        
   ,CODEINTE         
   ,RAISOCIN         
   ,NUMESINI         
   ,REFEINTE         
   ,NUMEPOLI         
   ,NUMEAVEN         
   ,DATEEFFE         
   ,DATEECHE         
   ,CODEASSU         
   ,NOM              
   ,CODEBRAN         
   ,CODE_BRAN_RE     
   ,LIBEBRAN         
   ,CODECATE         
   ,CATERISQ         
   ,LIBECATE         
   ,CODEGARA         
   ,CODERISQ         
   ,NATUSINI         
   ,LIBEGARA         
   ,DATESURV         
   ,DATEDECL         
   ,SINPAY           
   ,SINPAY_ANT       
   ,RECENC           
   ,RECENC_ANT       
   ,SOLDE_PAIEMENTS  
   ,EVAL             
   ,SAP              
   ,AREC             
   ,COUTS_SINISTRES  
   ,SINPAY_100       
   ,SINPAY_ANT_100   
   ,RECENC_100       
   ,RECENC_ANT_100   
   ,SOLDE_PAIEMENTS_100
   ,EVAL_100         
   ,SAP_100          
   ,AREC_100         
   ,COUTS_SINISTRES_100
   ,CODTYPSO         
   ,LIBTYPSO 

     



)i,

(select
distinct ANNEE,CODEINTE,NUMESINI, graves

from actuary.tb_sinistre_grave where graves=1

)j

where i.CODEINTE=j.CODEINTE(+)
  and i.NUMESINI=j.NUMESINI(+)
  and i.ANNEE=j.ANNEE(+)
)
--where
--ASSIETE_SINISTRE>=10000000
)
;


--delete from reass_db.TABLE_SINREASS_3 ;

---insert into reass_db.TABLE_SINREASS_3 
drop table reass_db.TABLE_SINREASS_3 ;
create table  reass_db.TABLE_SINREASS_3 as ---table qui ramene pour chaque sinitres les traités XS associés et les calculs XS


(select
  
   i.*
  ,j.NUMETRAI
  ,j.NUMETRAN
  ,j.desitran
  ,j.NUMESECT
  ,j.PRIORITE
  ,j.PORTEE
  ,j.TAUX_XS
  
   ,case when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  PAYES_BR_REC_CEDES_XS
  
  ,case when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  PAYES_NT_REC_CEDES_XS
  

 ,case when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end -
 case when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end SAP_BR_REC_CEDES_XS 
 
  
  ,case when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end -
 case when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end SAP_NT_REC_CEDES_XS 
 
 
 
 
  ,case when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  COUTS_BR_REC_CEDES_XS

  ,case when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  COUTS_NT_REC_CEDES_XS




from
(select *from TABLE_SINREASS_2)i,
(select distinct * FROM TRAITE_XS_0622) j

where 
i.CODEBRAN=j.CODEBRAN(+) 
and  i.CODEGARA=j.CODEGARA(+) 
and i.CODECATE=j.CODECATE(+)
and i.annee=j.exertrai(+)

)

;
----------- Redesign reass_db.TABLE_SINREASS_3----------------------
drop table reass_db.TABLE_SINREASS_4;
create table  reass_db.TABLE_SINREASS_4 as
(select
   i.ANNEE
   ,'XS' CODTYPTR
   --,DESITRAI
   ,i.CODEBRAN
   ,i.CODE_BRAN_RE 
   ,i.SINPAY               
   ,i.SINPAY_ANT 
   ,i.RECENC               
   ,i.RECENC_ANT 
   ,i.EVAL                 
   ,i.SAP                  
   ,i.AREC  
   ,i.COUTS_SINISTRES
   ,i.SINPAY_100           
   ,i.SINPAY_ANT_100 
   ,i.RECENC_100           
   ,i.RECENC_ANT_100 
   ,i.EVAL_100             
   ,i.SAP_100 
   ,i.AREC_100             
   ,i.COUTS_SINISTRES_100 
   ,j.desitran
  --,j.NUMETRAI
  --,j.NUMETRAN
  --,j.desitran
  --,j.NUMESECT
  --,j.PRIORITE
  --,j.PORTEE
  ,j.TAUX_XS
 -- ,decode(i.couts_sinistres_100,0,0,(case when ASSIETE_COUTS_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_SINISTRE-j.PRIORITE
 --       when ASSIETE_COUTS_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 --else 0
 -- end   )/i.couts_sinistres_100) TAUX_XS2
  ,i.graves
  
 ,case when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  PAYES_BR_REC_CEDES_XS
  
  ,case when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  PAYES_NT_REC_CEDES_XS
  

 ,case when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end -
 case when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end SAP_BR_REC_CEDES_XS 
 
  
  ,case when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end -
 case when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end SAP_NT_REC_CEDES_XS 
 
 
 
 
  ,case when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  COUTS_BR_REC_CEDES_XS

  ,case when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  COUTS_NT_REC_CEDES_XS
  --case 
  --   when CODEBRAN=4 and SOLDE_PAIEMENTS>=10000000 then 1
  --   when (CODEBRAN=2 or CODEBRAN=3)  and SOLDE_PAIEMENTS>=10000000 then 1 
  --   when CODEBRAN=7 and SOLDE_PAIEMENTS>=10000000 then 1
  --   when CODEBRAN in (51,52,53,54) and SOLDE_PAIEMENTS>=10000000 then 1 else 0 END graves


from
(select *from reass_db.TABLE_SINREASS_2)i,
(select distinct * FROM reass_db.TRAITE_XS_0622)j

where 
i.CODEBRAN=j.CODEBRAN(+) 
and  i.CODEGARA=j.CODEGARA(+) 
and i.CODECATE=j.CODECATE(+)
and i.annee=j.exertrai(+))

;

drop table sinistre_cedes_reass;
create table reass_db.sinistre_cedes_reass as
select annee,CODTYPTR,desitran,codebran,CODE_BRAN_RE,sum(sinpay) sinpay, sum(sinpay_ant) sinpay_ant, sum(recenc) recenc,--,DESITRAI,
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
  sum(couts_sinistres_100*graves) couts_sinistres_graves_100
  
  ,sum(0) sinpay_cess
  ,sum(0) sinpay_ant_cess
  ,sum(PAYES_BR_REC_CEDES_XS) PAYES_BR_REC_CESS
  ,sum(0) recenc_cess
  ,sum(0) recenc_ant_cess
  ,sum(PAYES_NT_REC_CEDES_XS) PAYES_NT_REC_CESS
  ,sum(0) eval_cess
 -- ,sum(0) sap_cess
  ,sum(SAP_BR_REC_CEDES_XS) SAP_BR_REC_CESS
  ,sum(0) arec_cess
  ,sum(SAP_NT_REC_CEDES_XS) SAP_NT_REC_CESS
 -- ,0 couts_sinistres_cess
  ,sum(COUTS_BR_REC_CEDES_XS) COUTS_BR_REC_CESS
  ,sum(COUTS_NT_REC_CEDES_XS) COUTS_NT_REC_CESS
  
  
   ,sum(0) sinpay_graves_cess
  ,sum(0) sinpay_ant_graves_cess
  ,sum(PAYES_BR_REC_CEDES_XS*graves) PAYES_BR_REC_GRAVES_CESS
  ,sum(0) recenc_graves_cess
  ,sum(0) recenc_ant_graves_cess
  ,sum(PAYES_NT_REC_CEDES_XS*graves) PAYES_NT_REC_GRAVES_CESS
  ,sum(0) eval_graves_cess
 -- ,sum(0) sap_graves_cess
  ,sum(SAP_BR_REC_CEDES_XS*graves) SAP_BR_REC_GRAVES_CESS
  ,sum(0) arec_graves_cess
  ,sum(SAP_NT_REC_CEDES_XS*graves) SAP_NT_REC_GRAVES_CESS
 -- ,0 couts_sinistres_cess
  ,sum(COUTS_BR_REC_CEDES_XS*graves) COUTS_BR_REC_GRAVES_CESS
  ,sum(COUTS_NT_REC_CEDES_XS*graves) COUTS_NT_REC_GRAVES_CESS
  
  
  from reass_db.TABLE_SINREASS_4
  group by annee,CODTYPTR,codebran,desitran,CODE_BRAN_RE
;
commit;











--------edp
--delete from reass_db.TABLE_SINREASS_3_edp ;

---insert into reass_db.TABLE_SINREASS_3_edp 
drop table reass_db.TABLE_SINREASS_3_edp ;
create table  reass_db.TABLE_SINREASS_3_edp as ---table qui ramene pour chaque sinitres les traités XS associés et les calculs XS


(select
  
   i.*
  ,j.NUMETRAI
  ,j.NUMETRAN
  ,j.desitran
  ,j.NUMESECT
  ,j.PRIORITE
  ,j.PORTEE
  ,j.TAUX_XS
  
   ,case when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  PAYES_BR_REC_CEDES_XS
  
  ,case when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  PAYES_NT_REC_CEDES_XS
  

 ,case when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end -
 case when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end SAP_BR_REC_CEDES_XS 
 
  
  ,case when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end -
 case when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end SAP_NT_REC_CEDES_XS 
 
 
 
 
  ,case when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  COUTS_BR_REC_CEDES_XS

  ,case when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  COUTS_NT_REC_CEDES_XS




from
(select i.*

  ,case when CODE_BRAN_RE=20    and MONT_SMP <=1000000000 then '[0-1 000 M] INC'
     when  CODE_BRAN_RE=20 and MONT_SMP <=7000000000 then ']1 000 M -3 000 M] INC'
      when  CODE_BRAN_RE=20 and MONT_SMP <=7000000000 then ']3 000 M -7 000 M] INC'
     when  CODE_BRAN_RE=20 and MONT_SMP <=14000000000 then ']7 000 M -14 000 M] INC'
      when CODE_BRAN_RE=20  and MONT_SMP >14000000000 then 'sup à 14 000 M INC'
     
       when CODE_BRAN_RE=22 and MONT_SMP <=500000000 then '[0-500 M] BDM - TRI'
      when  CODE_BRAN_RE=22 and MONT_SMP <=2000000000 then ']500 M -2 000 M] BDM - TRI'
      when  CODE_BRAN_RE=22 and MONT_SMP <=4000000000 then ']2 000 M -4 000 M] BDM - TRI'
      when  CODE_BRAN_RE=22 and MONT_SMP >4000000000 then 'sup à 4 000 M BDM - TRI'
      
      when  CODE_BRAN_RE=21 and MONT_SMP <=1000000000 then '[0-1 000 M] TRM - TRC'
       when CODE_BRAN_RE=21 and MONT_SMP <=4000000000 then ']1 000 M -4 000 M] TRM - TRC'
       when CODE_BRAN_RE=21 and MONT_SMP <=8000000000 then ']4 000 M -8 000 M] TRM - TRC'
       when CODE_BRAN_RE=21 and MONT_SMP >8000000000 then 'sup à 8 000 M TRM - TRC'
       
       when  CODE_BRAN_RE=23 and MONT_SMP <=250000000 then '[0-250 M] RD'
        when CODE_BRAN_RE=23 and MONT_SMP <=1000000000 then ']250 M -1 000 M] RD'
        when CODE_BRAN_RE=23 and MONT_SMP <=2000000000 then ']1 000 M -2 000 M] RD'
        when CODE_BRAN_RE=23 and MONT_SMP >2000000000 then 'sup à 2 000 M RD'

      else 'NONE'
      end Tranche_Retention

from 
(select *from reass_db.TABLE_SINREASS_2)i,

(select  distinct CODEINTE,NUMEPOLI,nvl(NUMEAVEN,0) NUMEAVEN ,CODERISQ
,max(MONT_SMP) MONT_SMP
--,MONT_SMP 
from ORASS_V6.SMP_RISQUE

group by
  CODEINTE,NUMEPOLI,nvl(NUMEAVEN,0)  ,CODERISQ
)j


where
      i.CODEINTE=  j.CODEINTE(+) 
  and i.NUMEPOLI=J.NUMEPOLI(+) 
  and i.NUMEAVEN=J.NUMEAVEN(+) 
  and i.CODERISQ=J.CODERISQ(+) 
  )i,
(select distinct * FROM TRAITE_XS_0622) j

where 
i.CODEBRAN=j.CODEBRAN(+) 
and  i.CODEGARA=j.CODEGARA(+) 
and i.CODECATE=j.CODECATE(+)
and i.annee=j.exertrai(+)

)

;



drop table reass_db.TABLE_SINREASS_4_EDP;
create table  reass_db.TABLE_SINREASS_4_EDP as
(select
distinct
   i.ANNEE
   ,'XS' CODTYPTR
   --,DESITRAI
   ,i.CODEBRAN
   ,i.CODE_BRAN_RE 
   ,i.Tranche_Retention
   ,i.SINPAY               
   ,i.SINPAY_ANT 
   ,i.RECENC               
   ,i.RECENC_ANT 
   ,i.EVAL                 
   ,i.SAP                  
   ,i.AREC  
   ,i.COUTS_SINISTRES
   ,i.SINPAY_100           
   ,i.SINPAY_ANT_100 
   ,i.RECENC_100           
   ,i.RECENC_ANT_100 
   ,i.EVAL_100             
   ,i.SAP_100 
   ,i.AREC_100             
   ,i.COUTS_SINISTRES_100 
   ,j.desitran
  --,j.NUMETRAI
  --,j.NUMETRAN
  --,j.desitran
  --,j.NUMESECT
  --,j.PRIORITE
  --,j.PORTEE
  ,j.TAUX_XS
 -- ,decode(i.couts_sinistres_100,0,0,(case when ASSIETE_COUTS_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_SINISTRE-j.PRIORITE
 --       when ASSIETE_COUTS_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 --else 0
 -- end   )/i.couts_sinistres_100) TAUX_XS2
  ,i.graves
  
 ,case when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  PAYES_BR_REC_CEDES_XS
  
  ,case when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  PAYES_NT_REC_CEDES_XS
  

 ,case when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end -
 case when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end SAP_BR_REC_CEDES_XS 
 
  
  ,case when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end -
 case when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_PAYES_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0 end SAP_NT_REC_CEDES_XS 
 
 
 
 
  ,case when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_BR_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  COUTS_BR_REC_CEDES_XS

  ,case when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE< j.PORTEE then ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE
        when ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>0  and  ASSIETE_COUTS_NT_REC_SINISTRE-j.PRIORITE>=j.PORTEE then j.PORTEE
 else 0
  end  COUTS_NT_REC_CEDES_XS
  --case 
  --   when CODEBRAN=4 and SOLDE_PAIEMENTS>=10000000 then 1
  --   when (CODEBRAN=2 or CODEBRAN=3)  and SOLDE_PAIEMENTS>=10000000 then 1 
  --   when CODEBRAN=7 and SOLDE_PAIEMENTS>=10000000 then 1
  --   when CODEBRAN in (51,52,53,54) and SOLDE_PAIEMENTS>=10000000 then 1 else 0 END graves


from
(
select k.*

  ,i.Tranche_Retention

from 
(select *from reass_db.TABLE_SINREASS_2)k,

(select distinct CODEINTE cod,NUMEPOLI n,NUMEAVEN nu,CODERISQ co,CODEGARA gar,
        -- CODE_BRAN_RE,
         --LIBEL_BRAN_RE,
         Tranche_Retention
   from reass_db.PRIME_CEDEE_ANT2021_EDP)i

where
     k.CODEINTE=nvl(i.cod(+),0)
and  k.NUMEPOLI=nvl(i.n(+)    ,0)
and  k.NUMEAVEN=nvl(i.nu(+)   ,0)
and  k.CODERISQ=nvl(i.co(+)   ,0)
and  k.CODEGARA=nvl(i.gar(+)  ,0)
  )i,
(select distinct * FROM reass_db.TRAITE_XS_0622)j

where 
i.CODEBRAN=j.CODEBRAN(+) 
and  i.CODEGARA=j.CODEGARA(+) 
and i.CODECATE=j.CODECATE(+)
and i.annee=j.exertrai(+))

;

drop table sinistre_cedes_reass_edp;
create table reass_db.sinistre_cedes_reass_edp as
select annee,CODTYPTR,desitran,codebran,CODE_BRAN_RE,tranche_retention,sum(sinpay) sinpay, sum(sinpay_ant) sinpay_ant, sum(recenc) recenc,--,DESITRAI,
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
  sum(couts_sinistres_100*graves) couts_sinistres_graves_100
  
  ,sum(0) sinpay_cess
  ,sum(0) sinpay_ant_cess
  ,sum(PAYES_BR_REC_CEDES_XS) PAYES_BR_REC_CESS
  ,sum(0) recenc_cess
  ,sum(0) recenc_ant_cess
  ,sum(PAYES_NT_REC_CEDES_XS) PAYES_NT_REC_CESS
  ,sum(0) eval_cess
 -- ,sum(0) sap_cess
  ,sum(SAP_BR_REC_CEDES_XS) SAP_BR_REC_CESS
  ,sum(0) arec_cess
  ,sum(SAP_NT_REC_CEDES_XS) SAP_NT_REC_CESS
 -- ,0 couts_sinistres_cess
  ,sum(COUTS_BR_REC_CEDES_XS) COUTS_BR_REC_CESS
  ,sum(COUTS_NT_REC_CEDES_XS) COUTS_NT_REC_CESS
  
  
   ,sum(0) sinpay_graves_cess
  ,sum(0) sinpay_ant_graves_cess
  ,sum(PAYES_BR_REC_CEDES_XS*graves) PAYES_BR_REC_GRAVES_CESS
  ,sum(0) recenc_graves_cess
  ,sum(0) recenc_ant_graves_cess
  ,sum(PAYES_NT_REC_CEDES_XS*graves) PAYES_NT_REC_GRAVES_CESS
  ,sum(0) eval_graves_cess
 -- ,sum(0) sap_graves_cess
  ,sum(SAP_BR_REC_CEDES_XS*graves) SAP_BR_REC_GRAVES_CESS
  ,sum(0) arec_graves_cess
  ,sum(SAP_NT_REC_CEDES_XS*graves) SAP_NT_REC_GRAVES_CESS
 -- ,0 couts_sinistres_cess
  ,sum(COUTS_BR_REC_CEDES_XS*graves) COUTS_BR_REC_GRAVES_CESS
  ,sum(COUTS_NT_REC_CEDES_XS*graves) COUTS_NT_REC_GRAVES_CESS
  
  
  from reass_db.TABLE_SINREASS_4_EDP
  group by annee,CODTYPTR,codebran,desitran,CODE_BRAN_RE,tranche_retention
;
commit;