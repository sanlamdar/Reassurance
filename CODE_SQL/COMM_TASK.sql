--6AC93AF19C03F822FB375775490B3A6F
--drop  table reass_db.for_check
--create table reass_db.for_check as
select t.codeinte
       ,t.numepoli
       ,t.numeaven
       ,t.codebran
       ,t.libebran
      -- ,codegara
       --,codecate
      -- ,libecate
      -- ,coderisq
       ,'AVANT' ETAT
     
       ,sum(t.PRM_NT_100*t.taux_com_cl*t.taux_coass) COM_CL
       
       ,sum(t.PRM_NT_100*t.taux_com_edp) COM_EDP
   
       ,sum(t.PRM_NT_100*t.taux_com_fac) COM_FAC
       
       from
(select  i.ID_GARANTIE          
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
       ,l.tauccl TAUX_COM_CL
       ,case when m.S_P<=0.43 then 0.325 
             when m.S_P>=0.65  then 0.325 
             else 0.275 end TAUX_COM_EDP
       
       ,decode(SOURCE,'NOS CALCULS',decode(i.DATE_INS,to_date('06/07/2022'),j.TAUX_COM_AVT,j.TAUX_COM_APR),0) TAUX_COM_FAC
       ,i.STATUT               
 
from REASS_DB.SAT_OUTPUT_REASS i,
REASS_DB.BASE_COM l,
(select b.*,decode(prime_acquise,0,0,charge/prime_acquise) S_P from
                                                    (select codebran
                                                            ,max(branche1)branche1
                                                            ,max(branche2) branche2
                                                            ,annee
                                                            ,sum(prime_acquise) prime_acquise
                                                            ,sum(charge) charge 
                                                            from actuary.tb_analyse_sp f
                                                            group by codebran, annee)b)m,

(select K.ID_REASSURANCE
        , sum(decode(k.DATE_INS,to_date('06/07/2022'),K.TAUX_COM_REASSURANCE*K.PART_REASSUREUR,0)) TAUX_COM_AVT
        ,sum(decode(K.STATUT,'ACTIF',K.TAUX_COM_REASSURANCE*K.PART_REASSUREUR,0)) TAUX_COM_APR 
from REASS_DB.SAT_LINK_REASS_REASSUREUR k 
group by K.ID_REASSURANCE)j
where I.ID_REASSURANCE_FAC=J.ID_REASSURANCE(+) 
  and i.CODEINTE=l.CODEINTE(+) 
  and i.CODEBRAN=L.BR(+) 
  and i.codebran=m.codebran(+) 
  and m.annee=2021)t
where t.DATE_INS = to_date('06/07/2022') and t.source = 'NOS CALCULS'
group by t.codeinte
       ,t.numepoli
       ,t.numeaven
       ,t.codebran
       ,t.libebran
       ,taux_com_fac
       ,taux_com_edp
       ,taux_com_cl

union all

select t.codeinte
       ,t.numepoli
       ,t.numeaven
       ,t.codebran
       ,t.libebran
      -- ,codegara
       --,codecate
      -- ,libecate
      -- ,coderisq
       ,'APRES' ETAT
      
       ,sum(t.PRM_NT_100*t.taux_com_cl*t.taux_coass) COM_CL
      
       ,sum(t.PRM_NT_100*t.taux_com_edp) COM_EDP

       ,sum(t.PRM_NT_100*t.taux_com_fac) COM_FAC
       
       from
(select  i.ID_GARANTIE          
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
       ,l.tauccl TAUX_COM_CL
       ,case when m.S_P<=0.43 then 0.325 
             when m.S_P>=0.65  then 0.325 
             else 0.275 end TAUX_COM_EDP
       
       ,decode(SOURCE,'NOS CALCULS',decode(i.DATE_INS,to_date('06/07/2022'),j.TAUX_COM_AVT,j.TAUX_COM_APR),0) TAUX_COM_FAC
       ,i.STATUT               
 
from REASS_DB.SAT_OUTPUT_REASS i,
REASS_DB.BASE_COM l,
(select b.*,decode(prime_acquise,0,0,charge/prime_acquise) S_P from
                                                    (select codebran
                                                            ,max(branche1)branche1
                                                            ,max(branche2) branche2
                                                            ,annee
                                                            ,sum(prime_acquise) prime_acquise
                                                            ,sum(charge) charge 
                                                            from actuary.tb_analyse_sp f
                                                            group by codebran, annee)b) m,

(select K.ID_REASSURANCE
        , sum(decode(k.DATE_INS,to_date('06/07/2022'),K.TAUX_COM_REASSURANCE*K.PART_REASSUREUR,0)) TAUX_COM_AVT
        ,sum(decode(K.STATUT,'ACTIF',K.TAUX_COM_REASSURANCE*K.PART_REASSUREUR,0)) TAUX_COM_APR 
from REASS_DB.SAT_LINK_REASS_REASSUREUR k 
group by K.ID_REASSURANCE)j
where I.ID_REASSURANCE_FAC=J.ID_REASSURANCE(+) 
  and i.CODEINTE=l.CODEINTE(+) 
  and i.CODEBRAN=L.BR(+) 
  and i.codebran=m.codebran(+) 
  and m.annee=2021)t
where t.STATUT='ACTIF' and t.source = 'NOS CALCULS'
group by t.codeinte
       ,t.numepoli
       ,t.numeaven
       ,t.codebran
       ,t.libebran
       ,taux_com_fac
       ,taux_com_edp
       ,taux_com_cl






