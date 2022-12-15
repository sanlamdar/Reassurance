def spoolfile="&&1" heading "nom du fichier temporaire de sortie (avec extension)"
def spoolformat="&&2" heading "format du fichier (txt ou xls ou html)"
   
rem DEBUT PARAMETRES UTILISATEURS 
    def P_date__au="&&3" heading "Date invententaire"
    def P_date__du="&&4" heading "Date du jour"


      
rem FIN PARAMETRES UTILISATEURS 
rem ---------------------------------------------------  ABI ORSYS ------------------------

Set echo off newpage 0 feedback off verify off

ttitle off
set termout off

rem initialisations
col fileextension noprint new_value fileextension
col markup noprint new_value markup
col now noprint format a16 new_value now
col today noprint new_value today
select decode(lower('&&spoolformat'),'xls','xls','html','html','txt') fileextension,
       decode(lower('&&spoolformat'),'xls', 'markup html on spool on',
                                     'html','markup html on spool on',
                                            'markup html off spool off') markup,
       to_char(sysdate,'DD/MM/YYYY') today,
       to_char(sysdate,'DD/MM/YYYY-HH24:MI') now
  from dual
/
set pause off &&markup

rem fin des initialisations
rem ---------------------------------------------------------------------

rem c’est ici qu’on construit le report, laisser une seule ligne blanche après

set verify off feedback off trimspool on heading on

    def v_TYPEREGR_1='RE'
    def v_TYPEREGR_2='RE'

clear columns
ttitle off

rem initialisation des variables calculees:
set heading off
select abresoci  v_cie,
       '&&P_date__du'   v_Per_du,  '&&P_date__du'  v_Per_Au,
       to_char(sysdate,'dd/mm/yyyy hh24:mi') v_Today
  from societe
/


col annee2 noprint new_value annee2


select to_char(to_date('&&P_date__au','DD/MM/YYYY'),'YYYY') annee2
  from dual
/


rem colonnes pour initialiser des variables:
col per_du   format dd/mm/yyyy     noprint new_value v_per_du
col per_au   format dd/mm/yyyy     noprint new_value v_per_au
col cie                            noprint new_value v_cie
col today    format dd/mm/yyyy     noprint new_value v_today

set heading on
set linesize 1000
set pagesize 50000
alter session set nls_date_format='DD/MM/YYYY';


col  numepoli format 999999999999999 print
col  numequit format 999999999999999 print
col  Codecate    format 9999999999 print
col  dateeffe format dd/mm/yyyy  print
col  datecomp  format dd/mm/yyyy  print
col  codeinte   format 999999999999999 print
col  taux_fac    format 9999 print
col  raissoin    format a100 print
col  "mont_smp_INC"   format 999999999999999 print
col  "mont_smp_TRM_TRC"  format 999999999999999 print
col  "mont_smp_BDM_TRI" format 999999999999999 print
col  "mont_smp_RD" format 999999999999999 print
col  "mont_smp_Globale" format 999999999999999 print
col  "activite"    format a90 print
col  nom_assure    format a100 print
col  liberisq    format a90 print
col  classerisque    format a5 print
col  coderisq    format 99999999 print
col  NUMEAVEN    format 999999999999999 print

ttitle    left  cie                center  "PREC REASS"       right "Page " sql.pno skip -
       left ""                  center  "________________________________"       right  today skip -
       left " " skip

SELECT /*+ use_hash(pr) */
      DECODE('&&v_TYPEREGR_1','RE',to_char(codereas),'TR',exertrai||'-'||numetrai||'-'||to_char(dateeffe,'DD/MM/YYYY'),'P',CODECATE,'B',CODBRARE,'BP',codebran,'C',NVL(CATECOMP,CODECATE),'R',NVL(CATEREAS,CODECATE)) CODE,
      DECODE('&&v_TYPEREGR_2','RE',to_char(raisocre),'TR',desitrai,'P',LIBECATE,'B',libbrare,'BP',libebran,'C',nvl(libcatre_c,LIBECATE),'R',nvl(libcatre_r,libecate))  LIBELLE,
      codebran,
      libebran,
      numequit,
      codeinte,
      numepoli,
      numeaven,
      numepoli||'/'||codeinte||'/'||nvl(numeaven,0) refer,
      codecate,
      dateeffe,
      dateeche,
      exertrai,
      annetrai,
      codtyptr,
      raissoci,
      sum(primenett) primenett,
      sum(montcess) montcess,
      sum(montcomm) montcomm
   FROM(
      select /*+ use_hash(pr) */
      r.codereas,
      pr.numequit,
      pr.codeinte,
      pr.numepoli,
      decode(pr.numeaven,9999,null,pr.numeaven) numeaven,
      pr.dateffqu dateeffe,
      pr.datechqu dateeche,
      pr.numetrai,
      pr.exertrai,
      pr.dateeffe datefftr,
      pr.numesect,
      pr.numetran,
      pr.codecate,
      decode(pr.flagannu,'O','*',' ') flagannu,
      pr.annetrai,
      pr.peritrai,
      pr.tauxprim,
      pr.coderisq,
      pr.codtyptr,
      pr.tauxprim*nvl(r.tauxreas,0)/100 tauxcess,
      pr.capibrut*nvl(r.tauxreas,0)/100 capitaux,
      pr.mont_smp*nvl(r.tauxreas,0)/100 mont_smp,
      pr.primbrut*nvl(r.tauxreas,0)/100 primenett,
      nvl(pr.montprim,0)*nvl(r.tauxreas,0)/100 montcess,
      nvl(pr.montcomm,0)*nvl(r.tauxreas,0)/100 montcomm,
      nvl(r.tauxreas,100)*nvl(r.tauxcomm,100)/100 tauxcomm,
      pr.desitrai,
      rea.raisocre,
      rea.adrereas,
      pr.libecate,
      pr.libebran,
      pr.codebran,
      pr.raissoci,
      pr.catereas,
      pr.catecomp,
      pr.libbrare,
      pr.libcatre_c,
      pr.libcatre_r,
      pr.codbrare
    from  reassureur_tranche r, reass.VR_PRIME_REASSURANCE pr  ,reassureur rea
    where  pr.anneproj between 1900 and '&&annee2'
           and    pr.datetrai between '01/01/1900' and '&&P_date__au'
--           and (:v_codeinte  is null or
--            pr.codeinte = to_number(:v_codeinte) or
--            pr.codeinte in (select codeinte from intermediaire
--                           where lieninte = to_number(:v_codeinte)))
--        and   (:v_numetrai is null or pr.numetrai = :v_numetrai )
--        and   (:v_exertrai is null or pr.exertrai = :v_exertrai )
--        and   (:v_codtyptr is null or pr.codtyptr = :v_codtyptr or (:v_codtyptr='FAC' and pr.codtyptr in('FX','FQ')))
--        and   (:v_codereas is null or r.codereas = :v_codereas)
        and    r.numetrai = pr.numetrai
        and    r.exertrai = pr.exertrai
        and    r.dateeffe = pr.dateeffe
        and    r.numesect = pr.numesect
        and    nvl(r.numetran,0) = nvl(pr.numetran,0)
        and    rea.codereas = r.codereas
         and (('&&P_date__au' > pr.dateffqu   and  '&&P_date__au' < pr.datechqu)
   or ('&&P_date__au' < pr.dateffqu   and  '&&P_date__au' < pr.datechqu ) )
        ) k
--        where k.codtyptr IN ('CL','FO','FQ','OP','OQ','FX','XP','QP')
       where k.codtyptr IN ('FQ')
     Group by
      DECODE('&&v_TYPEREGR_1','RE',to_char(codereas),'TR',exertrai||'-'||numetrai||'-'||to_char(dateeffe,'DD/MM/YYYY'),'P',CODECATE,'B',CODBRARE,'BP',codebran,'C',NVL(CATECOMP,CODECATE),'R',NVL(CATEREAS,CODECATE)),
      DECODE('&&v_TYPEREGR_2','RE',to_char(raisocre),'TR',desitrai,'P',LIBECATE,'B',libbrare,'BP',libebran,'C',nvl(libcatre_c,LIBECATE),'R',nvl(libcatre_r,libecate)),
       codebran,
       libebran,
       numequit,
      codeinte,
      numepoli,
      numeaven,
      codecate,
      dateeffe,
      dateeche,
      exertrai,
      annetrai,
      codtyptr,
       raissoci
     order by k.codeinte, k.numepoli,k.numeaven, nvl(k.numequit,0)
  
spool &&spoolfile
/
spool off

rem ----------------------------------------------------------------------

set markup html off spool off

rem fin. sortir de sql*plus.
exit



