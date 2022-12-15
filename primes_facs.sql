
		rem - nom - primes_facs.sql
rem - designation - Comparaison Production <-> Comptabilite.
rem - version - 1.0
rem - auteur - roger.costandi@saham-it.com
rem - historique - 2016-10-21 version 1.0

def spoolfile="&&1" heading "nom du fichier temporaire de sortie (avec extension)"
def spoolformat="&&2" heading "format du fichier (txt ou xls ou html)"
     
   
rem DEBUT PARAMETRES UTILISATEURS 
  def debut="&&3" heading "Date de debut)"
   def fin="&&4" heading "Date de fin"
   def Annee="&&5" heading "Annee"  
    
 
   
   
rem FIN PARAMETRES UTILISATEURS 

rem ---------------------------------------------------------------------------

set echo off newpage 0 feedback off verify off

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

-- Rapprochement entre la compta et la production
--
-- Comme tout processus de rapprochement qui se respecte, on isole totalement ( = aucune jointure ):
--   les tables techniques (quittance, quittance_coassurance)
--   des tables comptables (mouvement_comptable).
-- Le seul point où les deux se rejoignent est le schéma de comptabilisation (schema_comptable_production)
--
-- RC 20101010
-- 
-- Principe de ce rapprochement:
-- * on sélectionne toutes les quittances émises (datecomp) dans la période considérée
--   (y compris si elles sont annulées), quelque soit leur type (mais pas les projets)
-- * on y ajoute (en inversant le sens) les quittances annulées directement,
--   c'est à dire sans réémission de quittance mais en renseignant datsorqu et piecannu
--   (en prenant en compte leur date de sort qui est comparée à la période de sélection)
-- * exemple: (période considérée: juin 201X)
--   quittance N° 8  datecomp en mai 201X --> non sélectionnée
--   quittance N° 9  datecomp en mai 201X et datsorqu en juin 201X --> seule l'annulation est sélectionnée
--   quittance N° 10 datecomp en juin 201X --> sélectionnée
--   quittance N° 11 datecomp en juin 201X et datsorqu en juin 201X --> sélectionnée 2 fois, dont 1 en sens inverse
--   quittance N° 12 datecomp en juin 201X et datsorqu en août 201X --> émission sélectionnée, mais pas l'annulation
-- * de cette manière, si une quittance est annulée sur une période différente de son émission, tout
--   reste cohérent (et conforme à ce qui est comptabilisé)
-- * ensuite on sélectionne les mouvements comptables passés dans la période considérée et 
--   sur les comptes mentionnés dans le schéma comptable
 
-- la différence représente les écarts:
-- - montants différents
-- - quittances non comptabilisées
-- - mouvements comptables sans équivalent dans les quittances
--
-- Dans le tableau résultat:
-- les données précédées d'un libellé ("quittance",...) proviennent de la production,
-- celles qui sont précédées d'un numéro de compte (10 chiffres) proviennent de la compta.
--
-- les signes à appliquer aux différentes colonnes de montants (production) proviennent de type_quittance.
-- on les inverse dans le cas des annulations sans quittance séparée.
--
-- Les montants en provenance de la compta sont calculés ainsi:  CREDIT - DEBIT
-- (car le Chiffre d'Affaires est plutôt créditeur, ce serait l'inverse dans le cas des sinistres)
-- Les montants en provenance de la production sont présentés avec un signe opposé à la compta,
-- pour aboutir à une somme algébrique de zéro s'il n'y a pas d'écart.
--
-- En compta, les mouvements saisis sur d'autres journaux que le journal de production sont identifiés
-- par des "******" dans la colonne "NOTE": en effet, ils représentent des saisies manuelles
-- (ou mal paramétrées) et provoquent des écarts.
--
-- Autres cas d'écart rencontrés:
-- - génération comptable non effectuée
-- - erreurs d'arrondis (coassurance)
--
-- La version actuelle des schémas utilise un seul journal comptable pour tous les mouvements,
-- d'où la simplification "(select distinct codejour from schema_comptable_production)" pour
-- identifier le journal en compta
--
-- les catégories sont extraites des numéros de comptes du schéma selon deux règles:
--   * compte 70201xxx00 (si dernier chiffre = "0")
--   * ou compte 7020101xxx
--   où "xxx" représente le numéro de catégorie
--   (le deuxième cas n'a été rencontré que dans le schéma Vie de Madagascar)
--   le numéro de branche est le 1er chiffre de la catégorie ci-dessus


-- bornes de la sélection (datecomp, datsorsi, datepiec):
 


set linesize 250 pagesize 50000 verify off feedback off

  codeinte  format a15
  numepoli format a15
  codecate format a15
  libecate format a15
  codeassu format a15
  CODEGARA format a15
  raissoci format a30
 numequit format 9999999999999
  PN format 9999999999999
  PN_g format 9999999999999
  Mtt_REC format 9999999999999
  Eval_TT format 9999999999999
  Eval_TT_gl format 9999999999999
  Eval_hono format 9999999999999
  Eval_hono_gl format 9999999999999
  Eval_PRINC format 9999999999999
  Eval_PRINC_gl format 9999999999999
  Mtt_reg format 9999999999999
  Mtt_reg_gl format 9999999999999
  S_P format 9999999999999
  S_P_gl format 9999999999999
clear break
break on branche dup skip 1 on categ dup skip 1 on journ dup skip 1 on report skip 2
clear compute
COMPUTE sum label "Total" OF prime           ON branche categ report
COMPUTE sum label "Total" OF accessoires     ON branche categ report
COMPUTE sum label "Total" OF commission      ON branche categ report
COMPUTE sum label "Total" OF aperition_coass ON branche categ report

col compagnie new_value compagnie
select raissoci compagnie from societe;

 ttitle left COMPAGNIE " - Primes Facs par Reassureur     "   '&&Annee'   "     " FIN " - Edite le &&now"

-- rapprochement

 

SELECT k.codereas,
      rr.raisocre,
      numequit,
      codeinte,
      numepoli,
      numeaven,
      dateeffe,
      dateeche,
      numetrai,
      exertrai,
      numesect,
      numetran,
      codecate,
      flagannu,
      annetrai,
      peritrai,
      datetrai,
      --tauxprim,
      coderisq,
      tauxreas,
      (select v.libevill from  ville v
      where rr.codevill = v.codevill) villreas,
      (select desitrai from traite
       where exertrai = k.exertrai
         and numetrai = k.numetrai
         and dateeffe = k.datefftr) desitrai,
      (select libeperi from  V_PERIODE_REASSURANCE
       where peritrai= k.peritrai) libeperi,
      (select ' '||raissoci
         from  assure a,  police p
         where p.codeinte = k.codeinte
         and   p.numepoli = k.numepoli
         and   a.codeassu = p.codeassu) raissoci,
      capitaux,
      mont_smp,
      primenett,
      montcess,
      montcomm,
      tauxcess,
      decode(nvl(k.primenett,0),0,0,decode(nvl(k.montcess,0),0,0,nvl(montcomm,0)*100/montcess)) tauxcomm,
      montcess-montcomm cessnett,
      (select codebran from  categorie where codecate=k.codecate) codebran,
      (select libebran from  categorie c,  branche b where c.codebran=b.codebran and c.codecate=k.codecate) libebran,
      codtyptr
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
      pr.datetrai,
      pr.tauxprim,
      pr.coderisq,
      pr.codtyptr,
      pr.tauxprim tauxcess,
      r.tauxreas,
      max(pr.capibrut) capitaux,
      max(pr.mont_smp) mont_smp,
      sum(pr.primbrut) primenett,
      sum(nvl(pr.montprim,0)*nvl(r.tauxreas,0))/100 montcess,
      sum(nvl(pr.montprim,0)*nvl(r.tauxreas,0)*nvl(r.tauxcomm,0))/10000 montcomm
    from   reassureur_tranche r,   vr_prime_reassurance pr,  projet_cession_prime p
    where  
          pr.codtyptr in ('FX','FQ')
        and    r.numetrai = pr.numetrai
        and    r.exertrai = pr.exertrai
        and    r.dateeffe = pr.dateeffe
        and    nvl(r.numesect,0) = nvl(pr.numesect,0)
        and    nvl(r.numetran,0) = nvl(pr.numetran,0)  
        and   pr.annetrai = '&&Annee'       
        and   ( pr.datetrai between to_date('&&debut','DD/MM/YYYY') and to_date('&&fin','DD/MM/YYYY'))       
        and    pr.anneproj = p.anneproj
        and    pr.numeproj = p.numeproj
        and    pr.statproj = p.statproj
        group by r.codereas, pr.numequit, pr.codeinte, pr.numepoli, pr.numeaven, pr.dateffqu, r.tauxreas,
                 pr.datechqu,  pr.numetrai, pr.exertrai,pr.datetrai, pr.dateeffe, pr.numesect, pr.numetran,pr.codecate,pr.tauxprim,pr.codtyptr,
                 decode(pr.flagannu,'O','*',' '), pr.annetrai, pr.peritrai, pr.tauxprim,pr.coderisq) k,
         reassureur rr
         where rr.codereas=k.codereas
        order by k.codeinte, k.numepoli,k.numeaven, nvl(k.numequit,0)

set feedback off verify off termout off
set &&markup
spool &&spoolfile
/
spool off

rem ----------------------------------------------------------------------

set markup html off spool off

rem fin. sortir de sql*plus.
exit

