rem - nom - Cesssions_sap.sql
rem - designation - Comparaison Production <-> Comptabilite.
rem - version - 1.0
rem - auteur - roger.costandi@saham-it.com
rem - historique - 2016-10-21 version 1.0

def spoolfile="&&1" heading "nom du fichier temporaire de sortie (avec extension)"
def spoolformat="&&2" heading "format du fichier (txt ou xls ou html)"
     
   
rem DEBUT PARAMETRES UTILISATEURS 
 
   def Annee="&&3" heading "Ann�e" 
   def periode="&&4" heading "P�riode"
 
   
   
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
-- Le seul point o� les deux se rejoignent est le sch�ma de comptabilisation (schema_comptable_production)
--
-- RC 20101010
-- 
-- Principe de ce rapprochement:
-- * on s�lectionne toutes les quittances �mises (datecomp) dans la p�riode consid�r�e
--   (y compris si elles sont annul�es), quelque soit leur type (mais pas les projets)
-- * on y ajoute (en inversant le sens) les quittances annul�es directement,
--   c'est � dire sans r��mission de quittance mais en renseignant datsorqu et piecannu
--   (en prenant en compte leur date de sort qui est compar�e � la p�riode de s�lection)
-- * exemple: (p�riode consid�r�e: juin 201X)
--   quittance N� 8  datecomp en mai 201X --> non s�lectionn�e
--   quittance N� 9  datecomp en mai 201X et datsorqu en juin 201X --> seule l'annulation est s�lectionn�e
--   quittance N� 10 datecomp en juin 201X --> s�lectionn�e
--   quittance N� 11 datecomp en juin 201X et datsorqu en juin 201X --> s�lectionn�e 2 fois, dont 1 en sens inverse
--   quittance N� 12 datecomp en juin 201X et datsorqu en ao�t 201X --> �mission s�lectionn�e, mais pas l'annulation
-- * de cette mani�re, si une quittance est annul�e sur une p�riode diff�rente de son �mission, tout
--   reste coh�rent (et conforme � ce qui est comptabilis�)
-- * ensuite on s�lectionne les mouvements comptables pass�s dans la p�riode consid�r�e et 
--   sur les comptes mentionn�s dans le sch�ma comptable
 
-- la diff�rence repr�sente les �carts:
-- - montants diff�rents
-- - quittances non comptabilis�es
-- - mouvements comptables sans �quivalent dans les quittances
--
-- Dans le tableau r�sultat:
-- les donn�es pr�c�d�es d'un libell� ("quittance",...) proviennent de la production,
-- celles qui sont pr�c�d�es d'un num�ro de compte (10 chiffres) proviennent de la compta.
--
-- les signes � appliquer aux diff�rentes colonnes de montants (production) proviennent de type_quittance.
-- on les inverse dans le cas des annulations sans quittance s�par�e.
--
-- Les montants en provenance de la compta sont calcul�s ainsi:  CREDIT - DEBIT
-- (car le Chiffre d'Affaires est plut�t cr�diteur, ce serait l'inverse dans le cas des sinistres)
-- Les montants en provenance de la production sont pr�sent�s avec un signe oppos� � la compta,
-- pour aboutir � une somme alg�brique de z�ro s'il n'y a pas d'�cart.
--
-- En compta, les mouvements saisis sur d'autres journaux que le journal de production sont identifi�s
-- par des "******" dans la colonne "NOTE": en effet, ils repr�sentent des saisies manuelles
-- (ou mal param�tr�es) et provoquent des �carts.
--
-- Autres cas d'�cart rencontr�s:
-- - g�n�ration comptable non effectu�e
-- - erreurs d'arrondis (coassurance)
--
-- La version actuelle des sch�mas utilise un seul journal comptable pour tous les mouvements,
-- d'o� la simplification "(select distinct codejour from schema_comptable_production)" pour
-- identifier le journal en compta
--
-- les cat�gories sont extraites des num�ros de comptes du sch�ma selon deux r�gles:
--   * compte 70201xxx00 (si dernier chiffre = "0")
--   * ou compte 7020101xxx
--   o� "xxx" repr�sente le num�ro de cat�gorie
--   (le deuxi�me cas n'a �t� rencontr� que dans le sch�ma Vie de Madagascar)
--   le num�ro de branche est le 1er chiffre de la cat�gorie ci-dessus


-- bornes de la s�lection (datecomp, datsorsi, datepiec):
 


set linesize 250 pagesize 50000 verify off feedback off

  codeinte  format a15
  numepoli format a15
  codecate format a15
  libecate format a15
  codeassu format a15
  CODEGARA format a15
  raissoci format a30
--  PTT  format 9999999999999
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

 ttitle left COMPAGNIE " - Cessions prime      "   '&&Annee'  "     " '&&periode' "     " FIN " - Edite le &&now"

-- rapprochement

select statproj, daterese, codeinte, exersini, numesini, exertrai, numetrai, dateeffe, codtyptr, ordrappl, numesect, codecate, intepoli, 
   numepoli, coderisq, datesurv, datecomp, flaggene, numeaven, catereas, catecomp, peritrai, annetrai, anneproj, numeproj, tauxcess, 
   sourprim, refesini, codebran, libebran, libecate, desitrai, codeassu, raissoci, refeinte, codbrare, libbrare, libcatre_c, libcatre_r,
   mont_smp, capibrut, moncaptr, monrestr, resebrut, monregtr, nb_gara
    from vr_reserve_reassurance 
    WHERE  anneproj='&&Annee' and peritrai = nvl(decode('&&periode','null','','&&periode'),peritrai)
order  by peritrai,dateeffe,codtyptr

set feedback off verify off termout off
set &&markup
spool &&spoolfile
/
spool off

rem ----------------------------------------------------------------------

set markup html off spool off

rem fin. sortir de sql*plus.
exit
