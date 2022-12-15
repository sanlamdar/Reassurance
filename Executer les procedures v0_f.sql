
---1 / Renseigner le datavault avec le données de MILIARD initiales
execute proc_insert_v2('MILLIARD','06072022');

---2 / Generer les output edp , cl , fac et output_reass 
-- sango lance le calcul R  
execute proc_output_reass;

---3 / Insert  les sat_link_reass_garantie fac, edp, cl 
execute INSERT_OUTPUT_CALCULS( 'NOS CALCULS', '06/07/2022') ;

--- 3 -1 verification de doublons

select  ID_GARANTIE from 
   (select  ID_GARANTIE, SOURCE ,ID_REASSURANCE_FAC,ID_REASSURANCE_CL,ID_REASSURANCE_EDP,count(*) n from SAT_OUTPUT_REASS
group by ID_GARANTIE, SOURCE,ID_REASSURANCE_FAC,ID_REASSURANCE_CL,ID_REASSURANCE_EDP) 
where n>=2
;

--- 4/ Resultats des primes cessions en batch
--execute proc_reporting_MG ('AVANT CORRECTION');
--execute proc_reporting_MP ('AVANT CORRECTION');
--execute proc_reporting_MB ('AVANT CORRECTION');

execute proc_reporting('AVANT CORRECTION')
commit;

---5/ Inserer  dans le datavault  les données corigées par les consultants
execute REASS_DB.proc_update_appli_datavault ('07/07/2022');
execute REASS_DB.proc_update_appli_datavault ('08/07/2022');
execute REASS_DB.proc_update_appli_datavault ('09/07/2022');
execute REASS_DB.proc_update_appli_datavault ('10/07/2022');
execute REASS_DB.proc_update_appli_datavault ('11/07/2022');
execute REASS_DB.proc_update_appli_datavault ('12/07/2022');
execute REASS_DB.proc_update_appli_datavault ('13/07/2022');
execute REASS_DB.proc_update_appli_datavault ('14/07/2022');
execute REASS_DB.proc_update_appli_datavault ('15/07/2022');
execute REASS_DB.proc_update_appli_datavault ('16/07/2022');
execute REASS_DB.proc_update_appli_datavault ('17/07/2022');
execute REASS_DB.proc_update_appli_datavault ('18/07/2022');
execute REASS_DB.proc_update_appli_datavault ('19/07/2022');
execute REASS_DB.proc_update_appli_datavault ('20/07/2022');
execute REASS_DB.proc_update_appli_datavault ('21/07/2022');
execute REASS_DB.proc_update_appli_datavault ('22/07/2022');
execute REASS_DB.proc_update_appli_datavault ('23/07/2022');
execute REASS_DB.proc_update_appli_datavault ('24/07/2022');
execute REASS_DB.proc_update_appli_datavault ('25/07/2022');
execute REASS_DB.proc_update_appli_datavault ('26/07/2022');
execute REASS_DB.proc_update_appli_datavault ('27/07/2022');
execute REASS_DB.proc_update_appli_datavault ('28/07/2022');
execute REASS_DB.proc_update_appli_datavault ('29/07/2022');
execute REASS_DB.proc_update_appli_datavault ('30/07/2022');
execute REASS_DB.proc_update_appli_datavault ('31/07/2022');
execute REASS_DB.proc_update_appli_datavault ('01/08/2022');
execute REASS_DB.proc_update_appli_datavault ('02/08/2022');
execute REASS_DB.proc_update_appli_datavault ('03/08/2022');
execute REASS_DB.proc_update_appli_datavault ('04/08/2022');
execute REASS_DB.proc_update_appli_datavault ('05/08/2022');
execute REASS_DB.proc_update_appli_datavault ('06/08/2022');
execute REASS_DB.proc_update_appli_datavault ('07/08/2022');
execute REASS_DB.proc_update_appli_datavault ('08/08/2022');
execute REASS_DB.proc_update_appli_datavault ('09/08/2022');
execute REASS_DB.proc_update_appli_datavault ('10/08/2022');
execute REASS_DB.proc_update_appli_datavault ('11/08/2022');
execute REASS_DB.proc_update_appli_datavault ('12/08/2022');
execute REASS_DB.proc_update_appli_datavault ('13/08/2022');
execute REASS_DB.proc_update_appli_datavault ('14/08/2022');
execute REASS_DB.proc_update_appli_datavault ('15/08/2022');
execute REASS_DB.proc_update_appli_datavault ('16/08/2022');

execute REASS_DB.proc_insert_temp_input('16/08/2022');
---sanogo execute calcul reass depuis R
execute REASS_DB.proc_update_batch_datavault ('NOS CALCULS','16/08/2022');

execute proc_reporting('APRES CORRECTION');


--execute REASS_DB.proc_update_appli_datavault ('2/07/2022');
commit; 