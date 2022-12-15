create or replace procedure reass_db.proc_staging(src in varchar2) as
begin
------------------------------------------
delete from reass_db.TEMP_COASSURANCES;
insert into reass_db.TEMP_COASSURANCES
(select * from REASS_DB.COASSURANCES);

delete from reass_db.TEMP_T_FAC;
insert into reass_db.TEMP_T_FAC
(select * from REASS_DB.T_FAC);

delete from reass_db.TEMP_RISQUE;
insert into reass_db.TEMP_RISQUE
(select * from REASS_DB.RISQUE);

delete from reass_db.TEMP_GARANTIES;
insert into reass_db.TEMP_GARANTIES
(select * from REASS_DB.GARANTIES);

delete from reass_db.TEMP_POLICE;
insert into reass_db.TEMP_POLICE
(select * from REASS_DB.POLICE);

delete from reass_db.TEMP_PARAM_GARANTIE_CATALOGUE;
insert into reass_db.TEMP_PARAM_GARANTIE_CATALOGUE
(select * from REASS_DB.PARAM_GARANTIE_CATALOGUE);


------------------------------------------
delete from reass_db.TEMP_HUB_COASSURANCES;
insert into reass_db.TEMP_HUB_COASSURANCES
(select I.CODEINTE,i.NUMEPOLI,
        i.NUMEAVEN,
        i.NOM_COASR,
        i.DATE_INS,
        i.SOURCE,
        i.ID_COASSURANCE 
from
(select CODEINTE,
        NUMEPOLI,
        NUMEAVEN,
        NOM_COASR, 
        trunc(sysdate) DATE_INS,
        --to_date('99990101','yyyymmdd') DATEFIN, 
        --1 STATUS,
        'MILLIARD' SOURCE,
        reass_db.MD5(CODEINTE||NUMEPOLI||NUMEAVEN||NOM_COASR) ID_COASSURANCE 
from reass_db.TEMP_COASSURANCES)i,
(select * from REASS_DB.HUB_COASSURANCE) m
where i.ID_COASSURANCE=m.ID_COASSURANCE(+) and m.ID_COASSURANCE IS NULL)
;


insert into REASS_DB.HUB_COASSURANCE
(select * from reass_db.TEMP_HUB_COASSURANCES)



insert into reass_db.TEMP_COASSURANCES_NEW
(select 
*
from
(select CODEINTE,NUMEPOLI,NUMEAVEN,NOM_COASR from reass_db.COASSURANCES)s,
(select CODEINTE,NUMEPOLI,NUMEAVEN,NOM_COASR
 from reass_db.TEMP_COASSURANCES)t
where s.CODEINTE=t.CODEINTE(+) and
      s.NUMEPOLI=t.NUMEPOLI(+)
      s.NUMEAVEN=t.NUMEAVEN(+) and
      s.NOM_COASR=t.NOM_COASR(+) and
      t.NUMEPOLI IS NULL);


insert into REASS_DB.SAT_TEMP_COASSURANCES
select 
CODEINTE,NUMEPOLI,NUMEAVEN,NOM_COASR,
decode(k.ID_COASSURANCES, null, 'I',
            decode(reass_db.MD5(CODEINTE||NUMEPOLI||NUMEAVEN||NOM_COASR),
                reass_db.MD5(CODEINTE||NUMEPOLI||NUMEAVEN||NOM_COASR), 'N', 'M'))ACTION
from
((select CODEINTE,NUMEPOLI,NUMEAVEN,CODETRAITE, CODEINTE||NUMEPOLI||NUMEAVEN||NOM_COASR ID_COASSURANCES from REASS_DB.COASSURANCES) a,
union all
(select CODEINTE,NUMEPOLI,CODETRAITE,CODEINTE||NUMEPOLI||NUMEAVEN||NOM_COASR ID_COASSURANCES from reass_db.TEMP_COASSURANCES_HUB)n
      )k

commit;

end;
/
