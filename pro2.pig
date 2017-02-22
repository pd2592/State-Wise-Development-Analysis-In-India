Problem1::
----------------------------------------------------------------------------------

REGISTER /usr/local/pig/lib/piggybank.jar;
 
DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();

A =  LOAD 'hdfs://localhost:9000/flume_import' using org.apache.pig.piggybank.storage.XMLLoader('row') as (x:chararray);
 
B = foreach A GENERATE FLATTEN(REGEX_EXTRACT_ALL(x,'<row>\\s*<State_Name>(.*)</State_Name>\\s*<District_Name>(.*)</District_Name>\\s*<Project_Objectives_IHHL_BPL>(.*)</Project_Objectives_IHHL_BPL>\\s*<Project_Objectives_IHHL_APL>(.*)</Project_Objectives_IHHL_APL>\\s*<Project_Objectives_IHHL_TOTAL>(.*)</Project_Objectives_IHHL_TOTAL>\\s*<Project_Objectives_SCW>(.*)</Project_Objectives_SCW>\\s*<Project_Objectives_School_Toilets>(.*)</Project_Objectives_School_Toilets>\\s*<Project_Objectives_Anganwadi_Toilets>(.*)</Project_Objectives_Anganwadi_Toilets>\\s*<Project_Objectives_RSM>(.*)</Project_Objectives_RSM>\\s*<Project_Objectives_PC>(.*)</Project_Objectives_PC>\\s*<Project_Performance-IHHL_BPL>(.*)</Project_Performance-IHHL_BPL>\\s*<Project_Performance-IHHL_APL>(.*)</Project_Performance-IHHL_APL>\\s*<Project_Performance-IHHL_TOTAL>(.*)</Project_Performance-IHHL_TOTAL>\\s*<Project_Performance-SCW>(.*)</Project_Performance-SCW>\\s*<Project_Performance-School_Toilets>(.*)</Project_Performance-School_Toilets>\\s*<Project_Performance-Anganwadi_Toilets>(.*)</Project_Performance-Anganwadi_Toilets>\\s*<Project_Performance-RSM>(.*)</Project_Performance-RSM>\\s*<Project_Performance-PC>(.*)</Project_Performance-PC>\\s*</row>'));

C = limit B 5;

dump C;

D = FILTER B by $2==$10;

E = foreach D generate $1;

store E into 'hdfs://localhost:9000/flume_import_out';



Problem2::
-----------------------------------------------------------------------------------------
store B into 'hdfs://localhost:9000/flume_import_out/parsexmlstatedistrictwise/';

REGISTER objBPL.jar

F = LOAD 'hdfs://localhost:9000/flume_import_out/parsexmlstatedistrictwise/p*' using PigStorage('\t') as (state_name:chararray, district_name:chararray, obj_bpl:int, obj_apl:int, obj_total:int, obj_scw:int, obj_school_toi:int, obj_anganwadi_toi:int, obj_rsm:int, obj_pc:int, per_bpl:int, per_apl:int, per_total:int, per_scw:int, per_school_toi:int, per_anganwadi_toi:int, per_rsm:int, per_pc:int);

G = Filter F by filter.statedistrict.ObjectivesBPL(obj_bpl,per_bpl)==true;

H = foreach G generate district_name; 

dump H;

store H into 'hdfs://localhost:9000/flume_import_out/parsexmlstatedistrictwiseout';




