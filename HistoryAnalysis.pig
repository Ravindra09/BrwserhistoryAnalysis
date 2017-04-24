				#Processing Browsing History csv file.
#Extracting only hostnames from url.
grunt> History = LOAD '/user/ravindr6/Browsing_History.csv' using PigStorage(',') AS (Url : chararray, Title:chararray, VisitDate:chararray, VistCount:int);

grunt> host = FOREACH History GENERATE org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(Url);
grunt> dump host;

grunt> host = FOREACH History GENERATE org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(Url) as HostNames, $3 as VisitCount;
grunt > dump host;
grunt> Grp_Data = GROUP host by HostNames;
#grunt> Count_Visit = FOREACH Grp_Data Generate FLATTEN(group) as HostNames, COUNT($1) as Count;
#grunt > Sorted_Out = ORDER Count_Visit  BY Count desc;

#2nd : Correct way
grunt> counter = FOREACH Grp_Data GENERATE group, SUM(host.VisitCount) as TVC;
grunt > Sorted_Output = ORDER counter BY TVC desc;
grunt> Top15 = limit Sorted_Output 15;
grunt> dump Top15;
