/* Roll up adverse events by patient and date -> ADVERSE_CONCAT.          */
/* Same HAVE data as the repo; the SQLite GROUP_CONCAT(ae_type) roll-up   */
/* is expressed here in base SAS (PROC SORT + by-group first./last. +     */
/* RETAIN + CATX). Output matches the repo's documented WANT dataset.     */

data have;
 input
     ID
     AE_DATE $10.
     AE_TYPE $;
cards;
1 2022-08-05 cough
1 2022-06-23 nausea
1 2022-08-05 dizzy
1 2022-08-05 vomit
1 2022-06-23 manic
2 2019-05-07 cough
2 2020-04-15 nausea
2 2019-05-07 dizzy
2 2020-04-15 vomit
3 2016-07-21 manic
3 2017-11-09 cough
3 2018-09-25 nausea
3 2017-11-09 dizzy
;;;;
run;quit;

proc sort data=have;
  by id ae_date;
run;quit;

data adverse_concat;
  set have;
  by id ae_date;
  length ae_events $200;
  retain ae_events;
  if first.ae_date then ae_events = '';
  ae_events = catx(',', ae_events, ae_type);
  if last.ae_date then output;
  keep id ae_date ae_events;
run;quit;

proc print data=adverse_concat width=min;
run;quit;
