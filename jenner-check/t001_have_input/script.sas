/* Input adverse-events table HAVE, one adverse event per record.        */
/* Verbatim from the repo's DATA workx.have step (libref folded to WORK). */

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

proc contents data=have;
run;quit;

proc print data=have;
run;quit;
