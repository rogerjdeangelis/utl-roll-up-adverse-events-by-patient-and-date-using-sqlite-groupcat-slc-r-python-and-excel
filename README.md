# utl-roll-up-adverse-events-by-patient-and-date-using-sqlite-groupcat-slc-r-python-and-excel
Roll up adverse events by patient and date using sql group_concat with slc, r, python, and excel
    %let pgm=utl-roll-up-adverse-events-by-patient-and-date-using-sqlite-groupcat-slc-r-python-and-excel;

    %stop_submission;

    Roll up adverse events by patient and date using sql group_concat with slc, r, python, and excel

    Two Solutions

     Problem given a sqlite table with one adverse event per record
     concatenate adverse events by patient and date

     Two types of output sas dataset

      ID     AE_DATE          AE_EVENTS

       1    2022-06-23    nausea,manic
       1    2022-08-05    cough,dizzy,vomit
       ...

    COMMENTS

    1. Base SAS does not support the group_cancat sql function in base sas
    2. Also sas and the slc cannot close then re-open a excel workbook and add a sheet directly.
    3. You can reopen a closed workbook with proc R and proc Python and add a sheet.
    4. As a side note I believe you can add a sheet using powershell programatically  (copysheet?)
       see
       https://github.com/rogerjdeangelis/utl-copy-excel-sheets-from-one-workbook-to-another-using-powershell


    CONTENTS

      Note the exact same sql script is used with all these languages

       1 input database & sqlite table
         d:/sqlite/mysqlite.db table have (one adverse event per record)

       2 slc odbc sql
         output d:/wpswrkx/adverse_concat.sas7bdat

         WORKX.HAVE total obs=13

         ID     AE_DATE          AE_EVENTS

          1    2022-06-23    nausea,manic
          1    2022-08-05    cough,dizzy,vomit
          ...

       3 slc odbc excel (slc does not support adding a sheet to a re-opened excel workbook directly)

         d:/xls/ae_events.xlsx

         ----------------------+
         | A1| fx   |   ID     |
         ------------------------------------------+
         [_] |    A |    B     |          C        |
         ------------------------------------------|
          1  |  ID  | AE_DATE  |      AE_EVENTS    |
          -- |------+----------+-------------------|
          2  |   1  |2022-06-23|   nausea,manic    |
          -- |------+----------+-------------------|
          3  |   1  |2022-08-05|   cough,dizzy,vomi|
          ...

       4 r sql

         d:/xls/have.xlsx

         -------------------+              ----------------------+
         | A1| fx|    ID    |              | A1| fx   |   ID     |
         -----------------------------+    ------------------------------------------+
         [_] |ID | AE_DATE  | AE_TYPE |    [_] |    A |    B     |          C        |
         -----------------------------|    ------------------------------------------|
          1  |   |          |         |     1  |  ID  | AE_DATE  |      AE_EVENTS    |
          -- |---+----------+---------|     -- |------+----------+-------------------|
          2  | 1 |2022-08-05|   cough |     2  |   1  |2022-06-23|   nausea,manic    |
          -- |---+----------+---------|     -- |------+----------+-------------------|
          3  | 1 |2022-06-23|   nausea|     3  |   1  |2022-08-05|   cough,dizzy,vomi|
          ...                               ...
          [HAVE]                            [WANT]

       5 python sql

         -------------------+              ----------------------+
         | A1| fx|    ID    |              | A1| fx   |   ID     |
         -----------------------------+    ------------------------------------------+
         [_] |ID | AE_DATE  | AE_TYPE |    [_] |    A |    B     |          C        |
         -----------------------------|    ------------------------------------------|
          1  |   |          |         |     1  |  ID  | AE_DATE  |      AE_EVENTS    |
          -- |---+----------+---------|     -- |------+----------+-------------------|
          2  | 1 |2022-08-05|   cough |     2  |   1  |2022-06-23|   nausea,manic    |
          -- |---+----------+---------|     -- |------+----------+-------------------|
          3  | 1 |2022-06-23|   nausea|     3  |   1  |2022-08-05|   cough,dizzy,vomi|
          ...                               ...
          [HAVE]                            [WANT]



    see for creating a dsn
    github
    https://github.com/rogerjdeangelis/utl-altair-slc-sqlite-odbc-reading-and-writing-tables-using-passthru-processing

    /*   _                   _        _       _        _                      ___    _        _     _
    / | (_)_ __  _ __  _   _| |_   __| | __ _| |_ __ _| |__   __ _ ___  ___  ( _ )  | |_ __ _| |__ | | ___
    | | | | `_ \| `_ \| | | | __| / _` |/ _` | __/ _` | `_ \ / _` / __|/ _ \ / _ \/\| __/ _` | `_ \| |/ _ \
    | | | | | | | |_) | |_| | |_ | (_| | (_| | || (_| | |_) | (_| \__ \  __/| (_>  <| || (_| | |_) | |  __/
    |_| |_|_| |_| .__/ \__,_|\__| \__,_|\__,_|\__\__,_|_.__/ \__,_|___/\___| \___/\/ \__\__,_|_.__/|_|\___|
                |_|
    NOTE SAS  SQLITE ODBC cannot create an empty sqlite database.
    You can use the the R program below to create an empty R sqlite
    database or download the one in this repo.
    The database has to match the name in the odbc definition.
    */

    /*--- CREATE EMPTY SQLITE DATABASE FILE IF IT DOES NOT EXIST ---*/
    proc r;
    submit;
    library(DBI)
    library(RSQLite)

    # Create empty SQLite database file if it does not exist
    con <- dbConnect(SQLite(), "d:/sqlite/mysqlite.db")
    dbDisconnect(con)
    endsubmit;
    run;quit;

    /*--- CREATE TABLE HAVE IN SQLITE FILE DATABASE D:/SQLITE/MYSQLITE.DB ---*/
    libname workx "d:/wpswrkx";

    data workx.have;
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

    libname sqlite odbc dsn="wernerdsn";

    /*--- delete sqlite table have if it exists ---*/
    proc delete data=sqlite.have;
    run;quit;

    data sqlite.have;
      set workx.have;
    run;quit;

    proc contents data=sqlite.have;
    run;quit;

    proc print data=sqlite.have;
    run;quit;

    /*---

    Altair SLC
    LIST: 8:31:42

    Altair SLC

    The CONTENTS  SQLITE DATBASE TABLE HAVE

    Data Set Name           HAVE
    Member Type             VIEW
    Engine
    Observations                .
    Variables               3
    Indexes                 0
    Observation Length      26
    Deleted Observations     0
    Data Set Type
    Label
    Compressed              NO
    Sorted                  NO
    Data Representation
    Encoding                wlatin1 Windows-1252 Western

                              Alphabetic List of Variables and Attributes

          Number    Variable    Type             Len             Pos    Format          Informat
    ________________________________________________________________________________________________
               2    AE_DATE     Char              10               8    $10.            $10.
               3    AE_TYPE     Char               8              18    $8.             $8.
               1    ID          Num                8               0

    Altair SLC

    Obs    ID    AE_DATE       AE_TYPE

      1     1    2022-08-05    cough
      2     1    2022-06-23    nausea
      3     1    2022-08-05    dizzy
      4     1    2022-08-05    vomit
      5     1    2022-06-23    manic
      6     2    2019-05-07    cough
      7     2    2020-04-15    nausea
      8     2    2019-05-07    dizzy
      9     2    2020-04-15    vomit
     10     3    2016-07-21    manic
     11     3    2017-11-09    cough
     12     3    2018-09-25    nausea
     13     3    2017-11-09    dizzy
    ---*/

    /*___        _                  _ _
    |___ \   ___| | ___    ___   __| | |__   ___
      __) | / __| |/ __|  / _ \ / _` | `_ \ / __|
     / __/  \__ \ | (__  | (_) | (_| | |_) | (__
    |_____| |___/_|\___|  \___/ \__,_|_.__/ \___|

    */

    proc sql;
      connect to odbc(dsn="wernerdsn");
      create table workx.adverse_concat as
      select * from connection to odbc (
        SELECT id, ae_date, GROUP_CONCAT(ae_type) AS ae_events
        FROM have
        GROUP BY id, ae_date
      );
      disconnect from odbc;
    quit;

    proc print data=workx.adverse_concat width=min;
    run;quit;

    /*---

    Altair SLC
    LIST: 8:48:50

    WORKX.HAVE total obs=13

    Altair SLC

    Obs    ID     AE_DATE          AE_EVENTS

     1      1    2022-06-23    nausea,manic
     2      1    2022-08-05    cough,dizzy,vomit

     3      2    2019-05-07    cough,dizzy
     4      2    2020-04-15    nausea,vomit

     5      3    2016-07-21    manic
     6      3    2017-11-09    cough,dizzy
     7      3    2018-09-25    nausea

    ---*/

    /*____       _                  _ _                              _
    |___ /   ___| | ___    ___   __| | |__   ___    _____  _____ ___| |
      |_ \  / __| |/ __|  / _ \ / _` | `_ \ / __|  / _ \ \/ / __/ _ \ |
     ___) | \__ \ | (__  | (_) | (_| | |_) | (__  |  __/>  < (_|  __/ |
    |____/  |___/_|\___|  \___/ \__,_|_.__/ \___|  \___/_/\_\___\___|_|

    */

    libname xls excel "d:/xls/ae_events.xlsx";

    proc delete data=xls.ae_events;
    run;quit;

    proc sql;
      connect to odbc(dsn="wernerdsn");
      create table xls.ae_events as
      select * from connection to odbc (
        SELECT id, ae_date, GROUP_CONCAT(ae_type) AS ae_events
        FROM have
        GROUP BY id, ae_date
      );
      disconnect from odbc;
    quit;

    proc print data=xls.ae_events width=min;
    run;quit;

    d:/xls/ae_events.xlsx

    ----------------------+
    | A1| fx   |   ID     |
    ------------------------------------------+
    [_] |    A |    B     |          C        |
    ------------------------------------------|
     1  |  ID  | AE_DATE  |      AE_EVENTS    |
     -- |------+----------+-------------------|
     2  |   1  |2022-06-23|   nausea,manic    |
     -- |------+----------+-------------------|
     3  |   1  |2022-08-05|   cough,dizzy,vomi|
     -- |------+----------+-------------------|
     4  |   2  |2019-05-07|   cough,dizzy     |
     -- |------+----------+-------------------|
     5  |   2  |2020-04-15|   nausea,vomit    |
     -- |------+----------+-------------------|
     6  |   3  |2016-07-21|   manic           |
     -- |------+----------+-------------------|
     7  |   3  |2017-11-09|   cough,dizzy     |
     -- |------+----------+-------------------|
     8  |   3  |2018-09-25|   nausea          |
     -- |------+----------+-------------------+
     [ae_events]

    /*  _                                        _
    | || |    _ __    ___  _ __   ___ _ __ __  _| |
    | || |_  | `__|  / _ \| `_ \ / _ \ `_ \\ \/ / |
    |__   _| | |    | (_) | |_) |  __/ | | |>  <| |
       |_|   |_|     \___/| .__/ \___|_| |_/_/\_\_|
                          |_|
    */

    /*--- create input xlsx workbook with sheet have           ---*/
    /*--- I want the input and output to be just excel         ---*/
    /*--- Iant the solution to have both input & output sheets ---*/

    %utlfkil(d:/xls/have.xlsx);

    libname xls excel "d:/xls/have.xlsx";

    proc delete data=xls.have;
    run;quit;

    data xls.have;
     set workx.have;
    run;quit;

    libname xls clear;

    /*---

    -------------------+
    | A1| fx|    ID    |
    -----------------------------+
    [_] |ID | AE_DATE  | AE_TYPE |
    -----------------------------|
     1  |   |          |         |
     -- |---+----------+---------|
     2  | 1 |2022-08-05|   cough |
     -- |---+----------+---------|
     3  | 1 |2022-06-23|   nausea|
     -- |---+---------++---------|
     4  | 1 |2022-08-05|   dizzy |
     -- |---+---------++---------|
     5  | 1 |2022-08-05|   vomit |
     -- |---+---------++---------|
     6  | 1 |2022-06-23|   manic |
     -- |---+---------++---------|
     7  | 2 |2019-05-07|   cough |
     -- |---+---------++---------|
     8  | 2 |2020-04-15|   nausea|
     -- |---+---------++---------|
     9  | 2 |2019-05-07|   dizzy |
     -- |---+---------++---------|
    10  | 2 |2020-04-15|   vomit |
     -- |---+---------++---------|
    11  | 3 |2016-07-21|   manic |
     -- |---+---------++---------|
    12  | 3 |2017-11-09|   cough |
     -- |---+---------++---------|
    13  | 3 |2018-09-25|   nausea|
     -- |---+---------++---------|
    14  | 3 |2017-11-09|   dizzy |
     -- |---+---------++---------+
    [HAVE]

    ---*/

    options set=RHOME "C:\Progra~1\R\R-4.5.2\bin\r";
    proc r;
    submit;
    library(openxlsx)
    library(sqldf)
    options(sqldf.dll = "d:/dll/sqlean.dll")
    wb<-loadWorkbook("d:/xls/have.xlsx")
    have<-read.xlsx(wb,"have")
    addWorksheet(wb, "want")
    want<-sqldf('
      select
         id
        ,ae_date
        ,group_concat(ae_type) as aes
      from
         have
      group
         by id, ae_date
     ')
    print(want)
    writeData(wb,sheet="want",x=want)
    saveWorkbook(
        wb
       ,"d:/xls/have.xlsx"
        ,overwrite=TRUE)
    endsubmit;
    run;quit;

    /*----

     R ADDED A SHEET TO THE INPUT WORKBOOK
     --------------------------------------

     d:/xls/have.xlsx

     -------------------+              ----------------------+
     | A1| fx|    ID    |              | A1| fx   |   ID     |
     -----------------------------+    ------------------------------------------+
     [_] |ID | AE_DATE  | AE_TYPE |    [_] |    A |    B     |          C        |
     -----------------------------|    ------------------------------------------|
      1  |   |          |         |     1  |  ID  | AE_DATE  |      AE_EVENTS    |
      -- |---+----------+---------|     -- |------+----------+-------------------|
      2  | 1 |2022-08-05|   cough |     2  |   1  |2022-06-23|   nausea,manic    |
      -- |---+----------+---------|     -- |------+----------+-------------------|
      3  | 1 |2022-06-23|   nausea|     3  |   1  |2022-08-05|   cough,dizzy,vomi|
      -- |---+---------++---------|     -- |------+----------+-------------------|
      4  | 1 |2022-08-05|   dizzy |     4  |   2  |2019-05-07|   cough,dizzy     |
      -- |---+---------++---------|     -- |------+----------+-------------------|
      5  | 1 |2022-08-05|   vomit |     5  |   2  |2020-04-15|   nausea,vomit    |
      -- |---+---------++---------|     -- |------+----------+-------------------|
      6  | 1 |2022-06-23|   manic |     6  |   3  |2016-07-21|   manic           |
      -- |---+---------++---------|     -- |------+----------+-------------------|
      7  | 2 |2019-05-07|   cough |     7  |   3  |2017-11-09|   cough,dizzy     |
      -- |---+---------++---------|     -- |------+----------+-------------------|
      8  | 2 |2020-04-15|   nausea|     8  |   3  |2018-09-25|   nausea          |
      -- |---+---------++---------|     -- |------+----------+-------------------+
      9  | 2 |2019-05-07|   dizzy |     [WANT]
      -- |---+---------++---------|
     10  | 2 |2020-04-15|   vomit |
      -- |---+---------++---------|
     11  | 3 |2016-07-21|   manic |
      -- |---+---------++---------|
     12  | 3 |2017-11-09|   cough |
      -- |---+---------++---------|
     13  | 3 |2018-09-25|   nausea|
      -- |---+---------++---------|
     14  | 3 |2017-11-09|   dizzy |
      -- |---+---------++---------+
     [HAVE]

    /*
    | | ___   __ _
    | |/ _ \ / _` |
    | | (_) | (_| |
    |_|\___/ \__, |
             |___/
    */

    1                                          Altair SLC        12:46 Friday, January 16, 2026

    NOTE: Copyright 2002-2025 World Programming, an Altair Company
    NOTE: Altair SLC 2026 (05.26.01.00.000758)
          Licensed to Roger DeAngelis
    NOTE: This session is executing on the X64_WIN11PRO platform and is running in 64 bit mode

    NOTE: AUTOEXEC processing beginning; file is C:\wpsoto\autoexec.sas
    NOTE: AUTOEXEC source line
    1       +  ï»¿ods _all_ close;
               ^
    ERROR: Expected a statement keyword : found "?"
    NOTE: Library workx assigned as follows:
          Engine:        SAS7BDAT
          Physical Name: d:\wpswrkx

    NOTE: Library slchelp assigned as follows:
          Engine:        WPD
          Physical Name: C:\Progra~1\Altair\SLC\2026\sashelp


    LOG:  12:46:11
    NOTE: 1 record was written to file PRINT

    NOTE: The data step took :
          real time : 0.031
          cpu time  : 0.015


    NOTE: AUTOEXEC processing completed

    1
    2         /*--- create input xlsx workbook with sheet have           ---*/
    3         /*--- I want the input and output to be just excel         ---*/
    4         /*--- Iant the solution to have both input & output sheets ---*/
    5
    6         %utlfkil(d:/xls/have.xlsx);
    7
    8         libname xls excel "d:/xls/have.xlsx";
    NOTE: Library xls assigned as follows:
          Engine:        OLEDB
          Physical Name: d:/xls/have.xlsx

    9
    10        proc delete data=xls.have;
    11        run;quit;
    NOTE: XLS.HAVE (memtype="DATA") was not found, and has not been deleted
    NOTE: Procedure delete step took :
          real time : 0.025
          cpu time  : 0.015


    12
    13        data xls.have;
    14         set workx.have;
    15        run;

    NOTE: 13 observations were read from "WORKX.have"
    NOTE: Data set "XLS.have" has an unknown number of observation(s) and 3 variable(s)
    NOTE: The data step took :
          real time : 0.065
          cpu time  : 0.046


    15      !     quit;

    2                                                                                                                         Altair SLC

    NOTE: Libref XLS has been deassigned.
    16
    17        libname xls clear;
    18
    19        /*---
    20
    21        -------------------+
    22        | A1| fx|    ID    |
    23        -----------------------------+
    24        [_] |ID | AE_DATE  | AE_TYPE |
    25        -----------------------------|
    26         1  |   |          |         |
    27         -- |---+----------+---------|
    28         2  | 1 |2022-08-05|   cough |
    29         -- |---+----------+---------|
    30         3  | 1 |2022-06-23|   nausea|
    31         -- |---+---------++---------|
    32         4  | 1 |2022-08-05|   dizzy |
    33         -- |---+---------++---------|
    34         5  | 1 |2022-08-05|   vomit |
    35         -- |---+---------++---------|
    36         6  | 1 |2022-06-23|   manic |
    37         -- |---+---------++---------|
    38         7  | 2 |2019-05-07|   cough |
    39         -- |---+---------++---------|
    40         8  | 2 |2020-04-15|   nausea|
    41         -- |---+---------++---------|
    42         9  | 2 |2019-05-07|   dizzy |
    43         -- |---+---------++---------|
    44        10  | 2 |2020-04-15|   vomit |
    45         -- |---+---------++---------|
    46        11  | 3 |2016-07-21|   manic |
    47         -- |---+---------++---------|
    48        12  | 3 |2017-11-09|   cough |
    49         -- |---+---------++---------|
    50        13  | 3 |2018-09-25|   nausea|
    51         -- |---+---------++---------|
    52        14  | 3 |2017-11-09|   dizzy |
    53         -- |---+---------++---------+
    54        [HAVE]
    55
    56        ---*/
    57
    58        options set=RHOME "C:\Progra~1\R\R-4.5.2\bin\r";
    59        proc r;
    60        submit;
    61        library(openxlsx)
    62        library(sqldf)
    63        options(sqldf.dll = "d:/dll/sqlean.dll")
    64        wb<-loadWorkbook("d:/xls/have.xlsx")
    65        have<-read.xlsx(wb,"have")
    66        addWorksheet(wb, "want")
    67        want<-sqldf('
    68          select
    69             id
    70            ,ae_date
    71            ,group_concat(ae_type) as aes
    72          from
    73             have
    74          group
    75             by id, ae_date
    76         ')
    77        print(want)

    3                                                                                                                         Altair SLC

    78        writeData(wb,sheet="want",x=want)
    79        saveWorkbook(
    80            wb
    81           ,"d:/xls/have.xlsx"
    82            ,overwrite=TRUE)
    83        endsubmit;
    NOTE: Using R version 4.5.2 (2025-10-31 ucrt) from C:\Program Files\R\R-4.5.2

    NOTE: Submitting statements to R:

    > library(openxlsx)
    > library(sqldf)
    Loading required package: gsubfn
    Loading required package: proto
    Loading required package: RSQLite
    > options(sqldf.dll = "d:/dll/sqlean.dll")
    > wb<-loadWorkbook("d:/xls/have.xlsx")
    > have<-read.xlsx(wb,"have")
    > addWorksheet(wb, "want")
    > want<-sqldf('
    +   select
    +      id
    +     ,ae_date
    +     ,group_concat(ae_type) as aes
    +   from
    +      have
    +   group
    +      by id, ae_date
    +  ')
    > print(want)
    > writeData(wb,sheet="want",x=want)
    > saveWorkbook(
    +     wb
    +    ,"d:/xls/have.xlsx"
    +     ,overwrite=TRUE)

    NOTE: Processing of R statements complete

    NOTE: Procedure r step took :
          real time : 1.870
          cpu time  : 0.031

    ERROR: Error printed on page 1

    NOTE: Submitted statements took :
          real time : 2.544
          cpu time  : 0.859

    /*___                _   _                             _
    | ___|   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
    |___ \  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
     ___) | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    |____/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
            |_|    |___/                                |_|

    For input use the d:/xls/have.xlsx and sheet have from 4. (R solution);
    */

    options set=PYTHONHOME "D:\py314";
    proc python;
    submit;
    import pandas as pd
    import openpyxl as px
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    file_path = 'd:/xls/have.xlsx'
    have = pd.read_excel(file_path)  # defaults to first sheet
    want=pdsql('''
      select
         id
        ,ae_date
        ,group_concat(ae_type) as aes
      from
         have
      group
         by id, ae_date
     ''')
    with pd.ExcelWriter(
        file_path,
        engine='openpyxl',
        mode='a',  # Append mode
        if_sheet_exists='replace'  # Replace if 'want' already exists
    ) as writer:
        want.to_excel(writer, sheet_name='want', index=False)

    print(f"Sheet 'want' added to {file_path}")
    endsubmit;
    run;quit;

    /*---

     R ADDED A SHEET TO THE INPUT WORKBOOK
     --------------------------------------

     -------------------+              ----------------------+
     | A1| fx|    ID    |              | A1| fx   |   ID     |
     -----------------------------+    ------------------------------------------+
     [_] |ID | AE_DATE  | AE_TYPE |    [_] |    A |    B     |          C        |
     -----------------------------|    ------------------------------------------|
      1  |   |          |         |     1  |  ID  | AE_DATE  |      AE_EVENTS    |
      -- |---+----------+---------|     -- |------+----------+-------------------|
      2  | 1 |2022-08-05|   cough |     2  |   1  |2022-06-23|   nausea,manic    |
      -- |---+----------+---------|     -- |------+----------+-------------------|
      3  | 1 |2022-06-23|   nausea|     3  |   1  |2022-08-05|   cough,dizzy,vomi|
      -- |---+---------++---------|     -- |------+----------+-------------------|
      4  | 1 |2022-08-05|   dizzy |     4  |   2  |2019-05-07|   cough,dizzy     |
      -- |---+---------++---------|     -- |------+----------+-------------------|
      5  | 1 |2022-08-05|   vomit |     5  |   2  |2020-04-15|   nausea,vomit    |
      -- |---+---------++---------|     -- |------+----------+-------------------|
      6  | 1 |2022-06-23|   manic |     6  |   3  |2016-07-21|   manic           |
      -- |---+---------++---------|     -- |------+----------+-------------------|
      7  | 2 |2019-05-07|   cough |     7  |   3  |2017-11-09|   cough,dizzy     |
      -- |---+---------++---------|     -- |------+----------+-------------------|
      8  | 2 |2020-04-15|   nausea|     8  |   3  |2018-09-25|   nausea          |
      -- |---+---------++---------|     -- |------+----------+-------------------+
      9  | 2 |2019-05-07|   dizzy |     [WANT]
      -- |---+---------++---------|
     10  | 2 |2020-04-15|   vomit |
      -- |---+---------++---------|
     11  | 3 |2016-07-21|   manic |
      -- |---+---------++---------|
     12  | 3 |2017-11-09|   cough |
      -- |---+---------++---------|
     13  | 3 |2018-09-25|   nausea|
      -- |---+---------++---------|
     14  | 3 |2017-11-09|   dizzy |
      -- |---+---------++---------+
     [HAVE]

    ---*/

    /*
    | | ___   __ _
    | |/ _ \ / _` |
    | | (_) | (_| |
    |_|\___/ \__, |
             |___/
    */

    1                                          Altair SLC        12:37 Friday, January 16, 2026

    NOTE: Copyright 2002-2025 World Programming, an Altair Company
    NOTE: Altair SLC 2026 (05.26.01.00.000758)
          Licensed to Roger DeAngelis
    NOTE: This session is executing on the X64_WIN11PRO platform and is running in 64 bit mode

    NOTE: AUTOEXEC processing beginning; file is C:\wpsoto\autoexec.sas
    NOTE: AUTOEXEC source line
    1       +  ï»¿ods _all_ close;
               ^
    ERROR: Expected a statement keyword : found "?"
    NOTE: Library workx assigned as follows:
          Engine:        SAS7BDAT
          Physical Name: d:\wpswrkx

    NOTE: Library slchelp assigned as follows:
          Engine:        WPD
          Physical Name: C:\Progra~1\Altair\SLC\2026\sashelp


    LOG:  12:37:34
    NOTE: 1 record was written to file PRINT

    NOTE: The data step took :
          real time : 0.016
          cpu time  : 0.031


    NOTE: AUTOEXEC processing completed

    1         options set=PYTHONHOME "D:\py314";
    2         proc python;
    3         submit;
    4         import pandas as pd
    5         import openpyxl as px
    6         from pandasql import sqldf;
    7         mysql = lambda q: sqldf(q, globals());
    8         from pandasql import PandaSQL;
    9         pdsql = PandaSQL(persist=True);
    10        sqlite3conn = next(pdsql.conn.gen).connection.connection;
    11        sqlite3conn.enable_load_extension(True);
    12        sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    13        mysql = lambda q: sqldf(q, globals());
    14        file_path = 'd:/xls/have.xlsx'
    15        have = pd.read_excel(file_path)  # defaults to first sheet
    16        want=pdsql('''
    17          select
    18             id
    19            ,ae_date
    20            ,group_concat(ae_type) as aes
    21          from
    22             have
    23          group
    24             by id, ae_date
    25         ''')
    26        print(want)
    27        with pd.ExcelWriter(
    28            file_path,
    29            engine='openpyxl',
    30            mode='a',  # Append mode
    31            if_sheet_exists='replace'  # Replace if 'want' already exists
    32        ) as writer:
    33            want.to_excel(writer, sheet_name='want', index=False)
    34

    2                                                                                                                         Altair SLC

    35        print(f"Sheet 'want' added to {file_path}")
    36        endsubmit;

    NOTE: Submitting statements to Python:

    NOTE: <string>:8: SADeprecationWarning: The _ConnectionFairy.connection attribute is deprecated; please use 'driver_connection' (deprecated since: 2.0)


    37        run;quit;
    NOTE: Procedure python step took :
          real time : 1.738
          cpu time  : 0.031


    ERROR: Error printed on page 1

    NOTE: Submitted statements took :
          real time : 1.801
          cpu time  : 0.093

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
