# ITUscoringAnalysis

This repository contains the R code used in the analysis of the MIMIC-III and eICU databases.
The objective of the research was to determine if there were significant differences in the performance of the APACHE, OASIS and SOFA scores in different ethnic groups in the USA.
Access to the data is through the www.physionet.org website. Formal application must be made via the website, so the data used cannot be published here.
Instead, a series of dummy files with one line of altered data is placed here so that the data structure can be seen and understood.

The SOFA scores in the MIMIC-III database can be found in the 'SOFA' table in the 'derived' section. 
However, we used the SQL code that can be found here by Alistair Johnson of the MIT Laboratory of Computational Physiology:

https://github.com/MIT-LCP/mimic-code/blob/b1a0ffeae62a09fe3284650aceb578dad14fb9b1/concepts/pivot/pivoted_sofa.sql

It can be found in the file 'MIMIC_SOFAscores_SQL.txt' in this repository.

For the eICU data, SOFA scores have to be extracting using SQL.
We using the SQL code provided by XXX. The code can be found in the file 'eICU_SOFAscores_SQL.txt'.

## MIMIC-III data
"bq-results-20200805-214538-n690szu0x80_SOFAscoresAdmission_DUMMY.csv"
This is the SOFA scores extracted using the SQl code described above. ('MIMIC_SOFAscores_SQL.txt')

"bq-results-20200806-202359-anbq1d2oo30vOASIS_DUMMY.csv"
This is the table 'oasis'.

"bq-results-20200806-225103-mcsk0rb8h76l_Patients_DUMMY.csv"
This is the table 'patients'.

"bq-results-20200806-231458-u3iow35qz30w_Admissions_DUMMY.csv"
This is the table 'admissions'.

"bq-results-20200808-092553-ttzoex49foff_ICUSTAYS_DUMMY.csv")
This is the table 'icustays'.

"bq-results-20200806-081503-l2nmx172wnn4_SubjectsEthnicities_DUMMY.csv"
A table of ethicities derived from teh 'admisisions' table.

## eICU
"bq-results-20200809-141932-y1zn72dtxh4e_EICU_PATIENT.csv"
This is the 'patient' table.

"bq-results-20200809-142217-bcushyutuc8s_EICU_ApacheRes.csv"
This is the 'apachepatientresutl' table.

"bq-results-20200810-120702-s3rw32q3hplb-eICU_SOFA.csv"
This is a table of SOFA scores extracted using the SQL code in 'eICU_SOFAscores_SQL.txt'

