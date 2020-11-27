with pivoted_sofa AS (
    with all_days as (
        select
        patientunitstayid
        -- ceiling the intime to the nearest hour by adding 1440 minutes then truncating
        , 0 as endoffset
        , unitdischargeoffset
        -- create integers for each charttime in day from admission
        -- so 0 is admission time, 1 is one day after admission, etc, up to ICU disch
        , GENERATE_ARRAY(1, CAST(ceil(unitdischargeoffset/1440) AS INT64)) as day
        from `physionet-data.eicu_crd.patient`
    ), daily AS (
        SELECT
        patientunitstayid
        , CAST(day AS INT64) as day
        , endoffset + (day-1)*1440 as startoffset
        , endoffset + day*1440 as endoffset
        FROM all_days
        CROSS JOIN UNNEST(all_days.day) AS day
    ), pivoted_gcs AS (
        WITH nc as(
            select
            patientunitstayid
            , nursingchartoffset as chartoffset
            , min(case
                when nursingchartcelltypevallabel = 'Glasgow coma score'
                and nursingchartcelltypevalname = 'GCS Total'
                and REGEXP_CONTAINS(nursingchartvalue, '^[-]?[0-9]+[.]?[0-9]*$')
                and nursingchartvalue not in ('-','.')
                    then cast(nursingchartvalue as numeric)
                when nursingchartcelltypevallabel = 'Score (Glasgow Coma Scale)'
                and nursingchartcelltypevalname = 'Value'
                and REGEXP_CONTAINS(nursingchartvalue, '^[-]?[0-9]+[.]?[0-9]*$')
                and nursingchartvalue not in ('-','.')
                    then cast(nursingchartvalue as numeric)
                else null end)
                as gcs
            , min(case
                when nursingchartcelltypevallabel = 'Glasgow coma score'
                and nursingchartcelltypevalname = 'Motor'
                and REGEXP_CONTAINS(nursingchartvalue, '^[-]?[0-9]+[.]?[0-9]*$')
                and nursingchartvalue not in ('-','.')
                    then cast(nursingchartvalue as numeric)
                else null end)
                as gcsmotor
            , min(case
                when nursingchartcelltypevallabel = 'Glasgow coma score'
                and nursingchartcelltypevalname = 'Verbal'
                and REGEXP_CONTAINS(nursingchartvalue, '^[-]?[0-9]+[.]?[0-9]*$')
                and nursingchartvalue not in ('-','.')
                    then cast(nursingchartvalue as numeric)
                else null end)
                as gcsverbal
            , min(case
                when nursingchartcelltypevallabel = 'Glasgow coma score'
                and nursingchartcelltypevalname = 'Eyes'
                and REGEXP_CONTAINS(nursingchartvalue, '^[-]?[0-9]+[.]?[0-9]*$')
                and nursingchartvalue not in ('-','.')
                    then cast(nursingchartvalue as numeric)
                else null end)
                as gcseyes
            from `physionet-data.eicu_crd.nursecharting`
            -- speed up by only looking at a subset of charted data
            where nursingchartcelltypecat in
            (
                'Scores', 'Other Vital Signs and Infusions'
            )
            group by patientunitstayid, nursingchartoffset
            )
            -- apply some preprocessing to fields
        select
        patientunitstayid,
        chartoffset,
        case when gcs > 2 and gcs < 16 then gcs else null end as gcs,
        gcsmotor, 
        gcsverbal, 
        gcseyes
        from nc
    ), pivoted_labs AS (
        SELECT
        patientunitstayid,
        labresultoffset as chartoffset,
        CASE WHEN labname = 'total bilirubin' THEN labresult ELSE NULL END AS bilirubin,
        CASE WHEN labname = 'platelets x 1000' THEN labresult ELSE NULL END AS platelet,
        CASE WHEN labname = 'creatinine' THEN labresult ELSE NULL END as creatinine
        FROM `physionet-data.eicu_crd.lab`
        WHERE labname IN('total bilirubin','platelets x 1000','creatinine')
    ), pivoted_map AS (
        SELECT
        patientunitstayid,
        observationoffset as chartoffset,
        MIN(CASE WHEN noninvasivemean IS NOT NULL AND noninvasivemean BETWEEN 20 AND 200 THEN noninvasivemean ELSE NULL END) AS map
        FROM `physionet-data.eicu_crd.vitalaperiodic`
        GROUP BY patientunitstayid, observationoffset
        UNION ALL
        SELECT
        patientunitstayid,
        observationoffset as chartoffset,
        MIN(CASE WHEN systemicmean IS NOT NULL AND systemicmean BETWEEN 20 AND 200 THEN systemicmean ELSE NULL END) AS map
        FROM `physionet-data.eicu_crd.vitalperiodic`
        GROUP BY patientunitstayid, observationoffset
    ), pivoted_vaso AS (
        with weight AS (
            SELECT 
            a.patientunitstayid,
            CASE WHEN weight BETWEEN 20 AND 400 THEN weight ELSE NULL END AS weight
            FROM `physionet-data.eicu_crd_derived.pivoted_weight` a
            INNER JOIN (
                SELECT 
                patientunitstayid,
                MIN(chartoffset) AS first_weight_offset,
                FROM `physionet-data.eicu_crd_derived.pivoted_weight`
                GROUP BY patientunitstayid
            ) b 
                ON a.patientunitstayid=b.patientunitstayid
                AND a.chartoffset=b.first_weight_offset 
        )
        SELECT 
        a.patientunitstayid,
        infusionoffset as chartoffset,
        CASE WHEN drugname IN(
            'DOPamine STD 400 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)',
            'DOPamine MAX 800 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)',
            'Dopamine (mcg/kg/min)',
            'dopamine (mcg/kg/min)',
            'DOPamine STD 15 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)',
            'DOPamine STD 400 mg Dextrose 5% 500 ml  Premix (mcg/kg/min)') THEN drugrate 
            WHEN drugname = 'Dopamine (mcg/kg/hr)' THEN drugrate/60 
            WHEN drugname = 'Dopamine (mcg/hr)' THEN drugrate/(weight*60) 
            WHEN drugname = 'Dopamine (mcg/min)' THEN drugrate/weight
            WHEN drugname = 'Dopamine (mg/hr)' THEN (drugrate*1000)/(weight*60) 
            WHEN drugname = 'Dopamine (nanograms/kg/min)' THEN drugrate/1000 
            WHEN drugname IN(
                'Dopamine (ml/hr)',
                'Dopamine ()',
                'Dopamine',
                'Dopamine (Unknown)'
            ) THEN 5 --if dose cannot be determined, set it to 5 mcg/kg/min
            ELSE NULL END AS dopamine,
        CASE WHEN drugname IN(
                'Epinephrine (mcg/kg/min)',
                'Norepinephrine (mcg/kg/min)'
            ) THEN drugrate
            WHEN drugname IN(
                'Epinephrine (mg/kg/min)',
                'Norepinephrine (mg/kg/min)'
            ) THEN CAST(drugrate*1000 AS INT64)
            WHEN drugname = 'Norepinephrine (mcg/kg/hr)' THEN drugrate/60 
            WHEN drugname IN(
                'Epinephrine (mcg/min)',
                'EPINEPHrine(Adrenalin)STD 4 mg Sodium Chloride 0.9% 250 ml (mcg/min)',
                'EPINEPHrine(Adrenalin)MAX 30 mg Sodium Chloride 0.9% 250 ml (mcg/min)',
                'EPINEPHrine(Adrenalin)STD 7 mg Sodium Chloride 0.9% 250 ml (mcg/min)',
                'EPINEPHrine(Adrenalin)STD 4 mg Sodium Chloride 0.9% 500 ml (mcg/min)',
                'Norepinephrine STD 32 mg Dextrose 5% 282 ml (mcg/min)',
                'Norepinephrine STD 8 mg Dextrose 5% 250 ml (mcg/min)',
                'Norepinephrine STD 8 mg Dextrose 5% 500 ml (mcg/min)',
                'Norepinephrine STD 4 mg Dextrose 5% 500 ml (mcg/min)',
                'Norepinephrine STD 4 mg Dextrose 5% 250 ml (mcg/min)',
                'Norepinephrine MAX 32 mg Dextrose 5% 250 ml (mcg/min)',
                'Norepinephrine MAX 32 mg Dextrose 5% 500 ml (mcg/min)',
                'Norepinephrine STD 32 mg Dextrose 5% 500 ml (mcg/min)',
                'Norepinephrine (mcg/min)'
            ) THEN drugrate/weight
            WHEN drugname IN(
                'Epinephrine (mcg/hr)',
                'Norepinephrine (mcg/hr)'
            ) THEN drugrate/(weight*60) 
            WHEN drugname IN(
                'Epinephrine (mg/hr)',
                'Norepinephrine (mg/hr)'
            ) THEN (drugrate*1000)/(weight*60)
            WHEN drugname = 'Norepinephrine (mg/min)' THEN (drugrate*1000)/weight
            WHEN drugname IN(
                'Epinephrine',
                'Epinephrine ()',
                'Epinephrine (ml/hr)',
                'norepinephrine Volume (ml) (ml/hr)',
                'Norepinephrine',
                'norepinephrine Volume (ml)',
                'Norepinephrine (Unknown)',
                'Norepinephrine ()',
                'Norepinephrine (ml/hr)',
                'Norepinephrine (units/min)'
            ) THEN 0.1 ELSE NULL END AS epi_norepi,
        CASE WHEN drugname IN(
            'Dobutamine (mcg/kg/min)',
            'Dobutamine ()',
            'Dobutamine',
            'DOBUTamine STD 500 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)',
            'DOBUTamine MAX 1000 mg Dextrose 5% 250 ml  Premix (mcg/kg/min)',
            'Dobutamine (ml/hr)',
            'Dobutamine (units/min)',
            'Dobutamine (mcg/kg/hr)',
            'Dobutamine (mcg/min)'
            ) THEN 1 ELSE NULL END AS dobutamine
        FROM (
            SELECT
            patientunitstayid,
            infusionoffset,
            drugname,
            CAST(drugrate AS FLOAT64) AS drugrate
            FROM `physionet-data.eicu_crd.infusiondrug`
            WHERE REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
            AND (LOWER(drugname) LIKE '%dopami%'
                OR LOWER(drugname) LIKE '%epinephri%'
                OR LOWER(drugname) LIKE '%dobuta%'
            )
        ) a
        LEFT JOIN weight b
            ON a.patientunitstayid=b.patientunitstayid
    ), pivoted_uo AS (
        with uo as (
            SELECT
            patientunitstayid,
            intakeoutputoffset,
            cellvaluenumeric,
            CASE WHEN cellpath not like 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|%' then 0
                WHEN cellpath in (
                    'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine' -- most data is here
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|3 way foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|3 Way Foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Actual Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Adjusted total UO NOC end shift'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|BRP (urine)'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|BRP (Urine)'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|condome cath urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|diaper urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|inc of urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontient urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontient urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontient Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontinence of urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontinence-urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontinence/ voids urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontinent of urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|INCONTINENT OF URINE'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontinent UOP'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontinent urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontinent (urine)'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontinent Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontinent urine counts'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont of urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont. of urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont. of urine count'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont. of urine count'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont. urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incont. urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incont. Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|inc urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|inc. urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Inc. urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Inc Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|indwelling foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Indwelling Foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Catheter-Foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Catheterization Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath UOP'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|straight cath urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|strait cath Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Suprapubic Urine Output'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|true urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|True Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|True Urine out'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|unmeasured urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Unmeasured Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|unmeasured urine output'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urethal Catheter'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urethral Catheter'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|urinary output 7AM - 7 PM'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|urinary output 7AM-7PM'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|URINE'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|URINE'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|URINE CATHETER'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Intermittent/Straight Cath (mL)'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|straightcath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|straight cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight  cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight  Cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|straight cath daily'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|straight cathed'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cathed'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Catheter-Foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight catheterization'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Catheterization Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Catheter Output'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Catheter-Straight Catheter'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight cath ml'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath Q6hrs'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight caths'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath UOP'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|straight cath urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-straight cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine-straight cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Straight Cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Condom Catheter'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|condom catheter'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|condome cath urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|condom cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Condom Cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|CONDOM CATHETER OUTPUT'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine via condom catheter'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine-foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine- foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine- Foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine foley catheter'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine, L neph:'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine (measured)'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|urine output'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-external catheter'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Foley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-FOLEY'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Foley cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-FOLEY CATH'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-foley catheter'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Foley Catheter'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-FOLEY CATHETER'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Foley Output'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Fpley'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Ileoconduit'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-left nephrostomy'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Left Nephrostomy'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Left Nephrostomy Tube'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-LEFT PCN TUBE'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-L Nephrostomy'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-L Nephrostomy Tube'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Nephrostomy'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-right nephrostomy'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-RIGHT Nephrouretero Stent Urine Output'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-R nephrostomy'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-R Nephrostomy'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-R. Nephrostomy'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-R Nephrostomy Tube'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Rt Nephrectomy'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-stent'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-straight cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-suprapubic'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Texas Cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Urine'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Urine Output'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine, R neph:'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine-straight cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Straight Cath'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|urine (void)'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine- void'
                    , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine, void:'
                ) then 1
                when cellpath LIKE 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|foley%'
                    AND lower(cellpath) not like '%pacu%'
                    AND lower(cellpath) not like '%or%'
                    AND lower(cellpath) not like '%ir%'
                    then 1
                when cellpath like 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Output%Urinary Catheter%' then 1
                when cellpath like 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Output%Urethral Catheter%' then 1
                when cellpath like 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output (mL)%' then 1
                when cellpath like 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Output%External Urethral%' then 1
                when cellpath like 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urinary Catheter Output%' then 1
                else 0 end as cellpath_is_uo
                from `physionet-data.eicu_crd.intakeoutput`
        )
        select
        d.patientunitstayid,
        d.day,
        SUM(cellvaluenumeric) as urineoutput
        FROM daily d
        LEFT JOIN uo
            ON d.patientunitstayid=uo.patientunitstayid
            AND uo.intakeoutputoffset BETWEEN d.startoffset AND d.endoffset
        WHERE uo.cellpath_is_uo = 1
            AND cellvaluenumeric is not null
        GROUP BY d.patientunitstayid, d.day
    ), pivoted_o2 as (
        -- create columns with only numeric data
        with nc as
        (
        select
            patientunitstayid
        , nursingchartoffset
        , nursingchartentryoffset
        , case
                WHEN nursingchartcelltypevallabel = 'O2 L/%'
                AND  nursingchartcelltypevalname = 'O2 L/%'
                -- verify it's numeric
                AND REGEXP_CONTAINS(nursingchartvalue,'^[-]?[0-9]+[.]?[0-9]*$') and nursingchartvalue not in ('-','.')
                then cast(nursingchartvalue as numeric)
            else null end
            as o2_flow
        , case
                WHEN nursingchartcelltypevallabel = 'O2 Admin Device'
                AND  nursingchartcelltypevalname = 'O2 Admin Device'
                then nursingchartvalue
            else null end
            as o2_device
        , case
                WHEN nursingchartcelltypevallabel = 'End Tidal CO2'
                AND  nursingchartcelltypevalname = 'End Tidal CO2'
                -- verify it's numeric
                AND REGEXP_CONTAINS(nursingchartvalue,'^[-]?[0-9]+[.]?[0-9]*$') and nursingchartvalue not in ('-','.')
                then cast(nursingchartvalue as numeric)
            else null end
            as etco2
        from `physionet-data.eicu_crd.nursecharting`
        -- speed up by only looking at a subset of charted data
        where nursingchartcelltypecat = 'Vital Signs'
        )
        select
        patientunitstayid
        , nursingchartoffset as chartoffset
        , nursingchartentryoffset as entryoffset
        , AVG(CASE WHEN o2_flow >= 0 AND o2_flow <= 100 THEN o2_flow ELSE NULL END) AS o2_flow
        , MAX(o2_device) AS o2_device
        , AVG(CASE WHEN etco2 >= 0 AND etco2 <= 1000 THEN etco2 ELSE NULL END) AS etco2
        from nc
        WHERE o2_flow    IS NOT NULL
        OR o2_device  IS NOT NULL
        OR etco2      IS NOT NULL
        group by patientunitstayid, nursingchartoffset, nursingchartentryoffset
    ), pivoted_fio2 AS (
        SELECT 
        patientunitstayid,
        chartoffset,
        o2_device,
        CASE WHEN o2_device IN(
                'ventilator',
                'vent',
                'VENT'
            ) AND o2_flow > 20 THEN o2_flow/100
            WHEN o2_device IN(
                'ventilator',
                'vent',
                'VENT'
            ) AND o2_flow BETWEEN 0.2 AND 1.0 THEN o2_flow
            WHEN o2_device IN(
                'BiPAP/CPAP',
                'NIV'
            ) AND o2_flow > 20 THEN o2_flow/100
            WHEN o2_device IN(
                'BiPAP/CPAP',
                'NIV'
            ) AND o2_flow BETWEEN 0.2 AND 1.0 THEN o2_flow
            WHEN o2_device IN(
                'HFNC'
            ) THEN 1.0
            WHEN o2_device IN(
                'non-rebreather'
            ) THEN 1.0
            WHEN o2_device IN(
                'venturi mask'
            ) AND o2_flow BETWEEN 12 AND 15 THEN 0.6
            WHEN o2_device IN(
                'venturi mask'
            ) AND o2_flow BETWEEN 10 AND 12 THEN 0.4    
            WHEN o2_device IN(
                'venturi mask'
            ) AND o2_flow BETWEEN 8 AND 10 THEN 0.35
            WHEN o2_device IN(
                'trach collar',
                'cool aerosol mask'
            ) AND o2_flow BETWEEN 5 AND 8 THEN (30 + CEIL(o2_flow/5)*10)/100
            WHEN o2_device IN(
                'nasal cannula',
                'NC',
                'nc'
            ) AND o2_flow BETWEEN 1 AND 6 THEN (20 + o2_flow*4)/100
            WHEN o2_device IN(
                'RA',
                'ra'
            ) THEN 0.21
            ELSE NULL END AS fio2
        FROM pivoted_o2
        WHERE o2_flow IS NOT NULL
        AND o2_device IN(
            'ventilator',
            'vent',
            'VENT',
            'BiPAP/CPAP',
            'NIV',
            'HFNC',
            'non-rebreather',
            'venturi mask',
            'trach collar',
            'cool aerosol mask',
            'nasal cannula',
            'NC',
            'nc',
            'RA',
            'ra'
        )
    ), pivoted_bg as (
        -- get blood gas measures
        with vw0 as
        (
        select
            patientunitstayid
            , labname
            , labresultoffset
            , labresultrevisedoffset
        from `physionet-data.eicu_crd.lab` 
        where labname in
        (
                'paO2'
            , 'paCO2'
            , 'pH'
            , 'FiO2'
            , 'anion gap'
            , 'Base Deficit'
            , 'Base Excess'
            , 'PEEP'
        )
        group by patientunitstayid, labname, labresultoffset, labresultrevisedoffset
        having count(distinct labresult)<=1
        )
        -- get the last lab to be revised
        , vw1 as
        (
        select
            lab.patientunitstayid
            , lab.labname
            , lab.labresultoffset
            , lab.labresultrevisedoffset
            , lab.labresult
            , ROW_NUMBER() OVER
                (
                PARTITION BY lab.patientunitstayid, lab.labname, lab.labresultoffset
                ORDER BY lab.labresultrevisedoffset DESC
                ) as rn
        from `physionet-data.eicu_crd.lab` lab
        inner join vw0
            ON  lab.patientunitstayid = vw0.patientunitstayid
            AND lab.labname = vw0.labname
            AND lab.labresultoffset = vw0.labresultoffset
            AND lab.labresultrevisedoffset = vw0.labresultrevisedoffset
        WHERE
            (lab.labname = 'paO2' and lab.labresult >= 15 and lab.labresult <= 720)
        OR (lab.labname = 'paCO2' and lab.labresult >= 5 and lab.labresult <= 250)
        OR (lab.labname = 'pH' and lab.labresult >= 6.5 and lab.labresult <= 8.5)
        OR (lab.labname = 'FiO2' and lab.labresult >= 0.2 and lab.labresult <= 1.0)
        -- we will fix fio2 units later
        OR (lab.labname = 'FiO2' and lab.labresult >= 20 and lab.labresult <= 100)
        OR (lab.labname = 'anion gap' and lab.labresult >= 0 and lab.labresult <= 300)
        OR (lab.labname = 'Base Deficit' and lab.labresult >= -100 and lab.labresult <= 100)
        OR (lab.labname = 'Base Excess' and lab.labresult >= -100 and lab.labresult <= 100)
        OR (lab.labname = 'PEEP' and lab.labresult >= 0 and lab.labresult <= 60)
        )
        select
            patientunitstayid
        , labresultoffset as chartoffset
        -- the aggregate (max()) only ever applies to 1 value due to the where clause
        , MAX(case
                when labname != 'FiO2' then null
                when labresult >= 20 then labresult/100.0
            else labresult end) as fio2
        , MAX(case when labname = 'paO2' then labresult else null end) as pao2
        , MAX(case when labname = 'paCO2' then labresult else null end) as paco2
        , MAX(case when labname = 'pH' then labresult else null end) as pH
        , MAX(case when labname = 'anion gap' then labresult else null end) as aniongap
        , MAX(case when labname = 'Base Deficit' then labresult else null end) as basedeficit
        , MAX(case when labname = 'Base Excess' then labresult else null end) as baseexcess
        , MAX(case when labname = 'PEEP' then labresult else null end) as peep
        from vw1
        where rn = 1
        group by patientunitstayid, labresultoffset
    ), pivoted_bg_fio2 AS (
        SELECT 
        b.patientunitstayid,
        b.chartoffset,
        MAX(CASE WHEN o2_device LIKE '%vent%' THEN 1 ELSE 0 END) AS ventilator_flag,
        MAX(CASE WHEN b.fio2 IS NOT NULL THEN b.fio2 ELSE f.fio2 END) AS fio2,
        MIN(CASE WHEN pao2 BETWEEN 30 AND 500 THEN pao2 ELSE NULL END) as pao2
        FROM pivoted_bg b 
        LEFT JOIN pivoted_fio2 f
            ON f.patientunitstayid=b.patientunitstayid
            AND f.chartoffset BETWEEN b.chartoffset - 120 AND b.chartoffset + 120
        GROUP BY b.patientunitstayid, b.chartoffset
    ), pivoted_pf AS (
        SELECT 
        patientunitstayid,
        chartoffset,
        ventilator_flag,
        pao2/fio2 AS pf_ratio 
        FROM pivoted_bg_fio2
        WHERE fio2 IS NOT NULL AND pao2 IS NOT NULL
    )
    SELECT
    d.patientunitstayid,
    d.day,
    --P/F ratio--
    --PaO2/FiO2 [mmHg (kPa)]	SOFA score
    --≥ 400 (53.3)	0
    --< 400 (53.3)	+1
    --< 300 (40)	+2
    --< 200 (26.7) and mechanically ventilated	+3
    --< 100 (13.3) and mechanically ventilated	+4
    MAX(CASE WHEN pf_ratio < 100 AND ventilator_flag = 1 THEN 4
            WHEN pf_ratio BETWEEN 100 AND 199 AND ventilator_flag = 1 THEN 3
            WHEN pf_ratio BETWEEN 200 AND 299 THEN 2
            WHEN pf_ratio BETWEEN 300 AND 399 THEN 1
            ELSE 0 END) AS sofa_resp,
    --GCS--
    --Glasgow coma scale	SOFA score
    --15	0
    --13–14	+1
    --10–12	+2
    --6–9	+3
    --< 6	+4
    MAX(CASE WHEN gcs < 6 THEN 4
            WHEN gcs BETWEEN 6 AND 9 THEN 3
            WHEN gcs BETWEEN 10 AND 12 THEN 2
            WHEN gcs BETWEEN 13 AND 14 THEN 1
            ELSE 0 END) AS sofa_gcs,
    --Circulation--
    --Mean arterial pressure OR administration of vasopressors required	SOFA score
    --MAP ≥ 70 mmHg	0
    --MAP < 70 mmHg	+1
    --dopamine ≤ 5 μg/kg/min or dobutamine (any dose)	+2
    --dopamine > 5 μg/kg/min OR epinephrine ≤ 0.1 μg/kg/min OR norepinephrine ≤ 0.1 μg/kg/min	+3
    --dopamine > 15 μg/kg/min OR epinephrine > 0.1 μg/kg/min OR norepinephrine > 0.1 μg/kg/min	+4
    MAX(CASE WHEN dopamine > 15 OR epi_norepi > 0.1 THEN 4
            WHEN dopamine BETWEEN 6 AND 15 OR epi_norepi <= 0.1 THEN 3
            WHEN dopamine <= 5 OR dobutamine > 0 THEN 2
            WHEN map < 70 THEN 1
            ELSE 0 END) AS sofa_circ,
    --Liver--
    --Bilirubin (mg/dl) [μmol/L]	SOFA score
    --< 1.2 [< 20]	0
    --1.2–1.9 [20-32]	+1
    --2.0–5.9 [33-101]	+2
    --6.0–11.9 [102-204]	+3
    --> 12.0 [> 204]	+4
    MAX(CASE WHEN bilirubin > 12 THEN 4
            WHEN bilirubin BETWEEN 6 AND 11.9 THEN 3
            WHEN bilirubin BETWEEN 2 AND 5.9 THEN 2
            WHEN bilirubin BETWEEN 1.2 AND 1.9 THEN 1
            ELSE 0 END) AS sofa_liver,
    --Hematology--
    --Platelets×103/μl	SOFA score
    --≥ 150	0
    --< 150	+1
    --< 100	+2
    --< 50	+3
    --< 20	+4
    MAX(CASE WHEN platelet < 20 THEN 4
            WHEN platelet BETWEEN 20 AND 49 THEN 3
            WHEN platelet BETWEEN 50 AND 99 THEN 2
            WHEN platelet BETWEEN 100 AND 149 THEN 1
            ELSE 0 END) AS sofa_hematology,
    --Renal--
    --Creatinine (mg/dl) [μmol/L] (or urine output)	SOFA score
    --< 1.2 [< 110]	0
    --1.2–1.9 [110-170]	+1
    --2.0–3.4 [171-299]	+2
    --3.5–4.9 [300-440] (or < 500 ml/d)	+3
    --> 5.0 [> 440] (or < 200 ml/d)	+4
    MAX(CASE WHEN creatinine > 5 OR urineoutput < 200 THEN 4
            WHEN creatinine BETWEEN 3.5 AND 4.9 OR urineoutput BETWEEN 200 AND 499 THEN 3
            WHEN creatinine BETWEEN 2.0 AND 3.4 THEN 2
            WHEN creatinine BETWEEN 1.2 AND 1.9 THEN 1
            ELSE 0 END) AS sofa_renal
    FROM daily d
    LEFT JOIN pivoted_pf pf
        ON d.patientunitstayid=pf.patientunitstayid
        AND pf.chartoffset BETWEEN d.startoffset AND d.endoffset
    LEFT JOIN pivoted_gcs g
        ON d.patientunitstayid=g.patientunitstayid
        AND g.chartoffset BETWEEN d.startoffset AND d.endoffset
    LEFT JOIN pivoted_map m 
        ON d.patientunitstayid=m.patientunitstayid
        AND m.chartoffset BETWEEN d.startoffset AND d.endoffset
    LEFT JOIN pivoted_vaso v
        ON d.patientunitstayid=v.patientunitstayid
        AND v.chartoffset BETWEEN d.startoffset AND d.endoffset
    LEFT JOIN pivoted_uo u
        ON d.patientunitstayid=u.patientunitstayid
        AND d.day=u.day
    LEFT JOIN pivoted_labs l
        ON d.patientunitstayid=l.patientunitstayid
        AND l.chartoffset BETWEEN d.startoffset AND d.endoffset
    GROUP BY d.patientunitstayid, d.day
)
SELECT
*,
sofa_resp + sofa_gcs + sofa_circ + sofa_liver + sofa_hematology + sofa_renal AS sofa,
sofa_resp + sofa_circ + sofa_liver + sofa_hematology + sofa_renal AS sofa_wo_gcs
FROM pivoted_sofa
ORDER BY patientunitstayid, day