def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):
    L_ABCLRISP_INSUNIX_LPG = f'''
                             (SELECT
                             'D' INDDETREC, 
                             'ABCLRISP' TABLAIFRS17, 
                             '' AS PK,
                             '' AS DTPREG,
                             '' AS TIOCPROC,
                             '' AS TIOCFRM, --PENDIENTE
                             '' AS TIOCTO,
                             'PIG' AS KGIORIGM,
                             PC.BRANCH ||'-'|| COALESCE (PC.PRODUCT, 0) ||'-'|| PC.POLICY ||'-'|| PC.CERTIF AS KABAPOL,
                             PC.BRANCH ||'-'|| COALESCE (PC.PRODUCT, 0) ||'-'|| PC.POLICY ||'-'|| PC.CERTIF || '-' || (SELECT EVI.SCOD_VT  FROM USINSUG01.EQUI_VT_INX EVI WHERE EVI.SCOD_INX = R.CLIENT)  AS KABUNRIS,
                             COALESCE(( SELECT COALESCE(GC.COVERGEN, 0)           
                                        FROM USINSUG01.GEN_COVER GC 
                                        JOIN USINSUG01.COVER C  
                                        ON  GC.USERCOMP = C.USERCOMP 
                                        AND GC.COMPANY  = C.COMPANY 
                                        AND GC.BRANCH   = C.BRANCH
                                        AND GC.PRODUCT  = PC.PRODUCT
                                        AND GC.SUB_PRODUCT = PC.SUB_PRODUCT
                                        AND GC.CURRENCY = C.CURRENCY
                                        AND GC.MODULEC =  C.MODULEC
                                        AND GC.COVER   =  C.COVER
                                        AND GC.EFFECDATE <= PC.EFFECDATE
                             		   AND (GC.NULLDATE IS NULL OR GC.NULLDATE > PC.EFFECDATE)		       		   
                                        WHERE C.USERCOMP   = PC.USERCOMP 
                                        AND   C.COMPANY    = PC.COMPANY 
                                        AND   C.CERTYPE    = '2' 
                                        AND   C.BRANCH     = PC.BRANCH 
                                        AND   C.POLICY     = PC.POLICY
                                        AND   C.CERTIF     = PC.CERTIF  
                                        AND   C.EFFECDATE <= PC.EFFECDATE
                                        AND  (C.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE)
                                        AND  C.COVER = 1
                             ) ,'0') AS KGCTPCBT,
                             ROW_NUMBER () OVER (PARTITION  BY PC.BRANCH, COALESCE (PC.PRODUCT, 0), PC.POLICY, PC.CERTIF ORDER BY R.CLIENT) AS DNPESEG,
                             (SELECT EVI.SCOD_VT  FROM USINSUG01.EQUI_VT_INX EVI WHERE EVI.SCOD_INX  = R.CLIENT) AS KEBENTID_PS,
                             (SELECT DATE_PART('YEAR', AGE(CURRENT_DATE, CLI.BIRTHDAT)) FROM USINSUG01.CLIENT CLI WHERE CLI.CODE = R.CLIENT) AS DIDADEAC,
                             '' AS DANOREF, --EN BLANCO
                             '' AS KACEMPR,
                             CASE PC.BRANCH
                             WHEN 23 
                             THEN (SELECT COALESCE (INSU_HE.ANUAL_SAL, 0) FROM USINSUG01.INSURED_HE INSU_HE
                                   WHERE INSU_HE.USERCOMP = PC.USERCOMP
                                   AND   INSU_HE.COMPANY  =  PC.COMPANY
                                   AND   INSU_HE.CERTYPE  =  PC.CERTYPE
                                   AND   INSU_HE.BRANCH = PC.BRANCH
                                   AND 	INSU_HE.POLICY = PC.POLICY
                             	  AND   INSU_HE.CERTIF = PC.CERTIF
                             	  AND   INSU_HE.CLIENT = R.CLIENT
                             	  AND   INSU_HE.EFFECDATE <= PC.EFFECDATE
                             	  AND   (INSU_HE.NULLDATE IS NULL OR INSU_HE.NULLDATE > PC.EFFECDATE))
                             ELSE 0
                             END VMTSALAR,
                             '' AS KACTPSAL,
                             '' AS TADMEMP,
                             '' AS TADMGRP,
                             '' AS TSAIDGRP,
                             'LPG' AS DCOMPA,
                             '' AS DMARCA,
                             '' AS TDNASCIM,
                             '' AS DQDIASUB,
                             '' AS DQESPING,
                             '' AS KACSEXO,
                             '' AS KACCAE,
                             '' AS DQTRABAL,
                             '' AS DINAPMEN,
                             '' AS DQCOEFEQ,
                             '' AS DQHORSEM, --EN BLANCO
                             '' AS DQMESES,
                             '' AS VMTALIME, --EN BLANCO
                             '' AS DQMESALI, --EN BLANCO
                             '' AS VMTALOJA,
                             '' AS DQMESALO,
                             '' AS VMTRENUM, --EN BLANCO
                             '' AS DQMESREN,
                             '' AS DQDIATRA,
                             '' AS DQCOPRE,
                             '' AS DITINER,
                             '' AS DNOMES,
                             '' AS KACUTILIZ,
                             '' AS VMTDESCO, --EN BLANCO
                             '' AS DFRANQU,  --EN BLANCO
                             '' AS DTARIFA,
                             '' AS DINTIPEXP,
                             '' AS KACESPAN,
                             '' AS DQANIMAL,
                             '' AS KACTIPSG,
                             '' AS DAPROESC,
                             '' AS DCMUDESC,
                             '' AS DQCAPITA,
                             '' AS VTXCOMPA,
                             '' AS DQCAES,
                             '' AS DINEXTTE,
                             '' AS KACCLTARI,
                             '' AS KACTIPVEI,
                             '' AS KACCATRIS,
                             '' AS DINDCOL,
                             '' AS DACONSTR,
                             '' AS DQPESSO1,
                             '' AS DQPESSO2,
                             '' AS DQVIAS,
                             '' AS KACAGRAV,
                             '' AS KACPARTI,
                             '' AS KACSERMD,
                             '' AS KACMRISC,
                             '' AS DINDCON,
                             '' AS DQPRAZO,
                             R.ROLE AS KACTPPES,
                             '' AS KACESPES, --EN BLANCO
                             '' AS KACMEPES,
                             '' AS TDESPES,  --EN BLANCO
                             '' AS KACTPPRA,
                             '' AS DEMPREST,
                             '' AS VMTPREST,
                             '' AS VMTEMPRE,
                             '' AS VMTPRCRD,
                             '' AS DCONTCGD,
                             '' AS DNCLICGD,
                             '' AS DCERTIFC,
                             R.EFFECDATE AS TINICIO,
                            coalesce(cast(cast(R.NULLDATE as date) as varchar), '') AS TTERMO,
                             '' AS DNOMEPAR,
                             '' AS KACPROF,
                             '' AS KACACTIV,
                             '' AS KACSACTIV,
                             '' AS VMTSALMD,
                             '' AS DCODSUB,
                             '' AS KACTPCON,
                             '' AS DAREACCV,
                             '' AS DAREACUL,
                             '' AS KACZONAG,
                             '' AS KACTPIDX,
                             '' AS TDTINDEX,
                             '' AS VMTPRMIN,
                             '' AS DCDREGIM,
                             '' AS DQHORTRA,
                             '' AS DQSEMTRA,  --EN BLANCO
                             '' AS DCAMPANH,
                             '' AS KACMODAL,
                             '' AS DENTIDSO,
                             '' AS DLOCREF,
                             '' AS KACINTNI,
                             '' AS KACCLRIS,
                             '' AS KACAMBCB,
                             '' AS KACTRAIN,
                             '' AS DINDCIRS,
                             '' AS DINCERPA,
                             '' AS DINDMOTO,
                             '' AS DMATRIC,
                             '' AS DINDMARK,
                             '' AS KACOPCBT,
                             '' AS VTXINDX,
                             '' AS DAGRIDAD,   --EN BLANCO
                             '' AS KACPAIS_DT, --NO
                             '' AS KACMDAC,    --EN BLANCO
                             CASE PC.BRANCH
                             WHEN 23 
                             THEN (SELECT COALESCE (INSU_HE.AGE_LIMIT, 0) FROM USINSUG01.INSURED_HE INSU_HE
                                   WHERE INSU_HE.USERCOMP = PC.USERCOMP
                                   AND   INSU_HE.COMPANY  =  PC.COMPANY
                                   AND   INSU_HE.CERTYPE  =  PC.CERTYPE
                                   AND   INSU_HE.BRANCH = PC.BRANCH
                                   AND 	INSU_HE.POLICY = PC.POLICY
                             	  AND   INSU_HE.CERTIF = PC.CERTIF
                             	  AND   INSU_HE.CLIENT = R.CLIENT
                             	  AND   INSU_HE.EFFECDATE <= PC.EFFECDATE
                             	  AND   (INSU_HE.NULLDATE IS NULL OR INSU_HE.NULLDATE > PC.EFFECDATE))
                             ELSE 0
                             END DIDADECOM,
                             '' AS VTXPERINDC,
                             '' AS TPGMYBENEF
                             FROM USINSUG01.ROLES R
                             JOIN ( SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE  
                                    FROM USINSUG01.POLICY P 
                             	   LEFT JOIN USINSUG01.CERTIFICAT CERT 
                             	   ON P.USERCOMP = CERT.USERCOMP 
                             	   AND P.COMPANY = CERT.COMPANY 
                             	   AND P.CERTYPE = CERT.CERTYPE 
                             	   AND P.BRANCH  = CERT.BRANCH 
                             	   AND P.POLICY  = CERT.policy
                             	   JOIN USINSUG01.POL_SUBPRODUCT PSP
                             	   ON  PSP.USERCOMP = P.USERCOMP
                             	   AND PSP.COMPANY  = P.COMPANY
                             	   AND PSP.CERTYPE  = P.CERTYPE
                             	   AND PSP.BRANCH   = P.BRANCH		   
                             	   AND PSP.PRODUCT  = P.PRODUCT
                             	   AND PSP.POLICY   = P.POLICY	
                             	   JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P.BRANCH AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usinsug01'
                             	   WHERE P.CERTYPE = '2' 
                                    AND P.STATUS_POL NOT IN ('2','3') 
                                    AND ( (P.POLITYPE = '1' -- INDIVIDUAL 
                                        AND P.EXPIRDAT >= '2021-12-31' 
                                        AND (P.NULLDATE IS NULL OR P.NULLDATE > '2021-12-31') )
                                        OR 
                                        (P.POLITYPE <> '1' -- COLECTIVAS 
                                        AND CERT.EXPIRDAT >= '2021-12-31' 
                                        AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2021-12-31'))
                                   )) AS PC	
                             ON  R.USERCOMP = PC.USERCOMP 
                             AND R.COMPANY  = PC.COMPANY 
                             AND R.CERTYPE  = PC.CERTYPE
                             AND R.BRANCH   = PC.BRANCH 
                             AND R.POLICY   = PC.POLICY 
                             AND R.CERTIF   = PC.CERTIF  
                             AND R.EFFECDATE <= PC.EFFECDATE 
                             AND (R.NULLDATE IS NULL OR R.NULLDATE > PC.EFFECDATE)
                             AND R.ROLE IN (2,8)) AS PIG
                             '''
    
    L_DF_ABCLRISP_INSUNIX_LPG = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_ABCLRISP_INSUNIX_LPG).load()

    print("INSUNIX LPG")

    L_ABCLRISP_INSUNIX_LPV = f'''(
                                    SELECT
                                    'D' INDDETREC, 
                                    'ABCLRISP' TABLAIFRS17, 
                                    '' AS PK,
                                    '' AS DTPREG,
                                    '' AS TIOCPROC,
                                    '' AS TIOCFRM,
                                    '' AS TIOCTO,
                                    'PIV' AS KGIORIGM,
                                    PC.BRANCH ||'-'|| COALESCE (PC.PRODUCT, 0) ||'-'|| PC.POLICY ||'-'|| PC.CERTIF AS KABAPOL,
                                    '' AS KABUNRIS, --PENDIENTE
                                    COALESCE(( SELECT COALESCE(GC.COVERGEN, 0)           
                                          FROM USINSUV01.life_cover GC 
                                          JOIN USINSUV01.COVER C  
                                          ON  GC.USERCOMP = C.USERCOMP 
                                          AND GC.COMPANY  = C.COMPANY 
                                          AND GC.BRANCH   = C.BRANCH
                                          AND GC.PRODUCT  = PC.PRODUCT
                                          --AND GC.SUB_PRODUCT = PC.SUB_PRODUCT
                                          AND GC.CURRENCY = C.CURRENCY
                                          --AND GC.MODULEC =  C.MODULEC
                                          AND GC.COVER   =  C.COVER
                                          AND GC.EFFECDATE <= PC.EFFECDATE
                                          AND (GC.NULLDATE IS NULL OR GC.NULLDATE > PC.EFFECDATE)		       		   
                                          WHERE C.USERCOMP   = PC.USERCOMP 
                                          AND   C.COMPANY    = PC.COMPANY 
                                          AND   C.CERTYPE    = '2' 
                                          AND   C.BRANCH     = PC.BRANCH 
                                          AND   C.POLICY     = PC.POLICY
                                          AND   C.CERTIF     = PC.CERTIF  
                                          AND   C.EFFECDATE <= PC.EFFECDATE
                                          AND  (C.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE)
                                          AND  C.COVER = 1 limit 1
                                    ) ,'0') AS KGCTPCBT,
                                    ROW_NUMBER () OVER ( PARTITION  BY PC.BRANCH, COALESCE (PC.PRODUCT, 0), PC.POLICY, PC.CERTIF order by R.CLIENT) AS DNPESEG, --PENDIENTE
                                    '' AS KEBENTID_PS,
                                    (SELECT (CURRENT_DATE - CLI.BIRTHDAT)/365 FROM USINSUG01.CLIENT CLI WHERE CLI.CODE = PC.TITULARC) AS DIDADEAC,
                                    '' AS DANOREF, --EN BLANCO
                                    '' AS KACEMPR,
                                    CASE PC.BRANCH
                                    WHEN 23 
                                    THEN (SELECT COALESCE (INSU_HE.ANUAL_SAL, 0) FROM USINSUV01.INSURED_HE INSU_HE
                                          WHERE INSU_HE.USERCOMP = PC.USERCOMP
                                          AND   INSU_HE.COMPANY  =  PC.COMPANY
                                          AND   INSU_HE.CERTYPE  =  PC.CERTYPE
                                          AND   INSU_HE.BRANCH = PC.BRANCH
                                          AND 	INSU_HE.POLICY = PC.POLICY
                                          AND   INSU_HE.CERTIF = PC.CERTIF
                                          AND   INSU_HE.CLIENT = R.CLIENT
                                          AND   INSU_HE.EFFECDATE <= PC.EFFECDATE
                                          AND   (INSU_HE.NULLDATE IS NULL OR INSU_HE.NULLDATE > PC.EFFECDATE))
                                    ELSE 0
                                    END VMTSALAR,
                                    '' AS KACTPSAL,
                                    '' AS TADMEMP,
                                    '' AS TADMGRP,
                                    '' AS TSAIDGRP,
                                    'LPG' AS DCOMPA,
                                    '' AS DMARCA,
                                    '' AS TDNASCIM,
                                    '' AS DQDIASUB,
                                    '' AS DQESPING,
                                    '' AS KACSEXO,
                                    '' AS KACCAE,
                                    '' AS DQTRABAL,
                                    '' AS DINAPMEN,
                                    '' AS DQCOEFEQ,
                                    '' AS DQHORSEM, --EN BLANCO
                                    '' AS DQMESES,
                                    '' AS VMTALIME, --EN BLANCO
                                    '' AS DQMESALI, --EN BLANCO
                                    '' AS VMTALOJA,
                                    '' AS DQMESALO,
                                    '' AS VMTRENUM, --EN BLANCO
                                    '' AS DQMESREN,
                                    '' AS DQDIATRA,
                                    '' AS DQCOPRE,
                                    '' AS DITINER,
                                    '' AS DNOMES,
                                    '' AS KACUTILIZ,
                                    '' AS VMTDESCO, --EN BLANCO
                                    '' AS DFRANQU,  --EN BLANCO
                                    '' AS DTARIFA,
                                    '' AS DINTIPEXP,
                                    '' AS KACESPAN,
                                    '' AS DQANIMAL,
                                    '' AS KACTIPSG,
                                    '' AS DAPROESC,
                                    '' AS DCMUDESC,
                                    '' AS DQCAPITA,
                                    '' AS VTXCOMPA,
                                    '' AS DQCAES,
                                    '' AS DINEXTTE,
                                    '' AS KACCLTARI,
                                    '' AS KACTIPVEI,
                                    '' AS KACCATRIS,
                                    '' AS DINDCOL,
                                    '' AS DACONSTR,
                                    '' AS DQPESSO1,
                                    '' AS DQPESSO2,
                                    '' AS DQVIAS,
                                    '' AS KACAGRAV,
                                    '' AS KACPARTI,
                                    '' AS KACSERMD,
                                    '' AS KACMRISC,
                                    '' AS DINDCON,
                                    '' AS DQPRAZO,
                                    R.ROLE AS KACTPPES,
                                    '' AS KACESPES, --EN BLANCO
                                    '' AS KACMEPES,
                                    '' AS TDESPES,  --EN BLANCO
                                    '' AS KACTPPRA,
                                    '' AS DEMPREST,
                                    '' AS VMTPREST,
                                    '' AS VMTEMPRE,
                                    '' AS VMTPRCRD,
                                    '' AS DCONTCGD,
                                    '' AS DNCLICGD,
                                    '' AS DCERTIFC,
                                    R.EFFECDATE AS TINICIO,
                                    coalesce(cast(cast(R.NULLDATE as date) as varchar), '') AS TTERMO,
                                    '' AS DNOMEPAR,
                                    '' AS KACPROF,
                                    '' AS KACACTIV,
                                    '' AS KACSACTIV,
                                    '' AS VMTSALMD,
                                    '' AS DCODSUB,
                                    '' AS KACTPCON,
                                    '' AS DAREACCV,
                                    '' AS DAREACUL,
                                    '' AS KACZONAG,
                                    '' AS KACTPIDX,
                                    '' AS TDTINDEX,
                                    '' AS VMTPRMIN,
                                    '' AS DCDREGIM,
                                    '' AS DQHORTRA,
                                    '' AS DQSEMTRA,  --EN BLANCO
                                    '' AS DCAMPANH,
                                    '' AS KACMODAL,
                                    '' AS DENTIDSO,
                                    '' AS DLOCREF,
                                    '' AS KACINTNI,
                                    '' AS KACCLRIS,
                                    '' AS KACAMBCB,
                                    '' AS KACTRAIN,
                                    '' AS DINDCIRS,
                                    '' AS DINCERPA,
                                    '' AS DINDMOTO,
                                    '' AS DMATRIC,
                                    '' AS DINDMARK,
                                    '' AS KACOPCBT,
                                    '' AS VTXINDX,
                                    '' AS DAGRIDAD,   --EN BLANCO
                                    '' AS KACPAIS_DT, --NO
                                    '' AS KACMDAC,    --EN BLANCO
                                    CASE PC.BRANCH
                                    WHEN 23 
                                    THEN (SELECT COALESCE (INSU_HE.AGE_LIMIT, 0) FROM USINSUV01.INSURED_HE INSU_HE
                                          WHERE INSU_HE.USERCOMP = PC.USERCOMP
                                          AND   INSU_HE.COMPANY  =  PC.COMPANY
                                          AND   INSU_HE.CERTYPE  =  PC.CERTYPE
                                          AND   INSU_HE.BRANCH = PC.BRANCH
                                          AND 	INSU_HE.POLICY = PC.POLICY
                                          AND   INSU_HE.CERTIF = PC.CERTIF
                                          AND   INSU_HE.CLIENT = R.CLIENT
                                          AND   INSU_HE.EFFECDATE <= PC.EFFECDATE
                                          AND   (INSU_HE.NULLDATE IS NULL OR INSU_HE.NULLDATE > PC.EFFECDATE))
                                    ELSE 0
                                    END DIDADECOM,
                                    '' AS VTXPERINDC,
                                    '' AS TPGMYBENEF
                                    FROM USINSUV01.ROLES R
                                    JOIN ( SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE
                                          FROM USINSUV01.POLICY P 
                                          LEFT JOIN USINSUV01.CERTIFICAT CERT 
                                          ON P.USERCOMP = CERT.USERCOMP 
                                          AND P.COMPANY = CERT.COMPANY 
                                          AND P.CERTYPE = CERT.CERTYPE 
                                          AND P.BRANCH  = CERT.BRANCH 
                                          AND P.POLICY  = CERT.policy	
                                          JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P.BRANCH AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                          WHERE P.CERTYPE = '2' 
                                          AND P.STATUS_POL NOT IN ('2','3') 
                                          AND ( (P.POLITYPE = '1' -- INDIVIDUAL 
                                          AND P.EXPIRDAT >= '2021-12-31' 
                                          AND (P.NULLDATE IS NULL OR P.NULLDATE > '2021-12-31') )
                                          OR 
                                          (P.POLITYPE <> '1' -- COLECTIVAS 
                                          AND CERT.EXPIRDAT >= '2021-12-31' 
                                          AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2021-12-31'))
                                    )) AS PC	
                                    ON  R.USERCOMP = PC.USERCOMP 
                                    AND R.COMPANY  = PC.COMPANY 
                                    AND R.CERTYPE  = PC.CERTYPE
                                    AND R.BRANCH   = PC.BRANCH 
                                    AND R.POLICY   = PC.POLICY 
                                    AND R.CERTIF   = PC.CERTIF  
                                    --AND R.CLIENT   = PC.TITULARC
                                    AND R.EFFECDATE <= PC.EFFECDATE 
                                    AND (R.NULLDATE IS NULL OR R.NULLDATE > PC.EFFECDATE)
                                    WHERE R.ROLE IN (2,8)
                                 ) as tmp
                              '''
    
    L_DF_ABCLRISP_INSUNIX_LPV = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_ABCLRISP_INSUNIX_LPV).load()

    print("INSUNIX LPV")
    #----------------------------------------------------------------------------------------------------------------------------------#

    L_ABCLRISP_VTIME_LPG = f'''
                              (
                                  SELECT
                                    'D' INDDETREC, 
                                    'ABCLRISP' TABLAIFRS17, 
                                    '' AS PK,
                                    '' AS DTPREG,
                                    '' AS TIOCPROC,
                                    '' AS TIOCFRM, --PENDIENTE
                                    '' AS TIOCTO,
                                    'PVG' AS KGIORIGM,
                                    PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" AS KABAPOL,
                                    PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" || '-' || R."SCLIENT"  AS KABUNRIS,
                                    COALESCE((SELECT COALESCE(GLC."NCOVERGEN", 0)           
                                              FROM USBI01.IFRS170_V_GEN_LIFE_COVER GLC
                                              JOIN USVTIMG01."COVER" C  
                                              ON  GLC."NBRANCH"   = C."NBRANCH"
                                              AND GLC."NPRODUCT"  = PC."NPRODUCT"
                                              AND GLC."NCURRENCY" = C."NCURRENCY"
                                              AND GLC."NMODULEC" =  C."NMODULEC"
                                              AND GLC."NCOVER"   =  C."NCOVER"
                                              AND GLC."DEFFECDATE" <= PC."DSTARTDATE"
                                              AND (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > PC."DSTARTDATE")		       		   
                                              WHERE C."SCERTYPE"    = PC."SCERTYPE" 
                                              AND   C."NBRANCH"     = PC."NBRANCH"
                                              AND   C."NPRODUCT"    = PC."NPRODUCT"
                                              AND   C."NPOLICY"     = PC."NPOLICY"
                                              AND   C."NCERTIF"     = PC."NCERTIF"
                                              AND   C."DEFFECDATE" <= PC."DSTARTDATE"
                                              AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE")
                                              AND  C."NCOVER" = 1
                                    ) ,'0') AS KGCTPCBT,
                                    ROW_NUMBER () OVER (PARTITION  BY PC."NBRANCH", PC."NPRODUCT", PC."NPOLICY", PC."NCERTIF" ORDER BY R."SCLIENT") AS DNPESEG,
                                    R."SCLIENT" AS KEBENTID_PS,
                                    (SELECT DATE_PART('YEAR', AGE(CURRENT_DATE, CLI."DBIRTHDAT")) FROM USVTIMG01."CLIENT" CLI WHERE CLI."SCLIENT" = R."SCLIENT") AS DIDADEAC,
                                    '' AS DANOREF, --EN BLANCO
                                    '' AS KACEMPR,
                                    '' AS VMTSALAR,
                                    '' AS KACTPSAL,
                                    '' AS TADMEMP,
                                    '' AS TADMGRP,
                                    '' AS TSAIDGRP,
                                    'LPG' AS DCOMPA,
                                    '' AS DMARCA,
                                    '' AS TDNASCIM,
                                    '' AS DQDIASUB,
                                    '' AS DQESPING,
                                    '' AS KACSEXO,
                                    '' AS KACCAE,
                                    '' AS DQTRABAL,
                                    '' AS DINAPMEN,
                                    '' AS DQCOEFEQ,
                                    '' AS DQHORSEM, --EN BLANCO
                                    '' AS DQMESES,
                                    '' AS VMTALIME, --EN BLANCO
                                    '' AS DQMESALI, --EN BLANCO
                                    '' AS VMTALOJA,
                                    '' AS DQMESALO,
                                    '' AS VMTRENUM, --EN BLANCO
                                    '' AS DQMESREN,
                                    '' AS DQDIATRA,
                                    '' AS DQCOPRE,
                                    '' AS DITINER,
                                    '' AS DNOMES,
                                    '' AS KACUTILIZ,
                                    '' AS VMTDESCO, --EN BLANCO
                                    '' AS DFRANQU,  --EN BLANCO
                                    '' AS DTARIFA,
                                    '' AS DINTIPEXP,
                                    '' AS KACESPAN,
                                    '' AS DQANIMAL,
                                    '' AS KACTIPSG,
                                    '' AS DAPROESC,
                                    '' AS DCMUDESC,
                                    '' AS DQCAPITA,
                                    '' AS VTXCOMPA,
                                    '' AS DQCAES,
                                    '' AS DINEXTTE,
                                    '' AS KACCLTARI,
                                    '' AS KACTIPVEI,
                                    '' AS KACCATRIS,
                                    '' AS DINDCOL,
                                    '' AS DACONSTR,
                                    '' AS DQPESSO1,
                                    '' AS DQPESSO2,
                                    '' AS DQVIAS,
                                    '' AS KACAGRAV,
                                    '' AS KACPARTI,
                                    '' AS KACSERMD,
                                    '' AS KACMRISC,
                                    '' AS DINDCON,
                                    '' AS DQPRAZO,
                                    R."NROLE" AS KACTPPES,
                                    '' AS KACESPES, --EN BLANCO
                                    '' AS KACMEPES,
                                    '' AS TDESPES,  --EN BLANCO
                                    '' AS KACTPPRA,
                                    '' AS DEMPREST,
                                    '' AS VMTPREST,
                                    '' AS VMTEMPRE,
                                    '' AS VMTPRCRD,
                                    '' AS DCONTCGD,
                                    '' AS DNCLICGD,
                                    '' AS DCERTIFC,
                                    R."DEFFECDATE" AS TINICIO,
                                    coalesce(cast(cast(R."DNULLDATE"as date) as varchar), '') AS TTERMO,
                                    '' AS DNOMEPAR,
                                    '' AS KACPROF,
                                    '' AS KACACTIV,
                                    '' AS KACSACTIV,
                                    '' AS VMTSALMD,
                                    '' AS DCODSUB,
                                    '' AS KACTPCON,
                                    '' AS DAREACCV,
                                    '' AS DAREACUL,
                                    '' AS KACZONAG,
                                    '' AS KACTPIDX,
                                    '' AS TDTINDEX,
                                    '' AS VMTPRMIN,
                                    '' AS DCDREGIM,
                                    '' AS DQHORTRA,
                                    '' AS DQSEMTRA,  --EN BLANCO
                                    '' AS DCAMPANH,
                                    '' AS KACMODAL,
                                    '' AS DENTIDSO,
                                    '' AS DLOCREF,
                                    '' AS KACINTNI,
                                    '' AS KACCLRIS,
                                    '' AS KACAMBCB,
                                    '' AS KACTRAIN,
                                    '' AS DINDCIRS,
                                    '' AS DINCERPA,
                                    '' AS DINDMOTO,
                                    '' AS DMATRIC,
                                    '' AS DINDMARK,
                                    '' AS KACOPCBT,
                                    '' AS VTXINDX,
                                    '' AS DAGRIDAD,   --EN BLANCO
                                    '' AS KACPAIS_DT, --NO
                                    '' AS KACMDAC,    --EN BLANCO
                                    '' AS DIDADECOM,  --PENDIENTE
                                    '' AS VTXPERINDC,
                                    '' AS TPGMYBENEF
                                    FROM USVTIMG01."ROLES" R
                                    JOIN ( SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE"  
                                          FROM USVTIMG01."POLICY" P 
                                          LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                                          ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                          AND P."NBRANCH"  = CERT."NBRANCH"
                                          AND P."NPRODUCT" = CERT."NPRODUCT"
                                          AND P."NPOLICY"  = CERT."NPOLICY"
                                          JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimg01'
                                          WHERE P."SCERTYPE" = '2' 
                                          AND P."SSTATUS_POL" NOT IN ('2','3') 
                                          AND ( (P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                                AND P."DEXPIRDAT" >= '2018-12-31' 
                                                AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '2018-12-31') )
                                                OR 
                                                (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                                AND CERT."DEXPIRDAT" >= '2018-12-31' 
                                                AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '2018-12-31'))
                                          )) AS PC	
                                    ON  R."SCERTYPE"  = PC."SCERTYPE"
                                    AND R."NBRANCH"   = PC."NBRANCH" 
                                    AND R."NPRODUCT"  = PC."NPRODUCT"
                                    AND R."NPOLICY"   = PC."NPOLICY" 
                                    AND R."NCERTIF"   = PC."NCERTIF"  
                                    AND R."DEFFECDATE" <= PC."DSTARTDATE" 
                                    AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > PC."DSTARTDATE")
                                    WHERE R."NROLE" IN (2,8)  
                              ) as tmp
                            '''
    
    L_DF_ABCLRISP_VTIME_LPG = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_ABCLRISP_VTIME_LPG).load()

    print("VTIME LPG")

    L_ABCLRISP_VTIME_LPV = f'''
                           (SELECT
                           'D' INDDETREC, 
                           'ABCLRISP' TABLAIFRS17, 
                           '' AS PK,
                           '' AS DTPREG,
                           '' AS TIOCPROC,
                           '' AS TIOCFRM, --PENDIENTE
                           '' AS TIOCTO,
                           'PVV' AS KGIORIGM,
                           PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" AS KABAPOL,
                           PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" || '-' || R."SCLIENT"  AS KABUNRIS,
                           COALESCE(( SELECT COALESCE(GC."NCOVERGEN", 0)           
                                      FROM USVTIMV01."LIFE_COVER" GC 
                                      JOIN USVTIMV01."COVER" C  
                                      ON  GC."NBRANCH"   = C."NBRANCH"
                                      AND GC."NPRODUCT"  = PC."NPRODUCT"
                                      AND GC."NCURRENCY" = C."NCURRENCY"
                                      AND GC."NMODULEC" =  C."NMODULEC"
                                      AND GC."NCOVER"   =  C."NCOVER"
                                      AND GC."DEFFECDATE" <= PC."DSTARTDATE"
                           		   AND (GC."DNULLDATE" IS NULL OR GC."DNULLDATE" > PC."DSTARTDATE")		       		   
                                      WHERE C."SCERTYPE"    = PC."SCERTYPE" 
                                      AND   C."NBRANCH"     = PC."NBRANCH"
                                      AND   C."NPRODUCT"    = PC."NPRODUCT"
                                      AND   C."NPOLICY"     = PC."NPOLICY"
                                      AND   C."NCERTIF"     = PC."NCERTIF"
                                      AND   C."DEFFECDATE" <= PC."DSTARTDATE"
                                      AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE")
                                      AND  C."NCOVER" = 1 LIMIT 1                                      
                           ) ,'0') AS KGCTPCBT,
                           ROW_NUMBER () OVER (PARTITION  BY PC."NBRANCH", PC."NPRODUCT", PC."NPOLICY", PC."NCERTIF" ORDER BY R."SCLIENT") AS DNPESEG,
                           R."SCLIENT" AS KEBENTID_PS,
                           (SELECT DATE_PART('YEAR', AGE(CURRENT_DATE, CLI."DBIRTHDAT")) FROM USVTIMG01."CLIENT" CLI WHERE CLI."SCLIENT" = R."SCLIENT") AS DIDADEAC,
                           '' AS DANOREF, --EN BLANCO
                           '' AS KACEMPR,
                           '' AS VMTSALAR,
                           '' AS KACTPSAL,
                           '' AS TADMEMP,
                           '' AS TADMGRP,
                           '' AS TSAIDGRP,
                           'LPV' AS DCOMPA,
                           '' AS DMARCA,
                           '' AS TDNASCIM,
                           '' AS DQDIASUB,
                           '' AS DQESPING,
                           '' AS KACSEXO,
                           '' AS KACCAE,
                           '' AS DQTRABAL,
                           '' AS DINAPMEN,
                           '' AS DQCOEFEQ,
                           '' AS DQHORSEM, --EN BLANCO
                           '' AS DQMESES,
                           '' AS VMTALIME, --EN BLANCO
                           '' AS DQMESALI, --EN BLANCO
                           '' AS VMTALOJA,
                           '' AS DQMESALO,
                           '' AS VMTRENUM, --EN BLANCO
                           '' AS DQMESREN,
                           '' AS DQDIATRA,
                           '' AS DQCOPRE,
                           '' AS DITINER,
                           '' AS DNOMES,
                           '' AS KACUTILIZ,
                           '' AS VMTDESCO, --EN BLANCO
                           '' AS DFRANQU,  --EN BLANCO
                           '' AS DTARIFA,
                           '' AS DINTIPEXP,
                           '' AS KACESPAN,
                           '' AS DQANIMAL,
                           '' AS KACTIPSG,
                           '' AS DAPROESC,
                           '' AS DCMUDESC,
                           '' AS DQCAPITA,
                           '' AS VTXCOMPA,
                           '' AS DQCAES,
                           '' AS DINEXTTE,
                           '' AS KACCLTARI,
                           '' AS KACTIPVEI,
                           '' AS KACCATRIS,
                           '' AS DINDCOL,
                           '' AS DACONSTR,
                           '' AS DQPESSO1,
                           '' AS DQPESSO2,
                           '' AS DQVIAS,
                           '' AS KACAGRAV,
                           '' AS KACPARTI,
                           '' AS KACSERMD,
                           '' AS KACMRISC,
                           '' AS DINDCON,
                           '' AS DQPRAZO,
                           R."NROLE" AS KACTPPES,
                           '' AS KACESPES, --EN BLANCO
                           '' AS KACMEPES,
                           '' AS TDESPES,  --EN BLANCO
                           '' AS KACTPPRA,
                           '' AS DEMPREST,
                           '' AS VMTPREST,
                           '' AS VMTEMPRE,
                           '' AS VMTPRCRD,
                           '' AS DCONTCGD,
                           '' AS DNCLICGD,
                           '' AS DCERTIFC,
                           R."DEFFECDATE" AS TINICIO,
                           coalesce(cast(cast(R."DNULLDATE" as date) as varchar), '') AS TTERMO,
                           '' AS DNOMEPAR,
                           '' AS KACPROF,
                           '' AS KACACTIV,
                           '' AS KACSACTIV,
                           '' AS VMTSALMD,
                           '' AS DCODSUB,
                           '' AS KACTPCON,
                           '' AS DAREACCV,
                           '' AS DAREACUL,
                           '' AS KACZONAG,
                           '' AS KACTPIDX,
                           '' AS TDTINDEX,
                           '' AS VMTPRMIN,
                           '' AS DCDREGIM,
                           '' AS DQHORTRA,
                           '' AS DQSEMTRA,  --EN BLANCO
                           '' AS DCAMPANH,
                           '' AS KACMODAL,
                           '' AS DENTIDSO,
                           '' AS DLOCREF,
                           '' AS KACINTNI,
                           '' AS KACCLRIS,
                           '' AS KACAMBCB,
                           '' AS KACTRAIN,
                           '' AS DINDCIRS,
                           '' AS DINCERPA,
                           '' AS DINDMOTO,
                           '' AS DMATRIC,
                           '' AS DINDMARK,
                           '' AS KACOPCBT,
                           '' AS VTXINDX,
                           '' AS DAGRIDAD,   --EN BLANCO
                           '' AS KACPAIS_DT, --NO
                           '' AS KACMDAC,    --EN BLANCO
                           '' AS DIDADECOM,  --PENDIENTE
                           '' AS VTXPERINDC,
                           '' AS TPGMYBENEF
                           FROM USVTIMV01."ROLES" R
                           JOIN ( SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE"  
                                  FROM USVTIMV01."POLICY" P 
                           	   LEFT JOIN USVTIMV01."CERTIFICAT" CERT 
                           	   ON  P."SCERTYPE" = CERT."SCERTYPE" 
                           	   AND P."NBRANCH"  = CERT."NBRANCH"
                           	   AND P."NPRODUCT" = CERT."NPRODUCT"
                           	   AND P."NPOLICY"  = CERT."NPOLICY"
                           	   JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
                           	   WHERE P."SCERTYPE" = '2' 
                                  AND P."SSTATUS_POL" NOT IN ('2','3') 
                                  AND ( (P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                      AND P."DEXPIRDAT" >= '2021-12-31' 
                                      AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '2021-12-31') )
                                      OR 
                                      (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                      AND CERT."DEXPIRDAT" >= '2021-12-31' 
                                      AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '2021-12-31'))
                                      AND p."DSTARTDATE" between '{P_FECHA_INICIO}' and '{P_FECHA_FIN}'
                                 )) AS PC	
                           ON  R."SCERTYPE"  = PC."SCERTYPE"
                           AND R."NBRANCH"   = PC."NBRANCH" 
                           AND R."NPRODUCT"  = PC."NPRODUCT"
                           AND R."NPOLICY"   = PC."NPOLICY" 
                           AND R."NCERTIF"   = PC."NCERTIF"  
                           AND R."DEFFECDATE" <= PC."DSTARTDATE" 
                           AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > PC."DSTARTDATE")
                           WHERE R."NROLE" IN (2,8) LIMIT 100) AS VTIME_LPV
                           '''
    
    L_DF_ABCLRISP_VTIME_LPV = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_ABCLRISP_VTIME_LPV).load()
    
    print("VTIME LPV")

    #----------------------------------------------------------------------------------------------------------------------------------#

    L_ABCLRISP_INSIS_LPV = f'''
                           (
                              SELECT
                              'D' INDDETREC, 
                              'ABCLRISP' TABLAIFRS17, 
                              '' AS PK,
                              '' AS DTPREG,
                              '' AS TIOCPROC,
                              IO."INSR_BEGIN" AS TIOCFRM,
                              '' AS TIOCTO,
                              'PNV' AS KGIORIGM,
                              CASE COALESCE(PP."ENG_POL_TYPE", '')
                                                WHEN 'DEPENDENT' THEN COALESCE(P."ATTR1", '0') || '-' || COALESCE(P."ATTR2", '0') || '-' || COALESCE (P."POLICY_NO", '0') || '-' || COALESCE (P."POLICY_ID")
                                                ELSE ''
                                                END KABAPOL,
                              IO."OBJECT_ID" || '-' || P."ATTR1" || '-' || P."INSR_TYPE" || '-' || P."POLICY_NO" || '-' || P."POLICY_ID" AS KABUNRIS,
                              '' AS KGCTPCBT, --EN BLANCO
                              ROW_NUMBER () OVER (PARTITION  BY P."ATTR1", P."ATTR2", P."POLICY_ID", P."POLICY_NO" /*ORDER BY R."SCLIENT"*/) AS DNPESEG,
                              (
                              SELECT ILPI."LEGACY_ID" 
                              FROM USINSIV01."INTRF_LPV_PEOPLE_IDS" ILPI
                              WHERE ILPI."MAN_ID" = OA."MAN_ID"
                              )
                              AS KEBENTID_PS,
                              '' AS DIDADEAC, --PENDIENTE
                              '' AS DANOREF,  --EN BLANCO
                              '' AS KACEMPR,
                              OA."OAIP1" AS VMTSALAR,
                              '' AS KACTPSAL,
                              '' AS TADMEMP,
                              IO."INSR_BEGIN" AS TADMGRP,
                              '' AS TSAIDGRP,
                              'LPV' AS DCOMPA,
                              '' AS DMARCA,
                              '' AS TDNASCIM,
                              '' AS DQDIASUB,
                              '' AS DQESPING,
                              '' AS KACSEXO,
                              '' AS KACCAE,
                              '' AS DQTRABAL,
                              '' AS DINAPMEN,
                              '' AS DQCOEFEQ,
                              '' AS DQHORSEM, --EN BLANCO
                              '' AS DQMESES,
                              '' AS VMTALIME, --EN BLANCO
                              '' AS DQMESALI, --EN BLANCO
                              '' AS VMTALOJA,
                              '' AS DQMESALO,
                              '' AS VMTRENUM, --EN BLANCO
                              '' AS DQMESREN,
                              '' AS DQDIATRA,
                              '' AS DQCOPRE,
                              '' AS DITINER,
                              '' AS DNOMES,
                              '' AS KACUTILIZ,
                              '' AS VMTDESCO, --EN BLANCO
                              '' AS DFRANQU,  --EN BLANCO
                              '' AS DTARIFA,
                              '' AS DINTIPEXP,
                              '' AS KACESPAN,
                              '' AS DQANIMAL,
                              '' AS KACTIPSG,
                              '' AS DAPROESC,
                              '' AS DCMUDESC,
                              '' AS DQCAPITA,
                              '' AS VTXCOMPA,
                              '' AS DQCAES,
                              '' AS DINEXTTE,
                              '' AS KACCLTARI,
                              '' AS KACTIPVEI,
                              '' AS KACCATRIS,
                              '' AS DINDCOL,
                              '' AS DACONSTR,
                              '' AS DQPESSO1,
                              '' AS DQPESSO2,
                              '' AS DQVIAS,
                              '' AS KACAGRAV,
                              '' AS KACPARTI,
                              '' AS KACSERMD,
                              '' AS KACMRISC,
                              '' AS DINDCON,
                              '' AS DQPRAZO,
                              '' AS KACTPPES,
                              IO."OBJECT_STATE" AS KACESPES, --EN BLANCO
                              '' AS KACMEPES,
                              '' AS TDESPES,  --EN BLANCO
                              '' AS KACTPPRA,
                              '' AS DEMPREST,
                              '' AS VMTPREST,
                              '' AS VMTEMPRE,
                              '' AS VMTPRCRD,
                              '' AS DCONTCGD,
                              '' AS DNCLICGD,
                              '' AS DCERTIFC,
                              '' AS TINICIO,  --EN BLANCO
                              ''  AS TTERMO,  --EN BLANCO
                              '' AS DNOMEPAR,
                              '' AS KACPROF,
                              '' AS KACACTIV,
                              '' AS KACSACTIV,
                              '' AS VMTSALMD,
                              '' AS DCODSUB,
                              '' AS KACTPCON,
                              '' AS DAREACCV,
                              '' AS DAREACUL,
                              '' AS KACZONAG,
                              '' AS KACTPIDX,
                              '' AS TDTINDEX,
                              '' AS VMTPRMIN,
                              '' AS DCDREGIM,
                              '' AS DQHORTRA,
                              '' AS DQSEMTRA,  --EN BLANCO
                              '' AS DCAMPANH,
                              '' AS KACMODAL,
                              '' AS DENTIDSO,
                              '' AS DLOCREF,
                              '' AS KACINTNI,
                              '' AS KACCLRIS,
                              '' AS KACAMBCB,
                              '' AS KACTRAIN,
                              '' AS DINDCIRS,
                              '' AS DINCERPA,
                              '' AS DINDMOTO,
                              '' AS DMATRIC,
                              '' AS DINDMARK,
                              '' AS KACOPCBT,
                              '' AS VTXINDX,
                              '' AS DAGRIDAD,   --EN BLANCO
                              '' AS KACPAIS_DT, --NO
                              '' AS KACMDAC,    --EN BLANCO
                              OA."AGE" AS DIDADECOM,  
                              '' AS VTXPERINDC,
                              '' AS TPGMYBENEF
                              FROM USINSIV01."INSURED_OBJECT" IO
                              JOIN USINSIV01."O_ACCINSURED" OA ON OA."OBJECT_ID" = IO."OBJECT_ID"
                              JOIN USINSIV01."POLICY" P on P."POLICY_ID" = IO."POLICY_ID" and P."INSR_TYPE" = IO."INSR_TYPE"
                              LEFT JOIN USINSIV01."POLICY_ENG_POLICIES" PP ON P."POLICY_ID" = PP."POLICY_ID"
                              WHERE P."INSR_END" >= '2021-12-31'
                           ) AS PNV
                           '''
    
    L_DF_ABCLRISP_INSIS_LPV = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_ABCLRISP_INSIS_LPV).load()

    print("INSIS LPV")
    
    L_DF_ABCLRISP = L_DF_ABCLRISP_INSUNIX_LPG.union(L_DF_ABCLRISP_INSUNIX_LPV).union(L_DF_ABCLRISP_VTIME_LPG).union(L_DF_ABCLRISP_VTIME_LPV).union(L_DF_ABCLRISP_INSIS_LPV)

    return L_DF_ABCLRISP