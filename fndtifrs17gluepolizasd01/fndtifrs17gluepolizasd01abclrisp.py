def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):
    L_ABCLRISP_INSUNIX_LPG = f'''
                             (SELECT
                             '' AS PK,
                             '' AS DTPREG,
                             '' AS TIOCPROC,
                             '' AS TIOCFRM, --PENDIENTE
                             '' AS TIOCTO,
                             '' AS KGIORIGM,
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
                             		   AND (GC.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE)		       		   
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
                             R.NULLDATE  AS TTERMO,
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
                                   ) AND P.EFFECDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}') AS PC	
                             ON  R.USERCOMP = PC.USERCOMP 
                             AND R.COMPANY  = PC.COMPANY 
                             AND R.CERTYPE  = PC.CERTYPE
                             AND R.BRANCH   = PC.BRANCH 
                             AND R.POLICY   = PC.POLICY 
                             AND R.CERTIF   = PC.CERTIF  
                             AND R.EFFECDATE <= PC.EFFECDATE 
                             AND (R.NULLDATE IS NULL OR R.NULLDATE > PC.EFFECDATE)
                             WHERE R.ROLE IN (2,8)) AS PIG
                             '''
    
    L_DF_ABCLRISP_INSUNIX_LPG = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_ABCLRISP_INSUNIX_LPG).load()