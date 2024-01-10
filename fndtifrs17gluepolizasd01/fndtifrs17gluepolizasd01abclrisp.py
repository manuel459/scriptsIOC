from pyspark.sql.types import StringType , DateType
from pyspark.sql.functions import col , coalesce , lit , format_number

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

    l_fecha_carga_inicial = '2021-12-31'
    
    l_abclrisp_insunix_lpg = f'''
                             (SELECT
                             'D' INDDETREC, 
                             'ABCLRISP' TABLAIFRS17, 
                             '' AS PK,
                             '' AS DTPREG,
                             '' AS TIOCPROC,
                             R.COMPDATE AS TIOCFRM,
                             '' AS TIOCTO,
                             'PIG' AS KGIORIGM,
                             PC.BRANCH ||'-'|| COALESCE (PC.PRODUCT, 0) ||'-'|| COALESCE(PC.SUB_PRODUCT, 0) ||'-'|| PC.POLICY ||'-'|| PC.CERTIF AS KABAPOL,
                             PC.BRANCH ||'-'|| COALESCE (PC.PRODUCT, 0) ||'-'|| PC.POLICY ||'-'|| PC.CERTIF || '-' || (SELECT EVI.SCOD_VT  FROM USINSUG01.EQUI_VT_INX EVI WHERE EVI.SCOD_INX = R.CLIENT)  AS KABUNRIS,
                             CASE PC.POLITYPE
                             WHEN '1' THEN ( SELECT COALESCE(GC.COVERGEN, 0) ||'-'|| COALESCE(GC.CURRENCY, 0)
                                                    FROM USINSUG01.GEN_COVER GC 
                                                    JOIN USINSUG01.COVER C  
                                                    ON  GC.USERCOMP    = C.USERCOMP 
                                                    AND GC.COMPANY     = C.COMPANY 
                                                    AND GC.BRANCH      = C.BRANCH
                                                    AND GC.PRODUCT     = PC.PRODUCT
                                                    AND GC.SUB_PRODUCT = PC.SUB_PRODUCT
                                                    AND GC.CURRENCY = C.CURRENCY
                                                    AND GC.MODULEC  =  C.MODULEC
                                                    AND GC.COVER    =  C.COVER
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
                                                    AND  C.COVER = 1 LIMIT 1) 
                             ELSE   ( SELECT COALESCE(GC.COVERGEN, 0) ||'-'|| COALESCE(GC.CURRENCY, 0)
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
                                                    AND GC.EFFECDATE <= PC.EFFECDATE_CERT
                             	            	    AND (GC.NULLDATE IS NULL OR GC.NULLDATE > PC.EFFECDATE_CERT)		       		   
                                                    WHERE C.USERCOMP   = PC.USERCOMP 
                                                    AND   C.COMPANY    = PC.COMPANY 
                                                    AND   C.CERTYPE    = '2' 
                                                    AND   C.BRANCH     = PC.BRANCH 
                                                    AND   C.POLICY     = PC.POLICY
                                                    AND   C.CERTIF     = PC.CERTIF  
                                                    AND   C.EFFECDATE <= PC.EFFECDATE_CERT
                                                    AND  (C.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE_CERT)
                                                    AND  C.COVER = 1 LIMIT 1
                                          )
                             END AS KGCTPCBT,
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
                             JOIN 
                             ( 
                               (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
                               FROM USINSUG01.POLICY P 
                             	 LEFT JOIN USINSUG01.CERTIFICAT CERT ON P.USERCOMP = CERT.USERCOMP AND P.COMPANY = CERT.COMPANY AND P.CERTYPE = CERT.CERTYPE AND P.BRANCH  = CERT.BRANCH AND P.POLICY  = CERT.policy
                             	 JOIN USINSUG01.POL_SUBPRODUCT PSP   ON  PSP.USERCOMP = P.USERCOMP AND PSP.COMPANY  = P.COMPANY AND PSP.CERTYPE  = P.CERTYPE AND PSP.BRANCH   = P.BRANCH		    AND PSP.PRODUCT  = P.PRODUCT AND PSP.POLICY   = P.POLICY	
                             	 JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                               (SELECT     unnest(ARRAY['usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                      'usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                      'usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                      'usinsug01','usinsug01','usinsug01']) AS "SOURCESCHEMA",  
				             unnest(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
				 	      unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR 
                               ON RTR."BRANCHCOM" = P.BRANCH 
                               AND  RTR."RISKTYPEN" = 1 
                               AND RTR."SOURCESCHEMA" = 'usinsug01'
                             	 WHERE P.CERTYPE = '2' 
                               AND P.STATUS_POL NOT IN ('2','3') 
                               AND ((P.POLITYPE = '1' -- INDIVIDUAL 
                                   AND P.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                   AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}') )
                                   OR 
                                   (P.POLITYPE <> '1' -- COLECTIVAS 
                                   AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                   AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}')))
                               AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')

                               /*UNION 

                               (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
                               FROM USINSUG01.POLICY P 
                             	 LEFT JOIN USINSUG01.CERTIFICAT CERT ON P.USERCOMP = CERT.USERCOMP AND P.COMPANY = CERT.COMPANY AND P.CERTYPE = CERT.CERTYPE AND P.BRANCH  = CERT.BRANCH AND P.POLICY  = CERT.policy
                             	 JOIN USINSUG01.POL_SUBPRODUCT PSP   ON  PSP.USERCOMP = P.USERCOMP AND PSP.COMPANY  = P.COMPANY AND PSP.CERTYPE  = P.CERTYPE AND PSP.BRANCH   = P.BRANCH		    AND PSP.PRODUCT  = P.PRODUCT AND PSP.POLICY   = P.POLICY	
                             	 JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                               (SELECT    unnest(ARRAY['usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                         'usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                         'usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                         'usinsug01','usinsug01','usinsug01']) AS "SOURCESCHEMA",  
				                  unnest(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
				 	            unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR 
                               ON RTR."BRANCHCOM" = P.BRANCH 
                               AND  RTR."RISKTYPEN" = 1 
                               AND RTR."SOURCESCHEMA" = 'usinsug01'
                             	 WHERE P.CERTYPE  = '2' 
                               AND P.STATUS_POL NOT IN ('2', '3') 
                               AND (((P.POLITYPE = '1' AND  P.EXPIRDAT < '{l_fecha_carga_inicial}' OR P.NULLDATE < '{l_fecha_carga_inicial}')
                               AND EXISTS (SELECT 1 FROM  USINSUG01.CLAIM CLA    
                                           JOIN  USINSUV01.CLAIM_HIS CLH 
                                           ON CLH.USERCOMP = CLA.USERCOMP 
                                           AND CLH.COMPANY = CLA.COMPANY 
                                           AND CLH.BRANCH = CLA.BRANCH 
                                           AND CLH.CLAIM = CLA.CLAIM
                                           WHERE CLA.BRANCH = P.BRANCH 
                                           AND CLA.POLICY = P.POLICY 
                                           AND TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2))
                                      		                       FROM 	USINSUG01.TAB_CL_OPE TCL
                                      		                       WHERE  (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                           AND  CLH.OPERDATE >= '{l_fecha_carga_inicial}'
                               AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'))))

                               UNION

                               (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
                                FROM USINSUG01.POLICY P 
                             	  LEFT JOIN USINSUG01.CERTIFICAT CERT ON P.USERCOMP = CERT.USERCOMP AND P.COMPANY = CERT.COMPANY AND P.CERTYPE = CERT.CERTYPE AND P.BRANCH  = CERT.BRANCH AND P.POLICY  = CERT.policy
                             	  JOIN USINSUG01.POL_SUBPRODUCT PSP   ON  PSP.USERCOMP = P.USERCOMP AND PSP.COMPANY  = P.COMPANY AND PSP.CERTYPE  = P.CERTYPE AND PSP.BRANCH = P.BRANCH AND PSP.PRODUCT = P.PRODUCT AND PSP.POLICY   = P.POLICY	
                             	  JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                (SELECT unnest(ARRAY['usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                                              'usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                                              'usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                                              'usinsug01','usinsug01','usinsug01']) AS "SOURCESCHEMA",  
				                unnest(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
				 	          unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR 
                                ON RTR."BRANCHCOM" = P.BRANCH 
                                AND  RTR."RISKTYPEN" = 1 
                                AND RTR."SOURCESCHEMA" = 'usinsug01'
                                WHERE P.CERTYPE  = '2' 
                                AND P.STATUS_POL NOT IN ('2', '3')
                                AND (((P.POLITYPE <> '1' AND CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  OR  CERT.NULLDATE < '{l_fecha_carga_inicial}') 
                                AND EXISTS (SELECT 1 FROM  USINSUG01.CLAIM CLA    
                                            JOIN  USINSUG01.CLAIM_HIS CLH  ON CLA.USERCOMP = CLH.USERCOMP AND CLA.COMPANY = CLH.COMPANY AND CLA.BRANCH = CLH.BRANCH  AND CLH.CLAIM = CLA.CLAIM
                                            WHERE CLA.BRANCH   = CERT.BRANCH
                                            AND   CLA.POLICY   = CERT.POLICY
                                            AND   CLA.CERTIF   = CERT.CERTIF
                                            AND   TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) 
                                                                          FROM USINSUG01.TAB_CL_OPE TCL 
                                                                          WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                            AND  CLH.OPERDATE >= '{l_fecha_carga_inicial}')))
                                AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')*/
                             ) AS PC	
                             ON  R.USERCOMP = PC.USERCOMP 
                             AND R.COMPANY  = PC.COMPANY 
                             AND R.CERTYPE  = PC.CERTYPE 
                             AND R.BRANCH   = PC.BRANCH 
                             AND R.POLICY   = PC.POLICY 
                             AND R.CERTIF   = PC.CERTIF  
                             AND R.EFFECDATE <= PC.EFFECDATE 
                             AND (R.NULLDATE IS NULL OR R.NULLDATE > PC.EFFECDATE)
                             AND R.ROLE IN (2,8)
                             LIMIT 100) AS PIG
                             '''
    
    l_df_abclrisp_insunix_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insunix_lpg).load()

    print("INSUNIX LPG")

    l_abclrisp_insunix_lpv = f'''
                             (
                               SELECT
                                    'D' INDDETREC, 
                                    'ABCLRISP' TABLAIFRS17, 
                                    '' AS PK,
                                    '' AS DTPREG,
                                    '' AS TIOCPROC,
                                    R.COMPDATE AS TIOCFRM,
                                    '' AS TIOCTO,
                                    'PIV' AS KGIORIGM,
                                    PC.BRANCH ||'-'|| COALESCE (PC.PRODUCT, 0) ||'-'|| PC.POLICY ||'-'|| PC.CERTIF AS KABAPOL,
                                    PC.BRANCH ||'-'|| COALESCE (PC.PRODUCT, 0) ||'-'|| PC.POLICY ||'-'|| PC.CERTIF || '-' || COALESCE((SELECT EVI.SCOD_VT FROM USINSUG01.EQUI_VT_INX EVI WHERE EVI.SCOD_INX = R.CLIENT), '0') AS KABUNRIS,
                                    case PC.POLITYPE when  '1'
                                    then                                    
                                    ( SELECT COALESCE(GC.COVERGEN, 0) ||'-'|| COALESCE(GC.CURRENCY, 0)
                                          FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_INXLPV*/
                                          (SELECT GC.USERCOMP,
										  GC.COMPANY,GC.BRANCH,GC.PRODUCT,GC.CURRENCY,
										  GC.MODULEC,GC.COVER,GC.EFFECDATE,GC.NULLDATE,
										  GC.COVERGEN
										  FROM USINSUV01.GEN_COVER GC
										  UNION 
										  SELECT LC.USERCOMP,
										  LC.COMPANY,LC.BRANCH,LC.PRODUCT,LC.CURRENCY,
										  0 AS MODULEC,LC.COVER,LC.EFFECDATE,LC.NULLDATE,
										  LC.COVERGEN
										  FROM USINSUV01.LIFE_COVER LC) GC 
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
                                           ) else
		                                           ( SELECT COALESCE(GC.COVERGEN, 0) ||'-'|| COALESCE(GC.CURRENCY, 0)
		                                          FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_INXLPV*/
		                                          (SELECT GC.USERCOMP,
												  GC.COMPANY,GC.BRANCH,GC.PRODUCT,GC.CURRENCY,
												  GC.MODULEC,GC.COVER,GC.EFFECDATE,GC.NULLDATE,
												  GC.COVERGEN
												  FROM USINSUV01.GEN_COVER GC
												  UNION 
												  SELECT LC.USERCOMP,
												  LC.COMPANY,LC.BRANCH,LC.PRODUCT,LC.CURRENCY,
												  0 AS MODULEC,LC.COVER,LC.EFFECDATE,LC.NULLDATE,
												  LC.COVERGEN
												  FROM USINSUV01.LIFE_COVER LC) GC 
		                                          JOIN USINSUV01.COVER C  
		                                          ON  GC.USERCOMP = C.USERCOMP 
		                                          AND GC.COMPANY  = C.COMPANY 
		                                          AND GC.BRANCH   = C.BRANCH
		                                          AND GC.PRODUCT  = PC.PRODUCT
		                                          --AND GC.SUB_PRODUCT = PC.SUB_PRODUCT
		                                          AND GC.CURRENCY = C.CURRENCY
		                                          --AND GC.MODULEC =  C.MODULEC
		                                          AND GC.COVER   =  C.COVER
		                                          AND GC.EFFECDATE <= PC.EFFECDATE_CERT
		                                          AND (GC.NULLDATE IS NULL OR GC.NULLDATE > PC.EFFECDATE_CERT)		       		   
		                                          WHERE C.USERCOMP   = PC.USERCOMP 
		                                          AND   C.COMPANY    = PC.COMPANY 
		                                          AND   C.CERTYPE    = '2' 
		                                          AND   C.BRANCH     = PC.BRANCH 
		                                          AND   C.POLICY     = PC.POLICY
		                                          AND   C.CERTIF     = PC.CERTIF  
		                                          AND   C.EFFECDATE <= PC.EFFECDATE_CERT
		                                          AND  (C.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE_CERT)
		                                          AND  C.COVER = 1 limit 1)
                                          END AS KGCTPCBT,
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
                                    JOIN 
                                    (   
                                        (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE , P.POLITYPE, CERT.EFFECDATE as EFFECDATE_CERT
                                          FROM USINSUV01.POLICY P 
                                          LEFT JOIN USINSUV01.CERTIFICAT CERT 
                                          ON P.USERCOMP = CERT.USERCOMP 
                                          AND P.COMPANY = CERT.COMPANY 
                                          AND P.CERTYPE = CERT.CERTYPE 
                                          AND P.BRANCH  = CERT.BRANCH 
                                          AND P.POLICY  = CERT.policy	
                                          JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                          (SELECT unnest(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                 'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                 'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                 'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
						                          unnest(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
							                      unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P.BRANCH AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                          WHERE P.CERTYPE = '2' 
                                          AND P.STATUS_POL NOT IN ('2','3') 
                                          AND ( (P.POLITYPE = '1' -- INDIVIDUAL 
                                          AND P.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                          AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}') )
                                          OR 
                                          (P.POLITYPE <> '1' -- COLECTIVAS 
                                          AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                          AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}'))))

                                          /*UNION 

                                          (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
                                           FROM USINSUV01.POLICY P 
                             	             LEFT JOIN USINSUV01.CERTIFICAT CERT ON P.USERCOMP = CERT.USERCOMP AND P.COMPANY = CERT.COMPANY AND P.CERTYPE = CERT.CERTYPE AND P.BRANCH  = CERT.BRANCH AND P.POLICY  = CERT.policy                             	           
                             	             JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                           (SELECT  unnest(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
						                            unnest(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
							                        unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P.BRANCH AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usinsuv01'
                             	             WHERE P.CERTYPE  = '2' 
                                           AND P.STATUS_POL NOT IN ('2', '3') 
                                           AND (((P.POLITYPE = '1' AND  P.EXPIRDAT < '2021-12-31' OR P.NULLDATE < '2021-12-31')
                                           AND EXISTS (SELECT 1 FROM USINSUV01.CLAIM CLA    
                                                       JOIN  USINSUV01.CLAIM_HIS CLH 
                                                       ON CLH.USERCOMP = CLA.USERCOMP 
                                                       AND CLH.COMPANY = CLA.COMPANY 
                                                       AND CLH.BRANCH = CLA.BRANCH 
                                                       AND CLH.CLAIM = CLA.CLAIM
                                                       WHERE CLA.BRANCH = P.BRANCH 
                                                       AND CLA.POLICY = P.POLICY 
                                                       AND TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2))
                                                  		                       FROM USINSUG01.TAB_CL_OPE TCL
                                                  		                       WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                                       AND  CLH.OPERDATE >= '{l_fecha_carga_inicial}'
                                           AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'))))

                                           UNION

                                           (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
                                            FROM USINSUV01.POLICY P 
                             	              LEFT JOIN USINSUV01.CERTIFICAT CERT ON P.USERCOMP = CERT.USERCOMP AND P.COMPANY = CERT.COMPANY AND P.CERTYPE = CERT.CERTYPE AND P.BRANCH  = CERT.BRANCH AND P.POLICY  = CERT.policy                             	              
                             	              JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                              (SELECT  unnest(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
						                            unnest(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
							                        unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P.BRANCH AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                            WHERE P.CERTYPE  = '2' 
                                            AND P.STATUS_POL NOT IN ('2', '3')
                                            AND (((P.POLITYPE <> '1' AND CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  OR  CERT.NULLDATE < '{l_fecha_carga_inicial}') 
                                            AND EXISTS (SELECT 1 FROM  USINSUV01.CLAIM CLA    
                                                        JOIN  USINSUV01.CLAIM_HIS CLH  
                                                        ON CLA.USERCOMP = CLH.USERCOMP 
                                                        AND CLA.COMPANY = CLH.COMPANY 
                                                        AND CLA.BRANCH = CLH.BRANCH  
                                                        AND CLH.CLAIM = CLA.CLAIM
                                                        WHERE CLA.BRANCH   = CERT.BRANCH
                                                        AND   CLA.POLICY   = CERT.POLICY
                                                        AND   CLA.CERTIF   = CERT.CERTIF
                                                        AND   TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) 
                                                                                      FROM  USINSUG01.TAB_CL_OPE TCL 
                                                                                      WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                                        AND  CLH.OPERDATE >= '{l_fecha_carga_inicial}')))
                                            AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')*/                                          
                                    ) AS PC	
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
                                    AND PC.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' 
                                    LIMIT 100      
                             ) AS TMP
                             '''
    
    l_df_abclrisp_insunix_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insunix_lpv).load()

    print("INSUNIX LPV")
    #----------------------------------------------------------------------------------------------------------------------------------#

    l_abclrisp_vtime_lpg = f'''
                              ( SELECT
                               'D' INDDETREC, 
                               'ABCLRISP' TABLAIFRS17, 
                               '' AS PK,
                               '' AS DTPREG,
                               '' AS TIOCPROC,
                               R."DCOMPDATE" AS TIOCFRM,
                               '' AS TIOCTO,
                               'PVG' AS KGIORIGM,
                               PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" AS KABAPOL,
                               PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" || '-' || R."SCLIENT"  AS KABUNRIS,
                               case PC."SPOLITYPE"  when '1' 
                               then
                               (SELECT COALESCE(cast(GLC."NCOVERGEN" as varchar), '0')
                                              FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER GLC*/
                                              (SELECT GC."NBRANCH",
                                              GC."NPRODUCT",GC."NMODULEC",GC."NCOVER",
                                              GC."DEFFECDATE",GC."DNULLDATE",GC."NCOVERGEN",
                                              GC."NCURRENCY"
										  FROM USVTIMG01."GEN_COVER" GC
										  UNION 
										  SELECT LC."NBRANCH",
                                              LC."NPRODUCT",LC."NMODULEC",LC."NCOVER",
                                              LC."DEFFECDATE",LC."DNULLDATE",LC."NCOVERGEN",
                                              LC."NCURRENCY"
										  FROM USVTIMG01."LIFE_COVER" LC) GLC
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
                               ) else (SELECT COALESCE(cast(GLC."NCOVERGEN" as varchar), '0')
                                              FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER GLC*/
                                              (SELECT GC."NBRANCH",
                                              GC."NPRODUCT",GC."NMODULEC",GC."NCOVER",
                                              GC."DEFFECDATE",GC."DNULLDATE",GC."NCOVERGEN",
                                              GC."NCURRENCY"
										  FROM USVTIMG01."GEN_COVER" GC
										  UNION 
										  SELECT LC."NBRANCH",
                                              LC."NPRODUCT",LC."NMODULEC",LC."NCOVER",
                                              LC."DEFFECDATE",LC."DNULLDATE",LC."NCOVERGEN",
                                              LC."NCURRENCY"
										  FROM USVTIMG01."LIFE_COVER" LC) GLC
                                              JOIN USVTIMG01."COVER" C  
                                              ON  GLC."NBRANCH"   = C."NBRANCH"
                                              AND GLC."NPRODUCT"  = PC."NPRODUCT"
                                              AND GLC."NCURRENCY" = C."NCURRENCY"
                                              AND GLC."NMODULEC" =  C."NMODULEC"
                                              AND GLC."NCOVER"   =  C."NCOVER"
                                              AND GLC."DEFFECDATE" <= PC."DSTARTDATE_CERT"
                                              AND (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > PC."DSTARTDATE_CERT")		       		   
                                              WHERE C."SCERTYPE"    = PC."SCERTYPE" 
                                              AND   C."NBRANCH"     = PC."NBRANCH"
                                              AND   C."NPRODUCT"    = PC."NPRODUCT"
                                              AND   C."NPOLICY"     = PC."NPOLICY"
                                              AND   C."NCERTIF"     = PC."NCERTIF"
                                              AND   C."DEFFECDATE" <= PC."DSTARTDATE_CERT"
                                              AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE_CERT")
                                              AND  C."NCOVER" = 1
                               )
                               END AS KGCTPCBT,
                               ROW_NUMBER () OVER (PARTITION  BY PC."NBRANCH", PC."NPRODUCT", PC."NPOLICY", PC."NCERTIF" ORDER BY R."SCLIENT") AS DNPESEG,
                               R."SCLIENT" AS KEBENTID_PS,
                               (SELECT DATE_PART('YEAR', AGE(CURRENT_DATE, CLI."DBIRTHDAT")) FROM USVTIMG01."CLIENT" CLI WHERE CLI."SCLIENT" = R."SCLIENT") AS DIDADEAC,
                               '' AS DANOREF, --EN BLANCO
                               '' AS KACEMPR,
                               0 AS VMTSALAR,
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
                               coalesce(cast(cast(R."DEFFECDATE" as date) as varchar), '') AS TINICIO,
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
                               FROM USVTIMG01."ROLES" R
                               JOIN 
                               (     (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
                                     FROM USVTIMG01."POLICY" P 
                                     LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                                     ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                     AND P."NBRANCH"  = CERT."NBRANCH"
                                     AND P."NPRODUCT" = CERT."NPRODUCT"
                                     AND P."NPOLICY"  = CERT."NPOLICY"
                                     JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                     (SELECT unnest(ARRAY['usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01',
                                                          'usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01']) AS "SOURCESCHEMA",  
			                      unnest(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
			       	         unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimg01'
                                     WHERE P."SCERTYPE" = '2' 
                                     AND P."SSTATUS_POL" NOT IN ('2','3') 
                                     AND ( (P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                           AND P."DEXPIRDAT" >= '2018-12-31'
                                           AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '2018-12-31') )
                                           OR 
                                           (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                           AND CERT."DEXPIRDAT" >= '2018-12-31' 
                                           AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '2018-12-31'))))
                                     
                                     /*UNION

                                     (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                      FROM USVTIMG01."POLICY" P 
                                      JOIN USVTIMG01."CERTIFICAT" CERT 
                                      ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                      AND P."NBRANCH"  = CERT."NBRANCH"
                                      AND P."NPRODUCT" = CERT."NPRODUCT"
                                      AND P."NPOLICY"  = CERT."NPOLICY"
                                      JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                      (SELECT UNNEST(ARRAY['USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01',
                                                          'USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01']) AS "SOURCESCHEMA",  
                                              UNNEST(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
                                              UNNEST(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'USVTIMG01'
                                      JOIN USVTIMG01."CLAIM" CLA ON CLA."SCERTYPE" = POL."SCERTYPE" AND CLA."NPOLICY" = POL."NPOLICY" AND CLA."NBRANCH" = POL."NBRANCH"
                                      JOIN (
                                             SELECT DISTINCT CLH."NCLAIM" 
                                             FROM (
                                                    SELECT CAST("SVALUE" AS INT4) "SVALUE" 
                                                    FROM USVTIMG01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)
                                                  ) CSV 
                                             JOIN USVTIMG01."CLAIM_HIS" CLH 
                                             ON COALESCE(CLH."NCLAIM", 0) > 0 
                                             AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                             AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                           ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                      WHERE P."SCERTYPE" = '2' 
                                      AND P."SSTATUS_POL" NOT IN ('2','3') 
                                      AND P."SPOLITYPE" = '1' 
                                      AND (P."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR P."DNULLDATE" < '{l_fecha_carga_inicial}'))


                                     UNION

                                     (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                      FROM USVTIMG01."POLICY" P 
                                      JOIN USVTIMG01."CERTIFICAT" CERT 
                                      ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                      AND P."NBRANCH"  = CERT."NBRANCH"
                                      AND P."NPRODUCT" = CERT."NPRODUCT"
                                      AND P."NPOLICY"  = CERT."NPOLICY"
                                      JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                      (SELECT UNNEST(ARRAY['USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01',
                                                          'USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01']) AS "SOURCESCHEMA",  
                                              UNNEST(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
                                              UNNEST(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'USVTIMG01'
                                      JOIN USVTIMG01."CLAIM" CLA ON CLA."SCERTYPE" = CERT."SCERTYPE" AND CLA."NBRANCH" = CERT."NBRANCH" AND CLA."NPOLICY" = CERT."NPOLICY"  AND CLA."NCERTIF" =  CERT."NCERTIF"
                                      JOIN (
                                            SELECT DISTINCT CLH."NCLAIM" 
                                            FROM (
                                                  SELECT CAST("SVALUE" AS INT4) "SVALUE" 
                                                  FROM USVTIMG01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)
                                                 ) CSV 
                                            JOIN USVTIMG01."CLAIM_HIS" CLH 
                                            ON COALESCE(CLH."NCLAIM", 0) > 0 
                                            AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                            AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                           ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                      WHERE P."SCERTYPE" = '2' 
                                      AND P."SSTATUS_POL" NOT IN ('2','3') 
                                      AND P."SPOLITYPE" <> '1' 
                                      AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}'))*/
                               ) AS PC	
                               ON  R."SCERTYPE"  = PC."SCERTYPE"
                               AND R."NBRANCH"   = PC."NBRANCH" 
                               AND R."NPRODUCT"  = PC."NPRODUCT"
                               AND R."NPOLICY"   = PC."NPOLICY" 
                               AND R."NCERTIF"   = PC."NCERTIF"  
                               AND R."DEFFECDATE" <= PC."DSTARTDATE" 
                               AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > PC."DSTARTDATE")
                               WHERE R."NROLE" IN (2,8) 
                               LIMIT 100
                              ) AS TMP
                            '''
    
    l_df_abclrisp_vtime_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_vtime_lpg).load()

    print("VTIME LPG")

    l_abclrisp_vtime_lpv = f'''
                           (SELECT
                           'D' INDDETREC, 
                           'ABCLRISP' TABLAIFRS17, 
                           '' AS PK,
                           '' AS DTPREG,
                           '' AS TIOCPROC,
                           R."DCOMPDATE" AS TIOCFRM,
                           '' AS TIOCTO,
                           'PVV' AS KGIORIGM,
                           PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" AS KABAPOL,
                           PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" || '-' || R."SCLIENT"  AS KABUNRIS,
                           case PC."SPOLITYPE"  when '1' 
                           then
                           ( SELECT COALESCE(cast(GC."NCOVERGEN" as varchar), '0')
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
                           ) else ( SELECT COALESCE(cast(GC."NCOVERGEN" as varchar), '0')
                                      FROM USVTIMV01."LIFE_COVER" GC 
                                      JOIN USVTIMV01."COVER" C  
                                      ON  GC."NBRANCH"   = C."NBRANCH"
                                      AND GC."NPRODUCT"  = PC."NPRODUCT"
                                      AND GC."NCURRENCY" = C."NCURRENCY"
                                      AND GC."NMODULEC" =  C."NMODULEC"
                                      AND GC."NCOVER"   =  C."NCOVER"
                                      AND GC."DEFFECDATE" <= PC."DSTARTDATE_CERT"
                           		   AND (GC."DNULLDATE" IS NULL OR GC."DNULLDATE" > PC."DSTARTDATE_CERT")		       		   
                                      WHERE C."SCERTYPE"    = PC."SCERTYPE" 
                                      AND   C."NBRANCH"     = PC."NBRANCH"
                                      AND   C."NPRODUCT"    = PC."NPRODUCT"
                                      AND   C."NPOLICY"     = PC."NPOLICY"
                                      AND   C."NCERTIF"     = PC."NCERTIF"
                                      AND   C."DEFFECDATE" <= PC."DSTARTDATE_CERT"
                                      AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE_CERT")
                                      AND  C."NCOVER" = 1 LIMIT 1                                      
                           )
                           END AS KGCTPCBT,
                           ROW_NUMBER () OVER (PARTITION  BY PC."NBRANCH", PC."NPRODUCT", PC."NPOLICY", PC."NCERTIF" ORDER BY R."SCLIENT") AS DNPESEG,
                           R."SCLIENT" AS KEBENTID_PS,
                           (SELECT DATE_PART('YEAR', AGE(CURRENT_DATE, CLI."DBIRTHDAT")) FROM USVTIMG01."CLIENT" CLI WHERE CLI."SCLIENT" = R."SCLIENT") AS DIDADEAC,
                           '' AS DANOREF, --EN BLANCO
                           '' AS KACEMPR,
                           0 AS VMTSALAR,
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
                           JOIN (   SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
                                    FROM USVTIMV01."POLICY" P 
                           	      LEFT JOIN USVTIMV01."CERTIFICAT" CERT 
                           	      ON  P."SCERTYPE" = CERT."SCERTYPE" 
                           	      AND P."NBRANCH"  = CERT."NBRANCH"
                           	      AND P."NPRODUCT" = CERT."NPRODUCT"
                           	      AND P."NPOLICY"  = CERT."NPOLICY"
                           	      JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                    (SELECT unnest(ARRAY['usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01',
                                                      'usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01']) AS "SOURCESCHEMA",  
						        unnest(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
						        unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
                           	      WHERE P."SCERTYPE" = '2' 
                                    AND P."SSTATUS_POL" NOT IN ('2','3') 
                                    AND ((P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                    AND P."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '{l_fecha_carga_inicial}') )
                                    OR 
                                    (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                    AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}'))
                                    AND p."DSTARTDATE" between '{p_fecha_inicio}' and '{p_fecha_fin}')

                                    /*UNION 
                              
                                    (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                     FROM USVTIMV01."POLICY" P 
                                     JOIN USVTIMV01."CERTIFICAT" CERT 
                                     ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                     AND P."NBRANCH"  = CERT."NBRANCH"
                                     AND P."NPRODUCT" = CERT."NPRODUCT"
                                     AND P."NPOLICY"  = CERT."NPOLICY"
                                     JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                           (SELECT unnest(ARRAY['usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01',
                                                                'usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01']) AS "SOURCESCHEMA",  
						        unnest(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
						        unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
                                     JOIN USVTIMV01."CLAIM" CLA ON CLA."SCERTYPE" = POL."SCERTYPE" AND CLA."NPOLICY" = POL."NPOLICY" AND CLA."NBRANCH" = POL."NBRANCH"
                                     JOIN (
                                           SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMV01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                           JOIN USVTIMV01."CLAIM_HIS" CLH ON COALESCE(CLH."NCLAIM", 0) > 0 AND CLH."NOPER_TYPE" = CSV."SVALUE" AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                          ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                     WHERE P."SCERTYPE" = '2' 
                                     AND P."SSTATUS_POL" NOT IN ('2','3') 
                                     AND P."SPOLITYPE" = '1' 
                                     AND (P."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR P."DNULLDATE" < '{l_fecha_carga_inicial}'))

                                     UNION

                                    (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                    FROM USVTIMV01."POLICY" P 
                                    JOIN USVTIMV01."CERTIFICAT" CERT 
                                    ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                    AND P."NBRANCH"  = CERT."NBRANCH"
                                    AND P."NPRODUCT" = CERT."NPRODUCT"
                                    AND P."NPOLICY"  = CERT."NPOLICY"
                                    JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                    (SELECT unnest(ARRAY['usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01',
                                                                'usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01']) AS "SOURCESCHEMA",  
						        unnest(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
						        unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
                                    JOIN USVTIMV01."CLAIM" CLA ON CLA."SCERTYPE" = CERT."SCERTYPE" AND CLA."NBRANCH" = CERT."NBRANCH" AND CLA."NPOLICY" = CERT."NPOLICY"  AND CLA."NCERTIF" =  CERT."NCERTIF"
                                    JOIN (
                                           SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" 
                                           FROM USVTIMV01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                           JOIN USVTIMV01."CLAIM_HIS" CLH 
                                           ON COALESCE(CLH."NCLAIM", 0) > 0 
                                           AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                           AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                         ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                    WHERE P."SCERTYPE" = '2' 
                                    AND P."SSTATUS_POL" NOT IN ('2','3') 
                                    AND P."SPOLITYPE" <> '1' 
                                    AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}'))*/ 

                              ) AS PC	
                           ON  R."SCERTYPE"  = PC."SCERTYPE"
                           AND R."NBRANCH"   = PC."NBRANCH" 
                           AND R."NPRODUCT"  = PC."NPRODUCT"
                           AND R."NPOLICY"   = PC."NPOLICY" 
                           AND R."NCERTIF"   = PC."NCERTIF"  
                           AND R."DEFFECDATE" <= PC."DSTARTDATE" 
                           AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > PC."DSTARTDATE")
                           WHERE R."NROLE" IN (2,8) 
                           LIMIT 100) AS VTIME_LPV
                           '''
    
    l_df_abclrisp_vtime_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_vtime_lpv).load()
    
    print("VTIME LPV")

    #----------------------------------------------------------------------------------------------------------------------------------#

    l_abclrisp_insis_lpv = f'''
                           (SELECT
                            'D' INDDETREC, 
                            'ABCLRISP' TABLAIFRS17, 
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            IO."INSR_BEGIN" AS TIOCFRM,
                            '' AS TIOCTO,
                            'PNV' AS KGIORIGM,
                            SUBSTRING(CAST(P."POLICY_ID" AS VARCHAR),6,12) AS KABAPOL,
                            SUBSTRING(CAST(P."POLICY_ID" AS VARCHAR),6,12) || '-' || coalesce(cast(IO."OBJECT_ID" as varchar), '0') AS KABUNRIS,
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
                            cast(OA."OAIP1" as numeric(12,2)) AS VMTSALAR,
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
                            AND P."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                            LIMIT 100) AS PNV'''
    
    l_df_abclrisp_insis_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insis_lpv).load()

    print("INSIS LPV")
    
    l_df_abclrisp = l_df_abclrisp_insunix_lpg.union(l_df_abclrisp_insunix_lpv).union(l_df_abclrisp_vtime_lpg).union(l_df_abclrisp_vtime_lpv).union(l_df_abclrisp_insis_lpv)

    l_df_abclrisp = l_df_abclrisp.withColumn("KGCTPCBT" , coalesce(col("KGCTPCBT"),lit("").cast(StringType()))).withColumn("DNPESEG", coalesce(col("DNPESEG"),lit("").cast(StringType()))).withColumn("DIDADEAC", coalesce(col("DIDADEAC"),lit("").cast(StringType()))).withColumn("VMTSALAR", format_number("VMTSALAR",2)).withColumn("KACTPPES", coalesce(col("KACTPPES"),lit("").cast(StringType()))).withColumn("TINICIO", coalesce(col("TINICIO"),lit("").cast(DateType()))).withColumn("TTERMO", coalesce(col("TTERMO"),lit("").cast(StringType())))

    return l_df_abclrisp