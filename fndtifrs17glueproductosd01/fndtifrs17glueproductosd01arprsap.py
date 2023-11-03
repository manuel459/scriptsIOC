def getData(glueContext,connection,L_FECHA_INICIO,L_FECHA_FIN):
  L_ARPRSAP_INSUNIX = f'''
                        (
                         (SELECT
                          'D' INDDETREC,
                          'ARPRSAP' TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          COALESCE(CAST(GC.EFFECDATE AS VARCHAR), '')  AS TIOCFRM,
                          '' AS TIOCTO,
                          'PIG' AS KGIORIGM,
                          'LPG' AS DCOMPA,
                          '' AS DMARCA,
                          COALESCE(P.BRANCH, 0) || '-' || COALESCE(P.PRODUCT, 0) AS KABPRODT,
                          COALESCE(CAST(GC.COVERGEN AS VARCHAR),'') AS KGCTPCBT,
                          '' AS KACCDFDO,
                          '' AS KACFUNAU,
                          COALESCE(
                                    COALESCE(
                                              (SELECT CAST(AA.BRANCH_PYG AS VARCHAR)
                                               FROM USINSUG01.ACC_AUTOM2 AA 
                                               WHERE GC.BRANCH = AA.BRANCH  
                                               AND   GC.PRODUCT = AA.PRODUCT 
                                               AND   GC.BILL_ITEM = AA.CONCEPT_FAC LIMIT 1), 
                                              (SELECT CAST(AA.BRANCH_PYG AS VARCHAR)
                                               FROM USINSUG01.ACC_AUTOM2 AA 
                                               WHERE GC.BRANCH = AA.BRANCH  
                                               AND   GC.PRODUCT = AA.PRODUCT LIMIT 1)
                                            ), 
                                    (SELECT CAST(AA.BRANCH_PYG AS VARCHAR)
                                     FROM USINSUG01.ACC_AUTOM2 AA 
                                     WHERE GC.BRANCH = AA.BRANCH LIMIT 1)
                          )  AS KGCRAMO_SAP,
                          '' AS DMASTER,
                          '' AS KACTPSPR, 
                          '' AS KACPARES, 
                          COALESCE(P.BRANCHT, '') AS KACCLAPD,
                          '' AS KACSCLAPD,
                          '' AS DRAMOSAP,
                          '' AS DPRODSAP,
                          '' AS KACCDFDO_PR
                          FROM USINSUG01.GEN_COVER GC 
                          LEFT JOIN
                          (
                          	SELECT	
                          	PRO.PRODUCT,
                          	PRO.BRANCH,
                            PRO.BRANCHT,
                            CASE WHEN PRO.NULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                            FROM (	
                                  SELECT	PRO.*,
                                  CASE	
                                  WHEN PRO.NULLDATE IS NULL THEN	PRO.CTID
                          		 	  ELSE	CASE	
                                        WHEN EXISTS (	SELECT	1
                          		 				              	FROM	USINSUG01.PRODUCT PR1
                          		 				              	WHERE 	PR1.USERCOMP = PRO.USERCOMP
                          		 				              	AND 	PR1.COMPANY = PRO.COMPANY
                          		 				              	AND 	PR1.BRANCH = PRO.BRANCH
                          		 				              	AND 	PR1.PRODUCT = PRO.PRODUCT
                          		 				              	AND		PR1.NULLDATE IS NULL) THEN 	NULL
                          		 		      ELSE  CASE	
                                              WHEN PRO.NULLDATE = (	SELECT	MAX(PR1.NULLDATE)
                          		 				 						                  FROM	USINSUG01.PRODUCT PR1
                          		 				 						                  WHERE 	PR1.USERCOMP = PRO.USERCOMP
                          		 				 						                  AND 	PR1.COMPANY = PRO.COMPANY
                          		 				 						                  AND 	PR1.BRANCH = PRO.BRANCH
                          		 				 						                  AND 	PR1.PRODUCT = PRO.PRODUCT) THEN PRO.CTID
                          		 			          ELSE NULL 
                                              END 
                                        END 
                                  END PRO_ID
                          		    FROM	USINSUG01.PRODUCT PRO
                          		    WHERE	BRANCH IN (SELECT BRANCH FROM USINSUG01.TABLE10B WHERE COMPANY = 1)) PR0, USINSUG01.PRODUCT PRO
                          	WHERE PRO.CTID = PR0.PRO_ID) P 
                          ON GC.BRANCH = P.BRANCH  AND GC.PRODUCT  = P.PRODUCT
                          WHERE GC.COMPDATE BETWEEN '{L_FECHA_INICIO}' AND '{L_FECHA_FIN}')
                          UNION ALL
                        
                          (SELECT
                          'D' INDDETREC,
                          'ARPRSAP' TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          COALESCE(CAST(LC.EFFECDATE AS VARCHAR), '')  AS TIOCFRM,
                          '' AS TIOCTO,
                          'PIV' AS KGIORIGM,
                          'LPV' AS DCOMPA,
                          '' AS DMARCA,
                          COALESCE(P.BRANCH, 0) || '-' || COALESCE(P.PRODUCT, 0) AS KABPRODT,
                          COALESCE(CAST(LC.COVERGEN AS VARCHAR),'') AS KGCTPCBT,
                          '' AS KACCDFDO,
                          '' AS KACFUNAU,
                          COALESCE(
                                    COALESCE((SELECT CAST(AA.BRANCH_PYG AS VARCHAR)
                                              FROM USINSUV01.ACC_AUTOM2 AA 
                                              WHERE LC.BRANCH = AA.BRANCH  
                                              AND   LC.PRODUCT = AA.PRODUCT 
                                              AND   LC.BILL_ITEM = AA.CONCEPT_FAC
                                              LIMIT 1), 
                                              (SELECT CAST(AA.BRANCH_PYG AS VARCHAR)
                                               FROM USINSUV01.ACC_AUTOM2 AA 
                                               WHERE LC.BRANCH = AA.BRANCH  
                                               AND   LC.PRODUCT = AA.PRODUCT LIMIT 1)), 
                                    (SELECT CAST(AA.BRANCH_PYG AS VARCHAR)
                                    FROM USINSUG01.ACC_AUTOM2 AA 
                                    WHERE LC.BRANCH = AA.BRANCH LIMIT 1)) AS KGCRAMO_SAP,
                          '' AS DMASTER,
                          '' AS KACTPSPR, 
                          '' AS KACPARES, 
                          COALESCE(P.BRANCHT, '') AS KACCLAPD,
                          '' AS KACSCLAPD,
                          '' AS DRAMOSAP,
                          '' AS DPRODSAP,
                          '' AS KACCDFDO_PR
                          FROM USINSUV01.LIFE_COVER LC 
                          LEFT JOIN
                          (
                          	SELECT	
                          	PRO.PRODUCT,
                          	PRO.BRANCH,
                            PRO.BRANCHT,
                            CASE WHEN PRO.NULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                            FROM (	
                                  SELECT	PRO.*,
                                  CASE	
                                  WHEN PRO.NULLDATE IS NULL THEN	PRO.CTID
                          		 	  ELSE	CASE	
                                        WHEN EXISTS (	SELECT	1
                          		 				              	FROM	USINSUV01.PRODUCT PR1
                          		 				              	WHERE 	PR1.USERCOMP = PRO.USERCOMP
                          		 				              	AND 	PR1.COMPANY = PRO.COMPANY
                          		 				              	AND 	PR1.BRANCH = PRO.BRANCH
                          		 				              	AND 	PR1.PRODUCT = PRO.PRODUCT
                          		 				              	AND		PR1.NULLDATE IS NULL) THEN 	NULL
                          		 		      ELSE  CASE	
                                              WHEN PRO.NULLDATE = (	SELECT	MAX(PR1.NULLDATE)
                          		 				 						                  FROM	USINSUV01.PRODUCT PR1
                          		 				 						                  WHERE 	PR1.USERCOMP = PRO.USERCOMP
                          		 				 						                  AND 	PR1.COMPANY = PRO.COMPANY
                          		 				 						                  AND 	PR1.BRANCH = PRO.BRANCH
                          		 				 						                  AND 	PR1.PRODUCT = PRO.PRODUCT) THEN PRO.CTID
                          		 			          ELSE NULL 
                                              END 
                                        END 
                                  END PRO_ID
                          		    FROM	USINSUV01.PRODUCT PRO
                          		    WHERE	BRANCH IN (SELECT BRANCH FROM USINSUG01.TABLE10B WHERE COMPANY = 2)) PR0, USINSUV01.PRODUCT PRO
                          	WHERE PRO.CTID = PR0.PRO_ID) P
                          ON LC.BRANCH = P.BRANCH  AND LC.PRODUCT  = P.PRODUCT
                          WHERE LC.COMPDATE BETWEEN '{L_FECHA_INICIO}' AND '{L_FECHA_FIN}')
                        ) AS TMP
                          '''
    
    #EJECUTAR CONSULTA
  
  L_DF_ARPRSAP_INSUNIX = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ARPRSAP_INSUNIX).load()
  #--------------------------------------------------------------------------------------------------------------------------#
  L_ARPRSAP_VTIME = f'''
                          (
                           (SELECT 
                            'D' INDDETREC,
                            'ARPRSAP' TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            CAST(CAST(GC."DEFFECDATE" AS DATE) AS VARCHAR)  AS TIOCFRM,
                            '' AS TIOCTO,
                            'PVG' AS KGIORIGM,
                            'LPG' AS DCOMPA,
                            '' AS DMARCA,
                            PM."NBRANCH" || '-' || PM."NPRODUCT" AS KABPRODT,
                            COALESCE(CAST(GC."NCOVERGEN" AS VARCHAR), '')  AS KGCTPCBT,
                            '' AS KACCDFDO,
                            '' AS KACFUNAU,
                            COALESCE(CAST(GC."NBRANCH_LED" AS VARCHAR), '')  AS KGCRAMO_SAP,
                            '' AS DMASTER,
                            '' AS KACTPSPR, 
                            '' AS KACPARES,
                            PM."SBRANCHT"  AS KACCLAPD, 
                            '' AS KACSCLAPD,
                            '' AS DRAMOSAP,
                            '' AS DPRODSAP,
                            '' AS KACCDFDO_PR
                            FROM USVTIMG01."GEN_COVER" GC 
                            LEFT JOIN USVTIMG01."PRODMASTER" PM  ON GC."NBRANCH" = PM."NBRANCH"  AND GC."NPRODUCT"  = PM."NPRODUCT"  
                            WHERE GC."DCOMPDATE" BETWEEN '{L_FECHA_INICIO}' AND '{L_FECHA_FIN}')
                           UNION ALL
                           (SELECT
                            'D' INDDETREC,
                            'ARPRSAP' TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            CAST(CAST(LC."DEFFECDATE" AS DATE) AS VARCHAR)  AS TIOCFRM,
                            '' AS TIOCTO,
                            'PVV' AS KGIORIGM,
                            'LPV' AS DCOMPA,
                            '' AS DMARCA,
                            PM."NBRANCH"|| '-' || PM."NPRODUCT" AS KABPRODT,
                            COALESCE (CAST(LC."NCOVERGEN" AS VARCHAR), '') AS KGCTPCBT,
                            '' AS KACCDFDO,
                            '' AS KACFUNAU,
                            COALESCE(CAST(LC."NBRANCH_LED" AS VARCHAR), '')  AS KGCRAMO_SAP,
                            '' AS DMASTER,
                            '' AS KACTPSPR,
                            '' AS KACPARES,
                            PM."SBRANCHT"  AS KACCLAPD,
                            '' AS KACSCLAPD,
                            '' AS DRAMOSAP,
                            '' AS DPRODSAP,
                            '' AS KACCDFDO_PR
                            FROM USVTIMV01."LIFE_COVER" LC
                            LEFT JOIN USVTIMV01."PRODMASTER" PM ON LC."NBRANCH" = PM."NBRANCH" AND LC."NPRODUCT" = PM."NPRODUCT"
                            WHERE LC."DCOMPDATE" BETWEEN '{L_FECHA_INICIO}' AND '{L_FECHA_FIN}')
                          ) AS TMP
                       '''
    #EJECUTAR CONSULTA
  
  L_DF_ARPRSAP_VTIME = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ARPRSAP_VTIME).load()
    #--------------------------------------------------------------------------------------------------------------------------#
    
  L_ARPRSAP_INSIS = f'''
                        (SELECT 
                        'D' INDDETREC,
                        'ARPRSAP' TABLAIFRS17,
                        '' AS PK,
                        '' AS DTPREG,
                        '' AS TIOCPROC,
                        '' AS TIOCFRM,
                        '' AS TIOCTO,
                        'PNV' AS KGIORIGM,
                        'LPV' AS DCOMPA,
                        '' AS DMARCA,
                        CAST(CNP."PRODUCT_CODE" AS VARCHAR) AS KABPRODT,
                        CAST(CNC."COVER_LINK_ID" AS VARCHAR(10)) AS KGCTPCBT,
                        '' AS KACCDFDO,
                        '' AS KACFUNAU,
                        '' AS KGCRAMO_SAP,
                        '' AS DMASTER,  
                        '' AS KACTPSPR, 
                        '' AS KACPARES,
                        '' AS KACCLAPD,
                        '' as KACSCLAPD,
                        '' AS DRAMOSAP,
                        '' AS DPRODSAP,
                        '' AS KACCDFDO_PR
                        FROM usinsiv01."CFG_NL_COVERS" CNC 
                        LEFT JOIN USINSIV01."CFG_NL_PRODUCT" CNP ON CNC."PRODUCT_LINK_ID" = CNP."PRODUCT_LINK_ID"
                        WHERE CNC."VALID_FROM" BETWEEN '{L_FECHA_INICIO}' AND '{L_FECHA_FIN}') AS TMP
                      '''
    
    #EJECUTAR CONSULTA
  L_DF_ARPRSAP_INSIS = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ARPRSAP_INSIS).load()
    
  
  #--------------------------------------------------------------------------------------------------------------------------#
  #UNION DATAFRAME POR COMPAÑIA
  L_DF_ARPRSAP = L_DF_ARPRSAP_INSUNIX.union(L_DF_ARPRSAP_VTIME).union(L_DF_ARPRSAP_INSIS)
    
  return L_DF_ARPRSAP