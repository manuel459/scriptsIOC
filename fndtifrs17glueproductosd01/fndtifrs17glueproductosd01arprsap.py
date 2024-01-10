def get_data(glue_context, connection):
  
  l_arprsap_insunix = '''
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
                          COALESCE(P.BRANCH, 0) || '-' || COALESCE(P.PRODUCT, 0) || '-' || COALESCE(P.SUB_PRODUCT) AS KABPRODT,
                          COALESCE(GC.COVERGEN, 0) || '-' || COALESCE(GC.CURRENCY, 0) AS KGCTPCBT,
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
                                               AND   GC.PRODUCT = AA.PRODUCT LIMIT 1)), 
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
                          '' AS KACCDFDO_PR/*,
                          GC.MODULEC AS MODULO*/
                          FROM USINSUG01.GEN_COVER GC 
                          LEFT JOIN
                          (
                          	SELECT	
                          	PRO.PRODUCT,
                          	PRO.BRANCH,
                            PRO.BRANCHT,
                            PRO.SUB_PRODUCT,
                            CASE WHEN PRO.NULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                            FROM (	
                                  SELECT	PRO.*,
                                  CASE	
                                  WHEN PRO.NULLDATE IS NULL THEN	PRO.CTID
                          		 	  ELSE	CASE	
                                        WHEN EXISTS (	SELECT	1
                          		 				              	FROM	USINSUG01.PRODUCT PR1
                          		 				              	WHERE PR1.USERCOMP = PRO.USERCOMP
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
                          ON GC.BRANCH = P.BRANCH  AND GC.PRODUCT  = P.PRODUCT)
                          
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
                          COALESCE(LC.covergen, 0) || '-' || coalesce(LC.currency, 0) AS KGCTPCBT,
                          '' AS KACCDFDO,
                          '' AS KACFUNAU,
                          COALESCE(
                                    COALESCE( (SELECT CAST(AA.BRANCH_PYG AS VARCHAR)
                                               FROM USINSUV01.ACC_AUTOM2 AA 
                                               WHERE LC.BRANCH = AA.BRANCH  
                                               AND   LC.PRODUCT = AA.PRODUCT 
                                               AND   LC.BILL_ITEM = AA.CONCEPT_FAC LIMIT 1), 
                                              (SELECT CAST(AA.BRANCH_PYG AS VARCHAR)
                                               FROM USINSUV01.ACC_AUTOM2 AA 
                                               WHERE LC.BRANCH = AA.BRANCH  
                                               AND   LC.PRODUCT = AA.PRODUCT LIMIT 1)), 
                                              (SELECT CAST(AA.BRANCH_PYG AS VARCHAR)
                                               FROM USINSUV01.ACC_AUTOM2 AA 
                                               WHERE LC.BRANCH = AA.BRANCH LIMIT 1)) AS KGCRAMO_SAP,
                          '' AS DMASTER,
                          '' AS KACTPSPR, 
                          '' AS KACPARES, 
                          COALESCE(P.BRANCHT, '') AS KACCLAPD,
                          '' AS KACSCLAPD,
                          '' AS DRAMOSAP,
                          '' AS DPRODSAP,
                          '' AS KACCDFDO_PR/*,
                          0  AS MODULO*/
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
                          ON LC.BRANCH = P.BRANCH  AND LC.PRODUCT  = P.PRODUCT)
                        ) AS TMP
                          '''
    
    #EJECUTAR CONSULTA
  
  l_df_arprsap_insunix = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_arprsap_insunix).load()

  #--------------------------------------------------------------------------------------------------------------------------#

  l_arprsap_vtime = '''
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
                            COALESCE(PM."NBRANCH", 0) || '-' || COALESCE(PM."NPRODUCT", 0) AS KABPRODT,
                            COALESCE(GC."NCOVERGEN", 0) AS KGCTPCBT,
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
                            '' AS KACCDFDO_PR/*,
                            GC."NMODULEC" AS MODULO*/
                            FROM USVTIMG01."GEN_COVER" GC 
                            LEFT JOIN USVTIMG01."PRODMASTER" PM  ON GC."NBRANCH" = PM."NBRANCH"  AND GC."NPRODUCT"  = PM."NPRODUCT")

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
                            COALESCE(PM."NBRANCH", 0) || '-' || COALESCE(PM."NPRODUCT", 0) AS KABPRODT,
                            COALESCE(LC."NCOVERGEN", 0) AS KGCTPCBT,
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
                            '' AS KACCDFDO_PR/*,
                            LC."NMODULEC" AS MODULO*/                          
                            FROM USVTIMV01."LIFE_COVER" LC
                            LEFT JOIN USVTIMV01."PRODMASTER" PM ON LC."NBRANCH" = PM."NBRANCH" AND LC."NPRODUCT" = PM."NPRODUCT")
                          ) AS TMP
                       '''
    #EJECUTAR CONSULTA
  
  l_df_arprsap_vtime = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_arprsap_vtime).load()
  #--------------------------------------------------------------------------------------------------------------------------#
    
  l_arprsap_insis = '''
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
                        SUBSTRING(CAST(CAST(CNC."COVER_REP_ID" as BIGINT) AS VARCHAR),5,10) AS KGCTPCBT,
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
                        '' AS KACCDFDO_PR/*,                        
                        CNC."OBJECT_LINK_ID" AS MODULO*/
                        FROM USINSIV01."CFG_NL_COVERS" CNC 
                        LEFT JOIN USINSIV01."CFG_NL_PRODUCT" CNP ON CNC."PRODUCT_LINK_ID" = CNP."PRODUCT_LINK_ID"
                        ) AS TMP
                      '''
    
    #EJECUTAR CONSULTA
  
  l_df_arprsap_insis = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_arprsap_insis).load()
      
  #--------------------------------------------------------------------------------------------------------------------------#
 
  l_df_arprsap = l_df_arprsap_insunix.union(l_df_arprsap_vtime).union(l_df_arprsap_insis)
    
  return l_df_arprsap