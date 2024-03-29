
def get_data(glue_context, connection):

  l_abprodt_insunix_lpg = '''
                          ( SELECT 
                          'D' AS INDDETREC,
                          'ABPRODT' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TIOCFRM,
                          '' AS TIOCTO,
                          'PIG' AS KGIORIGM,
                          COALESCE(P.BRANCH, 0) || '-' || COALESCE(P.PRODUCT, 0) || '-' || COALESCE(P.SUB_PRODUCT, 0) AS DCODIGO,
                          '' AS DDESC,
                          COALESCE(CAST(P.BRANCH AS VARCHAR), '') AS KGCRAMO,
                          'LPG' AS DCOMPA,
                          '' AS DMARCA,
                          'PER' AS KACPAIS,
                          COALESCE(P.BRANCHT, '') AS KACTPPRD,
                          '' AS KACTPSUB, 
                          '' AS KACPARES, 
                          '' AS KACRESGA, 
                          '' AS KACINPPR, 
                          '' AS KACCLREN,
                          '' AS KACTPCTR,
                          '' AS KACTPGRP,
                          '' AS KACCRHAB,
                          COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TINICOME,
                          '' AS TFIMCOME,
                          '' AS KACFPCAP,
                          '' AS DINDREDU, 
                          '' AS DINDRESG,
                          '' AS KACTPDUR,
                          '' AS DCDCNGR,
                          '' AS KACREINTE,
                          '' AS KABPRODT_RINV,
                          '' AS DSEGMENT,
                          '' AS KACSEGMEN,
                          '' AS KACTARTAB,
                          '' AS KACTPCRED,
                          '' AS KACTPOPS,
                          '' AS KACMOEDA,  
                          '' AS KACPERISC,
                          '' AS DINDGRPADES,
                          '' AS DINDENTEXT,
                          '' AS KACSBTPRD, 
                          '' AS KACTPPMA, 
                          '' AS KACTIPIFAP 
                          FROM
                          (
                          	SELECT	
                          	PRO.EFFECDATE,
                          	PRO.PRODUCT,
                          	PRO.BRANCH,
                            PRO.BRANCHT,
                          	PRO.NULLDATE,
                            PRO.SUB_PRODUCT,
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
                          	WHERE PRO.CTID = PR0.PRO_ID) P) AS TMP
                          '''

  df_abprodt_insunix_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abprodt_insunix_lpg).load()

  l_abprodt_insunix_lpv = '''
                          (                       
                            SELECT
                            'D' AS INDDETREC,
                            'ABPRODT' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            COALESCE(CAST(P.EFFECDATE AS VARCHAR), '')  AS TIOCFRM,
                            '' AS TIOCTO,
                            'PIV' AS KGIORIGM,
                            COALESCE(P.BRANCH, '0') || '-' || COALESCE(CAST(P.PRODUCT AS VARCHAR), '0') AS DCODIGO,
                            '' AS DDESC,
                            COALESCE(CAST(P.BRANCH AS VARCHAR), '') AS KGCRAMO,
                            'LPV' AS DCOMPA,
                            '' AS DMARCA,
                            'PER' AS KACPAIS,
                            COALESCE(P.BRANCHT, '') AS KACTPPRD, 
                            '' AS KACTPSUB, 
                            '' AS KACPARES, 
                            '' AS KACRESGA, 
                            '' AS KACINPPR, 
                            '' AS KACCLREN,
                            '' AS KACTPCTR,
                            '' AS KACTPGRP,
                            '' AS KACCRHAB, 
                            COALESCE(CAST(P.EFFECDATE AS VARCHAR), '')  AS TINICOME,
                            '' AS TFIMCOME,
                            '' AS KACFPCAP,
                            '' AS DINDREDU,  
                            '' AS DINDRESG,
                            '' AS KACTPDUR,
                            '' AS DCDCNGR,
                            '' AS KACREINTE,
                            '' AS KABPRODT_RINV, 
                            '' AS DSEGMENT,   
                            '' AS KACSEGMEN,   
                            '' AS KACTARTAB,   
                            '' AS KACTPCRED, 
                            '' AS KACTPOPS,  
                            '' AS KACMOEDA,  
                            '' AS KACPERISC,
                            '' AS DINDGRPADES,
                            '' AS DINDENTEXT,
                            '' AS KACSBTPRD,   
                            '' AS KACTPPMA,
                            '' AS KACTIPIFAP
                            FROM
                            (
                          	    SELECT	
                          	    PRO.EFFECDATE,
                          	    PRO.PRODUCT,
                          	    PRO.BRANCH,
                                PRO.BRANCHT,
                          	    PRO.NULLDATE,
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
                          	    WHERE PRO.CTID = PR0.PRO_ID
                            ) P
                          ) AS TMP
                          '''
  
  df_abprodt_insunix_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abprodt_insunix_lpv).load()

  df_abprodt_insunix = df_abprodt_insunix_lpg.union(df_abprodt_insunix_lpv)

  #--------------------------------------------------------------------------------------------------------------------------#

  l_abprodt_vtime_lpg = '''
                        (SELECT
                            'D' AS INDDETREC,
                            'ABPRODT' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            CAST(CAST(P."DEFFECDATE" AS DATE) AS VARCHAR) AS TIOCFRM,
                            '' AS TIOCTO,
                            'PVG' AS KGIORIGM,
                            coalesce(p."NBRANCH", 0)|| '-' || coalesce(P."NPRODUCT", 0) AS DCODIGO,
                            '' AS DDESC,
                            P."NBRANCH" AS KGCRAMO,
                            'LPG' AS DCOMPA,
                            '' AS DMARCA,
                            'PER' AS KACPAIS,
                            PM."SBRANCHT" AS KACTPPRD,
                            '' AS KACTPSUB, 
                            '' AS KACPARES, 
                            '' AS KACRESGA, 
                            '' AS KACINPPR, 
                            '' AS KACCLREN,
                            '' AS KACTPCTR,
                            '' AS KACTPGRP,
                            '' AS KACCRHAB, 
                            CAST(CAST(P."DEFFECDATE" AS DATE) AS VARCHAR) AS TINICOME,
                            '' AS TFIMCOME,
                            '' AS KACFPCAP,
                            '' AS DINDREDU,  
                            '' AS DINDRESG,
                            '' AS KACTPDUR,
                            '' AS DCDCNGR, 
                            '' AS KACREINTE,
                            '' AS KABPRODT_RINV, 
                            '' AS DSEGMENT,
                            '' AS KACSEGMEN,
                            '' AS KACTARTAB,    
                            '' AS KACTPCRED,
                            '' AS KACTPOPS,
                            '' AS KACMOEDA,  
                            '' AS KACPERISC,
                            '' AS DINDGRPADES,
                            '' AS DINDENTEXT,  
                            '' AS KACSBTPRD,
                            '' AS KACTPPMA,
                            '' AS KACTIPIFAP
                          FROM
                          (
                          	SELECT	
                          	PRO."DEFFECDATE",
                          	PRO."NPRODUCT",
                          	PRO."NBRANCH",
                          	PRO."DNULLDATE",
                            CASE WHEN PRO."DNULLDATE" IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                            FROM (	
                                  SELECT	PRO.*,
                                  CASE	
                                  WHEN PRO."DNULLDATE" IS NULL THEN	PRO.CTID
                          		 	  ELSE	CASE	
                                        WHEN EXISTS (	SELECT	1
                          		 				              	FROM	USVTIMG01."PRODUCT" PR1
                          		 				              	WHERE PR1."NBRANCH"  = PRO."NBRANCH"
                          		 				              	AND 	PR1."NPRODUCT" = PRO."NPRODUCT"
                          		 				              	AND		PR1."DNULLDATE" IS NULL) THEN 	NULL
                          		 		      ELSE  CASE	
                                              WHEN PRO."DNULLDATE" = (	SELECT	MAX(PR1."DNULLDATE")
                          		 				 						                  FROM	USVTIMG01."PRODUCT" PR1
                          		 				 						                  WHERE PR1."NBRANCH"  = PRO."NBRANCH" 
                          		 				 						                  AND 	PR1."NPRODUCT" = PRO."NPRODUCT") THEN PRO.CTID
                          		 			          ELSE NULL 
                                              END 
                                        END 
                                  END PRO_ID
                          		    FROM	USVTIMG01."PRODUCT" PRO) PR0, USVTIMG01."PRODUCT" PRO
                          	WHERE PRO.CTID = PR0.PRO_ID) P
                          LEFT JOIN USVTIMG01."PRODMASTER" PM ON PM."NBRANCH" = P."NBRANCH" AND PM."NPRODUCT" = P."NPRODUCT") AS TMP
                        '''

  l_df_abprodt_vtime_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abprodt_vtime_lpg).load()

  l_abprodt_vtime_lpv = '''
                          (SELECT
                            'D' AS INDDETREC,
                            'ABPRODT' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            CAST(CAST(P."DEFFECDATE" AS DATE) AS VARCHAR) AS TIOCFRM,
                            '' AS TIOCTO,
                            'PVV' AS KGIORIGM,
                            coalesce(p."NBRANCH", 0)|| '-' || coalesce(P."NPRODUCT", 0) AS DCODIGO,
                            '' AS DDESC,
                            P."NBRANCH" AS KGCRAMO,
                            'LPV' AS DCOMPA,
                            '' AS DMARCA,
                            'PER' AS KACPAIS,
                            PM."SBRANCHT" AS KACTPPRD,
                            '' AS KACTPSUB, 
                            '' AS KACPARES, 
                            '' AS KACRESGA, 
                            '' AS KACINPPR, 
                            '' AS KACCLREN,
                            '' AS KACTPCTR,
                            '' AS KACTPGRP,
                            '' AS KACCRHAB, 
                            CAST(CAST(P."DEFFECDATE" AS DATE) AS VARCHAR) AS TINICOME,
                            '' AS TFIMCOME,
                            '' AS KACFPCAP,
                            '' AS DINDREDU,  
                            '' AS DINDRESG,
                            '' AS KACTPDUR,
                            '' AS DCDCNGR,    
                            '' AS KACREINTE,
                            '' AS KABPRODT_RINV, 
                            '' AS DSEGMENT,       
                            '' AS KACSEGMEN,      
                            '' AS KACTARTAB,   	 
                            '' AS KACTPCRED,   	  
                            '' AS KACTPOPS,    	  
                            '' AS KACMOEDA,   
                            '' AS KACPERISC,   	  
                            '' AS DINDGRPADES, 	  
                            '' AS DINDENTEXT,  	 
                            '' AS KACSBTPRD,   	 
                            '' AS KACTPPMA,    	  
                            '' AS KACTIPIFAP   	    
                          FROM
                          (
                          	SELECT	
                          	PRO."DEFFECDATE",
                          	PRO."NPRODUCT",
                          	PRO."NBRANCH",
                          	PRO."DNULLDATE",
                            CASE WHEN PRO."DNULLDATE" IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                            FROM (	
                                  SELECT	PRO.*,
                                  CASE	
                                  WHEN PRO."DNULLDATE" IS NULL THEN	PRO.CTID
                          		 	  ELSE	CASE	
                                        WHEN EXISTS (	SELECT	1
                          		 				              	FROM	USVTIMV01."PRODUCT" PR1
                          		 				              	WHERE PR1."NBRANCH"  = PRO."NBRANCH"
                          		 				              	AND 	PR1."NPRODUCT" = PRO."NPRODUCT"
                          		 				              	AND		PR1."DNULLDATE" IS NULL) THEN 	NULL
                          		 		      ELSE  CASE	
                                              WHEN PRO."DNULLDATE" = (	SELECT	MAX(PR1."DNULLDATE")
                          		 				 						                  FROM	USVTIMV01."PRODUCT" PR1
                          		 				 						                  WHERE PR1."NBRANCH"  = PRO."NBRANCH" 
                          		 				 						                  AND 	PR1."NPRODUCT" = PRO."NPRODUCT") THEN PRO.CTID
                          		 			          ELSE NULL 
                                              END 
                                        END 
                                  END PRO_ID
                          		    FROM	USVTIMV01."PRODUCT" PRO) PR0, USVTIMV01."PRODUCT" PRO
                          	WHERE PRO.CTID = PR0.PRO_ID) P
                          LEFT JOIN USVTIMV01."PRODMASTER" PM ON PM."NBRANCH" = P."NBRANCH" AND PM."NPRODUCT" = P."NPRODUCT"
                        ) AS TMP
                       '''
  
  l_df_abprodt_vtime_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abprodt_vtime_lpv).load()

  l_df_abprodt_vtime = l_df_abprodt_vtime_lpg.union(l_df_abprodt_vtime_lpv)

  #--------------------------------------------------------------------------------------------------------------------------#

  l_abprodt_insis_lpv = ''' 
                      (
                          SELECT
                          'D' AS INDDETREC,
                          'ABPRODT' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          CAST(CNP."VALID_FROM" AS DATE)  AS TIOCFRM,
                          '' AS TIOCTO,
                          'PNV' AS KGIORIGM,
                          CAST(CNP."PRODUCT_CODE" AS VARCHAR) AS DCODIGO,
                          '' AS DDESC,
                          '' AS KGCRAMO,
                          'LPV' AS DCOMPA,
                          '' AS DMARCA,
                          'PER' AS KACPAIS,
                          COALESCE(CNP."PRODUCT_LOB", '')  AS KACTPPRD,
                          '' AS KACTPSUB,
                          '' AS KACPARES,
                          COALESCE
                          (
                            (
                              SELECT 'SI'
                              FROM USINSIV01."CFG_NL_COVERS" CNC 
                              WHERE CNC."PRODUCT_LINK_ID" = CNP."PRODUCT_LINK_ID"
                              AND CNC."MANDATORY" = 'Y'
                              AND "COVER_DESIGNATION" = 'INV_GCV'
                              LIMIT 1
                          ),'NO'
                          ) AS KACRESGA,
                          '' AS KACINPPR, 
                          '' AS KACCLREN,
                          '' AS KACTPCTR,
                          '' AS KACTPGRP,
                          '' AS KACCRHAB, 
                          CAST(CAST(CNP."VALID_FROM" AS DATE) AS VARCHAR) AS TINICOME,
                          '' AS TFIMCOME,
                          '' AS KACFPCAP,
                          '' AS DINDREDU,  
                          '' AS DINDRESG,
                          '' AS KACTPDUR,
                          '' AS DCDCNGR, 
                          '' AS KACREINTE,
                          '' AS KABPRODT_RINV, 
                          '' AS DSEGMENT,   
                          '' AS KACSEGMEN,
                          '' AS KACTARTAB,    
                          '' AS KACTPCRED,  
                          '' AS KACTPOPS,   
                          '' AS KACMOEDA,  
                          '' AS KACPERISC,   
                          '' AS DINDGRPADES, 
                          '' AS DINDENTEXT,
                          '' AS KACSBTPRD,
                          '' AS KACTPPMA,  
                          '' AS KACTIPIFAP 
                          FROM USINSIV01."CFG_NL_PRODUCT" CNP
                          --NO SE CONSIDERA EL FILTRO VALID_FROM YA QUE LA CANTIDAD DE DATOS ES MANEJABLE
                        ) AS TMP
                       '''   
  
  l_df_abprodt_insis = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abprodt_insis_lpv).load()
   
  
  #PERFORM THE UNION OPERATION 
  l_df_abprodt = df_abprodt_insunix.union(l_df_abprodt_vtime).union(l_df_abprodt_insis)

  return l_df_abprodt