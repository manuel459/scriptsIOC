def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):
  L_ABPRCOB_INSUNIX = f'''
                            (
                              (SELECT 
                              'D' INDDETREC,
                              'ABPRCOB' TABLAIFRS17,
                              '' AS PK,
                              '' AS DTPREG,       
                              '' AS TIOCPROC,     
                              COALESCE(CAST(GC.EFFECDATE AS VARCHAR), '') AS TIOCFRM,
                              '' AS TIOCTO,  	    
                              'PIG' AS KGIORIGM,  
                              'LPG' AS DCOMPA,
                              '' AS DMARCA,       
                              COALESCE(P.BRANCH, 0) || '-' || COALESCE (P.PRODUCT, 0) || '-' ||COALESCE(P.SUB_PRODUCT,0) AS KABPRODT,
                              COALESCE(CAST(GC.COVERGEN AS VARCHAR), '') || '-' || COALESCE(GC.CURRENCY, 0)  AS KGCTPCBT,
                              '' AS KACINDOPS,    
                              '' AS KACTIPCB,
                              '' AS KACTCOMP,     
                              '' AS DINDNIVEL,     
                              COALESCE(GC.ADDSUINI, '') AS KACSOCAP,
                              CASE 
                              WHEN GC.ROUCAPIT IS NOT NULL AND TRIM(GC.ROUCAPIT) != '' THEN 'RUTINA'
                              WHEN GC.CACALFIX IS NOT NULL THEN 'FIJO'
                              WHEN GC.CACALCOV IS NOT NULL THEN 'OTRACOBERTURA'
                              WHEN GC.CACALFRI = '1' THEN 'LIBRE'
                              WHEN GC.CACALILI = '1' THEN 'ILIMITADO'
                              ELSE ''
                              END KACFMCAL,
                              '' AS KACTPDPRIS,    
                              GC."MODULEC" AS KGCTPCBT_SUP, 
                              '' AS DINDANOS,      
                              '' AS KSCTPDAN,      
                              '' AS KACGPRIS,      
                              '' AS KGRAMO_SAP,    
                              '' AS DDURLIMINF,
                              '' AS DDURLIMSUP,
                              '' AS DLIMINFMASC, 
                              '' AS DLIMSUPMASC,
                              '' AS DLIMINFFEM,    
                              '' AS DLIMSUPFEM,
                              '' AS KACCLCAP,
                              '' AS KACFCAP,
                              '' AS DINDCARPF,
                              '' AS KACVALCB,      
                              COALESCE(CAST(GC.CACALMAX AS VARCHAR), '') AS VMTLIMCB,
                              '' AS VMTDEFCB,      
                              '' AS VTXLIMCB,
                              '' AS VTXDEFCB,      
                              '' AS KACTPTARCB,    
                              '' AS DINDLIBTAR
                              FROM USINSUG01.GEN_COVER GC 
                              JOIN (SELECT	
	                                  PRO.PRODUCT,
	                                  PRO.BRANCH,
                                    PRO.SUB_PRODUCT,
                                    CASE WHEN PRO.NULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                                    FROM (	
                                            SELECT	PRO.*,
                                            CASE	
                                            WHEN PRO.NULLDATE IS NULL THEN	PRO.CTID
	                          	 	            ELSE	CASE	
                                                  WHEN EXISTS
	                          	 	        			    (	SELECT	1
	                          	 	        			    	FROM	USINSUG01.PRODUCT PR1
	                          	 	        			    	WHERE PR1.USERCOMP = PRO.USERCOMP
	                          	 	        			    	AND 	PR1.COMPANY = PRO.COMPANY
	                          	 	        			    	AND 	PR1.BRANCH = PRO.BRANCH
	                          	 	        			    	AND 	PR1.PRODUCT = PRO.PRODUCT
	                          	 	        			    	AND		PR1.NULLDATE IS NULL) THEN 	NULL
	                          	 	        		ELSE  CASE	
                                                  WHEN PRO.NULLDATE =  
	                          	 	        			 		(	SELECT	MAX(PR1.NULLDATE)
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
                              WHERE GC.COMPDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}')

                              UNION ALL

                              (SELECT 
                              'D' INDDETREC,
                              'ABPRCOB' TABLAIFRS17,
                              '' AS PK,
                              '' AS DTPREG,       
                              '' AS TIOCPROC,     
                              COALESCE(CAST(LC.EFFECDATE AS VARCHAR), '')   AS TIOCFRM,
                              '' AS TIOCTO,  	    
                              'PIV' AS KGIORIGM,  
                              'LPV' AS DCOMPA,   
                              '' AS DMARCA,       
                              COALESCE(LC.BRANCH, 0) || '-' || COALESCE (LC.PRODUCT, 0)  AS KABPRODT,
                              COALESCE(CAST(LC.COVERGEN as VARCHAR), '') || '-' || COALESCE(LC.CURRENCY, 0) AS KGCTPCBT,
                              '' AS KACINDOPS,    
                              '' AS KACTIPCB,    
                              '' AS KACTCOMP,     
                              '' AS DINDNIVEL,     
                              COALESCE(LC.ADDCAPII, '') AS KACSOCAP,
                              CASE 
                              WHEN LC.ROUCHACA IS NOT NULL THEN 'RUTINA'
                              WHEN LC.CACALFIX IS NOT NULL THEN 'FIJO'
                              WHEN LC.CACALCOV IS NOT NULL THEN 'OTRACOBERTURA'
                              WHEN LC.CACALFRI = '1' THEN 'LIBRE'
                              ELSE ''
                              END KACFMCAL,
                              '' AS KACTPDPRIS,    
                              '' AS KGCTPCBT_SUP,
                              '' AS DINDANOS,      
                              '' AS KSCTPDAN,      
                              '' AS KACGPRIS,      
                              '' AS KGRAMO_SAP,    
                              '' AS DDURLIMINF,
                              '' AS DDURLIMSUP,
                              '' AS DLIMINFMASC,  
                              '' AS DLIMSUPMASC,
                              '' AS DLIMINFFEM,    
                              '' AS DLIMSUPFEM,
                              '' AS KACCLCAP,
                              '' AS KACFCAP,
                              '' AS DINDCARPF,
                              '' AS KACVALCB,      
                              '' AS VMTLIMCB,
                              '' AS VMTDEFCB,      
                              '' AS VTXLIMCB,
                              '' AS VTXDEFCB,      
                              '' AS KACTPTARCB,    
                              '' AS DINDLIBTAR
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
                                WHERE LC.COMPDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}')
                              ) AS TMP
                              '''
  #EJECUTAR CONSULTA
  L_DF_ABPRCOB_INSUNIX = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_ABPRCOB_INSUNIX).load()
  L_ABPRCOB_VTIME = f'''
                          (
                            (SELECT 
                            'D' INDDETREC,
                            'ABPRCOB' TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            CAST(CAST(GC."DEFFECDATE" AS DATE) AS VARCHAR) AS TIOCFRM,
                            '' AS TIOCTO,
                            'PVG' AS KGIORIGM,
                            'LPG' AS DCOMPA,
                            '' AS DMARCA,
                            COALESCE(P."NBRANCH", 0) || '-' || COALESCE(P."NPRODUCT", 0) AS KABPRODT,
                            COALESCE(CAST(GC."NCOVERGEN" AS VARCHAR), '') || '-' || COALESCE(GC."NCURRENCY", 0) AS KGCTPCBT,
                            '' AS KACINDOPS,
                            '' AS KACTIPCB,
                            '' AS KACTCOMP,
                            '' AS DINDNIVEL,
                            COALESCE(GC."SADDSUINI", '') AS KACSOCAP,
                            CASE 
                            WHEN GC."SROUCAPIT"  IS NOT NULL THEN 'RUTINA'
                            WHEN GC."NCACALFIX"  IS NOT NULL THEN 'FIJO'
                            WHEN GC."NCACALCOV"  IS NOT NULL THEN 'OTRACOBERTURA'
                            WHEN GC."SCACALFRI"  = '1' THEN 'LIBRE'
                            WHEN GC."SCACALILI"  = '1' THEN 'ILIMITADO'
                            ELSE ''
                            END KACFMCAL,
                            '' AS KACTPDPRIS,
                            GC."NMODULEC" AS KGCTPCBT_SUP,
                            '' AS DINDANOS,
                            '' AS KSCTPDAN,
                            '' AS KACGPRIS,
                            '' AS KGCRAMO_SAP,
                            '' AS DDURLIMINF,
                            '' AS DDURLIMSUP,
                            '' AS DLIMINFMASC,
                            '' AS DLIMSUPMASC,
                            '' AS DLIMINFFEM,
                            '' AS DLIMSUPFEM,
                            '' AS KACCLCAP,
                            '' AS KACFPCAP,
                            '' AS DINDCAPF,
                            '' AS KACVALCB,
                            '' AS VMTLIMCB,
                            '' AS VMTDEFCB,
                            '' AS VTXLIMCB,
                            '' AS VTXDEFCB,
                            '' AS KACTPTARCB,
                            '' AS DINDLIBTAR
                            FROM USVTIMG01."GEN_COVER" GC  
                            LEFT JOIN (
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
                          	WHERE PRO.CTID = PR0.PRO_ID) P ON GC."NBRANCH" = P."NBRANCH" AND GC."NPRODUCT"  = P."NPRODUCT"
                            WHERE GC."DCOMPDATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}')

                            UNION ALL

                            (SELECT 
                             'D' AS INDDETREC,
                             'ABPRCOB' AS TABLAIFRS17,
                             '' AS PK,
                             '' AS DTPREG,
                             '' AS TIOCPROC,
                             COALESCE(CAST(CAST(LC."DEFFECDATE" AS DATE) AS VARCHAR), '') AS TIOCFRM,
                             '' AS TIOCTO,
                             'PVV' AS KGIORIGM,
                             'LPV' AS DCOMPA,
                             '' AS DMARCA,
                             COALESCE(P."NBRANCH", 0) || '-' || COALESCE(P."NPRODUCT", 0) AS KABPRODT,
                             COALESCE(CAST(LC."NCOVERGEN" AS VARCHAR), '') || '-' || COALESCE(LC."NCURRENCY", 0) AS KGCTPCBT,
                             '' AS KACINDOPS,
                             '' AS KACTIPCB,
                             '' AS KACTCOMP,
                             '' AS DINDNIVEL,
                             COALESCE(LC."SADDSUINI", '') AS KACSOCAP,
                             CASE 
                             WHEN LC."SROURESER"  IS NOT NULL THEN 'RUTINA'
                             ELSE ''
                             END KACFMCAL,
                             '' AS KACTPDPRIS,
                             LC."NMODULEC" AS KGCTPCBT_SUP,
                             '' AS DINDANOS,
                             '' AS KSCTPDAN,
                             '' AS KACGPRIS,
                             '' AS KGCRAMO_SAP,
                             '' AS DDURLIMINF,
                             '' AS DDURLIMSUP,
                             '' AS DLIMINFMASC,
                             '' AS DLIMSUPMASC,
                             '' AS DLIMINFFEM,
                             '' AS DLIMSUPFEM,
                             '' AS KACCLCAP,
                             '' AS KACFPCAP,
                             '' AS DINDCAPF,
                             '' AS KACVALCB,
                             '' AS VMTLIMCB,
                             '' AS VMTDEFCB,
                             '' AS VTXLIMCB,
                             '' AS VTXDEFCB,
                             '' AS KACTPTARCB,
                             '' AS DINDLIBTAR
                             FROM USVTIMV01."LIFE_COVER" LC  
                             LEFT JOIN (
                          	 SELECT	
                          	 PRO."DEFFECDATE",
                          	 PRO."NPRODUCT",
                          	 PRO."NBRANCH",
                          	 PRO."DNULLDATE",
                             CASE WHEN PRO."DNULLDATE" IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                             FROM (SELECT	PRO.*,
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
                          		    FROM	USVTIMV01."PRODUCT" PRO) PR0, USVTIMV01."PRODUCT" PRO WHERE PRO.CTID = PR0.PRO_ID) P 
                             ON LC."NBRANCH" = P."NBRANCH" AND LC."NPRODUCT"  = P."NPRODUCT"
                             WHERE LC."DCOMPDATE"  BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}')
                     ) AS TMP
                     '''
  
  #EJECUTAR CONSULTA
  L_DF_ABPRCOB_VTIME = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_ABPRCOB_VTIME).load()
  L_ABPRCOB_INSIS = f'''
                    (                      
                    SELECT
                    'D' AS INDDETREC,
                    'ABPRCOB' AS TABLAIFRS17,
                    '' AS PK,
                    '' AS DTPREG,
                    '' AS TIOCPROC,
                    CAST(CAST(CNC."VALID_FROM" AS DATE) AS TEXT)  AS TIOCFRM,
                    '' AS TIOCTO,
                    'PNV' AS KGIORIGM,
                    'LPV' AS DCOMPA,
                    '' AS DMARCA,
                    CAST(CAST (CNP."PRODUCT_CODE" AS INT) AS TEXT) AS KABPRODT,
                    SUBSTRING(CAST(CAST(CNC."COVER_REP_ID" as BIGINT) AS VARCHAR),5,10) AS KGCTPCBT,
                    '' AS KACINDOPS,
                    CASE CNC."MANDATORY"
                    WHEN 'Y' THEN 'SI'
                    WHEN 'N' THEN 'NO'
                    ELSE ''
                    END KACTIPCB,
                    '' AS KACTCOMP,
                    '' AS DINDNIVEL,
                    CAST(CAST(CNC."IV_RULE" AS INT) AS TEXT) AS KACSOCAP,
                    '' AS KACFMCAL,
                    '' AS KACTPDPRIS, 
                    TRUNC(CNC."OBJECT_LINK_ID", 0) AS KGCTPCBT_SUP,
                    '' AS DINDANOS,
                    '' AS KSCTPDAN,
                    '' AS KACGPRIS,
                    '' AS KGCRAMO_SAP,
                    '' AS DDURLIMINF,
                    '' AS DDURLIMSUP,
                    '' AS DLIMINFMASC, 
                    /*(
                        SELECT CPC."DEFAULT_VALUE"
                        FROM  USINSIV01."CFGLPV_POLICY_CONDITIONS" CPC
                        WHERE CPC."INSR_TYPE" = 2002 
                        AND   CPC."AS_IS_PRODUCT" IN (7)
                        AND   CPC."COND_TYPE" = ' MAX_ENTRY_AGE'
                    ) AS DLIMSUPMASC*/
                    '' AS DLIMSUPMASC,
                    '' AS DLIMINFFEM,
                    /*(
                        SELECT CPC."DEFAULT_VALUE"
                        FROM  USINSIV01."CFGLPV_POLICY_CONDITIONS" CPC
                        WHERE CPC."INSR_TYPE" = 2002 
                        AND   CPC."AS_IS_PRODUCT" IN (7)
                        AND   CPC."COND_TYPE" = ' MAX_ENTRY_AGE'
                    ) AS DLIMSUPFEM*/
                    '' AS DLIMSUPFEM,  
                    '' AS KACCLCAP,
                    '' AS KACFCAP,
                    '' AS DINDCAPF,
                    '' AS KACVALCB,
                    /*(
                        SELECT CIL."MAX_IV"
                        FROM  USINSIV01."CFGLPV_IV_LIMITS"  CIL
                        WHERE CIL."INSR_TYPE"     = CNC.PRODUCT_CODE
                        AND   CIL."AS_IS_PRODUCT" = SUPTIPO_PRODUCTO 
                    ) AS VMTLIMCB*/
                    '' AS VMTLIMCB,
                    '' AS VMTDEFCB,
                    '' AS VTXLIMCB,
                    '' AS VTXDEFCB,
                    '' AS KACTPTARCB,
                    '' AS DINDLIBTAR
                    FROM USINSIV01."CFG_NL_COVERS" CNC
                    LEFT JOIN USINSIV01."CFG_NL_PRODUCT" CNP ON CNP."PRODUCT_LINK_ID" = CNC."PRODUCT_LINK_ID"
                    WHERE CNC."VALID_FROM" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'    
                    ) AS TEMP
                    '''
  
  #EJECUTAR CONSULTA
  L_DF_ABPRCOB_INSIS = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_ABPRCOB_INSIS).load()
  #PERFORM THE UNION OPERATION
  L_DF_ABPRCOB = L_DF_ABPRCOB_INSUNIX.union(L_DF_ABPRCOB_VTIME).union(L_DF_ABPRCOB_INSIS)
  ##
  
  return L_DF_ABPRCOB