
def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):

  L_ABPRODT_INSUNIX = f'''
                          (
                           (
                           SELECT 
                           'D' AS INDDETREC,
                           'ABPRODT' AS TABLAIFRS17,
                           '' AS PK,
                           '' AS DTPREG,
                           '' AS TIOCPROC,
                           COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TIOCFRM,
                           '' AS TIOCTO,
                           'PIG' AS KGIORIGM,
                           COALESCE(CAST(P.PRODUCT AS VARCHAR), '') AS DCODIGO,
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
                           COALESCE(CAST(P.SUB_PRODUCT AS VARCHAR), '')  AS KACSBTPRD, 
                           '' AS KACTPPMA, 
                           '' AS KACTIPIFAP 
                           FROM USINSUG01.PRODUCT P
                           JOIN USINSUG01.TABLE10B TB ON P.BRANCH = TB.BRANCH AND TB.COMPANY = 1
                           WHERE P.COMPDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}')

                          UNION ALL
                            
                          (SELECT
                           'D' AS INDDETREC,
                           'ABPRODT' AS TABLAIFRS17,
                           '' AS PK,
                           '' AS DTPREG,
                           '' AS TIOCPROC,
                           COALESCE(CAST(P.EFFECDATE AS VARCHAR), '')  AS TIOCFRM,
                           '' AS TIOCTO,
                           'PIV' AS KGIORIGM,
                           COALESCE(CAST(P.PRODUCT AS VARCHAR), '') AS DCODIGO,
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
                           FROM USINSUV01.PRODUCT P
                           JOIN USINSUG01.TABLE10B TB ON P.BRANCH = TB.BRANCH AND TB.COMPANY = 2
                           WHERE P.COMPDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}')
                          ) AS TMP
                        '''
  
  #EJECUTAR CONSULTA
  L_DF_ABPRODT_INSUNIX = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_ABPRODT_INSUNIX).load()

  #--------------------------------------------------------------------------------------------------------------------------#

  L_ABPRODT_VTIME = f'''
                          (
                            (SELECT
                            'D' AS INDDETREC,
                            'ABPRODT' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            CAST(CAST(P."DEFFECDATE" AS DATE) AS VARCHAR) AS TIOCFRM,
                            '' AS TIOCTO,
                            'PVG' AS KGIORIGM,
                            P."NPRODUCT" AS DCODIGO,
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
                            FROM USVTIMG01."PRODUCT" P
                            LEFT JOIN USVTIMG01."PRODMASTER" PM ON P."NBRANCH" = P."NBRANCH" AND P."NPRODUCT" = P."NPRODUCT"
                            WHERE P."DCOMPDATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}')

                          UNION ALL

                          ( SELECT
                            'D' AS INDDETREC,
                            'ABPRODT' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            CAST(CAST(P."DEFFECDATE" AS DATE) AS VARCHAR) AS TIOCFRM,
                            '' AS TIOCTO,
                            'PVV' AS KGIORIGM,
                            P."NPRODUCT" AS DCODIGO,
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
                            FROM USVTIMV01."PRODUCT" P
                            LEFT JOIN USVTIMV01."PRODMASTER" PM ON P."NBRANCH" = P."NBRANCH" AND P."NPRODUCT" = P."NPRODUCT"
                            WHERE P."DCOMPDATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}')
                          ) AS TMP
                       '''
  
  #EJECUTAR CONSULTA
  L_DF_ABPRODT_VTIME = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_ABPRODT_VTIME).load()

  #--------------------------------------------------------------------------------------------------------------------------#

  L_ABPRODT_INSIS = f''' 
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
                          WHERE CNP."VALID_FROM" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'
                        ) AS TMP
                       '''   
  
  #EJECUTAR CONSULTA
  L_DF_ABPRODT_INSIS = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_ABPRODT_INSIS).load()
   
  
  #PERFORM THE UNION OPERATION 
  L_DF_ABPRODT = L_DF_ABPRODT_INSUNIX.union(L_DF_ABPRODT_VTIME).union(L_DF_ABPRODT_INSIS)

  return L_DF_ABPRODT
    