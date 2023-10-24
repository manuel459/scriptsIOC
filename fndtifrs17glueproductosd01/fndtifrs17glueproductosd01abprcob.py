
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
                              COALESCE(P.BRANCH, 0) || '-' || COALESCE (P.PRODUCT, 0) AS KABPRODT,
                              COALESCE(CAST(GC.COVERGEN  AS VARCHAR), '')  AS KGCTPCBT,
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
                              COALESCE(CAST(GC.CACALMAX AS VARCHAR), '') AS VMTLIMCB,
                              '' AS VMTDEFCB,      
                              '' AS VTXLIMCB,
                              '' AS VTXDEFCB,      
                              '' AS KACTPTARCB,    
                              '' AS DINDLIBTAR
                              FROM USINSUG01.GEN_COVER GC 
                              LEFT JOIN USINSUG01.PRODUCT P ON GC.BRANCH = P.BRANCH  AND GC.PRODUCT  = P.PRODUCT
                              JOIN USINSUG01.TABLE10B TB ON GC.BRANCH = TB.BRANCH AND TB.COMPANY = 1
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
                              COALESCE(CAST(LC.COVERGEN as VARCHAR), '') AS KGCTPCBT,
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
                              LEFT JOIN USINSUV01.PRODUCT P ON LC.BRANCH = P.BRANCH  AND LC.PRODUCT  = P.PRODUCT
                              JOIN USINSUG01.TABLE10B TB ON LC.BRANCH = TB.BRANCH AND TB.COMPANY = 2
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
                            P."NBRANCH" || '-' || P."NPRODUCT" AS KABPRODT,
                            COALESCE(CAST(GC."NCOVERGEN" AS VARCHAR), '') AS KGCTPCBT,
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
                            '' AS KGCTPCBT_SUP,
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
                            LEFT JOIN USVTIMG01."PRODUCT" P ON GC."NBRANCH" = P."NBRANCH" AND GC."NPRODUCT"  = P."NPRODUCT"
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
                             P."NBRANCH" || '-' || P."NPRODUCT" AS KABPRODT,
                             COALESCE(CAST(LC."NCOVERGEN" AS VARCHAR), '') AS KGCTPCBT,
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
                             '' AS KGCTPCBT_SUP,
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
                             LEFT JOIN USVTIMV01."PRODUCT" P ON LC."NBRANCH" = P."NBRANCH" AND LC."NPRODUCT"  = P."NPRODUCT"
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
                    CAST(CAST (CNC."COVER_LINK_ID" AS INT) AS TEXT) AS KGCTPCBT,
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
                    '' AS KGCTPCBT_SUP,
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
  
  return L_DF_ABPRCOB