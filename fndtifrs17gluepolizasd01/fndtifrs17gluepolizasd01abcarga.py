from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):         

   l_fecha_carga_inicial = '2021-12-31'

   l_polizas_vtime_general = f'''
                             (SELECT  
                              'D' INDDETREC, 
                              'ABCARGA' TABLAIFRS17, 
                              '' PK,      
                              '' DTPREG,   --NO
                              '' TIOCPROC, --NO
                              COALESCE(CAST(CAST(DXP."DEFFECDATE" AS DATE) AS VARCHAR), '') TIOCFRM, --PENDIENTE
                              '' TIOCTO, --NO
                              'PVG' KGIORIGM, 
                              COALESCE(DXP."NBRANCH",0) || '-' || COALESCE(DXP."NPRODUCT",0) || '-' || COALESCE(DXP."NPOLICY",0) || '-' || COALESCE(DXP."NCERTIF",0) KABAPOL, --FK
                              '' KABUNRIS, --FK  
                              /*
                              *  DE ACUERDO A JAOS EL DATO SE PUDIERA OBTENER EN BASE A LA PRIMERA COVERTURA PERO LOS SISTEMAS 
                              NO TIENEN ESA INFORMACION DE MANERA DIRECTA EN LA TABLA DE RECARGOS Y DESCUENTOS
                              (
                                SELECT C."NCOVER" FROM USVTIMG01."COVER" C 
                                WHERE C."SCERTYPE" = '2' 
                                AND   C."NBRANCH" = DXP."NBRANCH"  
                                AND   C."NPOLICY" = DXP."NPOLICY"
                                AND   C."NCERTIF" = DXP."NCERTIF" 
                                AND   C."NCOVER"  = 1
                                AND   C."DEFFECDATE" <= DXP."DEFFECDATE" 
                                AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > DXP."DEFFECDATE") 
                              )*/
                              '' KGCTPCBT, 
                              '' KACCDFDO, --VALOR VACIO
                              COALESCE(DXP."SDISEXPRI" , '') KACTPCAG,
                              DXP."NDISC_CODE" KACCDCAG, 
                              DXP."NAMOUNT" VMTCARGA, 
                              COALESCE(CAST(CAST(DXP."DEFFECDATE" AS DATE) AS VARCHAR), '') TULTMALT, 
                              '' DUSRUPD, --NO
                              'LPG' DCOMPA, 
                              '' DMARCA, --NO
                              '' DINCPRM, --VALOR VACIO
                              CASE 
                              WHEN ( DXP."NAMOUNT" != 0 AND DXP."NAMOUNT" IS NOT NULL) AND ( CAST(DXP."NPERCENT" AS INTEGER)= 0 AND DXP."NPERCENT" IS NULL) 
                              THEN 'IMPORTE' ELSE 'PORCENTAJE' END KACTPVCG, 
                              '' DDURACAO, 
                              '' KACTPCBB --valor vacio
                              FROM
                              ( 
                                (SELECT DX."DEFFECDATE", DX."NBRANCH", DX."NPRODUCT", DX."NPOLICY", DX."NCERTIF", DX."NDISC_CODE", DX."NAMOUNT", DX."NPERCENT", DE."SDISEXPRI" 
                                FROM USVTIMG01."POLICY" P
                                LEFT JOIN USVTIMG01."CERTIFICAT" CERT ON CERT."SCERTYPE" = P."SCERTYPE" AND CERT."NBRANCH" = P."NBRANCH" AND CERT."NPRODUCT" = P."NPRODUCT" AND CERT."NPOLICY" = P."NPOLICY"
                                JOIN USVTIMG01."DISC_XPREM" DX        ON DX."SCERTYPE" = P."SCERTYPE" AND   DX."NBRANCH" = P."NBRANCH" AND DX."NPRODUCT" = P."NPRODUCT" AND DX."NPOLICY" = P."NPOLICY" AND DX."NCERTIF" = CERT."NCERTIF" AND DX."DEFFECDATE" <= P."DSTARTDATE" AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                                LEFT JOIN USVTIMG01."DISCO_EXPR" DE   ON DX."NBRANCH" = DE."NBRANCH" AND DX."NPRODUCT" = DE."NPRODUCT" AND DX."NDISC_CODE" = DE."NDISEXPRC"
                                WHERE P."SCERTYPE" = '2' 
                                AND P."SSTATUS_POL" NOT IN ('2', '3')
                                AND ((P."SPOLITYPE" = '1' AND P."DEXPIRDAT" >= '{l_fecha_carga_inicial}' AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '{l_fecha_carga_inicial}'))
                                OR  (P."SPOLITYPE" <> '1' AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}')))
                                AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')

                                /*UNION 
								
                                (SELECT DX."DEFFECDATE", DX."NBRANCH", DX."NPRODUCT", DX."NPOLICY", DX."NCERTIF", DX."NDISC_CODE", DX."NAMOUNT", DX."NPERCENT", DE."SDISEXPRI" 
                                FROM USVTIMG01."POLICY" P
                                JOIN USVTIMG01."CERTIFICAT" CERT ON CERT."SCERTYPE" = P."SCERTYPE" AND CERT."NBRANCH" = P."NBRANCH" AND CERT."NPRODUCT" = P."NPRODUCT" AND CERT."NPOLICY" = P."NPOLICY"
                                JOIN USVTIMG01."DISC_XPREM" DX        ON DX."SCERTYPE" = P."SCERTYPE" AND   DX."NBRANCH" = P."NBRANCH" AND DX."NPRODUCT" = P."NPRODUCT" AND DX."NPOLICY" = P."NPOLICY" AND DX."NCERTIF" = CERT."NCERTIF" AND DX."DEFFECDATE" <= P."DSTARTDATE" AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                                LEFT JOIN USVTIMG01."DISCO_EXPR" DE   ON DX."NBRANCH" = DE."NBRANCH" AND DX."NPRODUCT" = DE."NPRODUCT" AND DX."NDISC_CODE" = DE."NDISEXPRC"
                                JOIN USVTIMG01."CLAIM" CLA ON CLA."SCERTYPE" = P."SCERTYPE" AND CLA."NBRANCH" = P."NBRANCH" AND CLA."NPRODUCT" = P."NPRODUCT" AND CLA."NPOLICY" = P."NPOLICY" AND CLA."NCERTIF" = 0
                                JOIN(
                                      SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMG01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                      JOIN USVTIMG01."CLAIM_HIS" CLH ON COALESCE(CLH."NCLAIM", 0) > 0 AND CLH."NOPER_TYPE" = CSV."SVALUE" AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                    ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                WHERE P."SCERTYPE" = '2' 
                                AND P."SSTATUS_POL" NOT IN ('2','3') 
                                AND P."SPOLITYPE" = '1' 
                                AND (P."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR P."DNULLDATE" < '{l_fecha_carga_inicial}')
                                AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')

                                UNION 

                                (SELECT DX."DEFFECDATE", DX."NBRANCH", DX."NPRODUCT", DX."NPOLICY", DX."NCERTIF", DX."NDISC_CODE", DX."NAMOUNT", DX."NPERCENT", DE."SDISEXPRI" FROM USVTIMG01."POLICY" P
                                 LEFT JOIN USVTIMG01."CERTIFICAT" CERT ON CERT."SCERTYPE" = P."SCERTYPE" AND CERT."NBRANCH" = P."NBRANCH" AND CERT."NPRODUCT" = P."NPRODUCT" AND CERT."NPOLICY" = P."NPOLICY"
                                 JOIN USVTIMG01."DISC_XPREM" DX        ON   DX."SCERTYPE" = P."SCERTYPE" AND   DX."NBRANCH" = P."NBRANCH" AND   DX."NPRODUCT" = P."NPRODUCT" AND   DX."NPOLICY" = P."NPOLICY" AND DX."NCERTIF" = CERT."NCERTIF" AND DX."DEFFECDATE" <= P."DSTARTDATE" AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                                 LEFT JOIN USVTIMG01."DISCO_EXPR" DE   ON   DX."NBRANCH" = DE."NBRANCH" AND DX."NPRODUCT" = DE."NPRODUCT" AND DX."NDISC_CODE" = DE."NDISEXPRC"
                                 JOIN USVTIMG01."CLAIM" CLA ON CLA."SCERTYPE" = CERT."SCERTYPE" AND CLA."NBRANCH" = CERT."NBRANCH" AND CLA."NPOLICY" = CERT."NPOLICY"  AND CLA."NCERTIF" =  CERT."NCERTIF"
                                 JOIN (
                                       SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMG01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                       JOIN USVTIMG01."CLAIM_HIS" CLH ON COALESCE(CLH."NCLAIM", 0) > 0 AND CLH."NOPER_TYPE" = CSV."SVALUE" AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                      ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                 WHERE P."SCERTYPE" = '2' 
                                 AND P."SSTATUS_POL" NOT IN ('2','3') 
                                 AND P."SPOLITYPE" <> '1' 
                                 AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}')
                                 AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')*/
                              ) AS DXP) AS TMP
                              '''
   
   l_df_polizas_vtime_general = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_vtime_general).load() 

   print('USVTIMG01 EXITOSO')

   l_polizas_vtime_vida = f'''
                          (SELECT 
                           'D' INDDETREC, 
                           'ABCARGA' TABLAIFRS17, 
                           '' PK, --PENDIENTE
                           '' DTPREG, --NO
                           '' TIOCPROC, --NO
                           COALESCE(CAST(CAST(DXP."DEFFECDATE" AS DATE) AS VARCHAR), '') TIOCFRM, --PENDIENTE
                           '' TIOCTO, --NO
                           'PVV' KGIORIGM, 
                           COALESCE(DXP."NBRANCH", 0) || '-' || COALESCE(DXP."NPRODUCT",0) || '-' || COALESCE(DXP."NPOLICY",0) || '-' || COALESCE(DXP."NCERTIF",0) KABAPOL, --FK
                           '' KABUNRIS, --VALOR VACIO  
                           /*
                           DE ACUERDO A JAOS EL DATO SE PUDIERA OBTENER EN BASE A LA PRIMERA COVERTURA PERO LOS SISTEMAS 
                           NO TIENEN ESA INFORMACION DE MANERA DIRECTA EN LA TABLA DE RECARGOS Y DESCUENTOS
                           (SELECT C."NCOVER" FROM USVTIMV01."COVER" C 
                             WHERE C."SCERTYPE" = '2' 
                             AND   C."NBRANCH" = DXP."NBRANCH"  
                             AND   C."NPOLICY" = DXP."NPOLICY"
                             AND   C."NCERTIF" = DXP."NCERTIF" 
                             AND   C."NCOVER"  = 1
                             AND   C."DEFFECDATE" <= DXP."DEFFECDATE" 
                             AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > DXP."DEFFECDATE"))*/
                             '' KGCTPCBT, 
                             '' KACCDFDO, --VALOR VACIO
                             COALESCE(DXP."SDISEXPRI" , '') KACTPCAG,
                             DXP."NDISC_CODE" KACCDCAG, 
                             DXP."NAMOUNT" VMTCARGA, 
                             COALESCE(CAST(CAST(DXP."DEFFECDATE" AS DATE) AS VARCHAR), '') TULTMALT, 
                             '' DUSRUPD, --NO
                             'LPV' DCOMPA, 
                             '' DMARCA, --NO
                             '' DINCPRM, --VALOR VACIO
                             CASE WHEN (DXP."NAMOUNT" != 0 AND DXP."NAMOUNT" IS NOT NULL) 
                                  AND  (CAST(DXP."NPERCENT" AS INTEGER) = 0 AND DXP."NPERCENT" IS NULL) THEN 'IMPORTE' 
                             ELSE 'PORCENTAJE' 
                             END KACTPVCG, 
                             '' DDURACAO, --VALOR VACIO
                             '' KACTPCBB --VALOR VACIO
                             FROM
                             (
                               (SELECT DX."DEFFECDATE", DX."NBRANCH", DX."NPRODUCT", DX."NPOLICY", DX."NCERTIF", DX."NDISC_CODE", DX."NAMOUNT", DX."NPERCENT", DE."SDISEXPRI" 
                               FROM USVTIMV01."POLICY" P
                               LEFT JOIN USVTIMV01."CERTIFICAT" CERT ON CERT."SCERTYPE" = P."SCERTYPE" AND CERT."NBRANCH" = P."NBRANCH" AND CERT."NPRODUCT" = P."NPRODUCT" AND CERT."NPOLICY" = P."NPOLICY"
                               JOIN USVTIMV01."DISC_XPREM" DX ON DX."SCERTYPE" = P."SCERTYPE" AND DX."NBRANCH" = P."NBRANCH" AND DX."NPRODUCT" = P."NPRODUCT" AND DX."NPOLICY" = P."NPOLICY" AND DX."NCERTIF" = CERT."NCERTIF" AND DX."DEFFECDATE" <= P."DSTARTDATE" AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                               LEFT JOIN USVTIMV01."DISCO_EXPR" DE ON DX."NBRANCH" = DE."NBRANCH" AND DX."NPRODUCT" = DE."NPRODUCT" AND DX."NDISC_CODE" = DE."NDISEXPRC"
                               WHERE P."SCERTYPE" = '2'  AND P."SSTATUS_POL" NOT IN ('2', '3')
                               AND ((P."SPOLITYPE" = '1' AND P."DEXPIRDAT" >= '{l_fecha_carga_inicial}' AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '{l_fecha_carga_inicial}'))
                               OR  (P."SPOLITYPE" <> '1' AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}')))
                               AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' LIMIT 100)

                               /*UNION 
                              
                               (SELECT DX."DEFFECDATE", DX."NBRANCH", DX."NPRODUCT", DX."NPOLICY", DX."NCERTIF", DX."NDISC_CODE", DX."NAMOUNT", DX."NPERCENT", DE."SDISEXPRI" 
                               FROM USVTIMV01."POLICY" P
                               LEFT JOIN USVTIMV01."CERTIFICAT" CERT ON CERT."SCERTYPE" = P."SCERTYPE" AND CERT."NBRANCH" = P."NBRANCH" AND CERT."NPRODUCT" = P."NPRODUCT" AND CERT."NPOLICY" = P."NPOLICY"
                               JOIN USVTIMV01."DISC_XPREM" DX ON DX."SCERTYPE" = P."SCERTYPE" AND DX."NBRANCH" = P."NBRANCH" AND DX."NPRODUCT" = P."NPRODUCT" AND DX."NPOLICY" = P."NPOLICY" AND DX."NCERTIF" = CERT."NCERTIF" AND DX."DEFFECDATE" <= P."DSTARTDATE" AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                               LEFT JOIN USVTIMV01."DISCO_EXPR" DE ON DX."NBRANCH" = DE."NBRANCH" AND DX."NPRODUCT" = DE."NPRODUCT" AND DX."NDISC_CODE" = DE."NDISEXPRC"                                     
                                JOIN USVTIMV01."CLAIM" CLA ON CLA."SCERTYPE" = P."SCERTYPE" AND CLA."NPOLICY" = P."NPOLICY" AND CLA."NBRANCH" = P."NBRANCH"
                                JOIN (
                                      SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMV01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                      JOIN USVTIMV01."CLAIM_HIS" CLH ON COALESCE(CLH."NCLAIM", 0) > 0 AND CLH."NOPER_TYPE" = CSV."SVALUE" AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                     ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                WHERE P."SCERTYPE" = '2' 
                                AND P."SSTATUS_POL" NOT IN ('2','3') 
                                AND P."SPOLITYPE" = '1' 
                                AND (P."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR P."DNULLDATE" < '{l_fecha_carga_inicial}')
                                AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' LIMIT 100)

                                UNION

                                (SELECT DX."DEFFECDATE", DX."NBRANCH", DX."NPRODUCT", DX."NPOLICY", DX."NCERTIF", DX."NDISC_CODE", DX."NAMOUNT", DX."NPERCENT", DE."SDISEXPRI" 
                                 FROM USVTIMV01."POLICY" P
                                 LEFT JOIN USVTIMV01."CERTIFICAT" CERT ON CERT."SCERTYPE" = P."SCERTYPE" AND CERT."NBRANCH" = P."NBRANCH" AND CERT."NPRODUCT" = P."NPRODUCT" AND CERT."NPOLICY" = P."NPOLICY"
                                 JOIN USVTIMV01."DISC_XPREM" DX ON DX."SCERTYPE" = P."SCERTYPE" AND DX."NBRANCH" = P."NBRANCH" AND DX."NPRODUCT" = P."NPRODUCT" AND DX."NPOLICY" = P."NPOLICY" AND DX."NCERTIF" = CERT."NCERTIF" AND DX."DEFFECDATE" <= P."DSTARTDATE" AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                                 LEFT JOIN USVTIMV01."DISCO_EXPR" DE ON DX."NBRANCH" = DE."NBRANCH" AND DX."NPRODUCT" = DE."NPRODUCT" AND DX."NDISC_CODE" = DE."NDISEXPRC"
                                 JOIN USVTIMV01."CLAIM" CLA ON CLA."SCERTYPE" = CERT."SCERTYPE" AND CLA."NBRANCH" = CERT."NBRANCH" AND CLA."NPOLICY" = CERT."NPOLICY"  AND CLA."NCERTIF" =  CERT."NCERTIF"
                                 JOIN (
                                       SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMV01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                       JOIN USVTIMV01."CLAIM_HIS" CLH ON COALESCE(CLH."NCLAIM", 0) > 0 AND CLH."NOPER_TYPE" = CSV."SVALUE" AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                      ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                 WHERE P."SCERTYPE" = '2' 
                                 AND P."SSTATUS_POL" NOT IN ('2','3') 
                                 AND P."SPOLITYPE" <> '1' 
                                 AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}')  
                                 AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' LIMIT 100)*/
                             ) AS DXP) AS TMP
                          '''
   
   l_df_polizas_vtime_vida = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_vtime_vida).load() 
   
   print('USVTIMV01 EXITOSO')
   
   #----------------------------------------------------------------------------------------------------------------------------------#

   #DECLARAR CONSULTA INSUNIX
   l_polizas_insunix_general = f'''
                                ( SELECT
                                  'D' INDDETREC,
                                  'ABCARGA' TABLAIFRS17,
                                  '' PK,--PENDIENTE
                                  '' DTPREG,--NO
                                  '' TIOCPROC,--NO
                                  COALESCE(CAST(CAST(DXP.EFFECDATE AS DATE) AS VARCHAR),'') TIOCFRM,--PENDIENTE
                                  '' TIOCTO,--NO
                                  'PIG' KGIORIGM,
                                  COALESCE(DXP.BRANCH, 0) || '-' || COALESCE(DXP.PRODUCT,0) || '-' || COALESCE(DXP.SUB_PRODUCT,0) || '-' || COALESCE(DXP.POLICY,0) || '-' || COALESCE(DXP.CERTIF,0) KABAPOL,--FK PENDIENTE
                                  '' KABUNRIS,  --VALOR VACIO
                                  '' KGCTPCBT,  --VALOR VACIO
                                  '' KACCDFDO,  -- VALOR VACIO
                                  COALESCE(DXP.TYPE, '') KACTPCAG,
                                  COALESCE(DXP.CODE, 0) KACCDCAG,
                                  COALESCE(DXP.AMOUNT, 0) VMTCARGA,
                                  COALESCE(CAST(CAST(DXP.EFFECDATE AS DATE) AS VARCHAR),'') TULTMALT,
                                  '' DUSRUPD,   --NO
                                  'LPG' DCOMPA,
                                  '' DMARCA,    --NO
                                  '' DINCPRM,    --VALOR VACIO
                                  CASE
                                  WHEN (DXP.AMOUNT != 0 AND DXP.AMOUNT IS NOT NULL) AND (CAST(DXP.PERCENT AS INTEGER) = 0 AND DXP.PERCENT IS NULL) THEN 'IMPORTE'
                                  ELSE 'PORCENTAJE'
                                  END KACTPVCG,
                                  '' DDURACAO, --VALOR VACIO
                                  '' KACTPCBB  --VALOR VACIO
                                  FROM
                                  (
                                     SELECT 
                                     DX.EFFECDATE,  
                                     DX.TYPE, 
                                     DX.CODE, 
                                     DX.AMOUNT, 
                                     DX.PERCENT, P.* 
                                     FROM
                                     (
	                                     SELECT  
	                                     P.USERCOMP,
	                                     P.COMPANY,
	                                     P.CERTYPE,
	                                     P.BRANCH, 
	                                     P.PRODUCT, 
	                                     PSP.SUB_PRODUCT, 
	                                     P.POLICY,                                      
	                                     CERT.CERTIF,
	                                     CASE 
	                                     	  WHEN P.POLITYPE  = '1' THEN P.EFFECDATE
	                                     	  WHEN P.POLITYPE <> '1' THEN CERT.EFFECDATE 
	                                     END EFFECDATE_VAL                                     
	                                     FROM USINSUG01.POLICY P 
	                                     LEFT JOIN USINSUG01.CERTIFICAT CERT 
	                                     ON CERT.USERCOMP = P.USERCOMP 
	                                     AND CERT.COMPANY = P.COMPANY 
	                                     AND CERT.CERTYPE = P.CERTYPE 
	                                     AND CERT.BRANCH = P.BRANCH 
	                                     AND CERT.PRODUCT = P.PRODUCT 
	                                     AND CERT.POLICY = P.POLICY                                     
	                                     JOIN USINSUG01.POL_SUBPRODUCT PSP 
	                                     ON  PSP.USERCOMP = P.USERCOMP 
	                                     AND PSP.COMPANY  = P.COMPANY 
	                                     AND PSP.CERTYPE  = P.CERTYPE 
	                                     AND PSP.BRANCH   = P.BRANCH 
	                                     AND PSP.POLICY   = P.POLICY		    
	                                     AND PSP.PRODUCT  = P.PRODUCT
	                                     WHERE P.CERTYPE  = '2'
                                       AND P.STATUS_POL NOT IN ('2','3') 
                                     	 AND ((P.POLITYPE = '1' AND P.EXPIRDAT >= '{l_fecha_carga_inicial}' AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}'))
                                     	    OR (P.POLITYPE <> '1' AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}')))

                                       /*  
                                       UNION

                                       SELECT
                                       P.USERCOMP,
	                                     P.COMPANY,
	                                     P.CERTYPE,
	                                     P.BRANCH, 
	                                     P.PRODUCT, 
	                                     PSP.SUB_PRODUCT, 
	                                     P.POLICY,                                      
	                                     CERT.CERTIF,
	                                     CASE 
	                                     	  WHEN P.POLITYPE  = '1' THEN P.EFFECDATE
	                                     	  WHEN P.POLITYPE <> '1' THEN CERT.EFFECDATE 
	                                     END EFFECDATE_VAL 
                                       FROM USINSUG01.POLICY P 
                                       LEFT JOIN USINSUG01.CERTIFICAT CERT 
                                       ON CERT.USERCOMP = P.USERCOMP 
                                       AND CERT.COMPANY = P.COMPANY 
                                       AND CERT.CERTYPE = P.CERTYPE 
                                       AND CERT.BRANCH = P.BRANCH 
                                       AND CERT.PRODUCT = P.PRODUCT 
                                       AND CERT.POLICY = P.POLICY                                     
                                       JOIN USINSUG01.POL_SUBPRODUCT PSP 
                                       ON  PSP.USERCOMP = P.USERCOMP 
                                       AND PSP.COMPANY  = P.COMPANY 
                                       AND PSP.CERTYPE  = P.CERTYPE 
                                       AND PSP.BRANCH   = P.BRANCH 
                                       AND PSP.POLICY   = P.POLICY		    
                                       AND PSP.PRODUCT  = P.PRODUCT
                                       WHERE P.CERTYPE  = '2' 
                                       AND P.STATUS_POL NOT IN ('2','3') AND P.POLITYPE = '1'
                                       AND (((P.EXPIRDAT < '{l_fecha_carga_inicial}' OR P.NULLDATE < '{l_fecha_carga_inicial}')
                                       AND EXISTS (SELECT 1 FROM  USINSUG01.CLAIM CLA    
                                                   JOIN  USINSUG01.CLAIM_HIS CLH 
                                                   ON CLH.USERCOMP = CLA.USERCOMP 
                                                   AND CLH.COMPANY = CLA.COMPANY 
                                                   AND CLH.BRANCH  = CLA.BRANCH 
                                                   AND CLH.CLAIM   = CLA.CLAIM
                                                   WHERE CLA.USERCOMP = P.USERCOMP 
                                                   AND CLA.COMPANY = P.COMPANY 
                                                   AND CLA.BRANCH = P.BRANCH 
                                                   AND CLA.PRODUCT = P.PRODUCT 
                                                   AND CLA.POLICY = P.POLICY 
                                                   AND CLA.CERTIF = 0 
                                                   AND TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2))
                                        		                                   FROM 	USINSUG01.TAB_CL_OPE TCL
                                        		                                   WHERE  (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                                   AND CLH.OPERDATE >= '{l_fecha_carga_inicial}'))                                     
                                       /*AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'*/)

                                       UNION

                                       SELECT
                                       P.USERCOMP,
	                                     P.COMPANY,
	                                     P.CERTYPE,
	                                     P.BRANCH, 
	                                     P.PRODUCT, 
	                                     PSP.SUB_PRODUCT, 
	                                     P.POLICY,                                      
	                                     CERT.CERTIF,
	                                     CASE 
	                                     	  WHEN P.POLITYPE  = '1' THEN P.EFFECDATE
	                                     	  WHEN P.POLITYPE <> '1' THEN CERT.EFFECDATE 
	                                     END EFFECDATE_VAL
                                       FROM USINSUG01.POLICY P 
                                       LEFT JOIN USINSUG01.CERTIFICAT CERT 
                                       ON CERT.USERCOMP = P.USERCOMP 
                                       AND CERT.COMPANY = P.COMPANY 
                                       AND CERT.CERTYPE = P.CERTYPE 
                                       AND CERT.BRANCH = P.BRANCH 
                                       AND CERT.PRODUCT = P.PRODUCT 
                                       AND CERT.POLICY = P.POLICY                                     
                                       JOIN USINSUG01.POL_SUBPRODUCT PSP 
                                       ON  PSP.USERCOMP = P.USERCOMP 
                                       AND PSP.COMPANY  = P.COMPANY 
                                       AND PSP.CERTYPE  = P.CERTYPE 
                                       AND PSP.BRANCH   = P.BRANCH 
                                       AND PSP.POLICY = P.POLICY		    
                                       AND PSP.PRODUCT  = P.PRODUCT
                                       WHERE P.CERTYPE  = '2' AND P.STATUS_POL NOT IN ('2','3') AND P.POLITYPE <> '1'
                                       AND (((CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  OR  CERT.NULLDATE < '{l_fecha_carga_inicial}') 
                                       AND EXISTS (SELECT 1 FROM USINSUG01.CLAIM CLA    
                                                   JOIN  USINSUG01.CLAIM_HIS CLH  
                                                   ON CLA.USERCOMP = CLH.USERCOMP 
                                                   AND CLA.COMPANY = CLH.COMPANY 
                                                   AND CLA.BRANCH = CLH.BRANCH  
                                                   AND CLH.CLAIM = CLA.CLAIM
                                                   WHERE CLA.USERCOMP = P.USERCOMP 
                                                   AND CLA.COMPANY = P.COMPANY 
                                                   AND CLA.BRANCH = P.BRANCH 
                                                   AND CLA.PRODUCT = P.PRODUCT 
                                                   AND CLA.POLICY = P.POLICY 
                                                   AND CLA.CERTIF = CERT.CERTIF 
                                                   AND TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) 
                                                                               FROM  USINSUG01.TAB_CL_OPE TCL 
                                                                               WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) AND CLH.OPERDATE >= '{l_fecha_carga_inicial}')))
                                       /*AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'*/
                                       */
	                                    ) P
                                     LEFT JOIN USINSUG01.DISC_XPREM DX 
                                     ON DX.USERCOMP = P.USERCOMP 
                                     AND DX.COMPANY = P.COMPANY 
                                     AND DX.BRANCH  = P.BRANCH 
                                     AND DX.CERTYPE = P.CERTYPE 
                                     AND DX.POLICY  = P.POLICY 
                                     AND DX.CERTIF  = P.CERTIF 
                                     AND DX.EFFECDATE <= P.EFFECDATE_VAL 
                                     AND (DX.NULLDATE IS NULL OR DX.NULLDATE > P.EFFECDATE_VAL)
                                  ) DXP ) AS TMP
                                '''
   #Ejecutar consulta
   l_df_polizas_insunix_general = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_insunix_general).load()

   print('USINSUG01 EXITOSO')
   
   l_polizas_insunix_vida = f'''
                            (
                              SELECT 
                              'D' INDDETREC, 
                              'ABCARGA' TABLAIFRS17, 
                              '' PK, --PENDIENTE
                              '' DTPREG, --NO
                              '' TIOCPROC, --NO
                              COALESCE(CAST(CAST(DX.EFFECDATE AS DATE) AS VARCHAR), '') TIOCFRM, --PENDIENTE
                              '' TIOCTO, --NO
                              'PIV' KGIORIGM, 
                              COALESCE(DX.BRANCH, 0) || '-' || COALESCE(DX.PRODUCT, 0) || '-' || COALESCE(DX.POLICY,0) || '-' || COALESCE(DX.CERTIF,0) KABAPOL, --FK
                              '' KABUNRIS, --VALOR VACIO
                              /*
                              DE ACUERDO A JAOS EL DATO SE PUDIERA OBTENER EN BASE A LA PRIMERA COVERTURA PERO LOS SISTEMAS 
                              NO TIENEN ESA INFORMACION DE MANERA DIRECTA EN LA TABLA DE RECARGOS Y DESCUENTOS
                              (
                                SELECT C.COVER FROM USINSUV01.COVER C 
                                WHERE C.CERTYPE = '2' 
                                AND C.BRANCH = DX.BRANCH 
                                AND C.POLICY = DX.POLICY
                                AND C.CERTIF = DX.CERTIF 
                                AND C.COVER = 1
                                AND C.EFFECDATE <= DX.EFFECDATE 
                                AND (C.NULLDATE IS NULL OR C.NULLDATE > DX.EFFECDATE) 
                              ),*/
                              '' KGCTPCBT, 
                              '' KACCDFDO, --VALOR VACIO
                              COALESCE(DX.TYPE, '') KACTPCAG, 
                              COALESCE (DX.CODE, 0) KACCDCAG, 
                              COALESCE (DX.AMOUNT, 0) VMTCARGA, 
                              COALESCE (CAST(CAST(DX.EFFECDATE AS DATE) AS VARCHAR), '') TULTMALT, 
                              '' DUSRUPD, --NO
                              'LPV' DCOMPA, 
                              '' DMARCA, --NO
                              '' DINCPRM, 
                              CASE WHEN (DX.AMOUNT != 0 AND DX.AMOUNT IS NOT NULL) AND (CAST(DX.PERCENT AS INTEGER)= 0 AND DX.PERCENT IS NULL) 
                              THEN 'IMPORTE' ELSE 'PORCENTAJE' END KACTPVCG, 
                              '' DDURACAO, 
                              '' KACTPCBB --VALOR VACIO 
                              FROM
                              (
                                  (SELECT DX.EFFECDATE, DX.TYPE, DX.CODE, DX.AMOUNT, DX.PERCENT, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF FROM USINSUV01.POLICY P 
                                   LEFT JOIN USINSUV01.CERTIFICAT CERT  ON  CERT.USERCOMP = P.USERCOMP AND CERT.COMPANY = P.COMPANY AND CERT.CERTYPE = P.CERTYPE AND CERT.BRANCH = P.BRANCH AND CERT.POLICY = P.POLICY  AND CERT.PRODUCT = P.PRODUCT
                                   LEFT JOIN USINSUV01.DISC_XPREM DX ON DX.USERCOMP = P.USERCOMP AND DX.COMPANY = P.COMPANY AND DX.CERTYPE = P.CERTYPE AND DX.BRANCH = P.BRANCH AND DX.POLICY = P.POLICY AND DX.CERTIF = CERT.CERTIF AND DX.EFFECDATE <= P.EFFECDATE AND (DX.NULLDATE IS NULL OR DX.NULLDATE > P.EFFECDATE)
                                   WHERE P.CERTYPE  = '2' AND P.STATUS_POL NOT IN ('2','3') 
                                   AND ((P.POLITYPE = '1' AND P.EXPIRDAT >= '{l_fecha_carga_inicial}' AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}'))
                                   OR  (P.POLITYPE <> '1' AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}')) 
                                   AND DX.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'))  LIMIT 100
                                  
                                  /*UNION 
                                  (SELECT DX.EFFECDATE, DX.TYPE, DX.CODE, DX.AMOUNT, DX.PERCENT, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF FROM USINSUV01.POLICY P 
                                   LEFT JOIN USINSUV01.CERTIFICAT CERT  ON  CERT.USERCOMP = P.USERCOMP AND CERT.COMPANY = P.COMPANY AND CERT.CERTYPE = P.CERTYPE AND CERT.BRANCH = P.BRANCH AND CERT.POLICY = P.POLICY  AND CERT.PRODUCT = P.PRODUCT                                   
                                   LEFT JOIN USINSUV01.DISC_XPREM DX ON DX.USERCOMP = P.USERCOMP AND DX.COMPANY = P.COMPANY AND DX.BRANCH = P.BRANCH AND DX.CERTYPE = P.CERTYPE AND DX.POLICY = P.POLICY AND DX.CERTIF = CERT.CERTIF AND DX.EFFECDATE <= P.EFFECDATE AND (DX.NULLDATE IS NULL OR DX.NULLDATE > P.EFFECDATE)
                                   WHERE P.CERTYPE  = '2' AND P.STATUS_POL NOT IN ('2','3') AND P.POLITYPE = '1'
                                   AND (((P.EXPIRDAT < '{l_fecha_carga_inicial}' OR P.NULLDATE < '{l_fecha_carga_inicial}')
                                   AND EXISTS (SELECT 1 FROM  USINSUV01.CLAIM CLA    
                                               JOIN  USINSUV01.CLAIM_HIS CLH ON CLH.USERCOMP = CLA.USERCOMP AND CLH.COMPANY = CLA.COMPANY AND CLH.BRANCH = CLA.BRANCH AND CLH.CLAIM = CLA.CLAIM
                                               WHERE CLA.USERCOMP = P.USERCOMP AND CLA.COMPANY = P.COMPANY AND CLA.BRANCH = P.BRANCH AND CLA.PRODUCT = P.PRODUCT AND CLA.POLICY = P.POLICY AND CLA.CERTIF = 0 AND TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2))
                                    		                                                                                         FROM 	USINSUG01.TAB_CL_OPE TCL
                                    		                                                                                         WHERE  (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) AND CLH.OPERDATE >= '{l_fecha_carga_inicial}'))                                     
                                   AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')) 

                                  UNION
                                  
                                  (SELECT DX.EFFECDATE, DX.TYPE, DX.CODE, DX.AMOUNT, DX.PERCENT, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF FROM USINSUV01.POLICY P 
                                   LEFT JOIN USINSUV01.CERTIFICAT CERT  ON  CERT.USERCOMP = P.USERCOMP AND CERT.COMPANY = P.COMPANY AND CERT.CERTYPE = P.CERTYPE AND CERT.BRANCH = P.BRANCH AND CERT.POLICY = P.POLICY  AND CERT.PRODUCT = P.PRODUCT                                     
                                   LEFT JOIN USINSUV01.DISC_XPREM DX ON DX.USERCOMP = P.USERCOMP AND DX.COMPANY = P.COMPANY AND DX.BRANCH = P.BRANCH AND DX.CERTYPE = P.CERTYPE AND DX.POLICY = P.POLICY AND DX.CERTIF = CERT.CERTIF AND DX.EFFECDATE <= P.EFFECDATE AND (DX.NULLDATE IS NULL OR DX.NULLDATE > P.EFFECDATE)
                                   WHERE P.CERTYPE = '2' AND P.STATUS_POL NOT IN ('2','3') AND P.POLITYPE <> '1'
                                   AND (((CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  OR  CERT.NULLDATE < '{l_fecha_carga_inicial}') 
                                   AND EXISTS (SELECT 1 FROM  USINSUV01.CLAIM CLA    
                                              JOIN  USINSUV01.CLAIM_HIS CLH  ON CLA.USERCOMP = CLH.USERCOMP AND CLA.COMPANY = CLH.COMPANY AND CLA.BRANCH = CLH.BRANCH  AND CLH.CLAIM = CLA.CLAIM
                                              WHERE CLA.USERCOMP = P.USERCOMP AND CLA.COMPANY = P.COMPANY AND CLA.BRANCH = P.BRANCH AND CLA.PRODUCT = P.PRODUCT AND CLA.POLICY = P.POLICY AND CLA.CERTIF = CERT.CERTIF AND TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) 
                                                                                                                                                                              FROM  USINSUG01.TAB_CL_OPE TCL 
                                                                                                                                                                              WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) AND CLH.OPERDATE >= '{l_fecha_carga_inicial}')))
                                   AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'))*/ 
                              ) DX limit 1) AS TMP
                            '''
   #Ejecutar consulta
   l_df_polizas_insunix_vida = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_polizas_insunix_vida).load()
   
   print('USINSUV01 EXITOSO')

   #----------------------------------------------------------------------------------------------------------------------------------#

   #Declara consulta INSIS
   l_polizas_insis = f'''
                      (SELECT
                      'D' INDDETREC,
                      'ABCARGA' TABLAIFRS17,
                      '' PK,
                      '' DTPREG,
                      '' TIOCPROC,
                      COALESCE(CAST(CAST(GRD."EFFECTIVE_FROM" AS DATE) AS VARCHAR),'') TIOCFRM,
                      '' TIOCTO,
                      'PNV' KGIORIGM,
                      SUBSTRING(CAST(P."POLICY_ID" AS VARCHAR),6,12)  KABAPOL,
                      SUBSTRING(CAST(P."POLICY_ID" AS VARCHAR),6,12) || '-' || COALESCE(CAST(GRD."INSURED_OBJ_ID" AS VARCHAR),'0') AS KABUNRIS,
                      '' KGCTPCBT,--EN BLANCO
                      '' KACCDFDO,--EN BLANCO
                      COALESCE(GRD."DISCOUNT_TYPE", '') AS KACTPCAG,
                      TRUNC(GRD."DISCOUNT_ID", 0) AS KACCDCAG,
                      GRD."DISCOUNT_VALUE" AS VMTCARGA,
                      '' TULTMALT,--EN BLANCO
                      '' DUSRUPD,
                      'LPV' DCOMPA,
                      '' DMARCA,
                      '' DINCPRM,--EN BLANCO
                      '' KACTPVCG,--EN BLANCO 
                      '' DDURACAO,--EN BLANCO
                      COALESCE(GRD."COVER_TYPE",'') AS KACTPCBB
                      FROM
                      USINSIV01."POLICY" P
                      LEFT JOIN USINSIV01."POLICY_ENG_POLICIES" PP ON P."POLICY_ID" = PP."POLICY_ID"
                      LEFT JOIN USINSIV01."GEN_RISK_DISCOUNT" GRD ON GRD."POLICY_ID" = P."POLICY_ID"
                      WHERE (
                              P."INSR_END" >= '{l_fecha_carga_inicial}'		
                              OR  (P."INSR_END" < '{l_fecha_carga_inicial}' AND EXISTS (
                        	      	                              		       SELECT * FROM USINSIV01."CLAIM" C
                        	      	                              		       JOIN USINSIV01."CLAIM_OBJECTS" CO ON CO."CLAIM_ID" = C."CLAIM_ID" AND CO."POLICY_ID" = C."POLICY_ID"
                        	      	                              		       JOIN USINSIV01."CLAIM_RESERVE_HISTORY" CRH ON CO."CLAIM_ID" = CRH."CLAIM_ID" AND CO."REQUEST_ID" = CO."REQUEST_ID" AND CO."CLAIM_OBJ_SEQ" = CRH."CLAIM_OBJECT_SEQ"
                        	      	                              		       WHERE C."POLICY_ID" = P."POLICY_ID"
                        	      	                              		       AND "OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                        	      	                              		       AND CAST(CRH."REGISTRATION_DATE" AS DATE) >= '{l_fecha_carga_inicial}'
                        	      	                                        )
                            )
                          )  
                      AND P."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' LIMIT 100) AS TMP
                      '''

   #Ejecutar consulta
   l_df_polizas_insis = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_insis).load()

   print('USINSIV01 EXITOSO')
   
   #----------------------------------------------------------------------------------------------------------------------------------#

   #Perform the union operation
   l_df_polizas = l_df_polizas_vtime_general.union(l_df_polizas_vtime_vida).union(l_df_polizas_insis).union(l_df_polizas_insunix_general).union(l_df_polizas_insunix_vida)

   l_df_polizas = l_df_polizas.withColumn("VMTCARGA", col("VMTCARGA").cast(DecimalType(15, 5)))

   return l_df_polizas