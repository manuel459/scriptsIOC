from pyspark.sql.types import *
from pyspark.sql.functions import col


def getData(glueContext,connection,L_FECHA_INICIO,L_FECHA_FIN):

    L_ABCOSSEG_INSUNIX_G = f'''
                             (
                             (SELECT
                              'D' AS INDDETREC,
                              'ABCOSSEG' AS TABLAIFRS17,
                              '' AS PK,
                              '' AS DTPREG, --NO
                              '' AS TIOCPROC, --NO
                              COALESCE(CAST(C.EFFECDATE AS varchar), '') AS TIOCFRM,
                              '' AS TIOCTO, --NO
                              'PIG' AS KGIORIGM, --NO
                               C.BRANCH || '-' ||  PC.PRODUCT ||  '-' || PC.SUB_PRODUCT ||  '-' ||  C.POLICY ||  '-' || PC.CERTIF AS KABAPOL,
                              'LPG' AS DCOMPA,
                              '' AS DMARCA, --NO
                              '' AS TDPLANO,--NO
                              '' AS KACAREA, --NO
                              case when coalesce(cast(c.companyc as varchar),'') in ('1','12') then '1'
                              else '2' 
                              end  AS KACTPCSG,
                              COALESCE(CAST(C.COMPANYC AS VARCHAR), '') AS DCODCSG,
                              COALESCE 
                              (
                                right ((
                                 SELECT (
                                     SELECT VT.SCOD_VT
                                     FROM USINSUG01.EQUI_VT_INX VT
                                      WHERE VT.SCOD_INX = COMP.CLIENT
                                  )
                                  FROM USINSUG01.COMPANY COMP
                                  WHERE COMP.CODE = C.COMPANYC
                                       ),13),
                               ''
                              ) AS DCREFERE,
                              COALESCE(CAST(C.SHARE AS numeric(9,6)), '0') AS VTXQUOTA,
                              '' AS VMTCAPIT,
                              0 AS VTXCOMCB,
                              0 AS VTXCOMMD,
                              COALESCE(CAST(C.EXPENSIV AS numeric(10,7)), '0') AS VTXGESTAO,
                              CASE 
                              WHEN C.COMPANYC IN (1, 12) THEN 'S'
                              ELSE 'N'
                              END  DINDNSQ,
                              '' AS DINDLID, --NO
                              '' AS DNUMDIST, --NO
                              '' AS KACTPDIS,
                              '' AS TULTALT, --NO
                              '' AS DUSRUPD --no
                              FROM USINSUG01.COINSURAN C
                              JOIN ( SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
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
                             	   JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                 (SELECT  unnest(ARRAY['usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                         'usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                         'usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01',
					                         'usinsug01','usinsug01','usinsug01']) AS "SOURCESCHEMA",  
						      unnest(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
							unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P.BRANCH AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usinsug01'
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
                             ON  C.USERCOMP = PC.USERCOMP 
                             AND C.COMPANY  = PC.COMPANY 
                             AND C.CERTYPE  = PC.CERTYPE
                             AND C.BRANCH   = PC.BRANCH 
                             AND C.POLICY   = PC.POLICY 
                             AND C.EFFECDATE <= PC.EFFECDATE 
                             AND (C.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE)
                             AND C.EFFECDATE BETWEEN '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                              limit 100
                             )
                             ) AS TMP
                             '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_INX_G")
    L_DF_ABCOSSEG_INSUNIX_G = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABCOSSEG_INSUNIX_G).load()
    print("2-TERMINO TABLA ABCOSSEG_INX_G")

    L_ABCOSSEG_INSUNIX_V = f'''
                             (
                             (SELECT
                              'D' AS INDDETREC,
                              'ABCOSSEG' AS TABLAIFRS17,
                              '' AS PK,
                              '' AS DTPREG, --NO
                              '' AS TIOCPROC, --NO
                              COALESCE(CAST(C.EFFECDATE AS varchar), '') AS TIOCFRM,
                              '' AS TIOCTO, --NO
                              'PIV' AS KGIORIGM, --NO
                               C.BRANCH || '-' ||  PC.PRODUCT ||  '-' ||  C.POLICY ||  '-' || PC.CERTIF AS KABAPOL,
                              'LPV' AS DCOMPA,
                              '' AS DMARCA, --NO
                              '' AS TDPLANO,--NO
                              '' AS KACAREA, --NO
                              case when coalesce(cast(c.companyc as varchar),'') in ('1','12') then '1'
                              else '2' 
                              end  AS KACTPCSG,
                              COALESCE(CAST(C.COMPANYC AS VARCHAR), '') AS DCODCSG,
                              COALESCE 
                              (
                                right ((
                                 SELECT (
                                     SELECT VT.SCOD_VT
                                     FROM USINSUG01.EQUI_VT_INX VT
                                      WHERE VT.SCOD_INX = COMP.CLIENT
                                  )
                                  FROM USINSUG01.COMPANY COMP
                                   WHERE COMP.CODE = C.COMPANYC
                                       ),13),
                               ''
                              ) AS DCREFERE,
                              COALESCE(CAST(C.SHARE AS numeric(9,6)), '0') AS VTXQUOTA,
                              '' AS VMTCAPIT,
                              0 AS VTXCOMCB,
                              0 AS VTXCOMMD,
                              COALESCE(CAST(C.EXPENSIV AS numeric(10,7)), '0') AS VTXGESTAO,
                              CASE 
                              WHEN C.COMPANYC IN (1, 12) THEN 'S'
                              ELSE 'N'
                              END  DINDNSQ,
                              '' AS DINDLID, --NO
                              '' AS DNUMDIST, --NO
                              '' AS KACTPDIS,
                              '' AS TULTALT, --NO
                              '' AS DUSRUPD --NO
                              FROM usinsuv01.COINSURAN C
                              JOIN ( SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE , P.POLITYPE, CERT.EFFECDATE as EFFECDATE_CERT
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
                                          AND P.EXPIRDAT >= '2010-12-31' 
                                          AND (P.NULLDATE IS NULL OR P.NULLDATE > '2010-12-31') )
                                          OR 
                                          (P.POLITYPE <> '1' -- COLECTIVAS 
                                          AND CERT.EXPIRDAT >= '2010-12-31' 
                                          AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2010-12-31'))
                                    )) AS PC	
                                    ON  C.USERCOMP = PC.USERCOMP 
                             AND C.COMPANY  = PC.COMPANY 
                             AND C.CERTYPE  = PC.CERTYPE
                             AND C.BRANCH   = PC.BRANCH 
                             AND C.POLICY   = PC.POLICY 
                             AND C.EFFECDATE <= PC.EFFECDATE 
                             AND (C.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE) --1997-10-02	2020-11-02
                             AND C.EFFECDATE BETWEEN '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                             limit 100
                             )
                             ) AS TMP
                             '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_INX_V")
    L_DF_ABCOSSEG_INSUNIX_V = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABCOSSEG_INSUNIX_V).load()
    print("2-TERMINO TABLA ABCOSSEG_INX_V")
    
    L_ABCOSSEG_VTIME_G = f'''
                            (
                            (SELECT 
                            'D' AS INDDETREC,
	                        'ABCOSSEG' AS TABLAIFRS17,
	                        '' AS PK,
	                        '' AS DTPREG, --NO
	                        '' AS TIOCPROC, --NO
	                        COALESCE(CAST (cast(C."DEFFECDATE"  AS date)AS varchar) , '' ) AS TIOCFRM,
	                        '' AS TIOCTO, --NO
	                        'PVG' AS KGIORIGM, --NO	
	                        PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" AS KABAPOL,
	                        'LPG' AS DCOMPA,
	                        '' AS DMARCA, --NO
	                        '' AS TDPLANO,--NO
	                        '' AS KACAREA, --NO
	                        case when coalesce(cast(C."NCOMPANY" as varchar),'') <> '1' then '2'
                            else '1' 
                            end  AS KACTPCSG,
	                        CAST( C."NCOMPANY"  AS VARCHAR) AS DCODCSG,
	                        COALESCE 
	                        (
		                        right((
		                        SELECT  COMP."SCLIENT"  
		                        FROM USVTIMG01."COMPANY"   COMP
		                         WHERE COMP."NCOMPANY" = C."NCOMPANY" 
		                        ),13),
	               	           ''
	                        ) AS DCREFERE,
	                        COALESCE ( CAST ( C."NSHARE"  AS numeric(9,6)), '0') AS VTXQUOTA,
	                        '' AS VMTCAPIT,
	                        0 AS VTXCOMCB, 
	                        0 AS VTXCOMMD,
	                        COALESCE ( CAST (C."NEXPENSES" AS numeric(10,7)), '0') AS VTXGESTAO,
	                        CASE C."NCOMPANY"
                            WHEN 1 THEN 'S' --CODIGO GENERALES
                            ELSE 'N'
                            END  DINDNSQ,
	                          '' AS DINDLID, --NO
	                          '' AS DNUMDIST, --NO
	                          '' AS KACTPDIS,
	                          '' AS TULTALT, --NO
	                          '' AS DUSRUPD --NO
                            FROM USVTIMG01."COINSURAN" C
                            JOIN ( SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
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
                                                AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '2018-12-31'))
                                          )) AS PC	
                                    ON  C."SCERTYPE"  = PC."SCERTYPE"
                                    AND C."NBRANCH"   = PC."NBRANCH" 
                                    AND C."NPRODUCT"  = PC."NPRODUCT"
                                    AND C."NPOLICY"   = PC."NPOLICY"
                                    AND C."DEFFECDATE" <= PC."DSTARTDATE" 
                                    AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE") --0029-09-20	2019-12-17
                            where cast(c."DCOMPDATE" as date) between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            limit 100
                            )
                            ) AS TMP
                           '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_VT")
    L_DF_ABCOSSEG_VTIME_G = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABCOSSEG_VTIME_G).load()
    print("2-TERMINO TABLA ABCOSSEG_VT")    

    L_ABCOSSEG_VTIME_V = f'''
                            (
                            (SELECT 
                            'D' AS INDDETREC,
	                        'ABCOSSEG' AS TABLAIFRS17,
	                        '' AS PK,
	                        '' AS DTPREG, --NO
	                        '' AS TIOCPROC, --NO
	                        COALESCE(CAST (cast(C."DEFFECDATE"  AS date)AS varchar) , '' ) AS TIOCFRM,
	                        '' AS TIOCTO, --NO
	                        'PVV' AS KGIORIGM, --NO	
	                        PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" AS KABAPOL,
	                        'LPV' AS DCOMPA,
	                        '' AS DMARCA, --NO
	                        '' AS TDPLANO,--NO
	                        '' AS KACAREA, --NO
	                        case when coalesce(cast(C."NCOMPANY" as varchar),'') <> '2' then '2'
                            else '1' 
                            end  AS KACTPCSG,
	                        CAST( C."NCOMPANY"  AS VARCHAR) AS DCODCSG,
	                        COALESCE 
	                        (
		                        right((
		                        SELECT  COMP."SCLIENT"  
		                        FROM USVTIMG01."COMPANY"   COMP
		                         WHERE COMP."NCOMPANY" = C."NCOMPANY" 
		                        ),13),
	               	           ''
	                        ) AS DCREFERE,
	                        COALESCE ( CAST ( C."NSHARE"  AS numeric(9,6)), '0') AS VTXQUOTA,
	                        '' AS VMTCAPIT,
	                        0 AS VTXCOMCB, 
	                        0 AS VTXCOMMD,
	                        COALESCE ( CAST (C."NEXPENSES" AS numeric(10,7)), '0') AS VTXGESTAO,
	                        CASE C."NCOMPANY"
                            WHEN 1 THEN 'S' --CODIGO GENERALES
                            ELSE 'N'
                            END  DINDNSQ,
	                          '' AS DINDLID, --NO
	                          '' AS DNUMDIST, --NO
	                          '' AS KACTPDIS,
	                          '' AS TULTALT, --NO
	                          '' AS DUSRUPD --NO
                            FROM USVTIMV01."COINSURAN" C 
                            JOIN ( SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
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
                                  AND ( (P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                      AND P."DEXPIRDAT" >= '2021-12-31' 
                                      AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '2021-12-31') )
                                      OR 
                                      (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                      AND CERT."DEXPIRDAT" >= '2021-12-31' 
                                      AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '2021-12-31'))
                                      AND P."DSTARTDATE" between '{P_FECHA_INICIO}' and '{P_FECHA_FIN}'
                                 )) AS PC	
                           ON  C."SCERTYPE"  = PC."SCERTYPE"
                           AND C."NBRANCH"   = PC."NBRANCH" 
                           AND C."NPRODUCT"  = PC."NPRODUCT"
                           AND C."NPOLICY"   = PC."NPOLICY"  
                           AND C."DEFFECDATE" <= PC."DSTARTDATE" 
                           AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE") --'2013-12-05'  '2013-12-10'
                           limit 100
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_VT")
    L_DF_ABCOSSEG_VTIME_V = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABCOSSEG_VTIME_V).load()
    print("2-TERMINO TABLA ABCOSSEG_VT")
    
    #PERFORM THE UNION OPERATION 
    L_DF_ABCOSSEG = L_DF_ABCOSSEG_INSUNIX_G.union(L_DF_ABCOSSEG_INSUNIX_V).union(L_DF_ABCOSSEG_VTIME_G).union(L_DF_ABCOSSEG_VTIME_V)
    
    L_DF_ABCOSSEG = L_DF_ABCOSSEG.withColumn("VTXQUOTA",col("VTXQUOTA").cast(DecimalType(9,6))).withColumn("VTXQUOTA",col("VTXQUOTA").cast(DecimalType(9,6))).withColumn("VTXCOMCB",col("VTXCOMCB").cast(DecimalType(7,4))).withColumn("VTXCOMMD",col("VTXCOMMD").cast(DecimalType(7,4))).withColumn("VTXGESTAO",col("VTXGESTAO").cast(DecimalType(10,7)))

    print("AQUI SE MANDE EL CONTEO")
    print(L_DF_ABCOSSEG.count())


    return L_DF_ABCOSSEG