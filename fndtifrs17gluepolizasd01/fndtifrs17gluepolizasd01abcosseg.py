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
                               C.BRANCH || '-' ||  P.PRODUCT ||  '-' ||  C.POLICY  AS KABAPOL,
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
                              '' AS DUSRUPD --NO
                              FROM USINSUG01.COINSURAN C
                              LEFT JOIN USINSUG01.POLICY P
                              ON C.USERCOMP = P.USERCOMP
                              AND C.COMPANY = P.COMPANY
                              AND C.CERTYPE = P.CERTYPE
                              AND C.BRANCH = P.BRANCH
                              AND C.POLICY = P.policy --1994-03-05	2020-04-06
                              where c.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
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
                               C.BRANCH || '-' ||  P.PRODUCT ||  '-' ||  C.POLICY  AS KABAPOL,
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
                              LEFT JOIN usinsuv01.POLICY P
                              ON C.USERCOMP = P.USERCOMP
                              AND C.COMPANY = P.COMPANY
                              AND C.CERTYPE = P.CERTYPE
                              AND C.BRANCH = P.BRANCH
                              AND C.POLICY = P.policy --1997-10-02	2020-11-02
                             where c.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
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
	                        C."NBRANCH" || '-' || C."NPRODUCT"  || '-' || C."NPOLICY" AS KABAPOL,
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
                            FROM USVTIMG01."COINSURAN" C --0029-09-20	2019-12-17
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
	                        C."NBRANCH" || '-' || C."NPRODUCT"  || '-' || C."NPOLICY" AS KABAPOL,
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
                            FROM USVTIMV01."COINSURAN" C --'2013-12-05'  '2013-12-10'
                            where cast(c."DCOMPDATE" as date) between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
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