from pyspark.sql.types import *
from pyspark.sql.functions import col, format_number

def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):

  L_ABCOBAP_INSUNIX_LPG = f'''
                          (SELECT 
                          'D' AS INDDETREC,
                          'ABCOBAP' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          TIOCFRM,
                          '' AS TIOCTO,
                          'PIG' AS KGIORIGM,
                          KABAPOL,
                          '' AS KABUNRIS,
                          KGCTPCBT,
                          TINICIO,
                          TTERMO,
                          '' AS TSITCOB,
                          '' AS KACSITCB,
                          '' AS VMTPRMSP,
                          VMTCOMR,
                          '' AS VMTBOMAT,
                          '' AS VTXBOMAT,
                          '' AS VMTBOCOM,
                          '' AS VTXBOCOM,
                          '' AS VMTDECOM,
                          '' AS VTXDECOM,
                          '' AS VMTDETEC,
                          '' AS VTXDETEC,
                          '' AS VMTAGRAV,
                          '' AS VTXAGRAV,
                          '' AS VMTPRMTR,
                          '' AS VMTPRLIQ,
                          VMTPRMBR,
                          VTXCOB,
                          VCAPITAL, 
                          '' AS VTXCAPIT,
                          '' AS KACTPIDX,
                          '' AS VTXINDX,
                          'LPG' AS DCOMPA,
                          '' AS DMARCA,
                          '' AS TDACECOB,
                          '' AS TDCANCOB,
                          '' AS TDCRICOB,
                          TDRENOVA,
                          '' AS TDVENTRA,
                          '' AS DHORAINI,
                          VMTPREMC,
                          '' AS VMIBOMAT,
                          '' AS VMIBOCOM,
                          '' AS VMIDECOM,
                          '' AS VMIDETEC,
                          '' AS VMIRPMSP,
                          '' AS VMIPRMBR,
                          '' AS VMICOMR,
                          '' AS VMIPRLIQ,
                          '' AS VMICMNQP,
                          '' AS VMIPRMTR,
                          '' AS VMIAGRAV,
                          '' AS KACTIPCB,
                          '' AS VMTCAPLI,
                          '' AS KACTRARE,
                          '' AS KACFMCAL,
                          '' AS DFACMULT,
                          VMTCAPIN,                     
						              VMTPREIN,
                          '' AS DINDESES,
                          '' AS DINDMOTO,
                          '' AS KACSALIN,
                          '' AS VMTSALMD,
                          '' AS VTXLMRES,
                          '' AS VTXEQUIP,
                          '' AS VTXPRIOR,
                          '' AS VTXCONTR,
                          '' AS VTXESPEC,
                          '' AS DCAPMORT,
                          VMTPRRES,
                          '' AS DIDADETAR,
                          '' AS DIDADLIMCOBA,
                          '' AS KACTPDUR,
                          '' AS KGCRAMO_SAP,
                          '' AS KACTCOMP,
                          '' AS KACINDTX,
                          '' AS KACCALIDA,
                          '' AS DNCABCALP,
                          '' AS DINDNIVEL,
                          '' AS DURCOB,
                          '' AS DURPAGCOB,
                          '' AS KACTPDURCB,
                          '' AS DINCOBINDX,
                          '' AS KACGRCBT,
                          '' AS KABTRTAB_2,
                          '' AS VTXAJTBUA,
                          '' AS VMTCAPREM                         
                          FROM(
                          		 SELECT 
                               COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TIOCFRM,
                               CAST(COALESCE(C.BRANCH, 0) AS VARCHAR) ||'-'|| POL.PRODUCT
                                                                              /*(SELECT  COALESCE(P.PRODUCT, 0)
                                                                                FROM  USINSUG01.POLICY P
                                                                                WHERE P.USERCOMP = C.USERCOMP
                                                                                AND P.COMPANY = C.COMPANY
                                                                                AND P.CERTYPE = C.CERTYPE
                                                                                AND P.BRANCH = C.BRANCH
                                                                                AND P.POLICY = C.POLICY)*/
                               || '-' || COALESCE(C.POLICY, 0) || '-' || COALESCE(C.CERTIF, 0) AS KABAPOL,
                               COALESCE(( SELECT CAST(COALESCE(GC.COVERGEN, 0) AS VARCHAR) FROM USINSUG01.GEN_COVER GC 
                                 WHERE GC.USERCOMP = C.USERCOMP 
                                 AND GC.COMPANY = C.COMPANY 
                                 AND GC.BRANCH = C.BRANCH 
                                 AND GC.PRODUCT = POL.PRODUCT
                                                  /*(SELECT P.PRODUCT  FROM USINSUG01.POLICY P
                                                   WHERE P.USERCOMP = C.USERCOMP
                                                   AND P.COMPANY = C.COMPANY
                                                   AND P.CERTYPE = C.CERTYPE
                                                   AND P.BRANCH = C.BRANCH
                                                   AND P.POLICY = C.POLICY
                                                  )*/
                                 AND GC.CURRENCY = C.CURRENCY
                                 AND GC.MODULEC = C.MODULEC
                                 AND GC.COVER =C.COVER 
                                 AND GC.EFFECDATE <= C.EFFECDATE
                                 AND (GC.NULLDATE IS NULL OR GC.NULLDATE > C.EFFECDATE) LIMIT 1
                               ) ,'0') AS KGCTPCBT,
                               COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TINICIO,
                               COALESCE (CAST(C.NULLDATE  AS VARCHAR),'') AS TTERMO,
                               COALESCE(C.PREMIUM, 0) AS VMTCOMR,
                               COALESCE(C.PREMIUM,0)  AS VMTPRMBR,
                               COALESCE(C.RATECOVE, 0) AS VTXCOB,
                               COALESCE(CAST(C.CAPITAL AS VARCHAR),'0') AS VCAPITAL,
                               COALESCE(CAST (C.EFFECDATE AS VARCHAR),'') AS TDRENOVA,
                               COALESCE(CAST(((SELECT COALESCE(CO.SHARE, 0)
                               FROM USINSUG01.COINSURAN CO
                               WHERE CO.USERCOMP = C.USERCOMP 
                               AND CO.COMPANY = C.COMPANY 
                               AND CO.CERTYPE = C.CERTYPE
                               AND CO.BRANCH = C.BRANCH 
                               AND CO.POLICY = C.POLICY
                               AND CO.COMPANYC = 1
                               AND CO.EFFECDATE <= C.EFFECDATE
                               AND (CO.NULLDATE IS NULL OR CO.NULLDATE > C.EFFECDATE)) * C.PREMIUM) as VARCHAR), '100') AS VMTPREMC,
                               COALESCE(C.CAPITALI, 0) AS VMTCAPIN, -- IMPORTE DE CAPITAL DE LA COBERTURA A LA FECHA DE EMISION DE LA POLIZA,                       
					                     COALESCE((SELECT COV.PREMIUM FROM USINSUG01.COVER COV 
                               LEFT JOIN USINSUG01.CERTIFICAT CERT                           
                               ON COV.USERCOMP = CERT.USERCOMP 
                               AND COV.COMPANY  = CERT.COMPANY  
                               AND COV.CERTYPE  = CERT.CERTYPE 
                               AND COV.BRANCH   = CERT.BRANCH 
                               AND COV.POLICY   = CERT.POLICY
                               AND COV.CERTIF   = CERT.CERTIF
                               AND COV.CURRENCY = C.CURRENCY 
                               AND COV.COVER    = C.COVER
                               AND COV.MODULEC  = C.MODULEC
                               AND COV.EFFECDATE <= CERT.DATE_ORIGI
                               AND (COV.NULLDATE IS NULL OR COV.NULLDATE > CERT.DATE_ORIGI) LIMIT 1),
                               (SELECT COV.PREMIUM FROM USINSUG01.COVER COV 
                               LEFT JOIN USINSUG01.CERTIFICAT CERT                           
                               ON  COV.USERCOMP = CERT.USERCOMP 
                               AND COV.COMPANY  = CERT.COMPANY  
                               AND COV.CERTYPE  = CERT.CERTYPE 
                               AND COV.BRANCH   = CERT.BRANCH 
                               AND COV.POLICY   = CERT.POLICY
                               AND COV.CERTIF   = CERT.CERTIF
                               AND COV.CURRENCY = C.CURRENCY 
                               AND COV.COVER    = C.COVER
                               AND COV.MODULEC  = C.MODULEC
                               LEFT JOIN USINSUG01.POLICY POL
                               ON  POL.USERCOMP = CERT.USERCOMP 
                               AND POL.COMPANY  = CERT.COMPANY  
                               AND POL.CERTYPE  = CERT.CERTYPE
                               AND POL.BRANCH   = CERT.BRANCH 
                               AND POL.POLICY   = CERT.POLICY                                    
                               AND COV.EFFECDATE <= POL.DATE_ORIGI
                               AND (COV.NULLDATE IS NULL OR COV.NULLDATE > POL.DATE_ORIGI) LIMIT 1)) AS VMTPREIN,
                               COALESCE((COALESCE ((SELECT (SUM(R.SHARE/100)) * C.PREMIUM  FROM USINSUG01.REINSURAN R 
                               WHERE R.USERCOMP = C.USERCOMP 
                               AND R.COMPANY = C.COMPANY 
                               AND R.CERTYPE = C.CERTYPE  
                               AND R.BRANCH = C.BRANCH
                               AND R.POLICY = C.POLICY
                               AND R.CERTIF = C.CERTIF 
                               AND R.EFFECDATE <= C.EFFECDATE
                               AND (R.NULLDATE IS NULL OR R.NULLDATE > C.EFFECDATE)
                               AND R.TYPE <> 1),
                               (SELECT (SUM(R.SHARE/100)) * C.PREMIUM  FROM USINSUG01.REINSURAN R 
                               WHERE R.USERCOMP = C.USERCOMP 
                               AND R.COMPANY = C.COMPANY 
                               AND R.CERTYPE = C.CERTYPE  
                               AND R.BRANCH = C.BRANCH
                               AND R.POLICY = C.POLICY
                               AND R.CERTIF = 0
                               AND R.EFFECDATE <= C.EFFECDATE
                               AND (R.NULLDATE IS NULL OR R.NULLDATE > C.EFFECDATE)
                               AND R.TYPE <> 1))), 0) AS VMTPRRES,
                               CASE POL.POLITYPE
                               WHEN '1' THEN 0
                               ELSE CERT.CERTIF
                               END  CERTIF   --EVITAR REGISTROS DE DUPLICADOS PARA POLIZAS COLECTIVAS                                                     
                               FROM USINSUG01.COVER C  
                               LEFT JOIN USINSUG01.CERTIFICAT CERT
                               ON  C.USERCOMP = CERT.USERCOMP 
                               AND C.COMPANY  = CERT.COMPANY  
                               AND C.CERTYPE  = CERT.CERTYPE 
                               AND C.BRANCH   = CERT.BRANCH 
                               AND C.POLICY   = CERT.POLICY
                               AND C.CERTIF   = CERT.CERTIF
                               JOIN USINSUG01.POLICY POL
                               ON  POL.USERCOMP = C.USERCOMP 
                               AND POL.COMPANY  = C.COMPANY  
                               AND POL.CERTYPE  = C.CERTYPE
                               AND POL.BRANCH   = C.BRANCH 
                               AND POL.POLICY   = C.POLICY 
                               WHERE C.CERTYPE  = '2'
                               AND POL.STATUS_POL NOT IN ('2','3') 
                               AND ((POL.POLITYPE = '1' -- INDIVIDUAL 
                                      AND POL.EXPIRDAT >= '2021-12-31' 
                                      AND (POL.NULLDATE IS NULL OR POL.NULLDATE > '2021-12-31'))
                                      OR 
                                    (POL.POLITYPE <> '1' -- COLECTIVAS 
                                      AND CERT.EXPIRDAT >= '2021-12-31' 
                                      AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2021-12-31')))
                               AND POL.EFFECDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}') T) AS TMP
                          '''

  L_DF_ABCOBAP_INSUNIX_LPG = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_ABCOBAP_INSUNIX_LPG).load()

  L_ABCOBAP_INSUNIX_LPV = f'''
                          (SELECT 
                           'D' AS INDDETREC,
                           'ABCOBAP' AS TABLAIFRS17,
                           '' AS PK,
                           '' AS DTPREG,
                           '' AS TIOCPROC,
                           TIOCFRM,
                           '' AS TIOCTO,
                           'PIV' AS KGIORIGM,
                           KABAPOL,
                           '' AS KABUNRIS,
                           KGCTPCBT,
                           TINICIO,
                           TTERMO,
                           '' AS TSITCOB,
                           '' AS KACSITCB,
                           '' AS VMTPRMSP,
                           VMTCOMR,
                           '' AS VMTBOMAT,
                           '' AS VTXBOMAT,
                           '' AS VMTBOCOM,
                           '' AS VTXBOCOM,
                           '' AS VMTDECOM,
                           '' AS VTXDECOM,
                           '' AS VMTDETEC,
                           '' AS VTXDETEC,
                           '' AS VMTAGRAV,
                           '' AS VTXAGRAV,
                           '' AS VMTPRMTR,
                           '' AS VMTPRLIQ,
                           VMTPRMBR,
                           VTXCOB, --TASA APLICAR A LA COBERTURA
                           VCAPITAL,-- IMPORTE DE CAPITAL ASEGURADO EN EL CERTIFICADO 
                           '' AS VTXCAPIT,
                           '' AS KACTPIDX,
                           '' AS VTXINDX,
                           'LPV' AS DCOMPA,
                           '' AS DMARCA,
                           '' AS TDACECOB,
                           '' AS TDCANCOB,
                           '' AS TDCRICOB,
                           TDRENOVA,
                           '' AS TDVENTRA,
                           '' AS DHORAINI,
                           VMTPREMC,
                           '' AS VMIBOMAT,
                           '' AS VMIBOCOM,
                           '' AS VMIDECOM,
                           '' AS VMIDETEC,
                           '' AS VMIRPMSP,
                           '' AS VMIPRMBR,
                           '' AS VMICOMR,
                           '' AS VMIPRLIQ,
                           '' AS VMICMNQP,
                           '' AS VMIPRMTR,
                           '' AS VMIAGRAV,
                           '' AS KACTIPCB,
                           '' AS VMTCAPLI,
                           '' AS KACTRARE,
                           '' AS KACFMCAL,
                           '' AS DFACMULT,
                           VMTCAPIN, -- IMPORTE DE CAPITAL DE LA COBERTURA A LA FECHA DE EMISION DE LA POLIZA,
                           VMTPREIN,
                           '' AS DINDESES,
                           '' AS DINDMOTO,
                           '' AS KACSALIN,
                           '' AS VMTSALMD,
                           '' AS VTXLMRES,
                           '' AS VTXEQUIP,
                           '' AS VTXPRIOR,
                           '' AS VTXCONTR,
                           '' AS VTXESPEC,
                           '' AS DCAPMORT,
                           VMTPRRES,
                           '' AS DIDADETAR,
                           '' AS DIDADLIMCOBA,
                           '' AS KACTPDUR,
                           '' AS KGCRAMO_SAP,
                           '' AS KACTCOMP,
                           '' AS KACINDTX,
                           '' AS KACCALIDA,
                           '' AS DNCABCALP,
                           '' AS DINDNIVEL,
                           '' AS DURCOB,
                           '' AS DURPAGCOB,
                           '' AS KACTPDURCB,
                           '' AS DINCOBINDX,
                           '' AS KACGRCBT,
                           '' AS KABTRTAB_2,
                           '' AS VTXAJTBUA,
                           '' AS VMTCAPREM
                           FROM(
                                 SELECT 
                                 COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TIOCFRM,
                                 COALESCE(C.BRANCH, 0) || '-'|| C.PRODUCT
                                                                /*SELECT  P.PRODUCT 
                                                                FROM  USINSUV01.POLICY P
                                                                WHERE P.USERCOMP = C.USERCOMP
                                                                AND P.COMPANY = C.COMPANY
                                                                AND P.CERTYPE = C.CERTYPE
                                                                AND P.BRANCH = C.BRANCH
                                                                AND P.POLICY = C.POLICY*/
                                 || '-' ||  COALESCE(C.POLICY, 0)|| '-' || COALESCE(C.CERTIF, 0)  AS KABAPOL,
                                 '' AS KABUNRIS,
                                 COALESCE((SELECT COALESCE(CAST(GLC.COVERGEN AS VARCHAR), '0') FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_INXLPV*/
                                           (SELECT GC.USERCOMP,
										                        GC.COMPANY,
										                        GC.BRANCH,
										                        GC.PRODUCT,
										                        GC.CURRENCY,
										                        GC.MODULEC,
										                        GC.COVER,
										                        GC.EFFECDATE,
										                        GC.NULLDATE,
										                        GC.COVERGEN
										                        FROM USINSUV01.GEN_COVER GC
										                        UNION 
										                        SELECT LC.USERCOMP,
										                        LC.COMPANY,
										                        LC.BRANCH,
										                        LC.PRODUCT,
										                        LC.CURRENCY,
										                        0 AS MODULEC,
										                        LC.COVER,
										                        LC.EFFECDATE,
										                        LC.NULLDATE,
										                        LC.COVERGEN
										                       FROM USINSUV01.LIFE_COVER LC) GLC 
                                           WHERE GLC.USERCOMP = C.USERCOMP 
                                           AND GLC.COMPANY = C.COMPANY 
                                           AND GLC.BRANCH = C.BRANCH 
                                           AND GLC.PRODUCT = C.PRODUCT 
                                           AND GLC.CURRENCY = C.CURRENCY
                                           AND GLC.MODULEC = C.MODULEC
                                           AND GLC.COVER = C.COVER 
                                           AND GLC.EFFECDATE <= C.EFFECDATE
                                           AND (GLC.NULLDATE IS NULL OR GLC.NULLDATE > C.EFFECDATE) LIMIT 1), '0') AS KGCTPCBT,
                                 COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TINICIO,
                                 COALESCE (CAST(C.NULLDATE AS VARCHAR),'') AS TTERMO,
                                 COALESCE(C.PREMIUM, 0) AS VMTCOMR,
                                 COALESCE(C.PREMIUM,  0) AS VMTPRMBR,
                                 COALESCE(C.RATECOVE, 0) AS VTXCOB, --TASA APLICAR A LA COBERTURA
                                 COALESCE(CAST(C.CAPITAL AS VARCHAR), '0') AS VCAPITAL,-- IMPORTE DE CAPITAL ASEGURADO EN EL CERTIFICADO                         
                                 'LPV' AS DCOMPA,
                                 '' AS DMARCA,
                                 '' AS TDACECOB,
                                 '' AS TDCANCOB,
                                 '' AS TDCRICOB,
                                 COALESCE(CAST (C.EFFECDATE AS VARCHAR),'') AS TDRENOVA,
                                 '' AS TDVENTRA,
                                 '' AS DHORAINI,
                                 COALESCE(CAST(( (SELECT COALESCE(CO.SHARE, 0) FROM USINSUV01.COINSURAN CO
                                   WHERE CO.USERCOMP = C.USERCOMP 
                                   AND CO.COMPANY = C.COMPANY 
                                   AND CO.CERTYPE = C.CERTYPE
                                   AND CO.BRANCH = C.BRANCH 
                                   AND CO.POLICY = C.POLICY
                                   AND CO.COMPANYC = 12
                                   AND CO.EFFECDATE <= C.EFFECDATE
                                   AND (CO.NULLDATE IS NULL OR CO.NULLDATE > C.EFFECDATE)) * C.PREMIUM) AS VARCHAR), '100') AS VMTPREMC,                       
                                 COALESCE(C.CAPITALI, 0) AS VMTCAPIN, -- IMPORTE DE CAPITAL DE LA COBERTURA A LA FECHA DE EMISION DE LA POLIZA,
                                 COALESCE((SELECT COV.PREMIUM FROM USINSUV01.COVER COV 
                                         LEFT JOIN USINSUV01.CERTIFICAT CERT                           
                                         ON COV.USERCOMP = CERT.USERCOMP 
                                         AND COV.COMPANY  = CERT.COMPANY  
                                         AND COV.CERTYPE  = CERT.CERTYPE 
                                         AND COV.BRANCH   = CERT.BRANCH 
                                         AND COV.POLICY   = CERT.POLICY
                                         AND COV.CERTIF   = CERT.CERTIF
                                         AND COV.CURRENCY = C.CURRENCY 
                                         AND COV.COVER    = C.COVER
                                         AND COV.MODULEC  = C.MODULEC
                                         AND COV.EFFECDATE <= CERT.DATE_ORIGI
                                         AND (COV.NULLDATE IS NULL OR COV.NULLDATE > CERT.DATE_ORIGI) LIMIT 1),
                                         (SELECT COV.PREMIUM FROM USINSUV01.COVER COV 
                                         LEFT JOIN USINSUV01.CERTIFICAT CERT                           
                                         ON  COV.USERCOMP = CERT.USERCOMP 
                                         AND COV.COMPANY  = CERT.COMPANY  
                                         AND COV.CERTYPE  = CERT.CERTYPE 
                                         AND COV.BRANCH   = CERT.BRANCH 
                                         AND COV.POLICY   = CERT.POLICY
                                         AND COV.CERTIF   = CERT.CERTIF
                                         AND COV.CURRENCY = C.CURRENCY 
                                         AND COV.COVER    = C.COVER
                                         AND COV.MODULEC  = C.MODULEC
                                         LEFT JOIN USINSUV01.POLICY POL
                                         ON  POL.USERCOMP = CERT.USERCOMP 
                                         AND POL.COMPANY  = CERT.COMPANY  
                                         AND POL.CERTYPE  = CERT.CERTYPE
                                         AND POL.BRANCH   = CERT.BRANCH 
                                         AND POL.POLICY   = CERT.POLICY                                    
                                         AND COV.EFFECDATE <= POL.DATE_ORIGI
                                         AND (COV.NULLDATE IS NULL OR COV.NULLDATE > POL.DATE_ORIGI) LIMIT 1)) AS VMTPREIN,
                                 COALESCE((COALESCE ((SELECT (SUM(R.SHARE/100)) * C.PREMIUM  FROM USINSUV01.REINSURAN R 
                                 WHERE R.USERCOMP = C.USERCOMP 
                                 AND R.COMPANY = C.COMPANY 
                                 AND R.CERTYPE = C.CERTYPE  
                                 AND R.BRANCH = C.BRANCH
                                 AND R.POLICY = C.POLICY
                                 AND R.CERTIF = C.CERTIF 
                                 AND R.EFFECDATE <= C.EFFECDATE
                                 AND (R.NULLDATE IS NULL OR R.NULLDATE > C.EFFECDATE)
                                 AND R.TYPE <> 1),
                                 (SELECT (SUM(R.SHARE/100)) * C.PREMIUM  FROM USINSUV01.REINSURAN R 
                                 WHERE R.USERCOMP = C.USERCOMP 
                                 AND R.COMPANY = C.COMPANY 
                                 AND R.CERTYPE = C.CERTYPE  
                                 AND R.BRANCH = C.BRANCH
                                 AND R.POLICY = C.POLICY
                                 AND R.CERTIF = 0
                                 AND R.EFFECDATE <= C.EFFECDATE
                                 AND (R.NULLDATE IS NULL OR R.NULLDATE > C.EFFECDATE)
                                 AND R.TYPE <> 1))), 0) AS VMTPRRES
                                 /*CASE C.POLITYPE
                                 WHEN '1' THEN 0
                                 ELSE CERT.CERTIF
                                 END  CERTIF   --EVITAR REGISTROS DE DUPLICADOS PARA POLIZAS COLECTIVAS*/ 
                                 FROM 
                                 ( SELECT
                                   C.USERCOMP,
                                   C.COMPANY,
                                   C.CERTYPE,
                                   C.BRANCH,
                                   POL.PRODUCT,
                                   C.CURRENCY,
                                   C.MODULEC,
                                   C.COVER,
                                   C.EFFECDATE,
                                   C.NULLDATE,                                   
                                   C.POLICY,
                                   C.CERTIF, 
                                   C.PREMIUM,
                                   C.RATECOVE,
                                   C.CAPITAL,
                                   C.CAPITALI,
		                               CASE 
		                               WHEN POL.POLITYPE = '1' --INDIVIDUAL
                                   THEN 
                                        CASE WHEN (C.EFFECDATE <= POL.EFFECDATE AND (C.NULLDATE IS NULL OR C.NULLDATE > POL.EFFECDATE)) THEN 1		                               
		                                    ELSE 
											                      CASE	
                                        	  WHEN EXISTS ( SELECT	1
                                                          FROM	USINSUV01.COVER COV1
                                                          WHERE 	COV1.CERTYPE  = C.CERTYPE
                                                          AND     COV1.USERCOMP = C.USERCOMP
                                                          AND 	  COV1.COMPANY  = C.COMPANY		                              	                  
                                                          AND 	  COV1.BRANCH   = C.BRANCH
                                                          --AND 	COV1.PRODUCT  = C.PRODUCT
		                              	                      AND     COV1.MODULEC  = C.MODULEC
		                              	                      AND     COV1.POLICY   = C.POLICY
		                              	                      AND     COV1.CERTIF   = C.CERTIF
		                              	                      AND     COV1.CURRENCY = C.CURRENCY
		                              	                      AND     COV1.COVER    = C.COVER 
                                                          AND		  COV1.EFFECDATE <= POL.EFFECDATE
                                                          AND     (COV1.NULLDATE IS NULL OR COV1.NULLDATE > POL.EFFECDATE)) THEN 0
		                                        ELSE 
		                                            CASE	
                                            	  WHEN C.NULLDATE = (SELECT MAX(COV1.NULLDATE)
                   	 			                                         FROM	USINSUV01.COVER COV1
                   	 			                                         WHERE 	COV1.USERCOMP = C.USERCOMP
			     				                                                 AND    COV1.CERTYPE  = C.CERTYPE
                   	 			                                         AND 	  COV1.COMPANY  = C.COMPANY
                   	 			                                         AND 	  COV1.BRANCH   = C.BRANCH
                   	 			                                       --AND 	  COV1.PRODUCT  = C.PRODUCT
			     				                                                  AND   COV1.MODULEC  = C.MODULEC
			     				                                                  AND   COV1.POLICY   = C.POLICY
			     				                                                  AND   COV1.CERTIF   = C.CERTIF
			     				                                                  AND   COV1.CURRENCY = C.CURRENCY
			     				                                                  AND   COV1.COVER    = C.COVER ) THEN 1
                                                ELSE 0
                                                END 
                                            END
                                        END  
									                  ELSE
									                        CASE WHEN (C.EFFECDATE <= CERT.EFFECDATE AND (C.NULLDATE IS NULL OR C.NULLDATE > CERT.EFFECDATE)) THEN 1		                               
		                                      ELSE 
											                        CASE	
                                        	    WHEN EXISTS ( SELECT	1
                                                            FROM	  USINSUV01.COVER COV1
                                                            WHERE   COV1.CERTYPE  = C.CERTYPE
                                                            AND     COV1.USERCOMP = C.USERCOMP
                                                            AND 	  COV1.COMPANY  = C.COMPANY		                              	                  
                                                            AND 	  COV1.BRANCH   = C.BRANCH
                                                            --AND 	COV1.PRODUCT  = C.PRODUCT
		                              	                        AND     COV1.MODULEC  = C.MODULEC
		                              	                        AND     COV1.POLICY   = C.POLICY
		                              	                        AND     COV1.CERTIF   = C.CERTIF
		                              	                        AND     COV1.CURRENCY = C.CURRENCY
		                              	                        AND     COV1.COVER    = C.COVER 
                                                            AND	  COV1.EFFECDATE <= CERT.EFFECDATE
                                                            AND     (COV1.NULLDATE IS NULL OR COV1.NULLDATE > CERT.EFFECDATE)) THEN 0
		                                      ELSE 
		                                          CASE	
                                            	WHEN C.NULLDATE = (SELECT MAX(COV1.NULLDATE)
                   	 			                                   FROM	USINSUV01.COVER COV1
                   	 			                                   WHERE  COV1.USERCOMP = C.USERCOMP
			     				                                           AND    COV1.CERTYPE  = C.CERTYPE
                   	 			                                   AND 	  COV1.COMPANY  = C.COMPANY
                   	 			                                   AND 	  COV1.BRANCH   = C.BRANCH
                   	 			                                 --AND 	  COV1.PRODUCT  = C.PRODUCT
			     				                                           AND    COV1.MODULEC  = C.MODULEC
			     				                                           AND    COV1.POLICY   = C.POLICY
			     				                                           AND    COV1.CERTIF   = C.CERTIF
			     				                                           AND    COV1.CURRENCY = C.CURRENCY
			     				                                           AND    COV1.COVER    = C.COVER ) THEN 1
                                              ELSE 0
                                              END 
                                          END
                                       END 
                                   END FLAG 
                                   FROM USINSUV01.COVER C  
                                   LEFT JOIN USINSUV01.CERTIFICAT CERT
                                   ON  C.USERCOMP = CERT.USERCOMP 
                                   AND C.COMPANY  = CERT.COMPANY  
                                   AND C.CERTYPE  = CERT.CERTYPE 
                                   AND C.BRANCH   = CERT.BRANCH 
                                   AND C.POLICY   = CERT.POLICY
                                   AND C.CERTIF   = CERT.CERTIF
                                   JOIN USINSUV01.POLICY POL
                                   ON  POL.USERCOMP = C.USERCOMP 
                                   AND POL.COMPANY  = C.COMPANY  
                                   AND POL.CERTYPE  = C.CERTYPE
                                   AND POL.BRANCH   = C.BRANCH 
                                   AND POL.POLICY   = C.POLICY 
                                   WHERE C.CERTYPE  = '2'
                                   AND POL.STATUS_POL NOT IN ('2','3') 
                                   AND ((POL.POLITYPE = '1' -- INDIVIDUAL 
                                         AND POL.EXPIRDAT >= '2021-12-31' 
                                         AND (POL.NULLDATE IS NULL OR POL.NULLDATE > '2021-12-31'))
                                         OR 
                                       (POL.POLITYPE <> '1' -- COLECTIVAS 
                                         AND CERT.EXPIRDAT >= '2021-12-31' 
                                   AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2021-12-31')))
                                   AND POL.EFFECDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}') C WHERE C.FLAG = 1)T ) AS TMP '''
 
  L_DF_ABCOBAP_INSUNIX_LPV = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_ABCOBAP_INSUNIX_LPV).load()
  
  #-------------------------------------------------------------------------------------------------------------------------------#

  L_ABCOBAP_VTIME_LPG = f'''
                        (
                          SELECT 
                            'D' AS INDDETREC,
                            'ABCOBAP' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            TIOCFRM,
                            '' AS TIOCTO,
                            'PVG' AS KGIORIGM,
                            KABAPOL,
                            '' AS KABUNRIS,
                            KGCTPCBT,
                            TINICIO,
                            TTERMO,
                            '' AS TSITCOB,
                            '' AS KACSITCB,
                            '' AS VMTPRMSP,
                            VMTCOMR,
                            '' AS VMTBOMAT,
                            '' AS VTXBOMAT,
                            '' AS VMTBOCOM,
                            '' AS VTXBOCOM,
                            '' AS VMTDECOM,
                            '' AS VTXDECOM,
                            '' AS VMTDETEC,
                            '' AS VTXDETEC,
                            '' AS VMTAGRAV,
                            '' AS VTXAGRAV,
                            '' AS VMTPRMTR,
                            '' AS VMTPRLIQ,
                            VMTPRMBR,
                            VTXCOB,
                            VCAPITAL, 
                            '' AS VTXCAPIT,
                            '' AS KACTPIDX,
                            '' AS VTXINDX,
                            'LPG' AS DCOMPA,
                            '' AS DMARCA,
                            '' AS TDACECOB,
                            '' AS TDCANCOB,
                            '' AS TDCRICOB,
                            TDRENOVA,
                            '' AS TDVENTRA,
                            '' AS DHORAINI,
                            VMTPREMC,
                            '' AS VMIBOMAT,
                            '' AS VMIBOCOM,
                            '' AS VMIDECOM,
                            '' AS VMIDETEC,
                            '' AS VMIRPMSP,
                            '' AS VMIPRMBR,
                            '' AS VMICOMR,
                            '' AS VMIPRLIQ,
                            '' AS VMICMNQP,
                            '' AS VMIPRMTR,
                            '' AS VMIAGRAV,
                            '' AS KACTIPCB,
                            '' AS VMTCAPLI,
                            '' AS KACTRARE, --PENDIENTE
                            '' AS KACFMCAL,
                            '' AS DFACMULT,
                            VMTCAPIN,
                            VMTPREIN,
                            '' AS DINDESES,
                            '' AS DINDMOTO,
                            '' AS KACSALIN,
                            '' AS VMTSALMD,
                            '' AS VTXLMRES,
                            '' AS VTXEQUIP,
                            '' AS VTXPRIOR,
                            '' AS VTXCONTR,
                            '' AS VTXESPEC,
                            '' AS DCAPMORT,
                            VMTPRRES,
                            '' AS DIDADETAR,
                            '' AS DIDADLIMCOBA,
                            KACTPDUR,
                            '' AS KGCRAMO_SAP,
                            '' AS KACTCOMP,
                            '' AS KACINDTX,
                            '' AS KACCALIDA,
                            '' AS DNCABCALP,
                            '' AS DINDNIVEL,
                            DURCOB,
                            '' AS DURPAGCOB,
                            '' AS KACTPDURCB,
                            '' AS DINCOBINDX,
                            '' AS KACGRCBT,
                            '' AS KABTRTAB_2,
                            '' AS VTXAJTBUA,
                            '' AS VMTCAPREM
                            FROM(
                                  SELECT 
                                  COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE) AS VARCHAR),'') AS TIOCFRM,
                                  C."NBRANCH" ||'-'|| C."NPRODUCT" ||'-'|| C."NPOLICY" ||'-'|| C."NCERTIF" AS KABAPOL,
                                  COALESCE((SELECT CAST(GLC."NCOVERGEN" AS VARCHAR) || '-' || GLC."NCURRENCY" 
                                            FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_VTIMELPG*/
                                            (SELECT GC."NBRANCH",
                                            GC."NPRODUCT",
                                            GC."NMODULEC",
                                            GC."NCOVER",
                                            GC."DEFFECDATE",
                                            GC."DNULLDATE",
                                            GC."NCOVERGEN",
                                            GC."NCURRENCY"
                                            FROM USVTIMG01."GEN_COVER" GC
                                            UNION ALL
                                            SELECT LC."NBRANCH",
                                            LC."NPRODUCT",
                                            LC."NMODULEC",
                                            LC."NCOVER",
                                            LC."DEFFECDATE",
                                            LC."DNULLDATE",
                                            LC."NCOVERGEN",
                                            LC."NCURRENCY"
                                            FROM USVTIMG01."LIFE_COVER" LC)  GLC 
                                            WHERE GLC."NBRANCH" = C."NBRANCH" 
                                            AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                            AND   GLC."NMODULEC"  = C."NMODULEC"
                                            AND   GLC."NCOVER"    = C."NCOVER"
                                            AND   GLC."NCURRENCY" = C."NCURRENCY"
                                            AND   GLC."DEFFECDATE" <= C."DEFFECDATE" 
                                            AND (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > C."DEFFECDATE")), '') AS KGCTPCBT,
                                  COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE) AS VARCHAR),'') AS TINICIO,
                                  COALESCE(CAST(CAST(C."DNULLDATE" AS DATE) AS VARCHAR),'') AS TTERMO,
                                  COALESCE(C."NPREMIUM_O",0) AS VMTCOMR,
                                  COALESCE(C."NPREMIUM_O",0) AS VMTPRMBR,
                                  COALESCE(C."NRATECOVE",0) AS VTXCOB, --TASA APLICAR A LA COBERTURA
                                  COALESCE(CAST(C."NCAPITAL" AS VARCHAR), '0')  AS VCAPITAL,
                                  COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE )AS VARCHAR),'') AS TDRENOVA,
                                  COALESCE(CAST(((SELECT COALESCE(CO."NSHARE")  
                                                  FROM USVTIMG01."COINSURAN" CO
                                                  WHERE CO."SCERTYPE" = C."SCERTYPE" 
                                                  AND CO."NBRANCH"  = C."NBRANCH" 
                                                  AND CO."NPRODUCT" = C."NPRODUCT"
                                                  AND CO."NPOLICY" = C."NPOLICY"
                                                  AND CO."NCOMPANY" = 2
                                                  AND CO."DEFFECDATE"  <= C."DEFFECDATE"
                                                  AND (CO."DNULLDATE" IS NULL AND CO."DNULLDATE"  > C."DEFFECDATE")) * C."NPREMIUM") AS VARCHAR), '100') AS VMTPREMC,                                  
                                  COALESCE(C."NCAPITALI", 0)  AS VMTCAPIN,
                                  COALESCE(TRUNC(C."NPREMIUM_O", 2), 0) AS VMTPREIN,
                                  COALESCE((COALESCE ((SELECT (SUM(R."NSHARE"/100)) * C."NPREMIUM"  FROM USVTIMG01."REINSURAN" R 
                                        WHERE R."SCERTYPE" = C."SCERTYPE"  
                                        AND R."NBRANCH"  = C."NBRANCH"
                                        AND R."NPRODUCT" = C."NPRODUCT"
                                        AND R."NPOLICY"  = C."NPOLICY"
                                        AND R."NCERTIF"  = C."NCERTIF"
                                        AND R."NMODULEC" = C."NMODULEC"
                                        AND R."NCOVER" = C."NCOVER"
                                        AND R."DEFFECDATE" <= C."DEFFECDATE"
                                        AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > C."DEFFECDATE")
                                        AND R."NTYPE_REIN" <> 1),
                                        (SELECT (SUM(R."NSHARE"/100)) * C."NPREMIUM" FROM USVTIMG01."REINSURAN" R 
                                        WHERE R."SCERTYPE" = C."SCERTYPE"  
                                        AND R."NBRANCH"  = C."NBRANCH"
                                        AND R."NPRODUCT" = C."NPRODUCT"
                                        AND R."NPOLICY"  = C."NPOLICY"
                                        AND R."NCERTIF"  = 0
                                        AND R."NMODULEC" = C."NMODULEC"
                                        AND R."NCOVER" = C."NCOVER"
                                        AND R."DEFFECDATE" <= C."DEFFECDATE"
                                        AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > C."DEFFECDATE")
                                        AND R."NTYPE_REIN" <> 1))), 0) AS VMTPRRES,
                                  COALESCE(CAST(C."NTYPDURINS" AS VARCHAR),'0') AS KACTPDUR,
                                  COALESCE(CAST(C."NDURINSUR" AS VARCHAR),'0') AS DURCOB,    
                                CASE 
                                    WHEN POL."SPOLITYPE" = '1' --INDIVIDUAL
                                THEN 
                                      CASE WHEN (C."DEFFECDATE" <= POL."DSTARTDATE" AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > POL."DSTARTDATE")) THEN 1		                               
                                          ELSE 
                                    CASE	
                                          WHEN EXISTS ( SELECT	1
                                                        FROM	usvtimg01."COVER" COV1
                                                        WHERE 	COV1."SCERTYPE" = C."SCERTYPE"
                                                        AND     COV1."NBRANCH"    = C."NBRANCH"
                                                        AND 	  COV1."NPRODUCT"   = C."NPRODUCT"		                              	                  
                                                          AND     COV1."NMODULEC"   = C."NMODULEC"
                                                          AND     COV1."NPOLICY"    = C."NPOLICY"
                                                          AND     COV1."NCERTIF"    = C."NCERTIF"
                                                          AND     COV1."NCURRENCY"  = C."NCURRENCY"
                                                          AND     COV1."NCOVER"     = C."NCOVER" 
                                                        AND		  COV1."DEFFECDATE" <= POL."DSTARTDATE"
                                                        AND     (COV1."DNULLDATE" IS NULL OR COV1."DNULLDATE" > POL."DSTARTDATE")) THEN 0
                                              ELSE 
                                                  CASE	
                                              WHEN C."DNULLDATE" = (SELECT MAX(COV1."DNULLDATE")
                                                                    FROM	usvtimg01."COVER" COV1
                                                                    WHERE  COV1."SCERTYPE" = C."SCERTYPE"
                                                                    AND 	COV1."NBRANCH"  = C."NBRANCH"
                                                                    AND    COV1."NPRODUCT" = C."NPRODUCT"
                                                                        AND   COV1."NMODULEC"  = C."NMODULEC"
                                                                        AND   COV1."NPOLICY"   = C."NPOLICY"
                                                                        AND   COV1."NCERTIF"   = C."NCERTIF"
                                                                        AND   COV1."NCURRENCY" = C."NCURRENCY"
                                                                        AND   COV1."NCOVER"    = C."NCOVER" ) THEN 1
                                              ELSE 0
                                              END 
                                          END
                                      END  
                                                ELSE
                                                      CASE WHEN (C."DEFFECDATE" <= CERT."DSTARTDATE" AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > CERT."DSTARTDATE")) THEN 1		                               
                                            ELSE 
                                                  CASE	
                                            WHEN EXISTS ( SELECT	1
                                                        FROM	usvtimg01."COVER" COV1
                                                        WHERE 	COV1."SCERTYPE" = C."SCERTYPE"
                                                        AND     COV1."NBRANCH"    = C."NBRANCH"
                                                        AND 	  COV1."NPRODUCT"   = C."NPRODUCT"		                              	                  
                                                          AND     COV1."NMODULEC"   = C."NMODULEC"
                                                          AND     COV1."NPOLICY"    = C."NPOLICY"
                                                          AND     COV1."NCERTIF"    = C."NCERTIF"
                                                          AND     COV1."NCURRENCY"  = C."NCURRENCY"
                                                          AND     COV1."NCOVER"     = C."NCOVER" 
                                                        AND		  COV1."DEFFECDATE" <= cert."DSTARTDATE"
                                                        AND     (COV1."DNULLDATE" IS NULL OR COV1."DNULLDATE" > cert."DSTARTDATE")) THEN 0
                                            ELSE 
                                                CASE	
                                            WHEN C."DNULLDATE" = (SELECT MAX(COV1."DNULLDATE")
                                                                    FROM	usvtimg01."COVER" COV1
                                                                    WHERE  COV1."SCERTYPE" = C."SCERTYPE"
                                                                    AND 	COV1."NBRANCH"  = C."NBRANCH"
                                                                    AND    COV1."NPRODUCT" = C."NPRODUCT"
                                                                        AND   COV1."NMODULEC"  = C."NMODULEC"
                                                                        AND   COV1."NPOLICY"   = C."NPOLICY"
                                                                        AND   COV1."NCERTIF"   = C."NCERTIF"
                                                                        AND   COV1."NCURRENCY" = C."NCURRENCY"
                                                                        AND   COV1."NCOVER"    = C."NCOVER") THEN 1
                                            ELSE 0
                                            END 
                                        END
                                    END 
                                END FLAG
                                  FROM USVTIMG01."COVER" C  
                                            LEFT JOIN USVTIMG01."CERTIFICAT" CERT
                                            ON  C."SCERTYPE"      = CERT."SCERTYPE"  
                                      AND C."NBRANCH"   = CERT."NBRANCH"
                                      AND C."NPRODUCT"  = CERT."NPRODUCT"
                                      AND C."NPOLICY"   = CERT."NPOLICY"
                                      AND C."NCERTIF"   = CERT."NCERTIF"
                                      JOIN USVTIMG01."POLICY" POL
                                      ON  POL."SCERTYPE"  = C."SCERTYPE"
                                      AND POL."NBRANCH"   = C."NBRANCH" 
                                      AND POL."NPRODUCT"  = C."NPRODUCT"
                                      AND POL."NPOLICY"   = C."NPOLICY" 
                                      WHERE POL."SCERTYPE" = '2' 
                                      AND   POL."SSTATUS_POL" NOT IN ('2','3') 
                                      AND ((POL."SPOLITYPE" = '1' -- INDIVIDUAL 
                                      AND POL."DEXPIRDAT" >= '2021-12-31' 
                                      AND (POL."DNULLDATE" IS NULL OR POL."DNULLDATE" > '2021-12-31'))
                                      OR 
                                      (POL."SPOLITYPE" <> '1' -- COLECTIVAS 
                                      AND CERT."DEXPIRDAT" >= '2021-12-31' 
                                      AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '2021-12-31')))
                                      AND POL."DSTARTDATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'
                          ) COV_CERT
                          where FLAG = 1
                        ) AS TMP'''

  L_DF_ABCOBAP_VTIME_LPG = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_ABCOBAP_VTIME_LPG).load()

  L_ABCOBAP_VTIME_LPV = f'''
                        ( SELECT 
                         'D' AS INDDETREC,
                         'ABCOBAP' AS TABLAIFRS17,
                         '' AS PK,
                         '' AS DTPREG,
                         '' AS TIOCPROC,
                         TIOCFRM,
                         '' AS TIOCTO,
                         'PVV' AS KGIORIGM,
                         KABAPOL,
                         '' AS KABUNRIS,
                         KGCTPCBT,
                         TINICIO,
                         TTERMO,
                         '' AS TSITCOB,
                         '' AS KACSITCB,
                         '' AS VMTPRMSP,
                         VMTCOMR,
                         '' AS VMTBOMAT,
                         '' AS VTXBOMAT,
                         '' AS VMTBOCOM,
                         '' AS VTXBOCOM,
                         '' AS VMTDECOM,
                         '' AS VTXDECOM,
                         '' AS VMTDETEC,
                         '' AS VTXDETEC,
                         '' AS VMTAGRAV,
                         '' AS VTXAGRAV,
                         '' AS VMTPRMTR,
                         '' AS VMTPRLIQ,
                         VMTPRMBR,
                         VTXCOB, --TASA APLICAR A LA COBERTURA
                         VCAPITAL,-- IMPORTE DE CAPITAL ASEGURADO EN EL CERTIFICADO 
                         '' AS VTXCAPIT,
                         '' AS KACTPIDX,
                         '' AS VTXINDX,
                         'LPV' AS DCOMPA,
                         '' AS DMARCA,
                         '' AS TDACECOB,
                         '' AS TDCANCOB,
                         '' AS TDCRICOB,
                         TDRENOVA,
                         '' AS TDVENTRA,
                         '' AS DHORAINI,
                         VMTPREMC,
                         '' AS VMIBOMAT,
                         '' AS VMIBOCOM,
                         '' AS VMIDECOM,
                         '' AS VMIDETEC,
                         '' AS VMIRPMSP,
                         '' AS VMIPRMBR,
                         '' AS VMICOMR,
                         '' AS VMIPRLIQ,
                         '' AS VMICMNQP,
                         '' AS VMIPRMTR,
                         '' AS VMIAGRAV,
                         '' AS KACTIPCB,
                         '' AS VMTCAPLI,
                         '' AS KACTRARE,
                         '' AS KACFMCAL,
                         '' AS DFACMULT,
                         VMTCAPIN, 
                         VMTPREIN,
                         '' AS DINDESES,
                         '' AS DINDMOTO,
                         '' AS KACSALIN,
                         '' AS VMTSALMD,
                         '' AS VTXLMRES,
                         '' AS VTXEQUIP,
                         '' AS VTXPRIOR,
                         '' AS VTXCONTR,
                         '' AS VTXESPEC,
                         '' AS DCAPMORT,
                         VMTPRRES,
                         '' AS DIDADETAR,
                         '' AS DIDADLIMCOBA,
                         KACTPDUR,
                         '' AS KGCRAMO_SAP,
                         '' AS KACTCOMP,
                         '' AS KACINDTX,
                         '' AS KACCALIDA,
                         '' AS DNCABCALP,
                         '' AS DINDNIVEL,
                         DURCOB,
                         '' AS DURPAGCOB,
                         '' AS KACTPDURCB,
                         '' AS DINCOBINDX,
                         '' AS KACGRCBT,
                         '' AS KABTRTAB_2,
                         '' AS VTXAJTBUA,
                         '' AS VMTCAPREM
                         FROM (SELECT                       
                               COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE ) AS VARCHAR),'') AS TIOCFRM,
                               C."NBRANCH" ||'-'|| C."NPRODUCT" ||'-'|| C."NPOLICY" ||'-'|| C."NCERTIF" AS KABAPOL,
                               COALESCE((SELECT CAST(LC."NCOVERGEN" AS VARCHAR)  FROM USVTIMV01."LIFE_COVER" LC 
                                 WHERE LC."NBRANCH" = C."NBRANCH" 
                                 AND LC."NPRODUCT" = C."NPRODUCT"
                                 AND LC."NMODULEC" = C."NMODULEC"
                                 AND LC."NCOVER" = C."NCOVER"
                                 AND LC."DEFFECDATE" <= C."DEFFECDATE" 
                                 AND (LC."DNULLDATE" IS NULL OR LC."DNULLDATE" > C."DEFFECDATE")
                               ), '0') AS KGCTPCBT,
                               COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE )AS VARCHAR),'') AS TINICIO,
                               COALESCE(CAST(CAST(C."DNULLDATE" AS DATE )AS VARCHAR),'') AS TTERMO,
                               COALESCE(C."NPREMIUM_O", 0) AS VMTCOMR,
                               COALESCE(C."NPREMIUM_O", 0) AS VMTPRMBR,
                               COALESCE(C."NRATECOVE", 0) AS VTXCOB, --TASA APLICAR A LA COBERTURA
                               COALESCE(CAST(C."NCAPITAL" AS VARCHAR), '0') AS VCAPITAL,
                               COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE )AS VARCHAR),'') AS TDRENOVA,
                               COALESCE((CAST(((SELECT COALESCE (CO."NSHARE", 0)  FROM USVTIMV01."COINSURAN" CO
                                 WHERE CO."SCERTYPE" = C."SCERTYPE" 
                                 AND CO."NBRANCH" = C."NBRANCH" 
                                 AND CO."NPOLICY" = C."NPOLICY"
                                 AND CO."NCOMPANY" = 2
                                 AND CO."DEFFECDATE"  <= C."DEFFECDATE"
                                 AND (CO."DNULLDATE" IS NULL AND CO."DNULLDATE"  > C."DEFFECDATE")
                               ) * C."NPREMIUM") AS VARCHAR)), '100') AS VMTPREMC,
                               COALESCE(C."NCAPITALI", 0)  AS VMTCAPIN, -- IMPORTE DE CAPITAL DE LA COBERTURA A LA FECHA DE EMISION DE LA POLIZA,
                               COALESCE(TRUNC(C."NPREMIUM_O", 2), 0) AS VMTPREIN,
                               COALESCE((COALESCE ((SELECT (SUM(R."NSHARE"/100)) * C."NPREMIUM"  FROM USVTIMV01."REINSURAN" R 
                               WHERE R."SCERTYPE" = C."SCERTYPE"  
                               AND R."NBRANCH"  = C."NBRANCH"
                               AND R."NPRODUCT" = C."NPRODUCT"
                               AND R."NPOLICY"  = C."NPOLICY"
                               AND R."NCERTIF"  = C."NCERTIF"
                               AND R."NMODULEC" = C."NMODULEC"
                               AND R."NCOVER" = C."NCOVER"
                               AND R."DEFFECDATE" <= C."DEFFECDATE"
                               AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > C."DEFFECDATE")
                               AND R."NTYPE_REIN" <> 1),
                               (SELECT (SUM(R."NSHARE"/100)) * C."NPREMIUM" FROM USVTIMV01."REINSURAN" R 
                               WHERE R."SCERTYPE" = C."SCERTYPE"  
                               AND R."NBRANCH"  = C."NBRANCH"
                               AND R."NPRODUCT" = C."NPRODUCT"
                               AND R."NPOLICY"  = C."NPOLICY"
                               AND R."NCERTIF"  = 0
                               AND R."NMODULEC" = C."NMODULEC"
                               AND R."NCOVER" = C."NCOVER"
                               AND R."DEFFECDATE" <= C."DEFFECDATE"
                               AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > C."DEFFECDATE")
                               AND R."NTYPE_REIN" <> 1))), 0) AS VMTPRRES,
                               COALESCE(CAST(C."NTYPDURINS" AS VARCHAR),'0') AS KACTPDUR,
                               COALESCE(CAST(C."NDURINSUR" AS VARCHAR),'0') AS DURCOB
 					                     FROM USVTIMV01."COVER" C  
 					                     LEFT JOIN USVTIMV01."CERTIFICAT" CERT
 					                     ON  C."SCERTYPE"  = CERT."SCERTYPE"  
                               AND C."NBRANCH"   = CERT."NBRANCH"
                               AND C."NPRODUCT"  = CERT."NPRODUCT"
                               AND C."NPOLICY"   = CERT."NPOLICY"
                               AND C."NCERTIF"   = CERT."NCERTIF"
                               JOIN USVTIMV01."POLICY" POL
                               ON  POL."SCERTYPE"  = C."SCERTYPE"
                               AND POL."NBRANCH"   = C."NBRANCH" 
                               AND POL."NPRODUCT"  = C."NPRODUCT"
                               AND POL."NPOLICY"   = C."NPOLICY" 
                               WHERE POL."SCERTYPE" = '2' 
                               AND POL."SSTATUS_POL" NOT IN ('2','3') 
                               AND ((POL."SPOLITYPE" = '1' -- INDIVIDUAL 
                               AND POL."DEXPIRDAT" >= '2021-12-31' 
                               AND (POL."DNULLDATE" IS NULL OR POL."DNULLDATE" > '2021-12-31'))
                               OR 
                               (POL."SPOLITYPE" <> '1' -- COLECTIVAS 
                               AND CERT."DEXPIRDAT" >= '2021-12-31' 
                               AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '2021-12-31')))
                               AND POL."DSTARTDATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}')T ) AS TMP
                          '''

  L_DF_ABCOBAP_VTIME_LPV = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_ABCOBAP_VTIME_LPV).load()

  #-------------------------------------------------------------------------------------------------------------------------------#

  L_ABCOBAP_INSIS = f'''
                    (
                      SELECT 
                      'D' AS INDDETREC,
                      'ABCOBAP' AS TABLAIFRS17,
                      '' AS PK,
                      '' AS DTPREG,  --NO
                      '' AS TIOCPROC,--NO
                      CAST(CAST(GRC."INSR_BEGIN" AS DATE)AS VARCHAR) AS TIOCFRM, --BEGIN OF INSURING.
                      '' AS TIOCTO,
                      'PNV' AS KGIORIGM,
                        (
                      	SELECT P."POLICY_NAME" FROM USINSIV01."POLICY" P
                      	WHERE P."POLICY_ID" = GRC."POLICY_ID"
                      ) AS KABAPOL,
                      GRC."INSURED_OBJ_ID" ||'-'|| GRC."ANNEX_ID"  AS KABUNRIS,
                      GRC."COVER_TYPE"  AS KGCTPCBT,
                      CAST(CAST(GRC."INSR_BEGIN" AS DATE) AS VARCHAR) AS TINICIO,
                      CAST(CAST(GRC."INSR_END" AS DATE)AS VARCHAR)  AS TTERMO,
                      '' AS TSITCOB,
                      '' AS KACSITCB,
                      '' AS VMTPRMSP,
                      TRUNC(GRC."PREMIUM", 2) AS VMTCOMR,
                      '' AS VMTBOMAT,
                      '' AS VTXBOMAT,
                      '' AS VMTBOCOM,
                      '' AS VTXBOCOM,
                      '' AS VMTDECOM,
                      '' AS VTXDECOM,
                      '' AS VMTDETEC,
                      '' AS VTXDETEC,
                      '' AS VMTAGRAV,
                      '' AS VTXAGRAV,
                      '' AS VMTPRMTR,
                      '' AS VMTPRLIQ,
                      TRUNC(GRC."PREMIUM", 2) AS VMTPRMBR,
                      TRUNC(GRC."TARIFF_PERCENT", 9) AS VTXCOB,
                      CAST(TRUNC(GRC."INSURED_VALUE", 2) AS VARCHAR) AS VCAPITAL,
                      '' AS VTXCAPIT, --EN BLANCO
                      '' AS KACTPIDX, --NO
                      '' AS VTXINDX,  --EN BLANCO
                      'LPV' AS DCOMPA,
                      '' AS DMARCA,   --NO  
                      '' AS TDACECOB, --NO
                      '' AS TDCANCOB, --NO
                      '' AS TDCRICOB, --NO
                      CAST(CAST(GRC."INSR_BEGIN" AS DATE)AS VARCHAR) AS TDRENOVA,
                      '' AS TDVENTRA, --NO
                      '' AS DHORAINI, --NO
                      '' AS VMTPREMC, --PENDIENTE
                      '' AS VMIBOMAT, --NO
                      '' AS VMIBOCOM, --NO
                      '' AS VMIDECOM, --NO
                      '' AS VMIDETEC, --NO
                      '' AS VMIRPMSP, --NO
                      '' AS VMIPRMBR, --NO
                      '' AS VMICOMR,  --NO
                      '' AS VMIPRLIQ, --NO
                      '' AS VMICMNQP, --NO
                      '' AS VMIPRMTR, --NO
                      '' AS VMIAGRAV, --NO
                      '' AS KACTIPCB, --EN BLANCO
                      '' AS VMTCAPLI, --EN BLANCO
                      '' AS KACTRARE, --EN BLANCO
                      '' AS KACFMCAL, --EN BLANCO
                      '' AS DFACMULT, --NO
                      TRUNC(GRC."INSURED_VALUE", 0)  AS VMTCAPIN,
                      TRUNC(GRC."ANNUAL_PREMIUM", 0) AS VMTPREIN,
                      '' AS DINDESES,    --NO
                      '' AS DINDMOTO,    --NO
                      '' AS KACSALIN,    --NO
                      '' AS VMTSALMD,    --NO
                      '' AS VTXLMRES,    --EN BLANCO
                      '' AS VTXEQUIP,    --NO
                      '' AS VTXPRIOR,    --NO
                      '' AS VTXCONTR,    --NO
                      '' AS VTXESPEC,    --NO
                      '' AS DCAPMORT,    --NO
                      0 AS VMTPRRES,    --PENDIENTE
                      '' AS DIDADETAR,   --EN BLANCO
                      '' AS DIDADLIMCOBA,--EN BLANCO
                      '' AS KACTPDUR,    --EN BLANCO
                      '' AS KGCRAMO_SAP, --NO
                      '' AS KACTCOMP,    --NO
                      '' AS KACINDTX,    --EN BLANCO
                      '' AS KACCALIDA,   --EN BLANCO
                      '' AS DNCABCALP,   --EN BLANCO
                      '' AS DINDNIVEL,   --NO
                      '' AS DURCOB,      --EN BLANCO
                      '' AS DURPAGCOB,   --EN BLANCO
                      '' AS KACTPDURCB,  --NO
                      '' AS DINCOBINDX,  --NO
                      '' AS KACGRCBT,    --NO
                      '' AS KABTRTAB_2,  --NO
                      '' AS VTXAJTBUA,   --NO
                      '' AS VMTCAPREM    --NO
                      FROM USINSIV01."GEN_RISK_COVERED" GRC
                      JOIN USINSIV01."POLICY" POL ON POL."POLICY_ID" = GRC."POLICY_ID" AND POL."INSR_TYPE" = GRC."INSR_TYPE"
                      WHERE POL."INSR_END" >= '2021-12-31'
                      AND POL."REGISTRATION_DATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' LIMIT 100
                    ) AS TMP
                    '''
    
    #EJECUTAR CONSULTA
  L_DF_ABCOBAP_INSIS = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_ABCOBAP_INSIS).load()

  #PERFORM THE UNION OPERATION
  L_DF_ABCOBAP = L_DF_ABCOBAP_INSUNIX_LPG.union(L_DF_ABCOBAP_INSUNIX_LPV).union(L_DF_ABCOBAP_VTIME_LPG).union(L_DF_ABCOBAP_VTIME_LPV).union(L_DF_ABCOBAP_INSIS)

  L_DF_ABCOBAP = L_DF_ABCOBAP.withColumn("VMTCOMR", col("VMTCOMR").cast(DecimalType(12, 2))).withColumn("VMTPRMBR", col("VMTPRMBR").cast(DecimalType(12, 2))).withColumn("VTXCOB", format_number("VTXCOB",9)).withColumn("VCAPITAL", col("VCAPITAL").cast(DecimalType(14, 2))).withColumn("VTXCAPIT", col("VTXCAPIT").cast(DecimalType(9, 5))).withColumn("VTXINDX", col("VTXINDX").cast(DecimalType(7, 4))).withColumn("VMTPREMC", col("VMTPREMC").cast(DecimalType(12, 2))).withColumn("VMTCAPLI", col("VMTCAPLI").cast(DecimalType(14, 2))).withColumn("VMTCAPIN", col("VMTCAPIN").cast(DecimalType(14, 2))).withColumn("VMTPREIN", col("VMTPREIN").cast(DecimalType(14, 2))).withColumn("VTXLMRES", col("VTXLMRES").cast(DecimalType(7, 4))).withColumn("VMTPRRES", col("VMTPRRES").cast(DecimalType(12, 2))).withColumn("VTXAJTBUA", col("VTXAJTBUA").cast(DecimalType(9, 4))).withColumn("VMTCAPREM", col("VMTCAPREM").cast(DecimalType(12, 2)))
  
  return L_DF_ABCOBAP