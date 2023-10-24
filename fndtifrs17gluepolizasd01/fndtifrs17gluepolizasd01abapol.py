#Declararación de variables
L_FECHA_INICIO = '2020-01-01'
L_FECHA_FIN    = '2020-01-03'

def getData(glueContext, connection):       

   #Declara consulta VTIME
   L_POLIZAS_VTIME = f'''
                  ((SELECT
                  'D' AS INDDETREC, 
                  'ABAPOL' AS TABLAIFRS17,
                  '' AS PK,              
                  '' AS DTPREG,			    --NO
                  '' AS TIOCPROC,             --NO
                  COALESCE(cast(CAST(P."DDATE_ORIGI" AS DATE) as VARCHAR), '') AS TIOCFRM,
                  '' AS TIOCTO,
                  'PVG' AS KGIORIGM,
                  'LPG' AS KACCOMPA,
                  CAST(P."NBRANCH" AS VARCHAR) AS KGCRAMO,      
                  CAST(P."NPRODUCT" AS VARCHAR) AS KABPRODT,   
                  CASE COALESCE(P."SPOLITYPE", '')
                  WHEN '2' THEN CASE WHEN CERT."NCERTIF" <> 0 THEN P."NBRANCH" || '-' || P."NPRODUCT" || '-' || P."NPOLICY" || '-' || '0'
                                ELSE ''
                                END
                  ELSE '' 
                  END AS KABAPOL,
                  CAST(P."NBRANCH" AS VARCHAR) || '-' || CAST(P."NPRODUCT" AS VARCHAR) || '-' || P."NPOLICY" AS DNUMAPO,     
                  CAST(CERT."NCERTIF" AS VARCHAR) AS DNMCERT,  
                  '' AS DTERMO, --EN BLANCO
                  COALESCE(P."SCLIENT", '') AS KEBENTID_TO,
                  COALESCE(cast(CAST(P."DDATE_ORIGI" AS DATE) as VARCHAR), '') AS TCRIAPO,
                  COALESCE(cast(CAST(P."DISSUEDAT"   AS DATE) as VARCHAR), '') AS TEMISSAO,
                  COALESCE(cast(CAST(P."DDATE_ORIGI" AS DATE) as VARCHAR), '') AS TINICIO,
                  '' AS DHORAINI,
                  COALESCE(CAST(CAST(P."DEXPIRDAT" AS DATE)  AS VARCHAR), '') AS TTERMO,
                  COALESCE(CAST(CAST(P."DSTARTDATE" AS DATE) AS VARCHAR), '') AS TINIANU,
                  COALESCE(CAST(CAST(P."DEXPIRDAT" AS DATE) AS  VARCHAR), '') AS TVENANU,
                  COALESCE(CAST(CAST(P."DNULLDATE" AS DATE) AS  VARCHAR), '') AS TANSUSP,
                  '' AS TESTADO,              --EN BLANCO
                  COALESCE(P."SSTATUS_POL", '') AS KACESTAP,
                  '' AS KACMOEST,
                  COALESCE(cast(cast(P."DSTARTDATE" as DATE) as VARCHAR), '') AS TEFEACTA,             --ACLARAR
                  '' AS DULTACTA,
                  '' AS KACCNEMI,
                  '' AS KACARGES,
                  '' AS KACAGENC,
                  '' AS KACPROTO,
                  COALESCE(P."SPOLITYPE", '') AS KACTIPAP, 
                  '' AS DFROTA,
                  '' AS KACTPDUR,             --ACLARAR
                  COALESCE(P."SRENEWAL", '') AS KACMODRE,
                  '' AS KACMTNRE, --NO
                  '' AS KACTPCOB, --NO
                  CAST(P."NPAYFREQ" AS VARCHAR) AS KACTPFRC,
                  '' AS KACTPCSG,
                  COALESCE(P."SCOLREINT", '') AS KACINDRE,
                  '' AS KACCDGER,
                  (    
                       SELECT CAST(CP."NCURRENCY" AS VARCHAR) FROM USVTIMG01."CURREN_POL" CP 
                       WHERE  CP."SCERTYPE" = P."SCERTYPE"
                       AND    CP."NBRANCH"  = P."NBRANCH"
                       AND    CP."NPRODUCT" = P."NPRODUCT" 
                       AND    CP."NPOLICY"  = P."NPOLICY"
                       AND    CP."NCERTIF"  = CERT."NCERTIF"
                       AND    CP."DEFFECDATE" <= P."DSTARTDATE" 
                       AND ( CP."DNULLDATE" IS NULL OR CP."DNULLDATE" > P."DSTARTDATE")
                       LIMIT 1
                  )
                  AS KACMOEDA,
                  (
                     SELECT COALESCE(CAST(E."NEXCHANGE" AS VARCHAR), '')
                     FROM USVTIMG01."EXCHANGE" E 
                     WHERE E."NCURRENCY" = ( SELECT CP."NCURRENCY" 
                                             FROM USVTIMG01."CURREN_POL" CP
                                             WHERE "SCERTYPE" = '2'
                                             AND "NBRANCH"    = P."NBRANCH"
                                             AND "NPRODUCT"   = P."NPRODUCT"
                                             AND "NPOLICY"    = P."NPOLICY"
                                             AND "NCERTIF"    = 0
                                             AND CP."DEFFECDATE" <= P."DSTARTDATE"
                                             AND (CP."DNULLDATE" IS NULL OR CP."DNULLDATE" > P."DSTARTDATE"))
                      AND E."DEFFECDATE" <= P."DSTARTDATE"
                      AND (E."DNULLDATE" IS NULL OR E."DNULLDATE" > P."DSTARTDATE")
                  ) AS VCAMBIO,
                  '' AS KACREGCB,  --NO
                  '' AS KCBMED_DRA,--NO
                  '' AS KCBMED_CB, --NO
                  '' AS VTXCOMCB,  --ACLARAR 
                  '' AS VMTCOMCB,  --ACLARAR
                  '' AS KCBMED_PD,
                  COALESCE(  --POR CERTIFICADO
                      	   (SELECT COALESCE(CAST("NPERCENT" AS VARCHAR)) FROM  USVTIMG01."COMMISSION" CO 
                              WHERE  CO."SCERTYPE" = '2'
                              AND    CO."NBRANCH"  = P."NBRANCH"
                              AND	   CO."NPRODUCT" = P."NPRODUCT" 	
                              AND    CO."NPOLICY"  = P."NPOLICY"  
                              AND    CO."NCERTIF"  = CERT."NCERTIF" 
                              AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                              AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                              AND    CO."NINTERTYP" <> 1
                              LIMIT 1),
                              --POR POLIZA
                              (SELECT COALESCE(CAST("NPERCENT" AS VARCHAR)) FROM  USVTIMG01."COMMISSION" CO 
                               WHERE  CO."SCERTYPE" = '2'
                               AND    CO."NBRANCH"  = P."NBRANCH"
                               AND	CO."NPRODUCT" = P."NPRODUCT" 	
                               AND    CO."NPOLICY"  = P."NPOLICY"  
                               AND    CO."NCERTIF"  = 0
                               AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                               AND 	(CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                               AND    CO."NINTERTYP" <> 1
                               LIMIT 1)  
                  ) AS VTXCOMMD,
                  COALESCE(  --POR CERTIFICADO
                      	   (SELECT COALESCE(CAST(ROUND("NAMOUNT", 2) AS VARCHAR), '') FROM  USVTIMG01."COMMISSION" CO 
                              WHERE  CO."SCERTYPE" = '2'
                              AND    CO."NBRANCH"  = P."NBRANCH"
                              AND	   CO."NPRODUCT" = P."NPRODUCT" 	
                              AND    CO."NPOLICY"  = P."NPOLICY"  
                              AND    CO."NCERTIF"  = CERT."NCERTIF" 
                              AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                              AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                              AND    CO."NINTERTYP" <> 1
                              LIMIT 1),
                              --POR POLIZA
                              (SELECT COALESCE(CAST(ROUND("NAMOUNT", 2) AS VARCHAR), '') FROM  USVTIMG01."COMMISSION" CO 
                               WHERE  CO."SCERTYPE" = '2'
                               AND    CO."NBRANCH"  = P."NBRANCH"
                               AND	CO."NPRODUCT" = P."NPRODUCT" 	
                               AND    CO."NPOLICY"  = P."NPOLICY"  
                               AND    CO."NCERTIF"  = 0
                               AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                               AND 	(CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                               AND    CO."NINTERTYP" <> 1
                               LIMIT 1) 
                  ) AS VMTCOMMD,
                  '' AS KCBMED_P2, --NO 
                  '' AS VTXCOMME,  --NO
                  '' AS VMTCOMME,  --NO
                  (
                    SELECT COALESCE(CAST(ROUND(SUM(COV."NCAPITAL"),2) AS VARCHAR), '') FROM USVTIMG01."COVER" COV
                    WHERE COV."SCERTYPE" = P."SCERTYPE"
                    AND   COV."NBRANCH"  = P."NBRANCH"
                    AND   COV."NPRODUCT" = P."NPRODUCT"
                    AND   COV."NPOLICY"  = P."NPOLICY" 
                    AND   COV."NCERTIF"  = CERT."NCERTIF"
                    AND   COV."DEFFECDATE" <= P."DSTARTDATE"
                    AND ( COV."DNULLDATE" IS NULL OR COV."DNULLDATE" > P."DSTARTDATE") 
                  )  AS VCAPITAL,
                  '' AS VMTPRMSP, --NO
                  COALESCE(CAST(ROUND(P."NPREMIUM", 2) AS VARCHAR), '') AS VMTCOMR,
                  (
                     SELECT CAST((COALESCE(C."NSHARE", 0) * COALESCE(P."NPREMIUM", 0)) AS VARCHAR)
                     FROM USVTIMG01."COINSURAN" C
                     WHERE C."SCERTYPE" = '2'
                     AND   C."NBRANCH"  = P."NBRANCH" 
                     AND   C."NPRODUCT" = P."NPRODUCT"
                     AND   C."NPOLICY"  = P."NPOLICY"
                     AND   C."NCOMPANY" =  1 --CODIGO DE LA CIA LIDER POSITIVA
                     AND   C."DEFFECDATE" <= P."DSTARTDATE"
                     AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")
                  ) AS VMTCMNQP,
                  '' AS VMTBOMAT, --NO
                  '' AS VTXBOMAT, --NO
                  '' AS VMTBOCOM, --NO
                  '' AS VTXBOCOM, --NO
                  '' AS VMTDECOM, --NO
                  '' AS VTXDECOM, --NO
                  '' AS VMTDETEC, --NO
                  '' AS VTXDETEC, --NO
                  '' AS VMTAGRAV, --NO
                  '' AS VTXAGRAV, --NO
                  '' AS VMTCSAP,  --NO
                  '' AS VMTPRMIN, --NO
                  '' AS VMTPRMTR, --NO
                  '' AS VMTPRLIQ, --NO
                  COALESCE(CAST (ROUND(P."NPREMIUM", 2) AS VARCHAR), '') AS VMTPRMBR,
                  (
                     SELECT COALESCE(CAST(C."NSHARE" AS VARCHAR), '')
                     FROM USVTIMG01."COINSURAN" C
                     WHERE C."SCERTYPE" = '2'
                     AND   C."NBRANCH"  = P."NBRANCH" 
                     AND   C."NPRODUCT" = P."NPRODUCT"
                     AND   C."NPOLICY"  = P."NPOLICY"
                     AND   C."NCOMPANY" =  1 --CODIGO DE LA CIA LIDER POSITIVA
                     AND   C."DEFFECDATE" <= P."DSTARTDATE"
                     AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")
                  ) AS VTXCOSSG,
                  '' AS VTXRETEN, --CERRADO PERO NO ESPECIFICA VALOR
                  CAST(((SELECT COALESCE(R."NSHARE", 0) /100
                      FROM USVTIMG01."REINSURAN" R
                      WHERE R."SCERTYPE" =  '2'
                      AND   R."NBRANCH"  = P."NBRANCH"
                      AND   R."NPRODUCT" = P."NPRODUCT" 
                      AND   R."NPOLICY"  = P."NPOLICY" 
                      AND   R."NCERTIF"  = CERT."NCERTIF"
                      AND   R."DEFFECDATE" <= P."DSTARTDATE" 
                      AND  (R."DNULLDATE" IS NULL OR R."DNULLDATE" > P."DSTARTDATE")
                      AND   R."NCOMPANY" = 1
                      AND  R."NTYPE_REIN" = 1
                      AND  R."NCOVER" = 1)*
                     (SELECT COALESCE(SUM(C."NCAPITAL"), 0) FROM USVTIMG01."COVER" C
                      WHERE C."SCERTYPE" = '2'
                      AND   C."NBRANCH"  = P."NBRANCH"
                      AND   C."NPRODUCT" = P."NPRODUCT"
                      AND   C."NPOLICY"  = P."NPOLICY"
                      AND   C."NCERTIF"  = CERT."NCERTIF"
                      AND   C."DEFFECDATE" <= P."DSTARTDATE"
                      AND ( C."DNULLDATE" IS NULL OR P."DNULLDATE" > P."DSTARTDATE"))
                  ) AS VARCHAR) AS VMTCAPRE,
                  '' AS DNUMVIAS,--NO
                  '' AS DQTCRED,--NO
                  '' AS DNIB,--NO
                  '' AS DLOCREF,--NO
                  '' AS KEBMORAD,--NO
                  '' AS DLOCCOBR,--NO
                  '' AS TULTMALT,--NO
                  '' AS DUSRUPD,--NO
                  '' AS VMIPRMTR,--NO
                  '' AS VMIPRLIQ,--NO
                  '' AS VMIPRMBR,--NO
                  '' AS VMIRPMSP,--NO
                  '' AS VMIAGRAV,--NO
                  '' AS VMIDETEC,--NO
                  '' AS VMIDECOM,--NO
                  '' AS VMIBOCOM,--NO
                  '' AS VMIBOMAT,--NO
                  '' AS VMICOMR,--NO
                  '' AS VMICMNQP,--NO
                  '' AS VMICOMME,--NO
                  '' AS VMICOMMD,--NO
                  '' AS VMICOMCB,--NO
                  'LPG' AS DCOMPA,
                  '' AS DMARCA,    --NO
                  '' AS KACRGMCB,  --NO
                  '' AS KABAPOL_MP,--NO
                  '' AS TMIGPARA,  --NO
                  '' AS KABAPOL_MD,--NO
                  '' AS TMIGDE,    --NO
                  '' AS KACPGPRE,  
                  '' AS TDPGPRE,   
                  '' AS TINIPGPR,  
                  '' AS TFIMPGPR,  
                  '' AS KABAPOL_ESQ, --NO
                  '' AS KABAPOL_EFT, --NO
                  '' AS DSUBSCR,     --NO
                  '' AS DNUAPLI,     --NO 
                  COALESCE(P."SNONULL", '') AS DINDINIB,
                  '' AS DLOCRECB, --NO 
                  '' AS KACCLCLI, --NO
                  COALESCE(CAST (P."NNULLCODE" AS VARCHAR), '') AS KACMTALT,
                  '' AS KACTPTRA, --ACLARAR
                  '' AS TEMICANC, --NO
                  '' AS DENTIDSO, --NO
                  '' AS DARQUIVO, --NO
                  '' AS TARQUIVO, --NO
                  COALESCE(P."SPOLITYPE", '') AS KACTPSUB,
                  '' AS KACPARES, --ACLARAR
                  '' AS KGCRAMO_SAP, --NO
                  '' AS KACTPCRED,   --NO
                  '' AS DIBAN,       --NO
                  '' AS DSWIFT,      --NO
                  '' AS KARMODALID,  --NO
                  '' AS DUSREMIS,    --NO
                  '' AS KCBMED_VENDA,--NO
                  '' AS DUSRACEIT,   --NO
                  '' AS DCANALOPE,   --NO
                  '' AS KAICANEM,    --NO
                  '' AS DIDCANEM,    --NO
                  '' AS KAICANVD,    --NO
                  '' AS DIDCANVD,    --NO
                  '' AS DNMMULTI,    --NO
                  '' AS DOBSERV,     --NO
                  (
                  	SELECT CAST(COUNT(*) AS VARCHAR) FROM USVTIMG01."ROLES" R 
                  	WHERE R."SCERTYPE" = P."SCERTYPE"
                  	AND   R."NBRANCH"  = P."NBRANCH" 
                  	AND   R."NPOLICY"  = P."NPOLICY"
                  	AND   R."NCERTIF"  = CERT."NCERTIF"
                  ) AS DQTDPART,
                  '' AS TINIPRXANU,
                  '' AS KACTPREAP,   
                  '' AS DENTCONGE,       --NO
                  '' AS KCBMED_PARCE,    --NO
                  '' AS DCODPARC,        --NO
                  '' AS DMODPARC,        --NO
                  COALESCE(P."SBUSSITYP", '') AS DTIPSEG,
                  '' AS KACTPNEG,        --NO
                  '' AS DURPAGAPO,
                  '' AS DNMINTERP,       --NO
                  '' AS DNUMADES,        --NO
                  '' AS KACTPPRD,        --ACLARAR
                  '' AS KACSBTPRD,       --ACLARAR
                  '' AS KABPRODT_REL,    --NO
                  '' AS KACTPPARES,      --NO      
                  '' AS KACTIPIFAP,      --NO
                  '' AS KACTPALT_IFRS17, --NO
                  '' AS TEFEALTE,        --NO
                  '' AS TINITARLTA,      --NO
                  '' AS TFIMTARLTA,      --NO
                  '' AS DTERMO_IFRS17,   --NO
                  '' AS TEMISREN         --NO
                  FROM USVTIMG01."POLICY" P 
                  LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                  ON CERT."SCERTYPE" = P."SCERTYPE" 
                  AND CERT."NBRANCH" = P."NBRANCH" 
                  AND CERT."NPRODUCT" = P."NPRODUCT" 
                  AND CERT."NPOLICY" = P."NPOLICY"
                  WHERE P."DCOMPDATE" BETWEEN '{L_FECHA_INICIO}' AND '{L_FECHA_FIN}')
      
                  UNION ALL
      
                  (SELECT
                  'D' AS INDDETREC, 
                  'ABAPOL' AS TABLAIFRS17,
                  '' AS PK,
                  '' AS DTPREG,  --NO
                  '' AS TIOCPROC,--NO
                  COALESCE(CAST(CAST(P."DDATE_ORIGI" AS DATE) AS VARCHAR), '') AS TIOCFRM,
                  '' AS TIOCTO, --NO
                  'PVV' AS KGIORIGM,  --NO
                  'LPV' AS KACCOMPA,
                  CAST(P."NBRANCH" AS VARCHAR) KGCRAMO,
                  CAST(P."NPRODUCT" AS VARCHAR) KABPRODT,
                  CASE COALESCE(P."SPOLITYPE", '')
                  WHEN '2' THEN CASE WHEN CERT."NCERTIF" <> 0 THEN P."NBRANCH" || '-' || P."NPRODUCT" || '-' || P."NPOLICY" || '-' || '0'
                                ELSE ''
                                END
                  ELSE '' 
                  END AS KABAPOL,
                  P."NBRANCH" || '-' || P."NPRODUCT" || '-' || P."NPOLICY" AS DNUMAPO,
                  CAST(CERT."NCERTIF" AS VARCHAR) AS DNMCERT,
                  '' AS DTERMO,
                  COALESCE(P."SCLIENT", '') AS KEBENTID_TO,
                  COALESCE(CAST(CAST(P."DDATE_ORIGI" AS DATE) AS VARCHAR), '') AS TCRIAPO,
                  COALESCE(CAST(CAST(P."DISSUEDAT" AS DATE)   AS VARCHAR), '') AS TEMISSAO,
                  COALESCE(CAST(CAST(P."DDATE_ORIGI" AS DATE) AS VARCHAR), '') AS TINICIO,
                  '' AS DHORAINI,
                  COALESCE(CAST(CAST(P."DEXPIRDAT" AS DATE) AS VARCHAR) , '')AS TTERMO,
                  COALESCE(CAST(CAST(P."DSTARTDATE" AS DATE) AS VARCHAR), '') AS TINIANU,
                  COALESCE(CAST(CAST(P."DEXPIRDAT" AS DATE) AS VARCHAR) , '')AS TVENANU,
                  COALESCE(CAST(CAST(P."DNULLDATE" AS DATE) AS VARCHAR) , '')AS TANSUSP,
                  '' AS TESTADO,
                  COALESCE(P."SSTATUS_POL", '') AS KACESTAP,
                  '' AS KACMOEST, --NO
                  COALESCE(CAST(CAST(P."DSTARTDATE" AS DATE) AS VARCHAR), '')  AS TEFEACTA,
                  '' AS DULTACTA, --NO
                  '' AS KACCNEMI, --NO
                  '' AS KACARGES, --NO
                  '' AS KACAGENC, --NO
                  '' AS KACPROTO, --NO
                  COALESCE(P."SPOLITYPE", '') AS KACTIPAP, --NO
                  '' AS DFROTA, --NO
                  '' AS KACTPDUR, --ACLARAR
                  COALESCE(P."SRENEWAL", '') AS KACMODRE,
                  '' AS KACMTNRE, --NO
                  '' AS KACTPCOB, --NO
                  COALESCE(CAST(P."NPAYFREQ" AS VARCHAR), '') AS KACTPFRC,
                  '' AS KACTPCSG,
                  COALESCE(P."SCOLREINT", '') AS KACINDRE,
                  '' AS KACCDGER, --NO
                  CAST((
                       SELECT CP."NCURRENCY" FROM USVTIMV01."CURREN_POL" CP 
                       WHERE  CP."SCERTYPE" = P."SCERTYPE"
                       AND    CP."NBRANCH"  = P."NBRANCH"
                       AND    CP."NPRODUCT" = P."NPRODUCT" 
                       AND    CP."NPOLICY"  = P."NPOLICY"
                       AND    CP."NCERTIF"  = CERT."NCERTIF"
                       AND    CP."DEFFECDATE" <= P."DSTARTDATE" 
                       AND ( CP."DNULLDATE" IS NULL OR CP."DNULLDATE" > P."DSTARTDATE")
                  ) AS VARCHAR) AS KACMOEDA,
                  CAST( ROUND((
                     		SELECT COALESCE(E."NEXCHANGE", 0)
                     		FROM USVTIMV01."EXCHANGE" E 
                  	      WHERE E."NCURRENCY" = ( SELECT CP."NCURRENCY" 
                                             		FROM USVTIMV01."CURREN_POL" CP
                                             		WHERE "SCERTYPE" = '2'
                                             		AND "NBRANCH"    = P."NBRANCH"
                                             		AND "NPRODUCT"   = P."NPRODUCT"
                                             		AND "NPOLICY"    = P."NPOLICY"
                  	                            AND "NCERTIF"    = 0
                                                  AND CP."DEFFECDATE" <= P."DSTARTDATE"
                                                  AND (CP."DNULLDATE" IS NULL OR CP."DNULLDATE" > P."DSTARTDATE"))
                      AND E."DEFFECDATE" <= P."DSTARTDATE"
                      AND (E."DNULLDATE" IS NULL OR E."DNULLDATE" > P."DSTARTDATE")
                  ), 4) AS VARCHAR) AS VCAMBIO,
                  '' AS KACREGCB, --NO
                  '' AS KCBMED_DRA,--NO
                  '' AS KCBMED_CB,--NO
                  '' AS VTXCOMCB, --ACLARAR
                  '' AS VMTCOMCB, --ACLARAR
                  '' AS KCBMED_PD,--NO
                  CAST(COALESCE(  --POR CERTIFICADO
                      	   (SELECT COALESCE("NPERCENT", 0) FROM  USVTIMV01."COMMISSION" CO 
                              WHERE  CO."SCERTYPE" = '2'
                              AND    CO."NBRANCH"  = P."NBRANCH"
                              AND	   CO."NPRODUCT" = P."NPRODUCT" 	
                              AND    CO."NPOLICY"  = P."NPOLICY"  
                              AND    CO."NCERTIF"  = CERT."NCERTIF" 
                              AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                              AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                              AND    CO."NINTERTYP" <> 1
                              LIMIT 1),
                              --POR POLIZA
                              (SELECT COALESCE("NPERCENT", 0) FROM  USVTIMV01."COMMISSION" CO 
                               WHERE  CO."SCERTYPE" = '2'
                               AND    CO."NBRANCH"  = P."NBRANCH"
                               AND	CO."NPRODUCT" = P."NPRODUCT" 	
                               AND    CO."NPOLICY"  = P."NPOLICY"  
                               AND    CO."NCERTIF"  = 0
                               AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                               AND 	(CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                               AND    CO."NINTERTYP" <> 1
                               LIMIT 1)  
                  ) AS VARCHAR) AS VTXCOMMD,
                  CAST(COALESCE(  --POR CERTIFICADO
                      	     (SELECT COALESCE(ROUND("NAMOUNT", 2), 0) FROM  USVTIMV01."COMMISSION" CO 
                              WHERE  CO."SCERTYPE" = '2'
                              AND    CO."NBRANCH"  = P."NBRANCH"
                              AND	   CO."NPRODUCT" = P."NPRODUCT" 	
                              AND    CO."NPOLICY"  = P."NPOLICY"  
                              AND    CO."NCERTIF"  = CERT."NCERTIF" 
                              AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                              AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                              AND    CO."NINTERTYP" <> 1
                              LIMIT 1),
                              --POR POLIZA
                              (SELECT COALESCE(ROUND("NAMOUNT", 2), 0) FROM  USVTIMV01."COMMISSION" CO 
                               WHERE  CO."SCERTYPE" = '2'
                               AND    CO."NBRANCH"  = P."NBRANCH"
                               AND	CO."NPRODUCT" = P."NPRODUCT" 	
                               AND    CO."NPOLICY"  = P."NPOLICY"  
                               AND    CO."NCERTIF"  = 0
                               AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                               AND 	(CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                               AND    CO."NINTERTYP" <> 1
                               LIMIT 1) 
                  ) AS VARCHAR) AS VMTCOMMD,
                  '' AS KCBMED_P2, --NO
                  '' AS VTXCOMME,--NO
                  '' AS VMTCOMME,--NO
                  CAST((
                    SELECT COALESCE(ROUND(SUM(COV."NCAPITAL"), 2), 0)  FROM USVTIMV01."COVER" COV
                    WHERE COV."SCERTYPE" = P."SCERTYPE"
                    AND   COV."NBRANCH"  = P."NBRANCH"
                    AND   COV."NPRODUCT" = P."NPRODUCT"
                    AND   COV."NPOLICY"  = P."NPOLICY" 
                    AND   COV."NCERTIF"  = CERT."NCERTIF"
                    AND   COV."DEFFECDATE" <= P."DSTARTDATE"
                    AND ( COV."DNULLDATE" IS NULL OR COV."DNULLDATE" > P."DSTARTDATE") 
                  ) AS VARCHAR) AS VCAPITAL,
                  '' AS VMTPRMSP, --NO
                  COALESCE(CAST(ROUND(P."NPREMIUM", 2) AS VARCHAR), '') AS VMTCOMR,
                  CAST((
                     SELECT ROUND((COALESCE(C."NSHARE", 0) * COALESCE(P."NPREMIUM", 0)),2)
                     FROM USVTIMV01."COINSURAN" C
                     WHERE C."SCERTYPE" = '2'
                     AND   C."NBRANCH"  = P."NBRANCH" 
                     AND   C."NPRODUCT" = P."NPRODUCT"
                     AND   C."NPOLICY"  = P."NPOLICY"
                     AND   C."NCOMPANY" =  1 --CODIGO DE LA CIA LIDER POSITIVA
                     AND   C."DEFFECDATE" <= P."DSTARTDATE"
                     AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")
                  ) AS VARCHAR) AS VMTCMNQP,
                  '' AS VMTBOMAT,--NO
                  '' AS VTXBOMAT,--NO
                  '' AS VMTBOCOM,--NO
                  '' AS VTXBOCOM,--NO
                  '' AS VMTDECOM,--NO
                  '' AS VTXDECOM,--NO
                  '' AS VMTDETEC,--NO
                  '' AS VTXDETEC,--NO
                  '' AS VMTAGRAV,--NO
                  '' AS VTXAGRAV,--NO
                  '' AS VMTCSAP, --NO
                  '' AS VMTPRMIN,--NO
                  '' AS VMTPRMTR,--NO
                  '' AS VMTPRLIQ,--NO
                  COALESCE(CAST(ROUND(P."NPREMIUM", 2) AS VARCHAR), '') AS VMTPRMBR,
                  (
                     SELECT COALESCE(CAST(C."NSHARE" AS VARCHAR), '')
                     FROM USVTIMV01."COINSURAN" C
                     WHERE C."SCERTYPE" = '2'
                     AND   C."NBRANCH"  = P."NBRANCH" 
                     AND   C."NPRODUCT" = P."NPRODUCT"
                     AND   C."NPOLICY"  = P."NPOLICY"
                     AND   C."NCOMPANY" =  1 --CODIGO DE LA CIA LIDER POSITIVA
                     AND   C."DEFFECDATE" <= P."DSTARTDATE"
                     AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")
                  )  AS VTXCOSSG,
                  '' AS VTXRETEN,
                  CAST(ROUND(((SELECT COALESCE(R."NSHARE", 0) /100
                      FROM USVTIMV01."REINSURAN" R
                      WHERE R."SCERTYPE" =  '2'
                      AND   R."NBRANCH"  = P."NBRANCH"
                      AND   R."NPRODUCT" = P."NPRODUCT" 
                      AND   R."NPOLICY"  = P."NPOLICY" 
                      AND   R."NCERTIF"  = CERT."NCERTIF"
                      AND   R."DEFFECDATE" <= P."DSTARTDATE" 
                      AND  (R."DNULLDATE" IS NULL OR R."DNULLDATE" > P."DSTARTDATE")
                      AND   R."NCOMPANY" = 1
                      AND  R."NTYPE_REIN" = 1
                      AND  R."NCOVER" = 1)*
                     (SELECT COALESCE(SUM(C."NCAPITAL"), 0) FROM USVTIMV01."COVER" C
                      WHERE C."SCERTYPE" = '2'
                      AND   C."NBRANCH"  = P."NBRANCH"
                      AND   C."NPRODUCT" = P."NPRODUCT"
                      AND   C."NPOLICY"  = P."NPOLICY"
                      AND   C."NCERTIF"  = CERT."NCERTIF"
                      AND   C."DEFFECDATE" <= P."DSTARTDATE"
                      AND ( C."DNULLDATE" IS NULL OR P."DNULLDATE" > P."DSTARTDATE"))
                  ), 2) AS VARCHAR) AS VMTCAPRE,
                  '' AS DNUMVIAS,--NO
                  '' AS DQTCRED,--NO
                  '' AS DNIB,--NO
                  '' AS DLOCREF,--NO
                  '' AS KEBMORAD,--NO
                  '' AS DLOCCOBR,--NO
                  '' AS TULTMALT,--NO
                  '' AS DUSRUPD,--NO
                  '' AS VMIPRMTR,--NO
                  '' AS VMIPRLIQ,--NO
                  '' AS VMIPRMBR,--NO
                  '' AS VMIRPMSP,--NO
                  '' AS VMIAGRAV,--NO
                  '' AS VMIDETEC,--NO
                  '' AS VMIDECOM,--NO
                  '' AS VMIBOCOM,--NO
                  '' AS VMIBOMAT,--NO
                  '' AS VMICOMR,--NO
                  '' AS VMICMNQP,--NO
                  '' AS VMICOMME,--NO
                  '' AS VMICOMMD,--NO
                  '' AS VMICOMCB,--NO
                  'LPV' AS DCOMPA,
                  '' AS DMARCA,--NO
                  '' AS KACRGMCB,--NO
                  '' AS KABAPOL_MP,--NO
                  '' AS TMIGPARA,--NO
                  '' AS KABAPOL_MD,--NO
                  '' AS TMIGDE,--NO
                  '' AS KACPGPRE,
                  '' AS TDPGPRE,
                  '' AS TINIPGPR,
                  '' AS TFIMPGPR,
                  '' AS KABAPOL_ESQ,--NO
                  '' AS KABAPOL_EFT,--NO
                  '' AS DSUBSCR,--NO
                  '' AS DNUAPLI,--NO
                  COALESCE(P."SNONULL", '') AS DINDINIB,
                  '' AS DLOCRECB,--NO
                  '' AS KACCLCLI,--NO
                  COALESCE(CAST (P."NNULLCODE" AS VARCHAR), '') AS KACMTALT,
                  '' AS KACTPTRA,
                  '' AS TEMICANC,--NO
                  '' AS DENTIDSO,--NO
                  '' AS DARQUIVO,--NO
                  '' AS TARQUIVO,--NO
                  COALESCE(P."SPOLITYPE", '') AS KACTPSUB,
                  '' AS KACPARES,
                  '' AS KGCRAMO_SAP,--NO
                  '' AS KACTPCRED, --NO
                  '' AS DIBAN,--NO
                  '' AS DSWIFT,--NO
                  '' AS KARMODALID,--NO
                  '' AS DUSREMIS,--NO
                  '' AS KCBMED_VENDA,--NO
                  '' AS DUSRACEIT,--NO
                  '' AS DCANALOPE,--NO
                  '' AS KAICANEM,--NO
                  '' AS DIDCANEM,--NO
                  '' AS KAICANVD,--NO
                  '' AS DIDCANVD,--NO
                  '' AS DNMMULTI,--NO
                  '' AS DOBSERV, --NO
                  (
                  	SELECT CAST(COUNT(*) AS VARCHAR) FROM USVTIMG01."ROLES" R 
                  	WHERE R."SCERTYPE" = P."SCERTYPE"
                  	AND   R."NBRANCH"  = P."NBRANCH" 
                  	AND   R."NPOLICY"  = P."NPOLICY"
                  	AND   R."NCERTIF"  = CERT."NCERTIF"
                  ) AS DQTDPART,
                  '' AS TINIPRXANU,
                  '' AS KACTPREAP,
                  '' AS DENTCONGE, --NO
                  '' AS KCBMED_PARCE, --NO
                  '' AS DCODPARC, --NO
                  '' AS DMODPARC, --NO
                  COALESCE(P."SBUSSITYP", '') AS DTIPSEG,
                  '' AS KACTPNEG, --NO
                  '' AS DURPAGAPO,
                  '' AS DNMINTERP,--NO
                  '' AS DNUMADES, --NO
                  '' AS KACTPPRD, --ACLARAR
                  '' AS KACSBTPRD,    --ACLARAR
                  '' AS KABPRODT_REL, --NO
                  '' AS KACTPPARES,   --NO
                  '' AS KACTIPIFAP,   --NO
                  '' AS KACTPALT_IFRS17, --NO
                  '' AS TEFEALTE,   --NO
                  '' AS TINITARLTA, --NO
                  '' AS TFIMTARLTA, --NO
                  '' AS DTERMO_IFRS17, --NO
                  '' AS TEMISREN      --NO
                  FROM USVTIMV01."POLICY" P
                  LEFT JOIN USVTIMV01."CERTIFICAT" CERT ON CERT."SCERTYPE" = P."SCERTYPE" AND CERT."NBRANCH" = P."NBRANCH" AND CERT."NPRODUCT" = P."NPRODUCT" AND CERT."NPOLICY" = P."NPOLICY"
                  WHERE P."DCOMPDATE" BETWEEN '{L_FECHA_INICIO}' AND '{L_FECHA_FIN}')) as tmp           
                  '''

   #Ejecutar consulta
   L_DF_POLIZAS_VTIME = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_POLIZAS_VTIME).load()

   #------------------------------------------------------------------------------------------------------------------#

   #Declara consulta INSIS
   L_POLIZAS_INSIS = f'''
                  (SELECT
                  'D' AS INDDETREC, 
                  'ABAPOL' AS TABLAIFRS17,
                  '' AS PK,
                  '' AS DTPREG,   --NO
                  '' AS TIOCPROC, --NO
                  coalesce(cast(CAST(P."INSR_BEGIN" AS DATE) as VARCHAR), '') AS TIOCFRM,
                  '' AS TIOCTO,   --NO
                  'PNV' AS KGIORIGM,
                  'LPV' AS KACCOMPA,
                  COALESCE(P."ATTR1", '') AS KGCRAMO,
                  COALESCE(P."ATTR2", '') AS KABPRODT,
                  CASE COALESCE(PP."ENG_POL_TYPE", '')
                  WHEN 'DEPENDENT' THEN P."ATTR1" || '-' || P."ATTR2" || '-' || P."POLICY_NO" || '-' || PP."MASTER_POLICY_ID"
                  ELSE ''
                  END KABAPOL,
                  COALESCE(P."ATTR1", '')  || '-' || COALESCE(P."ATTR2", '') || '-' || COALESCE(P."POLICY_NO", '') AS DNUMAPO,
                  CAST(P."POLICY_ID" AS VARCHAR) AS DNMCERT,
                  '' AS DTERMO,
                  (SELECT ILPI."LEGACY_ID" from
                  USINSIV01."INTRF_LPV_PEOPLE_IDS" ilpi
                  WHERE ILPI."MAN_ID" = PC."MAN_ID") AS KEBENTID_TO,
                  coalesce(cast(CAST(P."REGISTRATION_DATE" AS DATE) as VARCHAR), '') AS TCRIAPO,
                  coalesce(cast(CAST(P."DATE_GIVEN" AS DATE) as VARCHAR), '')  AS TEMISSAO,
                  coalesce(cast(CAST(P."INSR_BEGIN" AS DATE) as VARCHAR), '')  AS TINICIO,
                  '' AS DHORAINI, --NO
                  COALESCE (CAST (CAST(P."INSR_END" AS DATE) as VARCHAR), '') AS TTERMO,
                  '' AS TINIANU, --CERRADO PERO NO ESPECIFICA
                  '' AS TVENANU, --CERRADO PERO NO ESPECIFICA
                  ( SELECT COALESCE (cast(CAST(GA."INSR_BEGIN" as DATE) as VARCHAR), '')
                          FROM USINSIV01."GEN_ANNEX" GA
                          WHERE GA."POLICY_ID" = P."POLICY_ID"
                          AND GA."ANNEX_TYPE"  = '17'
                          AND GA."ANNEX_STATE" = 0
                          AND GA."ANNEX_ID"    = (
                                                   SELECT  MAX(GAX."ANNEX_ID")
                                                   FROM    USINSIV01."GEN_ANNEX" GAX
                                                   WHERE   GAX."POLICY_ID" = P."POLICY_ID"
                                                   AND     GAX."ANNEX_TYPE" = '17'
                                                   AND     GAX."ANNEX_STATE" = 0
                                                 )
                  ) 
                  AS TANSUSP,
                  '' AS TESTADO,
                  CAST(P."POLICY_STATE" AS VARCHAR) AS  KACESTAP,
                  '' AS KACMOEST, --NO
                  COALESCE (cast(CAST(P."INSR_BEGIN" as DATE) as VARCHAR), '') AS TEFEACTA, --CERRADO PERO NO ESPECIFICA
                  '' AS DULTACTA, --NO
                  '' AS KACCNEMI, --NO
                  '' AS KACARGES, --NO
                  '' AS KACAGENC, --NO
                  '' AS KACPROTO, --NO
                  '' AS KACTIPAP, --CERRADO PERO NO ESPECIFICA
                  '' AS DFROTA,   --NO
                  '' AS KACTPDUR, --ACLARAR
                  COALESCE(P."RENEWABLE_FLAG", '') AS KACMODRE,
                  '' AS KACMTNRE, --NO
                  '' AS KACTPCOB, --NO
                  COALESCE(P."ATTR5", '') AS KACTPFRC,
                  '' AS KACTPCSG, --EN REVISION
                  '' AS KACINDRE,
                  '' AS KACCDGER, --NO
                  CASE COALESCE((SELECT DISTINCT COALESCE(IO."AV_CURRENCY", '')
                                 FROM USINSIV01."INSURED_OBJECT" IO 
                                 WHERE IO."POLICY_ID" = P."POLICY_ID" LIMIT 1 ),'') 
                  WHEN 'USD' THEN '2'
                  WHEN 'PEN' THEN '1'
                  ELSE '0'
                  END KACMOEDA,
                  '' AS VCAMBIO,
                  '' AS KACREGCB,   --NO
                  '' AS KCBMED_DRA, --NO
                  '' AS KCBMED_CB,  --ACLARAR
                  '' AS VTXCOMCB,   --ACLARAR
                  '' AS VMTCOMCB,   --NO
                  '' AS KCBMED_PD,  --NO  
                  '' AS VTXCOMMD,
                  '' AS VMTCOMMD,
                  '' AS KCBMED_P2,  --NO
                  '' AS VTXCOMME,   --NO
                  '' AS VMTCOMME,   --NO
                  '' AS VCAPITAL,   --CERRADO PERO NO ESPECIFICA
                  '' AS VMTPRMSP,   --NO
                  '' AS VMTCOMR,
                  '' AS VMTCMNQP,
                  '' AS VMTBOMAT,   --NO
                  '' AS VTXBOMAT,   --NO
                  '' AS VMTBOCOM,   --NO
                  '' AS VTXBOCOM,   --NO
                  '' AS VMTDECOM,   --NO
                  '' AS VTXDECOM,   --NO
                  '' AS VMTDETEC,   --NO
                  '' AS VTXDETEC,   --NO
                  '' AS VMTAGRAV,   --NO
                  '' AS VTXAGRAV,   --NO
                  '' AS VMTCSAP,    --NO
                  '' AS VMTPRMIN,   --NO
                  '' AS VMTPRMTR,   --NO
                  '' AS VMTPRLIQ,   --NO
                  '' AS VMTPRMBR,
                  '' AS VTXCOSSG,
                  '' AS VTXRETEN,
                  '' AS VMTCAPRE,
                  '' AS DNUMVIAS,   --NO
                  '' AS DQTCRED,    --NO
                  '' AS DNIB,       --NO
                  '' AS DLOCREF,    --NO
                  '' AS KEBMORAD,   --NO
                  '' AS DLOCCOBR,   --NO
                  '' AS TULTMALT,   --NO
                  '' AS DUSRUPD,    --NO
                  '' AS VMIPRMTR,   --NO
                  '' AS VMIPRLIQ,   --NO
                  '' AS VMIPRMBR,   --NO
                  '' AS VMIRPMSP,   --NO
                  '' AS VMIAGRAV,   --NO
                  '' AS VMIDETEC,   --NO
                  '' AS VMIDECOM,   --NO
                  '' AS VMIBOCOM,   --NO
                  '' AS VMIBOMAT,   --NO
                  '' AS VMICOMR,    --NO
                  '' AS VMICMNQP,   --NO
                  '' AS VMICOMME,   --NO
                  '' AS VMICOMMD,   --NO
                  '' AS VMICOMCB,   --NO
                  'LPV' AS DCOMPA,
                  '' AS DMARCA,     --NO
                  '' AS KACRGMCB,   --NO
                  '' AS KABAPOL_MP, --NO
                  '' AS TMIGPARA,   --NO
                  '' AS KABAPOL_MD, --NO
                  '' AS TMIGDE,     --NO
                  '' AS KACPGPRE, 
                  '' AS TDPGPRE,
                  '' AS TINIPGPR,
                  '' AS TFIMPGPR,
                  '' AS KABAPOL_ESQ, --NO
                  '' AS KABAPOL_EFT, --NO
                  '' AS DSUBSCR,     --NO
                  '' AS DNUAPLI,     --NO
                  '' AS DINDINIB,
                  '' AS DLOCRECB,    --NO
                  '' AS KACCLCLI,    --NO
                  '' AS KACMTALT,     
                  '' AS KACTPTRA,
                  '' AS TEMICANC,   --NO 
                  '' AS DENTIDSO,   --NO
                  '' AS DARQUIVO,   --NO
                  '' AS TARQUIVO,   --NO
                  '' AS KACTPSUB,
                  '' AS KACPARES,
                  '' AS KGCRAMO_SAP, --NO
                  '' AS KACTPCRED,   --NO
                  '' AS DIBAN,       --NO
                  '' AS DSWIFT,      --NO
                  '' AS KARMODALID,  --NO
                  '' AS DUSREMIS,--NO
                  '' AS KCBMED_VENDA,--NO
                  '' AS DUSRACEIT,   --NO
                  '' AS DCANALOPE,   --NO
                  '' AS KAICANEM,    --NO
                  '' AS DIDCANEM,   --NO  
                  '' AS KAICANVD,    --NO
                  '' AS DIDCANVD,    --NO
                  '' AS DNMMULTI,    --NO
                  '' AS DOBSERV,     --NO
                  '' AS DQTDPART,
                  '' AS TINIPRXANU,
                  '' AS KACTPREAP,
                  '' AS DENTCONGE,    --NO
                  '' AS KCBMED_PARCE, --NO
                  '' AS DCODPARC, --NO
                  '' AS DMODPARC, --NO
                  '' AS DTIPSEG,  
                  '' AS KACTPNEG, --NO
                  '' AS DURPAGAPO,
                  '' AS DNMINTERP,--NO
                  '' AS DNUMADES,--NO
                  '' AS KACTPPRD,
                  '' AS KACSBTPRD,
                  '' AS KABPRODT_REL,--NO
                  '' AS KACTPPARES,--NO
                  '' AS KACTIPIFAP,--NO
                  '' AS KACTPALT_IFRS17,--NO
                  '' AS TEFEALTE,--NO
                  '' AS TINITARLTA,--NO
                  '' AS TFIMTARLTA,--NO
                  '' AS DTERMO_IFRS17,--NO
                  '' AS TEMISREN--NO
                  FROM USINSIV01."POLICY" P 
                  LEFT JOIN USINSIV01."P_CLIENTS" PC ON P."CLIENT_ID" = PC."CLIENT_ID"
                  LEFT JOIN USINSIV01."POLICY_ENG_POLICIES" PP ON P."POLICY_ID" = PP."POLICY_ID"
                  WHERE P."REGISTRATION_DATE" BETWEEN '{L_FECHA_INICIO}' AND '{L_FECHA_FIN}') as tmp
                  '''

   #Ejecutar consulta
   L_DF_POLIZAS_INSIS = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_POLIZAS_INSIS).load()

   #------------------------------------------------------------------------------------------------------------------#

   #DECLARAR CONSULTA INSUNIX
   L_POLIZAS_INSUNIX = f'''
                          ((SELECT 
                          'D',
                          'ABAPOL',
                          '' AS PK,
                          '' AS DTPREG,      --NO
                          '' AS TIOCPROC,    --NO
                          COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TIOCFRM,
                          '' AS TIOCTO,      --NO
                          'PIG' AS KGIORIGM,    --NO
                          'LPG' AS KACCOMPA,
                          CAST(P.BRANCH AS VARCHAR) AS KGCRAMO,
                          CAST(P.PRODUCT AS VARCHAR) AS KABPRODT,
                          CASE P.POLITYPE
                          WHEN '2' THEN CASE WHEN CERT.CERTIF <> 0 THEN P.BRANCH || '-' || P.PRODUCT || '-' || P.POLICY || '-' || '0'
                                        ELSE ''
                                        END
                          ELSE '' 
                          END AS KABAPOL,
                          P.BRANCH || '-' || P.PRODUCT || '-' || P.POLICY AS DNUMAPO,
                          CAST(CERT.CERTIF AS VARCHAR) AS DNMCERT,
                          '' AS DTERMO,
                          (
                          	SELECT SCOD_VT FROM USINSUG01.EQUI_VT_INX EVI
                          	WHERE EVI.SCOD_INX = P.TITULARC
                          ) AS KEBENTID_TO,
                          COALESCE(CAST(P.DATE_ORIGI AS VARCHAR), '') AS TCRIAPO,
                          COALESCE(CAST(P.ISSUEDAT  AS VARCHAR) , '') AS TEMISSAO,
                          COALESCE(CAST(P.EFFECDATE AS VARCHAR) , '') AS TINICIO,
                          '' AS DHORAINI, --NO
                          COALESCE(CAST(P.EXPIRDAT  AS VARCHAR) , '')  AS TTERMO,
                          COALESCE(CAST(P.EFFECDATE AS VARCHAR) , '')  AS TINIANU,
                          COALESCE(CAST(P.EXPIRDAT  AS VARCHAR) , '')  AS TVENANU,
                          COALESCE(CAST(P.NULLDATE  AS VARCHAR) , '')  AS TANSUSP,
                          '' AS TESTADO,
                          COALESCE(P.STATUS_POL, '') AS KACESTAP,
                          '' AS KACMOEST, --NO
                          coalesce(cast(P.EFFECDATE as VARCHAR), '') AS TEFEACTA,
                          '' AS DULTACTA,--NO
                          '' AS KACCNEMI,--NO
                          '' AS KACARGES,--NO
                          '' AS KACAGENC,--NO
                          '' AS KACPROTO,--NO
                          COALESCE(P.POLITYPE, '') AS KACTIPAP,
                          '' AS DFROTA,--NO
                          '' AS KACTPDUR,
                          COALESCE(P.RENEWAL, '')  AS KACMODRE,
                          '' AS KACMTNRE,--NO
                          '' AS KACTPCOB,--NO
                          COALESCE(P.PAYFREQ, '')  AS KACTPFRC,
                          '' AS KACTPCSG,
                          COALESCE(P.REINTYPE,'') AS KACINDRE,
                          '' AS KACCDGER,--NO
                          (
                            SELECT COALESCE(CAST(CURRENCY AS VARCHAR), '') FROM USINSUG01.CURREN_POL CP 
                            WHERE CP.USERCOMP = P.USERCOMP 
                            AND   CP.COMPANY = P.COMPANY 
                            AND   CP.CERTYPE = P.CERTYPE
                            AND   CP.BRANCH  = P.BRANCH
                            AND   CP.POLICY  = P.POLICY
                            AND   CP.CERTIF  = CERT.CERTIF 
                            AND   CP.EFFECDATE <= P.EFFECDATE 
                            AND   ( CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE)
                          ) AS KACMOEDA,
                          (
                             SELECT COALESCE(CAST(E.EXCHANGE AS VARCHAR), '')
                             FROM USINSUG01.EXCHANGE E
                             WHERE E.USERCOMP = 1
                             AND   E.COMPANY = 1 
                             AND   E.CURRENCY = (   SELECT CP.CURRENCY 
                                                    FROM USINSUG01.CURREN_POL CP
                                                    WHERE USERCOMP = 1 
                                                    AND COMPANY = 1 
                                                    AND CERTYPE = '2'
                                                    AND BRANCH  = P.BRANCH
                                                    AND POLICY  = P.POLICY
                                                    AND CERTIF  = 0
                                                    AND CP.EFFECDATE <= P.EFFECDATE 
                                                    AND (CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE)
                                                 )
                              AND E.EFFECDATE <= P.EFFECDATE
                              AND (E.NULLDATE IS NULL OR E.NULLDATE > P.EFFECDATE)
                          )AS VCAMBIO,
                          '' AS KACREGCB,  --NO
                          '' AS KCBMED_DRA,--NO
                          '' AS KCBMED_CB, --NO
                          '' AS VTXCOMCB,  --ACLARAR
                          '' AS VMTCOMCB,
                          '' AS KCBMED_PD, --NO
                          COALESCE(
                          	      (SELECT CAST(PERCENT AS VARCHAR) 
                          	       FROM USINSUG01.COMMISSION C 
                          	       WHERE C.USERCOMP = P.USERCOMP 
                          	       AND   C.COMPANY  = P.COMPANY 
                          	       AND   C.CERTYPE  = P.CERTYPE
                          	       AND   C.BRANCH   = P.BRANCH
                          	       AND   C.POLICY   = P.POLICY 
                          	       AND   C.CERTIF   = CERT.CERTIF 
                          	       AND   C.EFFECDATE <= P.EFFECDATE
                          	       AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          	       AND   C.ROLE <> 1),
                          		  (SELECT CAST(PERCENT AS VARCHAR) 
                          	       FROM USINSUG01.COMMISSION C 
                          	       WHERE C.USERCOMP = P.USERCOMP 
                          	       AND   C.COMPANY  = P.COMPANY 
                          	       AND   C.CERTYPE  = P.CERTYPE
                          	       AND   C.BRANCH   = P.BRANCH
                          	       AND   C.POLICY   = P.POLICY 
                          	       AND   C.CERTIF   = 0 
                          	       AND   C.EFFECDATE <= P.EFFECDATE
                          	       AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          	       AND   C.ROLE <> 1)
                          ) AS VTXCOMMD
                          ,
                          COALESCE(
                          	      (SELECT CAST(AMOUNT AS VARCHAR) 
                          	       FROM USINSUG01.COMMISSION C 
                          	       WHERE C.USERCOMP = P.USERCOMP 
                          	       AND   C.COMPANY  = P.COMPANY 
                          	       AND   C.CERTYPE  = P.CERTYPE
                          	       AND   C.BRANCH   = P.BRANCH
                          	       AND   C.POLICY   = P.POLICY 
                          	       AND   C.CERTIF   = CERT.CERTIF 
                          	       AND   C.EFFECDATE <= P.EFFECDATE
                          	       AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          	       AND   C.ROLE <> 1),
                          		   (SELECT CAST(AMOUNT AS VARCHAR) 
                          	       FROM USINSUG01.COMMISSION C 
                          	       WHERE C.USERCOMP = P.USERCOMP 
                          	       AND   C.COMPANY  = P.COMPANY 
                          	       AND   C.CERTYPE  = P.CERTYPE
                          	       AND   C.BRANCH   = P.BRANCH
                          	       AND   C.POLICY   = P.POLICY 
                          	       AND   C.CERTIF   = 0 
                          	       AND   C.EFFECDATE <= P.EFFECDATE
                          	       AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          	       AND   C.ROLE <> 1)
                          ) AS VMTCOMMD,
                          '' AS KCBMED_P2, --NO
                          '' AS VTXCOMME,  --NO
                          '' AS VMTCOMME,  --NO
                          (
                          	SELECT COALESCE(CAST(SUM(C.CAPITAL) AS VARCHAR), '') FROM USINSUG01.COVER C 
                          	WHERE C.USERCOMP = P.USERCOMP 
                          	AND   C.COMPANY  = P.COMPANY 
                          	AND   C.CERTYPE  = P.CERTYPE
                          	AND   C.BRANCH   = P.BRANCH
                          	AND   C.POLICY   = P.POLICY 
                          	AND   C.CERTIF   = CERT.CERTIF 
                          	AND   C.EFFECDATE <= P.EFFECDATE
                          	AND   (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          ) AS VCAPITAL,
                          '' AS VMTPRMSP,--NO
                          COALESCE(CAST(P.PREMIUM AS VARCHAR), '') AS VMTCOMR,
                          (
                             SELECT COALESCE(CAST((SHARE * P.PREMIUM) AS VARCHAR), '')
                             FROM USINSUG01.COINSURAN C
                             WHERE C.USERCOMP = 1
                             AND C.COMPANY = 1 -- VALOR DE LA COMPAÑIA
                             AND C.CERTYPE = '2'
                             AND C.BRANCH = P.BRANCH 
                             AND C.POLICY = P.POLICY
                             AND C.EFFECDATE <= P.EFFECDATE
                             AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          )AS VMTCMNQP,
                          '' AS VMTBOMAT,--NO
                          '' AS VTXBOMAT,--NO
                          '' AS VMTBOCOM,--NO
                          '' AS VTXBOCOM,--NO
                          '' AS VMTDECOM,--NO
                          '' AS VTXDECOM,--NO
                          '' AS VMTDETEC,--NO
                          '' AS VTXDETEC,--NO
                          '' AS VMTAGRAV,--NO
                          '' AS VTXAGRAV,--NO
                          '' AS VMTCSAP, --NO
                          '' AS VMTPRMIN,--NO
                          '' AS VMTPRMTR,--NO
                          '' AS VMTPRLIQ,--NO
                          COALESCE(CAST(P.PREMIUM AS VARCHAR), '')  AS VMTPRMBR,
                          (
                            SELECT COALESCE(CAST(SHARE AS VARCHAR), '')
                            FROM USINSUG01.COINSURAN C
                            WHERE C.USERCOMP = 1
                            AND C.COMPANY = 1 -- VALOR DE LA COMPAÑIA
                            AND C.CERTYPE = '2'
                            AND C.BRANCH  = P.BRANCH 
                            AND C.POLICY  = P.POLICY
                            AND C.EFFECDATE <= P.EFFECDATE
                            AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          ) AS VTXCOSSG,
                          (    
                            SELECT COALESCE(CAST((SHARE /100) AS VARCHAR), '')
                            FROM USINSUV01.REINSURAN R
                            WHERE R.USERCOMP = 1
                            AND R.CERTYPE =  '2'
                            AND R.BRANCH = P.BRANCH
                            AND R.POLICY = P.POLICY
                            AND R.CERTIF = CERT.CERTIF
                            AND EFFECDATE <= P.EFFECDATE
                            AND (R.NULLDATE IS NULL OR R.NULLDATE > P.EFFECDATE)
                            AND R.TYPE = 1
                          ) AS VTXRETEN,
                          COALESCE(CAST((
                          (SELECT (SHARE /100)
                          	 FROM USINSUV01.REINSURAN R
                          	 WHERE R.USERCOMP = 1
                          	 AND R.CERTYPE =  '2'
                          	 AND R.BRANCH = P.BRANCH
                          	 AND R.POLICY = P.POLICY
                          	 AND R.CERTIF = CERT.CERTIF
                          	 AND EFFECDATE <= P.EFFECDATE
                          	 AND (R.NULLDATE IS NULL OR R.NULLDATE > P.EFFECDATE)
                          	 AND R.TYPE = 1
                          ) *
                          (SELECT SUM(CAPITAL)
                          	 FROM USINSUV01.COVER C
                          	 WHERE C.USERCOMP = 1
                          	 AND C.CERTYPE =  '2'
                          	 AND C.BRANCH = P.BRANCH
                          	 AND C.POLICY = P.POLICY
                          	 AND C.CERTIF = CERT.CERTIF
                          	 AND EFFECDATE <= P.EFFECDATE
                          	 AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)	
                          )
                          ) AS VARCHAR), '') AS VMTCAPRE,
                          '' AS DNUMVIAS,    --NO
                          '' AS DQTCRED,     --NO
                          '' AS DNIB,        --NO
                          '' AS DLOCREF,     --NO
                          '' AS KEBMORAD,    --NO
                          '' AS DLOCCOBR,    --NO
                          '' AS TULTMALT,    --NO
                          '' AS DUSRUPD,     --NO
                          '' AS VMIPRMTR,    --NO
                          '' AS VMIPRLIQ,    --NO
                          '' AS VMIPRMBR,    --NO
                          '' AS VMIRPMSP,    --NO
                          '' AS VMIAGRAV,    --NO
                          '' AS VMIDETEC,    --NO
                          '' AS VMIDECOM,    --NO
                          '' AS VMIBOCOM,    --NO
                          '' AS VMIBOMAT,    --NO
                          '' AS VMICOMR,     --NO
                          '' AS VMICMNQP,    --NO
                          '' AS VMICOMME,    --NO
                          '' AS VMICOMMD,    --NO
                          '' AS VMICOMCB,    --NO
                          'LPG' AS DCOMPA,
                          '' AS DMARCA,      --NO
                          '' AS KACRGMCB,    --NO
                          '' AS KABAPOL_MP,  --NO
                          '' AS TMIGPARA,    --NO
                          '' AS KABAPOL_MD,  --NO
                          '' AS TMIGDE,      --NO
                          '' AS KACPGPRE,
                          '' AS TDPGPRE,
                          '' AS TINIPGPR,
                          '' AS TFIMPGPR,
                          '' AS KABAPOL_ESQ, --NO
                          '' AS KABAPOL_EFT, --NO
                          '' AS DSUBSCR,     --NO
                          '' AS DNUAPLI,     --NO
                          '' AS DINDINIB,
                          '' AS DLOCRECB,    --NO
                          '' AS KACCLCLI,    --NO
                          COALESCE(CAST(P.NULLCODE AS VARCHAR), '') AS KACMTALT,
                          '' AS KACTPTRA,
                          '' AS TEMICANC,    --NO
                          '' AS DENTIDSO,    --NO
                          '' AS DARQUIVO,    --NO
                          '' AS TARQUIVO,    --NO
                          COALESCE(P.POLITYPE, '') AS KACTPSUB,
                          '' AS KACPARES,
                          '' AS KGCRAMO_SAP, --NO
                          '' AS KACTPCRED,   --NO
                          '' AS DIBAN,       --NO
                          '' AS DSWIFT,      --NO
                          '' AS KARMODALID,  --NO
                          '' AS DUSREMIS,    --NO
                          '' AS KCBMED_VENDA,--NO
                          '' AS DUSRACEIT,   --NO
                          '' AS DCANALOPE,   --NO
                          '' AS KAICANEM,    --NO
                          '' AS DIDCANEM,    --NO
                          '' AS KAICANVD,    --NO
                          '' AS DIDCANVD,    --NO
                          '' AS DNMMULTI,    --NO
                          '' AS DOBSERV,     --NO
                          (
                          	SELECT CAST(COUNT(*) AS VARCHAR) FROM USINSUG01.ROLES R 
                          	WHERE R.USERCOMP = P.USERCOMP 
                          	AND R.COMPANY 	 = P.COMPANY 
                          	AND R.CERTYPE 	 = P.CERTYPE 
                          	AND R.BRANCH 	 = P.BRANCH 
                          	AND R.POLICY 	 = P.POLICY 
                          	AND R.CERTIF 	 = CERT.CERTIF 
                          ) AS DQTDPART,
                          COALESCE(CAST(P.YEAR_MONTH AS VARCHAR), '') AS TINIPRXANU,
                          '' AS KACTPREAP,
                          '' AS DENTCONGE,    --NO
                          '' AS KCBMED_PARCE, --NO
                          '' AS DCODPARC,  	--NO
                          '' AS DMODPARC,  	--NO
                          COALESCE(CAST(P.BUSSITYP AS VARCHAR), '') AS DTIPSEG,
                          '' AS KACTPNEG,  	--NO
                          '' AS DURPAGAPO,
                          '' AS DNMINTERP, 	--NO
                          '' AS DNUMADES,  	--NO
                          '' AS KACTPPRD,  	--ACLARAR
                          '' AS KACSBTPRD, 	--ACLARAR
                          '' AS KABPRODT_REL, --NO
                          '' AS KACTPPARES, 	--NO
                          '' AS KACTIPIFAP, 	--NO
                          '' AS KACTPALT_IFRS17, --NO
                          '' AS TEFEALTE, 	   --NO
                          '' AS TINITARLTA,      --NO
                          '' AS TFIMTARLTA,      --NO
                          '' AS DTERMO_IFRS17,   --NO
                          '' AS TEMISREN		   --NO
                          FROM USINSUG01.POLICY P 
                          LEFT JOIN USINSUG01.CERTIFICAT CERT 
                          ON  CERT.USERCOMP = P.USERCOMP 
                          AND CERT.COMPANY = P.COMPANY   
                          AND CERT.CERTYPE = P.CERTYPE 
                          AND CERT.BRANCH  = P.BRANCH 
                          AND CERT.PRODUCT = P.PRODUCT 
                          AND CERT.POLICY  = P.POLICY
                          WHERE P.EFFECDATE BETWEEN '' AND '')

                          UNION ALL

                          (SELECT 
                          'D',
                          'ABAPOL',
                          '' AS PK,
                          '' AS DTPREG,      --NO
                          '' AS TIOCPROC,    --NO
                          COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TIOCFRM,
                          '' AS TIOCTO,      --NO
                          'PIV' AS KGIORIGM,    --NO
                          'LPV' AS KACCOMPA, --NO
                          CAST(P.BRANCH AS VARCHAR) AS KGCRAMO,
                          CAST(P.PRODUCT AS VARCHAR) AS KABPRODT,
                          CASE P.POLITYPE
                          WHEN '2' THEN CASE WHEN CERT.CERTIF <> 0 THEN P.BRANCH || '-' || P.PRODUCT || '-' || P.POLICY || '-' || '0'
                                        ELSE ''
                                        END
                          ELSE '' 
                          END AS KABAPOL,
                          P.BRANCH || '-' || P.PRODUCT || '-' || P.POLICY AS DNUMAPO,
                          CAST(CERT.CERTIF AS VARCHAR) AS DNMCERT,
                          '' AS DTERMO,
                          (
                          	SELECT SCOD_VT FROM USINSUG01.EQUI_VT_INX EVI
                          	WHERE EVI.SCOD_INX = P.TITULARC
                          )
                          AS KEBENTID_TO,
                          COALESCE(CAST(P.DATE_ORIGI AS VARCHAR), '') AS TCRIAPO,
                          COALESCE(CAST(P.ISSUEDAT AS VARCHAR), '') AS TEMISSAO,
                          COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TINICIO,
                          '' AS DHORAINI, --NO
                          COALESCE(CAST(P.EXPIRDAT  AS VARCHAR), '') AS TTERMO,
                          COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TINIANU,
                          COALESCE(CAST(P.EXPIRDAT  AS VARCHAR), '') AS TVENANU,
                          COALESCE(CAST(P.NULLDATE  AS VARCHAR), '') AS TANSUSP,
                          '' AS TESTADO,
                          COALESCE(P.STATUS_POL, '') AS KACESTAP,
                          '' AS KACMOEST, --NO
                          coalesce(cast(P.EFFECDATE as VARCHAR), '') AS TEFEACTA,
                          '' AS DULTACTA,--NO
                          '' AS KACCNEMI,--NO
                          '' AS KACARGES,--NO
                          '' AS KACAGENC,--NO
                          '' AS KACPROTO,--NO
                          COALESCE(P.POLITYPE, '') AS KACTIPAP,
                          '' AS DFROTA,--NO
                          '' AS KACTPDUR,
                          COALESCE(P.RENEWAL, '') AS KACMODRE,
                          '' AS KACMTNRE,--NO
                          '' AS KACTPCOB,--NO
                          COALESCE(P.PAYFREQ, '') AS KACTPFRC,
                          '' AS KACTPCSG,
                          COALESCE(P.REINTYPE, '') AS KACINDRE,
                          '' AS KACCDGER,--NO
                          (
                            SELECT COALESCE(CAST(CURRENCY AS VARCHAR), '') FROM USINSUV01.CURREN_POL CP 
                            WHERE CP.USERCOMP = P.USERCOMP 
                            AND   CP.COMPANY = P.COMPANY 
                            AND   CP.CERTYPE = P.CERTYPE
                            AND   CP.BRANCH  = P.BRANCH
                            AND   CP.POLICY  = P.POLICY
                            AND   CP.CERTIF  = CERT.CERTIF 
                            AND   CP.EFFECDATE <= P.EFFECDATE 
                            AND   (CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE)
                          ) AS KACMOEDA,
                          (
                             SELECT CAST(EXCHANGE AS VARCHAR)
                             FROM USINSUG01.EXCHANGE E
                             WHERE E.USERCOMP = 1
                             AND   E.COMPANY = 1 
                             AND   E.CURRENCY = (   SELECT CP.CURRENCY 
                                                    FROM USINSUV01.CURREN_POL CP
                                                    WHERE USERCOMP = 1 
                                                    AND COMPANY = 1 
                                                    AND CERTYPE = '2'
                                                    AND BRANCH  = P.BRANCH
                                                    AND POLICY  = P.POLICY
                                                    AND CERTIF  = 0
                                                    AND CP.EFFECDATE <= P.EFFECDATE 
                                                    AND (CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE)
                                                 )
                              AND E.EFFECDATE <= P.EFFECDATE
                              AND (E.NULLDATE IS NULL OR E.NULLDATE > P.EFFECDATE)
                          ) AS VCAMBIO,
                          '' AS KACREGCB,  --NO
                          '' AS KCBMED_DRA,--NO
                          '' AS KCBMED_CB, --NO
                          '' AS VTXCOMCB,  --ACLARAR
                          '' AS VMTCOMCB,  --ACLARAR
                          '' AS KCBMED_PD,--NO
                          COALESCE(
                          	      (SELECT COALESCE(CAST(PERCENT AS VARCHAR), '') 
                          	       FROM USINSUV01.COMMISSION C 
                          	       WHERE C.USERCOMP = P.USERCOMP 
                          	       AND   C.COMPANY  = P.COMPANY 
                          	       AND   C.CERTYPE  = P.CERTYPE
                          	       AND   C.BRANCH   = P.BRANCH
                          	       AND   C.POLICY   = P.POLICY 
                          	       AND   C.CERTIF   = CERT.CERTIF 
                          	       AND   C.EFFECDATE <= P.EFFECDATE
                          	       AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          	       AND   C.ROLE <> 1),
                          		   (SELECT COALESCE(CAST(PERCENT AS VARCHAR), '') 
                          	       FROM USINSUV01.COMMISSION C 
                          	       WHERE C.USERCOMP = P.USERCOMP 
                          	       AND   C.COMPANY  = P.COMPANY 
                          	       AND   C.CERTYPE  = P.CERTYPE
                          	       AND   C.BRANCH   = P.BRANCH
                          	       AND   C.POLICY   = P.POLICY 
                          	       AND   C.CERTIF   = 0 
                          	       AND   C.EFFECDATE <= P.EFFECDATE
                          	       AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          	       AND   C.ROLE <> 1)
                          ) AS VTXCOMMD
                          ,
                          COALESCE(
                          	      (SELECT COALESCE(CAST(AMOUNT AS VARCHAR), '') 
                          	       FROM USINSUV01.COMMISSION C 
                          	       WHERE C.USERCOMP = P.USERCOMP 
                          	       AND   C.COMPANY  = P.COMPANY 
                          	       AND   C.CERTYPE  = P.CERTYPE
                          	       AND   C.BRANCH   = P.BRANCH
                          	       AND   C.POLICY   = P.POLICY 
                          	       AND   C.CERTIF   = CERT.CERTIF 
                          	       AND   C.EFFECDATE <= P.EFFECDATE
                          	       AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          	       AND   C.ROLE <> 1),
                          		   (SELECT COALESCE(CAST(AMOUNT AS VARCHAR), '') 
                          	       FROM USINSUV01.COMMISSION C 
                          	       WHERE C.USERCOMP = P.USERCOMP 
                          	       AND   C.COMPANY  = P.COMPANY 
                          	       AND   C.CERTYPE  = P.CERTYPE
                          	       AND   C.BRANCH   = P.BRANCH
                          	       AND   C.POLICY   = P.POLICY 
                          	       AND   C.CERTIF   = 0 
                          	       AND   C.EFFECDATE <= P.EFFECDATE
                          	       AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          	       AND   C.ROLE <> 1)
                          ) AS VMTCOMMD,
                          '' AS KCBMED_P2, --NO
                          '' AS VTXCOMME,  --NO
                          '' AS VMTCOMME,  --NO
                          (
                          	SELECT COALESCE(CAST(SUM(C.CAPITAL) AS VARCHAR), '') FROM USINSUV01.COVER C 
                          	WHERE C.USERCOMP = P.USERCOMP 
                          	AND   C.COMPANY  = P.COMPANY 
                          	AND   C.CERTYPE  = P.CERTYPE
                          	AND   C.BRANCH   = P.BRANCH
                          	AND   C.POLICY   = P.POLICY 
                          	AND   C.CERTIF   = CERT.CERTIF 
                          	AND   C.EFFECDATE <= P.EFFECDATE
                          	AND   (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          ) AS VCAPITAL,
                          '' AS VMTPRMSP,--NO
                          COALESCE(CAST(P.PREMIUM AS VARCHAR), '')  AS VMTCOMR,
                          (
                             SELECT COALESCE(CAST((SHARE * P.PREMIUM) AS VARCHAR), '')
                             FROM USINSUV01.COINSURAN C
                             WHERE C.USERCOMP = 1
                             AND C.COMPANY = 1 -- VALOR DE LA COMPAÑIA
                             AND C.CERTYPE = '2'
                             AND C.BRANCH = P.BRANCH 
                             AND C.POLICY = P.POLICY
                             AND C.EFFECDATE <= P.EFFECDATE
                             AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          )AS VMTCMNQP,
                          '' AS VMTBOMAT,--NO
                          '' AS VTXBOMAT,--NO
                          '' AS VMTBOCOM,--NO
                          '' AS VTXBOCOM,--NO
                          '' AS VMTDECOM,--NO
                          '' AS VTXDECOM,--NO
                          '' AS VMTDETEC,--NO
                          '' AS VTXDETEC,--NO
                          '' AS VMTAGRAV,--NO
                          '' AS VTXAGRAV,--NO
                          '' AS VMTCSAP, --NO
                          '' AS VMTPRMIN,--NO
                          '' AS VMTPRMTR,--NO
                          '' AS VMTPRLIQ,--NO
                          COALESCE(CAST(P.PREMIUM AS VARCHAR), '')  AS VMTPRMBR,
                          (
                            SELECT COALESCE(CAST(SHARE AS VARCHAR), '')
                            FROM USINSUV01.COINSURAN C
                            WHERE C.USERCOMP = 1
                            AND C.COMPANY = 1 -- VALOR DE LA COMPAÑIA
                            AND C.CERTYPE = '2'
                            AND C.BRANCH  = P.BRANCH 
                            AND C.POLICY  = P.POLICY
                            AND C.EFFECDATE <= P.EFFECDATE
                            AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          ) AS VTXCOSSG,
                          '' AS VTXRETEN,
                          COALESCE(CAST((
                              (SELECT (SHARE/100)
                          	 FROM USINSUV01.REINSURAN R
                          	 WHERE R.USERCOMP = 1
                          	 AND R.COMPANY = 1
                          	 AND R.CERTYPE =  '2'
                          	 AND R.BRANCH = P.BRANCH
                          	 AND R.POLICY = P.POLICY
                          	 AND R.CERTIF = CERT.CERTIF
                          	 AND EFFECDATE <= P.EFFECDATE
                          	 AND (R.NULLDATE IS NULL OR R.NULLDATE > P.EFFECDATE)
                          	 AND R.TYPE = 1
                              ) *
                          	(SELECT SUM(CAPITAL) 
                          	 FROM USINSUV01.COVER C
                          	 WHERE C.USERCOMP = 1
                          	 AND C.CERTYPE =  '2'
                          	 AND C.BRANCH = P.BRANCH
                          	 AND C.POLICY = P.POLICY
                          	 AND C.CERTIF = CERT.CERTIF
                          	 AND EFFECDATE <= P.EFFECDATE
                          	 AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          	)
                          ) AS VARCHAR), '') AS VMTCAPRE,
                          '' AS DNUMVIAS, --NO
                          '' AS DQTCRED,  --NO
                          '' AS DNIB,     --NO
                          '' AS DLOCREF,  --NO
                          '' AS KEBMORAD, --NO
                          '' AS DLOCCOBR,--NO
                          '' AS TULTMALT,--NO
                          '' AS DUSRUPD, --NO
                          '' AS VMIPRMTR,--NO
                          '' AS VMIPRLIQ,--NO
                          '' AS VMIPRMBR,--NO
                          '' AS VMIRPMSP,--NO
                          '' AS VMIAGRAV,--NO
                          '' AS VMIDETEC,--NO
                          '' AS VMIDECOM,--NO
                          '' AS VMIBOCOM,--NO
                          '' AS VMIBOMAT,--NO
                          '' AS VMICOMR, --NO
                          '' AS VMICMNQP,--NO
                          '' AS VMICOMME,--NO
                          '' AS VMICOMMD,--NO
                          '' AS VMICOMCB,--NO
                          'LPV' AS DCOMPA,
                          '' AS DMARCA,--NO
                          '' AS KACRGMCB,--NO
                          '' AS KABAPOL_MP,--NO
                          '' AS TMIGPARA,--NO
                          '' AS KABAPOL_MD,--NO
                          '' AS TMIGDE,--NO
                          '' AS KACPGPRE,
                          '' AS TDPGPRE,
                          '' AS TINIPGPR,
                          '' AS TFIMPGPR,
                          '' AS KABAPOL_ESQ,--NO
                          '' AS KABAPOL_EFT,--NO
                          '' AS DSUBSCR,--NO
                          '' AS DNUAPLI,--NO
                          '' AS DINDINIB,
                          '' AS DLOCRECB,--NO
                          '' AS KACCLCLI,--NO
                          COALESCE(CAST(P.NULLCODE AS VARCHAR), '') AS KACMTALT,
                          (
                          	SELECT CAST(PH.TYPE AS VARCHAR) FROM 
                          	USINSUV01.POLICY_HIS PH
                          	WHERE PH.CERTYPE = P.CERTYPE
                          	AND   PH.BRANCH  = P.BRANCH
                          	AND   PH.POLICY = P.POLICY
                          	AND   PH.CERTIF  = CERT.CERTIF
                          	AND   PH.EFFECDATE <= P.EFFECDATE
                          	AND  (PH.NULLDATE IS NULL OR PH.NULLDATE > P.EFFECDATE)
                          	AND   PH.TYPE = 1 --EN EMISION
                          ) AS KACTPTRA,
                          '' AS TEMICANC,--NO
                          '' AS DENTIDSO,--NO
                          '' AS DARQUIVO,--NO
                          '' AS TARQUIVO,--NO
                          COALESCE(P.POLITYPE, '') AS KACTPSUB,
                          '' AS KACPARES,
                          '' AS KGCRAMO_SAP, --NO
                          '' AS KACTPCRED,   --NO
                          '' AS DIBAN,       --NO
                          '' AS DSWIFT, --NO
                          '' AS KARMODALID, --NO
                          '' AS DUSREMIS,--NO
                          '' AS KCBMED_VENDA,--NO
                          '' AS DUSRACEIT,--NO
                          '' AS DCANALOPE,--NO
                          '' AS KAICANEM,--NO
                          '' AS DIDCANEM,--NO
                          '' AS KAICANVD,--NO
                          '' AS DIDCANVD,--NO
                          '' AS DNMMULTI,--NO
                          '' AS DOBSERV, --NO
                          CAST((
                          	SELECT COUNT(*) FROM USINSUV01.ROLES R 
                          	WHERE R.USERCOMP = P.USERCOMP 
                          	AND R.COMPANY 	 = P.COMPANY 
                          	AND R.CERTYPE 	 = P.CERTYPE 
                          	AND R.BRANCH 	 = P.BRANCH 
                          	AND R.POLICY 	 = P.POLICY 
                          	AND R.CERTIF 	 = CERT.CERTIF 
                          ) AS VARCHAR) AS DQTDPART,
                          CAST(P.YEAR_MONTH AS VARCHAR) AS TINIPRXANU,
                          '' AS KACTPREAP,
                          '' AS DENTCONGE,    --NO
                          '' AS KCBMED_PARCE, --NO
                          '' AS DCODPARC,  	--NO
                          '' AS DMODPARC,  	--NO
                          P.BUSSITYP AS DTIPSEG,
                          '' AS KACTPNEG,  	--NO
                          '' AS DURPAGAPO,
                          '' AS DNMINTERP, 	--NO
                          '' AS DNUMADES,  	--NO
                          '' AS KACTPPRD,  	--ACLARAR
                          '' AS KACSBTPRD, 	--ACLARAR
                          '' AS KABPRODT_REL, --NO
                          '' AS KACTPPARES, 	--NO
                          '' AS KACTIPIFAP, 	--NO
                          '' AS KACTPALT_IFRS17, --NO
                          '' AS TEFEALTE, 	   --NO
                          '' AS TINITARLTA,      --NO
                          '' AS TFIMTARLTA,      --NO
                          '' AS DTERMO_IFRS17,   --NO
                          '' AS TEMISREN		   --NO
                          FROM USINSUV01.POLICY P 
                          LEFT JOIN USINSUV01.CERTIFICAT CERT 
                          ON  CERT.USERCOMP = P.USERCOMP 
                          AND CERT.COMPANY = P.COMPANY   
                          AND CERT.CERTYPE = P.CERTYPE 
                          AND CERT.BRANCH  = P.BRANCH 
                          AND CERT.PRODUCT = P.PRODUCT 
                          AND CERT.POLICY  = P.POLICY
                          WHERE P.EFFECDATE BETWEEN '' AND ''))
                          '''

   #Ejecutar consulta
   L_DF_POLIZAS_INSUNIX = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_POLIZAS_INSUNIX).load()

   #Perform the union operation
   L_DF_POLIZAS = L_DF_POLIZAS_VTIME.union(L_DF_POLIZAS_INSIS).union(L_DF_POLIZAS_INSUNIX)

   return L_DF_POLIZAS
