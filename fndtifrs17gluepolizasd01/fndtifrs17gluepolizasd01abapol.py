from pyspark.sql.types import *
from pyspark.sql.functions import col

def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):         

   #Declara consulta VTIME
   L_POLIZAS_VTIME = f'''
                     (
                      (SELECT
                        'D' AS INDDETREC, 
                        'ABAPOL' AS TABLAIFRS17,
                        '' AS PK,              
                        '' AS DTPREG,   --NO
                        '' AS TIOCPROC, --NO
                        COALESCE(CAST(CAST(P."DDATE_ORIGI" AS DATE) AS VARCHAR), '') AS TIOCFRM,
                        '' AS TIOCTO,
                        'PVG' AS KGIORIGM,
                        'LPG' AS KACCOMPA,
                        CAST(P."NBRANCH" AS VARCHAR) AS KGCRAMO,      
                        CAST(P."NBRANCH" AS VARCHAR) || '-' || CAST(P."NPRODUCT" AS VARCHAR) AS KABPRODT,   
                        CASE COALESCE(P."SPOLITYPE", '')
                        WHEN '2' THEN CASE WHEN CERT."NCERTIF" <> 0 THEN CAST(P."NBRANCH" AS VARCHAR) || '-' || CAST(P."NPRODUCT" AS VARCHAR) || '-' || P."NPOLICY" || '-' || '0'
                                      ELSE ''
                                      END
                        ELSE '' 
                        END AS KABAPOL,
                        CAST(P."NBRANCH" AS VARCHAR) || '-' || CAST(P."NPRODUCT" AS VARCHAR) || '-' || P."NPOLICY" AS DNUMAPO,     
                        CAST(CERT."NCERTIF" AS VARCHAR) AS DNMCERT,  
                        '' AS DTERMO, --EN BLANCO
                        COALESCE(P."SCLIENT", '') AS KEBENTID_TO,
                        COALESCE(CAST(CAST(P."DDATE_ORIGI" AS DATE) AS VARCHAR), '') AS TCRIAPO,
                        COALESCE(CAST(CAST(P."DISSUEDAT"   AS DATE) AS VARCHAR), '') AS TEMISSAO,
                        COALESCE(CAST(CAST(P."DDATE_ORIGI" AS DATE) AS VARCHAR), '') AS TINICIO,
                        '' AS DHORAINI,
                        COALESCE(CAST(CAST(P."DEXPIRDAT" AS DATE)  AS VARCHAR), '') AS TTERMO,
                        COALESCE(CAST(CAST(P."DSTARTDATE" AS DATE) AS VARCHAR), '') AS TINIANU,
                        COALESCE(CAST(CAST(P."DEXPIRDAT" AS DATE) AS  VARCHAR), '') AS TVENANU,
                        COALESCE(CAST(CAST(P."DNULLDATE" AS DATE) AS  VARCHAR), '') AS TANSUSP,
                        '' AS TESTADO,              --EN BLANCO
                        COALESCE(P."SSTATUS_POL", '') AS KACESTAP,
                        '' AS KACMOEST,
                        COALESCE(CAST(CAST(P."DSTARTDATE" AS DATE) AS VARCHAR), '') AS TEFEACTA,             --ACLARAR
                        '' AS DULTACTA,
                        '' AS KACCNEMI,
                        '' AS KACARGES,
                        '' AS KACAGENC,
                        '' AS KACPROTO,
                        COALESCE(P."SPOLITYPE", '') AS KACTIPAP, 
                        '' AS DFROTA,
                        '' AS KACTPDUR,             --ACLARAR
                        COALESCE(P."SRENEWAL", '') AS KACMODRE,
                        '' AS KACMTNRE, --no 
                        '' AS KACTPCOB, --NO
                        COALESCE(CAST(P."NPAYFREQ" AS VARCHAR),'') AS KACTPFRC,
                        CASE P."SBUSSITYP"  
                        WHEN '2' THEN  '2' 
                        WHEN '1' THEN COALESCE((SELECT '1' FROM USVTIMG01."COINSURAN" C
                                                WHERE C."SCERTYPE" = '2'
                                                AND C."NBRANCH" = P."NBRANCH"
                                                AND C."NPRODUCT" = P."NPRODUCT"
                                                AND C."NPOLICY" = P."NPOLICY"
                                                AND C."DEFFECDATE" <= P."DSTARTDATE"
                                              AND ( C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")LIMIT 1),'')
                        ELSE ''
                        END AS KACTPCSG, --TODAY
                        COALESCE(P."SCOLREINT", '') AS KACINDRE, 
                        '' AS KACCDGER,
                        COALESCE((    
                            SELECT CAST(CP."NCURRENCY" AS VARCHAR) FROM USVTIMG01."CURREN_POL" CP 
                            WHERE  CP."SCERTYPE" = P."SCERTYPE"
                            AND    CP."NBRANCH"  = P."NBRANCH"
                            AND    CP."NPRODUCT" = P."NPRODUCT"
                            AND    CP."NPOLICY"  = P."NPOLICY"
                            AND    CP."NCERTIF"  = CERT."NCERTIF"
                            AND    CP."DEFFECDATE" <= P."DSTARTDATE"
                            AND ( CP."DNULLDATE" IS NULL OR CP."DNULLDATE" > P."DSTARTDATE")
                            LIMIT 1
                        ),'0')
                        AS KACMOEDA,
                        COALESCE((
                          SELECT COALESCE((E."NEXCHANGE"), 0)
                          FROM USVTIMG01."EXCHANGE" E 
                          WHERE E."NCURRENCY" = ( SELECT CP."NCURRENCY" 
                                                  FROM USVTIMG01."CURREN_POL" CP
                                                  WHERE CP."SCERTYPE" = '2'
                                                  AND CP."NBRANCH"    = P."NBRANCH"
                                                  AND CP."NPRODUCT"   = P."NPRODUCT"
                                                  AND CP."NPOLICY"    = P."NPOLICY"
                                                  AND CP."NCERTIF"    = 0
                                                  AND CP."DEFFECDATE" <= P."DSTARTDATE"
                                                  AND (CP."DNULLDATE" IS NULL OR CP."DNULLDATE" > P."DSTARTDATE") limit 1)
                            AND E."DEFFECDATE" <= P."DSTARTDATE"
                            AND (E."DNULLDATE" IS NULL OR E."DNULLDATE" > P."DSTARTDATE")
                        ), 0) AS VCAMBIO,
                        '' AS KACREGCB,  --NO
                        '' AS KCBMED_DRA,--NO
                        '' AS KCBMED_CB, --NO
                        COALESCE(
                        (SELECT
                          COALESCE (DX."NPERCENT",0)
                        FROM
                          USVTIMG01."DISC_XPREM" DX
                        JOIN USVTIMG01."DISCO_EXPR" DE
                        ON
                          DX."NBRANCH" = DE."NBRANCH"
                          AND DX."NPRODUCT" = DE."NPRODUCT"
                          AND DX."NDISC_CODE" = DE."NDISEXPRC"
                        WHERE
                          DX."SCERTYPE" = P."SCERTYPE"
                          AND DX."NBRANCH" = P."NBRANCH"
                          AND DX."NPRODUCT" = P."NPRODUCT"
                          AND DX."NPOLICY" = P."NPOLICY"
                          AND DX."NCERTIF" = CERT."NCERTIF"
                          AND DX."DEFFECDATE" <= P."DSTARTDATE"
                          AND (DX."DNULLDATE" IS NULL
                            OR DX."DNULLDATE" > P."DSTARTDATE")
                          AND DE."NBILL_ITEM" = 4)
                        ,0) AS VTXCOMCB,  --ACLARAR
                        '' AS VMTCOMCB,   --EN BLANCO
                        '' AS KCBMED_PD,  --NO
                        COALESCE(COALESCE(  --POR CERTIFICADO
                                (SELECT COALESCE(CO."NPERCENT",0) FROM  USVTIMG01."COMMISSION" CO 
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
                                  (SELECT COALESCE(CO."NPERCENT",0) FROM  USVTIMG01."COMMISSION" CO 
                                    WHERE  CO."SCERTYPE" = '2'
                                    AND    CO."NBRANCH"  = P."NBRANCH"
                                    AND	CO."NPRODUCT" = P."NPRODUCT" 	
                                    AND    CO."NPOLICY"  = P."NPOLICY"  
                                    AND    CO."NCERTIF"  = 0
                                    AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                                    AND 	(CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                                    AND    CO."NINTERTYP" <> 1
                                    LIMIT 1)  
                        ),0) AS VTXCOMMD,
                        COALESCE(COALESCE(  --POR CERTIFICADO
                                (SELECT COALESCE("NAMOUNT", 0) FROM  USVTIMG01."COMMISSION" CO 
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
                                  (SELECT COALESCE(CO."NAMOUNT", 0) FROM  USVTIMG01."COMMISSION" CO 
                                    WHERE CO."SCERTYPE" = '2'
                                    AND   CO."NBRANCH"  = P."NBRANCH"
                                    AND	 CO."NPRODUCT" = P."NPRODUCT"
                                    AND   CO."NPOLICY"  = P."NPOLICY"  
                                    AND   CO."NCERTIF"  = 0
                                    AND   CO."DEFFECDATE" <= P."DSTARTDATE"
                                    AND 	(CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                                    AND    CO."NINTERTYP" <> 1
                                    LIMIT 1)
                        ),0) AS VMTCOMMD,
                        '' AS KCBMED_P2, --NO 
                        '' AS VTXCOMME,  --NO
                        '' AS VMTCOMME,  --NO
                        COALESCE((
                          SELECT SUM(COALESCE(COV."NCAPITAL",0)) FROM USVTIMG01."COVER" COV
                          WHERE COV."SCERTYPE" = P."SCERTYPE"
                          AND   COV."NBRANCH"  = P."NBRANCH"
                          AND   COV."NPRODUCT" = P."NPRODUCT"
                          AND   COV."NPOLICY"  = P."NPOLICY" 
                          AND   COV."NCERTIF"  = CERT."NCERTIF"
                          AND   COV."DEFFECDATE" <= P."DSTARTDATE"
                          AND ( COV."DNULLDATE" IS NULL OR COV."DNULLDATE" > P."DSTARTDATE") 
                        ),0)  AS VCAPITAL,
                        '' AS VMTPRMSP, --NO
                        COALESCE(P."NPREMIUM", 0) AS VMTCOMR,
                        COALESCE((
                          SELECT (COALESCE(C."NSHARE", 0) * COALESCE(P."NPREMIUM", 0))
                          FROM USVTIMG01."COINSURAN" C
                          WHERE C."SCERTYPE" = '2'
                          AND   C."NBRANCH"  = P."NBRANCH" 
                          AND   C."NPRODUCT" = P."NPRODUCT"
                          AND   C."NPOLICY"  = P."NPOLICY"
                          AND   C."NCOMPANY" =  1 --CODIGO DE LA CIA LIDER POSITIVA
                          AND   C."DEFFECDATE" <= P."DSTARTDATE"
                          AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")
                        ),0) AS VMTCMNQP,
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
                        COALESCE(P."NPREMIUM", 0) AS VMTPRMBR,
                        COALESCE((
                          SELECT COALESCE(C."NSHARE", 0)
                          FROM USVTIMG01."COINSURAN" C
                          WHERE C."SCERTYPE" = '2'
                          AND   C."NBRANCH"  = P."NBRANCH" 
                          AND   C."NPRODUCT" = P."NPRODUCT"
                          AND   C."NPOLICY"  = P."NPOLICY"
                          AND   C."NCOMPANY" =  1 --CODIGO DE LA CIA LIDER POSITIVA
                          AND   C."DEFFECDATE" <= P."DSTARTDATE"
                          AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")
                        ),0) AS VTXCOSSG,
                        COALESCE((
                          SELECT
                            TRUNC(COALESCE(R."NSHARE",0),4)/100
                          FROM
                            USVTIMG01."REINSURAN" R
                          JOIN USVTIMG01."COVER" C
                        ON
                            R."SCERTYPE" = C."SCERTYPE"
                            AND R."NBRANCH" = C."NBRANCH"
                            AND R."NPRODUCT" = C."NPRODUCT"
                            AND R."NPOLICY" = C."NPOLICY"
                            AND R."NCERTIF" = C."NCERTIF"
                            AND R."NMODULEC" = C."NMODULEC"
                            AND R."NCOVER" = C."NCOVER"
                            AND R."DEFFECDATE" <= C."DEFFECDATE"
                            AND (R."DNULLDATE" IS NULL
                              OR R."DNULLDATE" > C."DEFFECDATE")
                            AND R."NTYPE_REIN" = 1
                          JOIN USVTIMG01."GEN_COVER" GC
                        ON
                            GC."NBRANCH" = C."NBRANCH"
                            AND GC."NPRODUCT" = C."NPRODUCT"
                            AND GC."NMODULEC" = C."NMODULEC"
                            AND GC."NCOVER" = C."NCOVER"
                            AND GC."DEFFECDATE" <= C."DEFFECDATE"
                            AND (GC."DNULLDATE" IS NULL
                              OR GC."DNULLDATE" > C."DEFFECDATE")
                            AND GC."SADDSUINI" IN('1', '3')
                            AND GC."NBRANCH_REI" = R."NBRANCH_REI"
                          WHERE
                            R."SCERTYPE" = '2'
                            AND R."NBRANCH" = P."NBRANCH"
                            AND R."NPRODUCT" = P."NPRODUCT"
                            AND R."NPOLICY" = P."NPOLICY"
                            AND R."NCERTIF" = CERT."NCERTIF"
                            AND R."DEFFECDATE" <= P."DSTARTDATE"
                            AND ( R."DNULLDATE" IS NULL
                              OR R."DNULLDATE" > P."DSTARTDATE")
                          ORDER BY R."NCAPITAL" DESC LIMIT 1
                        ),0) AS VTXRETEN, --TODAY CERRADO PERO NO ESPECIFICA VALOR
                        COALESCE(((SELECT
                            TRUNC(COALESCE(R."NSHARE",0),4)/100
                          FROM
                            USVTIMG01."REINSURAN" R
                          JOIN USVTIMG01."COVER" C
                        ON
                            R."SCERTYPE" = C."SCERTYPE"
                            AND R."NBRANCH" = C."NBRANCH"
                            AND R."NPRODUCT" = C."NPRODUCT"
                            AND R."NPOLICY" = C."NPOLICY"
                            AND R."NCERTIF" = C."NCERTIF"
                            AND R."NMODULEC" = C."NMODULEC"
                            AND R."NCOVER" = C."NCOVER"
                            AND R."DEFFECDATE" <= C."DEFFECDATE"
                            AND (R."DNULLDATE" IS NULL
                              OR R."DNULLDATE" > C."DEFFECDATE")
                            AND R."NTYPE_REIN" = 1
                          JOIN USVTIMG01."GEN_COVER" GC
                        ON
                            GC."NBRANCH" = C."NBRANCH"
                            AND GC."NPRODUCT" = C."NPRODUCT"
                            AND GC."NMODULEC" = C."NMODULEC"
                            AND GC."NCOVER" = C."NCOVER"
                            AND GC."DEFFECDATE" <= C."DEFFECDATE"
                            AND (GC."DNULLDATE" IS NULL
                              OR GC."DNULLDATE" > C."DEFFECDATE")
                            AND GC."SADDSUINI" IN('1', '3')
                            AND GC."NBRANCH_REI" = R."NBRANCH_REI"
                          WHERE
                            R."SCERTYPE" = '2'
                            AND R."NBRANCH" = P."NBRANCH"
                            AND R."NPRODUCT" = P."NPRODUCT"
                            AND R."NPOLICY" = P."NPOLICY"
                            AND R."NCERTIF" = CERT."NCERTIF"
                            AND R."DEFFECDATE" <= P."DSTARTDATE"
                            AND ( R."DNULLDATE" IS NULL
                              OR R."DNULLDATE" > P."DSTARTDATE")
                          ORDER BY R."NCAPITAL" DESC LIMIT 1) *
                          (SELECT SUM(COALESCE(C."NCAPITAL", 0)) FROM USVTIMG01."COVER" C
                            WHERE C."SCERTYPE" = '2'
                            AND   C."NBRANCH"  = P."NBRANCH"
                            AND   C."NPRODUCT" = P."NPRODUCT"
                            AND   C."NPOLICY"  = P."NPOLICY"
                            AND   C."NCERTIF"  = CERT."NCERTIF"
                            AND   C."DEFFECDATE" <= P."DSTARTDATE"
                            AND ( C."DNULLDATE" IS NULL OR P."DNULLDATE" > P."DSTARTDATE"))
                        ),0) AS VMTCAPRE,
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
                        COALESCE(
                        (SELECT CAST(COALESCE(PH."NTYPE_HIST",0) AS VARCHAR) FROM 
                                USVTIMG01."POLICY_HIS" PH
                              WHERE PH."SCERTYPE" = P."SCERTYPE"
                                AND   PH."NBRANCH" = P."NBRANCH"
                                AND   PH."NPOLICY" = P."NPOLICY"
                                AND   PH."NCERTIF" = CERT."NCERTIF"
                                AND   PH."DEFFECDATE" <= P."DSTARTDATE"
                                AND  (PH."DNULLDATE" IS NULL OR PH."DNULLDATE" > P."DSTARTDATE")
                                AND   PH."NTYPE_HIST" = 1) --EN EMISION
                        ,'0') AS KACTPTRA, --TODAY ACLARAR
                        '' AS TEMICANC, --NO
                        '' AS DENTIDSO, --NO
                        '' AS DARQUIVO, --NO
                        '' AS TARQUIVO, --NO
                        COALESCE(P."SPOLITYPE", '') AS KACTPSUB,
                        '' AS KACPARES, -- EN BLANCO
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
                        '' AS TINIPRXANU, --NO APLICA
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
                        WHERE P."SCERTYPE" = '2' AND CAST(P."DCOMPDATE" AS DATE) BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' limit 100
                        )
                        
                        UNION ALL
                  
                        (SELECT
                          'D' AS INDDETREC, 
                          'ABAPOL' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,       --no 
                          '' AS TIOCPROC,     --NO
                          COALESCE(CAST(CAST(P."DDATE_ORIGI" AS DATE) AS VARCHAR), '') AS TIOCFRM,
                          '' AS TIOCTO,       --NO
                          'PVV' AS KGIORIGM,  --NO
                          'LPV' AS KACCOMPA,
                          CAST(P."NBRANCH" AS VARCHAR) KGCRAMO,
                          CAST(P."NBRANCH" AS VARCHAR) || '-' || CAST(P."NPRODUCT" AS VARCHAR) KABPRODT,
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
                          CASE P."SBUSSITYP"  
                          WHEN '2' THEN  '2' 
                          WHEN '1' THEN COALESCE((SELECT '1' FROM USVTIMV01."COINSURAN" C
                                                  WHERE C."SCERTYPE" = '2'
                                                  AND C."NBRANCH" = P."NBRANCH"
                                                  AND C."NPRODUCT" = P."NPRODUCT"
                                                  AND C."NPOLICY" = P."NPOLICY"
                                                  AND C."DEFFECDATE" <= P."DSTARTDATE"
                                                  AND ( C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")LIMIT 1),'')
                          ELSE ''
                          END AS KACTPCSG,
                          COALESCE(P."SCOLREINT", '') AS KACINDRE,
                          '' AS KACCDGER, --NO
                          COALESCE((
                              SELECT CAST(CP."NCURRENCY" AS VARCHAR) FROM USVTIMV01."CURREN_POL" CP 
                              WHERE  CP."SCERTYPE" = P."SCERTYPE"
                              AND    CP."NBRANCH"  = P."NBRANCH"
                              AND    CP."NPRODUCT" = P."NPRODUCT" 
                              AND    CP."NPOLICY"  = P."NPOLICY"
                              AND    CP."NCERTIF"  = CERT."NCERTIF"
                              AND    CP."DEFFECDATE" <= P."DSTARTDATE" 
                              AND ( CP."DNULLDATE" IS NULL OR CP."DNULLDATE" > P."DSTARTDATE")
                          ) ,'0') AS KACMOEDA,
                          COALESCE((
                          SELECT COALESCE(E."NEXCHANGE", 0)
                          FROM USVTIMV01."EXCHANGE" E 
                          WHERE E."NCURRENCY" = ( SELECT CP."NCURRENCY" FROM USVTIMV01."CURREN_POL" CP
                                                  WHERE "SCERTYPE" = '2'
                                                  AND "NBRANCH"    = P."NBRANCH"
                                                  AND "NPRODUCT"   = P."NPRODUCT"
                                                  AND "NPOLICY"    = P."NPOLICY"
                                                  AND "NCERTIF"    = 0
                                                  AND CP."DEFFECDATE" <= P."DSTARTDATE"
                                                  AND (CP."DNULLDATE" IS NULL OR CP."DNULLDATE" > P."DSTARTDATE") limit 1)
                          AND E."DEFFECDATE" <= P."DSTARTDATE"
                          AND (E."DNULLDATE" IS NULL OR E."DNULLDATE" > P."DSTARTDATE")
                          ) ,0) AS VCAMBIO,
                          '' AS KACREGCB,  --NO
                          '' AS KCBMED_DRA,--NO
                          '' AS KCBMED_CB, --NO
                          COALESCE((SELECT COALESCE(DX."NPERCENT",0) FROM USVTIMV01."DISC_XPREM" DX
                          JOIN USVTIMV01."DISCO_EXPR" DE
                          ON DX."NBRANCH" = DE."NBRANCH"
                          AND DX."NPRODUCT" = DE."NPRODUCT"
                          AND DX."NDISC_CODE" = DE."NDISEXPRC"
                          WHERE DX."SCERTYPE" = P."SCERTYPE"
                          AND DX."NBRANCH" = P."NBRANCH"
                          AND DX."NPRODUCT" = P."NPRODUCT"
                          AND DX."NPOLICY" = P."NPOLICY"
                          AND DX."NCERTIF" = CERT."NCERTIF"
                          AND DX."DEFFECDATE" <= P."DSTARTDATE"
                          AND (DX."DNULLDATE" IS NULL
                          OR DX."DNULLDATE" > P."DSTARTDATE")
                          AND DE."NBILL_ITEM" = 4), 0) AS VTXCOMCB, --ACLARAR
                          '' AS VMTCOMCB, --ACLARAR
                          '' AS KCBMED_PD,--NO
                          COALESCE(COALESCE(  --POR CERTIFICADO
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
                          ),0) AS VTXCOMMD,
                          COALESCE(COALESCE(  --POR CERTIFICADO
                                    (SELECT COALESCE("NAMOUNT", 0) FROM  USVTIMV01."COMMISSION" CO 
                                      WHERE  CO."SCERTYPE" = '2'
                                      AND    CO."NBRANCH"  = P."NBRANCH"
                                      AND	 CO."NPRODUCT" = P."NPRODUCT" 	
                                      AND    CO."NPOLICY"  = P."NPOLICY"  
                                      AND    CO."NCERTIF"  = CERT."NCERTIF" 
                                      AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                                      AND   (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                                      AND    CO."NINTERTYP" <> 1
                                      LIMIT 1),
                                      --POR POLIZA
                                    (SELECT COALESCE("NAMOUNT", 0) FROM  USVTIMV01."COMMISSION" CO 
                                      WHERE  CO."SCERTYPE" = '2'
                                      AND    CO."NBRANCH"  = P."NBRANCH"
                                      AND	CO."NPRODUCT" = P."NPRODUCT" 	
                                      AND    CO."NPOLICY"  = P."NPOLICY"  
                                      AND    CO."NCERTIF"  = 0
                                      AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                                      AND 	(CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                                      AND    CO."NINTERTYP" <> 1
                                      LIMIT 1)
                          ),0) AS VMTCOMMD,
                          '' AS KCBMED_P2, --NO
                          '' AS VTXCOMME,--NO
                          '' AS VMTCOMME,--NO
                          COALESCE((
                            SELECT SUM(COALESCE (COV."NCAPITAL",0))  FROM USVTIMV01."COVER" COV
                            WHERE COV."SCERTYPE" = P."SCERTYPE"
                            AND   COV."NBRANCH"  = P."NBRANCH"
                            AND   COV."NPRODUCT" = P."NPRODUCT"
                            AND   COV."NPOLICY"  = P."NPOLICY" 
                            AND   COV."NCERTIF"  = CERT."NCERTIF"
                            AND   COV."DEFFECDATE" <= P."DSTARTDATE"
                            AND ( COV."DNULLDATE" IS NULL OR COV."DNULLDATE" > P."DSTARTDATE") 
                          ) ,0) AS VCAPITAL,
                          '' AS VMTPRMSP, --NO
                          COALESCE(P."NPREMIUM",0) AS VMTCOMR,
                          COALESCE((
                            SELECT (COALESCE(C."NSHARE", 0) * COALESCE(P."NPREMIUM", 0))
                            FROM USVTIMV01."COINSURAN" C
                            WHERE C."SCERTYPE" = '2'
                            AND   C."NBRANCH"  = P."NBRANCH" 
                            AND   C."NPRODUCT" = P."NPRODUCT"
                            AND   C."NPOLICY"  = P."NPOLICY"
                            AND   C."NCOMPANY" =  1 --CODIGO DE LA CIA LIDER POSITIVA
                            AND   C."DEFFECDATE" <= P."DSTARTDATE"
                            AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")
                          ) ,0) AS VMTCMNQP,
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
                          COALESCE(P."NPREMIUM",0) AS VMTPRMBR,
                          COALESCE((
                            SELECT COALESCE(C."NSHARE",0)
                            FROM USVTIMV01."COINSURAN" C
                            WHERE C."SCERTYPE" = '2'
                            AND   C."NBRANCH"  = P."NBRANCH" 
                            AND   C."NPRODUCT" = P."NPRODUCT"
                            AND   C."NPOLICY"  = P."NPOLICY"
                            AND   C."NCOMPANY" =  1 --CODIGO DE LA CIA LIDER POSITIVA
                            AND   C."DEFFECDATE" <= P."DSTARTDATE"
                            AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")
                          ),0)  AS VTXCOSSG,
                          0 AS VTXRETEN,
                          COALESCE(((SELECT TRUNC(COALESCE(R."NSHARE", 0),2) /100
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
                            (SELECT SUM(COALESCE(C."NCAPITAL", 0)) FROM USVTIMV01."COVER" C
                              WHERE C."SCERTYPE" = '2'
                              AND   C."NBRANCH"  = P."NBRANCH"
                              AND   C."NPRODUCT" = P."NPRODUCT"
                              AND   C."NPOLICY"  = P."NPOLICY"
                              AND   C."NCERTIF"  = CERT."NCERTIF"
                              AND   C."DEFFECDATE" <= P."DSTARTDATE"
                              AND ( C."DNULLDATE" IS NULL OR P."DNULLDATE" > P."DSTARTDATE"))
                          ), 0) AS VMTCAPRE,
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
                          COALESCE ((
                          SELECT CAST(COALESCE (PH."NTYPE_HIST",0) AS VARCHAR) FROM 
                                  USVTIMV01."POLICY_HIS" PH
                                WHERE PH."SCERTYPE" = P."SCERTYPE"
                                  AND   PH."NBRANCH" = P."NBRANCH"
                                  AND   PH."NPRODUCT" = P."NPRODUCT"
                                  AND   PH."NPOLICY" = P."NPOLICY"
                                  AND   PH."NCERTIF" = CERT."NCERTIF"
                                  AND   PH."DEFFECDATE" <= P."DSTARTDATE"
                                  AND  (PH."DNULLDATE" IS NULL OR PH."DNULLDATE" > P."DSTARTDATE")
                                  AND   PH."NTYPE_HIST" = 1 --EN EMISION
                                  LIMIT 1
                          ),'0') AS KACTPTRA, --TODAY ACLARAR
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
                          LEFT JOIN USVTIMV01."CERTIFICAT" CERT
                          ON CERT."SCERTYPE" = P."SCERTYPE" 
                          AND CERT."NBRANCH" = P."NBRANCH" 
                          AND CERT."NPRODUCT" = P."NPRODUCT" 
                          AND CERT."NPOLICY" = P."NPOLICY"
                        WHERE P."SCERTYPE" = '2' AND P."DCOMPDATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' limit 100
                         )
                  ) AS TMP           
                  '''

   #Ejecutar consulta
   L_DF_POLIZAS_VTIME = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_POLIZAS_VTIME).load()  

   print('USVTIMV01 exitoso')
   #------------------------------------------------------------------------------------------------------------------#

   #DECLARAR CONSULTA INSUNIX
   L_POLIZAS_INSUNIX = f'''
                       (( SELECT 
                          'D' AS INDDETREC, 
                          'ABAPOL' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,      --NO
                          '' AS TIOCPROC,    --NO
                          COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TIOCFRM,
                          '' AS TIOCTO,      --NO
                          'PIG' AS KGIORIGM, --NO
                          'LPG' AS KACCOMPA,
                          CAST(P.BRANCH AS VARCHAR) AS KGCRAMO,
                          CAST(P.BRANCH AS VARCHAR) || '-' || CAST(P.PRODUCT AS VARCHAR) AS KABPRODT,
                          CASE P.POLITYPE
                          WHEN '2' THEN CASE WHEN CERT.CERTIF <> 0 THEN P.BRANCH || '-' || CAST(COALESCE (P.PRODUCT, 0) AS VARCHAR) || '-' || P.POLICY || '-' || '0'
                                        ELSE ''
                                        END
                          ELSE '' 
                          END AS KABAPOL,
                          P.BRANCH || '-' || CAST(COALESCE (P.PRODUCT, 0) AS VARCHAR) || '-' || P.POLICY AS DNUMAPO,
                          CAST(CERT.CERTIF AS VARCHAR) AS DNMCERT,
                          '' AS DTERMO,     --BLANCO
                          COALESCE((
                            SELECT SCOD_VT FROM USINSUG01.EQUI_VT_INX EVI
                            WHERE EVI.SCOD_INX = P.TITULARC
                          ), '') AS KEBENTID_TO,
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
                          COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TEFEACTA,
                          '' AS DULTACTA,--NO
                          '' AS KACCNEMI,--NO
                          '' AS KACARGES,--NO
                          '' AS KACAGENC,--NO
                          '' AS KACPROTO,--NO
                          COALESCE(P.POLITYPE, '') AS KACTIPAP,
                          '' AS DFROTA,  --NO
                          '' AS KACTPDUR,--EN BLANCO
                          COALESCE(P.RENEWAL, '')  AS KACMODRE,
                          '' AS KACMTNRE,--NO
                          '' AS KACTPCOB,--NO
                          COALESCE(P.PAYFREQ, '')  AS KACTPFRC,
                          (
                            CASE P.BUSSITYP  
                              WHEN '2' THEN  '2' 
                              WHEN '1' THEN COALESCE(( SELECT '1' 
                                                      FROM USINSUG01.COINSURAN C
                                                      WHERE C.USERCOMP = 1
                                                      AND C.COMPANY = 1
                                                      AND C.CERTYPE = '2'
                                                      AND C.BRANCH = P.BRANCH
                                                      AND C.POLICY = P.POLICY
                                                      AND C.EFFECDATE <= P.EFFECDATE
                                                      AND ( C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                                      LIMIT 1), '')
                              ELSE ''
                              END                                                       
                          ) AS KACTPCSG,
                          COALESCE(P.REINTYPE,'') AS KACINDRE,
                          '' AS KACCDGER,--NO
                          COALESCE ((
                            SELECT CAST(CURRENCY AS VARCHAR) FROM USINSUG01.CURREN_POL CP 
                            WHERE CP.USERCOMP = P.USERCOMP 
                            AND   CP.COMPANY = P.COMPANY 
                            AND   CP.CERTYPE = P.CERTYPE
                            AND   CP.BRANCH  = P.BRANCH
                            AND   CP.POLICY  = P.POLICY
                            AND   CP.CERTIF  = CERT.CERTIF 
                            AND   CP.EFFECDATE <= P.EFFECDATE 
                            AND   ( CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE) LIMIT 1
                          ), '0') AS KACMOEDA,
                          COALESCE((
                            SELECT COALESCE(E.EXCHANGE, 0)
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
                                                    AND (CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE) limit 1
                                                )
                              AND E.EFFECDATE <= P.EFFECDATE
                              AND (E.NULLDATE IS NULL OR E.NULLDATE > P.EFFECDATE)
                          ), 0) AS VCAMBIO,
                          '' AS KACREGCB,  --NO
                          '' AS KCBMED_DRA,--NO
                          '' AS KCBMED_CB, --NO
                          COALESCE((
                            SELECT COALESCE(TRUNC(DX.PERCENT, 4), 0) 
                              FROM USINSUG01.DISC_XPREM DX 
                              JOIN USINSUG01.DISCO_EXPR DE 
                                ON  DX.USERCOMP = DE.USERCOMP 
                                AND DX.COMPANY = DE.COMPANY
                                AND DX.CERTYPE = dx.certype 
                                AND DX.BRANCH    = DE.BRANCH
                                AND DX.CODE = DE.DISEXPRC
                              WHERE DX.usercomp = p.usercomp
                                and dx.company = p.company
                                and dx.branch  = p.branch
                                and DE.PRODUCT = P.PRODUCT
                                AND DX.POLICY  = P.POLICY
                                AND DX.CERTIF  = CERT.CERTIF 
                                AND DX.EFFECDATE <= P.EFFECDATE
                                AND (DX.NULLDATE IS NULL OR DX.NULLDATE > P.EFFECDATE)
                                AND DE.BILL_ITEM = 4
                                and de.currency = (select cp.currency from USINSUG01.curren_pol cp
                                                    where cp.usercomp = p.usercomp 
                                                      and cp.company = p.company
                                                      and cp.certype = p.certype
                                                      and cp.branch = p.branch
                                                      and cp."policy" = p."policy"
                                                      and cp.certif = cert.certif
                                                      and cp.effecdate <= p.effecdate
                                                      and (cp.nulldate is null or cp.nulldate > p.effecdate) limit 1
                                                  ) limit 1
                          ), 0) AS VTXCOMCB, 
                          '' AS VMTCOMCB,  --EN BLANCO
                          '' AS KCBMED_PD, --NO
                          COALESCE (COALESCE(
                                  (SELECT COALESCE(PERCENT, 0)
                                  FROM USINSUG01.COMMISSION C 
                                  WHERE C.USERCOMP = P.USERCOMP 
                                  AND   C.COMPANY  = P.COMPANY 
                                  AND   C.CERTYPE  = P.CERTYPE
                                  AND   C.BRANCH   = P.BRANCH
                                  AND   C.POLICY   = P.POLICY 
                                  AND   C.CERTIF   = CERT.CERTIF 
                                  AND   C.EFFECDATE <= P.EFFECDATE
                                  AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                  AND   C.ROLE <> 1 LIMIT 1),
                                (SELECT COALESCE(PERCENT, 0) 
                                  FROM USINSUG01.COMMISSION C 
                                  WHERE C.USERCOMP = P.USERCOMP 
                                  AND   C.COMPANY  = P.COMPANY 
                                  AND   C.CERTYPE  = P.CERTYPE
                                  AND   C.BRANCH   = P.BRANCH
                                  AND   C.POLICY   = P.POLICY 
                                  AND   C.CERTIF   = 0 
                                  AND   C.EFFECDATE <= P.EFFECDATE
                                  AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                  AND   C.ROLE <> 1 LIMIT 1)
                          ), 0) AS VTXCOMMD,
                          COALESCE (COALESCE(
                                  (SELECT COALESCE(C.AMOUNT, 0) 
                                  FROM USINSUG01.COMMISSION C 
                                  WHERE C.USERCOMP = P.USERCOMP 
                                  AND   C.COMPANY  = P.COMPANY 
                                  AND   C.CERTYPE  = P.CERTYPE
                                  AND   C.BRANCH   = P.BRANCH
                                  AND   C.POLICY   = P.POLICY 
                                  AND   C.CERTIF   = CERT.CERTIF 
                                  AND   C.EFFECDATE <= P.EFFECDATE
                                  AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                  AND   C.ROLE <> 1 LIMIT 1),
                                (SELECT COALESCE(C.AMOUNT, 0) 
                                  FROM USINSUG01.COMMISSION C 
                                  WHERE C.USERCOMP = P.USERCOMP 
                                  AND   C.COMPANY  = P.COMPANY
                                  AND   C.CERTYPE  = P.CERTYPE
                                  AND   C.BRANCH   = P.BRANCH
                                  AND   C.POLICY   = P.POLICY 
                                  AND   C.CERTIF   = 0 
                                  AND   C.EFFECDATE <= P.EFFECDATE
                                  AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                  AND   C.ROLE <> 1 LIMIT 1) 
                          ), 0) AS VMTCOMMD, 
                          '' AS KCBMED_P2, --NO
                          '' AS VTXCOMME,  --NO
                          '' AS VMTCOMME,  --NO
                          COALESCE((
                          SELECT SUM(COALESCE(C.CAPITAL, 0)) FROM USINSUG01.COVER C 
                          WHERE C.USERCOMP = P.USERCOMP 
                          AND   C.COMPANY  = P.COMPANY 
                          AND   C.CERTYPE  = P.CERTYPE
                          AND   C.BRANCH   = P.BRANCH
                          AND   C.POLICY   = P.POLICY 
                          AND   C.CERTIF   = CERT.CERTIF 
                          AND   C.EFFECDATE <= P.EFFECDATE
                          AND   (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                          ), 0) AS VCAPITAL,
                          '' AS VMTPRMSP, --NO
                          COALESCE(P.PREMIUM, 0) AS VMTCOMR,
                          COALESCE ((
                            SELECT COALESCE (C.SHARE, 0) * COALESCE (P.PREMIUM, 0)
                            FROM USINSUG01.COINSURAN C
                            WHERE C.USERCOMP = 1
                            AND C.COMPANY = 1 -- VALOR DE LA COMPANIA
                            AND C.CERTYPE = '2'
                            AND C.BRANCH = P.BRANCH 
                            AND C.POLICY = P.POLICY
                            AND C.EFFECDATE <= P.EFFECDATE
                            AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                            AND C.companyc = 1
                          ), 0)AS VMTCMNQP,
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
                          COALESCE(P.PREMIUM, 0) AS VMTPRMBR,
                          COALESCE((
                            SELECT COALESCE(C.SHARE, 0)
                            FROM USINSUG01.COINSURAN C
                            WHERE C.USERCOMP = 1
                            AND C.COMPANY = 1 -- VALOR DE LA COMPANIA
                            AND C.CERTYPE = '2'
                            AND C.BRANCH  = P.BRANCH 
                            AND C.POLICY  = P.POLICY
                            AND C.EFFECDATE <= P.EFFECDATE
                            AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                            AND C.companyc = 1
                          ), 0) AS VTXCOSSG,
                          COALESCE ((    
                            SELECT (TRUNC(COALESCE(SHARE, 0),4) /100)
                            FROM USINSUG01.REINSURAN R
                            WHERE R.USERCOMP = 1
                            AND R.COMPANY = 1
                            AND R.CERTYPE =  '2'
                            AND R.BRANCH = P.BRANCH
                            AND R.POLICY = P.POLICY
                            AND R.CERTIF = CERT.CERTIF
                            AND EFFECDATE <= P.EFFECDATE
                            AND (R.NULLDATE IS NULL OR R.NULLDATE > P.EFFECDATE)
                            AND R.TYPE = 1
                          ), 0) AS VTXRETEN,
                          COALESCE(((SELECT (TRUNC(COALESCE(SHARE, 0),4)/100)
                            FROM USINSUG01.REINSURAN R
                            WHERE R.USERCOMP = 1
                            AND R.COMPANY = 1
                            AND R.CERTYPE =  '2'
                            AND R.BRANCH = P.BRANCH
                            AND R.POLICY = P.POLICY
                            AND R.CERTIF = CERT.CERTIF
                            AND EFFECDATE <= P.EFFECDATE
                            AND (R.NULLDATE IS NULL OR R.NULLDATE > P.EFFECDATE)
                            AND R.TYPE = 1) *
                          (SELECT SUM(COALESCE (CAPITAL, 0))
                            FROM USINSUG01.COVER C
                            WHERE C.USERCOMP = 1
                            AND C.COMPANY = 1
                            AND C.CERTYPE =  '2'
                            AND C.BRANCH = P.BRANCH
                            AND C.POLICY = P.POLICY
                            AND C.CERTIF = CERT.CERTIF
                            AND EFFECDATE <= P.EFFECDATE
                            AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)	
                          )), 0) AS VMTCAPRE,
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
                          '' AS KACPGPRE,    --EN BLANCO
                          '' AS TDPGPRE,     --EN BLANCO
                          '' AS TINIPGPR,    --EN BLANCO
                          '' AS TFIMPGPR,    --EN BLANCO
                          '' AS KABAPOL_ESQ, --NO
                          '' AS KABAPOL_EFT, --NO
                          '' AS DSUBSCR,     --NO
                          '' AS DNUAPLI,     --NO
                          '' AS DINDINIB,    --EN BLANCO
                          '' AS DLOCRECB,    --NO
                          '' AS KACCLCLI,    --NO
                          COALESCE(CAST(P.NULLCODE AS VARCHAR), '') AS KACMTALT,
                          COALESCE((
                            SELECT CAST(COALESCE(PH.TYPE, 0) AS VARCHAR) FROM 
                              USINSUG01.POLICY_HIS PH
                              WHERE PH.USERCOMP = P.USERCOMP
                              AND   PH.COMPANY  = P.COMPANY
                              AND   PH.CERTYPE  = P.CERTYPE
                              AND   PH.BRANCH   = P.BRANCH
                              AND   PH.POLICY   = P.POLICY
                              AND   PH.CERTIF   = CERT.CERTIF
                              AND   PH.EFFECDATE <= P.EFFECDATE
                              AND  (PH.NULLDATE IS NULL OR PH.NULLDATE > P.EFFECDATE)
                              AND   PH.TYPE = 1
                          ), '0') AS KACTPTRA,  
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
                          '' AS KACTPREAP,    --EN BLANCO
                          '' AS DENTCONGE,    --NO
                          '' AS KCBMED_PARCE, --NO
                          '' AS DCODPARC,  	--NO
                          '' AS DMODPARC,  	--NO
                          COALESCE(P.BUSSITYP, '') AS DTIPSEG,
                          '' AS KACTPNEG,  	--NO
                          '' AS DURPAGAPO,    --EN BLANCO
                          '' AS DNMINTERP, 	--NO
                          '' AS DNUMADES,  	--NO
                          '' AS KACTPPRD,  	--ACLARAR
                          '' AS KACSBTPRD, 	--ACLARAR
                          '' AS KABPRODT_REL, --NO
                          '' AS KACTPPARES, 	--NO
                          '' AS KACTIPIFAP, 	   --NO
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
                          AND CERT.POLICY  = P.POLICY 
                          AND CERT.PRODUCT = P.PRODUCT
                          WHERE P.CERTYPE = '2' AND P.COMPDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' limit 100)

                          UNION ALL

                          (SELECT 
                            'D' AS INDDETREC, 
                            'ABAPOL' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,      --NO
                            '' AS TIOCPROC,    --NO
                            COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TIOCFRM,
                            '' AS TIOCTO,      --NO
                            'PIV' AS KGIORIGM, --NO
                            'LPV' AS KACCOMPA, --NO
                            CAST(P.BRANCH AS VARCHAR) AS KGCRAMO,
                            CAST(P.BRANCH AS VARCHAR) || '-' || CAST(P.PRODUCT AS VARCHAR) AS KABPRODT,
                            CASE P.POLITYPE
                            WHEN '2' THEN CASE WHEN CERT.CERTIF <> 0 THEN P.BRANCH || '-' || P.PRODUCT || '-' || P.POLICY || '-' || '0'
                                          ELSE ''
                                          END
                            ELSE '' 
                            END AS KABAPOL,
                            P.BRANCH || '-' || P.PRODUCT || '-' || P.POLICY AS DNUMAPO,
                            CAST(CERT.CERTIF AS VARCHAR) AS DNMCERT,
                            '' AS DTERMO,
                            COALESCE((
                              SELECT SCOD_VT FROM USINSUG01.EQUI_VT_INX EVI
                              WHERE EVI.SCOD_INX = P.TITULARC
                            ), '')
                            AS KEBENTID_TO,
                            COALESCE(CAST(P.DATE_ORIGI AS VARCHAR), '') AS TCRIAPO,
                            COALESCE(CAST(P.ISSUEDAT AS VARCHAR), '') AS TEMISSAO,
                            COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TINICIO,
                            '' AS DHORAINI, --NO
                            COALESCE(CAST(P.EXPIRDAT  AS VARCHAR), '') AS TTERMO,
                            COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TINIANU,
                            COALESCE(CAST(P.EXPIRDAT  AS VARCHAR), '') AS TVENANU,
                            COALESCE(CAST(P.NULLDATE  AS VARCHAR), '') AS TANSUSP,
                            '' AS TESTADO, --EN BLANCO
                            COALESCE(P.STATUS_POL, '') AS KACESTAP,
                            '' AS KACMOEST, --NO
                            COALESCE(CAST(P.EFFECDATE AS VARCHAR), '') AS TEFEACTA,
                            '' AS DULTACTA,--NO
                            '' AS KACCNEMI,--NO
                            '' AS KACARGES,--NO
                            '' AS KACAGENC,--NO
                            '' AS KACPROTO,--NO
                            COALESCE(P.POLITYPE, '') AS KACTIPAP,
                            '' AS DFROTA,  --NO
                            '' AS KACTPDUR,--EN BLANCO
                            COALESCE(P.RENEWAL, '') AS KACMODRE,
                            '' AS KACMTNRE,--NO
                            '' AS KACTPCOB,--NO
                            COALESCE(P.PAYFREQ, '') AS KACTPFRC,
                            CASE P.BUSSITYP
                            WHEN '2' THEN  '2' 
                            WHEN '1' THEN COALESCE(( SELECT '1' 
                                                    FROM USINSUV01.COINSURAN C
                                                    WHERE C.USERCOMP = 1
                                                    AND C.COMPANY = 1
                                                    AND C.CERTYPE = '2'
                                                    AND C.BRANCH = P.BRANCH
                                                    AND C.POLICY = P.POLICY
                                                    AND C.EFFECDATE <= P.EFFECDATE
                                                    AND ( C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE) LIMIT 1),'')
                            ELSE ''
                            END KACTPCSG,
                            COALESCE(P.REINTYPE, '') AS KACINDRE,
                            '' AS KACCDGER,--NO
                            COALESCE((
                              SELECT CAST(CURRENCY AS VARCHAR) FROM USINSUV01.CURREN_POL CP 
                              WHERE CP.USERCOMP = P.USERCOMP 
                              AND   CP.COMPANY = P.COMPANY 
                              AND   CP.CERTYPE = P.CERTYPE
                              AND   CP.BRANCH  = P.BRANCH
                              AND   CP.POLICY  = P.POLICY
                              AND   CP.CERTIF  = CERT.CERTIF 
                              AND   CP.EFFECDATE <= P.EFFECDATE 
                              AND   (CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE)
                            ), '0') AS KACMOEDA,
                            (
                              SELECT COALESCE(EXCHANGE, 0)
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
                                                      AND (CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE) limit 1
                                                  )
                                AND E.EFFECDATE <= P.EFFECDATE
                                AND (E.NULLDATE IS NULL OR E.NULLDATE > P.EFFECDATE)
                            ) AS VCAMBIO,
                            '' AS KACREGCB,  --NO
                            '' AS KCBMED_DRA,--NO
                            '' AS KCBMED_CB, --NO
                            (
                              COALESCE((
                              SELECT COALESCE(TRUNC(DX.PERCENT, 4), 0) 
                              FROM USINSUV01.DISC_XPREM DX 
                              JOIN USINSUV01.DISCO_EXPR DE 
                                ON  DX.USERCOMP = DE.USERCOMP 
                                AND DX.COMPANY = DE.COMPANY
                                AND DX.CERTYPE = dx.certype 
                                AND DX.BRANCH    = DE.BRANCH
                                AND DX.CODE = DE.DISEXPRC
                              WHERE DX.usercomp = p.usercomp
                                and dx.company = p.company
                                and dx.branch  = p.branch
                                and DE.PRODUCT = P.PRODUCT
                                AND DX.POLICY  = P.POLICY
                                AND DX.CERTIF  = CERT.CERTIF 
                                AND DX.EFFECDATE <= P.EFFECDATE
                                AND (DX.NULLDATE IS NULL OR DX.NULLDATE > P.EFFECDATE)
                                AND DE.BILL_ITEM = 4
                                and de.currency = (select cp.currency from usinsuv01.curren_pol cp
                                                    where cp.usercomp = p.usercomp 
                                                      and cp.company = p.company
                                                      and cp.certype = p.certype
                                                      and cp.branch = p.branch
                                                      and cp."policy" = p."policy"
                                                      and cp.certif = cert.certif
                                                      and cp.effecdate <= p.effecdate
                                                      and (cp.nulldate is null or cp.nulldate > p.effecdate) limit 1
                                                  ) limit 1
                            ), 0)
                            ) AS VTXCOMCB,  --ACLARAR  
                            '' AS VMTCOMCB, --EN BLANCO
                            '' AS KCBMED_PD,--NO
                            COALESCE(COALESCE(
                                    (SELECT COALESCE(PERCENT, 0) 
                                    FROM USINSUV01.COMMISSION C 
                                    WHERE C.USERCOMP = P.USERCOMP 
                                    AND   C.COMPANY  = P.COMPANY 
                                    AND   C.CERTYPE  = P.CERTYPE
                                    AND   C.BRANCH   = P.BRANCH
                                    AND   C.POLICY   = P.POLICY 
                                    AND   C.CERTIF   = CERT.CERTIF 
                                    AND   C.EFFECDATE <= P.EFFECDATE
                                    AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                    AND   C.ROLE <> 1 LIMIT 1),
                                  (SELECT COALESCE(PERCENT, 0) 
                                    FROM USINSUV01.COMMISSION C 
                                    WHERE C.USERCOMP = P.USERCOMP 
                                    AND   C.COMPANY  = P.COMPANY 
                                    AND   C.CERTYPE  = P.CERTYPE
                                    AND   C.BRANCH   = P.BRANCH
                                    AND   C.POLICY   = P.POLICY 
                                    AND   C.CERTIF   = 0 
                                    AND   C.EFFECDATE <= P.EFFECDATE
                                    AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                    AND   C.ROLE <> 1 LIMIT 1)
                            ), 0) AS VTXCOMMD,
                            COALESCE(COALESCE(
                                    (SELECT COALESCE(AMOUNT, 0) 
                                    FROM USINSUV01.COMMISSION C 
                                    WHERE C.USERCOMP = P.USERCOMP 
                                    AND   C.COMPANY  = P.COMPANY 
                                    AND   C.CERTYPE  = P.CERTYPE
                                    AND   C.BRANCH   = P.BRANCH
                                    AND   C.POLICY   = P.POLICY 
                                    AND   C.CERTIF   = CERT.CERTIF 
                                    AND   C.EFFECDATE <= P.EFFECDATE
                                    AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                    AND   C.ROLE <> 1 LIMIT 1),
                                  (SELECT COALESCE(AMOUNT, 0) 
                                    FROM USINSUV01.COMMISSION C 
                                    WHERE C.USERCOMP = P.USERCOMP 
                                    AND   C.COMPANY  = P.COMPANY 
                                    AND   C.CERTYPE  = P.CERTYPE
                                    AND   C.BRANCH   = P.BRANCH
                                    AND   C.POLICY   = P.POLICY 
                                    AND   C.CERTIF   = 0 
                                    AND   C.EFFECDATE <= P.EFFECDATE
                                    AND  (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                    AND   C.ROLE <> 1 LIMIT 1)
                            ), 0) AS VMTCOMMD,
                            '' AS KCBMED_P2, --NO
                            '' AS VTXCOMME,  --NO
                            '' AS VMTCOMME,  --NO
                            (
                              SELECT SUM(COALESCE(C.CAPITAL,0)) FROM USINSUV01.COVER C 
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
                            COALESCE(P.PREMIUM, 0)  AS VMTCOMR,
                            (
                              SELECT COALESCE(SHARE, 0) * COALESCE(P.PREMIUM, 0)
                              FROM USINSUV01.COINSURAN C
                              WHERE C.USERCOMP = 1
                              AND C.COMPANY = 1 -- VALOR DE LA COMPAÃƒâ€˜IA
                              AND C.CERTYPE = '2'
                              AND C.BRANCH = P.BRANCH 
                              AND C.POLICY = P.POLICY
                              AND C.EFFECDATE <= P.EFFECDATE
                              AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                              AND   C.COMPANYC = 12 --COMPAÃƒâ€˜IA LPV
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
                            COALESCE(P.PREMIUM, 0)  AS VMTPRMBR,
                            COALESCE((
                              SELECT COALESCE(SHARE, 0)
                              FROM USINSUV01.COINSURAN C
                              WHERE C.USERCOMP = 1
                              AND C.COMPANY = 1 -- VALOR DE LA COMPAÃƒâ€˜IA
                              AND C.CERTYPE = '2'
                              AND C.BRANCH  = P.BRANCH 
                              AND C.POLICY  = P.POLICY
                              AND C.EFFECDATE <= P.EFFECDATE
                              AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                              AND C.COMPANYC = 12
                            ), 0) AS VTXCOSSG,
                            0 AS VTXRETEN, --EN BLANCO
                            COALESCE((
                                (SELECT (TRUNC(COALESCE(SHARE, 0),4)/100)
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
                              (SELECT SUM(COALESCE(CAPITAL, 0)) 
                              FROM USINSUV01.COVER C
                              WHERE C.USERCOMP = 1
                              AND C.COMPANY = 1
                              AND C.CERTYPE =  '2'
                              AND C.BRANCH = P.BRANCH
                              AND C.POLICY = P.POLICY
                              AND C.CERTIF = CERT.CERTIF
                              AND EFFECDATE <= P.EFFECDATE
                              AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                              )
                            ), 0) AS VMTCAPRE,
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
                            '' AS DINDINIB, --EN BLANCO
                            '' AS DLOCRECB,--NO
                            '' AS KACCLCLI,--NO
                            COALESCE(CAST(P.NULLCODE AS VARCHAR), '') AS KACMTALT,
                            (
                              SELECT CAST(PH.TYPE AS VARCHAR) FROM 
                              USINSUV01.POLICY_HIS PH
                              WHERE PH.USERCOMP = P.USERCOMP
                              AND   PH.COMPANY  = P.COMPANY
                              AND   PH.CERTYPE  = P.CERTYPE
                              AND   PH.BRANCH   = P.BRANCH
                              AND   PH.POLICY   = P.POLICY
                              AND   PH.CERTIF   = CERT.CERTIF
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
                            '' AS KACTPREAP,    --EN BLANCO
                            '' AS DENTCONGE,    --NO
                            '' AS KCBMED_PARCE, --NO
                            '' AS DCODPARC,  	--NO
                            '' AS DMODPARC,  	--NO
                            P.BUSSITYP AS DTIPSEG,
                            '' AS KACTPNEG,  	--NO
                            '' AS DURPAGAPO,    --EN BLANCO
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
                            '' AS TEMISREN		   --NO*/
                            FROM USINSUV01.POLICY P 
                            LEFT JOIN USINSUV01.CERTIFICAT CERT 
                            ON  CERT.USERCOMP = P.USERCOMP 
                            AND CERT.COMPANY = P.COMPANY   
                            AND CERT.CERTYPE = P.CERTYPE 
                            AND CERT.BRANCH  = P.BRANCH
                            AND CERT.POLICY  = P.POLICY
                            AND CERT.PRODUCT = P.PRODUCT
                          WHERE P.CERTYPE = '2' AND P.COMPDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' limit 100)
                          ) as TMP
                          '''
   #Ejecutar consulta
   L_DF_POLIZAS_INSUNIX = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_POLIZAS_INSUNIX).load()
   print('USINSUV01 exitoso')

    #------------------------------------------------------------------------------------------------------------------#

   #Declara consulta INSIS
   L_POLIZAS_INSIS = f'''
                     (SELECT
                      'D' AS INDDETREC, 
                      'ABAPOL' AS TABLAIFRS17,
                      '' AS PK,
                      '' AS DTPREG,   --NO
                      '' AS TIOCPROC, --NO
                      CAST(CAST(P."INSR_BEGIN" AS DATE) AS VARCHAR) AS TIOCFRM, --CAMBIO
                      '' AS TIOCTO,   --NO
                      'PNV' AS KGIORIGM,
                      'LPV' AS KACCOMPA,
                      COALESCE(P."ATTR1", '') AS KGCRAMO,
                      COALESCE(P."ATTR1", '') || '-' || COALESCE(P."ATTR2", '') AS KABPRODT,
                      CASE COALESCE(PP."ENG_POL_TYPE", '')
                      WHEN 'DEPENDENT' THEN P."ATTR1" || '-' || P."ATTR2" || '-' || P."POLICY_NO" || '-' || PP."MASTER_POLICY_ID"
                      ELSE ''
                      END KABAPOL,
                      COALESCE(P."ATTR1", '')  || '-' || COALESCE(P."ATTR2", '') || '-' || COALESCE(P."POLICY_NO", '') AS DNUMAPO,
                      CAST(P."POLICY_ID" AS VARCHAR) AS DNMCERT,
                      '' AS DTERMO, --EN BLANCO
                      COALESCE((
                                SELECT ILPI."LEGACY_ID" FROM
                                USINSIV01."INTRF_LPV_PEOPLE_IDS" ILPI
                                WHERE ILPI."MAN_ID" = PC."MAN_ID"), '') AS KEBENTID_TO,
                      CAST(CAST(P."REGISTRATION_DATE" AS DATE) AS VARCHAR) AS TCRIAPO,
                      CAST(CAST(P."DATE_GIVEN" AS DATE) AS VARCHAR) AS TEMISSAO,
                      CAST(CAST(P."INSR_BEGIN" AS DATE) AS VARCHAR) AS TINICIO,
                      '' AS DHORAINI, --NO
                      CAST(CAST(P."INSR_END" AS DATE) AS VARCHAR) AS TTERMO,
                      CAST(CAST(P."INSR_BEGIN" AS DATE) AS VARCHAR) AS TINIANU,
                      CAST(CAST(P."INSR_END" AS DATE) AS VARCHAR)   AS TVENANU,
                      ( 
                        SELECT CAST(CAST(GA."INSR_BEGIN" AS DATE) AS VARCHAR)
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
                      ) AS TANSUSP,
                      '' AS TESTADO, --EN BLANCO
                      CAST(P."POLICY_STATE" AS VARCHAR) AS  KACESTAP,
                      '' AS KACMOEST, --NO
                      CAST(CAST(P."INSR_BEGIN" AS DATE) AS VARCHAR) AS TEFEACTA, --CERRADO PERO NO ESPECIFICA
                      '' AS DULTACTA, --NO
                      '' AS KACCNEMI, --NO
                      '' AS KACARGES, --NO
                      '' AS KACAGENC, --NO
                      '' AS KACPROTO, --NO
                      CASE PP."ENG_POL_TYPE"
                      WHEN 'REGULAR'   THEN 'INDIVIDUAL'
                      WHEN 'MASTER'    THEN 'COLECTIVA'
                      WHEN 'DEPENDENT' THEN 'COLECTIVA'
                      ELSE ''
                      END AS KACTIPAP,
                      '' AS DFROTA,   --NO
                      '' AS KACTPDUR, --EN BLANCO
                      COALESCE(P."RENEWABLE_FLAG", '') AS KACMODRE,
                      '' AS KACMTNRE, --NO
                      '' AS KACTPCOB, --NO
                      COALESCE(P."ATTR5", '') AS KACTPFRC,
                      /*
                      COALESCE(
                      (SELECT '1' FROM USINSIV01."RI_FAC" RF
                      WHERE RF."POLICY_ID" = P."POLICY_ID"  
                      AND RF."FAC_TYPE" = 'COINS'
                      LIMIT 1), '') AS KACTPCSG, 
                      */
                      '' AS KACTPCSG,
                      /*(
                        SELECT * FROM USINSIV01."RI_CEDED_PREMIUNS"
                      ) AS KACINDRE,*/
                      '' AS KACINDRE,
                      '' AS KACCDGER, --NO
                      CASE COALESCE((SELECT DISTINCT COALESCE(IO."AV_CURRENCY", '')
                                    FROM USINSIV01."INSURED_OBJECT" IO 
                                    WHERE IO."POLICY_ID" = P."POLICY_ID" LIMIT 1 ),'') 
                      WHEN 'USD' THEN '2'
                      WHEN 'PEN' THEN '1'
                      ELSE '0'
                      END KACMOEDA,
                      0 AS VCAMBIO,     --EN BLANCO
                      '' AS KACREGCB,   --NO
                      '' AS KCBMED_DRA, --NO
                      '' AS KCBMED_CB,  --NO
                      0 AS VTXCOMCB,    --EN BLANCO  
                      '' AS VMTCOMCB,   --EN BLANCO
                      '' AS KCBMED_PD,  --NO  
                      0 AS VTXCOMMD,    --PENDIENTE CONSULTA  
                      0 AS VMTCOMMD,    --EN BLANCO
                      '' AS KCBMED_P2,  --NO
                      '' AS VTXCOMME,   --NO
                      '' AS VMTCOMME,   --NO
                      0 AS VCAPITAL,    --PENDIENTE CONSULTA  
                      '' AS VMTPRMSP,   --NO
                      0 AS VMTCOMR,     --EN BLANCO
                      0 AS VMTCMNQP,    --PENDIENTE CONSULTA  
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
                      0 AS VMTPRMBR,   --PENDIENTE CONSULTA  
                      0 AS VTXCOSSG,   --PENDIENTE CONSULTA  
                      0 AS VTXRETEN,   --PENDIENTE CONSULTA  
                      0 AS VMTCAPRE,   --PENDIENTE CONSULTA  
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
                      '' AS DINDINIB,    --EN BLANCO
                      '' AS DLOCRECB,    --NO
                      '' AS KACCLCLI,    --NO
                      '' AS KACMTALT,    --PENDIENTE CONSULTA   CONSULTA  
                      '' AS KACTPTRA,    --EN BLANCO
                      '' AS TEMICANC,    --NO 
                      '' AS DENTIDSO,    --NO
                      '' AS DARQUIVO,    --NO
                      '' AS TARQUIVO,    --NO
                      CASE PP."ENG_POL_TYPE"
                      WHEN 'REGULAR'   THEN 'INDIVIDUAL'
                      WHEN 'MASTER'    THEN 'COLECTIVA'
                      WHEN 'DEPENDENT' THEN 'COLECTIVA'
                      ELSE ''
                      END AS KACTPSUB,
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
                        CAST(COALESCE((  SELECT COUNT(DISTINCT PAR."MAN_ID")
                                    FROM USINSIV01."POLICY_PARTICIPANTS" PAR
                                        WHERE   PAR."POLICY_ID" = P."POLICY_ID"
                                        AND     PAR."PARTICPANT_ROLE" = 'PHOLDER'
                                        AND     PAR."ANNEX_ID" = 0
                                        AND     DATE_TRUNC('SECOND', PAR."VALID_FROM") <= CAST(P."INSR_BEGIN" AS DATE)
                                        AND     (PAR."VALID_TO" IS NULL OR DATE_TRUNC('SECOND', PAR."VALID_TO") >= CAST(P."INSR_BEGIN" AS DATE))),
                                      (  SELECT  COUNT(DISTINCT PAR."MAN_ID") 
                                        FROM    USINSIV01."POLICY_PARTICIPANTS" PAR
                                        WHERE   PAR."POLICY_ID" = P."POLICY_ID"
                                        AND     PAR."PARTICPANT_ROLE" = 'PHOLDER'
                                        AND     DATE_TRUNC('SECOND', PAR."VALID_FROM") <= CAST(P."INSR_BEGIN" AS DATE)
                                        AND     (PAR."VALID_TO" IS NULL OR DATE_TRUNC('SECOND', PAR."VALID_TO") >= CAST(P."INSR_BEGIN" AS DATE)))) 
                        AS VARCHAR)
                      ) AS DQTDPART,
                      '' AS TINIPRXANU,
                      '' AS KACTPREAP,
                      '' AS DENTCONGE,    --NO
                      '' AS KCBMED_PARCE, --NO
                      '' AS DCODPARC,     --NO
                      '' AS DMODPARC,     --NO
                      '' AS DTIPSEG,  
                      '' AS KACTPNEG,     --NO
                      '' AS DURPAGAPO,
                      '' AS DNMINTERP,    --NO
                      '' AS DNUMADES,     --NO
                      COALESCE(CAST(P."INSR_TYPE" AS VARCHAR), '') AS KACTPPRD,
                      (	
                          SELECT PC."COND_VALUE" FROM USINSIV01."POLICY_CONDITIONS" PC
                          WHERE "COND_TYPE" LIKE '%AS_IS%'
                          AND PC."POLICY_ID" = P."POLICY_ID" 	
                          AND PC."INSR_TYPE" = P."INSR_TYPE"	
                          AND PC."ANNEX_ID"  = (SELECT MAX("ANNEX_ID") FROM USINSIV01."POLICY_CONDITIONS" PC 	  
                                                WHERE "COND_TYPE" LIKE '%AS_IS%'	     				  
                                                AND PC."POLICY_ID" = P."POLICY_ID" 	    				  
                                                AND PC."INSR_TYPE" = P."INSR_TYPE" )                   	 
                      ) AS KACSBTPRD,
                      '' AS KABPRODT_REL,   --NO
                      '' AS KACTPPARES,     --NO
                      '' AS KACTIPIFAP,     --NO
                      '' AS KACTPALT_IFRS17,--NO
                      '' AS TEFEALTE,       --NO
                      '' AS TINITARLTA,     --NO
                      '' AS TFIMTARLTA,     --NO
                      '' AS DTERMO_IFRS17,  --NO
                      '' AS TEMISREN        --NO
                      FROM USINSIV01."POLICY" P 
                      LEFT JOIN USINSIV01."P_CLIENTS" PC ON P."CLIENT_ID" = PC."CLIENT_ID"
                      LEFT JOIN USINSIV01."POLICY_ENG_POLICIES" PP ON P."POLICY_ID" = PP."POLICY_ID"
                        WHERE P."REGISTRATION_DATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' limit 100) AS TMP 
                     '''

   #Ejecutar consulta
   L_DF_POLIZAS_INSIS = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_POLIZAS_INSIS).load()

   print('EXITOSO USINSIV01')

   #Perform the union operation
   L_DF_POLIZAS = L_DF_POLIZAS_VTIME.union(L_DF_POLIZAS_INSIS).union(L_DF_POLIZAS_INSUNIX)

   L_DF_POLIZAS = L_DF_POLIZAS.withColumn("VCAMBIO", col("VCAMBIO").cast(DecimalType(7, 4))).withColumn("VTXCOMCB", col("VTXCOMCB").cast(DecimalType(7, 4))).withColumn("VMTCOMCB", col("VMTCOMCB").cast(DecimalType(12, 2))).withColumn("VTXCOMMD", col("VTXCOMMD").cast(DecimalType(7, 4))).withColumn("VMTCOMMD", col("VMTCOMMD").cast(DecimalType(12, 2))).withColumn("VCAPITAL", col("VCAPITAL").cast(DecimalType(14, 2))).withColumn("VMTCOMR", col("VMTCOMR").cast(DecimalType(12, 2))).withColumn("VMTCMNQP", col("VMTCMNQP").cast(DecimalType(12, 2))).withColumn("VMTPRMBR", col("VMTPRMBR").cast(DecimalType(12, 2))).withColumn("VTXCOSSG", col("VTXCOSSG").cast(DecimalType(7, 4))).withColumn("VTXRETEN", col("VTXRETEN").cast(DecimalType(7, 4))).withColumn("VMTCAPRE", col("VMTCAPRE").cast(DecimalType(12, 2))).withColumn("DQTDPART", col("DQTDPART").cast(DecimalType(5, 0)))

   return L_DF_POLIZAS