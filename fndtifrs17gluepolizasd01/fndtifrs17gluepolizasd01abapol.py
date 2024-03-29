from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, coalesce, lit

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):
  
  l_fecha_carga_inicial = '2021-12-31'

  #Declara consulta VTIME
  l_polizas_vtime_lpg = f'''
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
                         COALESCE(CASE
                                 WHEN P."SPOLITYPE" = '1' THEN P."SCLIENT"
                                 ELSE CERT."SCLIENT"
                                 END, '') AS KEBENTID_TO,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DDATE_ORIGI"
                                             ELSE CERT."DDATE_ORIGI" 
                                             END) AS DATE) AS VARCHAR), '') AS TCRIAPO,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DISSUEDAT"
                                             ELSE CERT."DISSUEDAT"
                                             END) AS DATE) AS VARCHAR), '') AS TEMISSAO,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DDATE_ORIGI"
                                             ELSE CERT."DDATE_ORIGI"
                                             END) AS DATE) AS VARCHAR), '') AS TINICIO, '' AS DHORAINI,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DEXPIRDAT"
                                             ELSE CERT."DEXPIRDAT"
                                             END) AS DATE) AS VARCHAR), '') AS TTERMO,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DSTARTDATE"
                                             ELSE CERT."DSTARTDATE"
                                             END) AS DATE) AS VARCHAR), '') AS TINIANU,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DEXPIRDAT"
                                             ELSE CERT."DEXPIRDAT"
                                             END) AS DATE) AS VARCHAR),
                                             '') AS TVENANU,
                         COALESCE(CAST(CAST((CASE
                                           WHEN P."SPOLITYPE" = '1' THEN P."DNULLDATE"
                                           ELSE CERT."DNULLDATE"
                                           END) AS DATE) AS VARCHAR),'') AS TANSUSP,
                         '' AS TESTADO,              --EN BLANCO
                         COALESCE ((CASE
                                    WHEN P."SPOLITYPE" IN ('2', '3')
                                    AND CERT."NCERTIF" <> 0 THEN CERT."SSTATUSVA"
                                    ELSE P."SSTATUS_POL"END), '') AS KACESTAP,
                         '' AS KACMOEST,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DSTARTDATE"
                                             ELSE CERT."DSTARTDATE"
                                             END) AS DATE) AS VARCHAR), '') AS TEFEACTA,
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
                             LIMIT 1),'0') AS KACMOEDA,
                         COALESCE((SELECT COALESCE((E."NEXCHANGE"), 0)
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
                                   AND (E."DNULLDATE" IS NULL OR E."DNULLDATE" > P."DSTARTDATE")), 0) AS VCAMBIO,
                         '' AS KACREGCB,  --NO
                         '' AS KCBMED_DRA,--NO
                         '' AS KCBMED_CB, --NO
                         COALESCE((SELECT COALESCE (DX."NPERCENT",0)
                                   FROM USVTIMG01."DISC_XPREM" DX
                                   JOIN USVTIMG01."DISCO_EXPR" DE
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
                                   AND DE."NBILL_ITEM" = 4),0) AS VTXCOMCB,  --ACLARAR
                         '' AS VMTCOMCB,   --EN BLANCO
                         '' AS KCBMED_PD,  --NO
                         COALESCE(COALESCE(  --POR CERTIFICADO
                                 (SELECT COALESCE(CO."NPERCENT",0) FROM  USVTIMG01."COMMISSION" CO 
                                     WHERE  CO."SCERTYPE" = '2'
                                     AND    CO."NBRANCH"  = P."NBRANCH"
                                     AND       CO."NPRODUCT" = P."NPRODUCT"     
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
                                     AND    CO."NPRODUCT" = P."NPRODUCT"    
                                     AND    CO."NPOLICY"  = P."NPOLICY"  
                                     AND    CO."NCERTIF"  = 0
                                     AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                                     AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                                     AND    CO."NINTERTYP" <> 1
                                     LIMIT 1)  
                         ),0) AS VTXCOMMD,
                         COALESCE(COALESCE(  --POR CERTIFICADO
                                 (SELECT COALESCE("NAMOUNT", 0) FROM  USVTIMG01."COMMISSION" CO 
                                     WHERE  CO."SCERTYPE" = '2'
                                     AND    CO."NBRANCH"  = P."NBRANCH"
                                     AND       CO."NPRODUCT" = P."NPRODUCT"     
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
                                     AND     CO."NPRODUCT" = P."NPRODUCT"
                                     AND   CO."NPOLICY"  = P."NPOLICY"  
                                     AND   CO."NCERTIF"  = 0
                                     AND   CO."DEFFECDATE" <= P."DSTARTDATE"
                                     AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
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
                                 AND   PH."NTYPE_HIST" = 1 LIMIT 1) --EN EMISION
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
                         WHERE P."SCERTYPE" = '2' 
                         AND P."SSTATUS_POL" NOT IN ('2','3')
                         AND ((P."SPOLITYPE" = '1' -- INDIVIDUAL 
                              AND P."DEXPIRDAT" >= '{l_fecha_carga_inicial}' AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '{l_fecha_carga_inicial}') )
                         OR  (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                              AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}')))
                         AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                        
                        UNION 

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
                         COALESCE(CASE
                                  WHEN P."SPOLITYPE" = '1' THEN P."SCLIENT"
                                  ELSE CERT."SCLIENT"
                                  END, '') AS KEBENTID_TO,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DDATE_ORIGI"
                                             ELSE CERT."DDATE_ORIGI" 
                                             END) AS DATE) AS VARCHAR), '') AS TCRIAPO,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DISSUEDAT"
                                             ELSE CERT."DISSUEDAT"
                                             END) AS DATE) AS VARCHAR), '') AS TEMISSAO,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DDATE_ORIGI"
                                             ELSE CERT."DDATE_ORIGI"
                                             END) AS DATE) AS VARCHAR), '') AS TINICIO, '' AS DHORAINI,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DEXPIRDAT"
                                             ELSE CERT."DEXPIRDAT"
                                             END) AS DATE) AS VARCHAR), '') AS TTERMO,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DSTARTDATE"
                                             ELSE CERT."DSTARTDATE"
                                             END) AS DATE) AS VARCHAR), '') AS TINIANU,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DEXPIRDAT"
                                             ELSE CERT."DEXPIRDAT"
                                             END) AS DATE) AS VARCHAR),
                                             '') AS TVENANU,
                         COALESCE(CAST(CAST((CASE
                                           WHEN P."SPOLITYPE" = '1' THEN P."DNULLDATE"
                                           ELSE CERT."DNULLDATE"
                                           END) AS DATE) AS VARCHAR),'') AS TANSUSP,
                         '' AS TESTADO,              --EN BLANCO
                         COALESCE ((CASE
                                    WHEN P."SPOLITYPE" IN ('2', '3')
                                    AND CERT."NCERTIF" <> 0 THEN CERT."SSTATUSVA"
                                    ELSE P."SSTATUS_POL"END), '') AS KACESTAP,
                         '' AS KACMOEST,
                         COALESCE(CAST(CAST((CASE
                                             WHEN P."SPOLITYPE" = '1' THEN P."DSTARTDATE"
                                             ELSE CERT."DSTARTDATE"
                                             END) AS DATE) AS VARCHAR), '') AS TEFEACTA,
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
                             LIMIT 1),'0') AS KACMOEDA,
                         COALESCE((SELECT COALESCE((E."NEXCHANGE"), 0)
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
                                   AND (E."DNULLDATE" IS NULL OR E."DNULLDATE" > P."DSTARTDATE")), 0) AS VCAMBIO,
                         '' AS KACREGCB,  --NO
                         '' AS KCBMED_DRA,--NO
                         '' AS KCBMED_CB, --NO
                         COALESCE((SELECT COALESCE (DX."NPERCENT",0)
                                   FROM USVTIMG01."DISC_XPREM" DX
                                   JOIN USVTIMG01."DISCO_EXPR" DE
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
                                   AND DE."NBILL_ITEM" = 4),0) AS VTXCOMCB,  --ACLARAR
                         '' AS VMTCOMCB,   --EN BLANCO
                         '' AS KCBMED_PD,  --NO
                         COALESCE(COALESCE(  --POR CERTIFICADO
                                 (SELECT COALESCE(CO."NPERCENT",0) FROM  USVTIMG01."COMMISSION" CO 
                                     WHERE  CO."SCERTYPE" = '2'
                                     AND    CO."NBRANCH"  = P."NBRANCH"
                                     AND       CO."NPRODUCT" = P."NPRODUCT"     
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
                                     AND    CO."NPRODUCT" = P."NPRODUCT"    
                                     AND    CO."NPOLICY"  = P."NPOLICY"  
                                     AND    CO."NCERTIF"  = 0
                                     AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                                     AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                                     AND    CO."NINTERTYP" <> 1
                                     LIMIT 1)  
                         ),0) AS VTXCOMMD,
                         COALESCE(COALESCE(  --POR CERTIFICADO
                                 (SELECT COALESCE("NAMOUNT", 0) FROM  USVTIMG01."COMMISSION" CO 
                                     WHERE  CO."SCERTYPE" = '2'
                                     AND    CO."NBRANCH"  = P."NBRANCH"
                                     AND       CO."NPRODUCT" = P."NPRODUCT"     
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
                                     AND     CO."NPRODUCT" = P."NPRODUCT"
                                     AND   CO."NPOLICY"  = P."NPOLICY"  
                                     AND   CO."NCERTIF"  = 0
                                     AND   CO."DEFFECDATE" <= P."DSTARTDATE"
                                     AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
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
                         WHERE P."SCERTYPE" = '2' 
                         AND P."SSTATUS_POL" NOT IN ('2','3')
                         AND ((P."SPOLITYPE" = '1' AND (P."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR P."DNULLDATE" < '{l_fecha_carga_inicial}')
                             AND (EXISTS (SELECT 1 FROM USVTIMG01."CLAIM" CLA                                           
                                           JOIN (SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMG01."CONDITION_SERV" CS 
                                                 WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                 JOIN USVTIMG01."CLAIM_HIS" CLH 
                                                 ON COALESCE(CLH."NCLAIM", 0) > 0 
                                                 AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                                 AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}') CLH 
                                           ON CLH."NCLAIM" = CLA."NCLAIM"
                                           WHERE CLA."SCERTYPE"  = P."SCERTYPE" 
                                           AND CLA."NBRANCH"  = P."NBRANCH" 
                                           AND CLA."NPRODUCT" = P."NPRODUCT"
                                           AND CLA."NPOLICY"  = P."NPOLICY"  
                                           AND CLA."NCERTIF"  =  0)))
                            OR (P."SPOLITYPE" <> '1' AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}')
                             AND ((EXISTS (SELECT 1 FROM USVTIMG01."CLAIM" CLA                                           
                                           JOIN (SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMG01."CONDITION_SERV" CS 
                                                 WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                 JOIN USVTIMG01."CLAIM_HIS" CLH 
                                                 ON COALESCE(CLH."NCLAIM", 0) > 0 
                                                 AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                                 AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}') CLH 
                                           ON CLH."NCLAIM" = CLA."NCLAIM"
                                           WHERE CLA."SCERTYPE"  = P."SCERTYPE" 
                                           AND CLA."NBRANCH"  = P."NBRANCH" 
                                           AND CLA."NPRODUCT" = P."NPRODUCT"
                                           AND CLA."NPOLICY"  = P."NPOLICY"  
                                           AND CLA."NCERTIF"  =  CERT."NCERTIF"))))))                        
                      ) AS TMP'''

  l_df_polizas_vtime_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_vtime_lpg).load()  

  print("ABAPOL USVTIMG01 EXITOSO")

  l_polizas_vtime_lpv = f'''
                        (
                          (SELECT
                           'D' AS INDDETREC, 
                           'ABAPOL' AS TABLAIFRS17,
                           '' AS PK,
                           '' AS DTPREG,       --NO 
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
                           COALESCE(CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."SCLIENT"
                           ELSE CERT."SCLIENT"
                           END, '') AS KEBENTID_TO,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DDATE_ORIGI"
                           ELSE CERT."DDATE_ORIGI"
                           END) AS DATE) AS VARCHAR),
                           '') AS TCRIAPO,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DISSUEDAT"
                           ELSE CERT."DISSUEDAT"
                           END) AS DATE) AS VARCHAR),
                           '') AS TEMISSAO,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DDATE_ORIGI"
                           ELSE CERT."DDATE_ORIGI"
                           END) AS DATE) AS VARCHAR),
                           '') AS TINICIO,
                             '' AS DHORAINI,
                             COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DEXPIRDAT"
                           ELSE CERT."DEXPIRDAT"
                           END) AS DATE) AS VARCHAR),
                           '') AS TTERMO,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DSTARTDATE"
                           ELSE CERT."DSTARTDATE"
                           END) AS DATE) AS VARCHAR),
                           '') AS TINIANU,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DEXPIRDAT"
                           ELSE CERT."DEXPIRDAT"
                           END) AS DATE) AS VARCHAR),
                           '') AS TVENANU,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DNULLDATE"
                           ELSE CERT."DNULLDATE"
                           END) AS DATE) AS VARCHAR),
                           '') AS TANSUSP,
                             '' AS TESTADO,
                             COALESCE ((CASE
                             WHEN P."SPOLITYPE" IN ('2', '3')
                             AND CERT."NCERTIF" <> 0 THEN CERT."SSTATUSVA"
                             ELSE P."SSTATUS_POL"
                           END) ,
                           '') AS KACESTAP,
                             '' AS KACMOEST, --NO
                             COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DSTARTDATE"
                           ELSE CERT."DSTARTDATE"
                           END) AS DATE) AS VARCHAR),
                           '') AS TEFEACTA,
                             '' AS DULTACTA, --NO
                             '' AS KACCNEMI, --NO
                             '' AS KACARGES, --NO
                             '' AS KACAGENC, --NO
                             '' AS KACPROTO, --NO
                             COALESCE(P."SPOLITYPE", '')  AS KACTIPAP,
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
                                                     AND (CP."DNULLDATE" IS NULL OR CP."DNULLDATE" > P."DSTARTDATE") LIMIT 1)
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
                                         AND       CO."NPRODUCT" = P."NPRODUCT"     
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
                                         AND    CO."NPRODUCT" = P."NPRODUCT"    
                                         AND    CO."NPOLICY"  = P."NPOLICY"  
                                         AND    CO."NCERTIF"  = 0
                                         AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                                         AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                                         AND    CO."NINTERTYP" <> 1
                                         LIMIT 1)  
                             ),0) AS VTXCOMMD,
                             COALESCE(COALESCE(  --POR CERTIFICADO
                                       (SELECT COALESCE("NAMOUNT", 0) FROM  USVTIMV01."COMMISSION" CO 
                                         WHERE  CO."SCERTYPE" = '2'
                                         AND    CO."NBRANCH"  = P."NBRANCH"
                                         AND     CO."NPRODUCT" = P."NPRODUCT"   
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
                                         AND    CO."NPRODUCT" = P."NPRODUCT"    
                                         AND    CO."NPOLICY"  = P."NPOLICY"  
                                         AND    CO."NCERTIF"  = 0
                                         AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                                         AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
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
                             WHERE P."SCERTYPE" = '2' 
                             AND P."SSTATUS_POL" NOT IN ('2','3') 
                             AND ( (P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                   AND P."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                   AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '{l_fecha_carga_inicial}') )
                                   OR 
                                   (P."SPOLITYPE" <> '1' -- COLECTIVAS
                                   AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                   AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}'))
                                 )
                             AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' LIMIT 100)

                          UNION 

                          (SELECT
                           'D' AS INDDETREC, 
                           'ABAPOL' AS TABLAIFRS17,
                           '' AS PK,
                           '' AS DTPREG,       --NO 
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
                           COALESCE(CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."SCLIENT"
                           ELSE CERT."SCLIENT"
                           END, '') AS KEBENTID_TO,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DDATE_ORIGI"
                           ELSE CERT."DDATE_ORIGI"
                           END) AS DATE) AS VARCHAR),
                           '') AS TCRIAPO,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DISSUEDAT"
                           ELSE CERT."DISSUEDAT"
                           END) AS DATE) AS VARCHAR),
                           '') AS TEMISSAO,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DDATE_ORIGI"
                           ELSE CERT."DDATE_ORIGI"
                           END) AS DATE) AS VARCHAR),
                           '') AS TINICIO,
                             '' AS DHORAINI,
                             COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DEXPIRDAT"
                           ELSE CERT."DEXPIRDAT"
                           END) AS DATE) AS VARCHAR),
                           '') AS TTERMO,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DSTARTDATE"
                           ELSE CERT."DSTARTDATE"
                           END) AS DATE) AS VARCHAR),
                           '') AS TINIANU,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DEXPIRDAT"
                           ELSE CERT."DEXPIRDAT"
                           END) AS DATE) AS VARCHAR),
                           '') AS TVENANU,
                           COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DNULLDATE"
                           ELSE CERT."DNULLDATE"
                           END) AS DATE) AS VARCHAR),
                           '') AS TANSUSP,
                             '' AS TESTADO,
                             COALESCE ((CASE
                             WHEN P."SPOLITYPE" IN ('2', '3')
                             AND CERT."NCERTIF" <> 0 THEN CERT."SSTATUSVA"
                             ELSE P."SSTATUS_POL"
                           END) ,
                           '') AS KACESTAP,
                             '' AS KACMOEST, --NO
                             COALESCE(CAST(CAST((CASE
                           WHEN P."SPOLITYPE" = '1' THEN P."DSTARTDATE"
                           ELSE CERT."DSTARTDATE"
                           END) AS DATE) AS VARCHAR),
                           '') AS TEFEACTA,
                             '' AS DULTACTA, --NO
                             '' AS KACCNEMI, --NO
                             '' AS KACARGES, --NO
                             '' AS KACAGENC, --NO
                             '' AS KACPROTO, --NO
                             COALESCE(P."SPOLITYPE", '')  AS KACTIPAP,
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
                                                     AND (CP."DNULLDATE" IS NULL OR CP."DNULLDATE" > P."DSTARTDATE") LIMIT 1)
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
                                         AND       CO."NPRODUCT" = P."NPRODUCT"     
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
                                         AND    CO."NPRODUCT" = P."NPRODUCT"    
                                         AND    CO."NPOLICY"  = P."NPOLICY"  
                                         AND    CO."NCERTIF"  = 0
                                         AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                                         AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
                                         AND    CO."NINTERTYP" <> 1
                                         LIMIT 1)  
                             ),0) AS VTXCOMMD,
                             COALESCE(COALESCE(  --POR CERTIFICADO
                                       (SELECT COALESCE("NAMOUNT", 0) FROM  USVTIMV01."COMMISSION" CO 
                                         WHERE  CO."SCERTYPE" = '2'
                                         AND    CO."NBRANCH"  = P."NBRANCH"
                                         AND     CO."NPRODUCT" = P."NPRODUCT"   
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
                                         AND    CO."NPRODUCT" = P."NPRODUCT"    
                                         AND    CO."NPOLICY"  = P."NPOLICY"  
                                         AND    CO."NCERTIF"  = 0
                                         AND    CO."DEFFECDATE" <= P."DSTARTDATE"
                                         AND    (CO."DNULLDATE" IS NULL OR CO."DNULLDATE" > P."DSTARTDATE")
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
                             WHERE P."SCERTYPE" = '2' 
                             AND P."SSTATUS_POL" NOT IN ('2','3') 
                             AND ((P."SPOLITYPE" = '1' AND (P."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR P."DNULLDATE" < '{l_fecha_carga_inicial}')
                             AND (EXISTS (SELECT 1 FROM USVTIMV01."CLAIM" CLA                                           
                                           JOIN (SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMV01."CONDITION_SERV" CS 
                                                 WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                 JOIN USVTIMV01."CLAIM_HIS" CLH 
                                                 ON COALESCE(CLH."NCLAIM", 0) > 0 
                                                 AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                                 AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}') CLH 
                                           ON CLH."NCLAIM" = CLA."NCLAIM"
                                           WHERE CLA."SCERTYPE"  = P."SCERTYPE" 
                                           AND CLA."NBRANCH"  = P."NBRANCH" 
                                           AND CLA."NPRODUCT" = P."NPRODUCT"
                                           AND CLA."NPOLICY"  = P."NPOLICY"  
                                           AND CLA."NCERTIF"  =  0)))
                             OR (P."SPOLITYPE" <> '1' AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}')
                             AND ((EXISTS (SELECT 1 FROM USVTIMV01."CLAIM" CLA                                           
                                           JOIN (SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMV01."CONDITION_SERV" CS 
                                                 WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                 JOIN USVTIMV01."CLAIM_HIS" CLH 
                                                 ON COALESCE(CLH."NCLAIM", 0) > 0 
                                                 AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                                 AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}') CLH 
                                           ON CLH."NCLAIM" = CLA."NCLAIM"
                                           WHERE CLA."SCERTYPE"  = P."SCERTYPE" 
                                           AND CLA."NBRANCH"  = P."NBRANCH" 
                                           AND CLA."NPRODUCT" = P."NPRODUCT"
                                           AND CLA."NPOLICY"  = P."NPOLICY"  
                                           AND CLA."NCERTIF"  =  CERT."NCERTIF"))))))
                        ) AS TMP           
                        '''

  l_df_polizas_vtime_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_vtime_lpv).load()  

  print("ABAPOL USVTIMV01 EXITOSO")
  #------------------------------------------------------------------------------------------------------------------#

  #DECLARAR CONSULTA INSUNIX
  l_polizas_insunix_lpg = f'''
                          (SELECT 
                           'D' AS INDDETREC, 
                           'ABAPOL' AS TABLAIFRS17,
                           '' AS PK,
                           '' AS DTPREG,      --NO
                           '' AS TIOCPROC,    --NO
                           COALESCE(CAST((CASE WHEN P.POLITYPE = '1' THEN P.EFFECDATE ELSE P.CERT_EFFECDATE END) AS VARCHAR), '') AS TIOCFRM,
                           '' AS TIOCTO,     --NO
                           'PIG' AS KGIORIGM, --NO
                           'LPG' AS KACCOMPA, 
                           CAST(P.BRANCH AS VARCHAR) AS KGCRAMO,
                           COALESCE(P.BRANCH, 0) || '-' || COALESCE(P.PRODUCT, 0) || '-' || COALESCE(P.SUB_PRODUCT, 0) KABPRODT,
                           CASE P.POLITYPE
                           WHEN '2' THEN CASE WHEN P.CERTIF <> 0 THEN P.BRANCH || '-' || CAST(COALESCE (P.PRODUCT, 0) AS VARCHAR) || '-' || COALESCE(P.SUB_PRODUCT, 0) || '-' || P.POLICY || '-' || '0'
                                         ELSE ''
                                              END
                           ELSE '' 
                           END AS KABAPOL,
                           P.BRANCH || '-' || CAST(COALESCE (P.PRODUCT, 0) AS VARCHAR) || '-' || P.POLICY AS DNUMAPO,
                           CAST(P.CERTIF AS VARCHAR) AS DNMCERT,
                           '' AS DTERMO,     --BLANCO
                           COALESCE((
                                  SELECT SCOD_VT FROM USINSUG01.EQUI_VT_INX EVI
                                  WHERE EVI.SCOD_INX =  (CASE WHEN P.POLITYPE = '1' THEN P.TITULARC ELSE P.CERT_TITULARC END)
                           ), '') AS KEBENTID_TO,
                           COALESCE(CAST((CASE WHEN P.POLITYPE = '1' THEN P.DATE_ORIGI ELSE P.CERT_DATE_ORIGI END) AS VARCHAR), '') AS TCRIAPO, 
                           COALESCE(CAST((CASE WHEN P.POLITYPE = '1' THEN P.ISSUEDAT   ELSE P.CERT_ISSUEDAT   END) AS VARCHAR) , '') AS TEMISSAO,
                           COALESCE(CAST((CASE WHEN P.POLITYPE = '1' THEN P.EFFECDATE  ELSE P.CERT_EFFECDATE  END) AS VARCHAR) , '') AS TINICIO,
                           '' AS DHORAINI, --NO
                           COALESCE(CAST((CASE WHEN P.POLITYPE = '1' THEN P.EXPIRDAT  ELSE P.CERT_EXPIRDAT  END) AS VARCHAR) , '')  AS TTERMO,
                           COALESCE(CAST((CASE WHEN P.POLITYPE = '1' THEN P.EFFECDATE ELSE P.CERT_EFFECDATE END) AS VARCHAR) , '')  AS TINIANU,
                           COALESCE(CAST((CASE WHEN P.POLITYPE = '1' THEN P.EXPIRDAT  ELSE P.CERT_EXPIRDAT  END) AS VARCHAR) , '')  AS TVENANU,
                           COALESCE(CAST((CASE WHEN P.POLITYPE = '1' THEN P.NULLDATE  ELSE P.CERT_NULLDATE  END) AS VARCHAR) , '')  AS TANSUSP,
                           '' AS TESTADO,
                           COALESCE((CASE WHEN P.POLITYPE = '1' THEN P.STATUS_POL ELSE P.CERT_STATUSVA END) , '') AS KACESTAP,
                           '' AS KACMOEST, --NO
                           COALESCE(CAST((CASE WHEN P.POLITYPE = '1' THEN P.EFFECDATE ELSE P.CERT_EFFECDATE END) AS VARCHAR), '') AS TEFEACTA,
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
                             AND   CP.CERTIF  = P.CERTIF 
                             AND   CP.EFFECDATE <= P.EFFECDATE 
                             AND   ( CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE) LIMIT 1
                            ), '0') AS KACMOEDA,
                           COALESCE((
                             SELECT COALESCE(E.EXCHANGE, 0)
                             FROM USINSUG01.EXCHANGE E
                             WHERE E.USERCOMP = 1
                             AND   E.COMPANY = 1 
                             AND   E.CURRENCY = (SELECT CP.CURRENCY 
                                                 FROM USINSUG01.CURREN_POL CP
                                                 WHERE USERCOMP = 1 
                                                 AND COMPANY = 1 
                                                 AND CERTYPE = '2'
                                                 AND BRANCH  = P.BRANCH
                                                 AND POLICY  = P.POLICY
                                                 AND CERTIF  = 0
                                                 AND CP.EFFECDATE <= P.EFFECDATE 
                                                 AND (CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE) LIMIT 1
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
                                 AND DX.CERTYPE = '2'
                                 AND DX.BRANCH    = DE.BRANCH
                                 AND DX.CODE = DE.DISEXPRC
                               WHERE DX.USERCOMP = P.USERCOMP
                                 AND DX.COMPANY = P.COMPANY
                                 AND DX.BRANCH  = P.BRANCH
                                 AND DE.PRODUCT = P.PRODUCT
                                 AND DX.POLICY  = P.POLICY
                                 AND DX.CERTIF  = P.CERTIF 
                                 AND DX.EFFECDATE <= P.EFFECDATE
                                 AND (DX.NULLDATE IS NULL OR DX.NULLDATE > P.EFFECDATE)
                                 AND DE.BILL_ITEM = 4
                                 AND DE.CURRENCY = (SELECT CP.CURRENCY FROM USINSUG01.CURREN_POL CP
                                                     WHERE CP.USERCOMP = P.USERCOMP 
                                                       AND CP.COMPANY = P.COMPANY
                                                       AND CP.CERTYPE = P.CERTYPE
                                                       AND CP.BRANCH = P.BRANCH
                                                       AND CP.POLICY = P.POLICY
                                                       AND CP.CERTIF = P.CERTIF
                                                       AND CP.EFFECDATE <= P.EFFECDATE
                                                       AND (CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE) LIMIT 1
                                                   ) LIMIT 1
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
                                   AND   C.CERTIF   = P.CERTIF 
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
                                   AND   C.CERTIF   = P.CERTIF 
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
                           AND   C.CERTIF   = P.CERTIF 
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
                             AND C.COMPANYC = 1
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
                             AND C.COMPANYC = 1
                           ), 0) AS VTXCOSSG,
                           COALESCE ((    
                             SELECT (TRUNC(COALESCE(SHARE, 0),4) /100)
                             FROM USINSUG01.REINSURAN R
                             WHERE R.USERCOMP = 1
                             AND R.COMPANY = 1
                             AND R.CERTYPE =  '2'
                             AND R.BRANCH = P.BRANCH
                             AND R.POLICY = P.POLICY
                             AND R.CERTIF = P.CERTIF
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
                             AND R.CERTIF = P.CERTIF
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
                             AND C.CERTIF = P.CERTIF
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
                               AND   PH.CERTIF   = P.CERTIF
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
                             AND R.COMPANY   = P.COMPANY 
                             AND R.CERTYPE   = P.CERTYPE 
                             AND R.BRANCH    = P.BRANCH 
                             AND R.POLICY    = P.POLICY 
                             AND R.CERTIF    = P.CERTIF 
                           ) AS DQTDPART,
                           (CASE WHEN P.YEAR_MONTH > 3 THEN COALESCE(CAST(TO_DATE(P.YEAR_MONTH::TEXT, 'YYYYMM') AS VARCHAR),'') ELSE '' END) AS TINIPRXANU,
                           '' AS KACTPREAP,    --EN BLANCO
                           '' AS DENTCONGE,    --NO
                           '' AS KCBMED_PARCE, --NO
                           '' AS DCODPARC,      --NO
                           '' AS DMODPARC,      --NO
                           COALESCE(P.BUSSITYP, '') AS DTIPSEG,
                           '' AS KACTPNEG,      --NO
                           '' AS DURPAGAPO,    --EN BLANCO
                           '' AS DNMINTERP,     --NO
                           '' AS DNUMADES,      --NO
                           '' AS KACTPPRD,      --ACLARAR
                           '' AS KACSBTPRD,     --ACLARAR
                           '' AS KABPRODT_REL, --NO
                           '' AS KACTPPARES,    --NO
                           '' AS KACTIPIFAP,       --NO
                           '' AS KACTPALT_IFRS17, --NO
                           '' AS TEFEALTE,     --NO
                           '' AS TINITARLTA,      --NO
                           '' AS TFIMTARLTA,      --NO
                           '' AS DTERMO_IFRS17,   --NO
                           '' AS TEMISREN          --NO
                           FROM  
                           (
                            (SELECT 
                            P.USERCOMP,
                            P.COMPANY,     
                            P.CERTYPE,                       
                            P.BRANCH,
                            P.PRODUCT,
                            P.POLICY,                           
                            P.TITULARC,
                            P.EFFECDATE,
                            P.EXPIRDAT,
                            P.NULLDATE,
                            P.STATUS_POL,
                            P.POLITYPE,
                            P.DATE_ORIGI,
                            P.ISSUEDAT,
                            P.RENEWAL,
                            P.PAYFREQ,
                            P.BUSSITYP,
                            P.REINTYPE,
                            P.PREMIUM,
                            P.NULLCODE,
                            P.YEAR_MONTH,
                            POL.SUB_PRODUCT,
                            CERT.EFFECDATE  AS CERT_EFFECDATE,
                            CERT.STARTDATE  AS CERT_STARTDATE,
                            CERT.EXPIRDAT   AS CERT_EXPIRDAT,
                            CERT.CERTIF,
                            CERT.TITULARC   AS CERT_TITULARC,
                            CERT.DATE_ORIGI AS CERT_DATE_ORIGI,
                            CERT.ISSUEDAT   AS CERT_ISSUEDAT,
                            CERT.NULLDATE   AS CERT_NULLDATE,
                            CERT.STATUSVA   AS CERT_STATUSVA
                            FROM USINSUG01.POLICY P 
                            LEFT JOIN USINSUG01.CERTIFICAT CERT 
                            ON  CERT.USERCOMP = P.USERCOMP  
                            AND CERT.COMPANY = P.COMPANY   
                            AND CERT.CERTYPE = P.CERTYPE 
                            AND CERT.BRANCH  = P.BRANCH
                            AND CERT.POLICY  = P.POLICY 
                            AND CERT.PRODUCT = P.PRODUCT
                            JOIN USINSUG01.POL_SUBPRODUCT POL
                            ON POL.USERCOMP = P.USERCOMP
                            AND POL.COMPANY = P.COMPANY
                            AND POL.CERTYPE = P.CERTYPE
                            AND POL.BRANCH = P.BRANCH
                            AND POL.POLICY = P.POLICY
                            AND POL.PRODUCT = P.PRODUCT
                            WHERE P.CERTYPE  = '2'
                            AND P.STATUS_POL NOT IN ('2','3') 
                            AND ((P.POLITYPE = '1' -- INDIVIDUAL 
                                      AND P.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                      AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}'))
                                      OR 
                                    (P.POLITYPE <> '1' -- COLECTIVAS 
                                      AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}')))
                            AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')

                            /*UNION 

                            (SELECT 
                            P.USERCOMP,
                            P.COMPANY,     
                            P.CERTYPE,                       
                            P.BRANCH,
                            P.PRODUCT,
                            P.POLICY,                           
                            P.TITULARC,
                            P.EFFECDATE,
                            P.EXPIRDAT,
                            P.NULLDATE,
                            P.STATUS_POL,
                            P.POLITYPE,
                            P.DATE_ORIGI,
                            P.ISSUEDAT,
                            P.RENEWAL,
                            P.PAYFREQ,
                            P.BUSSITYP,
                            P.REINTYPE,
                            P.PREMIUM,
                            P.NULLCODE,
                            P.YEAR_MONTH,
                            POL.SUB_PRODUCT,
                            CERT.EFFECDATE  AS CERT_EFFECDATE,
                            CERT.STARTDATE  AS CERT_STARTDATE,
                            CERT.EXPIRDAT   AS CERT_EXPIRDAT,
                            CERT.CERTIF,
                            CERT.TITULARC   AS CERT_TITULARC,
                            CERT.DATE_ORIGI AS CERT_DATE_ORIGI,
                            CERT.ISSUEDAT   AS CERT_ISSUEDAT,
                            CERT.NULLDATE   AS CERT_NULLDATE,
                            CERT.STATUSVA   AS CERT_STATUSVA
                            FROM USINSUG01.POLICY P
                            LEFT JOIN USINSUG01.CERTIFICAT CERT 
                            ON  CERT.USERCOMP = P.USERCOMP 
                            AND CERT.COMPANY = P.COMPANY 
                            AND CERT.CERTYPE = P.CERTYPE 
                            AND CERT.BRANCH = P.BRANCH 
                            AND CERT.POLICY = P.POLICY 
                            AND CERT.PRODUCT = P.PRODUCT
                            JOIN USINSUG01.POL_SUBPRODUCT POL
                            ON POL.USERCOMP = P.USERCOMP
                            AND POL.COMPANY = P.COMPANY
                            AND POL.CERTYPE = P.CERTYPE
                            AND POL.BRANCH = P.BRANCH
                            AND POL.POLICY = P.POLICY
                            AND POL.PRODUCT = P.PRODUCT
                            WHERE P.CERTYPE = '2' 
                            AND P.STATUS_POL NOT IN ('2', '3') 
                            AND (((P.POLITYPE = '1' AND (P.EXPIRDAT < '{l_fecha_carga_inicial}' OR P.NULLDATE < '{l_fecha_carga_inicial}')) 
                            AND EXISTS (SELECT  1
                                        FROM  USINSUG01.CLAIM CLA    
                                        JOIN  USINSUG01.CLAIM_HIS CLH 
                                        ON    CLH.USERCOMP = CLA.USERCOMP 
                                        AND   CLH.COMPANY  = CLA.COMPANY 
                                        AND   CLH.BRANCH   = CLA.BRANCH
                                        AND   CLH.CLAIM    = CLA.CLAIM
                                        WHERE /*CLA.USERCOMP = P.USERCOMP 
                                          AND   CLA.COMPANY = P.COMPANY  
                                        AND  */ CLA.BRANCH = P.BRANCH
                                        AND   CLA.POLICY = P.POLICY
                                        AND   TRIM(CLH.OPER_TYPE) IN  (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) FROM    USINSUG01.TAB_CL_OPE TCL
                                                                           WHERE  (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                        AND   CLH.OPERDATE >= '{l_fecha_carga_inicial}'))                                 
                            OR ((P.POLITYPE <> '1' AND (CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  OR  CERT.NULLDATE < '{l_fecha_carga_inicial}')) 
                            AND EXISTS (SELECT  1
                                          FROM  USINSUG01.CLAIM CLA    
                                          JOIN  USINSUG01.CLAIM_HIS CLH 
                                          ON    CLA.USERCOMP = CLH.USERCOMP  
                                          AND   CLA.COMPANY = CLH.COMPANY 
                                          AND   CLH.CLAIM = CLA.CLAIM
                                          WHERE /*CLA.USERCOMP = CERT.USERCOMP 
                                          AND   CLA.COMPANY = CERT.COMPANY  
                                          AND */CLA.BRANCH   = CERT.BRANCH
                                          AND   CLA.POLICY = CERT.POLICY
                                          AND   CLA.CERTIF   = CERT.CERTIF
                                          AND   TRIM(CLH.OPER_TYPE) IN  (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) FROM  USINSUG01.TAB_CL_OPE TCL
                                                                         WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1))
                                          AND  CLH.OPERDATE >= '{l_fecha_carga_inicial}'))) 
                            AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')*/)P ) AS TMP 
                          '''

  l_df_polizas_insunix_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_insunix_lpg).load()                      

  print("ABAPOL USINSUG01 EXITOSO")

  l_polizas_insunix_lpv = f'''
                          (   SELECT 
                              'D' AS INDDETREC, 
                              'ABAPOL' AS TABLAIFRS17,
                              '' AS PK,
                              '' AS DTPREG,      --NO
                              '' AS TIOCPROC,    --NO
                              COALESCE(CAST((case when p.politype = '1' then P.EFFECDATE else P.effecdate end) AS VARCHAR), '') AS TIOCFRM,
                              '' AS TIOCTO,      --NO
                              'PIV' AS KGIORIGM, --NO
                              'LPV' AS KACCOMPA, --NO
                              CAST(P.BRANCH AS VARCHAR) AS KGCRAMO,
                              CAST(P.BRANCH AS VARCHAR) || '-' || CAST(P.PRODUCT AS VARCHAR) AS KABPRODT,
                              CASE P.POLITYPE
                              WHEN '2' THEN CASE WHEN P.CERTIF <> 0 THEN P.BRANCH || '-' || P.PRODUCT || '-' || P.POLICY || '-' || '0'
                                            ELSE ''
                                            END
                              ELSE '' 
                              END AS KABAPOL,
                              P.BRANCH || '-' || P.PRODUCT || '-' || P.POLICY AS DNUMAPO,
                              CAST(P.CERTIF AS VARCHAR) AS DNMCERT,
                              '' AS DTERMO,
                              COALESCE((
                                SELECT SCOD_VT FROM USINSUG01.EQUI_VT_INX EVI
                                WHERE EVI.SCOD_INX = (case when p.politype = '1' then P.TITULARC else P.titularc end)
                              ), '')
                              AS KEBENTID_TO,
                              COALESCE(CAST((case when p.politype = '1' then P.DATE_ORIGI else P.CERT_date_origi end) AS VARCHAR), '') AS TCRIAPO,
                              COALESCE(CAST((case when p.politype = '1' then P.ISSUEDAT else P.CERT_issuedat end) AS VARCHAR), '') AS TEMISSAO,
                              COALESCE(CAST((case when P.politype = '1' then p.effecdate else P.CERT_effecdate end) AS VARCHAR), '') AS TINICIO,
                              '' AS DHORAINI, --NO
                              COALESCE(CAST((case when p.politype = '1' then P.EXPIRDAT  else P.CERT_expirdat end) AS VARCHAR), '') AS TTERMO,
                              COALESCE(CAST((case when p.politype = '1' then P.EFFECDATE else P.CERT_effecdate end) AS VARCHAR), '') AS TINIANU,
                              COALESCE(CAST((case when p.politype = '1' then P.EXPIRDAT  else P.CERT_expirdat end) AS VARCHAR), '') AS TVENANU,
                              COALESCE(CAST((case when p.politype = '1' then P.NULLDATE  else P.CERT_nulldate end)  AS VARCHAR), '') AS TANSUSP,
                              '' AS TESTADO, --EN BLANCO
                              COALESCE((case when p.politype = '1' then P.STATUS_POL     else P.CERT_statusva end), '') AS KACESTAP,
                              '' AS KACMOEST, --NO
                              COALESCE(CAST((case when p.politype = '1' then P.EFFECDATE else P.CERT_effecdate end) AS VARCHAR), '') AS TEFEACTA,
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
                                AND   CP.CERTIF  = P.CERTIF 
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
                                AND DX.COMPANY  = DE.COMPANY
                                AND DX.CERTYPE  = '2' 
                                AND DX.BRANCH   = DE.BRANCH
                                AND DX.CODE     = DE.DISEXPRC
                                WHERE DX.USERCOMP = P.USERCOMP
                                AND DX.COMPANY = P.COMPANY
                                AND DX.BRANCH  = P.BRANCH
                                AND DE.PRODUCT = P.PRODUCT
                                AND DX.POLICY  = P.POLICY
                                AND DX.CERTIF  = P.CERTIF 
                                AND DX.EFFECDATE <= P.EFFECDATE
                                AND (DX.NULLDATE IS NULL OR DX.NULLDATE > P.EFFECDATE)
                                AND DE.BILL_ITEM = 4
                                AND DE.CURRENCY = (SELECT CP.CURRENCY FROM USINSUV01.CURREN_POL CP
                                                   WHERE CP.USERCOMP = P.USERCOMP 
                                                   AND CP.COMPANY    = P.COMPANY
                                                   AND CP.CERTYPE    = P.CERTYPE
                                                   AND CP.BRANCH     = P.BRANCH
                                                   AND CP.POLICY     = P.POLICY
                                                   AND CP.CERTIF     = P.CERTIF
                                                   AND CP.EFFECDATE <= P.EFFECDATE
                                                   AND (CP.NULLDATE IS NULL OR CP.NULLDATE > P.EFFECDATE) LIMIT 1) LIMIT 1), 0)
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
                                      AND   C.CERTIF   = P.CERTIF 
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
                                      AND   C.CERTIF   = P.CERTIF 
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
                                AND   C.CERTIF   = P.CERTIF 
                                AND   C.EFFECDATE <= P.EFFECDATE
                                AND   (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                              ) AS VCAPITAL,
                              '' AS VMTPRMSP,--NO
                              COALESCE(P.PREMIUM, 0)  AS VMTCOMR,
                              (
                                SELECT COALESCE(SHARE, 0) * COALESCE(P.PREMIUM, 0)
                                FROM USINSUV01.COINSURAN C
                                WHERE C.USERCOMP = 1
                                AND C.COMPANY = 1 -- VALOR DE LA COMPANIA
                                AND C.CERTYPE = '2'
                                AND C.BRANCH = P.BRANCH 
                                AND C.POLICY = P.POLICY
                                AND C.EFFECDATE <= P.EFFECDATE
                                AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                AND   C.COMPANYC = 12 --COMPANIA LPV
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
                                AND C.COMPANY = 1 -- VALOR DE LA COMPANIA
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
                                AND R.CERTIF = P.CERTIF
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
                                AND C.CERTIF = P.CERTIF
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
                                AND   PH.CERTIF   = P.CERTIF
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
                                AND R.COMPANY    = P.COMPANY 
                                AND R.CERTYPE    = P.CERTYPE 
                                AND R.BRANCH     = P.BRANCH 
                                AND R.POLICY     = P.POLICY 
                                AND R.CERTIF     = P.CERTIF 
                              ) AS VARCHAR) AS DQTDPART,
                              (case when P.YEAR_MONTH > 3 then coalesce(cast(to_date(P.YEAR_MONTH::text, 'YYYYMM') as VARCHAR),'') else '' end) AS TINIPRXANU,
                              '' AS KACTPREAP,    --EN BLANCO
                              '' AS DENTCONGE,    --NO
                              '' AS KCBMED_PARCE, --NO
                              '' AS DCODPARC,   --NO
                              '' AS DMODPARC,   --NO
                              P.BUSSITYP AS DTIPSEG,
                              '' AS KACTPNEG,   --NO
                              '' AS DURPAGAPO,    --EN BLANCO
                              '' AS DNMINTERP,  --NO
                              '' AS DNUMADES,   --NO
                              '' AS KACTPPRD,   --ACLARAR
                              '' AS KACSBTPRD,  --ACLARAR
                              '' AS KABPRODT_REL, --NO
                              '' AS KACTPPARES,     --NO
                              '' AS KACTIPIFAP,     --NO
                              '' AS KACTPALT_IFRS17, --NO
                              '' AS TEFEALTE,      --NO
                              '' AS TINITARLTA,      --NO
                              '' AS TFIMTARLTA,      --NO
                              '' AS DTERMO_IFRS17,   --NO
                              '' AS TEMISREN           --NO*/
                              FROM 
                              (
                               (SELECT                               
                               P.USERCOMP,
                               P.COMPANY,     
                               P.CERTYPE,                       
                               P.BRANCH,
                               P.PRODUCT,
                               P.POLICY,                           
                               P.TITULARC,
                               P.EFFECDATE,
                               P.EXPIRDAT,
                               P.NULLDATE,
                               P.STATUS_POL,
                               P.POLITYPE,
                               P.DATE_ORIGI,
                               P.ISSUEDAT,
                               P.RENEWAL,
                               P.PAYFREQ,
                               P.BUSSITYP,
                               P.REINTYPE,
                               P.PREMIUM,
                               P.NULLCODE,
                               P.YEAR_MONTH,
                               CERT.EFFECDATE  AS CERT_EFFECDATE,
                               CERT.STARTDATE  AS CERT_STARTDATE,
                               CERT.EXPIRDAT   AS CERT_EXPIRDAT,
                               CERT.CERTIF,
                               CERT.TITULARC   AS CERT_TITULARC,
                               CERT.DATE_ORIGI AS CERT_DATE_ORIGI,
                               CERT.ISSUEDAT   AS CERT_ISSUEDAT,
                               CERT.NULLDATE   AS CERT_NULLDATE,
                               CERT.STATUSVA   AS CERT_STATUSVA
                               from USINSUV01.POLICY P 
                               LEFT JOIN USINSUV01.CERTIFICAT CERT 
                               ON  CERT.USERCOMP = P.USERCOMP 
                               AND CERT.COMPANY = P.COMPANY   
                               AND CERT.CERTYPE = P.CERTYPE
                               AND CERT.BRANCH  = P.BRANCH
                               AND CERT.POLICY  = P.POLICY
                               AND CERT.PRODUCT = P.PRODUCT
                               WHERE P.CERTYPE  = '2'
                               AND P.STATUS_POL NOT IN ('2','3') 
                               AND ((P.POLITYPE = '1' -- INDIVIDUAL 
                                     AND P.EXPIRDAT >= '{l_fecha_carga_inicial}'  
                                     AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}'))
                                     OR 
                                   (P.POLITYPE <> '1' -- COLECTIVAS 
                                     AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                               AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}')))
                               AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                               
                               /*UNION

                               (SELECT 
                                P.USERCOMP,
                                P.COMPANY,     
                                P.CERTYPE,                       
                                P.BRANCH,
                                P.PRODUCT,
                                P.POLICY,                           
                                P.TITULARC,
                                P.EFFECDATE,
                                P.EXPIRDAT,
                                P.NULLDATE,
                                P.STATUS_POL,
                                P.POLITYPE,
                                P.DATE_ORIGI,
                                P.ISSUEDAT,
                                P.RENEWAL,
                                P.PAYFREQ,
                                P.BUSSITYP,
                                P.REINTYPE,
                                P.PREMIUM,
                                P.NULLCODE,
                                P.YEAR_MONTH,
                                CERT.EFFECDATE  AS CERT_EFFECDATE,
                                CERT.STARTDATE  AS CERT_STARTDATE,
                                CERT.EXPIRDAT   AS CERT_EXPIRDAT,
                                CERT.CERTIF,
                                CERT.TITULARC   AS CERT_TITULARC,
                                CERT.DATE_ORIGI AS CERT_DATE_ORIGI,
                                CERT.ISSUEDAT   AS CERT_ISSUEDAT,
                                CERT.NULLDATE   AS CERT_NULLDATE,
                                CERT.STATUSVA   AS CERT_STATUSVA
                                FROM USINSUV01.POLICY P
                                LEFT JOIN USINSUV01.CERTIFICAT CERT 
                                ON  CERT.USERCOMP = P.USERCOMP 
                                AND CERT.COMPANY = P.COMPANY 
                                AND CERT.CERTYPE = P.CERTYPE 
                                AND CERT.BRANCH  = P.BRANCH 
                                AND CERT.POLICY  = P.POLICY 
                                AND CERT.PRODUCT = P.PRODUCT
                                WHERE P.CERTYPE  = '2' 
                                AND P.STATUS_POL NOT IN ('2', '3') 
                                AND (((P.POLITYPE = '1' AND  P.EXPIRDAT < '{l_fecha_carga_inicial}' OR P.NULLDATE < '{l_fecha_carga_inicial}') 
                                AND EXISTS (SELECT  1
                                            FROM  USINSUV01.CLAIM CLA    
                                            JOIN  USINSUV01.CLAIM_HIS CLH 
                                            ON    CLH.USERCOMP = CLA.USERCOMP 
                                            AND   CLH.COMPANY  = CLA.COMPANY 
                                            AND   CLH.BRANCH   = CLA.BRANCH 
                                            AND   CLH.CLAIM    = CLA.CLAIM
                                            WHERE CLA.BRANCH   = P.BRANCH
                                            AND   CLA.POLICY   = P.POLICY
                                            AND   TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) FROM     USINSUG01.TAB_CL_OPE TCL
                                                                              WHERE  (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                            AND   CLH.OPERDATE >= '{l_fecha_carga_inicial}'))) 
                                AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')*/
                                
                                /*UNION
                                
                                (SELECT 
                                P.USERCOMP,
                                P.COMPANY,     
                                P.CERTYPE,                       
                                P.BRANCH,
                                P.PRODUCT,
                                P.POLICY,                           
                                P.TITULARC,
                                P.EFFECDATE,
                                P.EXPIRDAT,
                                P.NULLDATE,
                                P.STATUS_POL,
                                P.POLITYPE,
                                P.DATE_ORIGI,
                                P.ISSUEDAT,
                                P.RENEWAL,
                                P.PAYFREQ,
                                P.BUSSITYP,
                                P.REINTYPE,
                                P.PREMIUM,
                                P.NULLCODE,
                                P.YEAR_MONTH,
                                CERT.EFFECDATE  AS CERT_EFFECDATE,
                                CERT.STARTDATE  AS CERT_STARTDATE,
                                CERT.EXPIRDAT   AS CERT_EXPIRDAT,
                                CERT.CERTIF,
                                CERT.TITULARC   AS CERT_TITULARC,
                                CERT.DATE_ORIGI AS CERT_DATE_ORIGI,
                                CERT.ISSUEDAT   AS CERT_ISSUEDAT,
                                CERT.NULLDATE   AS CERT_NULLDATE,
                                CERT.STATUSVA   AS CERT_STATUSVA
                                FROM USINSUV01.POLICY P
                                LEFT JOIN USINSUV01.CERTIFICAT CERT 
                                ON  CERT.USERCOMP = P.USERCOMP 
                                AND CERT.COMPANY = P.COMPANY 
                                AND CERT.CERTYPE = P.CERTYPE 
                                AND CERT.BRANCH  = P.BRANCH 
                                AND CERT.POLICY  = P.POLICY 
                                AND CERT.PRODUCT = P.PRODUCT
                                WHERE P.CERTYPE  = '2' 
                                AND P.STATUS_POL NOT IN ('2', '3') 
                                AND (((P.POLITYPE <> '1' AND CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  OR  CERT.NULLDATE < '{l_fecha_carga_inicial}') 
                                AND EXISTS (SELECT  1
                                            FROM  USINSUV01.CLAIM CLA    
                                            JOIN  USINSUV01.CLAIM_HIS CLH 
                                            ON    CLA.USERCOMP = CLH.USERCOMP  
                                            AND   CLA.COMPANY  = CLH.COMPANY 
                                            AND   CLA.BRANCH   = CLH.BRANCH 
                                            AND   CLH.CLAIM    = CLA.CLAIM
                                            WHERE /*CLA.USERCOMP = CERT.USERCOMP 
                                              AND   CLA.COMPANY  = CERT.COMPANY  
                                            AND*/   CLA.BRANCH   = CERT.BRANCH
                                            AND   CLA.POLICY   = CERT.POLICY
                                            AND   CLA.CERTIF   = CERT.CERTIF
                                            AND   TRIM(CLH.OPER_TYPE) IN  (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) FROM  USINSUG01.TAB_CL_OPE TCL
                                                                             WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1))
                                            AND  CLH.OPERDATE >= '{l_fecha_carga_inicial}')))
                                AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin})*/) AS P) AS TMP
                          '''
   #Ejecutar consulta
  
  l_df_polizas_insunix_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_insunix_lpv).load()
  
  print("ABAPOL USINSUV01 EXITOSO")

  #------------------------------------------------------------------------------------------------------------------#

   #Declara consulta INSIS
  l_polizas_insis = f'''
                     ( SELECT
                      'D' AS INDDETREC, 
                      'ABAPOL' AS TABLAIFRS17,
                      '' AS PK,
                      '' AS DTPREG,   --NO
                      '' AS TIOCPROC, --NO
                      CAST(CAST(P."INSR_BEGIN" AS DATE) AS VARCHAR) AS TIOCFRM, --CAMBIO
                      '' AS TIOCTO,   --NO
                      'PNV' AS KGIORIGM,
                      'LPV' AS KACCOMPA,
                      COALESCE(P."ATTR1", '0') AS KGCRAMO,
                      P."INSR_TYPE" AS KABPRODT,
                      KABAPOL,
                      SUBSTRING(cast(DNUMAPO as varchar),6,12) DNUMAPO,
                      SUBSTRING(cast(DNMCERT as varchar),6,12) DNMCERT,
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
                      TANSUSP,
                      '' AS TESTADO, --EN BLANCO
                      CAST(P."POLICY_STATE" AS VARCHAR) AS  KACESTAP,
                      '' AS KACMOEST, --NO
                      CAST(CAST(P."INSR_BEGIN" AS DATE) AS VARCHAR) AS TEFEACTA, --CERRADO PERO NO ESPECIFICA
                      '' AS DULTACTA, --NO
                      '' AS KACCNEMI, --NO
                      '' AS KACARGES, --NO
                      '' AS KACAGENC, --NO
                      '' AS KACPROTO, --NO
                      CASE P."ENG_POL_TYPE"
                      WHEN 'POLICY'    THEN 1
                      WHEN 'MASTER'    THEN 2
                      WHEN 'DEPENDENT' THEN 2
                      END KACTIPAP,
                      /*(SELECT TP."COD_TIPO_POLIZA" FROM USBI01."IFRS170_T_TIPO_POLIZA" TP WHERE TP."NOM_TIPO_POLIZA" = P."ENG_POL_TYPE" ) AS KACTIPAP,*/
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
                      KACMOEDA,
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
                      /*(SELECT TP."COD_TIPO_POLIZA" FROM USBI01."IFRS170_T_TIPO_POLIZA" TP WHERE TP."NOM_TIPO_POLIZA" = P."ENG_POL_TYPE" ) AS KACTPSUB,*/
                      CASE P."ENG_POL_TYPE"
                      WHEN 'POLICY' THEN 1
                      WHEN 'MASTER' THEN 2
                      WHEN 'DEPENDENT' THEN 2
                      END KACTPSUB,
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
                      DQTDPART,
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
                      KACTPPRD,
                      KACSBTPRD,
                      '' AS KABPRODT_REL,   --NO
                      '' AS KACTPPARES,     --NO
                      '' AS KACTIPIFAP,     --NO
                      '' AS KACTPALT_IFRS17,--NO
                      '' AS TEFEALTE,       --NO
                      '' AS TINITARLTA,     --NO
                      '' AS TFIMTARLTA,     --NO
                      '' AS DTERMO_IFRS17,  --NO
                      '' AS TEMISREN        --NO
                      FROM        
                      ( SELECT                        
                        P."INSR_TYPE",
                        P."CLIENT_ID",
                        P."POLICY_STATE",
                        P."DATE_GIVEN",
                        P."INSR_BEGIN",
                        P."INSR_END",
                        P."ATTR1",
                        P."ATTR2",
                        P."RENEWABLE_FLAG",
                        P."REGISTRATION_DATE",                       
                        P."ATTR5",                        
                        PP."ENG_POL_TYPE",
                        CAST(P."POLICY_ID" AS VARCHAR) DNUMAPO,
                        0  AS DNMCERT,                        
                        '' AS KABAPOL,
                        (SELECT CAST(CAST(GA."INSR_BEGIN" AS DATE) AS VARCHAR)
                        FROM USINSIV01."GEN_ANNEX" GA
                        WHERE GA."POLICY_ID" = P."POLICY_ID"
                        AND GA."ANNEX_TYPE"  = '17'
                        AND GA."ANNEX_STATE" = 0
                        AND GA."ANNEX_ID"    = (SELECT  MAX(GAX."ANNEX_ID")
                                                FROM    USINSIV01."GEN_ANNEX" GAX
                                                WHERE   GAX."POLICY_ID" = P."POLICY_ID"
                                                AND     GAX."ANNEX_TYPE" = '17'
                                                AND     GAX."ANNEX_STATE" = 0)) AS TANSUSP,
                        CASE COALESCE((SELECT DISTINCT COALESCE(IO."AV_CURRENCY", '')
                        FROM USINSIV01."INSURED_OBJECT" IO 
                        WHERE IO."POLICY_ID" = P."POLICY_ID" LIMIT 1 ),'') 
                        WHEN 'USD' THEN '2'
                        WHEN 'PEN' THEN '1'
                        ELSE '0'
                        END KACMOEDA,
                        (CAST(COALESCE((  SELECT COUNT(DISTINCT PAR."MAN_ID")
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
                       COALESCE(CAST(P."INSR_TYPE" AS VARCHAR), '') AS KACTPPRD,
                       (SELECT PC."COND_VALUE" FROM USINSIV01."POLICY_CONDITIONS" PC
                          WHERE "COND_TYPE" LIKE '%AS_IS%'
                          AND PC."POLICY_ID" = P."POLICY_ID"    
                          AND PC."INSR_TYPE" = P."INSR_TYPE"    
                          AND PC."ANNEX_ID"  = (SELECT MAX("ANNEX_ID") FROM USINSIV01."POLICY_CONDITIONS" PC      
                                                WHERE "COND_TYPE" LIKE '%AS_IS%'                          
                                                AND PC."POLICY_ID" = P."POLICY_ID"                        
                                                AND PC."INSR_TYPE" = P."INSR_TYPE" )) AS KACSBTPRD
                        FROM USINSIV01."POLICY" P 
                        LEFT JOIN USINSIV01."POLICY_ENG_POLICIES" PP ON P."POLICY_ID" = PP."POLICY_ID" WHERE PP."ENG_POL_TYPE" = 'POLICY'                       
                        AND P."INSR_END" >= '{l_fecha_carga_inicial}'

                        UNION ALL

                        SELECT
                        P."INSR_TYPE",
                        P."CLIENT_ID",                         
                        P."POLICY_STATE",                        
                        P."DATE_GIVEN",
                        P."INSR_BEGIN",
                        P."INSR_END",
                        P."ATTR1", 
                        P."ATTR2",
                        P."RENEWABLE_FLAG",
                        P."REGISTRATION_DATE",                     
                        P."ATTR5",
                        PP."ENG_POL_TYPE",
                         CAST(P."POLICY_ID" AS VARCHAR) DNUMAPO,
                        0  AS DNMCERT,                                                 
                        '' AS KABAPOL,
                        (SELECT CAST(CAST(GA."INSR_BEGIN" AS DATE) AS VARCHAR)
                        FROM USINSIV01."GEN_ANNEX" GA
                        WHERE GA."POLICY_ID" = P."POLICY_ID"
                        AND GA."ANNEX_TYPE"  = '17'
                        AND GA."ANNEX_STATE" = 0
                        AND GA."ANNEX_ID"    = (SELECT  MAX(GAX."ANNEX_ID")
                                                FROM    USINSIV01."GEN_ANNEX" GAX
                                                WHERE   GAX."POLICY_ID" = P."POLICY_ID"
                                                AND     GAX."ANNEX_TYPE" = '17'
                                                AND     GAX."ANNEX_STATE" = 0)) AS TANSUSP,
                        CASE COALESCE((SELECT DISTINCT COALESCE(IO."AV_CURRENCY", '')
                                    FROM USINSIV01."INSURED_OBJECT" IO 
                                    WHERE IO."POLICY_ID" = P."POLICY_ID" LIMIT 1 ),'') 
                      WHEN 'USD' THEN '2'
                      WHEN 'PEN' THEN '1'
                      ELSE '0'
                      END KACMOEDA,
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
                      ) AS KACSBTPRD
                        FROM USINSIV01."POLICY" P 
                        LEFT JOIN  USINSIV01."POLICY_ENG_POLICIES" PP ON P."POLICY_ID" = PP."POLICY_ID" WHERE PP."ENG_POL_TYPE" = 'MASTER'
                        AND P."INSR_END" >= '{l_fecha_carga_inicial}'

                        UNION ALL

                        SELECT                         
                        P."INSR_TYPE",
                        P."CLIENT_ID",                        
                        P."POLICY_STATE", 
                        P."DATE_GIVEN",
                        P."INSR_BEGIN",
                        P."INSR_END",
                        P."ATTR1", 
                        P."ATTR2",
                        P."RENEWABLE_FLAG",
                        P."REGISTRATION_DATE",                        
                        P."ATTR5",
                        PP."ENG_POL_TYPE",
                        CAST(PP."MASTER_POLICY_ID" AS VARCHAR) DNUMAPO, 
                        PP."POLICY_ID" DNMCERT,
                        SUBSTRING(CAST(PP."MASTER_POLICY_ID" AS VARCHAR),6,12) AS KABAPOL,                      
                        (SELECT CAST(CAST(GA."INSR_BEGIN" AS DATE) AS VARCHAR)
                        FROM USINSIV01."GEN_ANNEX" GA
                        WHERE GA."POLICY_ID" = P."POLICY_ID"
                        AND GA."ANNEX_TYPE"  = '17'
                        AND GA."ANNEX_STATE" = 0
                        AND GA."ANNEX_ID"    = (SELECT  MAX(GAX."ANNEX_ID")
                                                FROM    USINSIV01."GEN_ANNEX" GAX
                                                WHERE   GAX."POLICY_ID" = P."POLICY_ID"
                                                AND     GAX."ANNEX_TYPE" = '17'
                                                AND     GAX."ANNEX_STATE" = 0)) AS TANSUSP,
                        CASE COALESCE((SELECT DISTINCT COALESCE(IO."AV_CURRENCY", '')
                                    FROM USINSIV01."INSURED_OBJECT" IO 
                                    WHERE IO."POLICY_ID" = P."POLICY_ID" LIMIT 1 ),'') 
                        WHEN 'USD' THEN '2'
                        WHEN 'PEN' THEN '1'
                        ELSE '0'
                        END KACMOEDA,
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
                      ) AS KACSBTPRD
                        FROM USINSIV01."POLICY" P
                        LEFT JOIN  USINSIV01."POLICY_ENG_POLICIES" PP ON P."POLICY_ID" = PP."POLICY_ID" WHERE PP."ENG_POL_TYPE" = 'DEPENDENT' AND P."INSR_END" >= '{l_fecha_carga_inicial}')P
                        LEFT JOIN USINSIV01."P_CLIENTS" PC ON P."CLIENT_ID" = PC."CLIENT_ID"
                        WHERE P."INSR_END" >= '{l_fecha_carga_inicial}' AND P."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' limit 100) AS TMP
                     '''

   #Ejecutar consulta
  
  l_df_polizas_insis = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_insis).load()

  print('ABAPOL USINSIV01 EXITOSO')

  l_df_polizas = l_df_polizas_vtime_lpg.union(l_df_polizas_vtime_lpv).union(l_df_polizas_insunix_lpg).union(l_df_polizas_insunix_lpv).union(l_df_polizas_insis)

  l_df_polizas = l_df_polizas.withColumn("VCAMBIO", coalesce(col("VCAMBIO").cast(DecimalType(7, 4)), lit(0))).withColumn("VTXCOMCB", col("VTXCOMCB").cast(DecimalType(7, 4))).withColumn("VMTCOMCB", col("VMTCOMCB").cast(DecimalType(12, 2))).withColumn("VTXCOMMD", col("VTXCOMMD").cast(DecimalType(7, 4))).withColumn("VMTCOMMD", col("VMTCOMMD").cast(DecimalType(12, 2))).withColumn("VCAPITAL", col("VCAPITAL").cast(DecimalType(14, 2))).withColumn("VMTCOMR", col("VMTCOMR").cast(DecimalType(12, 2))).withColumn("VMTCMNQP", col("VMTCMNQP").cast(DecimalType(12, 2))).withColumn("VMTPRMBR", col("VMTPRMBR").cast(DecimalType(12, 2))).withColumn("VTXCOSSG", col("VTXCOSSG").cast(DecimalType(7, 4))).withColumn("VTXRETEN", col("VTXRETEN").cast(DecimalType(7, 4))).withColumn("VMTCAPRE", col("VMTCAPRE").cast(DecimalType(12, 2))).withColumn("DQTDPART", col("DQTDPART").cast(DecimalType(5, 0)))

  return l_df_polizas