def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):
  L_ABCOBAP_INSUNIX = f'''
                      (
                        ( SELECT 
                          'D' AS INDDETREC,
                          'ABCOBAP' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TIOCFRM,
                          '' AS TIOCTO,
                          'PIG' AS KGIORIGM,
                          CAST(COALESCE(C.BRANCH, 0) AS VARCHAR) ||'-'||(
                              SELECT  COALESCE(P.PRODUCT, 0)
                              FROM  USINSUG01.POLICY P
                              WHERE P.USERCOMP = C.USERCOMP
                              AND P.COMPANY = C.COMPANY
                              AND P.CERTYPE = C.CERTYPE
                              AND P.BRANCH = C.BRANCH
                              AND P.POLICY = C.POLICY
                          ) || '-' || COALESCE(C.POLICY, 0) || '-' || COALESCE(C.CERTIF, 0) AS KABAPOL,
                          '' AS KABUNRIS,
                          COALESCE(( SELECT CAST(COALESCE(GC.COVERGEN, 0) AS VARCHAR) FROM USINSUG01.GEN_COVER GC 
                            WHERE GC.USERCOMP = C.USERCOMP 
                            AND GC.COMPANY = C.COMPANY 
                            AND GC.BRANCH = C.BRANCH 
                            AND GC.PRODUCT = (SELECT P.PRODUCT  FROM USINSUG01.POLICY P
                                              WHERE P.USERCOMP = C.USERCOMP
                                              AND P.COMPANY = C.COMPANY
                                              AND P.CERTYPE = C.CERTYPE
                                              AND P.BRANCH = C.BRANCH
                                              AND P.POLICY = C.POLICY
                                             )
                            AND GC.CURRENCY = C.CURRENCY
                            AND GC.MODULEC = C.MODULEC
                            AND GC.COVER =C.COVER 
                            AND GC.EFFECDATE <= C.EFFECDATE
                            AND (GC.NULLDATE IS NULL OR GC.NULLDATE > C.EFFECDATE) LIMIT 1
                          ) ,'0') AS KGCTPCBT,
                          COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TINICIO,
                          COALESCE (CAST(C.NULLDATE  AS VARCHAR),'') AS TTERMO,
                          '' AS TSITCOB,
                          '' AS KACSITCB,
                          '' AS VMTPRMSP,
                          COALESCE(C.PREMIUM, 0) AS VMTCOMR,
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
                          COALESCE(C.PREMIUM,0)  AS VMTPRMBR,
                          COALESCE(C.RATECOVE, 0) AS VTXCOB, --TASA APLICAR A LA COBERTURA
                          COALESCE(CAST(C.CAPITAL AS VARCHAR),'0') AS VCAPITAL,-- IMPORTE DE CAPITAL ASEGURADO EN EL CERTIFICADO 
                          '' AS VTXCAPIT,
                          '' AS KACTPIDX,
                          '' AS VTXINDX,
                          'LPG' AS DCOMPA,
                          '' AS DMARCA,
                          '' AS TDACECOB,
                          '' AS TDCANCOB,
                          '' AS TDCRICOB,
                          COALESCE(CAST (C.EFFECDATE AS VARCHAR),'') AS TDRENOVA,
                          '' AS TDVENTRA,
                          '' AS DHORAINI,
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
                          FROM USINSUG01.COVER C
                          WHERE C.COMPDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'
                          AND C.CERTYPE = '2')
                          UNION ALL
                          
                          ( SELECT 
                            'D' AS INDDETREC,
                            'ABCOBAP' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TIOCFRM,
                            '' AS TIOCTO,
                            'PIV' AS KGIORIGM,
                            COALESCE(C.BRANCH, 0) || '-'||(
                                SELECT  P.PRODUCT 
                                FROM  USINSUV01.POLICY P
                                WHERE P.USERCOMP = C.USERCOMP
                                AND P.COMPANY = C.COMPANY
                                AND P.CERTYPE = C.CERTYPE
                                AND P.BRANCH = C.BRANCH
                                AND P.POLICY = C.POLICY
                            ) || '-' ||  coalesce(C.policy, 0)|| '-' || coalesce(C.CERTIF, 0)  AS KABAPOL,
                            '' AS KABUNRIS,
                            COALESCE((SELECT COALESCE(CAST(GC.COVERGEN AS VARCHAR), '0') FROM USINSUV01.GEN_COVER GC 
                                      WHERE GC.USERCOMP = C.USERCOMP 
                                      AND GC.COMPANY = C.COMPANY 
                                      AND GC.BRANCH = C.BRANCH 
                                      AND GC.PRODUCT = (SELECT P.PRODUCT  FROM USINSUV01.POLICY P
                                                        WHERE P.USERCOMP = C.USERCOMP
                                                        AND P.COMPANY = C.COMPANY
                                                        AND P.CERTYPE = C.CERTYPE
                                                        AND P.BRANCH = C.BRANCH
                                                        AND P.POLICY = C.POLICY)
                            AND GC.CURRENCY = C.CURRENCY
                            AND GC.MODULEC = C.MODULEC
                            AND GC.COVER = C.COVER 
                            AND GC.EFFECDATE <= C.EFFECDATE
                            AND (GC.NULLDATE IS NULL OR GC.NULLDATE > C.EFFECDATE) LIMIT 1 --SUBPRODUCT COMPARTE COVERGEN
                            ), '0') AS KGCTPCBT,
                            COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TINICIO,
                            COALESCE (CAST(C.NULLDATE AS VARCHAR),'') AS TTERMO,
                            '' AS TSITCOB,
                            '' AS KACSITCB,
                            '' AS VMTPRMSP,
                            COALESCE(C.PREMIUM, 0) AS VMTCOMR,
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
                            COALESCE(C.PREMIUM,  0) AS VMTPRMBR,
                            COALESCE(C.RATECOVE, 0) AS VTXCOB, --TASA APLICAR A LA COBERTURA
                            COALESCE(cast(C.CAPITAL AS VARCHAR), '0') AS VCAPITAL,-- IMPORTE DE CAPITAL ASEGURADO EN EL CERTIFICADO 
                            '' AS VTXCAPIT,
                            '' AS KACTPIDX,
                            '' AS VTXINDX,
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
                            AND R.TYPE <> 1))), 0) AS VMTPRRES,
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
                            FROM USINSUV01.COVER C  
                            WHERE C.COMPDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'
                            AND C.CERTYPE = '2')
                      ) AS TMP
                      '''
 
  L_DF_ABCOBAP_INSUNIX = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_ABCOBAP_INSUNIX).load()
  #------------------------------------------------------------------------------------------------------------------------#
    
  L_ABCOBAP_VTIME = f'''
                    (
                     (SELECT 
                      'D' AS INDDETREC,
                      'ABCOBAP' AS TABLAIFRS17,
                      '' AS PK,
                      '' AS DTPREG,
                      '' AS TIOCPROC,
                      COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE ) AS VARCHAR),'') AS TIOCFRM,
                      '' AS TIOCTO,
                      'PVV' AS KGIORIGM,
                      C."NBRANCH" ||'-'|| C."NPRODUCT" ||'-'|| C."NPOLICY" ||'-'|| C."NCERTIF" AS KABAPOL,
                      '' AS KABUNRIS,
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
                      '' AS TSITCOB,
                      '' AS KACSITCB,
                      '' AS VMTPRMSP,
                      COALESCE(C."NPREMIUM_O", 0) AS VMTCOMR,
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
                      COALESCE(C."NPREMIUM_O", 0) AS VMTPRMBR,
                      COALESCE(C."NRATECOVE", 0) AS VTXCOB, --TASA APLICAR A LA COBERTURA
                      COALESCE(CAST(C."NCAPITAL" AS VARCHAR), '0') AS VCAPITAL,-- IMPORTE DE CAPITAL ASEGURADO EN EL CERTIFICADO 
                      '' AS VTXCAPIT,
                      '' AS KACTPIDX,
                      '' AS VTXINDX,
                      'LPV' AS DCOMPA,
                      '' AS DMARCA,
                      '' AS TDACECOB,
                      '' AS TDCANCOB,
                      '' AS TDCRICOB,
                      COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE )AS VARCHAR),'') AS TDRENOVA,
                      '' AS TDVENTRA,
                      '' AS DHORAINI,
                      COALESCE((CAST(((SELECT COALESCE (CO."NSHARE", 0)  FROM USVTIMV01."COINSURAN" CO
                        WHERE CO."SCERTYPE" = C."SCERTYPE" 
                        AND CO."NBRANCH" = C."NBRANCH" 
                        AND CO."NPOLICY" = C."NPOLICY"
                        AND CO."NCOMPANY" = 2
                        AND CO."DEFFECDATE"  <= C."DEFFECDATE"
                        AND (CO."DNULLDATE" IS NULL AND CO."DNULLDATE"  > C."DEFFECDATE")
                      ) * C."NPREMIUM") AS VARCHAR)), '100') AS VMTPREMC,
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
                      COALESCE(C."NCAPITALI", 0)  AS VMTCAPIN, -- IMPORTE DE CAPITAL DE LA COBERTURA A LA FECHA DE EMISION DE LA POLIZA,
                      COALESCE(TRUNC(C."NPREMIUM_O", 2), 0) AS VMTPREIN,
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
                      '' AS DIDADETAR,
                      '' AS DIDADLIMCOBA,
                      COALESCE(CAST(C."NTYPDURINS" AS VARCHAR),'0') AS KACTPDUR,
                      '' AS KGCRAMO_SAP,
                      '' AS KACTCOMP,
                      '' AS KACINDTX,
                      '' AS KACCALIDA,
                      '' AS DNCABCALP,
                      '' AS DINDNIVEL,
                      COALESCE(CAST(C."NDURINSUR" AS VARCHAR),'0') AS DURCOB,
                      '' AS DURPAGCOB,
                      '' AS KACTPDURCB,
                      '' AS DINCOBINDX,
                      '' AS KACGRCBT,
                      '' AS KABTRTAB_2,
                      '' AS VTXAJTBUA,
                      '' AS VMTCAPREM
                      FROM USVTIMV01."COVER" C
                      WHERE C."DCOMPDATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'
                      AND C."SCERTYPE" = '2')
                     
                     UNION ALL
                     (SELECT 
                      'D' AS INDDETREC,
                      'ABCOBAP' AS TABLAIFRS17,
                      '' AS PK,
                      '' AS DTPREG,
                      '' AS TIOCPROC,
                      COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE) AS VARCHAR),'') AS TIOCFRM,
                      '' AS TIOCTO,
                      'PVG' AS KGIORIGM,
                      C."NBRANCH" ||'-'|| C."NPRODUCT" ||'-'|| C."NPOLICY" ||'-'|| C."NCERTIF" AS KABAPOL,
                      '' AS KABUNRIS,
                      COALESCE((SELECT CAST(LC."NCOVERGEN" AS VARCHAR)  
                                 FROM USVTIMG01."GEN_COVER" LC 
                                 WHERE LC."NBRANCH" = C."NBRANCH" 
                                 AND LC."NPRODUCT" = C."NPRODUCT"
                                 AND LC."NMODULEC" = C."NMODULEC"
                                 AND LC."NCOVER" = C."NCOVER"
                                 AND LC."DEFFECDATE" <= C."DEFFECDATE" 
                                 AND (LC."DNULLDATE" IS NULL OR LC."DNULLDATE" > C."DEFFECDATE")), '') AS KGCTPCBT,
                      COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE) AS VARCHAR),'') AS TINICIO,
                      COALESCE(CAST(CAST(C."DNULLDATE" AS DATE) AS VARCHAR),'') AS TTERMO,
                      '' AS TSITCOB,
                      '' AS KACSITCB,
                      '' AS VMTPRMSP,
                      COALESCE(C."NPREMIUM_O",0) AS VMTCOMR,
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
                      COALESCE(C."NPREMIUM_O",0) AS VMTPRMBR,
                      COALESCE(C."NRATECOVE",0) AS VTXCOB, --TASA APLICAR A LA COBERTURA
                      COALESCE(CAST(C."NCAPITAL" AS VARCHAR), '0')  AS VCAPITAL,-- IMPORTE DE CAPITAL ASEGURADO EN EL CERTIFICADO 
                      '' AS VTXCAPIT,
                      '' AS KACTPIDX,
                      '' AS VTXINDX,
                      'LPG' AS DCOMPA,
                      '' AS DMARCA,
                      '' AS TDACECOB,
                      '' AS TDCANCOB,
                      '' AS TDCRICOB,
                      COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE )AS VARCHAR),'') AS TDRENOVA,
                      '' AS TDVENTRA,
                      '' AS DHORAINI,
                      COALESCE(CAST(((SELECT COALESCE(CO."NSHARE")  
                                      FROM USVTIMG01."COINSURAN" CO
                                      WHERE CO."SCERTYPE" = C."SCERTYPE" 
                                      AND CO."NBRANCH" = C."NBRANCH" 
                                      AND CO."NPOLICY" = C."NPOLICY"
                                      AND CO."NCOMPANY" = 2
                                      AND CO."DEFFECDATE"  <= C."DEFFECDATE"
                                      AND (CO."DNULLDATE" IS NULL AND CO."DNULLDATE"  > C."DEFFECDATE")) * C."NPREMIUM") AS VARCHAR), '100') AS VMTPREMC,
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
                      COALESCE(C."NCAPITALI", 0)  AS VMTCAPIN,
                      COALESCE(TRUNC(C."NPREMIUM_O", 2), 0) AS VMTPREIN,
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
                      '' AS DIDADETAR,
                      '' AS DIDADLIMCOBA,
                      COALESCE(CAST(C."NTYPDURINS" AS VARCHAR),'0') AS KACTPDUR,
                      '' AS KGCRAMO_SAP,
                      '' AS KACTCOMP,
                      '' AS KACINDTX,
                      '' AS KACCALIDA,
                      '' AS DNCABCALP,
                      '' AS DINDNIVEL,
                      COALESCE(CAST(C."NDURINSUR" AS VARCHAR),'0') AS DURCOB,
                      '' AS DURPAGCOB,
                      '' AS KACTPDURCB,
                      '' AS DINCOBINDX,
                      '' AS KACGRCBT,
                      '' AS KABTRTAB_2,
                      '' AS VTXAJTBUA,
                      '' AS VMTCAPREM
                      FROM USVTIMG01."COVER" C
                      WHERE C."DCOMPDATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'
                      AND C."SCERTYPE" = '2')                                                                         
                    ) AS TMP
                    '''
                    
    
  #EJECUTAR CONSULTA
  L_DF_ABCOBAP_VTIME = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_ABCOBAP_VTIME).load()
 
  #------------------------------------------------------------------------------------------------------------------------#  
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
                      WHERE GRC."REGISTRATION_DATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'
                    ) AS TMP
                    '''
    
    #EJECUTAR CONSULTA
  L_DF_ABCOBAP_INSIS = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_ABCOBAP_INSIS).load()
  #PERFORM THE UNION OPERATION
  L_DF_ABCOBAP = L_DF_ABCOBAP_INSUNIX.union(L_DF_ABCOBAP_VTIME).union(L_DF_ABCOBAP_INSIS)
  return L_DF_ABCOBAP