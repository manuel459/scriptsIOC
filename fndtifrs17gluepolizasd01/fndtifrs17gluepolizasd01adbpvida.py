def getData(glueContext,connection,L_FECHA_INICIO,L_FECHA_FIN):
    L_ABDPVIDA_INSUNIX_LIFE = f'''
                                   (
                                   (select 
                                   'D' as INDDETREC,
                                   'ABDPVIDA' as TABLAIFRS17, 
                                   a.branch || '-' || a.product || '-' || a.policy || '-' || a.certif as KABAPOL,
                                   '' as DTPREG,
                                   '' as TIOCPROC,
                                   cast(l.effecdate as varchar) as TIOCFRM,
                                   '' as TIOCTO,
                                   'PIV' as KGIORIGM,
                                   '' as DNUMPROP,
                                   '' as TINIPROP,
                                   (select  count(distinct r.client) from usinsuv01.roles r
                                     where r.usercomp = 1
                                       and r.company = 1
                                       and r.certype = '2'
                                       and r.branch = l.branch
                                       and r.policy = l.policy
                                       and r.effecdate <= l.effecdate
                                       and (r.nulldate is null or r.nulldate > l.effecdate)
                                   ) AS DNPESSEG,
                                   coalesce(l.capital,0)  as VMTCAPVD,
                                   0 as VMTCAPMT,
                                   coalesce(l.premium ,0) as VMTPRCBP,
                                   '' as DOBSERV,
                                   '' as DDIAPAG,
                                   '' as DTABCOM,
                                   '' as DREGCAL,
                                   '' as DDURAPO,
                                   '' as KACTPPMA,
                                   'LPV' as DCOMPA,
                                   '' as DMARCA,
                                   '' as DPLPGVID,
                                   '' as DPLPGMOR,
                                   '' as KACFMDEC,
                                   '' as KACTPCRE,
                                   '' as KACMOCRE,
                                   0 as KACTPREN,
                                   '' as KACTPCRR,
                                   '' as KACTPGRE,
                                   '' as VTXCRPR,
                                   '' as VMTCRPR,
                                   '' as TINIPGRE,
                                   '' as TFIMPGRE,
                                   '' as VTXREVER,
                                   '' as VTXCRRE,
                                   '' as VTXSBPRE,
                                   '' as VTXSLPAR,
                                   '' as VTXAUCAPS,
                                   '' as TEMISREN,
                                   '' as KACINREIN,
                                   '' as VMTRENAN,
                                   '' as KACREINTE,
                                   0 as KACCLREN,
                                   '' as TFIMDIFE,
                                   '' as TVRENPAG,
                                   '' as KACPERISC,
                                   '' as DDURAPOMES,
                                   '' as KACPZREN,
                                   '' as KACTPDEF,
                                   '' as DPRAZODEF,
                                   '' as KACUNIDEF,
                                   0 as DDURRENDA,
                                   '' as VTXINDX,
                                   '' as DMESPST13,
                                   '' as DMESPST14,
                                   '' as KACPROVID,
                                   '' as DMESPROVID,
                                   '' as KACPRVIDSC,
                                   '' as KACPRVIDSN,
                                   '' as KACESTREN,
                                   '' as TDTESTREN,
                                   '' as KACTPGRE_FR,
                                   '' as VMTCAPANREN,
                                   '' as DCDTRAT_SO,
                                   '' as VTXRSSREN,
                                   '' as VMTPLENO,
                                   '' as VMTCAPRSS
                                   from usinsuv01.life l 
                                   inner join (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT,P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE
                                   FROM usinsuv01.POLICY P
                            	   LEFT JOIN USINSUV01.CERTIFICAT CERT
                            	   ON P.USERCOMP = CERT.USERCOMP
                            	   AND P.COMPANY = CERT.COMPANY
                            	   AND P.CERTYPE = CERT.CERTYPE 
                            	   AND P.BRANCH  = CERT.BRANCH 
                            	   AND P.POLICY  = CERT.policy
                            	   JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                            	   ON  RTR."BRANCHCOM" = P.BRANCH 
                            	   AND RTR."RISKTYPEN" = 1 
                            	   AND RTR."SOURCESCHEMA" = 'usinsuv01'
                            	   WHERE P.CERTYPE = '2'
                                   AND P.STATUS_POL NOT IN ('2','3') 
                                   AND ((P.POLITYPE = '1' -- INDIVIDUAL 
                                       AND P.EXPIRDAT >= '2015-12-31' 
                                       AND (P.NULLDATE IS NULL OR P.NULLDATE > '2015-12-31') )
                                       OR 
                                       (P.POLITYPE <> '1' -- COLECTIVAS 
                                       AND CERT.EXPIRDAT >= '2015-12-31' 
                                       AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2015-12-31'))
                                    )  AND P.EFFECDATE between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}' 
                                       )  a --1,847
                                   on  l.usercomp  = a.usercomp
                                   and l.company  = a.company
                                   and l.certype  = a.certype
                                   and l.branch   = a.branch
                                   and l."policy" = a.policy 
                                   and l.certif   = a.certif
                                   )
                                   --1994-02-16 - 2023-08-04
                                   ) AS TMP         
                          '''

    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABDPVIDA_IN_LIFE")
    L_DF_ABDPVIDA_INSUNIX_LIFE = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABDPVIDA_INSUNIX_LIFE).load()
    print("2-TERMINO TABLA ABDPVIDA_IN_LIFE")
    
    L_ABDPVIDA_INSUNIX_LIFE_PREV = f'''
                                   (
                                   (select 
                                   'D' as INDDETREC,
                                   'ABDPVIDA' as TABLAIFRS17, 
                                   a.branch || '-' || a.product || '-' || a.policy || '-' || a.certif as KABAPOL,
                                   '' as DTPREG,
                                   '' as TIOCPROC,
                                   cast(lp.effecdate as varchar) as TIOCFRM,
                                   '' as TIOCTO,
                                   'PIV' as KGIORIGM,
                                   '' as DNUMPROP,
                                   '' as TINIPROP,
                                   (select  count(distinct r.client) from usinsuv01.roles r
                                     where r.usercomp = 1
                                       and r.company = 1
                                       and r.certype = '2'
                                       and r.branch = lp.branch
                                       and r.policy = lp.policy
                                       and r.effecdate <= lp.effecdate
                                       and (r.nulldate is null or r.nulldate > lp.effecdate)
                                   ) AS DNPESSEG,
                                   coalesce(lp.capital,0)  as VMTCAPVD,
                                   0 as VMTCAPMT,
                                   coalesce(lp.premium ,0) as VMTPRCBP,
                                   '' as DOBSERV,
                                   '' as DDIAPAG,
                                   '' as DTABCOM,
                                   '' as DREGCAL,
                                   '' as DDURAPO,
                                   '' as KACTPPMA,
                                   'LPV' as DCOMPA,
                                   '' as DMARCA,
                                   '' as DPLPGVID,
                                   '' as DPLPGMOR,
                                   '' as KACFMDEC,
                                   '' as KACTPCRE,
                                   '' as KACMOCRE,
                                   coalesce(lp.rent_type,0) as KACTPREN,
                                   '' as KACTPCRR,
                                   '' as KACTPGRE,
                                   '' as VTXCRPR,
                                   '' as VMTCRPR,
                                   coalesce(cast(lp.pay_first as varchar),'')  as TINIPGRE,
                                   '' as TFIMPGRE,
                                   '' as VTXREVER,
                                   '' as VTXCRRE,
                                   '' as VTXSBPRE,
                                   '' as VTXSLPAR,
                                   '' as VTXAUCAPS,
                                   '' as TEMISREN,
                                   '' as KACINREIN,
                                   '' as VMTRENAN,
                                   '' as KACREINTE,
                                   coalesce(lp.rent_type,0)  as KACCLREN,
                                   COALESCE(CAST((lp.startdate + make_interval(years => lp.time_difer)) AS VARCHAR),CAST(lp.startdate AS VARCHAR)) AS TFIMDIFE,
                                   '' as TVRENPAG,
                                   '' as KACPERISC,
                                   '' as DDURAPOMES,
                                   '' as KACPZREN,
                                   '' as KACTPDEF,
                                   '' as DPRAZODEF,
                                   '' as KACUNIDEF,
                                   (coalesce(lp.time_difer ,0) + coalesce(lp.time_garant,0))  AS DDURRENDA,
                                   '' as VTXINDX,
                                   '' as DMESPST13,
                                   '' as DMESPST14,
                                   '' as KACPROVID,
                                   '' as DMESPROVID,
                                   '' as KACPRVIDSC,
                                   '' as KACPRVIDSN,
                                   '' as KACESTREN,
                                   '' as TDTESTREN,
                                   '' as KACTPGRE_FR,
                                   '' as VMTCAPANREN,
                                   '' as DCDTRAT_SO,
                                   '' as VTXRSSREN,
                                   '' as VMTPLENO,
                                   '' as VMTCAPRSS
                                   from usinsuv01.life_prev lp 
                                   inner join (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT,P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE  
                                          FROM usinsuv01.POLICY P
                                   	   LEFT JOIN USINSUV01.CERTIFICAT CERT
                                   	   ON P.USERCOMP = CERT.USERCOMP
                                   	   AND P.COMPANY = CERT.COMPANY
                                   	   AND P.CERTYPE = CERT.CERTYPE 
                                   	   AND P.BRANCH  = CERT.BRANCH 
                                   	   AND P.POLICY  = CERT.policy
                                   	   JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                                   	   ON  RTR."BRANCHCOM" = P.BRANCH 
                                   	   AND RTR."RISKTYPEN" = 1 
                                   	   AND RTR."SOURCESCHEMA" = 'usinsug01'
                                   	   WHERE P.CERTYPE = '2'
                                          AND P.STATUS_POL NOT IN ('2','3') 
                                          AND ( (P.POLITYPE = '1' -- INDIVIDUAL 
                                              AND P.EXPIRDAT >= '2021-12-31' 
                                              AND (P.NULLDATE IS NULL OR P.NULLDATE > '2021-12-31') )
                                              OR 
                                              (P.POLITYPE <> '1' -- COLECTIVAS 
                                              AND CERT.EXPIRDAT >= '2021-12-31' 
                                              AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2021-12-31'))
                                        ) AND P.EFFECDATE  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}' 
                                    )  a -- 15 344 466
                                   on lp.usercomp = a.usercomp
                                   and lp.company = a.company
                                   and lp.certype = a.certype
                                   and lp.branch = a.branch
                                   and lp.policy = a.policy
                                   )
                                   ) AS TMP
                        '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABDPVIDA_LIFE_PREV")
    L_DF_ABDPVIDA_INSUNIX_LIFE_PREV = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABDPVIDA_INSUNIX_LIFE_PREV).load()
    print("2-TERMINO TABLA ABDPVIDA_LIFE_PREV")

    L_ABDPVIDA_VTIME_LIFE = f'''
                                   (
                                   (select 
                                    'D' as INDDETREC,
                                    'ABDPVIDA' as TABLAIFRS17, 
                                    a."NBRANCH" || '-' || a."NPRODUCT" || '-' || a."NPOLICY" || '-' || a."NCERTIF" as KABAPOL,
                                    '' as DTPREG,
                                    '' as TIOCPROC,
                                    cast(cast(l."DEFFECDATE" as date)as varchar) as TIOCFRM,
                                    '' as TIOCTO,
                                    'PVV' as KGIORIGM,
                                    '' as DNUMPROP,
                                    '' as TINIPROP,
                                    l."NCOUNT_INSU" AS DNPESSEG,
                                    coalesce(l."NCAPITAL",0)  as VMTCAPVD,
                                    0 as VMTCAPMT,
                                    coalesce(l."NPREMIUM",0) as VMTPRCBP,
                                    '' as DOBSERV,
                                    '' as DDIAPAG,
                                    '' as DTABCOM,
                                    '' as DREGCAL,
                                    '' as DDURAPO,
                                    '' as KACTPPMA,
                                    'LPV' as DCOMPA,
                                    '' as DMARCA,
                                    '' as DPLPGVID,
                                    '' as DPLPGMOR,
                                    '' as KACFMDEC,
                                    '' as KACTPCRE,
                                    '' as KACMOCRE,
                                    0 as KACTPREN,
                                    '' as KACTPCRR,
                                    '' as KACTPGRE,
                                    '' as VTXCRPR,
                                    '' as VMTCRPR,
                                    '' as TINIPGRE,
                                    '' as TFIMPGRE,
                                    '' as VTXREVER,
                                    '' as VTXCRRE,
                                    '' as VTXSBPRE,
                                    '' as VTXSLPAR,
                                    '' as VTXAUCAPS,
                                    '' as TEMISREN,
                                    '' as KACINREIN,
                                    '' as VMTRENAN,
                                    '' as KACREINTE,
                                    0 as KACCLREN,
                                    '' as TFIMDIFE,
                                    '' as TVRENPAG,
                                    '' as KACPERISC,
                                    '' as DDURAPOMES,
                                    '' as KACPZREN,
                                    '' as KACTPDEF,
                                    '' as DPRAZODEF,
                                    '' as KACUNIDEF,
                                    0 as DDURRENDA,
                                    '' as VTXINDX,
                                    '' as DMESPST13,
                                    '' as DMESPST14,
                                    '' as KACPROVID,
                                    '' as DMESPROVID,
                                    '' as KACPRVIDSC,
                                    '' as KACPRVIDSN,
                                    '' as KACESTREN,
                                    '' as TDTESTREN,
                                    '' as KACTPGRE_FR,
                                    '' as VMTCAPANREN,
                                    '' as DCDTRAT_SO,
                                    '' as VTXRSSREN,
                                    '' as VMTPLENO,
                                    '' as VMTCAPRSS
                                    from usvtimv01."LIFE" l 
                                    inner join (
                                    SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE"  
                                                                      FROM USVTIMV01."POLICY" P 
                                                               	   LEFT JOIN USVTIMV01."CERTIFICAT" CERT 
                                                               	   ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                                               	   AND P."NBRANCH"  = CERT."NBRANCH"
                                                               	   AND P."NPRODUCT" = CERT."NPRODUCT"
                                                               	   AND P."NPOLICY"  = CERT."NPOLICY"
                                                               	   JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
                                                               	   WHERE P."SCERTYPE" = '2' 
                                                                      AND P."SSTATUS_POL" NOT IN ('2','3') 
                                                                      AND ( (P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                                                          AND P."DEXPIRDAT" >= '2019-12-31' 
                                                                          AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '2019-12-31') )
                                                                          OR 
                                                                          (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                                                          AND CERT."DEXPIRDAT" >= '2019-12-31' 
                                                                          AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '2019-12-31')))
                                                                      AND P."DSTARTDATE" BETWEEN '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                                    ) a
                                    ON  l."SCERTYPE"  = a."SCERTYPE"
                                    AND l."NBRANCH"   = a."NBRANCH" 
                                    AND l."NPRODUCT"  = a."NPRODUCT"
                                    AND l."NPOLICY"   = a."NPOLICY" 
                                    AND l."NCERTIF"   = a."NCERTIF"  
                                    AND l."DEFFECDATE" <= a."DSTARTDATE" 
                                    AND (l."DNULLDATE" IS NULL OR l."DNULLDATE" > a."DSTARTDATE")
                                   )
                                   ) AS TMP
                        '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABDPVIDA_INS")
    L_DF_ABDPVIDA_VTIME_LIFE = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABDPVIDA_VTIME_LIFE).load()
    print("2-TERMINO TABLA ABDPVIDA_INS")
    
    #PERFORM THE UNION OPERATION
    L_DF_ABDPVIDA = L_DF_ABDPVIDA_INSUNIX_LIFE.union(L_DF_ABDPVIDA_INSUNIX_LIFE_PREV).union(L_DF_ABDPVIDA_VTIME_LIFE)

    return L_DF_ABDPVIDA

