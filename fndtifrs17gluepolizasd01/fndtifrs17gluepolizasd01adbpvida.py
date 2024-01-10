def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

    l_fecha_carga_inicial = '2021-12-31'

    l_abdpvida_insunix_life = f'''
                              (
                                SELECT 
                                'D' AS INDDETREC,
                                'ABDPVIDA' AS TABLAIFRS17, 
                                COALESCE(A.BRANCH, 0) || '-' || COALESCE(A.PRODUCT, 0) || '-' || COALESCE(A.POLICY, 0) || '-' || COALESCE(A.CERTIF, 0) AS KABAPOL,
                                '' AS DTPREG,
                                '' AS TIOCPROC,
                                CAST(L.EFFECDATE AS VARCHAR) AS TIOCFRM,
                                '' AS TIOCTO,
                                'PIV' AS KGIORIGM,
                                '' AS DNUMPROP,
                                '' AS TINIPROP,
                                (SELECT  COUNT(DISTINCT R.CLIENT) FROM USINSUV01.ROLES R
                                  WHERE R.USERCOMP = 1
                                  AND R.COMPANY    = 1
                                  AND R.CERTYPE    = '2'
                                  AND R.BRANCH     = L.BRANCH
                                  AND R.POLICY     = L.POLICY
                                  AND R.EFFECDATE <= L.EFFECDATE
                                  AND (R.NULLDATE IS NULL OR R.NULLDATE > L.EFFECDATE)) AS DNPESSEG,
                                COALESCE(L.CAPITAL,0)  AS VMTCAPVD,
                                0 AS VMTCAPMT,
                                COALESCE(L.PREMIUM ,0) AS VMTPRCBP,
                                '' AS DOBSERV,
                                '' AS DDIAPAG,
                                '' AS DTABCOM,
                                '' AS DREGCAL,
                                '' AS DDURAPO,
                                '' AS KACTPPMA,
                                'LPV' AS DCOMPA,
                                '' AS DMARCA,
                                '' AS DPLPGVID,
                                '' AS DPLPGMOR,
                                '' AS KACFMDEC,
                                '' AS KACTPCRE,
                                '' AS KACMOCRE,
                                0 AS KACTPREN,
                                '' AS KACTPCRR,
                                '' AS KACTPGRE,
                                '' AS VTXCRPR,
                                '' AS VMTCRPR,
                                '' AS TINIPGRE,
                                '' AS TFIMPGRE,
                                '' AS VTXREVER,
                                '' AS VTXCRRE,
                                '' AS VTXSBPRE,
                                '' AS VTXSLPAR,
                                '' AS VTXAUCAPS,
                                '' AS TEMISREN,
                                '' AS KACINREIN,
                                '' AS VMTRENAN,
                                '' AS KACREINTE,
                                0 AS KACCLREN,
                                '' AS TFIMDIFE,
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
                                FROM USINSUV01.LIFE L 
                                INNER JOIN 
                                (
                                  (
                                    SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT,P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE
                                        FROM USINSUV01.POLICY P
                                        LEFT JOIN USINSUV01.CERTIFICAT CERT
                                        ON P.USERCOMP = CERT.USERCOMP
                                        AND P.COMPANY = CERT.COMPANY
                                        AND P.CERTYPE = CERT.CERTYPE 
                                        AND P.BRANCH  = CERT.BRANCH 
                                        AND P.POLICY  = CERT.POLICY
                                        JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR*/  
                                        (SELECT UNNEST(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                      'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                      'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                      'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
                                        UNNEST(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
                                      UNNEST(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR
                                      ON  RTR."BRANCHCOM" = P.BRANCH 
                                      AND RTR."RISKTYPEN" = 1 
                                      AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                      WHERE P.CERTYPE = '2'
                                        AND P.STATUS_POL NOT IN ('2','3') 
                                        AND ((P.POLITYPE = '1' -- INDIVIDUAL 
                                            AND P.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                            AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}') )
                                            OR 
                                            (P.POLITYPE <> '1' -- COLECTIVAS 
                                            AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                            AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}'))
                                            )  AND P.EFFECDATE between '{p_fecha_inicio}' and '{p_fecha_fin}'
                                  )/*
                                  union
                                  (
                                    SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT,P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE
                                        FROM USINSUV01.POLICY P
                                        LEFT JOIN USINSUV01.CERTIFICAT CERT
                                        ON P.USERCOMP = CERT.USERCOMP
                                        AND P.COMPANY = CERT.COMPANY
                                        AND P.CERTYPE = CERT.CERTYPE 
                                        AND P.BRANCH  = CERT.BRANCH 
                                        AND P.POLICY  = CERT.POLICY
                                        JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR*/  
                                        (SELECT UNNEST(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                      'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                      'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                      'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
                                        UNNEST(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
                                      UNNEST(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR
                                      ON  RTR."BRANCHCOM" = P.BRANCH 
                                      AND RTR."RISKTYPEN" = 1 
                                      AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                      WHERE P.CERTYPE = '2' and P.STATUS_POL NOT IN ('2', '3') AND
                                                                      (
                                                                            (P.POLITYPE = '1' AND  (P.EXPIRDAT < '{l_fecha_carga_inicial}' or P.NULLDATE < '{l_fecha_carga_inicial}')) and exists
                                                                                                                                                          (   select  1
                                                                                                                                                                from  usinsuv01.claim cla    
                                                                                                                                                                join  usinsuv01.claim_his clh 
                                                                                                                                                                on   clh.usercomp = cla.usercomp 
                                                                                                                                                                and  clh.company = cla.company 
                                                                                                                                                                and  clh.claim = cla.claim
                                                                                                                                                                where cla.usercomp = P.USERCOMP 
                                                                                                                                                                and   cla.COMPANY = P.COMPANY  
                                                                                                                                                                and   cla.branch = p.branch
                                                                                                                                                                and   cla.product = p.product
                                                                                                                                                                and   cla."policy" = p.policy
                                                                                                                                                                and   cla.certif = 0
                                                                                                                                                                and   trim(clh.oper_type) in 
                                                                                                                                                                            (	select 	cast(tcl.operation as varchar(2))
                                                                                                                                                                                  from 	usinsug01.tab_cl_ope tcl
                                                                                                                                                                                  where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1))
                                                                                                                                                                and     clh.operdate >= '{l_fecha_carga_inicial}')
                                                                      )
                                  )
                                  union
                                  (
                                    SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT,P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE
                                        FROM USINSUV01.POLICY P
                                        LEFT JOIN USINSUV01.CERTIFICAT CERT
                                        ON P.USERCOMP = CERT.USERCOMP
                                        AND P.COMPANY = CERT.COMPANY
                                        AND P.CERTYPE = CERT.CERTYPE 
                                        AND P.BRANCH  = CERT.BRANCH 
                                        AND P.POLICY  = CERT.POLICY
                                        JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR*/  
                                        (SELECT UNNEST(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                      'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                      'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                      'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
                                        UNNEST(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
                                      UNNEST(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR
                                      ON  RTR."BRANCHCOM" = P.BRANCH 
                                      AND RTR."RISKTYPEN" = 1 
                                      AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                      where P.CERTYPE = '2' AND
                                                P.STATUS_POL NOT IN ('2', '3') AND
                                                (
                                                (P.POLITYPE <> '1' AND (CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  or  CERT.NULLDATE < '{l_fecha_carga_inicial}')) and exists
                                                                                                                                          (   select  1
                                                                                                                                                from  usinsuv01.claim cla    
                                                                                                                                                join  usinsuv01.claim_his clh 
                                                                                                                                                on cla.usercomp = clh.usercomp  
                                                                                                                                                and cla.company = clh.company 
                                                                                                                                                and  clh.claim = cla.claim
                                                                                                                                                where cla.usercomp = CERT.USERCOMP 
                                                                                                                                                and   cla.COMPANY = CERT.COMPANY  
                                                                                                                                                and   cla.branch = CERT.branch
                                                                                                                                                and   cla."policy" = CERT.policy
                                                                                                                                                and   cla.certif = CERT.certif
                                                                                                                                                and   trim(clh.oper_type) in 
                                                                                                                                                            (	select 	cast(tcl.operation as varchar(2))
                                                                                                                                                                  from 	usinsug01.tab_cl_ope tcl
                                                                                                                                                                  where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1))
                                                                                                                                                and     clh.operdate >= '{l_fecha_carga_inicial}') 
                                                )
                                  )*/
                                )  a --1,847
                                ON  L.USERCOMP  = A.USERCOMP
                                AND L.COMPANY   = A.COMPANY
                                AND L.CERTYPE   = A.CERTYPE
                                AND L.BRANCH    = A.BRANCH
                                AND L.POLICY  = A.POLICY 
                                AND L.CERTIF    = A.CERTIF limit 100
                              ) AS TMP         
                              '''

    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABDPVIDA_IN_LIFE")
    l_df_abdpvida_insunix_life = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abdpvida_insunix_life).load()
    print("2-TERMINO TABLA ABDPVIDA_IN_LIFE")
    
    l_abdpvida_insunix_life_prev = f'''
                                   (
                                    select 
                                      'D' as INDDETREC,
                                      'ABDPVIDA' as TABLAIFRS17, 
                                      coalesce(a.branch, 0) || '-' || coalesce(a.product, 0) || '-' || coalesce(a.policy, 0) || '-' || coalesce(a.certif,0) as KABAPOL,
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
                                      inner join 
                                      (
                                        (
                                          SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT,P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE  
                                                FROM usinsuv01.POLICY P
                                            LEFT JOIN USINSUV01.CERTIFICAT CERT
                                            ON P.USERCOMP = CERT.USERCOMP
                                            AND P.COMPANY = CERT.COMPANY
                                            AND P.CERTYPE = CERT.CERTYPE 
                                            AND P.BRANCH  = CERT.BRANCH 
                                            AND P.POLICY  = CERT.policy
                                            JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                            ((SELECT UNNEST(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                          'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                          'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                          'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
                                                      UNNEST(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
                                                      UNNEST(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) as "RISKTYPEN")) RTR 
                                            ON  RTR."BRANCHCOM" = P.BRANCH 
                                            AND RTR."RISKTYPEN" = 1 
                                            AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                            WHERE P.CERTYPE = '2'
                                                AND P.STATUS_POL NOT IN ('2','3') 
                                                AND ( (P.POLITYPE = '1' -- INDIVIDUAL 
                                                    AND P.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                                    AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}') )
                                                    OR 
                                                    (P.POLITYPE <> '1' -- COLECTIVAS 
                                                    AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                                    AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}'))
                                              ) AND P.EFFECDATE  between '{p_fecha_inicio}' and '{p_fecha_fin}'
                                        )/*
                                        union
                                        (
                                          SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT,P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE  
                                                FROM usinsuv01.POLICY P
                                            LEFT JOIN USINSUV01.CERTIFICAT CERT
                                            ON P.USERCOMP = CERT.USERCOMP
                                            AND P.COMPANY = CERT.COMPANY
                                            AND P.CERTYPE = CERT.CERTYPE 
                                            AND P.BRANCH  = CERT.BRANCH 
                                            AND P.POLICY  = CERT.policy
                                            JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                            ((SELECT UNNEST(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                          'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                          'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                          'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
                                                      UNNEST(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
                                                      UNNEST(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) as "RISKTYPEN")) RTR 
                                            ON  RTR."BRANCHCOM" = P.BRANCH 
                                            AND RTR."RISKTYPEN" = 1 
                                            AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                            WHERE P.CERTYPE = '2' and P.STATUS_POL NOT IN ('2', '3') AND
                                                                            (
                                                                                  (P.POLITYPE = '1' AND  (P.EXPIRDAT < '{l_fecha_carga_inicial}' or P.NULLDATE < '{l_fecha_carga_inicial}' )) and exists
                                                                                                                                                                (   select  1
                                                                                                                                                                      from  usinsuv01.claim cla    
                                                                                                                                                                      join  usinsuv01.claim_his clh 
                                                                                                                                                                      on   clh.usercomp = cla.usercomp 
                                                                                                                                                                      and  clh.company = cla.company 
                                                                                                                                                                      and  clh.claim = cla.claim
                                                                                                                                                                      where cla.usercomp = P.USERCOMP 
                                                                                                                                                                      and   cla.COMPANY = P.COMPANY  
                                                                                                                                                                      and   cla.branch = p.branch
                                                                                                                                                                      and   cla.product = p.product
                                                                                                                                                                      and   cla."policy" = p.policy
                                                                                                                                                                      and   cla.certif = 0
                                                                                                                                                                      and   trim(clh.oper_type) in 
                                                                                                                                                                                  (	select 	cast(tcl.operation as varchar(2))
                                                                                                                                                                                        from 	usinsug01.tab_cl_ope tcl
                                                                                                                                                                                        where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1))
                                                                                                                                                                      and     clh.operdate >= '{l_fecha_carga_inicial}' )
                                                                            )
                                        )
                                        union
                                        (
                                          SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT,P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE  
                                                FROM usinsuv01.POLICY P
                                            LEFT JOIN USINSUV01.CERTIFICAT CERT
                                            ON P.USERCOMP = CERT.USERCOMP
                                            AND P.COMPANY = CERT.COMPANY
                                            AND P.CERTYPE = CERT.CERTYPE 
                                            AND P.BRANCH  = CERT.BRANCH 
                                            AND P.POLICY  = CERT.policy
                                            JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                            ((SELECT UNNEST(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                          'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                          'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
                                                          'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
                                                      UNNEST(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
                                                      UNNEST(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) as "RISKTYPEN")) RTR 
                                            ON  RTR."BRANCHCOM" = P.BRANCH 
                                            AND RTR."RISKTYPEN" = 1 
                                            AND RTR."SOURCESCHEMA" = 'usinsuv01' 
                                            WHERE P.CERTYPE = '2' AND
                                                      P.STATUS_POL NOT IN ('2', '3') AND
                                                      (
                                                      (P.POLITYPE <> '1' AND (CERT.EXPIRDAT < '{l_fecha_carga_inicial}'   or  CERT.NULLDATE < '{l_fecha_carga_inicial}')) and exists
                                                                                                                                                (   select  1
                                                                                                                                                      from  usinsuv01.claim cla    
                                                                                                                                                      join  usinsuv01.claim_his clh 
                                                                                                                                                      on cla.usercomp = clh.usercomp  
                                                                                                                                                      and cla.company = clh.company 
                                                                                                                                                      and  clh.claim = cla.claim
                                                                                                                                                      where cla.usercomp = CERT.USERCOMP 
                                                                                                                                                      and   cla.COMPANY = CERT.COMPANY  
                                                                                                                                                      and   cla.branch = CERT.branch
                                                                                                                                                      and   cla."policy" = CERT.policy
                                                                                                                                                      and   cla.certif = CERT.certif
                                                                                                                                                      and   trim(clh.oper_type) in 
                                                                                                                                                                  (	select 	cast(tcl.operation as varchar(2))
                                                                                                                                                                        from 	usinsug01.tab_cl_ope tcl
                                                                                                                                                                        where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1))
                                                                                                                                                      and     clh.operdate >= '{l_fecha_carga_inicial}') 
                                                      )
                                        )*/
                                      )  a -- 15 344 466
                                      on lp.usercomp = a.usercomp
                                      and lp.company = a.company
                                      and lp.certype = a.certype
                                      and lp.branch = a.branch
                                      and lp.policy = a.policy
                                   ) AS TMP
                        '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABDPVIDA_LIFE_PREV")
    l_df_abdpvida_insunix_life_prev = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abdpvida_insunix_life_prev).load()
    print("2-TERMINO TABLA ABDPVIDA_LIFE_PREV")

    l_abdpvida_vtime_life = f'''
                            (
                              SELECT 
                                'D' AS INDDETREC,
                                'ABDPVIDA' AS TABLAIFRS17, 
                                A."NBRANCH" || '-' || A."NPRODUCT" || '-' || A."NPOLICY" || '-' || A."NCERTIF" AS KABAPOL,
                                '' AS DTPREG,
                                '' AS TIOCPROC,
                                CAST(CAST(L."DEFFECDATE" AS DATE)AS VARCHAR) AS TIOCFRM,
                                '' AS TIOCTO,
                                'PVV' AS KGIORIGM,
                                '' AS DNUMPROP,
                                '' AS TINIPROP,
                                L."NCOUNT_INSU" AS DNPESSEG,
                                COALESCE(L."NCAPITAL",0)  AS VMTCAPVD,
                                0 AS VMTCAPMT,
                                COALESCE(L."NPREMIUM",0) AS VMTPRCBP,
                                '' AS DOBSERV,
                                '' AS DDIAPAG,
                                '' AS DTABCOM,
                                '' AS DREGCAL,
                                '' AS DDURAPO,
                                '' AS KACTPPMA,
                                'LPV' AS DCOMPA,
                                '' AS DMARCA,
                                '' AS DPLPGVID,
                                '' AS DPLPGMOR,
                                '' AS KACFMDEC,
                                '' AS KACTPCRE,
                                '' AS KACMOCRE,
                                0 AS KACTPREN,
                                '' AS KACTPCRR,
                                '' AS KACTPGRE,
                                '' AS VTXCRPR,
                                '' AS VMTCRPR,
                                '' AS TINIPGRE,
                                '' AS TFIMPGRE,
                                '' AS VTXREVER,
                                '' AS VTXCRRE,
                                '' AS VTXSBPRE,
                                '' AS VTXSLPAR,
                                '' AS VTXAUCAPS,
                                '' AS TEMISREN,
                                '' AS KACINREIN,
                                '' AS VMTRENAN,
                                '' AS KACREINTE,
                                0 AS KACCLREN,
                                '' AS TFIMDIFE,
                                '' AS TVRENPAG,
                                '' AS KACPERISC,
                                '' AS DDURAPOMES,
                                '' AS KACPZREN,
                                '' AS KACTPDEF,
                                '' AS DPRAZODEF,
                                '' AS KACUNIDEF,
                                0 AS DDURRENDA,
                                '' AS VTXINDX,
                                '' AS DMESPST13,
                                '' AS DMESPST14,
                                '' AS KACPROVID,
                                '' AS DMESPROVID,
                                '' AS KACPRVIDSC,
                                '' AS KACPRVIDSN,
                                '' AS KACESTREN,
                                '' AS TDTESTREN,
                                '' AS KACTPGRE_FR,
                                '' AS VMTCAPANREN,
                                '' AS DCDTRAT_SO,
                                '' AS VTXRSSREN,
                                '' AS VMTPLENO,
                                '' AS VMTCAPRSS
                                FROM USVTIMV01."LIFE" L 
                                INNER JOIN 
                                (
                                  (
                                  SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE"  
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
                                      AND P."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                      AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '{l_fecha_carga_inicial}' ) )
                                      OR 
                                      (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                      AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                      AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}' )))
                                  AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' and '{p_fecha_fin}'
                                )
                                union 
                                (
                                  SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE"  
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
                                          JOIN USVTIMV01."CLAIM" CLA ON CLA."SCERTYPE" = P."SCERTYPE" AND CLA."NBRANCH" = P."NBRANCH" AND CLA."NPRODUCT" = P."NPRODUCT" AND CLA."NPOLICY" = P."NPOLICY" and CLA."NCERTIF" = 0
                                            JOIN(
                                                SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMV01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                JOIN USVTIMV01."CLAIM_HIS" CLH ON COALESCE(CLH."NCLAIM", 0) > 0 AND CLH."NOPER_TYPE" = CSV."SVALUE" AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                                ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                            WHERE P."SCERTYPE" = '2' 
                                              AND P."SSTATUS_POL" NOT IN ('2','3') 
                                              AND P."SPOLITYPE" = '1' 
                                              AND (P."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR P."DNULLDATE" < '{l_fecha_carga_inicial}')
                                              AND P."DSTARTDATE" between '{p_fecha_inicio}' AND '{p_fecha_fin}'
                                  
                                )
                                union
                                (
                                  SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE"  
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
                                          JOIN USVTIMV01."CLAIM" CLA ON CLA."SCERTYPE" = CERT."SCERTYPE" AND CLA."NBRANCH" = CERT."NBRANCH" AND CLA."NPOLICY" = CERT."NPOLICY"  AND CLA."NCERTIF" =  CERT."NCERTIF"
                                            JOIN (
                                                  SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMV01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                  JOIN USVTIMV01."CLAIM_HIS" CLH ON COALESCE(CLH."NCLAIM", 0) > 0 AND CLH."NOPER_TYPE" = CSV."SVALUE" AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                                ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                            WHERE P."SCERTYPE" = '2'
                                            AND P."SSTATUS_POL" NOT IN ('2','3') 
                                            AND P."SPOLITYPE" <> '1' 
                                            AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}')
                                            AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                                )
                              ) A
                                ON  L."SCERTYPE"  = A."SCERTYPE"
                                AND L."NBRANCH"   = A."NBRANCH" 
                                AND L."NPRODUCT"  = A."NPRODUCT"
                                AND L."NPOLICY"   = A."NPOLICY" 
                                AND L."NCERTIF"   = A."NCERTIF"  
                                AND L."DEFFECDATE" <= A."DSTARTDATE" 
                                AND (L."DNULLDATE" IS NULL OR L."DNULLDATE" > A."DSTARTDATE") 
                            ) AS TMP
                        '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABDPVIDA_INS")
    l_df_abdpvida_vtime_life = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abdpvida_vtime_life).load()
    print("2-TERMINO TABLA ABDPVIDA_INS")
    
    #PERFORM THE UNION OPERATION
    l_df_abdpvida = l_df_abdpvida_insunix_life.union(l_df_abdpvida_insunix_life_prev).union(l_df_abdpvida_vtime_life)

    return l_df_abdpvida

