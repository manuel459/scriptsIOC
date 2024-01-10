from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

    l_fecha_carga_inicial = '2021-12-31'
      
    l_abcosseg_insunix_g = f'''
                             (
                              SELECT
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
                              JOIN ( 
                                     (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
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
                                        AND P.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                        AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}') )
                                        OR 
                                        (P.POLITYPE <> '1' -- COLECTIVAS 
                                        AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                        AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}')))
                                    )/*
                                        
                                    union
                                    
                                    (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
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
                             	     WHERE P.CERTYPE = '2' AND
                                            P.STATUS_POL NOT IN ('2', '3') AND
                                            (
                                              (P.POLITYPE = '1' AND  P.EXPIRDAT < '{l_fecha_carga_inicial}' or P.NULLDATE < '{l_fecha_carga_inicial}') and exists
                                                                                                                              (   select  1
                                                                                                                                    from  usinsug01.claim cla    
                                                                                                                                    join  usinsug01.claim_his clh 
                                                                                                                                    on   clh.usercomp = cla.usercomp 
                                                                                                                                    and  clh.company = cla.company 
                                                                                                                                    and  clh.claim = cla.claim
                                                                                                                                    where /*cla.usercomp = P.USERCOMP 
                                                                                                                                    and   cla.COMPANY = P.COMPANY  
                                                                                                                                    and  */ cla.branch = p.branch
                                                                                                                                    and   cla."policy" = p.policy
                                                                                                                                    and   trim(clh.oper_type) in 
                                                                                                                                                (	select 	cast(tcl.operation as varchar(2))
                                                                                                                                                      from 	usinsug01.tab_cl_ope tcl
                                                                                                                                                      where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1))
                                                                                                                                    and     clh.operdate >= '{l_fecha_carga_inicial}')
                                            )
                                        
                                     )
                                        
                                     union
                                     
                                     (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
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
                             	      WHERE P.CERTYPE = '2' AND
                                            P.STATUS_POL NOT IN ('2', '3') AND
                                            (
                                              (P.POLITYPE <> '1' AND CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  or  CERT.NULLDATE < '{l_fecha_carga_inicial}') and exists
                                                                                                                                    (   select  1
                                                                                                                                          from  usinsug01.claim cla    
                                                                                                                                          join  usinsug01.claim_his clh 
                                                                                                                                          on cla.usercomp = clh.usercomp  
                                                                                                                                          and cla.company = clh.company 
                                                                                                                                          and  clh.claim = cla.claim
                                                                                                                                          where /*cla.usercomp = CERT.USERCOMP 
                                                                                                                                          and   cla.COMPANY = CERT.COMPANY  
                                                                                                                                          and */  cla.branch = CERT.branch
                                                                                                                                          and   cla."policy" = CERT.policy
                                                                                                                                          and   cla.certif = CERT.certif
                                                                                                                                          and   trim(clh.oper_type) in 
                                                                                                                                                      (	select 	cast(tcl.operation as varchar(2))
                                                                                                                                                            from 	usinsug01.tab_cl_ope tcl
                                                                                                                                                            where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1))
                                                                                                                                          and     clh.operdate >= '{l_fecha_carga_inicial}') 
                                            )
                                     )*/
                                        
                                 ) AS PC	
                             ON  C.USERCOMP = PC.USERCOMP 
                             AND C.COMPANY  = PC.COMPANY 
                             AND C.CERTYPE  = PC.CERTYPE
                             AND C.BRANCH   = PC.BRANCH 
                             AND C.POLICY   = PC.POLICY 
                             AND C.EFFECDATE <= PC.EFFECDATE 
                             AND (C.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE)
                             AND C.EFFECDATE BETWEEN '{p_fecha_inicio}' and '{p_fecha_fin}'
                             limit 100
                             ) AS TMP
                             '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_INX_G")
    l_df_abcosseg_insunix_g = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcosseg_insunix_g).load()
    print("2-TERMINO TABLA ABCOSSEG_INX_G")

    l_abcosseg_insunix_v = f'''
                             (
                              SELECT
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
                                    JOIN (
                                                (
                                                SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE , P.POLITYPE, CERT.EFFECDATE as EFFECDATE_CERT
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
                                                AND P.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                                AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}') )
                                                OR 
                                                (P.POLITYPE <> '1' -- COLECTIVAS 
                                                AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                                AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}')))
                                                )/*
                                                union
                                                (
                                                SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE , P.POLITYPE, CERT.EFFECDATE as EFFECDATE_CERT
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
                                                SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE , P.POLITYPE, CERT.EFFECDATE as EFFECDATE_CERT
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
                                    ) AS PC	
                              ON  C.USERCOMP = PC.USERCOMP 
                              AND C.COMPANY  = PC.COMPANY 
                              AND C.CERTYPE  = PC.CERTYPE
                              AND C.BRANCH   = PC.BRANCH 
                              AND C.POLICY   = PC.POLICY 
                              AND C.EFFECDATE <= PC.EFFECDATE 
                              AND (C.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE) --1997-10-02	2020-11-02
                              AND C.EFFECDATE BETWEEN '{p_fecha_inicio}' and '{p_fecha_fin}'
                              limit 100
                             ) AS TMP
                             '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_INX_V")
    l_df_abcosseg_insunix_v = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcosseg_insunix_v).load()
    print("2-TERMINO TABLA ABCOSSEG_INX_V")
    
    l_abcosseg_vtime_g = f'''
                            (
                              SELECT 
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
                              JOIN (
                                          (
                                                SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
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
                                                      AND P."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                                      AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '{l_fecha_carga_inicial}') )
                                                      OR 
                                                      (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                                      AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                                      AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}')))
                                          )
                                          union
                                          (
                                                SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
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
                                                JOIN USVTIMG01."CLAIM" CLA ON CLA."SCERTYPE" = P."SCERTYPE" AND CLA."NBRANCH" = P."NBRANCH" AND CLA."NPRODUCT" = P."NPRODUCT" AND CLA."NPOLICY" = P."NPOLICY" and CLA."NCERTIF" = 0
                                                JOIN(
                                                SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMG01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                JOIN USVTIMG01."CLAIM_HIS" CLH ON COALESCE(CLH."NCLAIM", 0) > 0 AND CLH."NOPER_TYPE" = CSV."SVALUE" AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                                ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                                WHERE P."SCERTYPE" = '2'
                                                AND P."SSTATUS_POL" NOT IN ('2','3') 
                                                AND P."SPOLITYPE" = '1'
                                                AND (P."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR P."DNULLDATE" < '{l_fecha_carga_inicial}')
                                                AND P."DSTARTDATE" between '{p_fecha_inicio}' AND '{p_fecha_fin}'
                                          
                                          )
                                          union
                                          (
                                                SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
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
                                                JOIN USVTIMG01."CLAIM" CLA ON CLA."SCERTYPE" = CERT."SCERTYPE" AND CLA."NBRANCH" = CERT."NBRANCH" AND CLA."NPOLICY" = CERT."NPOLICY"  AND CLA."NCERTIF" =  CERT."NCERTIF"
                                                JOIN (
                                                      SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMG01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                      JOIN USVTIMG01."CLAIM_HIS" CLH ON COALESCE(CLH."NCLAIM", 0) > 0 AND CLH."NOPER_TYPE" = CSV."SVALUE" AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                                ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                                WHERE P."SCERTYPE" = '2'
                                                AND P."SSTATUS_POL" NOT IN ('2','3') 
                                                AND P."SPOLITYPE" <> '1' 
                                                AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}')
                                                AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                                          )
                                    ) AS PC	
                              ON  C."SCERTYPE"  = PC."SCERTYPE"
                              AND C."NBRANCH"   = PC."NBRANCH" 
                              AND C."NPRODUCT"  = PC."NPRODUCT"
                              AND C."NPOLICY"   = PC."NPOLICY"
                              AND C."DEFFECDATE" <= PC."DSTARTDATE" 
                              AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE") --0029-09-20	2019-12-17
                              limit 100
                            ) AS TMP
                           '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_VT")
    l_df_abcosseg_vtime_g = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcosseg_vtime_g).load()
    print("2-TERMINO TABLA ABCOSSEG_VT")    

    l_abcosseg_vtime_v = f'''
                            (
                              SELECT 
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
                              JOIN 
                              (
                                    (
                                          SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
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
                                                      AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '{l_fecha_carga_inicial}') )
                                                      OR 
                                                      (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                                      AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                                      AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}'))
                                                      AND P."DSTARTDATE" between '{p_fecha_inicio}' and '{p_fecha_fin}') --'2013-12-05'  '2013-12-10'
                                    )
                                    union
                                    (
                                          SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
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
                                          AND P."DSTARTDATE" between '{p_fecha_inicio}' AND '{p_fecha_fin}' --'2013-12-05'  '2013-12-10'
                                          
                                    )
                                    union
                                    (
                                          SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
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
                                          AND P."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' --'2013-12-05'  '2013-12-10'
                                    )
                              ) AS PC	
                              ON  C."SCERTYPE"  = PC."SCERTYPE"
                              AND C."NBRANCH"   = PC."NBRANCH" 
                              AND C."NPRODUCT"  = PC."NPRODUCT"
                              AND C."NPOLICY"   = PC."NPOLICY"  
                              AND C."DEFFECDATE" <= PC."DSTARTDATE" 
                              limit 100
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_VT")
    l_df_abcosseg_vtime_v = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcosseg_vtime_v).load()
    print("2-TERMINO TABLA ABCOSSEG_VT")
    
    #PERFORM THE UNION OPERATION 
    l_df_abcosseg = l_df_abcosseg_insunix_g.union(l_df_abcosseg_insunix_v).union(l_df_abcosseg_vtime_g).union(l_df_abcosseg_vtime_v)
    
    l_df_abcosseg = l_df_abcosseg.withColumn("VTXQUOTA",col("VTXQUOTA").cast(DecimalType(9,6))).withColumn("VTXQUOTA",col("VTXQUOTA").cast(DecimalType(9,6))).withColumn("VTXCOMCB",col("VTXCOMCB").cast(DecimalType(7,4))).withColumn("VTXCOMMD",col("VTXCOMMD").cast(DecimalType(7,4))).withColumn("VTXGESTAO",col("VTXGESTAO").cast(DecimalType(10,7)))

    print("AQUI SE MANDE EL CONTEO")
    print(l_df_abcosseg.count())

    return l_df_abcosseg