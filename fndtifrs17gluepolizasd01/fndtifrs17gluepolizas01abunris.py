from pyspark.sql.types import *
from pyspark.sql.functions import col

def getData(glueContext,connection,L_FECHA_INICIO,L_FECHA_FIN):

    L_ABUNRIS_INSUNIX_G_PES = f'''
                             (
                             (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(rol.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                              '' as TIOCTO,
                              'PIG' KGIORIGM,                                                  -- Indicador
                              coalesce(cast(rol.branch as varchar),'') || '-' || coalesce(cast(PC.PRODUCT as varchar),'')|| '-' || coalesce(cast(PC.SUB_PRODUCT as varchar),'') || '-' || coalesce(cast(rol.policy as varchar),'') ||  '-' || coalesce(cast(rol.certif as varchar),'') KABAPOL,  -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol.branch  and "SOURCESCHEMA" = 'usinsug01')*/'PES' KACTPRIS ,           -- Codigo del Tipo de riesgo
                              (select evi.scod_vt  FROM usinsug01.equi_vt_inx evi  WHERE evi.scod_inx  = rol.client)  DUNIRIS,                                                          -- Codigo de unidad de riesgo 
                              coalesce(cast(rol.EFFECDATE as varchar),'')TINCRIS,             -- Fecha de inicio de riesgo
                              coalesce(cast(rol.NULLDATE  as varchar),'') TVENCRI,            -- Fecha de fin de riesgo
                              '' TSITRIS,                                                     -- Fecha de estado de la unidad del riesgo
                              '' KACSITUR,                                                    -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPG' as DCOMPA,                                                    -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                     -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                     -- Numero de objetos asegurados 
                              0  as VCAPITAL,                                                     -- Importe de capital
                              '' as VMTPRABP,
                              0  as VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0  as VMTCOMR,                                                      -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                     -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                     -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                     -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                     -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                              from usinsug01.roles rol
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
							unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR 
                                 ON RTR."BRANCHCOM" = P.BRANCH 
                                 AND  RTR."RISKTYPEN" = 1 
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
                                   )) AS PC	
                             ON  ROL.USERCOMP = PC.USERCOMP 
                             AND ROL.COMPANY  = PC.COMPANY 
                             AND ROL.CERTYPE  = PC.CERTYPE
                             AND ROL.BRANCH   = PC.BRANCH 
                             AND ROL.POLICY   = PC.POLICY 
                             AND ROL.CERTIF   = PC.CERTIF  
                             AND ROL.EFFECDATE <= PC.EFFECDATE 
                             AND (ROL.NULLDATE IS NULL OR ROL.NULLDATE > PC.EFFECDATE)
                             AND ROL.ROLE IN (2,8) -- Asegurado , Asegurado adicional
                             AND PC.EFFECDATE BETWEEN  '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            limit 100
                             )
                             ) AS TMP
                             '''

    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_INX_G_PES")
    L_DF_ABUNRIS_INSUNIX_G_PES = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSUNIX_G_PES).load()
    print("2-TERMINO TABLA ABUNRIS_INX_G_PES")

    L_ABUNRIS_INSUNIX_G_PAT = f'''
                             (
                             (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(PC.EFFECDATE as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                              '' as TIOCTO,
                              'PIG' KGIORIGM,                                                  -- Indicador
                              coalesce(cast(ad.branch as varchar),'') || '-' || coalesce(cast(PC.PRODUCT as varchar),'')|| '-' || coalesce(cast(PC.SUB_PRODUCT as varchar),'') || '-' || coalesce(cast(ad.policy as varchar),'') ||  '-' || coalesce(cast(ad.certif as varchar),'') KABAPOL,  -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = ad.branch  and "SOURCESCHEMA" = 'usinsug01')*/'PAT' KACTPRIS ,           -- Codigo del Tipo de riesgo
                              coalesce(cast(ad.branch as varchar),'') || '-' || coalesce(cast(ad.policy as varchar),'') ||  '-' || coalesce(cast(ad.certif as varchar),'')  DUNIRIS,                                                          -- Codigo de unidad de riesgo 
                              coalesce(cast(PC.EFFECDATE as varchar),'')TINCRIS,             -- Fecha de inicio de riesgo
                              coalesce(cast(PC.NULLDATE  as varchar),'') TVENCRI,            -- Fecha de fin de riesgo
                              '' TSITRIS,                                                     -- Fecha de estado de la unidad del riesgo
                              '' KACSITUR,                                                    -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPG' as DCOMPA,                                                    -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                     -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                     -- Numero de objetos asegurados 
                              0  as VCAPITAL,                                                     -- Importe de capital
                              '' as VMTPRABP,
                              0  as VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0  as VMTCOMR,                                                      -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                     -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                     -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                     -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                     -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                              from usinsug01.address ad
                              JOIN ( SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, P.NULLDATE, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
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
							unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR 
                                 ON RTR."BRANCHCOM" = P.BRANCH 
                                 AND  RTR."RISKTYPEN" = 1 
                                 AND RTR."SOURCESCHEMA" = 'usinsug01'
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
                             ON  AD.USERCOMP = PC.USERCOMP 
                             AND AD.COMPANY  = PC.COMPANY 
                             AND AD.CERTYPE  = PC.CERTYPE
                             AND AD.BRANCH   = PC.BRANCH 
                             AND AD.POLICY   = PC.POLICY 
                             AND AD.CERTIF   = PC.CERTIF
                             AND PC.EFFECDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' LIMIT 100
                              limit 100
                             )
                             ) AS TMP
                             '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_INX_G_PAT")
    L_DF_ABUNRIS_INSUNIX_G_PAT = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSUNIX_G_PAT).load()
    print("2-TERMINO TABLA ABUNRIS_INX_G_PAT")

    L_ABUNRIS_INSUNIX_G_AUT = f'''
                             (
                             (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(tnb.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                              '' as TIOCTO,
                              'PIG' KGIORIGM,                                                  -- Indicador
                              coalesce(cast(tnb.branch as varchar),'')|| '-' || coalesce(cast(PC.PRODUCT as varchar),'')|| '-' || coalesce(cast(PC.SUB_PRODUCT as varchar),'') || '-' || coalesce(cast(tnb.policy as varchar),'') ||  '-' || coalesce(cast(tnb.certif as varchar),'') KABAPOL,  --Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = tnb.branch  and "SOURCESCHEMA" = 'usinsug01')*/'AUT' KACTPRIS ,     -- Codigo del Tipo de riesgo 
                              trim(TNB.REGIST)|| '-' || trim(TNB.CHASSIS)  DUNIRIS,           -- Codigo de Unidad de riesgo,                                                          -- Codigo de unidad de riesgo 
                              coalesce(cast(TNB.STARTDATE as varchar),'') TINCRIS,            -- Fecha de inicio del riesgo
                              coalesce(cast(TNB.EXPIRDAT as varchar),'')TVENCRI,              -- Fecha de vencimiento del riesgo
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPG' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados 
                              coalesce(cast(tnb.CAPITAL as numeric(14,2)),0) VCAPITAL,        -- Importe Capital asegurado
                              '' as VMTPRABP,
                              coalesce(cast(tnb.PREMIUM as numeric(12,2)),0) VMTPRMBR,        -- Importe de Prima Bruta
                              coalesce(cast(tnb.PREMIUM as numeric(12,2)),0) VMTCOMR,         -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                              From usinsug01.auto_peru tnb
                              JOIN ( SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, P.NULLDATE, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
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
							unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR 
                                 ON RTR."BRANCHCOM" = P.BRANCH 
                                 AND  RTR."RISKTYPEN" = 1 
                                 AND RTR."SOURCESCHEMA" = 'usinsug01'
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
                             ON  TNB.USERCOMP = PC.USERCOMP 
                             AND TNB.COMPANY  = PC.COMPANY 
                             AND TNB.CERTYPE  = PC.CERTYPE
                             AND TNB.BRANCH   = PC.BRANCH 
                             AND TNB.POLICY   = PC.POLICY 
                             AND TNB.CERTIF   = PC.CERTIF
                             AND TNB.EFFECDATE <= PC.EFFECDATE 
                             AND (TNB.NULLDATE IS NULL OR TNB.NULLDATE > PC.EFFECDATE)
                             AND PC.EFFECDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' LIMIT 100
                              limit 100
                             )
                             ) AS TMP
                             '''

    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_INX_G_AUT")
    L_DF_ABUNRIS_INSUNIX_G_AUT = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSUNIX_G_AUT).load()
    print("2-TERMINO TABLA ABUNRIS_INX_G_AUT")

    #UNION DE INSUNIX GENERAL
    L_DF_ABUNRIS_INX_G = L_DF_ABUNRIS_INSUNIX_G_PES.union(L_DF_ABUNRIS_INSUNIX_G_PAT).union(L_DF_ABUNRIS_INSUNIX_G_AUT)
    print("3-UNION INSUNIX G")

    L_ABUNRIS_INSUNIX_V_PES = f'''
                            (
                            (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(rol.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                              '' as TIOCTO,
                              'PIV' KGIORIGM,                                                  -- Indicador
                              coalesce(cast(rol.branch as varchar),'') || '-' || coalesce(cast(PC.PRODUCT as varchar),'')|| '-' || coalesce(cast(rol.policy as varchar),'') ||  '-' || coalesce(cast(rol.certif as varchar),'') KABAPOL,  -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol.branch  and "SOURCESCHEMA" = 'usinsug01')*/'PES' KACTPRIS ,           -- Codigo del Tipo de riesgo 
                              (select evi.scod_vt  FROM usinsug01.equi_vt_inx evi  WHERE evi.scod_inx  = rol.client)  DUNIRIS,                                                          -- Codigo de unidad de riesgo 
                              coalesce(cast(rol.effecdate as varchar),'') TINCRIS,            -- Fecha de inicio de riesgo
                              coalesce(cast(rol.NULLDATE  as varchar),'') TVENCRI,            -- Fecha de fin de riesgo
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPV' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados 
                              0 VCAPITAL,                                                     -- Importe Capital asegurado
                              '' as VMTPRABP,
                              0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0 VMTCOMR,                                                      -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                             from usinsuv01.roles rol
                             join( SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE , P.POLITYPE, CERT.EFFECDATE as EFFECDATE_CERT
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
							unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR 
							ON RTR."BRANCHCOM" = P.BRANCH 
							AND  RTR."RISKTYPEN" = 1 
							AND RTR."SOURCESCHEMA" = 'usinsuv01'
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
                                    ON  ROL.USERCOMP = PC.USERCOMP 
                             AND ROL.COMPANY  = PC.COMPANY 
                             AND ROL.CERTYPE  = PC.CERTYPE
                             AND ROL.BRANCH   = PC.BRANCH 
                             AND ROL.POLICY   = PC.POLICY 
                             AND ROL.CERTIF   = PC.CERTIF  
                             AND ROL.EFFECDATE <= PC.EFFECDATE 
                             AND (ROL.NULLDATE IS NULL OR ROL.NULLDATE > PC.EFFECDATE)
                             AND ROL.ROLE IN (2,8)
                             AND PC.EFFECDATE BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' LIMIT 100
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_INX_V_PES")
    L_DF_ABUNRIS_INSUNIX_V_PES = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSUNIX_V_PES).load()
    print("2-TERMINO TABLA ABUNRIS_INX_V_PES")
    
    L_ABUNRIS_VTIME_G_PES = f'''
                            (
                            (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'') TIOCFRM,                          -- Fecha de inicio de validez del registro
                              '' as TIOCTO,
                              'PVG' KGIORIGM,                                                                                -- Indicador
                              rol."NBRANCH" || '-' || PC."NPRODUCT" || '-' || rol."NPOLICY"  ||  '-' || rol."NCERTIF"  KABAPOL,                      -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01')*/'PES' KACTPRIS ,     -- Codigo del Tipo de riesgo 
                              rol."SCLIENT" DUNIRIS,                                                                         -- Codigo de unidad de riesgo 
                              coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'')  TINCRIS,                         -- Fecha de inicio de riesgo
                              coalesce(cast(cast(rol."DNULLDATE" as date) as VARCHAR),'')TVENCRI,                            -- Fecha de fin de riesgo
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPG' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados
                              0 VCAPITA,                                                      -- Importe de capital Asegurado
                              '' as VMTPRABP,
                              0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0 VMTCOMR,                                                      -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                             from usvtimg01."ROLES" rol
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
                                    ON  ROL."SCERTYPE"  = PC."SCERTYPE"
                                    AND ROL."NBRANCH"   = PC."NBRANCH" 
                                    AND ROL."NPRODUCT"  = PC."NPRODUCT"
                                    AND ROL."NPOLICY"   = PC."NPOLICY" 
                                    AND ROL."NCERTIF"   = PC."NCERTIF"  
                                    AND ROL."DEFFECDATE" <= PC."DSTARTDATE" 
                                    AND (ROL."DNULLDATE" IS NULL OR ROL."DNULLDATE" > PC."DSTARTDATE")
                                    WHERE ROL."NROLE" IN (2,8) 
                                    and cast(rol."DCOMPDATE" as date)  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                             limit 100
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_VTIME_G_PES")
    L_DF_ABUNRIS_VTIME_G_PES = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_VTIME_G_PES).load()
    print("2-TERMINO TABLA ABUNRIS_VTIME_G_PES")                            

    L_ABUNRIS_VTIME_G_PAT = f'''
                            (
                            (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(cast(ad."DEFFECDATE" as date) as varchar),'')  TIOCFRM,                         -- Fecha de inicio de validez del registro
                              '' as TIOCTO,
                              'PVG' KGIORIGM,                                                                                -- Indicador
                              ad."NBRANCH" || '-' || PC."NPRODUCT" || '-' || ad."NPOLICY" ||  '-' || ad."NCERTIF"  KABAPOL,                          -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = ad."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01' )*/'PAT' KACTPRIS ,     -- Codigo del Tipo de riesgo 
                              ad."SKEYADDRESS" DUNIRIS,                                                                      -- Codigo de unidad de riesgo  
                              coalesce(cast(cast(ad."DEFFECDATE" as date) as varchar),'')  TINCRIS,                          -- Fecha de Inicio del riesgo
                              coalesce(cast(ad."DNULLDATE" as VARCHAR),'')  TVENCRI,                                         -- Fecha de vencimiento del riesgo 
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPG' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados
                              0 VCAPITA,                                                      -- Importe de capital Asegurado
                              '' as VMTPRABP,
                              0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0 VMTCOMR,                                                      -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                            from usvtimg01."ADDRESS" ad
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
                                    ON  AD."SCERTYPE"  = PC."SCERTYPE"
                                    AND AD."NBRANCH"   = PC."NBRANCH" 
                                    AND AD."NPRODUCT"  = PC."NPRODUCT"
                                    AND AD."NPOLICY"   = PC."NPOLICY" 
                                    AND AD."NCERTIF"   = PC."NCERTIF"
                                    AND cast(ad."DCOMPDATE" as date)  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_VTIME_G_PAT")
    L_DF_ABUNRIS_VTIME_G_PAT = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_VTIME_G_PAT).load()
    print("2-TERMINO TABLA ABUNRIS_VTIME_G_PAT")  

    L_ABUNRIS_VTIME_G_AUT = f'''
                            (
                            (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(cast(aut."DEFFECDATE" as date) as varchar),'')  TIOCFRM,                        -- Fecha de inicio de validez de registro
                              '' as TIOCTO,
                              'PVG' KGIORIGM,                                                                               -- Indicador
                              aut."NBRANCH" || '-' ||  PC."NPRODUCT" || '-' || aut."NPOLICY" ||  '-' || aut."NCERTIF" KABAPOL,                      -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = aut."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01')*/ 'AUT' KACTPRIS ,     -- Codigo del Tipo de riesgo
                              coalesce(trim(aut."SREGIST"),'') || '-' || coalesce(trim(aut."SCHASSIS"),'')  DUNIRIS,        -- Codigo de Unidad de riesgo
                              coalesce(cast(cast(aut."DSTARTDATE"as date)as varchar),'')  TINCRIS,                          -- Fecha de inicio del riesgo
                              coalesce(cast(aut."DEXPIRDAT" as varchar),'')  TVENCRI,                                       -- Fecha de vencimiento del riesgo
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPG' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados
                              coalesce(cast(aut."NCAPITAL" as NUMERIC(14,2)),0) VCAPITAL,     -- Importe Capital asegurado
                              '' as VMTPRABP,
                              coalesce(cast(aut."NPREMIUM" as NUMERIC(12,2)),0) VMTPRMBR,     -- Importe de Prima Bruta
                              coalesce(cast(aut."NPREMIUM" as NUMERIC(12,2)),0) VMTCOMR,      -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                            From usvtimg01."AUTO" aut
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
                                    ON  AUT."SCERTYPE"  = PC."SCERTYPE"
                                    AND AUT."NBRANCH"   = PC."NBRANCH" 
                                    AND AUT."NPRODUCT"  = PC."NPRODUCT"
                                    AND AUT."NPOLICY"   = PC."NPOLICY" 
                                    AND AUT."NCERTIF"   = PC."NCERTIF"
                                    AND AUT."DEFFECDATE" <= PC."DSTARTDATE"
                                    AND (AUT."DNULLDATE" IS NULL OR AUT."DNULLDATE" > PC."DSTARTDATE")
                            where cast(aut."DCOMPDATE" as date)  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_VTIME_G_AUT")
    L_DF_ABUNRIS_VTIME_G_AUT = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_VTIME_G_AUT).load()
    print("2-TERMINO TABLA ABUNRIS_VTIME_G_AUT")  

    #UNION DE VISUALTIME GENERAL
    L_DF_ABUNRIS_VTIME_G = L_DF_ABUNRIS_VTIME_G_PES.union(L_DF_ABUNRIS_VTIME_G_PAT).union(L_DF_ABUNRIS_VTIME_G_AUT)
    print("3-UNION VTIME G")

    L_ABUNRIS_VTIME_V_PES = f'''
                            (
                            (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'')  TIOCFRM,                        -- Fecha de inicio de validez de registro
                              '' as TIOCTO,
                              'PVV' KGIORIGM,                                                                               -- Indicador
                              rol."NBRANCH" || '-' |  PC."NPRODUCT" || '-' || rol."NPOLICY" ||  '-' || rol."NCERTIF" KABAPOL,                      -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol."NBRANCH" and "SOURCESCHEMA" = 'usvtimv01' )*/ 'PES' KACTPRIS ,     -- Codigo del Tipo de riesgo  
                              rol."SCLIENT"    DUNIRIS,                                                                     -- Codigo de unidad de riesgo 
                              coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'') TINCRIS,                         -- Fecha de inicio de riesgo
                              coalesce(cast(rol."DNULLDATE" as VARCHAR),'')  TVENCRI,                                       -- Fecha de vencimiento del riesgo
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPV' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados
                              0 VCAPITA,                                                      -- Importe de capital
                              '' as VMTPRABP,
                              0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0 VMTCOMR,                                                      -- Importe de Prima Comercial 
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                             FROM USVTIMV01."ROLES" ROL
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
                                      AND p."DSTARTDATE" between '{P_FECHA_INICIO}' and '{P_FECHA_FIN}'
                                 )) AS PC	
                           ON  ROL."SCERTYPE"  = PC."SCERTYPE"
                           AND ROL."NBRANCH"   = PC."NBRANCH" 
                           AND ROL."NPRODUCT"  = PC."NPRODUCT"
                           AND ROL."NPOLICY"   = PC."NPOLICY" 
                           AND ROL."NCERTIF"   = PC."NCERTIF"  
                           AND ROL."DEFFECDATE" <= PC."DSTARTDATE" 
                           AND (ROL."DNULLDATE" IS NULL OR ROL."DNULLDATE" > PC."DSTARTDATE")
                           WHERE ROL."NROLE" IN (2,8) LIMIT 100) AS VTIME_LPV
                             limit 100
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_VTIME_V_PES")
    L_DF_ABUNRIS_VTIME_V_PES = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_VTIME_V_PES).load()
    print("2-TERMINO TABLA ABUNRIS_VTIME_V_PES")

    L_ABUNRIS_INSIS_V = f'''
                            (
                            (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          
                              '' as DTPREG,
                              '' as TIOCPROC,
                              cast(cast(io."INSR_BEGIN" as date)as varchar) as TIOCFRM,                        
                              '' as TIOCTO,
                              'PNV' KGIORIGM,                                         
                              cast(io."POLICY_ID" as varchar)  KABAPOL,                      
                              '' KACTPRIS,     
                              '' DUNIRIS,                                                                     
                              cast(cast(io."INSR_BEGIN" as date)as varchar) TINCRIS,                         
                              cast(cast(io."INSR_END" as date)as varchar) TVENCRI,      
                              '' as TSITRIS,                                                  
                              io."INSURED_OBJ_ID"  as KACSITUR,                                                 
                              '' as KACESQM,
                              'LPV' as DCOMPA,                                                
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              cast(cast(io."INSR_BEGIN" as date)as varchar) as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 
                              io."INSURED_VALUE"  VCAPITA,                                                      
                              '' as VMTPRABP,
                              0 VMTPRMBR,                                                     
                              0 VMTCOMR,                                                      
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL                              
                             from  usinsiv01."INSURED_OBJECT" io 
                             where cast(io."REGISTRATION_DATE" as date)  between  '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                             limit 100
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_INSIS")
    L_DF_ABUNRIS_INSIS_V = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSIS_V).load()
    print("2-TERMINO TABLA ABUNRIS_INSIS")


    #PERFORM THE UNION OPERATION
    L_DF_ABUNRIS = L_DF_ABUNRIS_INX_G.union(L_DF_ABUNRIS_INSUNIX_V_PES).union(L_DF_ABUNRIS_VTIME_G).union(L_DF_ABUNRIS_VTIME_V_PES).union(L_DF_ABUNRIS_INSIS_V)
    
    L_DF_ABUNRIS = L_DF_ABUNRIS.withColumn("DQOBJSEG",col("DQOBJSEG").cast(DecimalType(10,0))).withColumn("VCAPITAL",col("VCAPITAL").cast(DecimalType(14,2))).withColumn("VMTPRMBR",col("VMTPRMBR").cast(DecimalType(12,2))).withColumn("VMTCOMR",col("VMTCOMR").cast(DecimalType(12,2)))

    print("AQUI SE MANDE EL CONTEO")
    print(L_DF_ABUNRIS.count())

    return L_DF_ABUNRIS
