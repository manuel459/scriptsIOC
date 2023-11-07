#ABUNRIS

def getData(glueContext,connection,L_FECHA_INICIO,L_FECHA_FIN):

    L_ABUNRIS_INSUNIX_G = f'''
                             (
                             (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta 
                              coalesce(cast(rol.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                              coalesce(cast(rol.branch as varchar),'') || '-' || coalesce(cast(rol.policy as varchar),'') ||  '-' || coalesce(cast(rol.certif as varchar),'') KABAPOL,  --Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol.branch  and "SOURCESCHEMA" = 'usinsug01') KACTPRIS ,     -- Codigo del Tipo de riesgo 
                              (select evi.scod_vt  FROM usinsug01.equi_vt_inx evi  WHERE evi.scod_inx  = rol.client)  DUNIRIS,                                       -- Codigo de unidad de riesgo 
                              coalesce(cast(rol.EFFECDATE as varchar),'')TINCRIS,             -- Fecha de inicio de riesgo
                              coalesce(cast(rol.NULLDATE  as varchar),'') TVENCRI,            -- Fecha de fin de riesgo
                              '' TSITRIS,                                                     -- Fecha de estado de la unidad del riesgo
                              '' KACSITUR,                                                    -- Codigo de estad de la unidad del riesgo
                              'LPG' DCOMPA,                                                   -- Empresa a la que pertenece la informacion
                              '' KACTPOPS,                                                    -- Codigo del tipo de persona asegurada
                              '' TDRENOVA,                                                    -- Fecha de inicio de termino de la Unidad de riesgo
                              1 DQOBJSEG,                                                     --Numero de objetos asegurados 
                              /*  -- Por el momento no se usara el dato pues es un calculo usando tablas relacionadas
                              COALESCE(
                                  (
                                      SELECT CAST(MAX(co.capital) AS VARCHAR)
                                      FROM usinsug01.cover co
                                      LEFT JOIN usinsug01.gen_cover gc ON gc.usercomp = co.usercomp
                                                                      AND gc.company = co.company
                                                                      AND gc.branch = co.branch
                                                                      AND gc.product = (select po.product 
                                                                                          from usinsug01.policy po
                                                                                         where po.usercomp= co.usercomp 
                                                                                           and po.company = co.company 
                                                                                           and po.certype = co.certype 
                                                                                           and po.branch = co.branch
                                                                                           and po.policy = co.policy
                                                                                        )
                                                                      and gc.currency = co.currency 
                                                                      and gc.modulec = co.modulec
                                                                      AND gc.cover = co.cover
                                                                      and gc.effecdate <= rol.effecdate
                                                                      and (gc.nulldate is null or gc.nulldate > rol.effecdate)
                                      WHERE co.usercomp = 1
                                          AND co.company = 1
                                          AND co.certype = '2'
                                          AND co.branch = cl.branch
                                          AND co.'policy' = cl.'policy'
                                          AND co.certif = cl.certif
                                          AND co.effecdate <= rol.effecdate
                                          AND (co.nulldate IS NULL OR co.nulldate > rol.effecdate)
                                          AND gc.addsuini IN ('3')
                                  )
                                  ,
                                  (
                                      SELECT CAST(SUM(co.capital) AS VARCHAR)
                                      FROM usinsug01.cover co
                                      LEFT JOIN usinsug01.gen_cover gc ON gc.usercomp = co.usercomp
                                                                      AND gc.company = co.company
                                                                      AND gc.branch = co.branch
                                                                      AND gc.product = (select po.product 
                                                                                          from usinsug01.policy po
                                                                                         where po.usercomp= co.usercomp 
                                                                                           and po.company = co.company 
                                                                                           and po.certype = co.certype 
                                                                                           and po.branch = co.branch
                                                                                           and po.policy = co.policy
                                                                                        )
                                                                      and gc.currency = co.currency 
                                                                      and gc.modulec = co.modulec
                                                                      AND gc.cover = co.cover
                                                                      and gc.effecdate <= rol.effecdate
                                                                      and (gc.nulldate is null or gc.nulldate > rol.effecdate)
                                      WHERE co.usercomp = 1
                                        AND co.company = 1
                                        AND co.certype = '2'
                                        AND co.branch = cl.branch
                                        AND co.'policy' = cl.'policy'
                                        AND co.certif = cl.certif
                                        AND co.effecdate <= rol.effecdate
                                        AND (co.nulldate IS NULL OR co.nulldate > tbn.effecdate)
                                        AND (gc.addsuini is null or gc.addsuini = '1')
                                  )
                                  ,
                                  '0'
                              ) 
                              */
                              0 VCAPITAL,                                                     -- Importe de capital
                              0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0 VMTCOMR,                                                      -- Importe de Prima Comercial
                              '' VMTPREMC,                                                    -- Comision de la prima
                              '' VMTBOMAT,                                                    -- Monto de bonificacion
                              '' VMTBOCOM,                                                    -- Monto de bonificacion comercial
                              '' KACINDRE,                                                    -- Indicador de Reaseguro
                              'PIG' KGIORIGM
                              from usinsug01.roles rol
                              where rol.usercomp = 1
                              and rol.company = 1
                              and rol.certype  = '2'
                              and rol.branch in  (select "BRANCHCOM" from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "SOURCESCHEMA" = 'usinsug01' and  "RISKTYPEN" = 1 )
                              and rol.role in (2,8) -- Asegurado , Asegurado adicional
                              and rol.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                              limit 100
                             )
                             union all

                             (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta 
                              coalesce(cast(rol.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                              coalesce(cast(rol.branch as varchar),'') || '-' || coalesce(cast(rol.policy as varchar),'') ||  '-' || coalesce(cast(rol.certif as varchar),'') KABAPOL,  --Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol.branch  and "SOURCESCHEMA" = 'usinsug01') KACTPRIS ,     -- Codigo del Tipo de riesgo 
                              (select evi.scod_vt  FROM usinsug01.equi_vt_inx evi  WHERE evi.scod_inx  = rol.client)  DUNIRIS,                                       -- Codigo de unidad de riesgo 
                              coalesce(cast(rol.EFFECDATE as varchar),'')TINCRIS,             -- Fecha de inicio de riesgo
                              coalesce(cast(rol.NULLDATE  as varchar),'') TVENCRI,            -- Fecha de fin de riesgo
                              '' TSITRIS,                                                     -- Fecha de estado de la unidad del riesgo
                              '' KACSITUR,                                                    -- Codigo de estad de la unidad del riesgo
                              'LPG' DCOMPA,                                                   -- Empresa a la que pertenece la informacion
                              '' KACTPOPS,                                                    -- Codigo del tipo de persona asegurada
                              '' TDRENOVA,                                                    -- Fecha de inicio de termino de la Unidad de riesgo
                              1 DQOBJSEG,                                                   --Numero de objetos asegurados 
                              /*  -- Por el momento no se usara el dato pues es un calculo usando tablas relacionadas
                              COALESCE(
                                  (
                                      SELECT CAST(MAX(co.capital) AS VARCHAR)
                                      FROM usinsug01.cover co
                                      LEFT JOIN usinsug01.gen_cover gc ON gc.usercomp = co.usercomp
                                                                      AND gc.company = co.company
                                                                      AND gc.branch = co.branch
                                                                      AND gc.product = (select po.product 
                                                                                          from usinsug01.policy po
                                                                                         where po.usercomp= co.usercomp 
                                                                                           and po.company = co.company 
                                                                                           and po.certype = co.certype 
                                                                                           and po.branch = co.branch
                                                                                           and po.policy = co.policy
                                                                                        )
                                                                      and gc.currency = co.currency 
                                                                      and gc.modulec = co.modulec
                                                                      AND gc.cover = co.cover
                                                                      and gc.effecdate <= rol.effecdate
                                                                      and (gc.nulldate is null or gc.nulldate > rol.effecdate)
                                      WHERE co.usercomp = 1
                                          AND co.company = 1
                                          AND co.certype = '2'
                                          AND co.branch = cl.branch
                                          AND co.'policy' = cl.'policy'
                                          AND co.certif = cl.certif
                                          AND co.effecdate <= rol.effecdate
                                          AND (co.nulldate IS NULL OR co.nulldate > rol.effecdate)
                                          AND gc.addsuini IN ('3')
                                  )
                                  ,
                                  (
                                      SELECT CAST(SUM(co.capital) AS VARCHAR)
                                      FROM usinsug01.cover co
                                      LEFT JOIN usinsug01.gen_cover gc ON gc.usercomp = co.usercomp
                                                                      AND gc.company = co.company
                                                                      AND gc.branch = co.branch
                                                                      AND gc.product = (select po.product 
                                                                                          from usinsug01.policy po
                                                                                         where po.usercomp= co.usercomp 
                                                                                           and po.company = co.company 
                                                                                           and po.certype = co.certype 
                                                                                           and po.branch = co.branch
                                                                                           and po.policy = co.policy
                                                                                        )
                                                                      and gc.currency = co.currency 
                                                                      and gc.modulec = co.modulec
                                                                      AND gc.cover = co.cover
                                                                      and gc.effecdate <= rol.effecdate
                                                                      and (gc.nulldate is null or gc.nulldate > rol.effecdate)
                                      WHERE co.usercomp = 1
                                        AND co.company = 1
                                        AND co.certype = '2'
                                        AND co.branch = cl.branch
                                        AND co.'policy' = cl.'policy'
                                        AND co.certif = cl.certif
                                        AND co.effecdate <= rol.effecdate
                                        AND (co.nulldate IS NULL OR co.nulldate > tbn.effecdate)
                                        AND (gc.addsuini is null or gc.addsuini = '1')
                                  )
                                  ,
                                  '0'
                              ) 
                              */
                              0 VCAPITAL,                                                     -- Importe de capital
                              0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0 VMTCOMR,                                                      -- Importe de Prima Comercial
                              '' VMTPREMC,                                                    -- Comision de la prima
                              '' VMTBOMAT,                                                    -- Monto de bonificacion
                              '' VMTBOCOM,                                                    -- Monto de bonificacion comercial
                              '' KACINDRE,                                                    -- Indicador de Reaseguro
                              'PIG' KGIORIGM
                              from usinsug01.roles rol
                              where rol.usercomp = 1
                              and rol.company = 1
                              and rol.certype  = '2'
                              and rol.branch in  (select "BRANCHCOM" from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "SOURCESCHEMA" = 'usinsug01' and  "RISKTYPEN" = 2 )
                              and rol.role in (2,8) -- Asegurado , Asegurado adicional
                              and rol.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                              limit 100
                             )
                            union all 
                             (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta  
                              coalesce(cast(tnb.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                              coalesce(cast(tnb.branch as varchar),'') || '-' || coalesce(cast(tnb.policy as varchar),'') ||  '-' || coalesce(cast(tnb.certif as varchar),'') KABAPOL,  --Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = tnb.branch  and "SOURCESCHEMA" = 'usinsug01') KACTPRIS ,     -- Codigo del Tipo de riesgo 
                              trim(TNB.REGIST)|| '-' || trim(TNB.CHASSIS)  DUNIRIS,                                                          -- Codigo de Unidad de riesgo
                              coalesce(cast(TNB.STARTDATE as varchar),'') TINCRIS,                                                                                         -- Fecha de inicio del riesgo            
                              coalesce(cast(TNB.EXPIRDAT as varchar),'')TVENCRI,                                                                                          -- Fecha de vencimiento del riesgo        
                              '' TSITRIS,                                                                                                    -- Fecha de estado de la unidad de riesgo
                              '' KACSITUR,                                                                                                   -- Codigo del estado de la unidad de riesgo 
                              'LPG' DCOMPA,                                                                                                  -- Empresa a la que pertenece la informacion 
                              '' KACTPOPS,                                                    -- Codigo del tipo de Objeto de persona asegurada 
                              '' TDRENOVA,                                                    -- Fecha de inicio del termino UR 
                              1 DQOBJSEG,                                                    -- Numero de objetos asegurados
                              coalesce(cast(tnb.CAPITAL as numeric(14,2)),0) VCAPITAL,        -- Importe Capital asegurado
                              coalesce(cast(tnb.PREMIUM as numeric(12,2)),0) VMTPRMBR,        -- Importe de Prima Bruta 
                              coalesce(cast(tnb.PREMIUM as numeric(12,2)),0) VMTCOMR,         -- Importe de Prima Comercial
                              '' VMTPREMC,                                                    -- Comision de la prima
                              '' VMTBOMAT,                                                    -- Monto de bonificacion
                              '' VMTBOCOM,                                                    -- Monto de bonificacion comercial
                              '' KACINDRE,                                                    -- Indicador de Reaseguro
                              'PIG' KGIORIGM
                              From usinsug01.auto_peru tnb
                              where tnb.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                              limit 100
                             )
                             ) AS TMP
                             '''

    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_INX_G")
    L_DF_ABUNRIS_INSUNIX_G = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSUNIX_G).load()
    print("2-TERMINO TABLA ABUNRIS_INX_G")

    L_ABUNRIS_INSUNIX_V = f'''
                            (
                            (select 
                             'D' AS INDDETREC,
                             'ABUNRIS' AS TABLAIFRS17,
                             '' PK,                                                          -- Clave compuesta 
                             coalesce(cast(rol.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                             coalesce(cast(rol.branch as varchar),'') || '-' || coalesce(cast(rol.policy as varchar),'') ||  '-' || coalesce(cast(rol.certif as varchar),'') KABAPOL,  --Numero de Poliza
                             (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol.branch  and "SOURCESCHEMA" = 'usinsug01') KACTPRIS ,     -- Codigo del Tipo de riesgo 
                             (select evi.scod_vt  FROM usinsug01.equi_vt_inx evi  WHERE evi.scod_inx  = rol.client)  DUNIRIS,                                       -- Codigo de unidad de riesgo 
                             coalesce(cast(rol.effecdate as varchar),'') TINCRIS,            -- Fecha de inicio de riesgo
                             coalesce(cast(rol.NULLDATE  as varchar),'') TVENCRI,            -- Fecha de fin de riesgo
                             '' TSITRIS,                                                     -- Fecha de estado de la unidad del riesgo
                             '' KACSITUR,                                                    -- Codigo de estad de la unidad del riesgo
                             'LPV' DCOMPA,                                                   -- Empresa a la que pertenece la informacion
                             '' KACTPOPS,                                                    -- Codigo del tipo de persona asegurada
                             '' TDRENOVA,                                                    -- Fecha de inicio de termino de la Unidad de riesgo
                             1 DQOBJSEG,                                                     -- Numero de objetos asegurados
                             /*  -- Por el momento no se usara el dato pues es un calculo usando tablas relacionadas
                             COALESCE(
                                 (
                                     SELECT CAST(MAX(co.capital) AS VARCHAR)
                                     FROM usinsug01.cover co
                                     LEFT JOIN usinsug01.gen_cover gc ON gc.usercomp = co.usercomp
                                                                     AND gc.company = co.company
                                                                     AND gc.branch = co.branch
                                                                     AND gc.product = (select po.product 
                                                                                         from usinsug01.policy po
                                                                                        where po.usercomp= co.usercomp 
                                                                                          and po.company = co.company 
                                                                                          and po.certype = co.certype 
                                                                                          and po.branch = co.branch
                                                                                          and po.policy = co.policy
                                                                                       )
                                                                     and gc.currency = co.currency 
                                                                     and gc.modulec = co.modulec
                                                                     AND gc.cover = co.cover
                                                                     and gc.effecdate <= rol.effecdate
                                                                     and (gc.nulldate is null or gc.nulldate > rol.effecdate)
                                     WHERE co.usercomp = 1
                                         AND co.company = 1
                                         AND co.certype = '2'
                                         AND co.branch = cl.branch
                                         AND co.'policy' = cl.'policy'
                                         AND co.certif = cl.certif
                                         AND co.effecdate <= rol.effecdate
                                         AND (co.nulldate IS NULL OR co.nulldate > rol.effecdate)
                                         AND gc.addsuini IN ('3')
                                 )
                                 ,
                                 (
                                     SELECT CAST(SUM(co.capital) AS VARCHAR)
                                     FROM usinsug01.cover co
                                     LEFT JOIN usinsug01.gen_cover gc ON gc.usercomp = co.usercomp
                                                                     AND gc.company = co.company
                                                                     AND gc.branch = co.branch
                                                                     AND gc.product = (select po.product 
                                                                                         from usinsug01.policy po
                                                                                        where po.usercomp= co.usercomp 
                                                                                          and po.company = co.company 
                                                                                          and po.certype = co.certype 
                                                                                          and po.branch = co.branch
                                                                                          and po.policy = co.policy
                                                                                       )
                                                                     and gc.currency = co.currency 
                                                                     and gc.modulec = co.modulec
                                                                     AND gc.cover = co.cover
                                                                     and gc.effecdate <= rol.effecdate
                                                                     and (gc.nulldate is null or gc.nulldate > rol.effecdate)
                                     WHERE co.usercomp = 1
                                       AND co.company = 1
                                       AND co.certype = '2'
                                       AND co.branch = cl.branch
                                       AND co.'policy' = cl.'policy'
                                       AND co.certif = cl.certif
                                       AND co.effecdate <= rol.effecdate
                                       AND (co.nulldate IS NULL OR co.nulldate > tbn.effecdate)
                                       AND (gc.addsuini is null or gc.addsuini = '1')
                                 )
                                 ,
                                 '0'
                             ) 
                             */
                             0 VCAPITAL,                                                     -- Importe de capital
                             0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                             0 VMTCOMR,                                                      -- Importe de Prima Comercial 
                             '' VMTPREMC,                                                    -- Comision de la prima
                             '' VMTBOMAT,                                                    -- Monto de bonificacion
                             '' VMTBOCOM,                                                    -- Monto de bonificacion comercial
                             '' KACINDRE,                                                    -- Indicador de Reaseguro
                             'PIV' KGIORIGM
                             from usinsuv01.roles rol
                             where rol.usercomp = 1
                             and rol.company = 1
                             and rol.certype  = '2'
                             and rol.branch in  (select "BRANCHCOM" from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "SOURCESCHEMA" = 'usinsuv01' and  "RISKTYPEN" = 1 )
                             and rol.role in (2,8) -- Asegurado , Asegurado adicional
                             and rol.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            limit 100
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_INX_V")
    L_DF_ABUNRIS_INSUNIX_V = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSUNIX_V).load()
    print("2-TERMINO TABLA ABUNRIS_INX_V")
    
    L_ABUNRIS_VTIME_G = f'''
                            (
                            (select 
                             'D' as INDDETREC,
                             'ABUNRIS' as TABLAIFRS17, 
                             '' PK,                                                                                         -- Clave compuesta 
                             coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'') TIOCFRM,                          -- Fecha de inicio de validez del registro
                             rol."NBRANCH" || '-' || rol."NPOLICY"  ||  '-' || rol."NCERTIF"  KABAPOL,                      -- Numero de Poliza
                             (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01'  ) KACTPRIS ,     -- Codigo del Tipo de riesgo 
                             rol."SCLIENT" DUNIRIS,                                                                         -- Codigo de unidad de riesgo 
                             coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'')  TINCRIS,                         -- Fecha de inicio de riesgo
                             coalesce(cast(cast(rol."DNULLDATE" as date) as VARCHAR),'')TVENCRI,                            -- Fecha de fin de riesgo
                             '' TSITRIS,                                                                                    -- Fecha de estado de la unidad del riesgo
                             '' KACSITUR,                                                                                   -- Codigo de estad de la unidad del riesgo
                             'LPG' DCOMPA,                                                                                  -- Empresa a la que pertenece la informacion
                             '' KACTPOPS,                                                                                   -- Codigo del tipo de persona asegurada
                             '' TDRENOVA,                                                                                   -- Fecha de inicio de termino de la Unidad de riesgo
                             1 DQOBJSEG,                                                                                    -- Numero de objetos asegurados 
                             0 VCAPITA,                                                                                     -- Importe de capital Asegurado
                             0 VMTPRMBR,                                                                                    -- Importe de Prima Bruta
                             0 VMTCOMR,                                                                                     -- Importe de Prima Comercial  
                             '' VMTPREMC,                                                                                   -- Comision de la prima
                             '' VMTBOMAT,                                                                                   -- Monto de bonificacion
                             '' VMTBOCOM,                                                                                   -- Monto de bonificacion comercial
                             '' KACINDRE,                                                                                   -- Indicador de Reaseguro
                             'PVG' KGIORIGM
                             from usvtimg01."ROLES" rol
                             where rol."SCERTYPE" = '2'
                             and rol."NBRANCH"  in  (select "BRANCHCOM" from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "SOURCESCHEMA" = 'usvtimg01' and  "RISKTYPEN" = 1 )
                             and rol."NROLE" in (2,8) -- Asegurado , Asegurado adicional
                             and cast(rol."DCOMPDATE" as date)  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
                            union all
                            (
                            select 
                            'D' as INDDETREC,
                            'ABUNRIS' as TABLAIFRS17, 
                            '' PK,                                                                                        -- Clave compuesta 
                            coalesce(cast(cast(ad."DEFFECDATE" as date) as varchar),'')  TIOCFRM,                         -- Fecha de inicio de validez del registro
                            ad."NBRANCH" || '-' || ad."NPOLICY" ||  '-' || ad."NCERTIF"  KABAPOL,                         -- Numero de Poliza
                            (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = ad."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01' ) KACTPRIS ,     -- Codigo del Tipo de riesgo 
                            ad."SKEYADDRESS" DUNIRIS,                                                                     -- Codigo de unidad de riesgo 
                            coalesce(cast(cast(ad."DEFFECDATE" as date) as varchar),'')  TINCRIS,                         -- Fecha de Inicio del riesgo
                            coalesce(cast(ad."DNULLDATE" as VARCHAR),'')  TVENCRI,                                                                      -- Fecha de vencimiento del riesgo 
                            '' TSITRIS,                                                                                   -- Fecha de estado de la unidad de riesgo 
                            '' KACSITUR,                                                                                  -- Codigo del estado de la unidad de riesgo
                            'LPG' DCOMPA,                                                                                 -- Empresa a la que pertenece la informacion
                            '' KACTPOPS,                                                                                  -- Codigo del tipo de Objeto de persona asegurada 
                            '' TDRENOVA,                                                                                  -- Fecha de inicio del termino UR 
                            1 DQOBJSEG,                                                                                   -- Numero de objetos asegurados 
                            0 VCAPITA,                                                                                    -- Importe de capital Asegurado
                            0 VMTPRMBR,                                                                                   -- Importe de Prima Bruta
                            0 VMTCOMR,                                                                                    -- Importe de Prima Comercial  
                            '' VMTPREMC,                                                                                  -- Comision de la prima
                            '' VMTBOMAT,                                                                                  -- Monto de bonificacion
                            '' VMTBOCOM,                                                                                  -- Monto de bonificacion comercial
                            '' KACINDRE,                                                                                  -- Indicador de Reaseguro
                            'PVG' KGIORIGM
                            from usvtimg01."ADDRESS" ad
                            join usvtimg01."POLICY" p on P."SCERTYPE"  = ad."SCERTYPE"
                                         and p."NBRANCH"  = ad."NBRANCH" 
                                         and P."NPRODUCT" = AD."NPRODUCT"
                                         and p."NPOLICY"  = ad."NPOLICY"
                            join usvtimg01."CERTIFICAT" c on c."SCERTYPE"  = ad."SCERTYPE"
                                         and c."NBRANCH"  = ad."NBRANCH" 
                                         and c."NPRODUCT" = AD."NPRODUCT"
                                         and c."NPOLICY"  = ad."NPOLICY"
                                         and c."NCERTIF"  = ad."NCERTIF"
                            where ad."NBRANCH"  in (select "BRANCHCOM" from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "SOURCESCHEMA" = 'usvtimg01' and "RISKTYPEN" = 2 )
                            and cast(ad."DCOMPDATE" as date)  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
                            union all
                            (
                            select 
                            'D' as INDDETREC,
                            'ABUNRIS' as TABLAIFRS17, 
                            '' PK,                                                                                        -- Clave compuesta
                            coalesce(cast(cast(aut."DEFFECDATE" as date) as varchar),'')  TIOCFRM,                        -- Fecha de inicio de validez de registro
                            aut."NBRANCH"  || '-' || aut."NPOLICY" ||  '-' || aut."NCERTIF" KABAPOL,                      -- Numero de Poliza
                            (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = aut."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01') KACTPRIS ,     -- Codigo del Tipo de riesgo 
                            coalesce(trim(aut."SREGIST"),'') || '-' || coalesce(trim(aut."SCHASSIS"),'')  DUNIRIS,        -- Codigo de Unidad de riesgo
                            coalesce(cast(cast(aut."DSTARTDATE"as date)as varchar),'')  TINCRIS,                          -- Fecha de inicio del riesgo            
                            coalesce(cast(aut."DEXPIRDAT" as varchar),'')  TVENCRI,                                       -- Fecha de vencimiento del riesgo        
                            '' TSITRIS,                                                                                   -- Fecha de estado de la unidad de riesgo
                            '' KACSITUR,                                                                                  -- Codigo del estado de la unidad de riesgo 
                            'LPG' DCOMPA,                                                                                 -- Empresa a la que pertenece la informacion 
                            '' KACTPOPS,                                                                                  -- Codigo del tipo de Objeto de persona asegurada 
                            '' TDRENOVA,                                                                                  -- Fecha de inicio del termino UR 
                            1 DQOBJSEG,                                                                                   -- Numero de objetos asegurados
                            coalesce(cast(aut."NCAPITAL" as NUMERIC(14,2)),0) VCAPITAL,                                   -- Importe Capital asegurado
                            coalesce(cast(aut."NPREMIUM" as NUMERIC(12,2)),0) VMTPRMBR,                                   -- Importe de Prima Bruta 
                            coalesce(cast(aut."NPREMIUM" as NUMERIC(12,2)),0) VMTCOMR,                                    -- Importe de Prima Comercial
                            '' VMTPREMC,                                                                                  -- Comision de la prima
                            '' VMTBOMAT,                                                                                  -- Monto de bonificacion
                            '' VMTBOCOM,                                                                                  -- Monto de bonificacion comercial
                            '' KACINDRE,                                                                                  -- Indicador de Reaseguro
                            'PVG' KGIORIGM
                            From usvtimg01."AUTO" aut
                            where cast(aut."DCOMPDATE" as date)  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            limit 100 
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_VTIME_G")
    L_DF_ABUNRIS_VTIME_G = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_VTIME_G).load()
    print("2-TERMINO TABLA ABUNRIS_VTIME_G")

    L_ABUNRIS_VTIME_V = f'''
                            (
                            (select 
                             'D' as INDDETREC,
                             'ABUNRIS' as TABLAIFRS17, 
                             '' PK,                                                                                        -- Clave compuesta 
                             coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'')  TIOCFRM,                        -- Fecha de inicio de validez de registro
                             rol."NBRANCH"  || '-' || rol."NPOLICY" ||  '-' || rol."NCERTIF" KABAPOL,                      -- Numero de Poliza
                             (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol."NBRANCH" and "SOURCESCHEMA" = 'usvtimv01' ) KACTPRIS ,     -- Codigo del Tipo de riesgo 
                             rol."SCLIENT"    DUNIRIS,                                                                     -- Codigo de unidad de riesgo 
                             coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'') TINCRIS,                         -- Fecha de inicio de riesgo
                             coalesce(cast(rol."DNULLDATE" as VARCHAR),'')  TVENCRI,                                        -- Fecha de vencimiento del riesgo 
                             '' TSITRIS,                                                                                   -- Fecha de estado de la unidad del riesgo
                             '' KACSITUR,                                                                                  -- Codigo de estad de la unidad del riesgo
                             'LPG' DCOMPA,                                                                                 -- Empresa a la que pertenece la informacion
                             '' KACTPOPS,                                                                                  -- Codigo del tipo de persona asegurada
                             '' TDRENOVA,                                                                                  -- Fecha de inicio de termino de la Unidad de riesgo
                             1 DQOBJSEG,                                                                                   -- Numero de objetos asegurados 
                             0 VCAPITA,                                                                                    -- Importe de capital
                             0 VMTPRMBR,                                                                                   -- Importe de Prima Bruta
                             0 VMTCOMR,                                                                                    -- Importe de Prima Comercial 
                             '' VMTPREMC,                                                                                  -- Comision de la prima
                             '' VMTBOMAT,                                                                                  -- Monto de bonificacion
                             '' VMTBOCOM,                                                                                  -- Monto de bonificacion comercial
                             '' KACINDRE,                                                                                  -- Indicador de Reaseguro
                             'PVV' KGIORIGM
                             from usvtimv01."ROLES" rol
                             where rol."SCERTYPE"  = '2'
                             and rol."NBRANCH"  in  (select "BRANCHCOM" from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "SOURCESCHEMA" = 'usvtimv01' and "RISKTYPEN" = 1 )
                             and rol."NROLE"  in (2,8) -- Asegurado , Asegurado adicional
                             and cast(rol."DCOMPDATE" as date)  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                             limit 100
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_VTIME_V")
    L_DF_ABUNRIS_VTIME_V = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_VTIME_V).load()
    print("2-TERMINO TABLA ABUNRIS_VTIME_V")


    #PERFORM THE UNION OPERATION
    L_DF_ABCOSSEG = L_DF_ABUNRIS_INSUNIX_G.union(L_DF_ABUNRIS_INSUNIX_V).union(L_DF_ABUNRIS_VTIME_G).union(L_DF_ABUNRIS_VTIME_V)
    
    print("AQUI SE MANDE EL CONTEO")
    print(L_DF_ABCOSSEG.count())

    return L_DF_ABCOSSEG