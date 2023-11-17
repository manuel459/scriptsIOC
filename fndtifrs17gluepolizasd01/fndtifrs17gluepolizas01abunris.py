
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
                              'PIG' KGIORIGM                                                  -- Indicador
                              coalesce(cast(rol.branch as varchar),'') || '-' || coalesce(cast(rol.policy as varchar),'') ||  '-' || coalesce(cast(rol.certif as varchar),'') KABAPOL,  -- Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol.branch  and "SOURCESCHEMA" = 'usinsug01') KACTPRIS ,           -- Codigo del Tipo de riesgo
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
                              where rol.usercomp = 1
                              and rol.company = 1
                              and rol.certype  = '2'
                              and rol.branch in  (select "BRANCHCOM" from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "SOURCESCHEMA" = 'usinsug01' and  "RISKTYPEN" = 1 )
                              and rol.role in (2,8) -- Asegurado , Asegurado adicional
                              and rol.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
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
                              coalesce(cast(p.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                              '' as TIOCTO,
                              'PIG' KGIORIGM                                                  -- Indicador
                              coalesce(cast(ad.branch as varchar),'') || '-' || coalesce(cast(ad.policy as varchar),'') ||  '-' || coalesce(cast(ad.certif as varchar),'') KABAPOL,  -- Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = ad.branch  and "SOURCESCHEMA" = 'usinsug01') KACTPRIS ,           -- Codigo del Tipo de riesgo
                              coalesce(cast(ad.branch as varchar),'') || '-' || coalesce(cast(ad.policy as varchar),'') ||  '-' || coalesce(cast(ad.certif as varchar),'')  DUNIRIS,                                                          -- Codigo de unidad de riesgo 
                              coalesce(cast(p.EFFECDATE as varchar),'')TINCRIS,             -- Fecha de inicio de riesgo
                              coalesce(cast(p.NULLDATE  as varchar),'') TVENCRI,            -- Fecha de fin de riesgo
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
                              join usinsug01.policy p 
                                           on p.usercomp = ad.usercomp 
                                           and p.company = ad.company 
                                           and p.certype = ad.certype
                                           and p.branch = ad.branch
                                           and p.policy = ad.policy
                              join usinsug01.certificat c
                                           on p.usercomp = ad.usercomp 
                                           and c.company = ad.company 
                                           and c.certype = ad.certype
                                           and c.branch = ad.branch 
                                           and c.policy = ad.policy
                                           and c.certif = ad.certif
                              where ad.branch in (select "BRANCHCOM" from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "SOURCESCHEMA" = 'usinsug01' and "RISKTYPEN" = 2)
                              and ad.compdate between  '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
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
                              coalesce(cast(tnb.branch as varchar),'') || '-' || coalesce(cast(tnb.policy as varchar),'') ||  '-' || coalesce(cast(tnb.certif as varchar),'') KABAPOL,  --Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = tnb.branch  and "SOURCESCHEMA" = 'usinsug01') KACTPRIS ,     -- Codigo del Tipo de riesgo 
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
                              where tnb.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
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
                              coalesce(cast(rol.branch as varchar),'') || '-' || coalesce(cast(rol.policy as varchar),'') ||  '-' || coalesce(cast(rol.certif as varchar),'') KABAPOL,  --Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol.branch  and "SOURCESCHEMA" = 'usinsug01') KACTPRIS ,           -- Codigo del Tipo de riesgo 
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
                              rol."NBRANCH" || '-' || rol."NPOLICY"  ||  '-' || rol."NCERTIF"  KABAPOL,                      -- Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01'  ) KACTPRIS ,     -- Codigo del Tipo de riesgo 
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
                             where rol."SCERTYPE" = '2'
                             and rol."NBRANCH"  in  (select "BRANCHCOM" from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "SOURCESCHEMA" = 'usvtimg01' and  "RISKTYPEN" = 1 )
                             and rol."NROLE" in (2,8) -- Asegurado , Asegurado adicional
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
                              rol."NBRANCH" || '-' || rol."NPOLICY"  ||  '-' || rol."NCERTIF"  KABAPOL,                      -- Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = ad."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01' ) KACTPRIS ,     -- Codigo del Tipo de riesgo 
                              ad."SKEYADDRESS" DUNIRIS,                                                                      -- Codigo de unidad de riesgo  
                              coalesce(cast(cast(ad."DEFFECDATE" as date) as varchar),'')  TINCRIS,                          -- Fecha de Inicio del riesgo
                              coalesce(cast(ad."DNULLDATE" as VARCHAR),'')  TVENCRI,                                        -- Fecha de vencimiento del riesgo 
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
                              aut."NBRANCH"  || '-' || aut."NPOLICY" ||  '-' || aut."NCERTIF" KABAPOL,                      -- Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = aut."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01') KACTPRIS ,     -- Codigo del Tipo de riesgo
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
                              rol."NBRANCH"  || '-' || rol."NPOLICY" ||  '-' || rol."NCERTIF" KABAPOL,                      -- Numero de Poliza
                              (select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol."NBRANCH" and "SOURCESCHEMA" = 'usvtimv01' ) KACTPRIS ,     -- Codigo del Tipo de riesgo  
                              rol."SCLIENT"    DUNIRIS,                                                                     -- Codigo de unidad de riesgo 
                              coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'') TINCRIS,                         -- Fecha de inicio de riesgo
                              coalesce(cast(rol."DNULLDATE" as VARCHAR),'')  TVENCRI,                                       -- Fecha de vencimiento del riesgo
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
                             from usvtimv01."ROLES" rol
                             where rol."SCERTYPE"  = '2'
                             and rol."NBRANCH"  in  (select "BRANCHCOM" from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "SOURCESCHEMA" = 'usvtimv01' and "RISKTYPEN" = 1 )
                             and rol."NROLE"  in (2,8) -- Asegurado , Asegurado adicional
                             and cast(rol."DCOMPDATE" as date)  between  '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
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
                              'PVV' KGIORIGM,                                                 
                              '' KABAPOL,                      
                              '' KACTPRIS ,     
                              '' DUNIRIS,                                                                     
                              cast(cast(io."INSR_BEGIN" as date)as varchar) TINCRIS,                         
                              cast(cast(io."INSR_END" as date)as varchar) TVENCRI,      
                              '' as TSITRIS,                                                  
                              io."INSURED_OBJ_ID"  as KACSITUR,                                                 
                              '' as KACESQM,
                              'LPG' as DCOMPA,                                                
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
                             and cast(io."REGISTRATION_DATE" as date)  between  '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                             limit 100
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABUNRIS_VTIME_V_PES")
    L_DF_ABUNRIS_INSIS_V = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSIS_V).load()
    print("2-TERMINO TABLA ABUNRIS_VTIME_V_PES")


    #PERFORM THE UNION OPERATION
    L_DF_ABUNRIS = L_DF_ABUNRIS_INX_G.union(L_DF_ABUNRIS_INSUNIX_V_PES).union(L_DF_ABUNRIS_VTIME_G).union(L_DF_ABUNRIS_VTIME_V_PES).union(L_DF_ABUNRIS_INSIS_V)
    
    print("AQUI SE MANDE EL CONTEO")
    print(L_DF_ABUNRIS.count())

    return L_DF_ABUNRIS