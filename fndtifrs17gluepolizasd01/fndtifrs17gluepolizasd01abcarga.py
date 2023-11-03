def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):         

   #Declara consulta VTIME
   L_POLIZAS_VTIME = f'''
                       (
                        (
                          select 
                          'D' INDDETREC, 
                          'ABCARGA' TABLAIFRS17, 
                          '' PK, --PENDIENTE
                          '' DTPREG, --NO
                          '' TIOCPROC, --NO
                          coalesce(cast(cast(DX."DEFFECDATE" as date) as varchar), '') TIOCFRM, --PENDIENTE
                          '' TIOCTO, --NO
                          'PVG' KGIORIGM, 
                          dx."NBRANCH" || '-' || DX."NPRODUCT" || '-' || DX."NPOLICY" || '-' || DX."NCERTIF" KABAPOL, --FK
                          '' KABUNRIS, --FK  
                          /*
                           *  DE ACUERDO A JAOS el dato se pudiera obtener en base a la primera covertura pero los sistemas 
                          no tienen esa informacion de manera directa en la tabla de recargos y descuentos
                          (
                            SELECT C."NCOVER" FROM USVTIMG01."COVER" C 
                            where C."SCERTYPE" = '2' 
                            and   C."NBRANCH" = DX."NBRANCH"  
                            AND   C."NPOLICY" = DX."NPOLICY"
                            AND   C."NCERTIF" = DX."NCERTIF" 
                            AND   C."NCOVER"  = 1
                            and   C."DEFFECDATE" <= DX."DEFFECDATE" 
                            AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > DX."DEFFECDATE") 
                          )*/
                          '' KGCTPCBT, 
                          '' KACCDFDO, --VALOR VACIO
                          coalesce(de."SDISEXPRI" , '') KACTPCAG,
                          DX."NDISC_CODE" KACCDCAG, 
                          DX."NAMOUNT" VMTCARGA, 
                          coalesce(cast(cast(DX."DEFFECDATE" as date) as varchar), '') TULTMALT, 
                          '' DUSRUPD, --NO
                          'LPG' DCOMPA, 
                          '' DMARCA, --NO
                          '' DINCPRM, --VALOR VACIO
                          CASE WHEN (
                            DX."NAMOUNT" != 0 
                            AND DX."NAMOUNT" IS NOT NULL
                          ) 
                          AND (
                            CAST(DX."NPERCENT" AS INTEGER)= 0 
                            AND DX."NPERCENT" IS NULL
                          ) THEN 'IMPORTE' ELSE 'PORCENTAJE' END KACTPVCG, 
                          '' DDURACAO, 
                          '' KACTPCBB, --valor vacio
                          'PVG' KGIORIGM
                          FROM 
                            USVTIMG01."DISC_XPREM" dx
                          left join usvtimg01."DISCO_EXPR" de
                          on dx."NBRANCH" = de."NBRANCH"
                          and dx."NPRODUCT" = de."NPRODUCT"
                          and dx."NDISC_CODE" = de."NDISEXPRC"
                          where dx."DCOMPDATE" between '{P_FECHA_INICIO}' and '{P_FECHA_FIN}'
                        )
                        /*
                        union all
                        (
                          SELECT 
                          'D' INDDETREC, 
                          'ABCARGA' TABLAIFRS17, 
                          '' PK, --PENDIENTE
                          '' DTPREG, --NO
                          '' TIOCPROC, --NO
                          coalesce(cast(CAST(DX."DEFFECDATE" AS DATE) as varchar), '') TIOCFRM, --PENDIENTE
                          '' TIOCTO, --NO
                          'PVV' KGIORIGM, 
                          dx."NBRANCH" || '-' || DX."NPRODUCT" || '-' || DX."NPOLICY" || '-' || DX."NCERTIF" KABAPOL, --FK
                          '' KABUNRIS, --VALOR VACIO  
                          /*
                          (
                            SELECT C."NCOVER" FROM USVTIMV01."COVER" C 
                            where C."SCERTYPE" = '2' 
                            and   C."NBRANCH" = DX."NBRANCH"  
                            AND   C."NPOLICY" = DX."NPOLICY"
                            AND   C."NCERTIF" = DX."NCERTIF" 
                            AND   C."NCOVER"  = 1
                            and   C."DEFFECDATE" <= DX."DEFFECDATE" 
                            AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > DX."DEFFECDATE") 
                          )*/
                          '' KGCTPCBT, 
                          '' KACCDFDO, --VALOR VACIO
                          coalesce(de."SDISEXPRI" , '') KACTPCAG,
                          DX."NDISC_CODE" KACCDCAG, 
                          DX."NAMOUNT" VMTCARGA, 
                          coalesce(CAST(CAST(DX."DEFFECDATE" AS DATE) as VARCHAR), '') TULTMALT, 
                          '' DUSRUPD, --NO
                          'LPV' DCOMPA, 
                          '' DMARCA, --NO
                          '' DINCPRM, --VALOR VACIO
                          CASE WHEN (
                            DX."NAMOUNT" != 0 
                            AND DX."NAMOUNT" IS NOT NULL
                          ) 
                          AND (
                            CAST(DX."NPERCENT" AS INTEGER)= 0 
                            AND DX."NPERCENT" IS NULL
                          ) THEN 'IMPORTE' ELSE 'PORCENTAJE' END KACTPVCG, 
                          '' DDURACAO, --VALOR VACIO
                          '' KACTPCBB, --valor vacio
                          'PVV' KGIORIGM
                         FROM 
                          USVTIMV01."DISC_XPREM" dx
                          left join usvtimv01."DISCO_EXPR" de
                          on dx."NBRANCH" = de."NBRANCH"
                          and dx."NPRODUCT" = de."NPRODUCT"
                          and dx."NDISC_CODE" = de."NDISEXPRC"
                         WHERE dx."DCOMPDATE" BETWEEN '{P_FECHA_INICIO}' and '{P_FECHA_FIN}'
                        )*/
                       ) as tmp
                      '''

   #Ejecutar consulta
   L_DF_POLIZAS_VTIME = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_POLIZAS_VTIME).load()  
   print('USVTIMV01 exitoso')
   #------------------------------------------------------------------------------------------------------------------#

   #DECLARAR CONSULTA INSUNIX
   L_POLIZAS_INSUNIX = f'''
                        (  
                          (
                            select
                            'D' INDDETREC,
                            'ABCARGA' TABLAIFRS17,
                            '' PK,--PENDIENTE
                            '' DTPREG,--NO
                            '' TIOCPROC,--NO
                            coalesce(cast(cast(dx.effecdate as date) as varchar),'') TIOCFRM,--PENDIENTE
                            '' TIOCTO,--NO
                            'PIG' KGIORIGM,
                            dx.branch || '-' || dx."policy" || '-' || dx.code KABAPOL,--FK pendiente
                            '' KABUNRIS,--valor vacio
                            '' KGCTPCBT,--valor vacio
                            '' KACCDFDO,-- valor vacio
                            coalesce(DX.type,
                            '') KACTPCAG,
                            coalesce(DX.CODE,
                            0) KACCDCAG,
                            coalesce(DX.AMOUNT,
                            0) VMTCARGA,
                            coalesce(cast(cast(DX.EFFECDATE as DATE) as varchar),'') TULTMALT,
                            '' DUSRUPD,--NO
                            'LPG' DCOMPA,
                            '' DMARCA,--NO
                            '' DINCPRM,-- valor vacio
                            case
                            	when (
                              DX.AMOUNT != 0
                            	and DX.AMOUNT is not null
                            )
                            	and (
                              cast(DX.PERCENT as INTEGER)= 0
                            	and DX.PERCENT is null
                            ) then 'IMPORTE'
                            	else 'PORCENTAJE'
                            end KACTPVCG,
                            '' DDURACAO,--valor vacio
                            '' KACTPCBB,--valor vacio
                            'PIG' KGIORIGM
                            from
                            	USINSUG01.DISC_XPREM DX
                            WHERE DX.compdate between '{P_FECHA_INICIO}' and '{P_FECHA_FIN}'
                          )
                          UNION ALL
                          (
                            select 
                            'D' INDDETREC, 
                            'ABCARGA' TABLAIFRS17, 
                            '' PK, --PENDIENTE
                            '' DTPREG, --NO
                            '' TIOCPROC, --NO
                            coalesce(cast(CAST(DX.EFFECDATE AS DATE) as varchar), '') TIOCFRM, --PENDIENTE
                            '' TIOCTO, --NO
                            'PIV' KGIORIGM, 
                            dx.branch || '-' || dx."policy" || '-' || dx.code KABAPOL, --FK
                            '' KABUNRIS, --valor vacio
                            /*
                            DE ACUERDO A JAOS el dato se pudiera obtener en base a la primera covertura pero los sistemas 
                            no tienen esa informacion de manera directa en la tabla de recargos y descuentos
                            (
                              SELECT C.COVER FROM USINSUV01.COVER C 
                              where C.CERTYPE = '2' 
                              and C.BRANCH = DX.BRANCH 
                              AND C.POLICY = DX.POLICY
                              AND C.CERTIF = DX.CERTIF 
                              AND C.COVER = 1
                              and C.EFFECDATE <= DX.EFFECDATE 
                              AND (C.NULLDATE IS NULL OR C.NULLDATE > DX.EFFECDATE) 
                            ),*/
                            '' KGCTPCBT, 
                            '' KACCDFDO, --valor vacio
                            coalesce(DX.type, '') KACTPCAG, 
                            coalesce (DX.CODE, 0) KACCDCAG, 
                            coalesce (DX.AMOUNT, 0) VMTCARGA, 
                            coalesce (cast(CAST(DX.EFFECDATE AS DATE) as varchar), '') TULTMALT, 
                            '' DUSRUPD, --NO
                            'LPV' DCOMPA, 
                            '' DMARCA, --NO
                            '' DINCPRM, 
                            CASE WHEN (
                              DX.AMOUNT != 0 
                              AND DX.AMOUNT IS NOT NULL
                            ) 
                            AND (
                              CAST(DX.PERCENT AS INTEGER)= 0 
                              AND DX.PERCENT IS NULL
                            ) THEN 'IMPORTE' ELSE 'PORCENTAJE' END KACTPVCG, 
                            '' DDURACAO, 
                            '' KACTPCBB, --valor vacio
                            'PIV' KGIORIGM
                           FROM 
                           USINSUV01.DISC_XPREM DX
                           WHERE DX.compdate between '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'
                          ) 
                        ) AS TMP
                        '''
   #Ejecutar consulta
   L_DF_POLIZAS_INSUNIX = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_POLIZAS_INSUNIX).load()
   print('USINSUV01 exitoso')

    #------------------------------------------------------------------------------------------------------------------#

   #Declara consulta INSIS
   L_POLIZAS_INSIS = f'''
                      (
                        select
                            'D' INDDETREC,
                            'ABCARGA' TABLAIFRS17,
                            '' PK,
                            '' DTPREG,
                            '' TIOCPROC,
                            GRD."EFFECTIVE_FROM" TIOCFRM,
                            '' TIOCTO,
                            case
                                coalesce(PP."ENG_POL_TYPE",'')
                                when 'DEPENDENT' then P."ATTR1" || '-' || P."ATTR2" || '-' || P."POLICY_NO" || '-' || PP."MASTER_POLICY_ID"
                                else ''
                            end KABAPOL,
                            GRD."INSURED_OBJ_ID" as KABUNRIS,
                            '' KGCTPCBT,--EN BLANCO
                            '' KACCDFDO,--EN BLANCO
                            GRD."DISCOUNT_TYPE" as KACTPCAG,
                            GRD."DISCOUNT_ID" as KACCDCAG,
                            GRD."DISCOUNT_VALUE" as VMTCARGA,
                            '' TULTMALT,--EN BLANCO
                            '' DUSRUPD,
                            'LPG' DCOMPA,
                            '' DMARCA,
                            '' DINCPRM,--EN BLANCO
                            '' KACTPVCG,--EN BLANCO
                            '' DDURACAO,--EN BLANCO
                            GRD."COVER_TYPE" as KACTPCBB,
                            'PNV' KGIORIGM
                        from
                            USINSIV01."POLICY" P
                        left join USINSIV01."POLICY_ENG_POLICIES" PP 
                        on P."POLICY_ID" = PP."POLICY_ID"
                        left join usinsiv01."GEN_RISK_DISCOUNT" grd
                        on grd."POLICY_ID" = p."POLICY_ID"
                        WHERE P."REGISTRATION_DATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'
                      )
                      '''

   #Ejecutar consulta
   L_DF_POLIZAS_INSIS = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_POLIZAS_INSIS).load()

   print('EXITOSO USINSIV01')

   #Perform the union operation
   L_DF_POLIZAS = L_DF_POLIZAS_VTIME.union(L_DF_POLIZAS_INSIS).union(L_DF_POLIZAS_INSUNIX)

   return L_DF_POLIZAS