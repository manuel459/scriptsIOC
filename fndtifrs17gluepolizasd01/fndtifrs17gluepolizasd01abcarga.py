from pyspark.sql.types import *
from pyspark.sql.functions import col

def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):         

   #Declara consulta VTIME
   L_POLIZAS_VTIME_GENERAL = f'''
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
                                  '' KACTPCBB --valor vacio
                              FROM USVTIMG01."POLICY" P
                              left join USVTIMG01."CERTIFICAT" CERT 
                                on CERT."SCERTYPE" = P."SCERTYPE"
                                  and CERT."NBRANCH" = P."NBRANCH"
                                  and CERT."NPRODUCT" = P."NPRODUCT"
                                  and CERT."NPOLICY" = P."NPOLICY"
                                join USVTIMG01."DISC_XPREM" dx
                                on dx."SCERTYPE" = P."SCERTYPE"
                                  and dx."NBRANCH" = P."NBRANCH"
                                  and dx."NPRODUCT" = P."NPRODUCT"
                                  and dx."NPOLICY" = p."NPOLICY"
                                  and dx."NCERTIF" = cert."NCERTIF"
                                  and dx."DEFFECDATE" <= p."DSTARTDATE"
                                  and (dx."DNULLDATE" is null or dx."DNULLDATE" > P."DSTARTDATE")
                                left join usvtimg01."DISCO_EXPR" de
                                on dx."NBRANCH" = de."NBRANCH"
                                  and dx."NPRODUCT" = de."NPRODUCT"
                                  and dx."NDISC_CODE" = de."NDISEXPRC"
                                where
                                P."SCERTYPE" = '2' 
                                and p."SSTATUS_POL" not in ('2', '3')
                                and ( (p."SPOLITYPE" = '1'
                                -- INDIVIDUAL 
                                  and P."DEXPIRDAT" >= '2021-12-31'
                                  and (p."DNULLDATE" is null
                                    or p."DNULLDATE" > '2021-12-31') )
                                or 
                                        (p."SPOLITYPE" <> '1'
                                -- COLECTIVAS
                                  and CERT."DEXPIRDAT" >= '2021-12-31'
                                  and (CERT."DNULLDATE" is null
                                    or CERT."DNULLDATE" > '2021-12-31')) )
                                AND dx."DCOMPDATE" between '{P_FECHA_INICIO}' and '{P_FECHA_FIN}'                         
                              ) as tmp
                              '''
  #Ejecutar consulta
   L_DF_POLIZAS_VTIME_GENERAL = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_POLIZAS_VTIME_GENERAL).load() 

   print('USVTIMG01 exitoso')

   L_POLIZAS_VTIME_VIDA = f'''
                          (SELECT 
                           'D' INDDETREC, 
                           'ABCARGA' TABLAIFRS17, 
                           '' PK, --PENDIENTE
                           '' DTPREG, --NO
                           '' TIOCPROC, --NO
                           COALESCE(CAST(CAST(DX."DEFFECDATE" AS DATE) AS VARCHAR), '') TIOCFRM, --PENDIENTE
                           '' TIOCTO, --NO
                           'PVV' KGIORIGM, 
                           DX."NBRANCH" || '-' || DX."NPRODUCT" || '-' || DX."NPOLICY" || '-' || DX."NCERTIF" KABAPOL, --FK
                           '' KABUNRIS, --VALOR VACIO  
                           /*
                           DE ACUERDO A JAOS EL DATO SE PUDIERA OBTENER EN BASE A LA PRIMERA COVERTURA PERO LOS SISTEMAS 
                           NO TIENEN ESA INFORMACION DE MANERA DIRECTA EN LA TABLA DE RECARGOS Y DESCUENTOS
                           (SELECT C."NCOVER" FROM USVTIMV01."COVER" C 
                             WHERE C."SCERTYPE" = '2' 
                             AND   C."NBRANCH" = DX."NBRANCH"  
                             AND   C."NPOLICY" = DX."NPOLICY"
                             AND   C."NCERTIF" = DX."NCERTIF" 
                             AND   C."NCOVER"  = 1
                             AND   C."DEFFECDATE" <= DX."DEFFECDATE" 
                             AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > DX."DEFFECDATE"))*/
                             '' KGCTPCBT, 
                             '' KACCDFDO, --VALOR VACIO
                             COALESCE(DE."SDISEXPRI" , '') KACTPCAG,
                             DX."NDISC_CODE" KACCDCAG, 
                             DX."NAMOUNT" VMTCARGA, 
                             COALESCE(CAST(CAST(DX."DEFFECDATE" AS DATE) AS VARCHAR), '') TULTMALT, 
                             '' DUSRUPD, --NO
                             'LPV' DCOMPA, 
                             '' DMARCA, --NO
                             '' DINCPRM, --VALOR VACIO
                             CASE WHEN (DX."NAMOUNT" != 0 AND DX."NAMOUNT" IS NOT NULL) 
                                  AND  (CAST(DX."NPERCENT" AS INTEGER) = 0 AND DX."NPERCENT" IS NULL) THEN 'IMPORTE' 
                             ELSE 'PORCENTAJE' 
                             END KACTPVCG, 
                             '' DDURACAO, --VALOR VACIO
                             '' KACTPCBB --VALOR VACIO
                             FROM USVTIMV01."POLICY" P
                             LEFT JOIN USVTIMV01."CERTIFICAT" CERT ON CERT."SCERTYPE" = P."SCERTYPE" AND CERT."NBRANCH" = P."NBRANCH" AND CERT."NPRODUCT" = P."NPRODUCT" AND CERT."NPOLICY" = P."NPOLICY"
                             JOIN USVTIMV01."DISC_XPREM" DX ON DX."SCERTYPE" = P."SCERTYPE" AND DX."NBRANCH" = P."NBRANCH" AND DX."NPRODUCT" = P."NPRODUCT" AND DX."NPOLICY" = P."NPOLICY" AND DX."NCERTIF" = CERT."NCERTIF" AND DX."DEFFECDATE" <= P."DSTARTDATE" AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                             LEFT JOIN USVTIMV01."DISCO_EXPR" DE ON DX."NBRANCH" = DE."NBRANCH" AND DX."NPRODUCT" = DE."NPRODUCT" AND DX."NDISC_CODE" = DE."NDISEXPRC"
                             WHERE
                             P."SCERTYPE" = '2' 
                             AND P."SSTATUS_POL" NOT IN ('2', '3')
                             AND ((P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                  AND P."DEXPIRDAT" >= '2021-12-31'
                                  AND (P."DNULLDATE" IS NULL
                                  OR P."DNULLDATE" > '2021-12-31') )
                             OR 
                             (P."SPOLITYPE" <> '1' -- COLECTIVAS
                              AND CERT."DEXPIRDAT" >= '2021-12-31'
                              AND (CERT."DNULLDATE" IS NULL
                              OR CERT."DNULLDATE" > '2021-12-31')))
                             AND DX."DCOMPDATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' LIMIT 100) AS TMP
                      '''
   
   L_DF_POLIZAS_VTIME_VIDA = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_POLIZAS_VTIME_VIDA).load() 
   
   print('USVTIMV01 exitoso')
   #------------------------------------------------------------------------------------------------------------------#

   #DECLARAR CONSULTA INSUNIX
   L_POLIZAS_INSUNIX_GENERAL = f'''
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
                                  p.branch || '-' || p.product || '-' || PSP.sub_product || '-' || p.policy || '-' || cert.certif KABAPOL,--FK pendiente
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
                                  '' KACTPCBB--valor vacio
                                  from USINSUG01.POLICY P 
                                  LEFT JOIN USINSUG01.CERTIFICAT CERT 
                                    ON  CERT.USERCOMP = P.USERCOMP  
                                    AND CERT.COMPANY = P.COMPANY
                                    AND CERT.CERTYPE = P.CERTYPE
                                    AND CERT.BRANCH  = P.BRANCH
                                    AND CERT.POLICY  = P.POLICY 
                                    AND CERT.PRODUCT = P.PRODUCT
                                  JOIN USINSUG01.POL_SUBPRODUCT PSP
                             	      ON  PSP.USERCOMP = P.USERCOMP
                             	      AND PSP.COMPANY  = P.COMPANY
                             	      AND PSP.CERTYPE  = P.CERTYPE
                             	      AND PSP.BRANCH   = P.BRANCH		   
                             	      AND PSP.PRODUCT  = P.PRODUCT
                             	      AND PSP.POLICY   = P.POLICY
                                  left join USINSUG01.DISC_XPREM DX
                                    on dx.usercomp = p.usercomp
                                    and dx.company = p.company
                                    and dx.branch = p.branch
                                    and dx.policy = p.policy
                                    and dx.certif = cert.certif
                                    and dx.effecdate <= p.effecdate
                                    and (dx.nulldate is null or dx.nulldate > p.effecdate)
                                  WHERE P.CERTYPE  = '2'
                                  AND P.STATUS_POL NOT IN ('2','3') 
                                  AND ((P.POLITYPE = '1' -- INDIVIDUAL 
                                        AND P.EXPIRDAT >= '2021-12-31' 
                                        AND (P.NULLDATE IS NULL OR P.NULLDATE > '2021-12-31'))
                                        OR 
                                      (P.POLITYPE <> '1' -- COLECTIVAS 
                                        AND CERT.EXPIRDAT >= '2021-12-31' 
                                  AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2021-12-31')))
                                  AND DX.compdate between '{P_FECHA_INICIO}' and '{P_FECHA_FIN}' limit 100
                                ) AS TMP
                                '''
   #Ejecutar consulta
   L_DF_POLIZAS_INSUNIX_GENERAL = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_POLIZAS_INSUNIX_GENERAL).load()

   print('USINSUG01 exitoso')
   
   L_POLIZAS_INSUNIX_VIDA = f'''
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
                                    p.branch || '-' || p.product || '-' || p."policy" || '-' || cert.certif KABAPOL, --FK
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
                                    '' KACTPCBB --valor vacio
                                  FROM 
                                  from USINSUV01.POLICY P 
                                  LEFT JOIN USINSUV01.CERTIFICAT CERT 
                                    ON  CERT.USERCOMP = P.USERCOMP  
                                    AND CERT.COMPANY = P.COMPANY
                                    AND CERT.CERTYPE = P.CERTYPE
                                    AND CERT.BRANCH  = P.BRANCH
                                    AND CERT.POLICY  = P.POLICY 
                                    AND CERT.PRODUCT = P.PRODUCT
                                  left join USINSUV01.DISC_XPREM DX
                                    on dx.usercomp = p.usercomp
                                    and dx.company = p.company
                                    and dx.branch = p.branch
                                    and dx.policy = p.policy
                                    and dx.certif = cert.certif
                                    and dx.effecdate <= p.effecdate
                                    and (dx.nulldate is null or dx.nulldate > p.effecdate)
                                  WHERE P.CERTYPE  = '2'
                                  AND P.STATUS_POL NOT IN ('2','3') 
                                  AND ((P.POLITYPE = '1' -- INDIVIDUAL 
                                        AND P.EXPIRDAT >= '2021-12-31' 
                                        AND (P.NULLDATE IS NULL OR P.NULLDATE > '2021-12-31'))
                                        OR 
                                      (P.POLITYPE <> '1' -- COLECTIVAS 
                                        AND CERT.EXPIRDAT >= '2021-12-31' 
                                  AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2021-12-31'))
                                  AND DX.compdate between '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' limit 100
                              ) as tmp
                             '''
   #Ejecutar consulta
   L_DF_POLIZAS_INSUNIX_VIDA = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable", L_POLIZAS_INSUNIX_VIDA).load()
   
   print('USINSUV01 exitoso')

    #------------------------------------------------------------------------------------------------------------------#

   #Declara consulta INSIS
   L_POLIZAS_INSIS = f'''
                      (SELECT
                      'D' INDDETREC,
                      'ABCARGA' TABLAIFRS17,
                      '' PK,
                      '' DTPREG,
                      '' TIOCPROC,
                      COALESCE(CAST(CAST(GRD."EFFECTIVE_FROM" AS DATE) AS VARCHAR),'') TIOCFRM,
                      '' TIOCTO,
                      'PNV' KGIORIGM,
                      CASE
                      COALESCE(PP."ENG_POL_TYPE",'')
                      WHEN 'DEPENDENT' THEN P."ATTR1" || '-' || P."ATTR2" || '-' || P."POLICY_NO" || '-' || PP."MASTER_POLICY_ID"
                      ELSE ''
                      END KABAPOL,
                      COALESCE(CAST(TRUNC(GRD."INSURED_OBJ_ID") AS VARCHAR),'') AS KABUNRIS,
                      '' KGCTPCBT,--EN BLANCO
                      '' KACCDFDO,--EN BLANCO
                      COALESCE(GRD."DISCOUNT_TYPE", '') AS KACTPCAG,
                      TRUNC(GRD."DISCOUNT_ID", 0) AS KACCDCAG,
                      GRD."DISCOUNT_VALUE" AS VMTCARGA,
                      '' TULTMALT,--EN BLANCO
                      '' DUSRUPD,
                      'LPG' DCOMPA,
                      '' DMARCA,
                      '' DINCPRM,--EN BLANCO
                      '' KACTPVCG,--EN BLANCO 
                      '' DDURACAO,--EN BLANCO
                      COALESCE(GRD."COVER_TYPE",'') AS KACTPCBB
                      FROM
                      USINSIV01."POLICY" P
                      LEFT JOIN USINSIV01."POLICY_ENG_POLICIES" PP ON P."POLICY_ID" = PP."POLICY_ID"
                      LEFT JOIN USINSIV01."GEN_RISK_DISCOUNT" GRD ON GRD."POLICY_ID" = P."POLICY_ID"
                      WHERE P."REGISTRATION_DATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}' LIMIT 100) AS TMP
                      '''

   #Ejecutar consulta
   L_DF_POLIZAS_INSIS = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_POLIZAS_INSIS).load()

   print('EXITOSO USINSIV01')

   #Perform the union operation
   L_DF_POLIZAS = L_DF_POLIZAS_VTIME_GENERAL.union(L_DF_POLIZAS_VTIME_VIDA).union(L_DF_POLIZAS_INSIS).union(L_DF_POLIZAS_INSUNIX_GENERAL).union(L_DF_POLIZAS_INSUNIX_VIDA)

   L_DF_POLIZAS = L_DF_POLIZAS.withColumn("VMTCARGA", col("VMTCARGA").cast(DecimalType(15, 5)))

   return L_DF_POLIZAS