#CODE-ENTIDADES01-EBENTID

def getData(glueContext,connection,P_FECHA_INICIO, P_FECHA_FIN):

    L_EBENTID_INSIS = f'''
                          -------------------------
                          --        INSIS        --
                          -------------------------
                          (
                            SELECT 
                            'D' AS INDDETREC,
                            'EBENTID' AS TABLAIFRS17, 
                            COALESCE((SELECT ILPI."LEGACY_ID" FROM
                            USINSIV01."INTRF_LPV_PEOPLE_IDS" ILPI
                            WHERE ILPI."MAN_ID" = PP."MAN_ID"), '') AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            '' AS TIOCFRM,
                            '' AS TIOCTO,
                            'PNV' AS KGIORIGM,
                            COALESCE((SELECT ILPI."LEGACY_ID" FROM
                            USINSIV01."INTRF_LPV_PEOPLE_IDS" ILPI
                            WHERE ILPI."MAN_ID" = PP."MAN_ID"), '') AS DCODIGO,
                            '' AS  DNOME,
                            COALESCE(CAST(CAST(PP."BIRTH_DATE" AS DATE) AS VARCHAR), '')  AS TNASCIAC,
                            '' AS DBI,
                            '' AS DNIF,
                            '' AS KECTITLO,
                            '' AS KECNAC,
                            '' AS KECTPENT,
                            '' AS KECPROF,
                            '' AS KECESTCV,
                            COALESCE(CAST(PP."SEX" AS VARCHAR), '')  AS KECSEXO,
                            '' AS KECCAE,
                            'LPV' AS DCOMPA,
                            '' AS DLEICODE,
                            '' AS DRSRCODE,
                            '' AS VMTMAXCAP,
                            '' AS KECMOEDA
                            FROM USINSIV01."P_PEOPLE" PP
                            WHERE PP."REGISTRATION_DATE" BETWEEN '{P_FECHA_INICIO}' AND '{P_FECHA_FIN}'
                          ) as tmp
                       '''
    L_DF_EBENTID_INSIS = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_EBENTID_INSIS).load()

    return L_DF_EBENTID_INSIS
