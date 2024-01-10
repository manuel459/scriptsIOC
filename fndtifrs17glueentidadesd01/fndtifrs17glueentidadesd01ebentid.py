def get_data(glue_context,connection,p_fecha_inicio, p_fecha_fin):
    L_EBENTID_INSIS = f'''
                      ( 
                        SELECT 
                        'D' AS INDDETREC,
                        'EBENTID' AS TABLAIFRS17, 
                        COALESCE((SELECT ILPI."LEGACY_ID" FROM
                        USINSIV01."INTRF_LPV_PEOPLE_IDS" ILPI
                        WHERE ILPI."MAN_ID" = PP."MAN_ID"), '') AS PK,
                        '' AS DTPREG,
                        '' AS TIOCPROC,
                        COALESCE(CAST(PX."VALID_FROM" AS DATE), CAST(PP."REGISTRATION_DATE" AS DATE)) AS TIOCFRM,
                        '' AS TIOCTO,
                        'PNV' AS KGIORIGM,
                        COALESCE((SELECT ILPI."LEGACY_ID" FROM
                        USINSIV01."INTRF_LPV_PEOPLE_IDS" ILPI
                        WHERE ILPI."MAN_ID" = PP."MAN_ID"), '') AS DCODIGO,
                        CASE
                        WHEN (PP."COMP_TYPE" NOT IN ('IN', 'IS') AND PP."NATIONALITY" <> 'PT') THEN PP."NAME"
                        ELSE ''
                        END DNOME,
                        COALESCE(CAST(CAST(PP."BIRTH_DATE" AS DATE) AS VARCHAR), '')  AS TNASCIAC,
                        '' AS DBI,
                        '' AS DNIF,
                        '' AS KECTITLO,
                        '' AS KECNAC,
                        COALESCE(PP."COMP_TYPE", '') AS KECTPENT,
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
                        LEFT JOIN                        
                        (
                        	(SELECT  --CLIENTES CON REGISTROS VALIDOS
                        	 DISTINCT 
                        	 PPC."MAN_ID",
                        	 CAST(PPC."VALID_FROM" AS DATE) "VALID_FROM"                       
                             FROM USINSIV01."P_PEOPLE_CHANGES" PPC  
                             WHERE PPC."VALID_TO" IS null)
                           
                           UNION
                                                                                 
                           (SELECT --CLIENTES CON REGISTROS NO VALIDOS
                            PPC."MAN_ID",
                            CAST(PPC."VALID_FROM" AS DATE) "VALID_FROM"    
                            FROM USINSIV01."P_PEOPLE_CHANGES" PPC 
                            WHERE NOT EXISTS (SELECT 1 FROM USINSIV01."P_PEOPLE_CHANGES" PPC2  
					                         WHERE PPC2."VALID_TO" IS null and PPC2."MAN_ID" = PPC."MAN_ID" ))
                        ) PX
                        ON PP."MAN_ID" = PX."MAN_ID"                                                
                        WHERE PP."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                      ) AS TMP
                      '''
    L_DF_EBENTID_INSIS = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_EBENTID_INSIS).load()
    return L_DF_EBENTID_INSIS