def getData(glueContext,connection,L_FECHA_INICIO,L_FECHA_FIN):
    L_OCCDRESS_INSUNIX = f'''
                                   (
                                   (select
                                   'D' as INDDETREC,
                                   'OCCDRESS' as TABLAIFRS17,
                                   '' as PK,
                                   '' as DTPREG,
                                   '' as TIOCPROC,
                                   '' as TIOCFRM,
                                   '' as TIOCTO,
                                   'PIG' as KGIORIGM,
                                   coalesce((
                                             select evi.scod_vt  from usinsug01.equi_vt_inx evi
                                             where c.client = evi.scod_inx 
                                   ),'') as DCODIGO,
                                   '' as DDESC,
                                   '' as KOICDRESS
                                   from usinsug01.company c 
                                   where c.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                                   )
                                   union all
                                   --1994-02-16 - 2023-08-04
                                   
                                   (select
                                   'D' as INDDETREC,
                                   'OCCDRESS' as TABLAIFRS17,
                                   '' as PK,
                                   '' as DTPREG,
                                   '' as TIOCPROC,
                                   '' as TIOCFRM,
                                   '' as TIOCTO,
                                   'PIV' as KGIORIGM,
                                   coalesce((
                                             select evi.scod_vt  from usinsug01.equi_vt_inx evi
                                             where c.client = evi.scod_inx 
                                   ),'') as DCODIGO,
                                   '' as DDESC,
                                   '' as KOICDRESS
                                   from usinsug01.company c 
                                   where c.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                                   )
                                   --1994-02-16 - 2023-08-04
                                   ) AS TMP         
                          '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA OCCDRESS_IN")
    L_DF_OCCDRESS_INSUNIX = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_OCCDRESS_INSUNIX).load()
    print("2-TERMINO TABLA OCCDRESS_IN")
    
    L_OCCDRESS_VTIME = f'''
                                   (
                                   (
                                   ---------------------
                                   --     VTIME       --
                                   ---------------------
                                   select
                                   'D' as INDDETREC,
                                   'OCCDRESS' as TABLAIFRS17,
                                   '' as PK,
                                   '' as DTPREG,
                                   '' as TIOCPROC,
                                   '' as TIOCFRM,
                                   '' as TIOCTO,
                                   'PVG' as KGIORIGM,
                                   coalesce(c."SCLIENT",'')  as DCODIGO,
                                   '' as DDESC,
                                   '' as KOICDRESS
                                   from usvtimg01."COMPANY" c
                                   where c."DCOMPDATE"  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                                   )
                                   union all 
                                   --2007-12-03 - 2023-08-04 
                                   
                                   -- LPV
                                   (select
                                   'D' as INDDETREC,
                                   'OCCDRESS' as TABLAIFRS17,
                                   '' as PK,
                                   '' as DTPREG,
                                   '' as TIOCPROC,
                                   '' as TIOCFRM,
                                   '' as TIOCTO,
                                   'PVV' as KGIORIGM,
                                   coalesce(c."SCLIENT",'')  as DCODIGO,
                                   '' as DDESC,
                                   '' as KOICDRESS
                                   from usvtimg01."COMPANY" c
                                   where c."DCOMPDATE"  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                                   )
                                   --2007-12-03 - 2023-08-04 
                                   ) AS TMP
                        '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA OCCDRESS_VT")
    L_DF_OCCDRESS_VTIME = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_OCCDRESS_VTIME).load()
    print("2-TERMINO TABLA OCCDRESS_VT")

    L_OCCDRESS_INSIS = f'''
                                   (
                                   (select
                                   'D' as INDDETREC,
                                   'OCCDRESS' as TABLAIFRS17,
                                   '' as PK,
                                   '' as DTPREG,
                                   '' as TIOCPROC,
                                   '' as TIOCFRM,
                                   '' as TIOCTO,
                                   'PNV' as KGIORIGM,
                                   pin."MAN_ID" as DCODIGO,
                                   '' as DDESC,
                                   '' as KOICDRESS
                                   from usinsiv01."P_INSURERS" pin --2023-11-06 - 2023-11-06 
                                   where cast(pin.fecha_replicacion_positiva  as date)  between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}' --LA TABLA ORIGINAL NO TIENE FECHAS 
                                   )
                                   ) AS TMP
                        '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA OCCDRESS_INS")
    L_DF_OCCDRESS_INSIS = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_OCCDRESS_INSIS).load()
    print("2-TERMINO TABLA OCCDRESS_INS")
    
    #PERFORM THE UNION OPERATION
    L_DF_OCCDRESS = L_DF_OCCDRESS_INSUNIX.union(L_DF_OCCDRESS_VTIME).union(L_DF_OCCDRESS_INSIS)

    return L_DF_OCCDRESS


    
    #PERFORM THE UNION OPERATION
    L_DF_OCCDRESS = L_DF_OCCDRESS_INSUNIX.union(L_DF_OCCDRESS_VTIME)
    return L_DF_OCCDRESS