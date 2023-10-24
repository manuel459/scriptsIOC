
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
                                   c.client as DCODIGO,
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
                                   c.client as DCODIGO,
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
                                   c."SCLIENT"  as DCODIGO,
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
                                   c."SCLIENT"  as DCODIGO,
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
    
    #PERFORM THE UNION OPERATION
    L_DF_OCCDRESS = L_DF_OCCDRESS_INSUNIX.union(L_DF_OCCDRESS_VTIME)

    return L_DF_OCCDRESS

