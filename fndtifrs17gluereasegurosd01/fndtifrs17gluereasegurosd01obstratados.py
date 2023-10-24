
def getData(glueContext,connection,L_FECHA_INICIO,L_FECHA_FIN):

    L_OBSTRATADOS_INSUNIX = f'''
                             (
                             (select
                             'D' as INDDETREC,
                             'OBTRATADOS' as TABLAIFRS17,
                             '' as PK,
                             '' as DTPREG,
                             '' as TIOCPROC,
                             '' as TIOCFRM,
                             '' as TIOCTO,
                             'PIG' as KGIORIGM,
                             cast(c."number" as varchar) ||'-'|| c.branch ||'-'|| c.currency ||'-'|| c."type" as DCDINTTRA,
                             '' as DCDTRAT_SO,
                             '' as DDESCDTRA,
                             '' as DDESABRTRA,
                             cast(c.startdat as varchar) as TINICIO,
                             cast(c.expirdat as varchar) as TTERMO,
                             c.year_contr  as DANOTRAT,
                             c."type" as KOCTPRESS,
                             '' as KOCTPFRC,
                             '' as KOCVLDFRC,
                             '' as KOCTPTRT,
                             '' as KOCTPDUR,
                             '' as KOCTPOBJ,
                             '' as KOCSIT,
                             '' as DCDTRAT,
                             'LPG' as DCOMPA,
                             '' as DMARCA,
                             '' as KOCSCOPE,
                             '' as KOCIDFAC,
                             '' as KOCTPRNP,
                             '' as KACSEGM,
                             '' as KOCMOEDA,
                             '' as DINDPMAX,
                             '' as VMTMAXTR,
                             '' as VMTPLENO,
                             '' as VMTPDEDT,
                             '' as KOCGRCBT,
                             '' as VMTPRUN,
                             '' as KGCRAMO_SAP
                             from usinsug01.contrproc c
                             --1995-08-01 - 2023-07-31
                             where c.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                             )
                             union all

                             (select
                             'D' as INDDETREC,
                             'OBTRATADOS' as TABLAIFRS17,
                             '' as PK,
                             '' as DTPREG,
                             '' as TIOCPROC,
                             '' as TIOCFRM,
                             '' as TIOCTO,
                             'PIV' as KGIORIGM,
                             cast(c."number" as varchar) ||'-'|| c.branch ||'-'|| c.currency ||'-'|| c."type" as DCDINTTRA,
                             '' as DCDTRAT_SO,
                             '' as DDESCDTRA,
                             '' as DDESABRTRA,
                             cast(c.startdat as varchar) as TINICIO,
                             cast(c.expirdat as varchar) as TTERMO,
                             c.year_contr  as DANOTRAT,
                             c."type" as KOCTPRESS,
                             '' as KOCTPFRC,
                             '' as KOCVLDFRC,
                             '' as KOCTPTRT,
                             '' as KOCTPDUR,
                             '' as KOCTPOBJ,
                             '' as KOCSIT,
                             '' as DCDTRAT,
                             'LPV' as DCOMPA,
                             '' as DMARCA,
                             '' as KOCSCOPE,
                             '' as KOCIDFAC,
                             '' as KOCTPRNP,
                             '' as KACSEGM,
                             '' as KOCMOEDA,
                             '' as DINDPMAX,
                             '' as VMTMAXTR,
                             '' as VMTPLENO,
                             '' as VMTPDEDT,
                             '' as KOCGRCBT,
                             '' as VMTPRUN,
                             '' as KGCRAMO_SAP
                             from usinsuv01.contrproc c 
                             where c.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                             )
                             --1995-08-01 - 2022-12-30
                             ) AS TMP
                             '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA OBSTRATADOS_IX")
    L_DF_OBSTRATADOS_INSUNIX = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_OBSTRATADOS_INSUNIX).load()
    print("2-TERMINO TABLA OBSTRATADOS_IX")
    
    L_OBSTRATADOS_VTIME = f'''
                            (
                            (select 
                            'D' as INDDETREC,
                            'OBTRATADOS' as TABLAIFRS17,
                            '' as PK,
                            '' as DTPREG,
                            '' as TIOCPROC,
                            '' as TIOCFRM,
                            '' as TIOCTO,
                            'PVV' as KGIORIGM,
                            cast(c."NNUMBER" as varchar) ||'-'|| c."NBRANCH"  ||'-'|| c."NTYPE"  as DCDINTTRA,
                            '' as DCDTRAT_SO,
                            '' as DDESCDTRA,
                            '' as DDESABRTRA,
                            cast( cast(c."DEFFECDATE" as date) as varchar) as TINICIO,
                            cast( cast(c."DNULLDATE" as date) as varchar) as TTERMO,
                            c."NYEAR_BEGIN"  as DANOTRAT,
                            c."NTYPE"  as KOCTPRESS,
                            '' as KOCTPFRC,
                            '' as KOCVLDFRC,
                            '' as KOCTPTRT,
                            '' as KOCTPDUR,
                            '' as KOCTPOBJ,
                            '' as KOCSIT,
                            '' as DCDTRAT,
                            'LPV' as DCOMPA,
                            '' as DMARCA,
                            '' as KOCSCOPE,
                            '' as KOCIDFAC,
                            '' as KOCTPRNP,
                            '' as KACSEGM,
                            '' as KOCMOEDA,
                            '' as DINDPMAX,
                            '' as VMTMAXTR,
                            '' as VMTPLENO,
                            '' as VMTPDEDT,
                            '' as KOCGRCBT,
                            '' as VMTPRUN,
                            '' as KGCRAMO_SAP
                            from usvtimv01."CONTRPROC" c 
                            --2006-06-02 - 2017-08-14 
                            where c."DCOMPDATE" between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
                            union all
                            
                            (select
                            'D' as INDDETREC,
                            'OBTRATADOS' as TABLAIFRS17,
                            '' as PK,
                            '' as DTPREG,
                            '' as TIOCPROC,
                            '' as TIOCFRM,
                            '' as TIOCTO,
                            'PVG' as KGIORIGM,
                            cast(c."NNUMBER" as varchar) ||'-'|| c."NBRANCH"  ||'-'|| c."NTYPE"  as DCDINTTRA,
                            '' as DCDTRAT_SO,
                            '' as DDESCDTRA,
                            '' as DDESABRTRA,
                            cast( cast(c."DEFFECDATE" as date) as varchar) as TINICIO,
                            cast( cast(c."DNULLDATE" as date) as varchar) as TTERMO,
                            c."NYEAR_BEGIN"  as DANOTRAT,
                            c."NTYPE"  as KOCTPRESS,
                            '' as KOCTPFRC,
                            '' as KOCVLDFRC,
                            '' as KOCTPTRT,
                            '' as KOCTPDUR,
                            '' as KOCTPOBJ,
                            '' as KOCSIT,
                            '' as DCDTRAT,
                            'LPG' as DCOMPA,
                            '' as DMARCA,
                            '' as KOCSCOPE,
                            '' as KOCIDFAC,
                            '' as KOCTPRNP,
                            '' as KACSEGM,
                            '' as KOCMOEDA,
                            '' as DINDPMAX,
                            '' as VMTMAXTR,
                            '' as VMTPLENO,
                            '' as VMTPDEDT,
                            '' as KOCGRCBT,
                            '' as VMTPRUN,
                            '' as KGCRAMO_SAP
                            from usvtimg01."CONTRPROC" c 
                            --2009-01-17 - 2023-03-30
                            where c."DCOMPDATE" between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA OBSTRATADOS_VT")
    L_DF_OBTRATADOS_VTIME = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_OBSTRATADOS_VTIME).load()
    print("2-TERMINO TABLA OBSTRATADOS_VT")
    
    #PERFORM THE UNION OPERATION 
    L_DF_OBSTRATADOS = L_DF_OBSTRATADOS_INSUNIX.union(L_DF_OBTRATADOS_VTIME)

    return L_DF_OBSTRATADOS