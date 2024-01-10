def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):
    
    L_OBSTRATADOS_INSUNIX = f'''
                             (
                             (select
                             'D' as INDDETREC,
                             'OBTRATADOS' as TABLAIFRS17,
                             '' as PK,
                             '' as DTPREG,
                             '' as TIOCPROC,
                             coalesce(cast(C.effecdate as VARCHAR),'') as TIOCFRM,
                             '' as TIOCTO,
                             'PIG' as KGIORIGM,
                             cast(c."number" as varchar) ||'-'|| c.branch  as DCDINTTRA,
                             c.currency ||'-'|| c."type" as DCDTRAT_SO,
                             '' as DDESCDTRA,
                             '' as DDESABRTRA,
                             coalesce(cast(c.startdat as varchar),'') as TINICIO,
                             coalesce(cast(c.expirdat as varchar),'') as TTERMO,
                             coalesce(cast(c.year_contr as varchar),'') as DANOTRAT,
                             coalesce(cast(c."type" as varchar),'') as KOCTPRESS,
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
                             from usinsug01.contrproc c --1995-08-01 - 2023-07-31
                             where c.compdate between '{p_fecha_inicio}' and '{p_fecha_fin}'
                             )
                             union all

                             (select
                             'D' as INDDETREC,
                             'OBTRATADOS' as TABLAIFRS17,
                             '' as PK,
                             '' as DTPREG,
                             '' as TIOCPROC,
                             coalesce(cast(C.effecdate as VARCHAR),'') as TIOCFRM,
                             '' as TIOCTO,
                             'PIV' as KGIORIGM,
                             cast(c."number" as varchar) ||'-'|| c.branch  as DCDINTTRA,
                             '' as DCDTRAT_SO,
                             '' as DDESCDTRA,
                             '' as DDESABRTRA,
                             coalesce(cast(c.startdat as varchar),'') as TINICIO,
                             coalesce(cast(c.expirdat as varchar),'') as TTERMO,
                             coalesce(cast(c.year_contr as varchar),'') as DANOTRAT,
                             coalesce(cast(c."type" as varchar),'') as KOCTPRESS,
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
                             where c.compdate between '{p_fecha_inicio}' and '{p_fecha_fin}'
                             )
                             --1995-08-01 - 2022-12-30
                             ) AS TMP
                             '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA OBSTRATADOS_INX")
    L_DF_OBSTRATADOS_INSUNIX = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_OBSTRATADOS_INSUNIX).load()
    print("2-TERMINO TABLA OBSTRATADOS_INX")
    
    L_OBSTRATADOS_VTIME = f'''
                            (
                            (select 
                            'D' as INDDETREC,
                            'OBTRATADOS' as TABLAIFRS17,
                            '' as PK,
                            '' as DTPREG,
                            '' as TIOCPROC,
                            cast(C."DEFFECDATE" as DATE)  as TIOCFRM,
                            '' as TIOCTO,
                            'PVV' as KGIORIGM,
                            cast(c."NNUMBER" as varchar) ||'-'|| c."NBRANCH"  ||'-'|| c."NTYPE"  as DCDINTTRA,
                            '' as DCDTRAT_SO,
                            '' as DDESCDTRA,
                            '' as DDESABRTRA,
                            cast(cast(c."DEFFECDATE" as date) as varchar) as TINICIO,
                            coalesce(cast( cast(c."DNULLDATE" as date) as varchar),'') as TTERMO,
                            coalesce(cast(c."NYEAR_BEGIN" as varchar),'')  as DANOTRAT,
                            cast(c."NTYPE" as varchar)  as KOCTPRESS,
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
                            from usvtimv01."CONTRPROC" c --2006-06-02 - 2017-08-14 
                            where cast(c."DCOMPDATE" as date) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            )
                            union all
                            
                            (select
                            'D' as INDDETREC,
                            'OBTRATADOS' as TABLAIFRS17,
                            '' as PK,
                            '' as DTPREG,
                            '' as TIOCPROC,
                            cast(C."DEFFECDATE" as DATE)  as TIOCFRM,
                            '' as TIOCTO,
                            'PVG' as KGIORIGM,
                            cast(c."NNUMBER" as varchar) ||'-'|| c."NBRANCH"  ||'-'|| c."NTYPE"  as DCDINTTRA,
                            '' as DCDTRAT_SO,
                            '' as DDESCDTRA,
                            '' as DDESABRTRA,
                            cast( cast(c."DEFFECDATE" as date) as varchar) as TINICIO,
                            coalesce(cast( cast(c."DNULLDATE" as date) as varchar),'') as TTERMO,
                            coalesce(cast(c."NYEAR_BEGIN" as varchar),'') as DANOTRAT,
                            cast(c."NTYPE" as varchar)  as KOCTPRESS,
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
                            from usvtimg01."CONTRPROC" c --2009-01-17 - 2023-03-30
                            where cast(c."DCOMPDATE" as date) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA OBSTRATADOS_VT")
    L_DF_OBTRATADOS_VTIME = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_OBSTRATADOS_VTIME).load()
    print("2-TERMINO TABLA OBSTRATADOS_VT")
    
    L_OBSTRATADOS_INSIS = f'''
                            (
                            ( select 
                            'D' as INDDETREC,
                            'OBTRATADOS' as TABLAIFRS17,
                            '' as PK,
                            '' as DTPREG,
                            '' as TIOCPROC,
                            cast(RT."START_DATE" as DATE) as TIOCFRM,
                            '' as TIOCTO,
                            'PNV' as KGIORIGM,
                            RT."TREATY_ID"  as DCDINTTRA,
                            '' as DCDTRAT_SO,
                            '' as DDESCDTRA,
                            '' as DDESABRTRA,
                            cast(RT."START_DATE" as DATE) as TINICIO,
                            cast(RT."END_DATE"   as DATE) as TTERMO,
                            '' as DANOTRAT,
                            RT."TREATY_SUBTYPE" as KOCTPRESS,
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
                            from usinsiv01."RI_TREATY" rt --1995-01-01 - 2021-01-01
                            where cast(RT."START_DATE" as DATE) BETWEEN '{p_fecha_inicio}' and '{p_fecha_fin}'
                            )
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA OBSTRATADOS_INS")
    L_DF_OBTRATADOS_INSIS = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_OBSTRATADOS_INSIS).load()
    print("2-TERMINO TABLA OBSTRATADOS_INS")
    #PERFORM THE UNION OPERATION 
    L_DF_OBSTRATADOS = L_DF_OBSTRATADOS_INSUNIX.union(L_DF_OBTRATADOS_VTIME).union(L_DF_OBTRATADOS_INSIS)

    return L_DF_OBSTRATADOS