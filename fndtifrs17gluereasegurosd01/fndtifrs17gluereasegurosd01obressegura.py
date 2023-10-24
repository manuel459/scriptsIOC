
def getData(glueContext,connection,L_FECHA_INICIO,L_FECHA_FIN):

  L_OBRESSEGURA_INSUNIX = f'''
                            (
                            (select
                            'D' as INDDETREC,
                            'OBRESSEGURA' as TABLAIFRS17,
                            '' as PK,
                            '' as DTPREG,
                            '' as TIOCPROC,
                            cast(cc.effecdate as varchar)as TIOCFRM,
                            '' as TIOCTO,
                            'PIG' as KGIORIGM,
                            cc."number"  ||'-'|| cc.branch  ||'-'|| cc.currency ||'-'|| cc.year_contr  ||'-'|| cc."type"  as DCDINTTRA,
                            '' as KOCSBTRT,
                            '' as DDESCDSBTRT,
                            '' as KOCPOOL,
                            cc.companyc  as KOCCDRESS,
                            '' as DCDRESS,
                            cc.supervis  as DCDCORR,
                            (
                            select evi.scod_vt  from usinsug01.equi_vt_inx evi
                            left join  usinsug01.company c  
                            on c.client = evi.scod_inx 
                            where cc.companyc = c.code 
                            )  as KEBENTID_RSS,
                            cc."share"  as VTXPART,
                            'LPG' as DCOMPA,
                            '' as DMARCA
                            from usinsug01.contr_comp cc 
                            where cc.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
                            --1999-10-06 - 2023-07-31 

                            --union all 
                            
                            --LPV
                            --NO EXISTE REASEGUROS PARA ESTE SISTEMA
                            /*
                            select
                            'D' as INDDETREC,
                            'OBRESSEGURA' as TABLAIFRS17,
                            '' as PK,
                            '' as DTPREG,
                            '' as TIOCPROC,
                            cc.effecdate as TIOCFRM,
                            '' as TIOCTO,
                            'PIG' as KGIORIGM,
                            cc."number"  ||'-'|| cc.branch  ||'-'|| cc.currency ||'-'|| cc.year_contr  ||'-'|| cc."type"  as DCDINTTRA,
                            '' as KOCSBTRT,
                            '' as DDESCDSBTRT,
                            '' as KOCPOOL,
                            cc.companyc  as KOCCDRESS,
                            '' as DCDRESS,
                            cc.supervis  as DCDCORR,
                            '' as KEBENTID_RSS,
                            cc."share"  as VTXPART,
                            'LPG' as DCOMPA,
                            '' as DMARCA
                            from usinsuv01.contr_comp cc   
                            */
                            ) AS TMP
                                '''

  #EJECUTAR CONSULTA
  print("1-TERMINO TABLA OBRESSEGURA_INX")
  L_DF_OBRESSEGURA_INSUNIX = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_OBRESSEGURA_INSUNIX).load()
  print("2-TERMINO TABLA OBRESSEGURA_INX")

  L_OBSTRATADOS_VTIME = f'''
                           (
                            (select
                            'D' as INDDETREC,
                            'OBRESSEGURA' as TABLAIFRS17,
                            '' as PK,
                            '' as DTPREG,
                            '' as TIOCPROC,
                            cast(cast(pc."DEFFECDATE" as date) as varchar) as TIOCFRM,
                            '' as TIOCTO,
                            'PVG' as KGIORIGM,
                            pc."NNUMBER"  ||'-'|| pc."NBRANCH"  ||'-'|| pc."NTYPE"  as DCDINTTRA,
                            '' as KOCSBTRT,
                            '' as DDESCDSBTRT,
                            '' as KOCPOOL,
                            PC."NCOMPANY"  as KOCCDRESS,
                            '' as DCDRESS,
                            pc."NCORREDOR"  as DCDCORR,
                            (
                            select evi."SCLIENT_VT" from usvtimg01."EQUI_VT_INX" evi 
                            left join usvtimg01."COMPANY" c
                            on c."SCLIENT"  = evi."SCLIENT_VT" 
                            where pc."NCOMPANY" = c."NCOMPANY" 
                            )  as KEBENTID_RSS,
                            pc."NSHARE"  as VTXPART,
                            'LPG' as DCOMPA,
                            '' as DMARCA
                            from usvtimg01."PART_CONTR" pc
                            --2008-12-05 - 2023-08-31
                            where pc."DCOMPDATE" between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
                            
                            /*
                            --VISUALTIME OBRESSEGURA VIDA
                            --NO EXISTE REASEGUROS PARA ESTE SISTEMA
                            select 
                            'D' as INDDETREC,
                            'OBRESSEGURA' as TABLAIFRS17,
                            '' as DTPREG,
                            '' as TIOCPROC,
                            pc."DEFFECDATE"  as TIOCFRM,
                            '' as TIOCTO,
                            'PVV' as KGIORIGM,
                            '' as DCDINTTRA,
                            '' as KOCSBTRT,
                            '' as DDESCDSBTRT,
                            '' as DCDRESS,
                            '' as DCDCORR,
                            pc."NSHARE"  as VTXPART,
                            '' as DMARCA,
                            '' as KEBENTID_COR,
                            '' as VTXJUROS,
                            '' as VTXPARES,
                            '' as KBENTID_CED
                            from usvtimv01./*NO HAY*/
                            */
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
  print("1-TERMINO TABLA OBRESSEGURA_VT")
  L_DF_OBRESSEGURA_VTIME = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_OBSTRATADOS_VTIME).load()
  print("2-TERMINO TABLA OBRESSEGURA_VT")

  #PERFORM THE UNION OPERATION
  L_DF_OBRESSEGURA = L_DF_OBRESSEGURA_INSUNIX.union(L_DF_OBRESSEGURA_VTIME)

  return L_DF_OBRESSEGURA
