def getData(glueContext,connection,L_FECHA_INICIO,L_FECHA_FIN):
  L_OBRESSEGURA_INSUNIX = f'''
                            (
                            (select 
                             'D' as INDDETREC,
                             'OBRESSEGURA' as TABLAIFRS17,
                             '' as PK,
                             '' as DTPREG,
                             '' as TIOCPROC,
                             coalesce(cast(cc.effecdate as varchar) , '')as TIOCFRM,
                             '' as TIOCTO,
                             'PIG' as KGIORIGM,
                             cc."number"  ||'-'|| cc.branch   as DCDINTTRA,
                             cc.currency ||'-'|| cc.year_contr  ||'-'|| cc."type" as KOCSBTRT,
                             '' as DDESCDSBTRT,
                             '' as KOCPOOL,
                             coalesce(cc.companyc,0) as KOCCDRESS,
                             coalesce(cc.companyc,0) as DCDRESS,
                             coalesce(cast(cc.supervis as char(4)),'') as DCDCORR,
                             coalesce((
                             select evi.scod_vt  from usinsug01.equi_vt_inx evi
                             left join  usinsug01.company c  
                             on c.client = evi.scod_inx 
                             where cc.companyc = c.code 
                             ),'')  as KEBENTID_RSS,
                             coalesce(cc.share,0)  as VTXPART,
                             'LPG' as DCOMPA,
                             '' as DMARCA
                             from usinsug01.contr_comp cc 
                             where cc.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
                            union all 
                            
                            --LPV
                            (select
                             'D' as INDDETREC,
                             'OBRESSEGURA' as TABLAIFRS17,
                             '' as PK,
                             '' as DTPREG,
                             '' as TIOCPROC,
                             cast(cc.effecdate as varchar)as TIOCFRM,
                             '' as TIOCTO,
                             'PIV' as KGIORIGM,
                             cc."number"  ||'-'|| cc.branch   as DCDINTTRA,
                             cc.currency ||'-'|| cc.year_contr  ||'-'|| cc."type" as KOCSBTRT,
                             '' as DDESCDSBTRT,
                             '' as KOCPOOL,
                             coalesce(cc.companyc,0) as KOCCDRESS,
                             coalesce(cc.companyc,0) as DCDRESS,
                             coalesce(cast(cc.supervis as char(4)),'') as DCDCORR,
                             coalesce((
                             select evi.scod_vt  from usinsug01.equi_vt_inx evi
                             left join  usinsug01.company c  
                             on c.client = evi.scod_inx 
                             where cc.companyc = c.code 
                             ),'')  as KEBENTID_RSS,
                             coalesce(cc.share,0)  as VTXPART,
                             'LPV' as DCOMPA,
                             '' as DMARCA
                             from usinsuv01.contr_comp cc --'1999-10-06' '2017-02-22'
                            where cc.compdate between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
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
                             coalesce(cast(cast(pc."DEFFECDATE" as date) as varchar),'') as TIOCFRM,
                             '' as TIOCTO,
                             'PVG' as KGIORIGM,
                             pc."NNUMBER"  ||'-'|| pc."NBRANCH"  ||'-'|| pc."NTYPE"  as DCDINTTRA,
                             '' as KOCSBTRT,
                             '' as DDESCDSBTRT,
                             '' as KOCPOOL,
                             PC."NCOMPANY"   as KOCCDRESS,
                             PC."NCOMPANY"  as DCDRESS,
                             coalesce(cast(pc."NCORREDOR" as char(4)),'')  as DCDCORR,
                             coalesce((
                             select c."SCLIENT" from usvtimg01."COMPANY" c
                             where pc."NCOMPANY" = c."NCOMPANY" 
                             ),'')  as KEBENTID_RSS,
                             coalesce(pc."NSHARE",0)  as VTXPART,
                             'LPG' as DCOMPA,
                             '' as DMARCA
                             from usvtimg01."PART_CONTR" pc --2008-12-05 - 2023-08-31
                            where pc."DCOMPDATE" between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}'
                            )
                            
                            union all
                            --LPV
                            (select
                            'D' as INDDETREC,
                            'OBRESSEGURA' as TABLAIFRS17,
                            '' as PK,
                            '' as DTPREG,
                            '' as TIOCPROC,
                            cast(cast(pc."DEFFECDATE" as date) as varchar) as TIOCFRM,
                            '' as TIOCTO,
                            'PVV' as KGIORIGM,
                            pc."NNUMBER"  ||'-'|| pc."NBRANCH"  ||'-'|| pc."NTYPE"  as DCDINTTRA,
                            '' as KOCSBTRT,
                            '' as DDESCDSBTRT,
                            '' as KOCPOOL,
                            PC."NCOMPANY"  as KOCCDRESS,
                            PC."NCOMPANY" as DCDRESS,
                            coalesce(cast(pc."NCORREDOR" as char(4)),'')  as DCDCORR,
                            coalesce((
                            select c."SCLIENT" from usvtimg01."COMPANY" c
                            where pc."NCOMPANY" = c."NCOMPANY" 
                            ),'')  as KEBENTID_RSS,
                            coalesce(pc."NSHARE",0)  as VTXPART,
                            'LPV' as DCOMPA,
                            '' as DMARCA
                            from usvtimv01."PART_CONTR" pc --'2007-11-20'	'2016-04-15' 
                            where pc."DCOMPDATE" between '{L_FECHA_INICIO}' and '{L_FECHA_FIN}')
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
  print("1-TERMINO TABLA OBRESSEGURA_VT")
  L_DF_OBRESSEGURA_VTIME = glueContext.read.format('jdbc').options(**connection).option("dbtable",L_OBSTRATADOS_VTIME).load()
  print("2-TERMINO TABLA OBRESSEGURA_VT")
  #PERFORM THE UNION OPERATION
  L_DF_OBRESSEGURA = L_DF_OBRESSEGURA_INSUNIX.union(L_DF_OBRESSEGURA_VTIME)#.union(L_DF_OBRESSEGURA_INSIS)
  return L_DF_OBRESSEGURA