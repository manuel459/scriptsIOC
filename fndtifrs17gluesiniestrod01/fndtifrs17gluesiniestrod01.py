
def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):

  L_SBCOSSEG_INSUNIX = f'''
                            (
                                ---------------------------LPG BUSSITYP 1----------------------------------
                                (select	
                                        'D' as "INDDETREC",
                                        'SBCOSSEG' as "TABLAIFRS17",
                                        '' AS "PK",
                                        '' AS "DTPREG"	,
                                        '' AS "TIOCPROC",
                                        '' AS "TIOCFRM" ,
                                        '' as "TIOCTO",
                                        'PIG' as "KGIORIGM",
                                        'LPV' AS "DCOMPA",
                                        '' as "DMARCA",
                                        '' as  "KSBSIN",
                                        coalesce ( cast ( coi.companyc as varchar ), '0' ) as "DCODCSG",
                                        '' as "DNUMSEQ",
                                        '' as "TDPLANO",
                                        cast (cla.capital as varchar )  as "VMTCAPIT", --agregar c�lculo
                                        coalesce ( cast (coi.share as varchar ), '0' ) as "VTXQUOTA",
                                        '' as "VMTINDEM", --NOAPP
                                        cast(cla.policy as varchar) as "DNUMAPO_CSG",
                                        cast (cla.moneda_cod  as varchar ) as "KSCMOEDA" --agregar c�lculo
                                from	(	select 	cla.claim,
                                                    cla.branch,
                                                    cla.policy,
                                                    cla.occurdat,
                                                    cla.staclaim,
                                                    case	when	exists
                                                                    (	select  1
                                                                        from    usinsug01.cover cov
                                                                        join 	usinsug01.gen_cover gco
                                                                        on      cov.usercomp = cla.usercomp
                                                                        and     cov.company = cla.company
                                                                        and     cov.certype = '2'
                                                                        and     cov.branch = cla.branch
                                                                        and     cov.policy = cla.policy
                                                                        and     cov.certif = cla.certif
                                                                        and     cov.effecdate <= cla.occurdat
                                                                        and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                        and     gco.ctid =
                                                                                coalesce((	select  max(ctid)
                                                                                            from    usinsug01.gen_cover gco
                                                                                            where   gco.usercomp  =  cov.usercomp
                                                                                            and     gco.company   =  cov.company
                                                                                            and     gco.branch    =  cov.branch
                                                                                            and     gco.product  =  pol.product
                                                                                            and     gco.currency = cov.currency
                                                                                            and     gco.modulec = cov.modulec
                                                                                            and     gco.cover = cov.cover
                                                                                            and     gco.effecdate <= cla.occurdat
                                                                                            and     (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                            and     coalesce(gco.addsuini,'1') = '3'
                                                                                            and     gco.statregt <> '4'),
                                                                                        (   select  max(ctid)
                                                                                            from    usinsug01.gen_cover gco
                                                                                            where   gco.usercomp  =  cov.usercomp
                                                                                            and     gco.company   =  cov.company
                                                                                            and     gco.branch    =  cov.branch
                                                                                            and     gco.product  =  pol.product
                                                                                            and     gco.currency = cov.currency
                                                                                            and     gco.cover = cov.cover
                                                                                            and     gco.effecdate <= cla.occurdat
                                                                                            and     (gco.nulldate is null or nulldate > cla.occurdat)
                                                                                            and     coalesce(gco.addsuini,'1') = '3'
                                                                                            and     gco.statregt <> '4')))
                                                            then	(	select  max(cov.capital)
                                                                        from    usinsug01.cover cov
                                                                        where   cov.usercomp = cla.usercomp
                                                                        and     cov.company = cla.company
                                                                        and     cov.certype = '2'
                                                                        and     cov.branch = cla.branch
                                                                        and     cov.policy = cla.policy
                                                                        and     cov.certif = cla.certif
                                                                        and     cov.effecdate <= cla.occurdat
                                                                        and     (cov.nulldate is null or cov.nulldate > cla.occurdat))
                                                            else	(	select  sum(cov.capital)
                                                                        from    usinsug01.cover cov,
                                                                                usinsug01.gen_cover gco
                                                                        where   cov.usercomp = cla.usercomp
                                                                        and     cov.company = cla.company
                                                                        and     cov.certype = '2'
                                                                        and     cov.branch = cla.branch
                                                                        and     cov.policy = cla.policy
                                                                        and     cov.certif = cla.certif
                                                                        and     cov.effecdate <= cla.occurdat
                                                                        and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                        and     gco.ctid =
                                                                                coalesce((	select  max(ctid)
                                                                                            from    usinsug01.gen_cover gco
                                                                                            where   gco.usercomp  =  cov.usercomp
                                                                                            and     gco.company   =  cov.company
                                                                                            and     gco.branch    =  cov.branch
                                                                                            and     gco.product  =  pol.product
                                                                                            and     gco.currency = cov.currency
                                                                                            and     gco.modulec = cov.modulec
                                                                                            and     gco.cover = cov.cover
                                                                                            and     gco.effecdate <= cla.occurdat
                                                                                            and     (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                            and     (gco.addsuini is null or gco.addsuini ='1')
                                                                                            and     gco.statregt <> '4'),
                                                                                        (   select  max(ctid)
                                                                                            from    usinsug01.gen_cover gco
                                                                                            where   gco.usercomp  =  cov.usercomp
                                                                                            and     gco.company   =  cov.company
                                                                                            and     gco.branch    =  cov.branch
                                                                                            and     gco.product  =  pol.product
                                                                                            and     gco.currency = cov.currency
                                                                                            and     gco.cover = cov.cover
                                                                                            and     gco.effecdate <= cla.occurdat
                                                                                            and     (gco.nulldate is null or nulldate > cla.occurdat)
                                                                                            and     (gco.addsuini is null or gco.addsuini ='1')
                                                                                            and     gco.statregt <> '4'))) 
                                                            end capital,
                                                    coalesce(	coalesce((	select	max(cpl.currency)
                                                                            from	usinsug01.curren_pol cpl
                                                                            where 	cpl.usercomp = cla.usercomp
                                                                            and 	cpl.company = cla.company
                                                                            and		cpl.certype = '2'
                                                                            and		cpl.branch = cla.branch
                                                                            and 	cpl.policy = cla.policy
                                                                            and		cpl.certif = cla.certif),
                                                                        (	select	max(cpl.currency)
                                                                            from	usinsug01.curren_pol cpl
                                                                            where 	cpl.usercomp = cla.usercomp
                                                                            and 	cpl.company = cla.company
                                                                            and		cpl.certype = '2'
                                                                            and		cpl.branch = cla.branch
                                                                            and 	cpl.policy = cla.policy)),0) moneda_cod
                                            from	usinsug01.claim cla
                                            join    usinsug01.policy pol
                                            on 	((cla.branch = 23 and cla.staclaim = '6') or (cla.branch <> 23 and cla.staclaim <> '6'))
                                            and		pol.usercomp = cla.usercomp 
                                            and 	pol.company = cla.company
                                            and 	pol.branch = cla.branch 
                                            and 	pol.policy = cla.policy
                                            and		pol.bussityp = '1') cla
                                join 	usinsug01.coinsuran coi
                                on	coi.usercomp = 1
                                and     coi.company = 1
                                and     coi.certype = '2'
                                and     coi.branch = cla.branch
                                and     coi.policy = cla.policy
                                and     coi.effecdate <= cla.occurdat
                                and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                )
                                union all 
                                ---------------------------LPG BUSSITYP 2-------------------------------------------
                                (select	
                                        'D' as "INDDETREC",
                                        'SBCOSSEG' as "TABLAIFRS17",
                                        '' AS "PK",
                                        '' AS "DTPREG"	,
                                        '' AS "TIOCPROC",
                                        '' AS "TIOCFRM" ,
                                        '' as "TIOCTO",
                                        'PIG' as "KGIORIGM",
                                        'LPG' AS "DCOMPA",
                                        '' as "DMARCA",
                                        '' as "KSBSIN",
                                        '1' "DCODCSG",
                                        '' as "DNUMSEQ",
                                        '' as "TDPLANO",
                                        case	when	exists
                                                        (	select  1
                                                            from   usinsug01.cover cov
                                                            join   usinsug01.gen_cover gco
                                                            on      cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = '2'
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                            and     gco.ctid =
                                                                    coalesce((	select   max(ctid)
                                                                                from    usinsug01.gen_cover gco
                                                                                where   gco.usercomp  =  cov.usercomp
                                                                                and     gco.company   =  cov.company
                                                                                and     gco.branch    =  cov.branch
                                                                                and     gco.product  =  pol.product
                                                                                and     gco.currency = cov.currency
                                                                                and     gco.modulec = cov.modulec
                                                                                and     gco.cover = cov.cover
                                                                                and     gco.effecdate <= cla.occurdat
                                                                                and     (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                and     coalesce(gco.addsuini,'1') = '3'
                                                                                and     gco.statregt <> '4'),
                                                                            (   select  max(ctid) 
                                                                                from    usinsug01.gen_cover gco
                                                                                where   gco.usercomp  =  cov.usercomp
                                                                                and     gco.company   =  cov.company
                                                                                and     gco.branch    =  cov.branch
                                                                                and     gco.product  =  pol.product
                                                                                and     gco.currency = cov.currency
                                                                                and     gco.cover = cov.cover
                                                                                and     gco.effecdate <= cla.occurdat
                                                                                and     (gco.nulldate is null or nulldate > cla.occurdat)
                                                                                and     coalesce(gco.addsuini,'1') = '3'
                                                                                and     gco.statregt <> '4')))
                                                then	coalesce ( (  select cast( max(cov.capital) as varchar)
                                                            from    usinsug01.cover cov
                                                            where   cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = '2'
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)),'0')
                                                else	coalesce ( (select cast( sum(cov.capital) as varchar )
                                                            from    usinsug01.cover cov
                                                            join	usinsug01.gen_cover gco
                                                            on 		cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = '2'
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif							
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                            and     gco.ctid =
                                                                    coalesce((	select   max(ctid)
                                                                                from    usinsug01.gen_cover gco
                                                                                where   gco.usercomp  =  cov.usercomp
                                                                                and     gco.company   =  cov.company
                                                                                and     gco.branch    =  cov.branch
                                                                                and     gco.product  =  pol.product
                                                                                and     gco.currency = cov.currency
                                                                                and     gco.modulec = cov.modulec
                                                                                and     gco.cover = cov.cover
                                                                                and     gco.effecdate <= cla.occurdat
                                                                                    and     (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                    and     (gco.addsuini is null or gco.addsuini ='1')
                                                                                    and     gco.statregt <> '4'),
                                                                                (   select  max(ctid) 
                                                                                    from    usinsug01.gen_cover gco
                                                                                    where   gco.usercomp  =  cov.usercomp
                                                                                    and     gco.company   =  cov.company
                                                                                    and     gco.branch    =  cov.branch
                                                                                    and     gco.product  =  pol.product
                                                                                    and     gco.currency = cov.currency
                                                                                    and     gco.cover = cov.cover
                                                                                    and     gco.effecdate <= cla.occurdat
                                                                                    and     (gco.nulldate is null or nulldate > cla.occurdat)
                                                                                    and     (gco.addsuini is null or gco.addsuini ='1')
                                                                                    and     gco.statregt <> '4'))),'0' )
                                                end AS "VMTCAPIT",
                                        '100' AS "VTXQUOTA",
                                        '' as "VMTINDEM",
                                        coalesce ( pol.leadpoli , '') as "DNUMAPO_CSG",
                                        coalesce(	coalesce((	select	cast ( max(cpl.currency) as varchar )
                                                                from	usinsug01.curren_pol cpl
                                                                where 	cpl.usercomp = cla.usercomp
                                                                and 	cpl.company = cla.company
                                                                and		cpl.certype = '2'
                                                                and		cpl.branch = cla.branch
                                                                and 	cpl.policy = cla.policy
                                                                and		cpl.certif = cla.certif),
                                                            (	select	cast (max(cpl.currency) as varchar )
                                                                from	usinsug01.curren_pol cpl
                                                                where 	cpl.usercomp = cla.usercomp
                                                                and 	cpl.company = cla.company
                                                                and		cpl.certype = '2'
                                                                and		cpl.branch = cla.branch
                                                                and 	cpl.policy = cla.policy)),'0') AS "KSCMOEDA"
                                from	usinsug01.claim cla
                                join 	usinsug01.policy pol
                                on 	cla.branch = 23  or (cla.branch <> 23 and cla.staclaim <> '6')
                                and		pol.usercomp = cla.usercomp 
                                and 	pol.company = cla.company
                                and 	pol.branch = cla.branch 
                                and 	pol.policy = cla.policy
                                and		pol.bussityp = '2'
                                )
                                union all
                                ---------------------------LPV BUSSITYP 1----------------------------
                                (select	
                                        'D' as "INDDETREC",
                                        'SBCOSSEG' as "TABLAIFRS17",
                                        '' AS "PK",
                                        '' AS "DTPREG"	,
                                        '' AS "TIOCPROC",
                                        '' AS "TIOCFRM" ,
                                        '' as "TIOCTO",
                                        'PIV' as "KGIORIGM",
                                        'LPV' AS "DCOMPA",
                                        '' as "DMARCA",
                                        '' as  "KSBSIN",
                                        coalesce ( cast ( coi.companyc as varchar) , '0') as "DCODCSG",
                                        '' as "DNUMSEQ",
                                        '' as "TDPLANO",
                                        cast (cla.capital as varchar )as "VMTCAPIT", --agregar c�lculo
                                        coalesce(cast( coi.share as varchar ), '0' ) as "VTXQUOTA",
                                        '' as "VMTINDEM", --NOAPP
                                        cast(cla.policy as varchar) as  "DNUMAPO_CSG",
                                        cast (cla.moneda_cod as varchar ) as "KSCMOEDA" --agregar c�lculo
                                from	(	select 	cla.claim,
                                                    cla.branch,
                                                    cla.policy,
                                                    cla.occurdat,
                                                    cla.staclaim,
                                                    case    when    coalesce((	select  min(brancht)
                                                                                from    usinsuv01.product 
                                                                                where   usercomp = cla.usercomp
                                                                                and     company = cla.company
                                                                                and     branch = cla.branch
                                                                                and     product = pol.product
                                                                                and     effecdate <= cla.occurdat
                                                                                and     (nulldate is null or nulldate > cla.occurdat)),'0') not in ('0','5','1')
                                                            then	(	select  sum(cov.capital)
                                                                        from    usinsuv01.cover  cov
                                                                        join 	usinsuv01.gen_cover  gco
                                                                        on   cov.usercomp = cla.usercomp
                                                                        and     cov.company = cla.company
                                                                        and     cov.certype = '2'
                                                                        and     cov.branch = cla.branch
                                                                        and     cov.policy = cla.policy
                                                                        and     cov.certif = cla.certif
                                                                        and     cov.effecdate <= cla.occurdat
                                                                        and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                        and     gco.ctid =
                                                                                coalesce((	select  max(ctid)
                                                                                            from    usinsuv01.gen_cover gco
                                                                                            where   gco.usercomp  =  cov.usercomp
                                                                                            and     gco.company   =  cov.company
                                                                                            and     gco.branch    =  cov.branch
                                                                                            and     gco.product  =  pol.product
                                                                                            and     gco.currency = cov.currency
                                                                                            and     gco.modulec = cov.modulec
                                                                                            and     gco.cover = cov.cover
                                                                                            and     gco.effecdate <= cla.occurdat
                                                                                            and     (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                            and     (gco.addsuini is null or gco.addsuini ='1')
                                                                                            and     gco.statregt <> '4'),
                                                                                        (   select  max(ctid)
                                                                                            from    usinsuv01.gen_cover gco
                                                                                            where   gco.usercomp  =  cov.usercomp
                                                                                            and     gco.company   =  cov.company
                                                                                            and     gco.branch    =  cov.branch
                                                                                            and     gco.product  =  pol.product
                                                                                            and     gco.currency = cov.currency
                                                                                            and     gco.cover = cov.cover
                                                                                            and     gco.effecdate <= cla.occurdat
                                                                                            and     (gco.nulldate is null or nulldate > cla.occurdat)
                                                                                            and     (gco.addsuini is null or gco.addsuini ='1')
                                                                                            and     gco.statregt <> '4'))) 
                                                            else	(	select  coalesce(sum(coalesce(cov.capital,0)), 0)
                                                                        from    usinsuv01.cover cov
                                                                        join	usinsuv01.life_cover lco
                                                                        on      cov.usercomp = cla.usercomp
                                                                        and     cov.company = cla.company
                                                                        and     cov.certype = '2'
                                                                        and     cov.branch = cla.branch
                                                                        and     cov.policy = cla.policy
                                                                        and     cov.certif = cla.certif
                                                                        and     cov.effecdate <= cla.occurdat
                                                                        and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                        and     cov.branch = lco.branch
                                                                        and     cov.currency = lco.currency
                                                                        and     cov.cover = lco.cover
                                                                        and     lco.usercomp = cla.usercomp
                                                                        and     lco.company = cla.company
                                                                        and     lco.product = pol.product
                                                                        and     lco.effecdate <= cla.occurdat
                                                                        and     (lco.nulldate is null or lco.nulldate > cla.occurdat)
                                                                        and     (lco.addcapii is null or lco.addcapii = '1'))
                                                            end capital,
                                                    coalesce(	coalesce((	select	max(cpl.currency)
                                                                            from	usinsuv01.curren_pol cpl
                                                                            where 	cpl.usercomp = cla.usercomp
                                                                            and 	cpl.company = cla.company
                                                                            and		cpl.certype = '2'
                                                                            and		cpl.branch = cla.branch
                                                                            and 	cpl.policy = cla.policy
                                                                            and		cpl.certif = cla.certif),
                                                                        (	select	max(cpl.currency)
                                                                            from	usinsuv01.curren_pol cpl
                                                                            where 	cpl.usercomp = cla.usercomp
                                                                            and 	cpl.company = cla.company
                                                                            and		cpl.certype = '2'
                                                                            and		cpl.branch = cla.branch
                                                                            and 	cpl.policy = cla.policy)),0) moneda_cod
                                            from	usinsuv01.claim cla
                                            join	usinsuv01.policy pol
                                            on 	((cla.branch = 23 and cla.staclaim = '6')
                                                    or (cla.branch <> 23 and cla.staclaim <> '6'))
                                            and		pol.usercomp = cla.usercomp 
                                            and 	pol.company = cla.company
                                            and 	pol.branch = cla.branch 
                                            and 	pol.policy = cla.policy
                                            and		pol.bussityp = '1') cla
                                join  usinsuv01.coinsuran coi
                                on	coi.usercomp = 1
                                and     coi.company = 1
                                and     coi.certype = '2'
                                and     coi.branch = cla.branch
                                and     coi.policy = cla.policy
                                and     coi.effecdate <= cla.occurdat
                                and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                )
                                union all 
                                ---------------------------LPV BUSSITYP 2----------------------------
                                (
                                select	
                                        'D' as "INDDETREC",
                                        'SBCOSSEG' as "TABLAIFRS17",
                                        '' AS "PK",
                                        '' AS "DTPREG"	,
                                        '' AS "TIOCPROC",
                                        '' AS "TIOCFRM" ,
                                        '' as "TIOCTO",
                                        'PIV' as "KGIORIGM",
                                        'LPV' AS "DCOMPA",
                                        '' as "DMARCA",
                                        '' as "KSBSIN",
                                        '1' "DCODCSG",
                                        '' as "DNUMSEQ",
                                        '' as "TDPLANO",
                                        case    when    coalesce((	select  min(brancht)
                                                        from    usinsuv01.product
                                                        where   usercomp = cla.usercomp
                                                        and     company = cla.company
                                                        and     branch = cla.branch
                                                        and     product = pol.product
                                                        and     effecdate <= cla.occurdat
                                                        and     (nulldate is null or nulldate > cla.occurdat)),'0') not in ('0','5','1')
                                                then	( select  cast( sum(cov.capital) as varchar)
                                                from    usinsuv01.cover cov
                                                join	usinsuv01.gen_cover gco
                                                on   cov.usercomp = cla.usercomp
                                                and     cov.company = cla.company
                                                and     cov.certype = '2'
                                                and     cov.branch = cla.branch
                                                and     cov.policy = cla.policy
                                                and     cov.certif = cla.certif
                                                and     cov.effecdate <= cla.occurdat
                                                and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                and     gco.ctid =
                                                        coalesce((	select  max(ctid)
                                                        from    usinsuv01.gen_cover gco
                                                        where   gco.usercomp  =  cov.usercomp
                                                        and     gco.company   =  cov.company
                                                        and     gco.branch    =  cov.branch
                                                        and     gco.product  =  pol.product
                                                        and     gco.currency = cov.currency
                                                        and     gco.modulec = cov.modulec
                                                        and     gco.cover = cov.cover
                                                        and     gco.effecdate <= cla.occurdat
                                                        and     (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                        and     (gco.addsuini is null or gco.addsuini ='1')
                                                        and     gco.statregt <> '4'),
                                                                (   select  max(ctid)
                                                        from    usinsuv01.gen_cover gco
                                                        where   gco.usercomp  =  cov.usercomp
                                                        and     gco.company   =  cov.company
                                                        and     gco.branch    =  cov.branch
                                                        and     gco.product  =  pol.product
                                                        and     gco.currency = cov.currency
                                                        and     gco.cover = cov.cover
                                                        and     gco.effecdate <= cla.occurdat
                                                        and     (gco.nulldate is null or nulldate > cla.occurdat)
                                                        and     (gco.addsuini is null or gco.addsuini ='1')
                                                        and     gco.statregt <> '4'))) 
                                                else	(	select  coalesce(cast ( sum (cov.capital) as varchar ), '0')
                                                from    usinsuv01.cover cov
                                                join	usinsuv01.life_cover lco
                                                on   	cov.usercomp = cla.usercomp
                                                and     cov.company = cla.company
                                                and     cov.certype = '2'
                                                and     cov.branch = cla.branch
                                                and     cov.policy = cla.policy
                                                and     cov.certif = cla.certif
                                                and     cov.effecdate <= cla.occurdat
                                                and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                and     cov.branch = lco.branch
                                                and     cov.currency = lco.currency
                                                and     cov.cover = lco.cover
                                                and     lco.usercomp = cla.usercomp
                                                and     lco.company = cla.company
                                                and     lco.product = pol.product
                                                and     lco.effecdate <= cla.occurdat
                                                and     (lco.nulldate is null or lco.nulldate > cla.occurdat)
                                                and     (lco.addcapii is null or lco.addcapii = '1'))
                                                end "VMTCAPIT",
                                        '100' "VTXQUOTA",
                                        '' "VMTINDEM",
                                        coalesce (pol.leadpoli, '') "DNUMAPO_CSG",
                                        coalesce(	coalesce((	select	cast ( max(cpl.currency) as varchar )
                                                                from	usinsuv01.curren_pol cpl
                                                                where 	cpl.usercomp = cla.usercomp
                                                                and 	cpl.company = cla.company
                                                                and		cpl.certype = '2'
                                                                and		cpl.branch = cla.branch
                                                                and 	cpl.policy = cla.policy
                                                                and		cpl.certif = cla.certif),
                                                            (	select	cast ( max(cpl.currency) as varchar )
                                                                from	usinsuv01.curren_pol cpl
                                                                where 	cpl.usercomp = cla.usercomp
                                                                and 	cpl.company = cla.company
                                                                and		cpl.certype = '2'
                                                                and		cpl.branch = cla.branch
                                                                and 	cpl.policy = cla.policy)),'0') "KSCMOEDA"
                                from	usinsuv01.claim cla
                                join 	usinsuv01.policy pol
                                on 	    cla.branch = 23 and cla.staclaim = '6' or (cla.branch <> 23 and cla.staclaim <> '6')
                                and		pol.usercomp = cla.usercomp 
                                and 	pol.company = cla.company
                                and 	pol.branch = cla.branch 
                                and 	pol.policy = cla.policy
                                and		pol.bussityp = '2'
                                )			

                            ) AS TMP
                            '''

  L_DF_SBCOSSEG_INSUNIX = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_SBCOSSEG_INSUNIX).load()

  L_SBCOSSEG_VTIME = f'''
                            (
                                (
                                select	
                                    'D' as "INDDETREC",
                                    'SBCOSSEG' as "TABLAIFRS17",
                                        '' AS "PK",
                                        '' AS "DTPREG"	,
                                        '' AS "TIOCPROC",
                                        '' AS "TIOCFRM" ,
                                        '' as "TIOCTO",
                                        'PVV' as "KGIORIGM",
                                        'LPV' AS "DCOMPA",
                                        '' as "DMARCA",
                                        coalesce ( cla."NCLAIM" , 0 ) || '-' || 'LPV' AS "KSBSIN",
                                        '1'AS "DCODCSG",
                                        '' as "DNUMSEQ",
                                        '' as "TDPLANO",
                                        coalesce (	
                                                    (   select  cast( sum( cov."NCAPITAL") as varchar  )
                                                        from    usvtimv01."COVER" cov
                                                        join 	usvtimv01."LIFE_COVER"  lco
                                                        on      cov."SCERTYPE" = '2'
                                                        and     cov."NBRANCH"  = cla."NBRANCH" 
                                                        and     cov."NPRODUCT" = pol."NPRODUCT" 
                                                        and     cov."NPOLICY"  = cla."NPOLICY" 
                                                        and     cov."NCERTIF"  = cla."NCERTIF" 
                                                        and     cov."DEFFECDATE" <= cla."DOCCURDAT" 
                                                        and     (cov."DNULLDATE"  is null or cov."DNULLDATE" > cla."DOCCURDAT")
                                                        and     lco."NCOVER"  = cov."NCOVER" 
                                                        and     lco."NPRODUCT"  = pol."NPRODUCT" 
                                                        and 	lco."NMODULEC" = cov."NMODULEC" 
                                                        and     lco."NBRANCH"  = cov."NBRANCH" 
                                                        and     lco."DEFFECDATE"  <= cla."DOCCURDAT" 
                                                        and     (lco."DNULLDATE"  is null or lco."DNULLDATE" > cla."DOCCURDAT")
                                                        and     lco."SSTATREGT"  <> '4' 
                                                        and     (lco."SADDSUINI" in ( '1', '3') ) 
                                                    ),
                                                    '0'		
                                                ) AS "VMTCAPIT",
                                        '100' AS "VTXQUOTA",
                                        '' AS "VMTINDEM",
                                        pol."SLEADPOLI" AS "DNUMAPO_CSG",
                                        coalesce(	coalesce((	select	cast (max(cpl."NCURRENCY") as varchar)
                                                                from	usvtimv01."CURREN_POL"  cpl
                                                                where   cpl."SCERTYPE"  = '2'
                                                                and		cpl."NBRANCH"  = cla."NBRANCH" 
                                                                and 	cpl."NPOLICY"  = cla."NPOLICY" 
                                                                and		cpl."NCERTIF"  = cla."NCERTIF"),
                                                            (	select	cast (max(cpl."NCURRENCY") as varchar )
                                                                from	usvtimv01."CURREN_POL"  cpl
                                                                where 	cpl."SCERTYPE" = '2'
                                                                and		cpl."NBRANCH" = cla."NBRANCH" 
                                                                and 	cpl."NPOLICY"  = cla."NPOLICY")),'0') AS "KSCMOEDA"
                                from	usvtimv01."CLAIM"  cla
                                join	usvtimv01."POLICY"  pol
                                on 	(cla."NBRANCH" = 23 or (cla."NBRANCH" <> 23 and cla."SSTACLAIM" <> '6'))
                                and		pol."SCERTYPE"  = cla."SCERTYPE" 
                                and 	pol."NBRANCH"  = cla."NBRANCH" 
                                and 	pol."NPRODUCT"  = cla."NPRODUCT" 
                                and     pol."NPOLICY" = cla."NPOLICY" 
                                and		pol."SBUSSITYP" = '2'
                                )

                                union  all

                                (
                                select	
                                        'D' as "INDDETREC",
                                        'SBCOSSEG' as "TABLAIFRS17",
                                        '' AS "PK",
                                        '' AS "DTPREG"	,
                                        '' AS "TIOCPROC",
                                        '' AS "TIOCFRM" ,
                                        '' as "TIOCTO",
                                        'PVV' as "KGIORIGM",
                                        'LPV' as  "DCOMPA",
                                        '' as "DMARCA",
                                        tmpcla."NCLAIM" || '|' || 'LPV' as "KSBSIN",
                                        coi."NCOMPANY"  as "DCODCSG",
                                        '' as "DNUMSEQ",
                                        '' as "TDPLANO",
                                        cast (tmpcla."capital" as VARCHAR )  as "VMTCAPIT", --agregar c�lculo
                                        coalesce (cast (coi."NSHARE"  as varchar ) , '0') as  "VTXQUOTA",
                                        '' as "VMTINDEM", --NOAPP
                                        cast(tmpcla."NPOLICY" as varchar)as "DNUMAPO_CSG",
                                        cast (tmpcla."moneda_cod"   as varchar )  as "KSCMOEDA" --agregar c�lculo
                                from	(
                                        select 	cla."NCLAIM",
                                                    cla."NBRANCH" ,
                                                    cla."NPOLICY" ,
                                                    cla."DOCCURDAT" ,
                                                    cla."SSTACLAIM" ,
                                                    coalesce (
                                                        (	select  sum(cov."NCAPITAL")
                                                            from    usvtimv01."COVER" cov
                                                            JOIN	usvtimv01."LIFE_COVER"  lco
                                                            ON  cov."SCERTYPE" = '2'
                                                            and     cov."NBRANCH"  = cla."NBRANCH" 
                                                            and     cov."NPRODUCT" = pol."NPRODUCT" 
                                                            and     cov."NPOLICY"  = cla."NPOLICY" 
                                                            and     cov."NCERTIF"  = cla."NCERTIF" 
                                                            and     cov."DEFFECDATE" <= cla."DOCCURDAT"  
                                                            and     (cov."DNULLDATE"  is null or cov."DNULLDATE" > cla."DOCCURDAT")
                                                            and     lco."NCOVER"  = cov."NCOVER" 
                                                            and     lco."NPRODUCT"  = pol."NPRODUCT" 
                                                            and 	lco."NMODULEC" = cov."NMODULEC" 
                                                            and     lco."NBRANCH"  = cov."NBRANCH" 
                                                            and     lco."DEFFECDATE"  <= cla."DOCCURDAT" 
                                                            and     (lco."DNULLDATE"  is null or lco."DNULLDATE" > cla."DOCCURDAT")
                                                            and     lco."SSTATREGT"  <> '4' 
                                                            and     lco."SADDSUINI" in ( '1', '3')
                                                        ) , 
                                                        0
                                                    ) capital,
                                                    coalesce(	coalesce((	select	max(cpl."NCURRENCY")
                                                                            from	"CURREN_POL"  cpl
                                                                            where   cpl."SCERTYPE"  = '2'
                                                                            and		cpl."NBRANCH"  = cla."NBRANCH" 
                                                                            and 	cpl."NPOLICY"  = cla."NPOLICY" 
                                                                            and		cpl."NCERTIF"  = cla."NCERTIF"),
                                                                        (	select	max(cpl."NCURRENCY")
                                                                            from	"CURREN_POL"  cpl
                                                                            where 	cpl."SCERTYPE"  = '2'
                                                                            and		cpl."NBRANCH"  = cla."NBRANCH" 
                                                                            and 	cpl."NPOLICY"  = cla."NPOLICY")),0) moneda_cod
                                            from	usvtimv01."CLAIM"  cla,
                                                    usvtimv01."POLICY" pol
                                            where   cla."NBRANCH"  = 23  or (cla."NBRANCH"  <> 23 and cla."SSTACLAIM"  <> '6')
                                            and 	pol."NBRANCH"  = cla."NBRANCH" 
                                            and 	pol."NPOLICY"  = cla."NPOLICY" 
                                            and		pol."SBUSSITYP"  = '1'
                                        ) tmpcla
                                join usvtimv01."COINSURAN" coi
                                on	coi."NCOMPANY" = 1
                                and     coi."SCERTYPE" = '2'
                                and     coi."NBRANCH" = tmpcla."NBRANCH"
                                and     coi."NPOLICY" = tmpcla."NPOLICY"
                                and     coi."DEFFECDATE"  <= tmpcla."DOCCURDAT"
                                and     (coi."DNULLDATE" is null or coi."DNULLDATE" > tmpcla."DOCCURDAT")
                                ) 

                                union all

                                (
                                select	
                                        'D' as "INDDETREC",
                                        'SBCOSSEG' as "TABLAIFRS17",
                                        '' AS "PK",
                                        '' AS "DTPREG"	,
                                        '' AS "TIOCPROC",
                                        '' AS "TIOCFRM" ,
                                        '' as "TIOCTO",
                                        'PVG' as "KGIORIGM",
                                        'LPG' AS "DCOMPA",
                                        '' as "DMARCA",
                                        tmpcla."NCLAIM" || '|' || 'LPG'  as "KSBSIN",
                                        coi."NCOMPANY" as "DCODCSG",
                                        '' as "DNUMSEQ",
                                        '' as "TDPLANO",
                                        cast (tmpcla.capital as varchar ) "VMTCAPIT", --agregar c�lculo
                                        cast (coi."NSHARE" as varchar ) as  "VTXQUOTA",
                                        '' "VMTINDEM", --NOAPP
                                        cast(tmpcla."NPOLICY" as varchar) as  "DNUMAPO_CSG",
                                        cast (tmpcla.moneda_cod as varchar ) as  "KSCMOEDA" --agregar c�lculo
                                from	(
                                        select 	
                                                    cla."NCLAIM" ,
                                                    cla."NBRANCH" ,
                                                    cla."NPOLICY" ,
                                                    cla."DOCCURDAT" ,
                                                    cla."SSTACLAIM" ,
                                                    (
                                                        select  coalesce(sum(coalesce(cov."NCAPITAL" ,0)), 0)
                                                        from    "COVER" cov,
                                                                "LIFE_COVER"  lco
                                                        where   cov."SCERTYPE" = '2'
                                                        and     cov."NBRANCH"  = cla."NBRANCH" 
                                                        and     cov."NPRODUCT" = pol."NPRODUCT" 
                                                        and     cov."NPOLICY"  = cla."NPOLICY" 
                                                        and     cov."NCERTIF"  = cla."NCERTIF" 
                                                        and     cov."DEFFECDATE" <= cla."DOCCURDAT" 
                                                        and     (cov."DNULLDATE"  is null or cov."DNULLDATE" > cla."DOCCURDAT")
                                                        and     lco."NCOVER"  = cov."NCOVER" 
                                                        and     lco."NPRODUCT"  = pol."NPRODUCT" 
                                                        and 	lco."NMODULEC" = cov."NMODULEC" 
                                                        and     lco."NBRANCH"  = cov."NBRANCH" 
                                                        and     lco."DEFFECDATE"  <= cla."DOCCURDAT" 
                                                        and     (lco."DNULLDATE"  is null or lco."DNULLDATE" > cla."DOCCURDAT")
                                                        and     lco."SSTATREGT"  <> '4' 
                                                        and     lco."SADDSUINI" in ( '1', '3') 
                                                    ) capital,
                                                    coalesce(	coalesce((	select	max(cpl."NCURRENCY")
                                                                            from	"CURREN_POL"  cpl
                                                                            where 	cpl."SCERTYPE" = '2'
                                                                            and		cpl."NBRANCH"  = cla."NBRANCH" 
                                                                            and 	cpl."NPOLICY" = cla."NPOLICY" 
                                                                            and		cpl."NCERTIF"  = cla."NCERTIF"),
                                                                        (	select	max(cpl."NCURRENCY")
                                                                            from	"CURREN_POL"  cpl
                                                                            where 	cpl."SCERTYPE"  = '2'
                                                                            and		cpl."NBRANCH"  = cla."NBRANCH" 
                                                                            and 	cpl."NPOLICY"  = cla."NPOLICY")),0) moneda_cod
                                            from	usvtimg01."CLAIM"  cla,
                                                    usvtimg01."POLICY"  pol
                                            where 	cla."NBRANCH"  = 23 or (cla."NBRANCH" <> 23 and cla."SSTACLAIM" <> '6')
                                            and 	pol."NBRANCH"  = cla."NBRANCH" 
                                            and 	pol."NPOLICY"  = cla."NPOLICY" 
                                            and		pol."SBUSSITYP"  = '1'
                                        ) tmpcla
                                join  usvtimg01."COINSURAN" coi
                                on 	coi."NCOMPANY" = 1
                                and     coi."SCERTYPE" = '2'
                                and     coi."NBRANCH"  = tmpcla."NBRANCH" 
                                and     coi."NPOLICY" = tmpcla."NPOLICY"
                                and     coi."DEFFECDATE" <= tmpcla."DOCCURDAT" 
                                and     (coi."DNULLDATE" is null or coi."DNULLDATE" > tmpcla."DOCCURDAT")
                                )

                                union all 

                                (
                                select	
                                        'D' as "INDDETREC",
                                        'SBCOSSEG' as "TABLAIFRS17",
                                        '' AS "PK",
                                        '' AS "DTPREG"	,
                                        '' AS "TIOCPROC",
                                        '' AS "TIOCFRM" ,
                                        '' as "TIOCTO",
                                        'PVG' as "KGIORIGM",
                                        'LPG' "DCOMPA",
                                        '' as "DMARCA",
                                        coalesce ( cla."NCLAIM" , '0' ) || '-' || 'LPG' as "KSBSIN",
                                        '1' as  "DCODCSG", 
                                        '' as "DNUMSEQ",
                                        '' as "TDPLANO",
                                        coalesce(
                                                    (	select cast( sum(cov."NCAPITAL" ) as varchar)
                                                        from   usvtimg01."COVER" cov
                                                        join  usvtimg01."LIFE_COVER"  lco
                                                        on   cov."SCERTYPE" = '2'
                                                        and     cov."NBRANCH"  = cla."NBRANCH" 
                                                        and     cov."NPRODUCT" = pol."NPRODUCT" 
                                                        and     cov."NPOLICY"  = cla."NPOLICY" 
                                                        and     cov."NCERTIF"  = cla."NCERTIF" 
                                                        and     cov."DEFFECDATE" <= cla."DOCCURDAT" 
                                                        and     (cov."DNULLDATE"  is null or cov."DNULLDATE" > cla."DOCCURDAT")
                                                        and     lco."NCOVER"  = cov."NCOVER" 
                                                        and     lco."NPRODUCT"  = pol."NPRODUCT" 
                                                        and 	lco."NMODULEC" = cov."NMODULEC" 
                                                        and     lco."NBRANCH"  = cov."NBRANCH" 
                                                        and     lco."DEFFECDATE"  <= cla."DOCCURDAT" 
                                                        and     (lco."DNULLDATE"  is null or lco."DNULLDATE" > cla."DOCCURDAT")
                                                        and     lco."SSTATREGT"  <> '4' 
                                                        and     (lco."SADDSUINI" in ( '1', '3') ) 
                                                    ),
                                                    '0'
                                                ) as  "VMTCAPIT",
                                        '100' "VTXQUOTA",
                                        '' "VMTINDEM",
                                        pol."SLEADPOLI" as  "DNUMAPO_CSG",
                                        coalesce(	coalesce((	select	cast( max(cpl."NCURRENCY")as varchar )
                                                                from	usvtimg01."CURREN_POL"  cpl
                                                                where 	cpl."SCERTYPE" = '2'
                                                                and		cpl."NBRANCH" = cla."NBRANCH"       
                                                                and 	cpl."NPOLICY"  = cla."NPOLICY" 
                                                                and		cpl."NCERTIF"  = cla."NCERTIF"),
                                                            (	select	cast ( max(cpl."NCURRENCY") as varchar )
                                                                from	usvtimg01."CURREN_POL"  cpl
                                                                where   cpl."SCERTYPE" = '2'
                                                                and		cpl."NBRANCH"  = cla."NBRANCH" 
                                                                and 	cpl."NPOLICY"  = cla."NPOLICY")),'0') as  "KSCMOEDA"
                                    from	usvtimg01."CLAIM"  cla
                                    join 	usvtimg01."POLICY"  pol
                                    on      (cla."NBRANCH"  = 23 or (cla."NBRANCH" <> 23 and cla."SSTACLAIM" <> '6'))
                                    and 	pol."NBRANCH"  = cla."NBRANCH" 
                                    and 	pol."NPOLICY"  = cla."NPOLICY" 
                                    and		pol."SBUSSITYP"  = '2'
                                )
                            ) AS TMP
                            '''

  L_DF_SBCOSSEG_VTIME = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_SBCOSSEG_VTIME).load()

  L_DF_SBCOSSEG = L_SBCOSSEG_INSUNIX.union(L_DF_SBCOSSEG_VTIME)#.union(L_DF_ABPRCOB_INSIS)

  return L_DF_SBCOSSEG                          