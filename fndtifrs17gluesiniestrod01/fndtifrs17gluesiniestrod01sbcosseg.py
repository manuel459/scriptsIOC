
def get_data(glue_context, connection):

    LPG_NEGOCIO1_INSUNIX = '''
                            (
                                select	'D' as INDDETREC,
                                        'SBCOSSEG' as TABLAIFRS17,
                                        ''/*cla.claim || par.sep || par.cia || par.sep || coi.companyc*/ as  PK,
                                        '' as DTPREG,
                                        '' as TIOCPROC,
                                        COALESCE(CAST(coi.EFFECDATE as VARCHAR) , '') as TIOCFRM,
                                        '' as TIOCTO,
                                        'PIG' as KGIORIGM,
                                        'LPG' as  DCOMPA,
                                        '' as DMARCA,
                                        cla.claim as KSBSIN,
                                        cast (coi.companyc as varchar ) as  DCODCSG,
                                        '' as DNUMSEQ,
                                        '' as TDPLANO,
                                        cast ( coalesce ( case    when    exists
                                                        (   select  1
                                                            from    usinsug01.cover cov
                                                            join    usinsug01.gen_cover gco
                                                            on      cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = pol.certype
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                            and     gco.ctid =
                                                                    coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini ='3'), --variaci�n 3 reg. v�lido
                                                                                                        (   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini ='3')), --variaci�n 3 reg. v�lido
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini ='3'))), --no est� cortado
                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                    from    usinsug01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini ='3'),
                                                                                                                (   select  max(ctid)
                                                                                                                    from usinsug01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini ='3')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                            coalesce(coalesce(( select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate > cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                coalesce((  select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                            and     statregt = '4' and addsuini ='3'),
                                                                                                        (   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover
                                                                                                            and     effecdate > cla.occurdat
                                                                                                            and     statregt = '4' and addsuini ='3'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                then    (   select  max(cov.capital)
                                                            from    usinsug01.cover cov
                                                            join    usinsug01.gen_cover gco
                                                            on      cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = pol.certype
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                            and     gco.ctid =
                                                                    coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini ='3'), --variaci�n 3 reg. v�lido
                                                                                                        (   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini ='3')), --variaci�n 3 reg. v�lido
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini ='3'))), --no est� cortado
                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                    from    usinsug01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini ='3'),
                                                                                                                (   select  max(ctid)
                                                                                                                        from  usinsug01.gen_cover
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from  usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini ='3')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                            coalesce(coalesce(( select  max(ctid)
                                                                                                                from usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate > cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                coalesce((  select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                            and     statregt = '4' and addsuini ='3'),
                                                                                                        (   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover
                                                                                                            and     effecdate > cla.occurdat
                                                                                                            and     statregt = '4' and addsuini ='3'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                Else
                                                        (   select  sum(cov.capital)
                                                            from    usinsug01.cover cov
                                                            join    usinsug01.gen_cover gco
                                                            on cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = pol.certype
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                            and     gco.ctid =
                                                                    coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini = '1'), --variaci�n 3 reg. v�lido
                                                                                                        (   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini = '1')), --variaci�n 3 reg. v�lido
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini = '1'),
                                                                                                            (   select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini = '1'))), --no est� cortado
                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                    from    usinsug01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini = '1'),
                                                                                                                (   select  max(ctid)
                                                                                                                    from    usinsug01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini = '1')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini = '1'),
                                                                                                            (   select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini = '1')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                            coalesce(coalesce(( select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate > cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini = '1'),
                                                                                                            (   select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini = '1')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                coalesce((  select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                            and     statregt = '4' and addsuini = '1'),
                                                                                                        (   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover
                                                                                                            and     effecdate > cla.occurdat
                                                                                                            and     statregt = '4' and addsuini = '1'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                end *
                                            case    when    cla.branch = 66
                                                    then    (   select  max(exc.exchange)
                                                                from   usinsug01.exchange exc
                                                                where exc.usercomp = cla.usercomp
                                                                and     exc.company = cla.company
                                                                and     exc.currency = 99
                                                                and     exc.effecdate <= cla.occurdat
                                                                and     (exc.nulldate is null or exc.nulldate > cla.occurdat))
                                                    else    1 end , 0) as varchar) as  VMTCAPIT ,
                                        coi.share VTXQUOTA,
                                        '' VMTINDEM, --NOAPP	
                                        cast(cla.policy as varchar) DNUMAPO_CSG,
                                        cast ( coalesce(   coalesce((  select  max(cpl.currency)
                                                                from    usinsug01.curren_pol cpl
                                                                where	cpl.usercomp = cla.usercomp
                                                                and     cpl.company = cla.company
                                                                and     cpl.certype = pol.certype
                                                                and     cpl.branch = cla.branch
                                                                and     cpl.policy = cla.policy
                                                                and     cpl.certif = cla.certif),
                                                            (   select  max(cpl.currency)
                                                                from    usinsug01.curren_pol cpl
                                                                where	cpl.usercomp = cla.usercomp
                                                                and     cpl.company = cla.company
                                                                and     cpl.certype = pol.certype
                                                                and     cpl.branch = cla.branch
                                                                and     cpl.policy = cla.policy)),0) as varchar ) as KSCMOEDA
                                from 	(	select	case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6'))
                                                                    and pol.certype = '2' and pol.bussityp = '1'
                                                            then	cla.ctid
                                                            else	null 
                                                    end cla_id,
                                                    case 	when 	pol.certype = '2' and pol.bussityp = '1'
                                                            then 	pol.ctid
                                                            else	null 
                                                    end pol_id
                                            from    usinsug01.policy pol
                                            JOIN    usinsug01.claim cla on	cla.usercomp = pol.usercomp
                                                                        and     cla.company = pol.company
                                                                        and     cla.branch = pol.branch
                                                                        and     cla.policy = pol.policy 
                                                                        and     cla.branch  not in (60)
                                            join   (   select  distinct clh.claim
                                                    from    ( select  cast(tcl.operation as varchar(2)) operation
                                                                from    usinsug01.tab_cl_ope tcl
                                                                where   (tcl.reserve = 1 or tcl.ajustes = 1 or pay_amount = 1)) tcl --reservas, ajustes o pagos
                                                        JOIN    usinsug01.claim_his clh  ON coalesce (clh.claim,0) > 0
                                                                                        and trim(clh.oper_type) = tcl.operation
                                                                                        and clh.operdate >= '12/31/2022'
                                                    limit 10                                  
                                                    ) clh ON clh.claim = cla.claim /*4m 29 s*/limit 10) cl0
                                JOIN   usinsug01.claim cla ON cla.ctid = cl0.cla_id
                                JOIN   usinsug01.policy pol ON pol.ctid = cl0.pol_id
                                JOIN   usinsug01.coinsuran coi 	ON  coi.usercomp = cla.usercomp
                                                    and     coi.company = cla.company
                                                    and     coi.certype = pol.certype
                                                    and     coi.branch = cla.branch
                                                    and     coi.policy = cla.policy
                                                    and     coi.effecdate <= cla.occurdat
                                                    and     (coi.nulldate is null or coi.nulldate > cla.occurdat)  
                            ) AS TMP    
                            '''

    DF_LPG_NEGOCIO1_INSUNIX = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",LPG_NEGOCIO1_INSUNIX).load()

    LPG_NEGOCIO2_INSUNIX = '''
                             (
                                select	'D' as INDDETREC,
                                        'SBCOSSEG' as TABLAIFRS17, 
                                        '' /*cla.claim || par.sep || par.cia || par.sep || 1*/ as PK,
                                        '' as DTPREG,
                                        '' as TIOCPROC,
                                    coalesce ( CASE WHEN cla.certif = 0 
                                            THEN cast (pol.effecdate as varchar)
                                        ELSE 
                                            (SELECT cast (c.effecdate as varchar)
                                            FROM usinsug01.certificat c 
                                            WHERE c.usercomp = cla.usercomp
                                            AND c.company = cla.company
                                            AND c.certype = pol.certype
                                            AND c.branch = cla.branch
                                            AND c.policy = cla.policy
                                            AND c.certif = cla.certif
                                            )
                                        end, '')
                                        as TIOCFRM,
                                        '' as TIOCTO,
                                        'PIG' as KGIORIGM,
                                        'LPG' as DCOMPA,
                                        '' as DMARCA,
                                        cla.claim AS KSBSIN,
                                        '1' AS DCODCSG ,
                                        '' as DNUMSEQ,
                                        '' as TDPLANO,
                                    cast ( coalesce ( case    when    exists
                                                        (   select  1
                                                            from    usinsug01.cover cov
                                                            join    usinsug01.gen_cover gco
                                                            on cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = pol.certype
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                            and     gco.ctid =
                                                                    coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                            from usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini ='3'), --variaci�n 3 reg. v�lido
                                                                                                        (   select  max(ctid)
                                                                                                            from usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini ='3')), --variaci�n 3 reg. v�lido
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from  usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from  usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini ='3'))), --no est� cortado
                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                    from  usinsug01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini ='3'),
                                                                                                                (   select  max(ctid)
                                                                                                                    from usinsug01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from   usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from  usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini ='3')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                            coalesce(coalesce(( select  max(ctid)
                                                                                                                from usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate > cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from  usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                coalesce((  select  max(ctid)
                                                                                                            from  usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                            and     statregt = '4' and addsuini ='3'),
                                                                                                        (   select  max(ctid)
                                                                                                            from usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover
                                                                                                            and     effecdate > cla.occurdat
                                                                                                            and     statregt = '4' and addsuini ='3'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                then    (   select  max(cov.capital) 
                                                            from    usinsug01.cover cov
                                                            join    usinsug01.gen_cover gco
                                                            on     cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = pol.certype
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                            and     gco.ctid =
                                                                    coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini ='3'), --variaci�n 3 reg. v�lido
                                                                                                        (   select  max(ctid)
                                                                                                            from    usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini ='3')), --variaci�n 3 reg. v�lido
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from    usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini ='3'))), --no est� cortado
                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                    from usinsug01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini ='3'),
                                                                                                                (   select  max(ctid)
                                                                                                                        from  usinsug01.gen_cover
                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from  usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from   usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini ='3')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                            coalesce(coalesce(( select  max(ctid)
                                                                                                                from  usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate > cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini ='3'),
                                                                                                            (   select  max(ctid)
                                                                                                                from usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                coalesce((  select  max(ctid)
                                                                                                            from  usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                            and     statregt = '4' and addsuini ='3'),
                                                                                                        (   select  max(ctid)
                                                                                                            from usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover
                                                                                                            and     effecdate > cla.occurdat
                                                                                                            and     statregt = '4' and addsuini ='3'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                Else
                                                        (   select  sum(cov.capital)
                                                            from    usinsug01.cover cov
                                                            join    usinsug01.gen_cover gco
                                                            on cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = pol.certype
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                            and     gco.ctid =
                                                                    coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                            from  usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini = '1'), --variaci�n 3 reg. v�lido
                                                                                                        (   select  max(ctid)
                                                                                                            from usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini = '1')), --variaci�n 3 reg. v�lido
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from  usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini = '1'),
                                                                                                            (   select  max(ctid)
                                                                                                                from usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini = '1'))), --no est� cortado
                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                    from  usinsug01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini = '1'),
                                                                                                                (   select  max(ctid)
                                                                                                                    from  usinsug01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini = '1')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from  usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini = '1'),
                                                                                                            (   select  max(ctid)
                                                                                                                from usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini = '1')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                            coalesce(coalesce(( select  max(ctid)
                                                                                                                from  usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate > cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini = '1'),
                                                                                                            (   select  max(ctid)
                                                                                                                from usinsug01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini = '1')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                coalesce((  select  max(ctid)
                                                                                                            from  usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                            and     statregt = '4' and addsuini = '1'),
                                                                                                        (   select  max(ctid)
                                                                                                            from usinsug01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover
                                                                                                            and     effecdate > cla.occurdat
                                                                                                            and     statregt = '4' and addsuini = '1'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                end *
                                            case    when    cla.branch = 66
                                                    then    (   select  max(exc.exchange)
                                                                from    usinsug01.exchange exc
                                                                where exc.usercomp = cla.usercomp
                                                                and     exc.company = cla.company
                                                                and     exc.currency = 99
                                                                and     exc.effecdate <= cla.occurdat
                                                                and     (exc.nulldate is null or exc.nulldate > cla.occurdat))
                                                    else    1 end, 0) as varchar ) as VMTCAPIT,
                                        '100'  as VTXQUOTA,
                                        '' as VMTINDEM,
                                        pol.leadpoli as DNUMAPO_CSG,
                                        cast ( coalesce(   coalesce((  select  max(cpl.currency)
                                                                from    usinsug01.curren_pol cpl
                                                                where	cpl.usercomp = cla.usercomp
                                                                and     cpl.company = cla.company
                                                                and     cpl.certype = pol.certype
                                                                and     cpl.branch = cla.branch
                                                                and     cpl.policy = cla.policy
                                                                and     cpl.certif = cla.certif),
                                                            (   select  max(cpl.currency)
                                                                from    usinsug01.curren_pol cpl
                                                                where	cpl.usercomp = cla.usercomp
                                                                and     cpl.company = cla.company
                                                                and     cpl.certype = pol.certype
                                                                and     cpl.branch = cla.branch
                                                                and     cpl.policy = cla.policy)),0) as varchar ) as KSCMOEDA
                                from 	(	select	
                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6'))
                                                                    and pol.certype = '2' 
                                                                    and pol.bussityp = '2'
                                                            then	cla.ctid
                                                            else	null 
                                                end as cla_id,
                                                case 	when 	pol.certype = '2' 
                                                                and pol.bussityp = '2'
                                                        then 	pol.ctid
                                                        else	null 
                                                end  as pol_id
                                            from    usinsug01.policy pol
                                            join    usinsug01.claim cla on cla.usercomp = pol.usercomp
                                                                        AND cla.company = pol.company
                                                                        AND cla.branch = pol.branch
                                                                        AND cla.policy = pol.policy
                                                                        and cla.branch  not in (60)
                                                        
                                            join        (   select  distinct clh.claim
                                                            from    (   select  cast(tcl.operation as varchar(2)) operation
                                                                        from    usinsug01.tab_cl_ope tcl
                                                                        where   (tcl.reserve = 1 or tcl.ajustes = 1 or pay_amount = 1)) tcl --reservas, ajustes o pagos
                                                            join    usinsug01.claim_his clh  on   coalesce (clh.claim,0) > 0
                                                                                            and     trim(clh.oper_type) = tcl.operation
                                                                                            and     clh.operdate >= '12/31/2015'
                                                        limit 10                            
                                                        ) clh
                                            on	clh.claim = cla.claim limit 10) cl0
                                join  usinsug01.claim cla on	cla.ctid = cl0.cla_id
                                join usinsug01.policy pol on pol.ctid = cl0.pol_id
                             ) AS TMP   
                             '''

    DF_LPG_NEGOCIO2_INSUNIX = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",LPG_NEGOCIO2_INSUNIX).load()
  
    LPV_NEGOCIO1_INSUNIX = '''
                            (
                                select       'D' as INDDETREC,
                                                'SBCOSSEG' as TABLAIFRS17,
                                                '' /*cla.claim || par.sep || par.cia || par.sep || coi.companyc*/ as PK,
                                                '' as DTPREG,
                                                '' as TIOCPROC,
                                                COALESCE(CAST(coi.EFFECDATE as VARCHAR) , '') as TIOCFRM,
                                                '' as TIOCTO,
                                                'PIV' as  KGIORIGM,
                                                'LPV'  as DCOMPA,
                                                '' as DMARCA,
                                                cast (cla.claim as varchar) as KSBSIN,
                                                cast (coi.companyc as varchar ) as  DCODCSG,
                                                '' as DNUMSEQ,
                                                '' as TDPLANO,
                                                cast ( coalesce ( case    when    coalesce((  select  distinct pro.brancht
                                                                            from    usinsuv01.product pro
                                                                            where pro.usercomp = cla.usercomp
                                                                            and     pro.company = cla.company
                                                                            and     pro.branch = cla.branch
                                                                            and     pro.product = pol.product
                                                                            and     pro.effecdate <= cla.occurdat
                                                                            and     (pro.nulldate is null or pro.nulldate > cla.occurdat)),'0')
                                                                not in ('1')
                                                        then    (   select  sum(cov.capital)
                                                                    from    usinsuv01.cover cov
                                                                    join    usinsuv01.gen_cover gco
                                                                    on      cov.usercomp = cla.usercomp
                                                                    and     cov.company = cla.company
                                                                    and     cov.certype = pol.certype
                                                                    and     cov.branch = cla.branch
                                                                    and     cov.policy = cla.policy
                                                                    and     cov.certif = cla.certif
                                                                    and     cov.effecdate <= cla.occurdat
                                                                    and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                    and     gco.ctid =
                                                                            coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                                    from usinsuv01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                    and     modulec = cov.modulec and cover = cov.cover --variaci n 1 con modulec
                                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci n 2 vigencias
                                                                                                                    and     statregt <> '4' and addsuini = '1'), --variaci n 3 reg. v lido
                                                                                                                (   select  max(ctid)
                                                                                                                    from    usinsuv01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                    and     cover = cov.cover --variaci n 1 sin modulec
                                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci n 2 vigencias
                                                                                                                    and     statregt <> '4' and addsuini = '1')), --variaci n 3 reg. v lido
                                                                                                            coalesce((  select  max(ctid)
                                                                                                                        from    usinsuv01.gen_cover
                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                        and     statregt = '4' and addsuini = '1'),
                                                                                                                    (   select  max(ctid)
                                                                                                                        from    usinsuv01.gen_cover
                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                        and     cover = cov.cover
                                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                        and     statregt = '4' and addsuini = '1'))), --no est  cortado
                                                                                                        coalesce(coalesce(( select  max(ctid)
                                                                                                                            from    usinsuv01.gen_cover
                                                                                                                            where    usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                            and     statregt <> '4' and addsuini = '1'),
                                                                                                                        (   select  max(ctid)
                                                                                                                            from    usinsuv01.gen_cover
                                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                            and     cover = cov.cover
                                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                            and     statregt <> '4' and addsuini = '1')), -- no est  cortado, pero fue anulado antes del efecto del registro
                                                                                                            coalesce((  select  max(ctid)
                                                                                                                        from    usinsuv01.gen_cover
                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                        and     statregt = '4' and addsuini = '1'),
                                                                                                                    (   select  max(ctid)
                                                                                                                        from  usinsuv01.gen_cover
                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                        and     cover = cov.cover
                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                        and     statregt = '4' and addsuini = '1')))), --no est  cortado y aparte fue anulado antes del efecto del registro
                                                                                                    coalesce(coalesce(( select  max(ctid)
                                                                                                                        from  usinsuv01.gen_cover
                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                        and     effecdate > cla.occurdat
                                                                                                                        and     statregt <> '4' and addsuini = '1'),
                                                                                                                    (   select  max(ctid)
                                                                                                                        from    usinsuv01.gen_cover
                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                        and     cover = cov.cover
                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                        and     statregt <> '4' and addsuini = '1')), -- no est  cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                        coalesce((  select  max(ctid)
                                                                                                                    from  usinsuv01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt = '4' and addsuini = '1'),
                                                                                                                (   select  max(ctid)
                                                                                                                    from  usinsuv01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                    and     cover = cov.cover
                                                                                                                    and     effecdate > cla.occurdat
                                                                                                                    and     statregt = '4' and addsuini = '1'))))) --est  cortado y no al efecto de la tabla de datos particular adem s de estar cortado
                                                        else    (   select  sum(cov.capital)
                                                                    from    usinsuv01.cover cov
                                                                    join    usinsuv01.life_cover gco
                                                                    on      cov.usercomp = cla.usercomp
                                                                    and     cov.company = cla.company
                                                                    and     cov.certype = pol.certype
                                                                    and     cov.branch = cla.branch
                                                                    and     cov.policy = cla.policy
                                                                    and     cov.certif = cla.certif
                                                                    and     cov.effecdate <= cla.occurdat
                                                                    and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                    and     gco.ctid =
                                                                            coalesce(coalesce(coalesce((    select  max(ctid)
                                                                                                            from usinsuv01.life_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                            and     cover = cov.cover
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                            and     statregt <> '4'  and addcapii = '1'), --que no est  cortado
                                                                                                        (   select  max(ctid)
                                                                                                            from usinsuv01.life_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                            and     cover = cov.cover
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                            and     statregt = '4'  and addcapii = '1')),--est  cortado
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from usinsuv01.life_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt <> '4' and addcapii = '1'),--no est  cortado pero fue anulado antes del efecto del registro
                                                                                                            (   select  max(ctid)
                                                                                                                from usinsuv01.life_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addcapii = '1'))), --est  cortado y aparte fue anulado antes del efecto del registro
                                                                                                coalesce((  select  max(ctid)
                                                                                                            from usinsuv01.life_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                            and     cover = cov.cover
                                                                                                            and     effecdate > cla.occurdat
                                                                                                            and     statregt <> '4' and addcapii = '1'),
                                                                                                    (   select  max(ctid)
                                                                                                        from usinsuv01.life_cover
                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency -- ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate > cla.occurdat --est  cortado pero no al efecto de la tabla de datos particular
                                                                                                        and     statregt = '4' and addcapii = '1')))) end , 0)  as varchar) VMTCAPIT, --agregar c lculo
                                                coi.share VTXQUOTA,
                                                '' VMTINDEM, --NOAPP
                                                cast(cla.policy as varchar) DNUMAPO_CSG,
                                                cast (coalesce(   coalesce((  select  max(cpl.currency)
                                                                        from    usinsuv01.curren_pol cpl
                                                                        where   cpl.usercomp = cla.usercomp
                                                                        and     cpl.company = cla.company
                                                                        and     cpl.certype = pol.certype
                                                                        and     cpl.branch = cla.branch
                                                                        and     cpl.policy = cla.policy
                                                                        and     cpl.certif = cla.certif),
                                                                    (   select  max(cpl.currency)
                                                                        from    usinsuv01.curren_pol cpl
                                                                        where   cpl.usercomp = cla.usercomp
                                                                        and     cpl.company = cla.company
                                                                        and     cpl.certype = pol.certype
                                                                        and     cpl.branch = cla.branch
                                                                        and     cpl.policy = cla.policy)),0) as varchar) KSCMOEDA
                                    from    (   select  case    when    cla.staclaim <> '6' and pol.certype = '2' and pol.bussityp = '1'
                                                                then    cla.ctid
                                                                else    null end cla_id,
                                                        case    when    pol.certype = '2' and pol.bussityp = '1'
                                                                then    pol.ctid
                                                                else    null end pol_id
                                                from    usinsuv01.policy pol
                                                join    usinsuv01.claim cla 
                                                        on  cla.usercomp = pol.usercomp
                                                        and cla.company = pol.company
                                                        and cla.branch = pol.branch
                                                        and cla.policy = pol.policy 
                                                join    (   select  distinct clh.claim
                                                            from    (   select  cast(tcl.operation as varchar(2)) operation
                                                                        from    usinsug01.tab_cl_ope tcl
                                                                        where   (tcl.reserve = 1 or tcl.ajustes = 1 or pay_amount = 1)
                                                                    ) tcl --reservas, ajustes o pagos
                                                            join usinsuv01.claim_his clh
                                                                on  coalesce (clh.claim,0) > 0
                                                                and trim(clh.oper_type) = tcl.operation
                                                                and clh.operdate >= '12/31/2015'
                                                        ) clh 
                                                        on      clh.claim = cla.claim
                                            ) cl0
                                    join    usinsuv01.claim cla 
                                            on cla.ctid = cl0.cla_id
                                    join    usinsuv01.policy pol 
                                            on pol.ctid = cl0.pol_id
                                    join    usinsuv01.coinsuran coi     
                                            on      /*coi.usercomp = cla.usercomp
                                            and     coi.company = cla.company
                                            and     coi.certype = pol.certype
                                            and */    coi.branch = cla.branch
                                            and     coi.policy = cla.policy
                                            and     coi.effecdate <= cla.occurdat
                                            and     (coi.nulldate is null or coi.nulldate > cla.occurdat)       
                                    /*1.769 s dev comentando los indices coinsuran hasta certype*/    
                            ) AS TMP    
                            '''    

    DF_LPV_NEGOCIO1_INSUNIX = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",LPV_NEGOCIO1_INSUNIX).load()
  
    LPV_NEGOCIO2_INSUNIX = '''
                            (
                                select	'D' as INDDETREC,
                                        'SBCOSSEG' as TABLAIFRS17,
                                        '' /*cla.claim || par.sep || par.cia || par.sep || 1*/ as PK,
                                        '' as DTPREG,
                                        '' as TIOCPROC,
                                        coalesce (CASE WHEN cla.certif = 0 
                                            THEN cast (pol.effecdate as varchar)
                                        ELSE 
                                            (SELECT cast (c.effecdate as varchar)
                                            FROM usinsuv01.certificat c 
                                            WHERE c.usercomp = cla.usercomp
                                            AND c.company = cla.company
                                            AND c.certype = pol.certype
                                            AND c.branch = cla.branch
                                            AND c.policy = cla.policy
                                            AND c.certif = cla.certif
                                            )
                                        end,'')
                                        as TIOCFRM,
                                        '' as TIOCTO,
                                        'PIV' as KGIORIGM,
                                        'LPV' DCOMPA,
                                        '' as DMARCA,
                                        cast (cla.claim as varchar) as KSBSIN,
                                        '1' as DCODCSG,
                                        '' as DNUMSEQ,
                                        '' as TDPLANO,
                                        cast ( coalesce ( case    when    coalesce((  select  distinct pro.brancht
                                                                    from    usinsuv01.product pro
                                                                    where   pro.usercomp = cla.usercomp
                                                                    and     pro.company = cla.company
                                                                    and     pro.branch = cla.branch
                                                                    and     pro.product = pol.product
                                                                    and     pro.effecdate <= cla.occurdat
                                                                    and     (pro.nulldate is null or pro.nulldate > cla.occurdat)),'0')
                                                        not in ('1','5')
                                                then    (   select  sum(cov.capital)
                                                            from    usinsuv01.cover cov
                                                            join    usinsuv01.gen_cover gco
                                                            on   cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = pol.certype
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                            and     gco.ctid =
                                                                    coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                            from    usinsuv01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini = '1'), --variaci�n 3 reg. v�lido
                                                                                                        (   select  max(ctid)
                                                                                                            from    usinsuv01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                            and     statregt <> '4' and addsuini = '1')), --variaci�n 3 reg. v�lido
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from 	usinsuv01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini = '1'),
                                                                                                            (   select  max(ctid)
                                                                                                                from 	usinsuv01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                and     statregt = '4' and addsuini = '1'))), --no est� cortado
                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                    from 	usinsuv01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini = '1'),
                                                                                                                (   select  max(ctid)
                                                                                                                    from    usinsuv01.gen_cover
                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                    and     cover = cov.cover
                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                    and     statregt <> '4' and addsuini = '1')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                    coalesce((  select  max(ctid)
                                                                                                                from 	usinsuv01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini = '1'),
                                                                                                            (   select  max(ctid)
                                                                                                                from 	usinsuv01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt = '4' and addsuini = '1')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                            coalesce(coalesce(( select  max(ctid)
                                                                                                                from 	usinsuv01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                                and     effecdate > cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini = '1'),
                                                                                                            (   select  max(ctid)
                                                                                                                from 	usinsuv01.gen_cover
                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                and     cover = cov.cover
                                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                and     statregt <> '4' and addsuini = '1')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                coalesce((  select  max(ctid)
                                                                                                            from  	usinsuv01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                            and     statregt = '4' and addsuini = '1'),
                                                                                                        (   select  max(ctid)
                                                                                                            from 	usinsuv01.gen_cover
                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                            and     cover = cov.cover
                                                                                                            and     effecdate > cla.occurdat
                                                                                                            and     statregt = '4' and addsuini = '1'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                else    (   select  sum(cov.capital)
                                                            from    usinsuv01.cover cov
                                                            join    usinsuv01.life_cover gco
                                                            on      cov.usercomp = cla.usercomp
                                                            and     cov.company = cla.company
                                                            and     cov.certype = pol.certype
                                                            and     cov.branch = cla.branch
                                                            and     cov.policy = cla.policy
                                                            and     cov.certif = cla.certif
                                                            and     cov.effecdate <= cla.occurdat
                                                            and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                            and     gco.ctid =
                                                                    coalesce(coalesce(coalesce((    select  max(ctid)
                                                                                                    from    usinsuv01.life_cover
                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                    and     cover = cov.cover
                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                    and     statregt <> '4'  and addcapii = '1'), --que no est� cortado
                                                                                                (   select  max(ctid)
                                                                                                    from    usinsuv01.life_cover
                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                    and     cover = cov.cover
                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                    and     statregt = '4'  and addcapii = '1')),--est� cortado
                                                                                            coalesce((  select  max(ctid)
                                                                                                        from    usinsuv01.life_cover
                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4' and addcapii = '1'),--no est� cortado pero fue anulado antes del efecto del registro
                                                                                                    (   select  max(ctid)
                                                                                                        from 	usinsuv01.life_cover
                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4' and addcapii = '1'))), --est� cortado y aparte fue anulado antes del efecto del registro
                                                                                        coalesce((  select  max(ctid)
                                                                                                    from 	usinsuv01.life_cover
                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                    and     cover = cov.cover
                                                                                                    and     effecdate > cla.occurdat
                                                                                                    and     statregt <> '4' and addcapii = '1'),
                                                                                            (   select  max(ctid)
                                                                                                from 	usinsuv01.life_cover
                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     cover = cov.cover
                                                                                                and     effecdate > cla.occurdat --est� cortado pero no al efecto de la tabla de datos particular
                                                                                                and     statregt = '4' and addcapii = '1')))) end, 0 ) as varchar ) as VMTCAPIT,
                                        '100' as VTXQUOTA,
                                        '' as  VMTINDEM,
                                        pol.leadpoli as DNUMAPO_CSG,
                                        cast (coalesce(   coalesce((  select  max(cpl.currency)
                                                                from    usinsuv01.curren_pol cpl
                                                                where	cpl.usercomp = cla.usercomp
                                                                and     cpl.company = cla.company
                                                                and     cpl.certype = pol.certype
                                                                and     cpl.branch = cla.branch
                                                                and     cpl.policy = cla.policy
                                                                and     cpl.certif = cla.certif),
                                                            (   select  max(cpl.currency)
                                                                from    usinsuv01.curren_pol cpl
                                                                where	cpl.usercomp = cla.usercomp
                                                                and     cpl.company = cla.company
                                                                and     cpl.certype = pol.certype
                                                                and     cpl.branch = cla.branch
                                                                and     cpl.policy = cla.policy)),0) as varchar ) KSCMOEDA
                                from 	(	select	case 	when 	cla.staclaim <> '6' and pol.certype = '2' and pol.bussityp = '2'
                                                            then	cla.ctid
                                                            else	null end cla_id,
                                                    case 	when 	pol.certype = '2' and pol.bussityp = '2'
                                                            then 	pol.ctid
                                                            else	null end pol_id
                                            from    usinsuv01.policy pol
                                            join    usinsuv01.claim cla on cla.usercomp = pol.usercomp
                                                                        and     cla.company = pol.company
                                                                        and     cla.branch = pol.branch
                                                                        and     cla.policy = pol.policy
                                            join       (select  distinct clh.claim
                                                        from    (   select  cast(tcl.operation as varchar(2)) operation
                                                                    from    usinsug01.tab_cl_ope tcl
                                                                    where   (tcl.reserve = 1 or tcl.ajustes = 1 or pay_amount = 1)) tcl, --reservas, ajustes o pagos
                                                                usinsuv01.claim_his  clh
                                                        where   coalesce (clh.claim,0) > 0
                                                        and     trim(clh.oper_type) = tcl.operation
                                                        and     clh.operdate >= '12/31/2015') clh
                                            ON	clh.claim = cla.claim) cl0
                                join usinsuv01.claim cla on cla.ctid = cl0.cla_id
                                join usinsuv01.policy pol on pol.ctid = cl0.pol_id
                            ) AS TMP
                            '''

    DF_LPV_NEGOCIO2_INSUNIX = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",LPV_NEGOCIO2_INSUNIX).load()
                                

    L_DF_SBCOSSEG_INSUNIX =  DF_LPG_NEGOCIO1_INSUNIX.union(DF_LPG_NEGOCIO2_INSUNIX).union(DF_LPV_NEGOCIO1_INSUNIX).union(DF_LPV_NEGOCIO2_INSUNIX)

  
    LPG_NEGOCIO1_VTIME = '''
                           ( 
                             select	'D' as INDDETREC,
                                    'SBCOSSEG' as TABLAIFRS17,
                                    ''/*cla."NCLAIM" || par.sep || par.cia || par.sep || coi."NCOMPANY"*/ as  PK,
                                    '' as DTPREG,
                                    '' as TIOCPROC,
                                    CAST(coi."DEFFECDATE" AS DATE) as TIOCFRM,
                                    '' as TIOCTO,
                                    'PVG' as KGIORIGM,
                                    'LPG'  as  DCOMPA,
                                    '' as DMARCA,
                                    cla."NCLAIM" as KSBSIN,
                                    cast (coi."NCOMPANY" as varchar) as DCODCSG,
                                    '' as DNUMSEQ,
                                    ''  as TDPLANO,
                                    cast (coalesce ( case	when	cla."NBRANCH" = 21 
                                            then	(	select	sum(cov."NCAPITAL")
                                                        from    usvtimg01."COVER" cov
                                                        join    usvtimg01."LIFE_COVER" gen
                                                        on   cov."SCERTYPE"  = pol."SCERTYPE" 
                                                        and     cov."NBRANCH" = cla."NBRANCH"
                                                        and     cov."NPRODUCT" = cla."NPRODUCT"
                                                        and     cov."NPOLICY" = cla."NPOLICY"
                                                        and     cov."NCERTIF" = cla."NCERTIF"
                                                        and     cov."DEFFECDATE" <= "DOCCURDAT"
                                                        and     (cov."DNULLDATE" is null or cov."DNULLDATE" > "DOCCURDAT")
                                                        and     gen."NCOVER" = cov."NCOVER"
                                                        and     gen."NPRODUCT" = cov."NPRODUCT"
                                                        and     gen."NMODULEC" = cov."NMODULEC"
                                                        and     gen."NBRANCH" = cov."NBRANCH"
                                                        and     gen."DEFFECDATE" <= cla."DOCCURDAT"
                                                        and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla."DOCCURDAT")
                                                        and     gen."SSTATREGT" <> '4'
                                                        and     gen."SADDSUINI" in ('1','3')) --NO HAY '3' EN VTIME
                                            else 	(	select	sum(cov."NCAPITAL")
                                                        from    usvtimg01."COVER" cov
                                                        join    usvtimg01."GEN_COVER" gen
                                                        on      cov."SCERTYPE"  = pol."SCERTYPE" 
                                                        and     cov."NBRANCH" = cla."NBRANCH"
                                                        and     cov."NPRODUCT" = cla."NPRODUCT"
                                                        and     cov."NPOLICY" = cla."NPOLICY"
                                                        and     cov."NCERTIF" = cla."NCERTIF"
                                                        and     cov."DEFFECDATE" <= "DOCCURDAT"
                                                        and     (cov."DNULLDATE" is null or cov."DNULLDATE" > "DOCCURDAT")
                                                        and     gen."NCOVER" = cov."NCOVER"
                                                        and     gen."NPRODUCT" = cov."NPRODUCT"
                                                        and     gen."NMODULEC" = cov."NMODULEC"
                                                        and     gen."NBRANCH" = cov."NBRANCH"
                                                        and     gen."DEFFECDATE" <= cla."DOCCURDAT"
                                                        and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla."DOCCURDAT")
                                                        and     gen."SSTATREGT" <> '4'
                                                        and     gen."SADDSUINI" in ('1','3')) --NO HAY '3' EN VTIME
                                            end, 0) as varchar) as VMTCAPIT,
                                    cast (coi."NSHARE" as varchar ) as VTXQUOTA,
                                    '' as VMTINDEM, --NOAPP
                                    cast(cla."NPOLICY" as varchar) DNUMAPO_CSG,
                                cast(coalesce(   coalesce((  select  max(cpl."NCURRENCY")
                                                            from    usvtimg01."CURREN_POL" cpl
                                                            where   cpl."SCERTYPE" = cla."SCERTYPE"
                                                            and     cpl."NBRANCH" = cla."NBRANCH"
                                                            and     cpl."NPRODUCT" = pol."NPRODUCT"
                                                            and     cpl."NPOLICY" = cla."NPOLICY"
                                                            and     cpl."NCERTIF" = cla."NCERTIF"),
                                                        (   select  max(cpl."NCURRENCY")
                                                            from    usvtimg01."CURREN_POL" cpl
                                                            where   cpl."SCERTYPE" = cla."SCERTYPE"
                                                            and     cpl."NBRANCH" = cla."NBRANCH"
                                                            and     cpl."NPRODUCT" = pol."NPRODUCT"
                                                            and     cpl."NPOLICY" = cla."NPOLICY")),0) as varchar) as KSCMOEDA
                            from 	(	select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                    then    cla.ctid
                                                                    else    null end CLA_ID,
                                                case 	when 	pol."SCERTYPE" = '2' and pol."SBUSSITYP" = '1'
                                                        then 	pol.ctid
                                                        else	null end POL_ID
                                        from   usvtimg01."POLICY" pol
                                        join   usvtimg01."CLAIM" cla on cla."SCERTYPE" = pol."SCERTYPE"
                                                                    and     cla."NPOLICY" = pol."NPOLICY"
                                                                    and     cla."NBRANCH" = pol."NBRANCH"
                                        join        (   select  distinct clh."NCLAIM"
                                                    from    (   select  cast("SVALUE" as INT4) "SVALUE"
                                                                from    usvtimg01."CONDITION_SERV" cs
                                                                where   "NCONDITION" in (71,72,73)) csv --operaciones de reserva, ajustes o pagos
                                                    join        usvtimg01."CLAIM_HIS" clh
                                                    on   coalesce (clh."NCLAIM",0) > 0
                                                    and     clh."NOPER_TYPE" = csv."SVALUE"
                                                    and     cast (clh."DOPERDATE" as date ) >= '12/31/2015') clh
                                        
                                        on    clh."NCLAIM" = cla."NCLAIM") cl0
                            join 	usvtimg01."CLAIM" cla on cla.ctid = cl0.cla_id
                            join 	usvtimg01."POLICY" pol on pol.ctid = cl0.pol_id
                            join 	usvtimg01."COINSURAN" coi 	on coi."SCERTYPE" = cla."SCERTYPE"
                                                                and     coi."NBRANCH" = cla."NBRANCH"
                                                                and     coi."NPRODUCT" = pol."NPRODUCT"
                                                                and     coi."NPOLICY" = cla."NPOLICY"
                                                                and 	coi."NCOMPANY" is not null
                                                                and     coi."DEFFECDATE" <= cla."DOCCURDAT"
                                                                and     (coi."DNULLDATE" is null or coi."DNULLDATE" > cla."DOCCURDAT")
                           ) AS TMP
                           '''
    DF_LPG_NEGOCIO1_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",LPG_NEGOCIO1_VTIME).load()
  
    LPG_NEGOCIO2_VTIME =  '''
                           ( 
                                select  'D' as INDDETREC,
                                        'SBCOSSEG' as TABLAIFRS17,
                                        ''/* cla."NCLAIM" || par.sep || par.cia || par.sep || 1*/ as PK,
                                        '' as DTPREG,
                                        '' as TIOCPROC,
                                        CASE WHEN cla."NCERTIF" = 0 
                                                THEN CAST(POL."DSTARTDATE"AS DATE)
                                            ELSE 
                                                (SELECT CAST(c."DCOMPDATE" AS DATE)
                                                FROM  usvtimg01."CERTIFICAT" c 
                                                WHERE c."SCERTYPE" = cla."SCERTYPE"
                                                AND c."NBRANCH" = cla."NBRANCH"
                                                AND c."NPRODUCT" = cla."NPRODUCT"
                                                AND C."NPOLICY" = POL."NPOLICY"
                                                AND c."NCERTIF" = cla."NCERTIF"
                                                )
                                            END
                                            as TIOCFRM,
                                        '' as TIOCTO,
                                        'PVG' as KGIORIGM,
                                        'LPG' as  DCOMPA,
                                        '' as DMARCA,
                                        cla."NCLAIM"  as KSBSIN,
                                        '1' as  DCODCSG,
                                        '' as DNUMSEQ,
                                        '' as TDPLANO,
                                        cast (coalesce ( case   when    cla."NBRANCH" = 21 
                                                then    (   select  sum(cov."NCAPITAL")
                                                            from    usvtimg01."COVER" cov
                                                            join    usvtimg01."LIFE_COVER" gen
                                                            on      cov."SCERTYPE"  = pol."SCERTYPE" 
                                                            and     cov."NBRANCH" = cla."NBRANCH"
                                                            and     cov."NPRODUCT" = cla."NPRODUCT"
                                                            and     cov."NPOLICY" = cla."NPOLICY"
                                                            and     cov."NCERTIF" = cla."NCERTIF"
                                                            and     cov."DEFFECDATE" <= "DOCCURDAT"
                                                            and     (cov."DNULLDATE" is null or cov."DNULLDATE" > "DOCCURDAT")
                                                            and     gen."NCOVER" = cov."NCOVER"
                                                            and     gen."NPRODUCT" = cov."NPRODUCT"
                                                            and     gen."NMODULEC" = cov."NMODULEC"
                                                            and     gen."NBRANCH" = cov."NBRANCH"
                                                            and     gen."DEFFECDATE" <= cla."DOCCURDAT"
                                                            and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla."DOCCURDAT")
                                                            and     gen."SSTATREGT" <> '4'
                                                            and     gen."SADDSUINI" in ('1','3')) --NO HAY '3' EN VTIME
                                                else    (   select  sum(cov."NCAPITAL")
                                                            from    usvtimg01."COVER" cov
                                                            join    usvtimg01."GEN_COVER" gen
                                                            on   cov."SCERTYPE"  = pol."SCERTYPE" 
                                                            and     cov."NBRANCH" = cla."NBRANCH"
                                                            and     cov."NPRODUCT" = cla."NPRODUCT"
                                                            and     cov."NPOLICY" = cla."NPOLICY"
                                                            and     cov."NCERTIF" = cla."NCERTIF"
                                                            and     cov."DEFFECDATE" <= "DOCCURDAT"
                                                            and     (cov."DNULLDATE" is null or cov."DNULLDATE" > "DOCCURDAT")
                                                            and     gen."NCOVER" = cov."NCOVER"
                                                            and     gen."NPRODUCT" = cov."NPRODUCT"
                                                            and     gen."NMODULEC" = cov."NMODULEC"
                                                            and     gen."NBRANCH" = cov."NBRANCH"
                                                            and     gen."DEFFECDATE" <= cla."DOCCURDAT"
                                                            and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla."DOCCURDAT")
                                                            and     gen."SSTATREGT" <> '4'
                                                            and     gen."SADDSUINI" in ('1','3')) --NO HAY '3' EN VTIME
                                                end , 0)as varchar) as VMTCAPIT,
                                        '100'as VTXQUOTA,
                                        '' as  VMTINDEM,
                                        pol."SLEADPOLI" as DNUMAPO_CSG,
                                        cast (coalesce(   coalesce((  select  max(cpl."NCURRENCY")
                                                                from    usvtimg01."CURREN_POL" cpl
                                                                where   cpl."SCERTYPE" = cla."SCERTYPE"
                                                                and     cpl."NBRANCH" = cla."NBRANCH"
                                                                and     cpl."NPRODUCT" = pol."NPRODUCT"
                                                                and     cpl."NPOLICY" = cla."NPOLICY"
                                                                and     cpl."NCERTIF" = cla."NCERTIF"),
                                                            (   select  max(cpl."NCURRENCY")
                                                                from    usvtimg01."CURREN_POL" cpl
                                                                where   cpl."SCERTYPE" = cla."SCERTYPE"
                                                                and     cpl."NBRANCH" = cla."NBRANCH"
                                                                and     cpl."NPRODUCT" = pol."NPRODUCT"
                                                                and     cpl."NPOLICY" = cla."NPOLICY")),0)as  varchar)  as KSCMOEDA
                                from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                        then    cla.ctid
                                                                        else    null end CLA_ID,
                                                    case    when    pol."SCERTYPE" = '2' and pol."SBUSSITYP" = '2'
                                                            then    pol.ctid
                                                            else    null end POL_ID
                                            from    usvtimg01."POLICY" pol
                                            join    usvtimg01."CLAIM" cla   on  cla."SCERTYPE" = pol."SCERTYPE"
                                                                            and     cla."NPOLICY" = pol."NPOLICY"
                                                                            and     cla."NBRANCH" = pol."NBRANCH"
                                            join    (   select  distinct clh."NCLAIM"
                                                        from    (   select  cast("SVALUE" as INT4) "SVALUE"
                                                                    from    usvtimg01."CONDITION_SERV" cs
                                                                    where   "NCONDITION" in (71,72,73)) csv --operaciones de reserva, ajustes o pagos
                                                        join    usvtimg01."CLAIM_HIS" clh
                                                        on   coalesce (clh."NCLAIM",0) > 0
                                                        and     clh."NOPER_TYPE" = csv."SVALUE"
                                                        and     cast (clh."DOPERDATE" as date) >= '12/31/2018') clh
                                            on  clh."NCLAIM" = cla."NCLAIM") cl0
                                join  usvtimg01."CLAIM" cla on cla.ctid = cl0.cla_id
                                join  usvtimg01."POLICY" pol on pol.ctid = cl0.pol_id
                           ) AS TMP
                           '''

    DF_LPG_NEGOCIO2_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",LPG_NEGOCIO2_VTIME).load()
  
    LPV_NEGOCIO1_VTIME = '''
                           (
                                select	'D' as INDDETREC,
                                        'SBCOSSEG' as TABLAIFRS17,
                                        '' /*cla."NCLAIM" || par.sep || par.cia || par.sep || coi."NCOMPANY"*/ as PK,
                                        '' as DTPREG,
                                        '' as TIOCPROC,
                                        CAST(coi."DEFFECDATE" AS DATE) as TIOCFRM,
                                        '' as TIOCTO,
                                        'PVV' as KGIORIGM,
                                        'LPV'  "DCOMPA",
                                        '' as DMARCA,
                                        cla."NCLAIM"  as KSBSIN,
                                        cast (coi."NCOMPANY" as varchar) as DCODCSG,
                                        ''  as DNUMSEQ,
                                        '' as TDPLANO,
                                        cast ( coalesce ((	select	sum(cov."NCAPITAL")
                                            from    usvtimv01."COVER" cov
                                            join    usvtimv01."LIFE_COVER" gen
                                            on      cov."SCERTYPE"  = pol."SCERTYPE" 
                                            and     cov."NBRANCH" = cla."NBRANCH"
                                            and     cov."NPRODUCT" = cla."NPRODUCT"
                                            and     cov."NPOLICY" = cla."NPOLICY"
                                            and     cov."NCERTIF" = cla."NCERTIF"
                                            and     cov."DEFFECDATE" <= "DOCCURDAT"
                                            and     (cov."DNULLDATE" is null or cov."DNULLDATE" > "DOCCURDAT")
                                            and     gen."NCOVER" = cov."NCOVER"
                                            and     gen."NPRODUCT" = cov."NPRODUCT"
                                            and     gen."NMODULEC" = cov."NMODULEC"
                                            and     gen."NBRANCH" = cov."NBRANCH"
                                            and     gen."DEFFECDATE" <= cla."DOCCURDAT"
                                            and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla."DOCCURDAT")
                                            and     gen."SSTATREGT" <> '4'
                                            and     gen."SADDSUINI" in ('1','3')),0) as varchar) as VMTCAPIT,--NO HAY '3' EN VTIME
                                        cast (coi."NSHARE" as varchar)  as VTXQUOTA,
                                        '' as  VMTINDEM, --NOAPP
                                        cast(cla."NPOLICY" as varchar)  as DNUMAPO_CSG,
                                        cast (coalesce(   coalesce((  select  max(cpl."NCURRENCY")
                                                                from    usvtimv01."CURREN_POL" cpl
                                                                where   cpl."SCERTYPE" = cla."SCERTYPE"
                                                                and     cpl."NBRANCH" = cla."NBRANCH"
                                                                and     cpl."NPRODUCT" = pol."NPRODUCT"
                                                                and     cpl."NPOLICY" = cla."NPOLICY"
                                                                and     cpl."NCERTIF" = cla."NCERTIF"),
                                                            (   select  max(cpl."NCURRENCY")
                                                                from    usvtimv01."CURREN_POL" cpl
                                                                where   cpl."SCERTYPE" = cla."SCERTYPE"
                                                                and     cpl."NBRANCH" = cla."NBRANCH"
                                                                and     cpl."NPRODUCT" = pol."NPRODUCT"
                                                                and     cpl."NPOLICY" = cla."NPOLICY")),0) as varchar ) as  KSCMOEDA
                                from 	(	select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                        then    cla.ctid
                                                                        else    null end CLA_ID,
                                                    case 	when 	pol."SCERTYPE" = '2' and pol."SBUSSITYP" = '1'
                                                            then 	pol.ctid
                                                            else	null end POL_ID
                                            from    usvtimv01."POLICY" pol
                                            join    usvtimv01."CLAIM" cla 	on cla."SCERTYPE" = pol."SCERTYPE"
                                                                            and     cla."NPOLICY" = pol."NPOLICY"
                                                                            and     cla."NBRANCH" = pol."NBRANCH"
                                            join       (   select  distinct clh."NCLAIM"
                                                        from    (   select  cast("SVALUE" as INT4) "SVALUE"
                                                                    from    usvtimv01."CONDITION_SERV" cs
                                                                    where   "NCONDITION" in (71,72,73)) csv --operaciones de reserva, ajustes o pagos
                                                        join    usvtimv01."CLAIM_HIS" clh
                                                        on   coalesce (clh."NCLAIM",0) > 0
                                                        and     clh."NOPER_TYPE" = csv."SVALUE"
                                                        and     cast (clh."DOPERDATE" as date) >= '12/31/2010') clh
                                            on   clh."NCLAIM" = cla."NCLAIM") cl0
                                join 	usvtimv01."CLAIM" cla on cla.ctid = cl0.cla_id
                                join 	usvtimv01."POLICY" pol on pol.ctid = cl0.pol_id
                                join 	usvtimv01."COINSURAN" coi  	on coi."SCERTYPE" = cla."SCERTYPE"
                                                                    and     coi."NBRANCH" = cla."NBRANCH"
                                                                    and     coi."NPRODUCT" = pol."NPRODUCT"
                                                                    and     coi."NPOLICY" = cla."NPOLICY"
                                                                    and 	coi."NCOMPANY" is not null
                                                                    and     coi."DEFFECDATE" <= cla."DOCCURDAT"
                                                                    and     (coi."DNULLDATE" is null or coi."DNULLDATE" > cla."DOCCURDAT")
                           ) AS TMP
                           '''

    DF_LPV_NEGOCIO1_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",LPV_NEGOCIO1_VTIME).load()
   
    LPV_NEGOCIO2_VTIME = '''
                           (
                                select	'D' as INDDETREC,
                                        'SBCOSSEG' as TABLAIFRS17,
                                        '' /*cla."NCLAIM" || par.sep || par.cia || par.sep || 1*/ as PK,
                                        '' as DTPREG,
                                        '' as TIOCPROC,
                                        CASE WHEN cla."NCERTIF" = 0 
                                                THEN CAST(POL."DSTARTDATE"AS DATE)
                                            ELSE 
                                                (SELECT CAST(c."DCOMPDATE" AS DATE)
                                                FROM  usvtimg01."CERTIFICAT" c 
                                                WHERE c."SCERTYPE" = cla."SCERTYPE"
                                                AND c."NBRANCH" = cla."NBRANCH"
                                                AND c."NPRODUCT" = cla."NPRODUCT"
                                                AND C."NPOLICY" = POL."NPOLICY"
                                                AND c."NCERTIF" = cla."NCERTIF"
                                                )
                                            END
                                            as TIOCFRM,
                                        '' as TIOCTO,
                                        'PVV' as KGIORIGM,
                                        'LPV' as  DCOMPA,
                                        '' as DMARCA,
                                        cla."NCLAIM"  as KSBSIN,
                                        '1' as DCODCSG,
                                        ''  as DNUMSEQ,
                                        '' as TDPLANO,
                                        cast ( coalesce ( (	select	sum(cov."NCAPITAL")
                                            from    usvtimv01."COVER" cov
                                            join	usvtimv01."LIFE_COVER" gen
                                            on		cov."SCERTYPE"  = pol."SCERTYPE" 
                                            and     cov."NBRANCH" = cla."NBRANCH"
                                            and     cov."NPRODUCT" = cla."NPRODUCT"
                                            and     cov."NPOLICY" = cla."NPOLICY"
                                            and     cov."NCERTIF" = cla."NCERTIF"
                                            and     cov."DEFFECDATE" <= "DOCCURDAT"
                                            and     (cov."DNULLDATE" is null or cov."DNULLDATE" > "DOCCURDAT")
                                            and     gen."NCOVER" = cov."NCOVER"
                                            and     gen."NPRODUCT" = cov."NPRODUCT"
                                            and     gen."NMODULEC" = cov."NMODULEC"
                                            and     gen."NBRANCH" = cov."NBRANCH"
                                            and     gen."DEFFECDATE" <= cla."DOCCURDAT"
                                            and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla."DOCCURDAT")
                                            and     gen."SSTATREGT" <> '4'
                                            and     gen."SADDSUINI" in ('1','3')),0) as varchar) as VMTCAPIT,--NO HAY '3' EN VTIME
                                        '100' as VTXQUOTA,
                                        '' as VMTINDEM,
                                        pol."SLEADPOLI" as  DNUMAPO_CSG,
                                        cast ( coalesce(   coalesce((  select  max(cpl."NCURRENCY")
                                                                from    usvtimv01."CURREN_POL" cpl
                                                                where   cpl."SCERTYPE" = cla."SCERTYPE"
                                                                and     cpl."NBRANCH" = cla."NBRANCH"
                                                                and     cpl."NPRODUCT" = pol."NPRODUCT"
                                                                and     cpl."NPOLICY" = cla."NPOLICY"
                                                                and     cpl."NCERTIF" = cla."NCERTIF"),
                                                            (   select  max(cpl."NCURRENCY")
                                                                from    usvtimv01."CURREN_POL" cpl
                                                                where   cpl."SCERTYPE" = cla."SCERTYPE"
                                                                and     cpl."NBRANCH" = cla."NBRANCH"
                                                                and     cpl."NPRODUCT" = pol."NPRODUCT"
                                                                and     cpl."NPOLICY" = cla."NPOLICY")),0) as varchar) KSCMOEDA
                                from 	(	select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                        then    cla.ctid
                                                                        else    null end CLA_ID,
                                                    case 	when 	pol."SCERTYPE" = '2' and pol."SBUSSITYP" = '2'
                                                            then 	pol.ctid
                                                            else	null end POL_ID
                                            from    usvtimv01."POLICY" pol
                                            join 	usvtimv01."CLAIM" cla  on cla."SCERTYPE" = pol."SCERTYPE"
                                                                            and     cla."NPOLICY" = pol."NPOLICY"
                                                                            and     cla."NBRANCH" = pol."NBRANCH"
                                            join      (   select  distinct clh."NCLAIM"
                                                        from    (   select  cast("SVALUE" as INT4) "SVALUE"
                                                                    from    usvtimv01."CONDITION_SERV" cs
                                                                    where   "NCONDITION" in (71,72,73)) csv --operaciones de reserva, ajustes o pagos
                                                        join    usvtimv01."CLAIM_HIS" clh
                                                        on      coalesce (clh."NCLAIM",0) > 0
                                                        and     clh."NOPER_TYPE" = csv."SVALUE"
                                                        and    cast (clh."DOPERDATE" as date ) >= '12/31/2021') clh
                                            on     clh."NCLAIM" = cla."NCLAIM") cl0
                                join 	usvtimv01."CLAIM" cla on cla.ctid = cl0.cla_id
                                join 	usvtimv01."POLICY" pol on pol.ctid = cl0.pol_id
                           ) AS TMP
                           '''

    DF_LPV_NEGOCIO2_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",LPV_NEGOCIO2_VTIME).load()
                           
    L_DF_SBCOSSEG_VTIME =  DF_LPG_NEGOCIO1_VTIME.union(DF_LPG_NEGOCIO2_VTIME).union(DF_LPV_NEGOCIO1_VTIME).union(DF_LPV_NEGOCIO2_VTIME)

    L_DF_SBCOSSEG = L_DF_SBCOSSEG_INSUNIX.union(L_DF_SBCOSSEG_VTIME)

    return L_DF_SBCOSSEG                          