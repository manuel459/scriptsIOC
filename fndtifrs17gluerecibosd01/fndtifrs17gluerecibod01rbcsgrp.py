def get_data(glue_context, connection):

    L_RBCSGRP_INSUNIX_LPG_NEGOCIO1 = f'''
                                        (
                                        select	'D' as INDDETREC,
                                                        'RBCSGRP' as TABLAIFRS17, 
                                                                pre.receipt KRBRECPR,
                                                                '' DORDCSG, --excluido
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                '' TIOCFRM, --excluido
                                                                '' TIOCTO, --excluido
                                                                'PIG' KGIORIGM, --excluido
                                                                case	when	coi.companyc in (1,12) --se consdiera cedido a las otras compa��as de los esquemas de coaseguro
                                                                                then	1 --retenido
                                                                                else	2 ---cedido
                                                                                end DPLANO,
                                                                '' VTXCAPIT, --descartado
                                                                '' VTXPREM, --descartado
                                                                '' VTXCOMCB, --descartado
                                                                '' VTXCOMMD, --descartado
                                                                (	coalesce(dpr.capital,0) *
                                                                                case	when	pre.branch = 66
                                                                                                then	(	select	max(exc.exchange)
                                                                                                                        from 	usinsug01.exchange exc
                                                                                                                        where	exc.usercomp = pre.usercomp 
                                                                                                                        and 	exc.company = pre.company 
                                                                                                                        and 	exc.currency = 99
                                                                                                                        and 	exc.effecdate <= pre.effecdate
                                                                                                                        and 	(exc.nulldate is null or exc.nulldate > pre.effecdate))
                                                                                                else	1 end) * coi.share / 100 VMTCAPIT,
                                                                coalesce((	dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                                                        --se procede a calcular la distribuci�n por el certificado 0, incluyendo las individuales (nivel_1b)
                                                                                        --esto se debe a que no hay certificados para evaluar si ya se obtuvo o no el de la matriz o certificado 0
                                                                                        case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                                                                        (	select	1
                                                                                                                                from	usinsug01.detail_pre dp0
                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                and		dp0.certif = 0
                                                                                                                                and 	dp0.type_detai in ('1','3','4')
                                                                                                                                and		dp0.bill_item not in (4,5,9,97))
                                                                                                        then	coalesce((	select	sum(dp0.premium)
                                                                                                                                                from	usinsug01.detail_pre dp0
                                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                                and		dp0.certif = 0
                                                                                                                                                and		dp0.type_detai in ('1','3','4') 
                                                                                                                                                and		dp0.bill_item not in (4,5,9,97)),0)
                                                                                                                                        *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
                                                                                                        else	0 end),0) * coi.share / 100 VMTPREM,
                                                                '' VMTCSTFR, --descartado
                                                                '' VMTCOMCB, --descartado
                                                                '' VMTCOMMD, --descartado
                                                                coalesce(dpr.code,0) KRCTPCBT,
                                                                coalesce(coi.companyc,0) DCODCSG,
                                                                par.cia DCOMPA,
                                                                '' DMARCA, --excluido
                                                                '' DINDDESD, --excluido
                                                                case	when	coi.companyc in (1,12) --se consdiera cedido a las otras compa��as de los esquemas de coaseguro
                                                                                then	1 --retenido
                                                                                else	2 ---cedido
                                                                                end KRCTPQTP
                                                from	(	select	pre.ctid pre_id,
                                                                                        dpr.code,
                                                                                        SUM(coalesce(dpr.capital,0) * case when addsuini = '1' then 1 else 0 end) capital,
                                                                                        SUM(coalesce(dpr.premium,0)) premium
                                                                        from	usinsug01.premium pre
                                                                        join	usinsug01.detail_pre dpr
                                                                                        on		dpr.usercomp = pre.usercomp
                                                                                        and 	dpr.company = pre.company
                                                                                        and 	dpr.receipt = pre.receipt
                                                                                        and 	dpr.type_detai in ('1','3','4')
                                                                                        and 	dpr.bill_item not in (4,5,9,97)
                                                                        where 	pre.usercomp = 1
                                                                        and 	pre.company = 1
                                                                        and 	cast(pre.effecdate as date) <= '12/31/2020'
                                                                        and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                                                        and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                                                        and 	pre.statusva not in ('2','3')
                                                                        and		exists
                                                                                        (	select 	1
                                                                                                from	usinsug01.coinsuran coi
                                                                                                where	coi.usercomp = pre.usercomp
                                                                                                and		coi.company = pre.company
                                                                                                and 	coi.certype = pre.certype
                                                                                                and     coi.branch = pre.branch
                                                                                                and     coi.policy = pre.policy
                                                                                                and     coi.effecdate <= pre.effecdate
                                                                                                and     (coi.nulldate is null or coi.nulldate > pre.effecdate))
                                                                        group 	by 1,2 limit 10) dpr
                                                join	usinsug01.premium pre on pre.ctid = dpr.pre_id
                                                join 	(select 'LPG' cia, '-' sep) par on 1 = 1
                                                join	usinsug01.coinsuran coi
                                                                on 		coi.usercomp = pre.usercomp
                                                                and     coi.company = pre.company
                                                                and     coi.certype = pre.certype
                                                                and     coi.branch = pre.branch
                                                                and     coi.policy = pre.policy
                                                                and     coi.effecdate <= pre.effecdate
                                                                and     (coi.nulldate is null or coi.nulldate > pre.effecdate)
                                                --4m47s (todos los ramos, solo con esquemas de coaseguro)
                                                --3m 9s dev 1 limit 10 
                                        )AS TMP 
                                        '''

    DF_LPG_INSUNIX_NEGOCIO1 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRP_INSUNIX_LPG_NEGOCIO1).load()

    L_RBCSGRP_INSUNIX_LPG_NEGOCIO2 = f'''
                                        (
                                           select	'D' as INDDETREC,
                                                        'RBCSGRP' as TABLAIFRS17, 
                                                                pre.receipt KRBRECPR,
                                                                '' DORDCSG, --excluido
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                '' TIOCFRM, --excluido
                                                                '' TIOCTO, --excluido
                                                                'PIG' KGIORIGM, --excluido
                                                                1 DPLANO,--se consdiera cedido a las otras compa��as de los esquemas de coaseguro
                                                                '' VTXCAPIT, --descartado
                                                                '' VTXPREM, --descartado
                                                                '' VTXCOMCB, --descartado
                                                                '' VTXCOMMD, --descartado
                                                                (	coalesce(dpr.capital,0) *
                                                                                case	when	pre.branch = 66
                                                                                                then	(	select	max(exc.exchange)
                                                                                                                        from 	usinsug01.exchange exc
                                                                                                                        where	exc.usercomp = pre.usercomp 
                                                                                                                        and 	exc.company = pre.company 
                                                                                                                        and 	exc.currency = 99
                                                                                                                        and 	exc.effecdate <= pre.effecdate
                                                                                                                        and 	(exc.nulldate is null or exc.nulldate > pre.effecdate))
                                                                                                else	1 end) VMTCAPIT, --se considera el 100% retenido
                                                                coalesce((	dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                                                        --se procede a calcular la distribuci�n por el certificado 0, incluyendo las individuales (nivel_1b)
                                                                                        --esto se debe a que no hay certificados para evaluar si ya se obtuvo o no el de la matriz o certificado 0
                                                                                        case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                                                                        (	select	1
                                                                                                                                from	usinsug01.detail_pre dp0
                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                and		dp0.certif = 0
                                                                                                                                and 	dp0.type_detai in ('1','3','4')
                                                                                                                                and		dp0.bill_item not in (4,5,9,97))
                                                                                                        then	coalesce((	select	sum(dp0.premium)
                                                                                                                                                from	usinsug01.detail_pre dp0
                                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                                and		dp0.certif = 0
                                                                                                                                                and		dp0.type_detai in ('1','3','4') 
                                                                                                                                                and		dp0.bill_item not in (4,5,9,97)),0)
                                                                                                                                        *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
                                                                                                        else	0 end),0) VMTPREM, --se considera el 100% retenido
                                                                '' VMTCSTFR, --descartado
                                                                '' VMTCOMCB, --descartado
                                                                '' VMTCOMMD, --descartado
                                                                coalesce(dpr.code,0) KRCTPCBT,
                                                                1 DCODCSG,
                                                                par.cia DCOMPA,
                                                                '' DMARCA, --excluido
                                                                '' DINDDESD, --excluido
                                                                1 KRCTPQTP --se considera como retenido
                                                from	(	select	pre.ctid pre_id,
                                                                                        dpr.code,
                                                                                        SUM(coalesce(dpr.capital,0) * case when addsuini = '1' then 1 else 0 end) capital,
                                                                                        SUM(coalesce(dpr.premium,0)) premium
                                                                        from	usinsug01.premium pre
                                                                        join	usinsug01.detail_pre dpr
                                                                                        on		dpr.usercomp = pre.usercomp
                                                                                        and 	dpr.company = pre.company
                                                                                        and 	dpr.receipt = pre.receipt
                                                                                        and		dpr.type_detai in ('1','3','4')
                                                                                        and		dpr.bill_item not in (4,5,9,97)
                                                                        where 	pre.usercomp = 1
                                                                        and 	pre.company = 1
                                                                        and 	cast(pre.effecdate as date) <= '12/31/2020'
                                                                        and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                                                        and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                                                        and 	pre.statusva not in ('2','3')
                                                                        and		exists
                                                                                        (	select 	1
                                                                                                from	usinsug01.policy pol
                                                                                                where	pol.usercomp = pre.usercomp
                                                                                                and		pol.company = pre.company
                                                                                                and 	pol.certype = pre.certype
                                                                                                and     pol.branch = pre.branch
                                                                                                and     pol.policy = pre.policy
                                                                                                and		pol.bussityp = '2')
                                                                        group 	by 1,2 limit 10) dpr
                                                join	usinsug01.premium pre on pre.ctid = dpr.pre_id
                                                join 	(select 'LPG' cia, '-' sep) par on 1 = 1
                                                --4m04s (todos los ramos, solo con coaseguro recibido) * devolvi� solo 9 registros
                                                --3m 12 s limit 10 		             
                                        ) AS TMP
                                        '''                  
    DF_LPG_INSUNIX_NEGOCIO2 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRP_INSUNIX_LPG_NEGOCIO2).load()

    DF_LPG_INSUNIX =    DF_LPG_INSUNIX_NEGOCIO1.union(DF_LPG_INSUNIX_NEGOCIO2)  

    L_RBCSGRP_INSUNIX_LPV_NEGOCIO1 = f'''
                                        (
                                                select	'D' as INDDETREC,
                                                        'RBCSGRP' as TABLAIFRS17,
                                                                pre.receipt KRBRECPR,
                                                                '' DORDCSG, --excluido
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                '' TIOCFRM, --excluido
                                                                '' TIOCTO, --excluido
                                                                'PIV' KGIORIGM, --excluido
                                                                case	when	coi.companyc in (1,12) --se consdiera cedido a las otras compa��as de los esquemas de coaseguro
                                                                                then	1 --retenido
                                                                                else	2 ---cedido
                                                                                end DPLANO,
                                                                '' VTXCAPIT, --descartado
                                                                '' VTXPREM, --descartado
                                                                '' VTXCOMCB, --descartado
                                                                '' VTXCOMMD, --descartado
                                                                coalesce(dpr.capital,0) * coi.share / 100 VMTCAPIT,
                                                                coalesce((	dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                                                        --se procede a calcular la distribuci�n por el certificado 0, incluyendo las individuales (nivel_1b)
                                                                                        --esto se debe a que no hay certificados para evaluar si ya se obtuvo o no el de la matriz o certificado 0
                                                                                        case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                                                                        (	select	1
                                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                and		dp0.certif = 0
                                                                                                                                and 	dp0.type_detai in ('1','3','4')
                                                                                                                                and		dp0.bill_item not in (4,5,9,97))
                                                                                                        then	coalesce((	select	sum(dp0.premium)
                                                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                                and		dp0.certif = 0
                                                                                                                                                and		dp0.type_detai in ('1','3','4') 
                                                                                                                                                and		dp0.bill_item not in (4,5,9,97)),0)
                                                                                                                                        *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
                                                                                                        else	0 end),0) * coi.share / 100 VMTPREM,
                                                                '' VMTCSTFR, --descartado
                                                                '' VMTCOMCB, --descartado
                                                                '' VMTCOMMD, --descartado
                                                                coalesce(dpr.code,0) KRCTPCBT,
                                                                coalesce(coi.companyc,0) DCODCSG,
                                                                par.cia DCOMPA,
                                                                '' DMARCA, --excluido
                                                                '' DINDDESD, --excluido
                                                                case	when	coi.companyc in (1,12) --se consdiera cedido a las otras compa��as de los esquemas de coaseguro
                                                                                then	1 --retenido
                                                                                else	2 ---cedido
                                                                                end KRCTPQTP
                                                from 	(	select 	pre.pre_id,
                                                                                        dpr.code,
                                                                                        SUM(coalesce(dpr.capital,0) * case when addsuini = '1' then 1 else 0 end) capital,
                                                                                        SUM(coalesce(dpr.premium,0)) premium
                                                                        from	(	select	pre.ctid pre_id,
                                                                                                                pre.usercomp,
                                                                                                                pre.company,
                                                                                                                pre.receipt
                                                                                                from	usinsuv01.premium pre
                                                                                                where 	pre.usercomp = 1
                                                                                                and 	pre.company = 1
                                                                                                and 	cast(pre.effecdate as date) <= '12/31/2020'
                                                                                                and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                                                                                and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                                                                                and 	pre.statusva not in ('2','3')
                                                                                                and		exists
                                                                                                                (	select 	1
                                                                                                                        from	usinsuv01.coinsuran coi
                                                                                                                        where	coi.usercomp = pre.usercomp
                                                                                                                        and		coi.company = pre.company
                                                                                                                        and 	coi.certype = pre.certype
                                                                                                                        and     coi.branch = pre.branch
                                                                                                                        and     coi.policy = pre.policy)) pre
                                                                        join	usinsuv01.detail_pre dpr
                                                                                        on		dpr.usercomp = pre.usercomp
                                                                                        and 	dpr.company = pre.company
                                                                                        and 	dpr.receipt = pre.receipt
                                                                                        and		dpr.type_detai in ('1','3','4')
                                                                                        and		dpr.bill_item not in (4,5,9,97)
                                                                        group 	by 1,2 limit 10) dpr
                                                join	usinsuv01.premium pre on pre.ctid = dpr.pre_id
                                                join 	(select 'LPV' cia, '-' sep) par on 1 = 1
                                                join	usinsuv01.coinsuran coi
                                                                on 		coi.usercomp = pre.usercomp
                                                                and     coi.company = pre.company
                                                                and     coi.certype = pre.certype
                                                                and     coi.branch = pre.branch
                                                                and     coi.policy = pre.policy
                                                                and     coi.effecdate <= pre.effecdate
                                                                and     (coi.nulldate is null or coi.nulldate > pre.effecdate)
                                                -----53.601 s dev 1 limit 10		
                                        )AS TMP 
                                        '''

    DF_LPV_INSUNIX_NEGOCIO1 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRP_INSUNIX_LPV_NEGOCIO1).load()
     
    L_RBCSGRP_INSUNIX_LPV_NEGOCIO2 = f'''
                                        (
                                          select	'D' as INDDETREC,
                                                        'RBCSGRP' as TABLAIFRS17,
                                                                pre.receipt KRBRECPR,
                                                                '' DORDCSG, --excluido
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                '' TIOCFRM, --excluido
                                                                '' TIOCTO, --excluido
                                                                'PIV' KGIORIGM, --excluido
                                                                1 DPLANO,--se consdiera cedido a las otras compa��as de los esquemas de coaseguro
                                                                '' VTXCAPIT, --descartado
                                                                '' VTXPREM, --descartado
                                                                '' VTXCOMCB, --descartado
                                                                '' VTXCOMMD, --descartado
                                                                coalesce(dpr.capital,0) VMTCAPIT, --se considera el 100% retenido
                                                                coalesce((	dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                                                        --se procede a calcular la distribuci�n por el certificado 0, incluyendo las individuales (nivel_1b)
                                                                                        --esto se debe a que no hay certificados para evaluar si ya se obtuvo o no el de la matriz o certificado 0
                                                                                        case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                                                                        (	select	1
                                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                and		dp0.certif = 0
                                                                                                                                and 	dp0.type_detai in ('1','3','4')
                                                                                                                                and		dp0.bill_item not in (4,5,9,97))
                                                                                                        then	coalesce((	select	sum(dp0.premium)
                                                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                                and		dp0.certif = 0
                                                                                                                                                and		dp0.type_detai in ('1','3','4') 
                                                                                                                                                and		dp0.bill_item not in (4,5,9,97)),0)
                                                                                                                                        *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
                                                                                                        else	0 end),0) VMTPREM, --se considera el 100% retenido
                                                                '' VMTCSTFR, --descartado
                                                                '' VMTCOMCB, --descartado
                                                                '' VMTCOMMD, --descartado
                                                                coalesce(dpr.code,0) KRCTPCBT,
                                                                1 DCODCSG,
                                                                par.cia DCOMPA,
                                                                '' DMARCA, --excluido
                                                                '' DINDDESD, --excluido
                                                                1 KRCTPQTP --se considera como retenido
                                                from 	(	select	case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                                        then pre.ctid
                                                                                                        else null end pre_id,
                                                                                        dpr.code,
                                                                                        SUM(coalesce(dpr.capital,0) * case when addsuini = '1' then 1 else 0 end) capital,
                                                                                        SUM(case 	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                                                then coalesce(dpr.premium,0) else 0 end) premium
                                                                        from	(	select	certype,
                                                                                                                branch,
                                                                                                                case 	when certype = '2' 
                                                                                                                                then pol.policy
                                                                                                                                else null end as policy
                                                                                                from	usinsuv01.policy pol
                                                                                                where 	pol.bussityp = '2') pol
                                                                        join	usinsuv01.premium pre
                                                                                        on		pre.usercomp = 1
                                                                                        and 	pre.company = 1
                                                                                        and 	pre.certype = pol.certype 
                                                                                        and 	pre.branch = pol.branch
                                                                                        and 	pre.policy = pol.policy
                                                                                        and 	cast(pre.effecdate as date) <= '12/31/2020'
                                                                                        and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                                                                        and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                                                                        and 	pre.statusva not in ('2','3')
                                                                        join	usinsuv01.detail_pre dpr
                                                                                        on		dpr.usercomp = pre.usercomp
                                                                                        and 	dpr.company = pre.company
                                                                                        and 	dpr.receipt = pre.receipt
                                                                        group 	by 1,2 limit 10) dpr
                                                join	usinsuv01.premium pre on pre.ctid = dpr.pre_id
                                                join 	(select 'LPV' cia, '-' sep) par on 1 = 1
                                                --PRUEBAS EN DESARROLLO
                                                --1m23s (todos) * devolvi� 14 registros	
                                                --47.831 s dev 1 limit 10                      
                                        ) AS TMP
                                        '''

    DF_LPV_INSUNIX_NEGOCIO2 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRP_INSUNIX_LPV_NEGOCIO2).load()
     
    DF_LPV_INSUNIX = DF_LPV_INSUNIX_NEGOCIO1.union(DF_LPV_INSUNIX_NEGOCIO2)

    L_DF_RBCSGRP_INSUNIX  = DF_LPG_INSUNIX.union(DF_LPV_INSUNIX)            
    
    L_RBCSGRP_VTIME_LPG = f'''
                              ( 
                                  select	'D' as INDDETREC,
                                                        'RBCSGRP' as TABLAIFRS17,
                                                        pre."NRECEIPT" KRBRECPR,
                                                        '' DORDCSG, --excluido
                                                        '' DTPREG, --excluido
                                                        '' TIOCPROC, --excluido
                                                        '' TIOCFRM, --excluido
                                                        '' TIOCTO, --excluido
                                                        'PVG' KGIORIGM, --excluido
                                                        case	when	coi."NCOMPANY" in (1,12) --se consdiera cedido a las otras compañías de los esquemas de coaseguro
                                                                then	1 --retenido
                                                                else	2 ---cedido
                                                                end DPLANO,
                                                        '' VTXCAPIT, --descartado
                                                        '' VTXPREM, --descartado
                                                        '' VTXCOMCB, --descartado
                                                        '' VTXCOMMD, --descartado
                                                        dpr.ncapital * coi."NSHARE" / 100 VMTCAPIT,
                                                        dpr.npremium * coi."NSHARE" / 100 VMTPREM,
                                                        '' VMTCSTFR, --descartado
                                                        '' VMTCOMCB, --descartado
                                                        '' VMTCOMMD, --descartado
                                                        coalesce(dpr.ndet_code,0) KRCTPCBT,
                                                        coalesce(coi."NCOMPANY",0) DCODCSG,
                                                        par.cia DCOMPA,
                                                        '' DMARCA, --excluido
                                                        '' DINDDESD, --excluido
                                                        case	when	coi."NCOMPANY" in (1,12) --se consdiera cedido a las otras compañías de los esquemas de coaseguro
                                                                then	1 --retenido
                                                                else	2 ---cedido
                                                                end KRCTPQTP
                                                from	(	select 	pre.ctid pre_id,
                                                                        dpr."NDET_CODE" ndet_code,
                                                                        sum(coalesce(dpr."NCAPITAL",0)) ncapital,
                                                                        sum(coalesce(DPR."NPREMIUM",0)) npremium
                                                                FROM 	usvtimg01."PREMIUM" PRE
                                                                join 	usvtimg01."DETAIL_PRE" DPR
                                                                        on		DPR."NRECEIPT" = PRE."NRECEIPT" 
                                                                        and 	DPR."NDIGIT" = PRE."NDIGIT"
                                                                        and 	DPR."STYPE_DETAI" in ('1','2','4','6')
                                                                        and		dpr."NBILL_ITEM" not in (4,5,9,97)
                                                                WHERE 	PRE."NDIGIT" = 0
                                                                AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2020'
                                                                AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2020')
                                                                AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2020')
                                                                AND 	PRE."SSTATUSVA" NOT IN ('2','3')
                                                                and 	exists 
                                                                        (	select 	1
                                                                        from	usvtimg01."COINSURAN" coi
                                                                        where	coi."SCERTYPE" = pre."SCERTYPE"
                                                                        and     coi."NBRANCH" = pre."NBRANCH"
                                                                        and     coi."NPOLICY" = pre."NPOLICY"
                                                                        and     CAST(coi."DEFFECDATE" AS DATE) <= CAST(PRE."DEFFECDATE" AS DATE)
                                                                        and     (coi."DNULLDATE" is null or CAST(coi."DNULLDATE" AS DATE) > CAST(PRE."DEFFECDATE" AS DATE)))
                                                                group 	by 1,2) dpr
                                                join	usvtimg01."PREMIUM" pre on pre.ctid = dpr.pre_id
                                                join 	(select 'LPG' cia, '-' sep) par on 1 = 1
                                                join	usvtimg01."COINSURAN" coi
                                                        on		coi."SCERTYPE" = pre."SCERTYPE"
                                                        and     coi."NBRANCH" = pre."NBRANCH"
                                                        and     coi."NPOLICY" = pre."NPOLICY"
                                                        and     CAST(coi."DEFFECDATE" AS DATE) <= CAST(PRE."DEFFECDATE" AS DATE)
                                                        and     (coi."DNULLDATE" is null or CAST(coi."DNULLDATE" AS DATE) > CAST(PRE."DEFFECDATE" AS DATE))
                                                --7.922s prod (todos los ramos)
                                                --6.90s (todos los ramos) dev 1              
                              ) AS TMP 
                              '''

    DF_LPG_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRP_VTIME_LPG).load()
                
    L_RBCSGRP_VTIME_LPV = f'''
                              (
                                select	'D' as INDDETREC,
                                        'RBCSGRP' as TABLAIFRS17,
                                        pre."NRECEIPT" KRBRECPR,
                                        '' DORDCSG, --excluido
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        '' TIOCFRM, --excluido
                                        '' TIOCTO, --excluido
                                        'PVV' KGIORIGM, --excluido
                                        case	when	coi."NCOMPANY" in (1,12) --se consdiera cedido a las otras compañías de los esquemas de coaseguro
                                                then	1 --retenido
                                                else	2 ---cedido
                                                end DPLANO,
                                        '' VTXCAPIT, --descartado
                                        '' VTXPREM, --descartado
                                        '' VTXCOMCB, --descartado
                                        '' VTXCOMMD, --descartado
                                        dpr.ncapital * coi."NSHARE" / 100 VMTCAPIT,
                                        dpr.npremium * coi."NSHARE" / 100 VMTPREM,
                                        '' VMTCSTFR, --descartado
                                        '' VMTCOMCB, --descartado
                                        '' VMTCOMMD, --descartado
                                        coalesce(dpr.ndet_code,0) KRCTPCBT,
                                        coalesce(coi."NCOMPANY",0) DCODCSG,
                                        par.cia DCOMPA,
                                        '' DMARCA, --excluido
                                        '' DINDDESD, --excluido
                                        case	when	coi."NCOMPANY" in (1,12) --se consdiera cedido a las otras compañías de los esquemas de coaseguro
                                                then	1 --retenido
                                                else	2 ---cedido
                                                end KRCTPQTP
                                from	(	select 	pre.ctid pre_id,
                                                        dpr."NDET_CODE" ndet_code,
                                                        sum(coalesce(dpr."NCAPITAL",0)) ncapital,
                                                        sum(coalesce(DPR."NPREMIUM",0)) npremium
                                                FROM 	usvtimv01."PREMIUM" PRE
                                                join 	usvtimv01."DETAIL_PRE" DPR
                                                        on		DPR."NRECEIPT" = PRE."NRECEIPT"
                                                        and 	DPR."NDIGIT" = PRE."NDIGIT"
                                                        and 	DPR."STYPE_DETAI" in ('1','2','4','6')
                                                        and		dpr."NBILL_ITEM" not in (4,5,9,97)
                                                WHERE 	PRE."NDIGIT" = 0
                                                AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2019'
                                                AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2019')
                                                AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2019')
                                                AND 	PRE."SSTATUSVA" NOT IN ('2','3')
                                                and 	exists 
                                                        (	select 	1
                                                        from	usvtimv01."COINSURAN" coi
                                                        where	coi."SCERTYPE" = pre."SCERTYPE"
                                                        and     coi."NBRANCH" = pre."NBRANCH"
                                                        and     coi."NPOLICY" = pre."NPOLICY"
                                                        and     CAST(coi."DEFFECDATE" AS DATE) <= CAST(PRE."DEFFECDATE" AS DATE)
                                                        and     (coi."DNULLDATE" is null or CAST(coi."DNULLDATE" AS DATE) > CAST(PRE."DEFFECDATE" AS DATE)))
                                                group 	by 1,2) dpr
                                join	usvtimv01."PREMIUM" pre on pre.ctid = dpr.pre_id
                                join 	(select 'LPV' cia, '-' sep) par on 1 = 1
                                join	usvtimv01."COINSURAN" coi
                                        on		coi."SCERTYPE" = pre."SCERTYPE"
                                        and     coi."NBRANCH" = pre."NBRANCH"
                                        and     coi."NPOLICY" = pre."NPOLICY"
                                        and     CAST(coi."DEFFECDATE" AS DATE) <= CAST(PRE."DEFFECDATE" AS DATE)
                                        and     (coi."DNULLDATE" is null or CAST(coi."DNULLDATE" AS DATE) > CAST(PRE."DEFFECDATE" AS DATE))
                                --111ms (todos los ramos) no devolvió registros             
                              ) AS TMP 
                              '''

    DF_LPV_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRP_VTIME_LPV).load()

    L_DF_RBCSGRP_VTIME = DF_LPG_VTIME.union(DF_LPV_VTIME)

    L_DF_RBCOBRP = L_DF_RBCSGRP_INSUNIX.union(L_DF_RBCSGRP_VTIME)

    return L_DF_RBCOBRP