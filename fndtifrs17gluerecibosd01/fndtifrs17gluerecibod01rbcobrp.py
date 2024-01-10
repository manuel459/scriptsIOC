def get_data(glue_context, connection):

    L_RBCOBRP_INSUNIX_LPG = '''
                            (
                               select 	'D' as INDDETREC,
                                        'RBCOBRP' as TABLAIFRS17,
                                        pre.receipt KRBRECPR,
                                        coalesce(dpr.code,0) KRCTPCBT,
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        '' TIOCFRM, --excluido
                                        '' TIOCTO, --excluido
                                        'PIG' KGIORIGM, --excluido
                                        '' VTXPREM, --excluido
                                        dpr.premium_cob + --existen registros de primas con comisi�n
                                            case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                            (	select	1
                                                                from	usinsug01.detail_pre dp0
                                                                where	dp0.usercomp = pre.usercomp
                                                                and 	dp0.company = pre.company
                                                                and 	dp0.receipt = pre.receipt
                                                                and 	dp0.code = dpr.code
                                                                and		dp0.certif = 0
                                                                and 	dp0.type_detai = '1'
                                                                and		dp0.bill_item not in (4,5,9,97))
                                                    then	coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                    from	usinsug01.detail_pre dp0
                                                                    where	dp0.usercomp = pre.usercomp
                                                                    and 	dp0.company = pre.company
                                                                    and 	dp0.receipt = pre.receipt
                                                                    and		dp0.certif = 0
                                                                    and		dp0.type_detai = '1'
                                                                    and		dp0.bill_item not in (4,5,9,97)),0) *
                                                            (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
                                                    else 0 end VMTPREM,
                                        '' VTXBONUS, --excluido
                                        '' VMTBONUS, --descartado
                                        dpr.capital VCAPITAL,
                                        (	dpr.premium_recdes + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1b)
                                                case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                (	select	1
                                                                    from	usinsug01.detail_pre dp0
                                                                    where	dp0.usercomp = pre.usercomp
                                                                    and 	dp0.company = pre.company
                                                                    and 	dp0.receipt = pre.receipt
                                                                    and		dp0.certif = 0
                                                                    and 	dp0.type_detai in ('3','4')
                                                                    and		dp0.bill_item not in (4,5,9,97))
                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsug01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = 0
                                                                                and 	dp0.type_detai in ('3','4')
                                                                                and		dp0.bill_item not in (4,5,9,97)),0)
                                                                    *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                        else 	0 end) +
                                            (	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                            from	usinsug01.detail_pre dp0
                                                            where	dp0.usercomp = pre.usercomp
                                                            and 	dp0.company = pre.company
                                                            and 	dp0.receipt = pre.receipt
                                                            and		dp0.code = dpr.code
                                                            and		dp0.bill_item = 5),0) + --prima der. emisi�n (nivel_1)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1)
                                                case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                (	select	1
                                                                    from	usinsug01.detail_pre dp0
                                                                    where	dp0.usercomp = pre.usercomp
                                                                    and 	dp0.company = pre.company
                                                                    and 	dp0.receipt = pre.receipt
                                                                    and		dp0.certif = 0
                                                                    and		dp0.bill_item = 5)
                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsug01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = 0
                                                                                and		dp0.bill_item = 5),0) --solo interesa los casos con el monto asociado al der. emisi�n
                                                                    *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                        else 	0 end) VMTENCG,
                                        (	dpr.premium_imp + --prima impuesto a nivel del certificado/cobertura (nivel_1a)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1b)
                                                case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                (	select	1
                                                                    from	usinsug01.detail_pre dp0
                                                                    where	dp0.usercomp = pre.usercomp
                                                                    and 	dp0.company = pre.company
                                                                    and 	dp0.receipt = pre.receipt
                                                                    and		dp0.certif = 0
                                                                    and 	dp0.type_detai = '2'
                                                                    and		dp0.bill_item not in (4,5,9,97))
                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsug01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = 0
                                                                                and 	dp0.type_detai = '2'
                                                                                and		dp0.bill_item not in (4,5,9,97)),0)
                                                                    *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                        else 	0 end) +
                                            (	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                            from	usinsug01.detail_pre dp0
                                                            where	dp0.usercomp = pre.usercomp
                                                            and 	dp0.company = pre.company
                                                            and 	dp0.receipt = pre.receipt
                                                            and		dp0.code = dpr.code
                                                            and		dp0.bill_item = 9),0) + --prima igv (nivel_1)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1)
                                                case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                (	select	1
                                                                    from	usinsug01.detail_pre dp0
                                                                    where	dp0.usercomp = pre.usercomp
                                                                    and 	dp0.company = pre.company
                                                                    and 	dp0.receipt = pre.receipt
                                                                    and		dp0.certif = 0
                                                                    and		dp0.bill_item = 9)
                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsug01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = 0
                                                                                and		dp0.bill_item = 9),0) --solo interesa los casos con el monto asociado al igv
                                                                    *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                        else 	0 end) VMTIMPO,
                                        dpr.premium_des + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                            --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                            --(si el certificado es 0, ya fue calculado en nivel_1b)
                                            case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                            (	select	1
                                                                from	usinsug01.detail_pre dp0
                                                                where	dp0.usercomp = pre.usercomp
                                                                and 	dp0.company = pre.company
                                                                and 	dp0.receipt = pre.receipt
                                                                and		dp0.certif = 0
                                                                and 	dp0.type_detai = '4'
                                                                and		dp0.bill_item not in (4,5,9,97))
                                                    then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                            from	usinsug01.detail_pre dp0
                                                                            where	dp0.usercomp = pre.usercomp
                                                                            and 	dp0.company = pre.company
                                                                            and 	dp0.receipt = pre.receipt
                                                                            and		dp0.certif = 0
                                                                            and		dp0.type_detai = '4'
                                                                            and		dp0.bill_item not in (4,5,9,97) ),0)
                                                                *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                    else 	0 end VMTDESC,
                                        '' VMTAGRA, --descartado
                                        par.cia DCOMPA,
                                        '' DMARCA, --excluido
                                        coalesce((	select 	case	when	pre.branch = 1
                                                                    then	acc.branch_pyg
                                                                    else	acc.branch_bal end
                                                    from	usinsug01.acc_autom2 acc
                                                    where	ctid =
                                                            coalesce(
                                                                (   select  min(ctid) --b�squeda normal en acc_autom2(ramo;producto;concepto 1 para todos, excepto incendio)
                                                                    from   usinsug01.acc_autom2 abe
                                                                    where   abe.branch = pre.branch
                                                                    and 	abe.product = pre.product
                                                                    and 	abe.concept_fac =
                                                                            case	when	pre.branch <> 1
                                                                                    then 	1 --es universal para todos los casos, excepto incendio
                                                                                    else 	coalesce(dpr.bill_item,1) --en caso por error est� sin valor se asigna el base
                                                                                    end),
                                                                (   select  min(ctid) --b�squeda igual a anterior, pero en caso el producto no exista en acc_autom2 (error LP)
                                                                    from   usinsug01.acc_autom2 abe
                                                                    where   abe.branch = pre.branch
                                                                    and 	abe.concept_fac =
                                                                            case	when	pre.branch <> 1
                                                                                    then 	1 --es universal para todos los casos, excepto incendio
                                                                                    else 	coalesce(dpr.bill_item,1) --en caso por error est� sin valor se asigna el base
                                                                                    end))),0) KGCRAMO_SAP
                                from	(	select	case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                            then pre.ctid
                                                            else null end pre_id,
                                                    dpr.bill_item ,
                                                    dpr.code,
                                                    SUM(coalesce(dpr.capital,0) * case when addsuini = '1' then 1 else 0 end) capital,
                                                    SUM(case 	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                then coalesce(dpr.premium,0) 
                                                                else 0 end) premium,
                                                    SUM(case 	when dpr.type_detai = '1' and dpr.bill_item not in (4,5,9,97)
                                                                then coalesce(dpr.premium,0) 
                                                                else 0 end) premium_cob,
                                                    SUM(case	when dpr.type_detai in ('3','4') and dpr.bill_item not in (4,5,9,97)
                                                                then coalesce(dpr.premium,0)
                                                                else 0 end) premium_recdes, --de la prima contable, los recargos y descuentos
                                                    SUM(case	when dpr.type_detai = '4' and dpr.bill_item not in (4,5,9,97)
                                                                then coalesce(dpr.premium,0)
                                                                else 0 end) premium_des, --de la prima contable, los descuentos
                                                    SUM(case	when dpr.type_detai = '2' and dpr.bill_item not in (4,5,9,97)
                                                                then coalesce(dpr.premium,0)
                                                                else 0 end) premium_imp --de la prima contable, los impuestos
                                            from	usinsug01.premium pre
                                            join	usinsug01.detail_pre dpr
                                                    on		dpr.usercomp = pre.usercomp
                                                    and 	dpr.company = pre.company
                                                    and 	dpr.receipt = pre.receipt
                                            where 	pre.usercomp = 1
                                            and 	pre.company = 1
                                            and		pre.branch = 1
                                            and 	cast(pre.effecdate as date) <= '12/31/2020'
                                            and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                            and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                            and 	pre.statusva not in ('2','3')
                                            group 	by 1,2,3 limit 10) dpr
                                join	usinsug01.premium pre on pre.ctid = dpr.pre_id
                                join 	(select 'LPG' cia, '-' sep) par on 1 = 1
                                --7m16s (ramo incendio)
                                --2m (ramo incendio) dev 1   
                            )AS TMP 
                            '''

    DF_RBCOBRP_LPG_INSUNIX = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOBRP_INSUNIX_LPG).load()
                       

    L_RBCOBRP_INSUNIX_LPV = '''
                            (
                              select 	'D' as INDDETREC,
                                        'RBCOBRP' as TABLAIFRS17,
                                        pre.receipt KRBRECPR,
                                        coalesce(dpr.code,0) KRCTPCBT,
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        '' TIOCFRM, --excluido
                                        '' TIOCTO, --excluido
                                        'PIV' KGIORIGM, --excluido
                                        '' VTXPREM, --excluido
                                        dpr.premium_cob + --existen registros de primas con comisi�n
                                            case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                            (	select	1
                                                                from	usinsuv01.detail_pre dp0
                                                                where	dp0.usercomp = pre.usercomp
                                                                and 	dp0.company = pre.company
                                                                and 	dp0.receipt = pre.receipt
                                                                and		dp0.certif = 0
                                                                and 	dp0.type_detai = '1'
                                                                and		dp0.bill_item not in (4,5,9,97))
                                                    then	coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                    from	usinsuv01.detail_pre dp0
                                                                    where	dp0.usercomp = pre.usercomp
                                                                    and 	dp0.company = pre.company
                                                                    and 	dp0.receipt = pre.receipt
                                                                    and		dp0.certif = 0
                                                                    and		dp0.type_detai = '1'
                                                                    and		dp0.bill_item not in (4,5,9,97)),0) *
                                                            (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
                                                    else 0 end VMTPREM,
                                        '' VTXBONUS, --excluido
                                        '' VMTBONUS, --descartado
                                        dpr.capital VCAPITAL,
                                        (	dpr.premium_recdes + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1b)
                                                case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                (	select	1
                                                                    from	usinsuv01.detail_pre dp0
                                                                    where	dp0.usercomp = pre.usercomp
                                                                    and 	dp0.company = pre.company
                                                                    and 	dp0.receipt = pre.receipt
                                                                    and		dp0.certif = 0
                                                                    and 	dp0.type_detai in ('1','3','4')
                                                                    and		dp0.bill_item not in (4,5,9,97))
                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsuv01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = 0
                                                                                and 	dp0.type_detai in ('3','4')
                                                                                and		dp0.bill_item not in (4,5,9,97)),0)
                                                                    *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                        else 	0 end) +
                                            (	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                            from	usinsuv01.detail_pre dp0
                                                            where	dp0.usercomp = pre.usercomp
                                                            and 	dp0.company = pre.company
                                                            and 	dp0.receipt = pre.receipt
                                                            and		dp0.code = dpr.code
                                                            and		dp0.bill_item = 5),0) + --prima der. emisi�n (nivel_1)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1)
                                                case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                (	select	1
                                                                    from	usinsuv01.detail_pre dp0
                                                                    where	dp0.usercomp = pre.usercomp
                                                                    and 	dp0.company = pre.company
                                                                    and 	dp0.receipt = pre.receipt
                                                                    and		dp0.code = dpr.code
                                                                    and		dp0.certif = 0
                                                                    and 	dp0.bill_item = 5)
                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsuv01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = 0
                                                                                and		dp0.bill_item = 5),0) --solo interesa los casos con el monto asociado al der. emisi�n
                                                                    *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                        else 	0 end) VMTENCG,
                                        (	dpr.premium_imp + --prima impuestos a nivel del certificado/cobertura (nivel_1a)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1b)
                                                case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                (	select	1
                                                                    from	usinsuv01.detail_pre dp0
                                                                    where	dp0.usercomp = pre.usercomp
                                                                    and 	dp0.company = pre.company
                                                                    and 	dp0.receipt = pre.receipt
                                                                    and		dp0.certif = 0
                                                                    and 	dp0.type_detai = '2'
                                                                    and		dp0.bill_item not in (4,5,9,97))
                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsuv01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = 0
                                                                                and 	dp0.type_detai = '2'
                                                                                and		dp0.bill_item not in (4,5,9,97)),0)
                                                                    *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                        else 	0 end) +
                                            (	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                            from	usinsuv01.detail_pre dp0
                                                            where	dp0.usercomp = pre.usercomp
                                                            and 	dp0.company = pre.company
                                                            and 	dp0.receipt = pre.receipt
                                                            and		dp0.code = dpr.code
                                                            and		dp0.bill_item = 9),0) + --prima igv (nivel_1)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1)
                                                case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                (	select	1
                                                                    from	usinsuv01.detail_pre dp0
                                                                    where	dp0.usercomp = pre.usercomp
                                                                    and 	dp0.company = pre.company
                                                                    and 	dp0.receipt = pre.receipt
                                                                    and		dp0.code = dpr.code
                                                                    and		dp0.certif = 0
                                                                    and 	dp0.bill_item = 9)
                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsuv01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = 0
                                                                                and		dp0.bill_item = 9),0) --solo interesa los casos con el monto asociado al igv
                                                                    *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                        else 	0 end) VMTIMPO,
                                        dpr.premium_des + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                            --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                            --(si el certificado es 0, ya fue calculado en nivel_1b)
                                            case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                            (	select	1
                                                                from	usinsuv01.detail_pre dp0
                                                                where	dp0.usercomp = pre.usercomp
                                                                and 	dp0.company = pre.company
                                                                and 	dp0.receipt = pre.receipt
                                                                and		dp0.certif = 0
                                                                and 	dp0.type_detai = '4'
                                                                and		dp0.bill_item not in (4,5,9,97))
                                                    then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                            from	usinsuv01.detail_pre dp0
                                                                            where	dp0.usercomp = pre.usercomp
                                                                            and 	dp0.company = pre.company
                                                                            and 	dp0.receipt = pre.receipt
                                                                            and		dp0.certif = 0
                                                                            and		dp0.type_detai = '4'
                                                                            and		dp0.bill_item not in (4,5,9,97)),0)
                                                                *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                    else 	0 end VMTDESC,
                                        '' VMTAGRA, --descartado
                                        par.cia DCOMPA,
                                        '' DMARCA, --excluido
                                        coalesce((	select 	acc.branch_bal
                                                    from	usinsuv01.acc_autom2 acc
                                                    where	ctid =
                                                            coalesce(
                                                                (   select  min(ctid) --b�squeda normal en acc_autom2(ramo;producto;concepto 1 para todos, excepto incendio)
                                                                    from    usinsuv01.acc_autom2 abe
                                                                    where   abe.branch = pre.branch
                                                                    and 	abe.product = pre.product
                                                                    and 	abe.concept_fac = 1),
                                                                (   select  min(ctid) --b�squeda igual a anterior, pero en caso el producto no exista en acc_autom2 (error LP)
                                                                    from    usinsuv01.acc_autom2 abe
                                                                    where   abe.branch = pre.branch
                                                                    and 	abe.concept_fac = 1))),0) KGCRAMO_SAP
                                from	(	select	case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                            then pre.ctid
                                                            else null end pre_id,
                                                    dpr.code,
                                                    case	when	pre.branch = 31
                                                            then	(	select	case	when	substr(pol.titularc,0,1) = 'E'
                                                                                        then	73
                                                                                        else	case	when	coalesce((	select  sum(ili.quantity)
                                                                                                                            from    usinsuv01.insured_li ili
                                                                                                                            where   ili.usercomp = pre.usercomp
                                                                                                                            and     ili.company = pre.company
                                                                                                                            and     ili.certype = pre.certype
                                                                                                                            and     ili.branch = pre.branch
                                                                                                                            and     ili.policy = pre.policy
                                                                                                                            and     ili.certif = 0
                                                                                                                            and     ili.effecdate <= pol.effecdate
                                                                                                                            and     (ili.nulldate is null or ili.nulldate > pol.effecdate)
                                                                                                                            and     quantity is not null),0) <= 1
                                                                                                        then	82
                                                                                                        else	-1/*(	select  min(sbs.cod_sbs_gyp)
                                                                                                                    from    product_sbs sbs
                                                                                                                    where   sbs.usercomp = pre.usercomp
                                                                                                                    and     sbs.company = pre.company
                                                                                                                    and     sbs.branch = pre.branch
                                                                                                                    and     sbs.product = pre.product
                                                                                                                    and     sbs.effecdate <= pre.effecdate
                                                                                                                    and     (sbs.nulldate is null or sbs.nulldate > pre.effecdate))*/
                                                                                                        end end
                                                                        from 	usinsuv01.policy pol
                                                                        where	pol.usercomp = pre.usercomp
                                                                        and     pol.company = pre.company
                                                                        and     pol.certype = pre.certype
                                                                        and     pol.branch = pre.branch
                                                                        and     pol.policy = pre.policy)
                                                            when	(pre.branch = 75 and pre.product = 1)
                                                            then 	case	(	select 	type_cla
                                                                                from	usinsuv01.life_prev
                                                                                where	ctid = 
                                                                                        (select coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                            (   select max(ctid) from usinsuv01.life_prev
                                                                                                where usercomp = pre.usercomp and company = pre.company and certype = pre.certype
                                                                                                and branch = pre.branch and policy = pre.policy and certif = coalesce(dpr.certif,0)
                                                                                                and effecdate <= pre.effecdate
                                                                                                and (nulldate is null or nulldate > pre.effecdate)
                                                                                                and statusva not in ('2','3')),
                                                                                            (   select max(ctid) from usinsuv01.life_prev
                                                                                                where usercomp = pre.usercomp and company = pre.company and certype = pre.certype
                                                                                                and branch = pre.branch and policy = pre.policy and certif = coalesce(dpr.certif,0)
                                                                                                and effecdate <= pre.effecdate
                                                                                                and (nulldate is null or nulldate >= pre.effecdate)
                                                                                                and statusva not in ('2','3'))),
                                                                                            (   select max(ctid) from usinsuv01.life_prev
                                                                                                where usercomp = pre.usercomp and company = pre.company and certype = pre.certype
                                                                                                and branch = pre.branch and policy = pre.policy and certif = coalesce(dpr.certif,0)
                                                                                                and effecdate <= pre.effecdate
                                                                                                and (nulldate is null or nulldate < pre.effecdate)
                                                                                                and statusva not in ('2','3'))),
                                                                                            (   select min(ctid) from usinsuv01.life_prev
                                                                                                where usercomp = pre.usercomp and company = pre.company and certype = pre.certype
                                                                                                and branch = pre.branch and policy = pre.policy and certif = coalesce(dpr.certif,0)
                                                                                                and effecdate > pre.effecdate
                                                                                                and (nulldate is null or nulldate > pre.effecdate)
                                                                                                and statusva not in ('2','3'))),
                                                                                            (   select min(ctid) from usinsuv01.life_prev
                                                                                                where usercomp = pre.usercomp and company = pre.company and certype = pre.certype
                                                                                                and branch = pre.branch and policy = pre.policy and certif = coalesce(dpr.certif,0)
                                                                                                and statusva not in ('2','3'))),
                                                                                            (   select min(ctid) from usinsuv01.life_prev
                                                                                                where usercomp = pre.usercomp and company = pre.company and certype = pre.certype
                                                                                                and branch = pre.branch and policy = pre.policy and certif = coalesce(dpr.certif,0)
                                                                                                and statusva in ('2','3')))))
                                                                            when	4 then 76
                                                                            when	5 then 76
                                                                            when	2 then 94
                                                                            when	3 then 94
                                                                            when	1 then 95
                                                                            else	0 end 
                                                            else 	coalesce(/*(	select  sbs.cod_sbs_gyp
                                                                                from    product_sbs tnb
                                                                                join	anexo1_sbs sbs
                                                                                        on		sbs.cod_sbs_bal = tnb.cod_sbs_bal
                                                                                        and     sbs.cod_sbs_gyp = tnb.cod_sbs_gyp
                                                                                where   tnb.branch = pre.branch
                                                                                and 	tnb.product = pre.product
                                                                                and 	tnb.nulldate is null)*/-1, 0)
                                                        end	branch_led,
                                                    SUM(coalesce(dpr.capital,0) * case when addsuini = '1' then 1 else 0 end) capital,
                                                    SUM(case 	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                then coalesce(dpr.premium,0) 
                                                                else 0 end) premium,
                                                    SUM(case 	when dpr.type_detai = '1' and dpr.bill_item not in (4,5,9,97)
                                                                then coalesce(dpr.premium,0) 
                                                                else 0 end) premium_cob,
                                                    SUM(case	when dpr.type_detai in ('3','4') and dpr.bill_item not in (4,5,9,97)
                                                                then coalesce(dpr.premium,0)
                                                                else 0 end) premium_recdes, --de la prima contable, los recargos y descuentos
                                                    SUM(case	when dpr.type_detai = '4' and dpr.bill_item not in (4,5,9,97)
                                                                then coalesce(dpr.premium,0)
                                                                else 0 end) premium_des, --de la prima contable, los descuentos
                                                    SUM(case	when dpr.type_detai = '2' and dpr.bill_item not in (4,5,9,97)
                                                                then coalesce(dpr.premium,0)
                                                                else 0 end) premium_imp --de la prima contable, los impuestos
                                            from	usinsuv01.premium pre
                                            join	usinsuv01.detail_pre dpr
                                                    on		dpr.usercomp = pre.usercomp
                                                    and 	dpr.company = pre.company
                                                    and 	dpr.receipt = pre.receipt
                                            where 	pre.usercomp = 1
                                            and 	pre.company = 1
                                            --and		pre.branch = 42
                                            and 	cast(pre.effecdate as date) <= '12/31/2020'
                                            and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                            and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                            and 	pre.statusva not in ('2','3')
                                            group 	by 1,2,3 limit 10) dpr
                                join	usinsuv01.premium pre on pre.ctid = dpr.pre_id
                                join 	(select 'LPV' cia, '-' sep) par on 1 = 1
                                --7m55s (ramo 42) - 104 registros; existen casos en que no hay capital o primas asociado a los registros (puede ser por el ambiente)
                                --1m 1s limit 10
                            )AS TMP 
                            '''
    DF_RBCOBRP_LPV_INSUNIX = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOBRP_INSUNIX_LPV).load()
       
    L_DF_RBCOBRP_INSUNIX = DF_RBCOBRP_LPG_INSUNIX.union(DF_RBCOBRP_LPV_INSUNIX)

    L_RBCOBRP_VTIME_LPG = '''
                            (
                               select 	'D' as INDDETREC,
                                        'RBCOBRP' as TABLAIFRS17,
                                        dpr.nreceipt KRBRECPR,
                                        coalesce(dpr.ndet_code,0) KRCTPCBT,
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        '' TIOCFRM, --excluido
                                        '' TIOCTO, --excluido
                                        'PVG' KGIORIGM, --excluido
                                        '' VTXPREM, --excluido
                                        dpr.npremium_cob VMTPREM,
                                        '' VTXBONUS, --excluido
                                        '' VMTBONUS, --descartado
                                        dpr.ncapital VCAPITAL,
                                        dpr.npremium_enc + 
                                            coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                        where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                        and		dp0."NBILL_ITEM" = 5
                                                        and		dp0."NBRANCH_LED" = dpr.nbranch_led),0) + 
                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                                and		dp0."NBILL_ITEM" = 5
                                                                and		dp0."NBRANCH_LED" not in 
                                                                        (	select	"NBRANCH_LED"
                                                                            from	usvtimg01."DETAIL_PRE"
                                                                            where 	"NRECEIPT" = dp0."NRECEIPT" and "NDIGIT" = dp0."NDIGIT" 
                                                                            and		"STYPE_DETAI" in ('1','2','4','6')
                                                                            and		"NBILL_ITEM" not in (4,5,9,97))),0) * 
                                                    coalesce((	dpr.dpr_con	/
                                                                nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                    from	usvtimg01."DETAIL_PRE" dp0
                                                                                    where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                                                    and		dp0."STYPE_DETAI" in ('1','2','4','6') and dp0."NBILL_ITEM" not in (4,5,9,97)),0),0)),0)) VMTENCG,
                                        coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                    from	usvtimg01."DETAIL_PRE" dp0
                                                    where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                    and		dp0."NBILL_ITEM" = 9
                                                    and		dp0."NBRANCH_LED" = dpr.nbranch_led),0) + 
                                            (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                            from	usvtimg01."DETAIL_PRE" dp0
                                                            where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                            and		dp0."NBILL_ITEM" = 9
                                                            and		dp0."NBRANCH_LED" not in 
                                                                    (	select	"NBRANCH_LED"
                                                                        from	usvtimg01."DETAIL_PRE"
                                                                        where 	"NRECEIPT" = dp0."NRECEIPT" and "NDIGIT" = dp0."NDIGIT" 
                                                                        and		"STYPE_DETAI" in ('1','2','4','6')
                                                                        and		"NBILL_ITEM" not in (4,5,9,97))),0) * 
                                                coalesce((	dpr.dpr_con	/
                                                            nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6') and dp0."NBILL_ITEM" not in (4,5,9,97)),0),0)),0)) +
                                            coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                        where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                        and		dp0."STYPE_DETAI" = '3' and dp0."NBILL_ITEM" not in (4,5,9,97)
                                                        and		dp0."NBRANCH_LED" = dpr.nbranch_led),0) + 
                                            (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                            from	usvtimg01."DETAIL_PRE" dp0
                                                            where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                            and		dp0."STYPE_DETAI" = '3' and dp0."NBILL_ITEM" not in (4,5,9,97)
                                                            and		dp0."NBRANCH_LED" not in 
                                                                    (	select	"NBRANCH_LED"
                                                                        from	usvtimg01."DETAIL_PRE"
                                                                        where 	"NRECEIPT" = dp0."NRECEIPT" and "NDIGIT" = dp0."NDIGIT" 
                                                                        and		"STYPE_DETAI" in ('1','2','4','6')
                                                                        and		"NBILL_ITEM" not in (4,5,9,97))),0) * 
                                                coalesce((	dpr.dpr_con	/
                                                            nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6') and dp0."NBILL_ITEM" not in (4,5,9,97)),0),0)),0)) VMTIMPO,
                                        dpr.npremium_des VMTDESC,
                                        '' VMTAGRA, --descartado
                                        par.cia DCOMPA,
                                        '' DMARCA, --excluido
                                        dpr.nbranch_led KGCRAMO_SAP
                                from	(	select 	pre."NRECEIPT" nreceipt,
                                                    dpr."NDET_CODE" ndet_code,
                                                    dpr."NBRANCH_LED" nbranch_led,
                                                    sum(coalesce(dpr."NPREMIUM",0)) dpr_con,
                                                    sum(coalesce((DPR."NCAPITAL" * case when dpr."SADDSUINI" in ('1','3') then 1 else 0 end),0)) ncapital,
                                                    sum(coalesce(case	when DPR."STYPE_DETAI" = '1' then coalesce(DPR."NPREMIUM",0)
                                                                        else 0 end,0)) npremium_cob, --prima cobertura
                                                    sum(coalesce(case	when DPR."STYPE_DETAI" in ('2','4','6') then coalesce(DPR."NPREMIUM",0)
                                                                        else 0 end,0)) npremium_enc,--recargos (2) + descuentos (4,6)
                                                    sum(coalesce(case	when DPR."STYPE_DETAI" in ('4','6') and dpr."NBILL_ITEM" not in (4,5,9,97) then coalesce(DPR."NPREMIUM",0)
                                                                        else 0 end,0)) npremium_des --prima descuentos
                                            FROM 	usvtimg01."PREMIUM" PRE
                                            join 	usvtimg01."DETAIL_PRE" DPR
                                                    on		DPR."NRECEIPT" = PRE."NRECEIPT" 
                                                    and 	DPR."NDIGIT" = PRE."NDIGIT"
                                                    and		DPR."STYPE_DETAI" in ('1','2','4','6') --se equivale la selecci�n de ramos contables con RBRECPR
                                                    and		dpr."NBILL_ITEM" not in (4,5,9,97) --solo se
                                            WHERE 	PRE."NDIGIT" = 0
                                            AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2020'
                                            AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2020')
                                            AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2020')
                                            AND 	PRE."SSTATUSVA" NOT IN ('2','3')
                                            group 	by 1,2,3 limit 10) dpr
                                join 	(select 'LPG' cia, '-' sep) par on 1 = 1
                                --8.145s (todos los ramos)
                                --2m 38 s limit 10 
                            ) AS TMP 
                            '''

    DF_LPG_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOBRP_VTIME_LPG).load()
       
    L_RBCOBRP_VTIME_LPV = '''
                            (
                                select 	'D' as INDDETREC,
       	                                'RBCOBRP' as TABLAIFRS17,
                                        dpr.nreceipt KRBRECPR,
                                        coalesce(dpr.ndet_code,0) KRCTPCBT,
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        '' TIOCFRM, --excluido
                                        '' TIOCTO, --excluido
                                        'PVV' KGIORIGM, --excluido
                                        '' VTXPREM, --excluido
                                        dpr.npremium_cob VMTPREM,
                                        '' VTXBONUS, --excluido
                                        '' VMTBONUS, --descartado
                                        dpr.ncapital VCAPITAL,
                                        dpr.npremium_enc + 
                                            coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                        from	usvtimv01."DETAIL_PRE" dp0
                                                        where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                        and		dp0."NBILL_ITEM" = 5
                                                        and		dp0."NBRANCH_LED" = dpr.nbranch_led),0) + 
                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                                and		dp0."NBILL_ITEM" = 5
                                                                and		dp0."NBRANCH_LED" not in 
                                                                        (	select	"NBRANCH_LED"
                                                                            from	usvtimv01."DETAIL_PRE"
                                                                            where 	"NRECEIPT" = dp0."NRECEIPT" and "NDIGIT" = dp0."NDIGIT" 
                                                                            and		"STYPE_DETAI" in ('1','2','4','6')
                                                                            and		"NBILL_ITEM" not in (4,5,9,97))),0) * 
                                                    coalesce((	dpr.dpr_con	/
                                                                nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                    from	usvtimv01."DETAIL_PRE" dp0
                                                                                    where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                                                    and		dp0."STYPE_DETAI" in ('1','2','4','6') and dp0."NBILL_ITEM" not in (4,5,9,97)),0),0)),0)) VMTENCG,
                                        coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                    from	usvtimv01."DETAIL_PRE" dp0
                                                    where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                    and		dp0."NBILL_ITEM" = 9
                                                    and		dp0."NBRANCH_LED" = dpr.nbranch_led),0) + 
                                            (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                            from	usvtimv01."DETAIL_PRE" dp0
                                                            where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                            and		dp0."NBILL_ITEM" = 9
                                                            and		dp0."NBRANCH_LED" not in 
                                                                    (	select	"NBRANCH_LED"
                                                                        from	usvtimv01."DETAIL_PRE"
                                                                        where 	"NRECEIPT" = dp0."NRECEIPT" and "NDIGIT" = dp0."NDIGIT" 
                                                                        and		"STYPE_DETAI" in ('1','2','4','6')
                                                                        and		"NBILL_ITEM" not in (4,5,9,97))),0) * 
                                                coalesce((	dpr.dpr_con	/
                                                            nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6') and dp0."NBILL_ITEM" not in (4,5,9,97)),0),0)),0)) +
                                            coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                        from	usvtimv01."DETAIL_PRE" dp0
                                                        where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                        and		dp0."STYPE_DETAI" = '3' and dp0."NBILL_ITEM" not in (4,5,9,97)
                                                        and		dp0."NBRANCH_LED" = dpr.nbranch_led),0) + 
                                            (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                            from	usvtimv01."DETAIL_PRE" dp0
                                                            where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                            and		dp0."STYPE_DETAI" = '3' and dp0."NBILL_ITEM" not in (4,5,9,97)
                                                            and		dp0."NBRANCH_LED" not in 
                                                                    (	select	"NBRANCH_LED"
                                                                        from	usvtimv01."DETAIL_PRE"
                                                                        where 	"NRECEIPT" = dp0."NRECEIPT" and "NDIGIT" = dp0."NDIGIT" 
                                                                        and		"STYPE_DETAI" in ('1','2','4','6')
                                                                        and		"NBILL_ITEM" not in (4,5,9,97))),0) * 
                                                coalesce((	dpr.dpr_con	/
                                                            nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                where	dp0."NRECEIPT" = dpr.nreceipt and dp0."NDIGIT" = 0
                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6') and dp0."NBILL_ITEM" not in (4,5,9,97)),0),0)),0)) VMTIMPO,
                                        dpr.npremium_des VMTDESC,
                                        '' VMTAGRA, --descartado
                                        par.cia DCOMPA,
                                        '' DMARCA, --excluido
                                        dpr.nbranch_led KGCRAMO_SAP
                                from	(	select 	pre."NRECEIPT" nreceipt,
                                                    dpr."NDET_CODE" ndet_code,
                                                    dpr."NBRANCH_LED" nbranch_led,
                                                    sum(coalesce(dpr."NPREMIUM",0)) dpr_con,
                                                    sum(coalesce((DPR."NCAPITAL" * case when dpr."SADDSUINI" in ('1','3') then 1 else 0 end),0)) ncapital,
                                                    sum(coalesce(case	when DPR."STYPE_DETAI" = '1' then coalesce(DPR."NPREMIUM",0)
                                                                        else 0 end,0)) npremium_cob, --prima cobertura
                                                    sum(coalesce(case	when DPR."STYPE_DETAI" in ('2','4','6') then coalesce(DPR."NPREMIUM",0)
                                                                        else 0 end,0)) npremium_enc,--recargos (2) + descuentos (4,6)
                                                    sum(coalesce(case	when DPR."STYPE_DETAI" in ('4','6') and dpr."NBILL_ITEM" not in (4,5,9,97) then coalesce(DPR."NPREMIUM",0)
                                                                        else 0 end,0)) npremium_des --prima descuentos
                                            FROM 	usvtimv01."PREMIUM" PRE
                                            join 	usvtimv01."DETAIL_PRE" DPR
                                                    on		DPR."NRECEIPT" = PRE."NRECEIPT" 
                                                    and 	DPR."NDIGIT" = PRE."NDIGIT"
                                                    and		DPR."STYPE_DETAI" in ('1','2','4','6') --se equivale la selecci�n de ramos contables con RBRECPR
                                                    and		dpr."NBILL_ITEM" not in (4,5,9,97) --solo se
                                            WHERE 	PRE."NDIGIT" = 0
                                            AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2020'
                                            AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2020')
                                            AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2020')
                                            AND 	PRE."SSTATUSVA" NOT IN ('2','3')
                                            group 	by 1,2,3 limit 10) dpr
                                join 	(select 'LPV' cia, '-' sep) par on 1 = 1
                                --31.224s (todos los ramos)
                                --21.187 s limit 10 dev 1   
                            )AS TMP 
                            '''

    DF_LPV_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOBRP_VTIME_LPV).load()

    L_DF_RBCOBRP_VTIME = DF_LPG_VTIME.union(DF_LPV_VTIME)

    L_DF_RBCOBRP = L_DF_RBCOBRP_INSUNIX.union(L_DF_RBCOBRP_VTIME)

    return L_DF_RBCOBRP