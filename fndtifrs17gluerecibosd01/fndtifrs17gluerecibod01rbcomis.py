from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, coalesce, lit

def get_data(glue_context, connection):

    L_RBCOMIS_INSUNIX_LPG_INCENDIO = f'''
                                        (
                                            select 	'D' as INDDETREC,
                                                        'RBCOMIS' as TABLAIFRS17,
                                                        pre.receipt KRBRECPR, --
                                                        '' KRCTPCOM, --PENDIENTE 01
                                                        coalesce(dpr.code,0) KRCTPCBT,
                                                        '' DTPREG, --excluido
                                                        '' TIOCPROC, --excluido
                                                        '' TIOCFRM, --excluido
                                                        '' TIOCTO, --excluido
                                                        'PIG' KGIORIGM, --excluido
                                                        dpr.commision + --existen registros de primas con comisi�n
                                                        case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                        (	select	1
                                                                                from	usinsug01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and 	dp0.code = dpr.code
                                                                                and		dp0.certif = 0
                                                                                and 	dp0.type_detai in ('1','3','4')
                                                                                and		dp0.bill_item not in (4,5,9,97))
                                                                then	coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                                        from	usinsug01.detail_pre dp0
                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                        and 	dp0.company = pre.company
                                                                                        and 	dp0.receipt = pre.receipt
                                                                                        and		dp0.certif = 0
                                                                                        and		dp0.type_detai in ('1','3','4')
                                                                                        and		dp0.bill_item not in (4,5,9,97)),0) *
                                                                        (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)) 
                                                                else	0 end VCOMISS,
                                                        coalesce(
                                                        coalesce((	dpr.commision + --existen registros de primas con comisi�n
                                                                        case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                                        (	select	1
                                                                                        from	usinsug01.detail_pre dp0
                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                        and 	dp0.company = pre.company
                                                                                        and 	dp0.receipt = pre.receipt
                                                                                        and 	dp0.code = dpr.code
                                                                                        and		dp0.certif = 0
                                                                                        and 	dp0.type_detai in ('1','3','4')
                                                                                        and		dp0.bill_item not in (4,5,9,97))
                                                                                then	coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                                                from	usinsug01.detail_pre dp0
                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                and 	dp0.company = pre.company
                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                and		dp0.certif = 0
                                                                                                and		dp0.type_detai in ('1','3','4')
                                                                                                and		dp0.bill_item not in (4,5,9,97)),0) *
                                                                                        (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
                                                                                else 0 end),0) /
                                                        nullif(	coalesce((	dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                                                --se procede a calcular la distribuci�n por el certificado 0, incluyendo las individuales (nivel_1b)
                                                                                --esto se debe a que no hay certificados para evaluar si ya se obtuvo o no el de la matriz o certificado 0
                                                                                case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                                                (	select	1
                                                                                                from	usinsug01.detail_pre dp0
                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                and 	dp0.company = pre.company
                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                and 	dp0.code = dpr.code
                                                                                                and		dp0.certif = 0
                                                                                                and 	dp0.type_detai in ('1','3','4')
                                                                                                and		dp0.bill_item not in (4,5,9,97))
                                                                                        then	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                        from	usinsug01.detail_pre dp0
                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                        and 	dp0.company = pre.company
                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                        and		dp0.certif = 0
                                                                                                        and		dp0.type_detai in ('1','3','4') 
                                                                                                        and		dp0.bill_item not in (4,5,9,97)),0)
                                                                                                * (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
                                                                                        else	0 end),0),0),0) * 100 VTXCOMIS,
                                                        '' VMTCOMPG, --PENDIENTE 02
                                                        '' VMTCOMRT, --PENDIENTE 03
                                                        '' KCBMED, --excluido
                                                        par.cia DCOMPA,
                                                        '' DMARCA, --excluido
                                                        coalesce((	select 	case	when	pre.branch = 1
                                                                                then	acc.branch_pyg
                                                                                else	acc.branch_bal end
                                                                from	usinsug01.acc_autom2 acc
                                                                where	ctid =
                                                                        coalesce(
                                                                                (   select  min(ctid) --b�squeda normal en acc_autom2(ramo;producto;concepto 1 para todos, excepto incendio)
                                                                                from    usinsug01.acc_autom2 abe
                                                                                where   abe.branch = pre.branch
                                                                                and 	abe.product = pre.product
                                                                                and 	abe.concept_fac =
                                                                                        case	when	pre.branch <> 1
                                                                                                then 	1 --es universal para todos los casos, excepto incendio
                                                                                                else 	coalesce(dpr.bill_item,1) --en caso por error est� sin valor se asigna el base
                                                                                                end),
                                                                                (   select  min(ctid) --b�squeda igual a anterior, pero en caso el producto no exista en acc_autom2 (error LP)
                                                                                from    usinsug01.acc_autom2 abe
                                                                                where   abe.branch = pre.branch
                                                                                and 	abe.concept_fac =
                                                                                        case	when	pre.branch <> 1
                                                                                                then 	1 --es universal para todos los casos, excepto incendio
                                                                                                else 	coalesce(dpr.bill_item,1) --en caso por error est� sin valor se asigna el base
                                                                                                end))),0) KGCRAMO_SAP,
                                                        '' DCODCSG --no aplica en este caso (el modelo en el excel ejemplo no indica compa��as o un % identificador asociado a la participaci�n por coasegurador)
                                                from	(	select	case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                        then pre.ctid
                                                                        else null end pre_id,
                                                                dpr.bill_item ,
                                                                dpr.code,
                                                                SUM(case 	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                then coalesce(dpr.premium,0) else 0 end) premium,
                                                                SUM(case 	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                then coalesce(dpr.commision,0) else 0 end) commision
                                                        from	usinsug01.premium pre
                                                        join	usinsug01.detail_pre dpr
                                                                on	dpr.usercomp = pre.usercomp
                                                                and dpr.company = pre.company
                                                                and dpr.receipt = pre.receipt
                                                        where 	pre.usercomp = 1
                                                        and 	pre.company = 1
                                                        and		pre.branch = 1
                                                        and 	cast(pre.effecdate as date) <= '12/31/2020'
                                                        and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                                        and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                                        and 	pre.statusva not in ('2','3')
                                                        group 	by 1,2,3 limit 100) dpr
                                                join	usinsug01.premium pre on pre.ctid = dpr.pre_id
                                                join 	(select 'LPG' cia, '-' sep) par on 1 = 1
                                                --4m52s (ramo incendio) prod
                                                --4m 55s (ramo incendio ) dev limit 100  
                                        ) AS TMP    
                                        '''

    DF_LPG_INSUNIX_INCENDIO  = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOMIS_INSUNIX_LPG_INCENDIO).load()
                       
    L_RBCOMIS_INSUNIX_LPG_OTROS_RAMOS = f'''
                                           (
                                                select 	'D' as INDDETREC,
                                                        'RBCOMIS' as TABLAIFRS17,
                                                                pre.receipt KRBRECPR, --
                                                                '' KRCTPCOM, --PENDIENTE 01
                                                                coalesce(dpr.code,0) KRCTPCBT,
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                '' TIOCFRM, --excluido
                                                                '' TIOCTO, --excluido
                                                                'PIG' KGIORIGM, --excluido
                                                                dpr.commision + --existen registros de primas con comisi�n
                                                                        case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                                                        (	select	1
                                                                                                                from	usinsug01.detail_pre dp0
                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                and 	dp0.company = pre.company
                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                and		dp0.certif = 0
                                                                                                                and 	dp0.type_detai in ('1','3','4')
                                                                                                                and		dp0.bill_item not in (4,5,9,97))
                                                                                        then	coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                                                                                from	usinsug01.detail_pre dp0
                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                and		dp0.certif = 0
                                                                                                                                and		dp0.type_detai in ('1','3','4')
                                                                                                                                and		dp0.bill_item not in (4,5,9,97)),0) *
                                                                                                        (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)) 
                                                                                        else 0 end VCOMISS,
                                                                coalesce(
                                                                        coalesce((	dpr.commision + --existen registros de primas con comisi�n
                                                                                                case	when	not exists --valida que el certificado 0 no haya sido considerado en las operaciones para evitar duplicar sus primas asociadas
                                                                                                                                (	select	1
                                                                                                                                        from	usinsug01.detail_pre dp0
                                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                                        and 	dp0.company = pre.company
                                                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                                                        and		dp0.certif = 0
                                                                                                                                        and 	dp0.type_detai in ('1','3','4')
                                                                                                                                        and		dp0.bill_item not in (4,5,9,97))
                                                                                                                then	coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                                                                                                        from	usinsug01.detail_pre dp0
                                                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                                                        and 	dp0.company = pre.company
                                                                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                                                                        and		dp0.certif = 0
                                                                                                                                                        and		dp0.type_detai in ('1','3','4')
                                                                                                                                                        and		dp0.bill_item not in (4,5,9,97)),0) *
                                                                                                                                (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
                                                                                                                else	0 end),0) /
                                                                        nullif(	coalesce((	dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
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
                                                                                                                                else	0 end),0),0),0) * 100 VTXCOMIS,
                                                                '' VMTCOMPG, --PENDIENTE 02
                                                                '' VMTCOMRT, --PENDIENTE 03
                                                                '' KCBMED, --excluido
                                                                par.cia DCOMPA,
                                                                '' DMARCA, --excluido
                                                                coalesce((	select 	acc.branch_bal
                                                                                        from	usinsug01.acc_autom2 acc
                                                                                        where	ctid =
                                                                                                        coalesce(
                                                                                                (   select  min(ctid) --b�squeda normal en acc_autom2(ramo;producto;concepto 1 para todos, excepto incendio)
                                                                                                from    usinsug01.acc_autom2 abe
                                                                                                where   abe.branch = pre.branch
                                                                                                and 	abe.product = pre.product
                                                                                                and 	abe.concept_fac = 1),
                                                                                                (   select  min(ctid) --b�squeda igual a anterior, pero en caso el producto no exista en acc_autom2 (error LP)
                                                                                                from    usinsug01.acc_autom2 abe
                                                                                                where   abe.branch = pre.branch
                                                                                                and 	abe.concept_fac = 1))),0) KGCRAMO_SAP,
                                                                '' DCODCSG --no aplica en este caso (el modelo en el excel ejemplo no indica compa��as o un % identificador asociado a la participaci�n por coasegurador)
                                                from	(	select	case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                                        then pre.ctid
                                                                                                        else null end pre_id,
                                                                                        dpr.code,
                                                                                        SUM(case 	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                                                then coalesce(dpr.premium,0) else 0 end) premium,
                                                                                        SUM(case 	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                                                then coalesce(dpr.commision,0) else 0 end) commision
                                                                        from	usinsug01.premium pre
                                                                        join	usinsug01.detail_pre dpr
                                                                                        on		dpr.usercomp = pre.usercomp
                                                                                        and 	dpr.company = pre.company
                                                                                        and 	dpr.receipt = pre.receipt
                                                                        where 	pre.usercomp = 1
                                                                        and 	pre.company = 1
                                                                        and		pre.branch <> 1
                                                --			and		pre.receipt in (801922162) --803554678
                                                                        --and 	pre.receipt in (select receipt from premium where usercomp = 1 and company = 1 and branch = 1) --prueba por �ndice
                                                                        and 	cast(pre.effecdate as date) <= '12/31/2020'
                                                                        and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                                                        and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                                                        and 	pre.statusva not in ('2','3')
                                                                        group 	by 1,2) dpr
                                                join	usinsug01.premium pre on pre.ctid = dpr.pre_id
                                                join 	(select 'LPG' cia, '-' sep) par on 1 = 1;
                                                --11m22s (otros ramos)         
                                           ) AS TMP
                                           '''

    DF_LPG_INSUNIX_OTROS_RAMOS  = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOMIS_INSUNIX_LPG_OTROS_RAMOS).load()
                                                               
    DF_LPG_INSUNIX = DF_LPG_INSUNIX_INCENDIO.union(DF_LPG_INSUNIX_OTROS_RAMOS)

    L_RBCOMIS_INSUNIX_LPV_OTROS_RAMOS = f'''
                                           (
                                              select 	'D' INDDETREC,
                                                        'RBCOMIS' TABLAIFRS17,
                                                        pre.receipt KRBRECPR,
                                                        '' KRCTPCOM, --no disponible
                                                        dat.covergen KRCTPCBT,
                                                        '' DTPREG, --excluido
                                                        '' TIOCPROC, --excluido
                                                        '' TIOCFRM, --excluido
                                                        '' TIOCTO, --excluido
                                                        'PIV' KGIORIGM, --excluido
                                                        dat.commis_p VCOMISS,
                                                        dat.commis_c VTXCOMIS,
                                                        '' VMTCOMPG, --PENDIENTE 02
                                                        '' VMTCOMRT, --PENDIENTE 03
                                                        '' KCBMED, --excluido
                                                        'LPV' DCOMPA,
                                                        '' DMARCA, --excluido
                                                        case	when	pre.branch = 31
                                                                        then	(	select	case 	when	substr(pol.titularc,0,1) = 'E'
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
                                                                                                                                                                else	(	select  min(sbs.cod_sbs_gyp)
                                                                                                                                                                                        from    usinsuv01.product_sbs sbs
                                                                                                                                                                                        where   sbs.usercomp = pre.usercomp
                                                                                                                                                                                        and     sbs.company = pre.company
                                                                                                                                                                                        and     sbs.branch = pre.branch
                                                                                                                                                                                        and     sbs.product = pre.product
                                                                                                                                                                                        and     sbs.effecdate <= pre.effecdate
                                                                                                                                                                                        and     (sbs.nulldate is null or sbs.nulldate > pre.effecdate))
                                                                                                                                                                end end
                                                                                                from 	usinsuv01.policy pol
                                                                                                where	pol.usercomp = pre.usercomp
                                                                                                and     pol.company = pre.company
                                                                                                and     pol.certype = pre.certype
                                                                                                and     pol.branch = pre.branch
                                                                                                and     pol.policy = pre.policy)
                                                                        else 	coalesce((	select  distinct sbs.cod_sbs_gyp
                                                                                                                from    usinsuv01.product_sbs tnb
                                                                                                                join	usinsuv01.anexo1_sbs sbs
                                                                                                                                on		sbs.cod_sbs_bal = tnb.cod_sbs_bal
                                                                                                                                and     sbs.cod_sbs_gyp = tnb.cod_sbs_gyp
                                                                                                                where   tnb.branch = pre.branch
                                                                                                                and 	tnb.product = pre.product
                                                                                                                and 	tnb.nulldate is null), 0)
                                                                        end	KGCRAMO_SAP,
                                                        '' DCODCSG
                                                from	(	select	dpr.pre_id,
                                                                                        dpr.covergen,
                                                                                        dpr.commision +
                                                                                                coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                                                                        from	usinsuv01.detail_pre dp0
                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                        and 	dp0.company = pre.company
                                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                                        and		dp0.type_detai in ('3','4')
                                                                                                                        and		dp0.bill_item not in (4,5,9,97)),0) *
                                                                                                (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)) commis_p,
                                                                                        coalesce((	dpr.commision +
                                                                                                                        coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                                and		dp0.type_detai in ('3','4')
                                                                                                                                                and		dp0.bill_item not in (4,5,9,97)),0)
                                                                                                                * (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))) /
                                                                                                        nullif((dpr.premium +
                                                                                                                        coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                                and		dp0.type_detai in ('3','4')
                                                                                                                                                and		dp0.bill_item not in (4,5,9,97)),0)
                                                                                                                        * (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))),0),0) * 100 commis_c
                                                                        from	(	select	pre.ctid pre_id,
                                                                                                                coalesce(	case    when    coalesce((  select  distinct pro.brancht
                                                                                                                                                                                                from    usinsuv01.product pro
                                                                                                                                                                                                where	pro.usercomp = pre.usercomp
                                                                                                                                                                                                and		pro.company = pre.company
                                                                                                                                                                                                and		pro.branch = pre.branch
                                                                                                                                                                                                and		pro.product = pre.product
                                                                                                                                                                                                and		pro.effecdate <= pre.effecdate
                                                                                                                                                                                                and		(pro.nulldate is null or pro.nulldate > pre.effecdate)),'0') not in ('1','5')
                                                                                                                                                        then    (   select  distinct gco.covergen
                                                                                                                                                                                from    usinsuv01.cover cov
                                                                                                                                                                                join	usinsuv01.gen_cover gco on gco.ctid =
                                                                                                                                                                                                coalesce(coalesce(coalesce(coalesce(
                                                                                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                and		modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate) --variaci�n 2 vigencias
                                                                                                                                                                                                                                                                and		statregt <> '4'), --variaci�n 3 reg. v�lido
                                                                                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                and		cover = cov.cover --variaci�n 1 sin modulec
                                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate) --variaci�n 2 vigencias
                                                                                                                                                                                                                                                                and		statregt <> '4')), --variaci�n 3 reg. v�lido
                                                                                                                                                                                                                                        coalesce((  select  max(ctid)
                                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate)
                                                                                                                                                                                                                                                                and		statregt = '4'),
                                                                                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                and		cover = cov.cover
                                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate)
                                                                                                                                                                                                                                                                and		statregt = '4'))), --no est� cortado
                                                                                                                                                                                                                        coalesce(coalesce(( select  max(ctid)
                                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                                and		statregt <> '4'),
                                                                                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                and		cover = cov.cover
                                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                                and		statregt <> '4')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                                                                                                                                                coalesce((  select  max(ctid)
                                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                        and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                        and		statregt = '4'),
                                                                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                        and		cover = cov.cover
                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                        and		statregt = '4')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                                                                        coalesce(coalesce(( select  max(ctid)
                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                                                                and		effecdate > pre.effecdate
                                                                                                                                                                                                                                                and		statregt <> '4'),
                                                                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                and		cover = cov.cover
                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                and		statregt <> '4')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                                                                                                                                coalesce((  select  max(ctid)
                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                        and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                        and		statregt = '4'),
                                                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                        and		cover = cov.cover
                                                                                                                                                                                                                                        and		effecdate > pre.effecdate
                                                                                                                                                                                                                                        and		statregt = '4')))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                                                                                                                where	cov.usercomp = pre.usercomp
                                                                                                                                                                                and		cov.company = pre.company
                                                                                                                                                                                and		cov.certype = pre.certype
                                                                                                                                                                                and		cov.branch = pre.branch
                                                                                                                                                                                and		cov.policy = pre.policy
                                                                                                                                                                                and		cov.certif = coalesce(dpr.certif,0)
                                                                                                                                                                                and		cov.cover = coalesce(dpr.code,0)
                                                                                                                                                                                and		cov.effecdate <= pre.effecdate
                                                                                                                                                                                and		(cov.nulldate is null or cov.nulldate > pre.effecdate))
                                                                                                                                                        else    (   select  distinct gco.covergen
                                                                                                                                                                                from    usinsuv01.life_cover gco
                                                                                                                                                                                where	gco.ctid =
                                                                                                                                                                                                coalesce(coalesce(coalesce(
                                                                                                                                                                                                                                        (	select  max(ctid)
                                                                                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                                and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate)
                                                                                                                                                                                                                                                and		statregt <> '4'), --que no est� cortado
                                                                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                                and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate)
                                                                                                                                                                                                                                                and		statregt = '4')),--est� cortado
                                                                                                                                                                                                                        coalesce((  select  max(ctid)
                                                                                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                                and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                and		statregt <> '4'),--no est� cortado pero fue anulado antes del efecto del registro
                                                                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                                and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                and		statregt = '4'))), --est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                                                                        coalesce((  select  max(ctid)
                                                                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                and		effecdate > pre.effecdate
                                                                                                                                                                                                                                and		statregt <> '4'),
                                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                        and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                        and		effecdate > pre.effecdate --est� cortado pero no al efecto de la tabla de datos particular
                                                                                                                                                                                                                and		statregt = '4'))))
                                                                                                                                                end, coalesce(dpr.code,0) * -1) covergen,
                                                                                                                sum(coalesce(dpr.premium,0)) premium,
                                                                                                                sum(coalesce(dpr.commision,0)) commision
                                                                                                from	usinsuv01.detail_pre dpr
                                                                                                join	(select cast('12/31/2020' as date) fecha) par on 1 = 1 --fecha indicada
                                                                                                join	usinsuv01.premium pre
                                                                                                                on		pre.usercomp = dpr.usercomp
                                                                                                                and 	pre.company = dpr.company
                                                                                                                and		pre.receipt = dpr.receipt
                                                                                                                and 	cast(pre.effecdate as date) <= par.fecha
                                                                                                                and 	(pre.expirdat is null or cast(pre.expirdat as date) >= par.fecha)
                                                                                                                and 	(pre.nulldate is null or cast(pre.nulldate as date) > par.fecha)
                                                                                                                and 	pre.statusva not in ('2','3')
                                                                                                                and		not (pre.branch = 75 and pre.product = 1) --no alterar (el query no aplica para esta combinaci�n de datos)
                                                                                                                and		exists 
                                                                                                                                (	select	1
                                                                                                                                        from	usinsuv01.detail_pre dp0
                                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                                        and		dp0.company = pre.company
                                                                                                                                        and		dp0.receipt = pre.receipt
                                                                                                                                        and		coalesce(dp0.commision,0) <> 0) --que posea comisi�n a nivel del recibo
                                                                                                where	dpr.usercomp = 1
                                                                                                and 	dpr.company = 1
                                                                                                and 	dpr.receipt is not null
                                                                                                and		dpr.type_detai ='1' --solo a nivel de coberturas
                                                                                                and		dpr.bill_item not in (4,5,9,97)
                                                                                                group	by 1,2) dpr
                                                                        join	usinsuv01.premium pre on pre.ctid = dpr.pre_id) dat
                                                join	usinsuv01.premium pre on pre.ctid = dat.pre_id
                                                where	dat.commis_p <> 0
                                           )AS TMP 
                                           '''

    DF_LPV_INSUNIX_OTROS_RAMOS = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOMIS_INSUNIX_LPV_OTROS_RAMOS).load()

    L_RBCOMIS_INSUNIX_LPV_RENTA_VITALICIA  = f'''
                                                (
                                                    select      'D' INDDETREC,
                                                                'RBCOMIS' TABLAIFRS17,
                                                                pre.receipt KRBRECPR,
                                                                '' KRCTPCOM, --no disponible
                                                                dat.covergen KRCTPCBT,
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                '' TIOCFRM, --excluido
                                                                '' TIOCTO, --excluido
                                                                'PIV' KGIORIGM, --excluido
                                                                dat.commis_p VCOMISS,
                                                                dat.commis_c VTXCOMIS,
                                                                '' VMTCOMPG, --PENDIENTE 02
                                                                '' VMTCOMRT, --PENDIENTE 03
                                                                '' KCBMED, --excluido
                                                                'LPV' DCOMPA,
                                                                '' DMARCA, --excluido
                                                                dat.branch_led KGCRAMO_SAP,
                                                                '' DCODCSG
                                                        from	(	select 	dpr.pre_id,
                                                                                                dpr.covergen,
                                                                                                dpr.commision VCOMISS,
                                                                                                dpr.commision +
                                                                                                        coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                and		dp0.type_detai in ('3','4')
                                                                                                                                and		dp0.bill_item not in (4,5,9,97)),0) *
                                                                                                        (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)) commis_p,
                                                                                                coalesce((	dpr.commision +
                                                                                                                                coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                                                                                                        from	usinsuv01.detail_pre dp0
                                                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                                                        and 	dp0.company = pre.company
                                                                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                                                                        and		dp0.type_detai in ('3','4')
                                                                                                                                                        and		dp0.bill_item not in (4,5,9,97)),0)
                                                                                                                        * (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))) /
                                                                                                                nullif((dpr.premium +
                                                                                                                                coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                                                        from	usinsuv01.detail_pre dp0
                                                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                                                        and 	dp0.company = pre.company
                                                                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                                                                        and		dp0.type_detai in ('3','4')
                                                                                                                                                        and		dp0.bill_item not in (4,5,9,97)),0)
                                                                                                                                * (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))),0),0) * 100 commis_c,
                                                                                                dpr.branch_led
                                                                                from	(	select	pre.ctid pre_id,
                                                                                                                        coalesce(	case	(	select 	type_cla
                                                                                                                                                                        from	usinsuv01.life_prev
                                                                                                                                                                        where	ctid = 
                                                                                                                                                                        (select coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                                                                                (   select max(ctid)
                                                                                                                                                                                        from usinsuv01.life_prev
                                                                                                                                                                                        where usercomp = pre.usercomp and company = pre.company and certype = pre.certype
                                                                                                                                                                                        and branch = pre.branch and policy = pre.policy and certif = coalesce(dpr.certif,0)
                                                                                                                                                                                        and effecdate <= pre.effecdate
                                                                                                                                                                                        and (nulldate is null or nulldate > pre.effecdate)
                                                                                                                                                                                        and statusva not in ('2','3')),
                                                                                                                                                                                (   select max(ctid)
                                                                                                                                                                                        from usinsuv01.life_prev
                                                                                                                                                                                        where usercomp = pre.usercomp and company = pre.company and certype = pre.certype
                                                                                                                                                                                        and branch = pre.branch and policy = pre.policy and certif = coalesce(dpr.certif,0)
                                                                                                                                                                                        and effecdate <= pre.effecdate
                                                                                                                                                                                        and (nulldate is null or nulldate >= pre.effecdate)
                                                                                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                                                                                (   select max(ctid)
                                                                                                                                                                                        from usinsuv01.life_prev
                                                                                                                                                                                        where usercomp = pre.usercomp and company = pre.company and certype = pre.certype
                                                                                                                                                                                        and branch = pre.branch and policy = pre.policy and certif = coalesce(dpr.certif,0)
                                                                                                                                                                                        and effecdate <= pre.effecdate
                                                                                                                                                                                        and (nulldate is null or nulldate < pre.effecdate)
                                                                                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                                                                                (   select min(ctid)
                                                                                                                                                                                        from usinsuv01.life_prev
                                                                                                                                                                                        where usercomp = pre.usercomp and company = pre.company and certype = pre.certype
                                                                                                                                                                                        and branch = pre.branch and policy = pre.policy and certif = coalesce(dpr.certif,0)
                                                                                                                                                                                        and effecdate > pre.effecdate
                                                                                                                                                                                        and (nulldate is null or nulldate > pre.effecdate)
                                                                                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                                                                                (   select min(ctid)
                                                                                                                                                                                        from usinsuv01.life_prev
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
                                                                                                                                                                else	0 end, 0) branch_led,
                                                                                                                        coalesce(	case    when    coalesce((  select  distinct pro.brancht
                                                                                                                                                                                                        from    usinsuv01.product pro
                                                                                                                                                                                                        where	pro.usercomp = pre.usercomp
                                                                                                                                                                                                        and		pro.company = pre.company
                                                                                                                                                                                                        and		pro.branch = pre.branch
                                                                                                                                                                                                        and		pro.product = pre.product
                                                                                                                                                                                                        and		pro.effecdate <= pre.effecdate
                                                                                                                                                                                                        and		(pro.nulldate is null or pro.nulldate > pre.effecdate)),'0') not in ('1','5')
                                                                                                                                                                then    (   select  distinct gco.covergen
                                                                                                                                                                                        from    usinsuv01.cover cov
                                                                                                                                                                                        join	usinsuv01.gen_cover gco on gco.ctid =
                                                                                                                                                                                                        coalesce(coalesce(coalesce(coalesce(
                                                                                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                        and		modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate) --variaci�n 2 vigencias
                                                                                                                                                                                                                                                                        and		statregt <> '4'), --variaci�n 3 reg. v�lido
                                                                                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                        and		cover = cov.cover --variaci�n 1 sin modulec
                                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate) --variaci�n 2 vigencias
                                                                                                                                                                                                                                                                        and		statregt <> '4')), --variaci�n 3 reg. v�lido
                                                                                                                                                                                                                                                coalesce((  select  max(ctid)
                                                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                        and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate)
                                                                                                                                                                                                                                                                        and		statregt = '4'),
                                                                                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                        and		cover = cov.cover
                                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate)
                                                                                                                                                                                                                                                                        and		statregt = '4'))), --no est� cortado
                                                                                                                                                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                        and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                                        and		statregt <> '4'),
                                                                                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                        and		cover = cov.cover
                                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                                        and		statregt <> '4')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                                                                                                                                                        coalesce((  select  max(ctid)
                                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                                and		statregt = '4'),
                                                                                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                                and		cover = cov.cover
                                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                                and		statregt = '4')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                        and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                                                                        and		effecdate > pre.effecdate
                                                                                                                                                                                                                                                        and		statregt <> '4'),
                                                                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                        and		cover = cov.cover
                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                        and		statregt <> '4')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                                                                                                                                        coalesce((  select  max(ctid)
                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                                                                and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                and		statregt = '4'),
                                                                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                                                                and		cover = cov.cover
                                                                                                                                                                                                                                                and		effecdate > pre.effecdate
                                                                                                                                                                                                                                                and		statregt = '4')))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                                                                                                                        where	cov.usercomp = pre.usercomp
                                                                                                                                                                                        and		cov.company = pre.company
                                                                                                                                                                                        and		cov.certype = pre.certype
                                                                                                                                                                                        and		cov.branch = pre.branch
                                                                                                                                                                                        and		cov.policy = pre.policy
                                                                                                                                                                                        and		cov.certif = coalesce(dpr.certif,0)
                                                                                                                                                                                        and		cov.cover = coalesce(dpr.code,0)
                                                                                                                                                                                        and		cov.effecdate <= pre.effecdate
                                                                                                                                                                                        and		(cov.nulldate is null or cov.nulldate > pre.effecdate))
                                                                                                                                                                else    (   select  distinct gco.covergen
                                                                                                                                                                                        from    usinsuv01.life_cover gco
                                                                                                                                                                                        where	gco.ctid =
                                                                                                                                                                                                        coalesce(coalesce(coalesce(
                                                                                                                                                                                                                                                (	select  max(ctid)
                                                                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                                        and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate)
                                                                                                                                                                                                                                                        and		statregt <> '4'), --que no est� cortado
                                                                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                                        and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and (nulldate is null or nulldate > pre.effecdate)
                                                                                                                                                                                                                                                        and		statregt = '4')),--est� cortado
                                                                                                                                                                                                                                coalesce((  select  max(ctid)
                                                                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                                        and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                        and		statregt <> '4'),--no est� cortado pero fue anulado antes del efecto del registro
                                                                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                                        and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                                        and		effecdate <= pre.effecdate and nulldate <= pre.effecdate
                                                                                                                                                                                                                                                        and		statregt = '4'))), --est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                                                                                coalesce((  select  max(ctid)
                                                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                                                        where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                        and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                        and		effecdate > pre.effecdate
                                                                                                                                                                                                                                        and		statregt <> '4'),
                                                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                                                                where   usercomp = pre.usercomp and company = pre.company and branch = pre.branch and product = pre.product and currency = pre.currency --�ndice regular
                                                                                                                                                                                                                                and		cover = coalesce(dpr.code,0)
                                                                                                                                                                                                                                and		effecdate > pre.effecdate --est� cortado pero no al efecto de la tabla de datos particular
                                                                                                                                                                                                                        and		statregt = '4'))))
                                                                                                                                                        end, coalesce(dpr.code,0) * -1) covergen,
                                                                                                                        sum(coalesce(dpr.premium,0)) premium,
                                                                                                                        sum(coalesce(dpr.commision,0)) commision
                                                                                                        from	usinsuv01.detail_pre dpr
                                                                                                        join	(select cast('12/31/2020' as date) fecha) par on 1 = 1 --fecha indicada
                                                                                                        join	usinsuv01.premium pre
                                                                                                                        on		pre.usercomp = dpr.usercomp
                                                                                                                        and 	pre.company = dpr.company
                                                                                                                        and		pre.receipt = dpr.receipt
                                                                                                                        and 	cast(pre.effecdate as date) <= par.fecha
                                                                                                                        and 	(pre.expirdat is null or cast(pre.expirdat as date) >= par.fecha)
                                                                                                                        and 	(pre.nulldate is null or cast(pre.nulldate as date) > par.fecha)
                                                                                                                        and 	pre.statusva not in ('2','3')
                                                                                                                        and		pre.branch = 75 --no alterar (el query es exclusivo para este valor)
                                                                                                                        and 	pre.product = 1 --no alterar (el query es exclusivo para este valor)
                                                                                                                        and		exists 
                                                                                                                                        (	select	1
                                                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                                and		dp0.company = pre.company
                                                                                                                                                and		dp0.receipt = pre.receipt
                                                                                                                                                and		coalesce(dp0.commision,0) <> 0) --que posea comisi�n a nivel del recibo
                                                                                                        where	dpr.usercomp = 1
                                                                                                        and 	dpr.company = 1
                                                                                                        and 	dpr.receipt is not null
                                                                                                        and		dpr.type_detai in ('1','3','4')
                                                                                                        and		dpr.bill_item not in (4,5,9,97)
                                                                                                        group	by 1,2,3) dpr
                                                                                join	usinsuv01.premium pre on pre.ctid = dpr.pre_id) dat
                                                        join	usinsuv01.premium pre on pre.ctid = dat.pre_id
                                                        where	dat.commis_p <> 0    
                                                ) AS TMP
                                                '''
    DF_LPV_INSUNIX_OTROS_RENTA_VITALICIA = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOMIS_INSUNIX_LPV_RENTA_VITALICIA).load()

    DF_LPV_INSUNIX = DF_LPV_INSUNIX_OTROS_RAMOS.union(DF_LPV_INSUNIX_OTROS_RENTA_VITALICIA)

    L_DF_RBCOMIS_INSUNIX  =  DF_LPG_INSUNIX.union(DF_LPV_INSUNIX)          
                             
    L_RBCOMIS_VTIME_LPG = f'''
                            (
                                select 	'D' as INDDETREC,
                                        'RBCOMIS' as TABLAIFRS17,
                                        dpr.nreceipt KRBRECPR,
                                        '' KRCTPCOM, --PENDIENTE 01
                                        coalesce(dpr.ndet_code,0) KRCTPCBT,
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        '' TIOCFRM, --excluido
                                        '' TIOCTO, --excluido
                                        'PVG' KGIORIGM, --excluido
                                        dpr.ncommision VCOMISS,
                                        coalesce(coalesce(dpr.ncommision,0) / nullif(dpr.npremium,0),0) * 100 VTXCOMIS,
                                        '' VMTCOMPG, --PENDIENTE 02
                                        '' VMTCOMRT, --PENDIENTE 03
                                        '' KCBMED, --excluido
                                        par.cia DCOMPA,
                                        '' DMARCA, --excluido,
                                        dpr.nbranch_led KGCRAMO_SAP,
                                        '' DCODCSG --no aplica en este caso (el modelo en el excel ejemplo no indica compa��as o un % identificador asociado a la participaci�n por coasegurador)
                                from	(	select 	pre."NRECEIPT" nreceipt,
                                                    dpr."NDET_CODE" ndet_code,
                                                    dpr."NBRANCH_LED" nbranch_led,
                                                    sum(coalesce(DPR."NPREMIUM",0)) npremium,
                                                    sum(coalesce(DPR."NCOMMISION",0)) ncommision
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
                                            AND 	PRE."SSTATUSVA"  NOT IN ('2','3')
                                            group 	by 1,2,3 limit 100) dpr
                                join 	(select 'LPG' cia, '-' sep) par on 1 = 1
                                --8.314s (todos los ramos) prod
                                --- 3 m 22 s dev 1       
                            )AS TMP 
                            '''

    DF_LPG_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOMIS_VTIME_LPG).load()
       
    L_RBCOMIS_VTIME_LPV = f'''
                            (
                                select  'D' as INDDETREC,
                                        'RBCOMIS' as TABLAIFRS17,
                                        dpr.nreceipt KRBRECPR,
                                        '' KRCTPCOM, --PENDIENTE 01
                                        coalesce(dpr.ndet_code,0) KRCTPCBT,
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        '' TIOCFRM, --excluido
                                        '' TIOCTO, --excluido
                                        'PVV' KGIORIGM, --excluido
                                        dpr.ncommision VCOMISS,
                                        coalesce(coalesce(dpr.ncommision,0) / nullif(dpr.npremium,0),0) * 100 VTXCOMIS,
                                        '' VMTCOMPG, --PENDIENTE 02
                                        '' VMTCOMRT, --PENDIENTE 03
                                        '' KCBMED, --excluido
                                        par.cia DCOMPA,
                                        '' DMARCA, --excluido,
                                        dpr.nbranch_led KGCRAMO_SAP,
                                        '' DCODCSG --no aplica en este caso (el modelo en el excel ejemplo no indica compa��as o un % identificador asociado a la participaci�n por coasegurador)
                                from	(	select 	pre."NRECEIPT" nreceipt,
                                                    dpr."NDET_CODE" ndet_code,
                                                    dpr."NBRANCH_LED" nbranch_led,
                                                    sum(coalesce(DPR."NPREMIUM",0)) npremium,
                                                    sum(coalesce(DPR."NCOMMISION",0)) ncommision
                                            FROM 	usvtimv01."PREMIUM" PRE
                                            join 	usvtimv01."DETAIL_PRE" DPR
                                                    on		DPR."NRECEIPT" = PRE."NRECEIPT" 
                                                    and 	DPR."NDIGIT" = PRE."NDIGIT"
                                                    and 	DPR."STYPE_DETAI" in ('1','2','4','6')
                                                    and		dpr."NBILL_ITEM" not in (4,5,9,97)
                                            WHERE 	PRE."NDIGIT" = 0
                                            AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2020'
                                            AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2020')
                                            AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2020')
                                            AND 	PRE."SSTATUSVA"  NOT IN ('2','3')
                                            group 	by 1,2,3 limit 100) dpr
                                join 	(select 'LPV' cia, '-' sep) par on 1 = 1
                                --33.321s (todos los ramos) PROD
                                --37.279s dev 1        
                            )AS TMP 
                            '''

    DF_LPV_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOMIS_VTIME_LPV).load()

    L_DF_RBCOMIS_VTIME = DF_LPG_VTIME.union(DF_LPV_VTIME)

    L_RBCOMIS_INSIS_LPV = f'''
                          (
                                SELECT 
                                        'D' as INDDETREC,
                                        'RBCOMIS' as TABLAIFRS17,                 
                                        BDO."DOC_NUMBER" KRBRECPR,
                                        B."INTER_TYPE" KRCTPCOM,  
                                        '' KRCTPCBT,
                                        '' DTPREG,
                                        '' TIOCPROC,
                                        '' TIOCFRM,
                                        '' TIOCTO,
                                        'PNV' KGIORIGM,
                                        B."AMOUNT" VCOMISS, 
                                        B."COMM_CI_PERCENT"  VTXCOMIS, 
                                        B."AMOUNT" VMTCOMPG, 
                                        0 VMTCOMRT,
                                        '' KCBMED,        
                                        'LPV' DCOMPA,
                                        '' DMARCA,
                                        COALESCE(TRUNC(TB."TECHNICAL_BRANCH", 0), 0) || ' - ' || COALESCE(TB."TB_NAME", '0') KGCRAMO_SAP,
                                        '' DCODCSG        
                                FROM USINSIV01."POLICY" P 
                                INNER JOIN (SELECT DISTINCT CNP."PRODUCT_CODE", PC."POLICY_ID",/*,CP."PARAM_CPR_ID" , CPV."PARAM_VALUE_CPR_ID", CPV."DESCRIPTION",*/ PC."COND_DIMENSION"
                                        FROM usinsiv01."CFG_NL_PRODUCT" CNP                                      
                                        INNER JOIN usinsiv01."CFG_NL_PRODUCT_CONDS" CNPC ON CNPC."PRODUCT_LINK_ID" = CNP."PRODUCT_LINK_ID"
                                        INNER JOIN usinsiv01."CPR_PARAMS" CP ON CP."PARAM_CPR_ID" = CNPC."PARAM_CPR_ID"
                                        INNER JOIN usinsiv01."POLICY_CONDITIONS" PC ON CP."PARAM_NAME" = PC."COND_TYPE" AND PC."COND_TYPE" LIKE 'AS_IS%'
                                        INNER JOIN usinsiv01."CPRS_PARAM_VALUE"CPV ON CPV."PARAM_ID" = CP."PARAM_CPR_ID"   AND CPV."PARAM_VALUE" = PC."COND_DIMENSION"
                                ) PROD ON P."POLICY_ID" = PROD."POLICY_ID" AND P."INSR_TYPE" = PROD."PRODUCT_CODE"
                                INNER JOIN usinsiv01."CFGLPV_POLICY_TECHBRANCH_SBS" TB ON (PROD."PRODUCT_CODE" = TB."INSR_TYPE"  AND CAST(PROD."COND_DIMENSION" AS INT) = TB."AS_IS_PRODUCT" AND CAST(P."ATTR1" AS BIGINT) = TB."TECHNICAL_BRANCH")       
                                JOIN USINSIV01."BLC_ITEMS" I ON P."POLICY_ID" = CAST(I."COMPONENT" AS BIGINT)  
                                JOIN USINSIV01."BLC_TRANSACTIONS" TR ON  (I."ITEM_ID" = TR."ITEM_ID")
                                JOIN USINSIV01."BLC_DOCUMENTS" BDO ON (TR."DOC_ID" = BDO."DOC_ID")
                                LEFT JOIN (SELECT B."DOC_ID",B."DOC_NUMBER", /*"POLICY_NO", "STATUS",*/ "ACTION_TYPE"/*, "POLICY_CLASS", "DOC_REVERSE_DATE"*/, A."ID"
                                        FROM USINSIV01."BLC_PROFORMA_GEN" A 
                                        JOIN (SELECT "DOC_NUMBER","DOC_ID", MAX("ID") AS ID FROM USINSIV01."BLC_PROFORMA_GEN" GROUP BY "DOC_NUMBER","DOC_ID","ACTION_TYPE") B
                                        ON A."ID" = B.ID AND A."DOC_NUMBER" = B."DOC_NUMBER" AND A."DOC_ID" = B."DOC_ID") BP
                                ON (BDO."DOC_ID" = BP."DOC_ID" 
                                AND BDO."DOC_NUMBER" = BP."DOC_NUMBER" 
                                AND BP."ACTION_TYPE" = 'CRE')
                                LEFT JOIN USINSIV01."BLC_PROFORMA_ACC" B 
                                ON   B."ID" = BP."ID" AND  B."INTER_TYPE" IN ('BROKER', 'MARKETER')              
                                WHERE BDO."DOC_CLASS" = 'B' AND "DOC_TYPE_ID" IN (19997, 19713) LIMIT 100    
                          )AS TMP 
                          '''

    L_DF_RBCOMIS_INSIS = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCOMIS_INSIS_LPV).load()

    L_DF_RBCOMIS = L_DF_RBCOMIS_INSUNIX.union(L_DF_RBCOMIS_VTIME).union(L_DF_RBCOMIS_INSIS)

    L_DF_RBCOMIS = L_DF_RBCOMIS.withColumn("vcomiss", coalesce(col("vcomiss").cast(DecimalType(12, 2)), lit(0))).withColumn("vtxcomis", coalesce(col("vtxcomis").cast(DecimalType(12, 4)), lit(0))).withColumn("vmtcompg", coalesce(col("vmtcompg").cast(DecimalType(12, 2)), lit(0))).withColumn("vmtcomrt", coalesce(col("vmtcomrt").cast(DecimalType(12, 2)), lit(0)))

    return L_DF_RBCOMIS