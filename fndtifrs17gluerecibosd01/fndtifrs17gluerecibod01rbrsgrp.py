def get_data(glue_context, connection):

  L_RBRSGRP_INSUNIX_LPG = f'''
                             (
                                select	'D' as INDDETREC,
                                        'RBRSGRP' as TABLAIFRS17,
                                                pre.receipt KRBRECPR,
                                                '' DORDRSG, --excluido
                                                '' DTPREG, --excluido
                                                '' TIOCPROC, --excluido
                                                '' TIOCFRM, --excluido
                                                '' TIOCTO, --excluido
                                                'PIG' KGIORIGM, --excluido
                                                rea.rei_number DTRATADO,
                                                coalesce(rea.porc_capital,0) * 100 VTXCAPIT,
                                                '' VTXPREM, --descartado
                                                coalesce(rea.porc_commision,0) VTXCOMMD,
                                                coalesce((	select 	max(coalesce(dpr.capital,0))
                                                                        from	usinsug01.detail_pre dpr
                                                                        where	dpr.usercomp = pre.usercomp
                                                                        and		dpr.company = pre.company
                                                                        and		dpr.receipt = pre.receipt
                                                                        and		dpr.code = rea.dpr_code
                                                                        and 	dpr.type_detai in ('1','3','4')
                                                                        and		dpr.bill_item not in (4,5,9,97)),0) * 
                                                        case	when	pre.branch = 66
                                                                        then	(	select	max(exc.exchange)
                                                                                                from 	usinsug01.exchange exc
                                                                                                where	exc.usercomp = pre.usercomp 
                                                                                                and 	exc.company = pre.company 
                                                                                                and 	exc.currency = 99
                                                                                                and 	exc.effecdate <= pre.effecdate
                                                                                                and 	(exc.nulldate is null or exc.nulldate > pre.effecdate))
                                                                        else 	1 end VMTCAPIT,
                                                coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                        from	usinsug01.detail_pre dp0
                                                                        where	dp0.usercomp = pre.usercomp
                                                                        and 	dp0.company = pre.company
                                                                        and 	dp0.receipt = pre.receipt
                                                                        and		dp0.certif is not null
                                                                        and		dp0.code = rea.dpr_code
                                                                        and		dp0.type_detai in ('1','3','4')
                                                                        and		dp0.bill_item not in (4,5,9,97)
                                                                        and		coalesce(dp0.premium,0) <> 0),0) + 
                                                        case	when	not exists
                                                                                        (	select	1
                                                                                                from	usinsug01.detail_pre dp0
                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                and 	dp0.company = pre.company
                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                and		dp0.certif = 0
                                                                                                and		dp0.type_detai in ('1','3','4')
                                                                                                and		dp0.bill_item not in (4,5,9,97)
                                                                                                and		coalesce(dp0.premium,0) <> 0)
                                                                        then	coalesce((	select	sum(coalesce(dp1.premium,0))
                                                                                                                from	usinsug01.detail_pre dp1
                                                                                                                where	dp1.usercomp = pre.usercomp
                                                                                                                and 	dp1.company = pre.company
                                                                                                                and 	dp1.receipt = pre.receipt
                                                                                                                and		dp1.certif = 0
                                                                                                                and		dp1.type_detai in ('1','3','4') 
                                                                                                                and		dp1.bill_item not in (4,5,9,97)),0)
                                                                                        *	coalesce((	1 /	nullif((	select	cast(count(distinct dp2.code) as decimal(20))
                                                                                                                                                        from	usinsug01.detail_pre dp2
                                                                                                                                                        where	dp2.usercomp = pre.usercomp
                                                                                                                                                        and 	dp2.company = pre.company
                                                                                                                                                        and 	dp2.receipt = pre.receipt
                                                                                                                                                        and 	dp2.type_detai in ('1','3','4')
                                                                                                                                                        and		dp2.bill_item not in (4,5,9,97)
                                                                                                                                                        and		coalesce(dp2.premium,0) <> 0),0)),0)
                                                                        else 	0 end VMTPREM,
                                                coalesce(rea.rei_commision,0) * 
                                                        (	coalesce((	select	coalesce(coi.share,0)
                                                                                        from	usinsug01.coinsuran coi
                                                                                        where 	coi.usercomp = pre.usercomp
                                                                                        and     coi.company = pre.company
                                                                                        and     coi.certype = pre.certype
                                                                                        and     coi.branch = pre.branch
                                                                                        and     coi.policy = pre.policy
                                                                                        and     coi.effecdate <= pre.effecdate
                                                                                        and     (coi.nulldate is null or coi.nulldate > pre.effecdate)
                                                                                        and 	coalesce(coi.companyc,0) = 1),100) / 100) VMTCOMMD,
                                                rea.dpr_code KRCTPCBT,
                                                coalesce(rea.rei_premium,0) *
                                                        (	coalesce((	select	coalesce(coi.share,0)
                                                                                        from	usinsug01.coinsuran coi
                                                                                        where 	coi.usercomp = pre.usercomp
                                                                                        and     coi.company = pre.company
                                                                                        and     coi.certype = pre.certype
                                                                                        and     coi.branch = pre.branch
                                                                                        and     coi.policy = pre.policy
                                                                                        and     coi.effecdate <= pre.effecdate
                                                                                        and     (coi.nulldate is null or coi.nulldate > pre.effecdate)
                                                                                        and 	coalesce(coi.companyc,0) = 1),100) / 100) VMTRESG,
                                                rea.rei_company DCODRSG,
                                                par.cia DCOMPA,
                                                '' DMARCA, --excluido
                                                '' DCDCORR --excluido
                                from	(	select	pre.ctid pre_id,
                                                                        dpr.code dpr_code,
                                                                        case when coalesce(rei.type,0) not in (0,1,4) then rei.com_reinsu else null end rei_number,
                                                                case    when    t173.codigint in (1,4,9)
                                                                                then    case when t173.codigint = 1 then 1 else rei.com_reinsu end
                                                                                        else    coalesce(ctp.companyc,1) end rei_company,
                                                                        case	when	ctp.ctid is not null
                                                                                        then	case    when 	coalesce(t173.codigint,0) in (1,4,9) --en caso sea el tipo retenci�n o facultativos
                                                                                                then	coalesce(rei.com_reinsu,0)
                                                                                                                        else 	coalesce(ctp.share,0) --porcentaje por compa��as en el caso de no ser retenci�n/facultativo (sub-porcentaje)
                                                                                                                        end
                                                                                        else	case when coalesce(t173.codigint,0) = 1 then 100 else 0 end --caso no exista subdistribuci�n
                                                                                        end porc_commision,
                                                                        coalesce((	sum(coalesce(rei.capital, 0))	/
                                                                                                nullif(	sum(coalesce(case	when	dpr.addsuini in ('1', '3')
                                                                                                                                                then	coalesce(dpr.capital, 0) * 
                                                                                                                                                                case	when	pre.branch = 66
                                                                                                                                                                                then	(	select	max(exc.exchange)
                                                                                                                                                                                                        from 	usinsug01.exchange exc
                                                                                                                                                                                                        where	exc.usercomp = pre.usercomp 
                                                                                                                                                                                                        and 	exc.company = pre.company 
                                                                                                                                                                                                        and 	exc.currency = 99
                                                                                                                                                                                                        and 	exc.effecdate <= pre.effecdate
                                                                                                                                                                                                        and 	(exc.nulldate is null or exc.nulldate > pre.effecdate))
                                                                                                                                                                                else 	1 end
                                                                                                                                                else	0 end, 0)),0)),0) porc_capital,
                                                                        sum(coalesce(dpr.premium,0) + 
                                                                                case	when	not exists
                                                                                                                (	select	1
                                                                                                                        from	usinsug01.detail_pre dp0
                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                        and 	dp0.company = pre.company
                                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                                        and		dp0.certif = 0
                                                                                                                        and		dp0.type_detai in ('1','3','4')
                                                                                                                        and		dp0.bill_item not in (4,5,9,97)
                                                                                                                        and		coalesce(dp0.premium,0) <> 0)
                                                                                                then	coalesce((	select	sum(coalesce(dp1.premium,0))
                                                                                                                                        from	usinsug01.detail_pre dp1
                                                                                                                                        where	dp1.usercomp = pre.usercomp
                                                                                                                                        and 	dp1.company = pre.company
                                                                                                                                        and 	dp1.receipt = pre.receipt
                                                                                                                                        and		dp1.certif = 0
                                                                                                                                        and		dp1.type_detai in ('1','3','4') 
                                                                                                                                        and		dp1.bill_item not in (4,5,9,97)),0)
                                                                                                                *	coalesce((	1 /	nullif((	select	cast(count(*) as decimal(20))
                                                                                                                                                                                from	usinsug01.detail_pre dp2
                                                                                                                                                                                where	dp2.usercomp = pre.usercomp
                                                                                                                                                                                and 	dp2.company = pre.company
                                                                                                                                                                                and 	dp2.receipt = pre.receipt
                                                                                                                                                                                and 	dp2.type_detai in ('1','3','4')
                                                                                                                                                                                and		dp2.bill_item not in (4,5,9,97)
                                                                                                                                                                                and		coalesce(dp2.premium,0) <> 0),0)),0)
                                                                                                else 	0 end
                                                                                * 	case	when	rei.ctid is not null 
                                                                                                        then	coalesce(rei.commissi,0) /100 --distribuci�n reaseguro (tipo / compa��a)
                                                                                                        else	0 end --si no hay reaseguro no aplica la comisi�n de reaseguro
                                                                                *	case	when	ctp.ctid is not null
                                                                                                        then	case    when 	coalesce(t173.codigint,0) in (1,4,9) --en caso sea el tipo retenci�n o facultativos
                                                                                                                then	100
                                                                                                                                        else 	coalesce(ctp.com_rate,0) --porcentaje por compa��as en el caso de no ser retenci�n/facultativo (sub-porcentaje)
                                                                                                                                        end / 100
                                                                                                        else	0 --caso no exista reaseguro/subdistribuci�n
                                                                                                        end
                                                                                *	case	when	ctp.ctid is not null
                                                                                                        then	case    when 	coalesce(t173.codigint,0) in (1,4,9) --en caso sea el tipo retenci�n o facultativos
                                                                                                                then	coalesce(rei.com_reinsu,0)
                                                                                                                                        else 	coalesce(ctp.share,0) --porcentaje por compa��as en el caso de no ser retenci�n/facultativo (sub-porcentaje)
                                                                                                                                        end / 100
                                                                                                        else	0 --caso no exista subdistribuci�n
                                                                                                        end) rei_commision,
                                                                        sum(coalesce(dpr.premium,0) + 
                                                                                case	when	not exists
                                                                                                                (	select	1
                                                                                                                        from	usinsug01.detail_pre dp0
                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                        and 	dp0.company = pre.company
                                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                                        and		dp0.certif = 0
                                                                                                                        and		dp0.type_detai in ('1','3','4')
                                                                                                                        and		dp0.bill_item not in (4,5,9,97)
                                                                                                                        and		coalesce(dp0.premium,0) <> 0)
                                                                                                then	coalesce((	select	sum(coalesce(dp1.premium,0))
                                                                                                                                        from	usinsug01.detail_pre dp1
                                                                                                                                        where	dp1.usercomp = pre.usercomp
                                                                                                                                        and 	dp1.company = pre.company
                                                                                                                                        and 	dp1.receipt = pre.receipt
                                                                                                                                        and		dp1.certif = 0
                                                                                                                                        and		dp1.type_detai in ('1','3','4') 
                                                                                                                                        and		dp1.bill_item not in (4,5,9,97)),0)
                                                                                                                *	coalesce((	1 /	nullif((	select	cast(count(*) as decimal(20))
                                                                                                                                                                                from	usinsug01.detail_pre dp2
                                                                                                                                                                                where	dp2.usercomp = pre.usercomp
                                                                                                                                                                                and 	dp2.company = pre.company
                                                                                                                                                                                and 	dp2.receipt = pre.receipt
                                                                                                                                                                                and 	dp2.type_detai in ('1','3','4')
                                                                                                                                                                                and		dp2.bill_item not in (4,5,9,97)
                                                                                                                                                                                and		coalesce(dp2.premium,0) <> 0),0)),0)
                                                                                                else 	0 end
                                                                                * 	case	when	rei.ctid is not null 
                                                                                                        then	coalesce(rei.share,0) /100 --distribuci�n reaseguro (tipo / compa��a)
                                                                                                        else	1 end --si no hay reaseguro se mantiene el coaseguro origen
                                                                                *	case	when	ctp.ctid is not null
                                                                                                        then	case    when 	coalesce(t173.codigint,0) in (1,4,9) --en caso sea el tipo retenci�n o facultativos
                                                                                                                then	100
                                                                                                                                        else 	coalesce(ctp.share,0) --porcentaje por compa��as en el caso de no ser retenci�n/facultativo (sub-porcentaje)
                                                                                                                                        end / 100
                                                                                                        else	1 --caso no exista subdistribuci�n
                                                                                                        end) rei_premium
                                                        from	usinsug01.premium pre
                                                        join	usinsug01.detail_pre dpr
                                                                        on		dpr.usercomp = pre.usercomp
                                                                        and 	dpr.company = pre.company
                                                                        and 	dpr.receipt = pre.receipt
                                                                        and		dpr.type_detai in ('1','3','4')
                                                                        and		dpr.bill_item not in (4,5,9,97)
                                                                        and		coalesce(dpr.premium,0) <> 0
                                                        left	join usinsug01.reinsuran rei
                                                                        on		rei.usercomp = pre.usercomp
                                                                        and     rei.company = pre.company
                                                                        and     rei.certype = pre.certype
                                                                        and     rei.branch = pre.branch
                                                                        and     rei.policy = pre.policy
                                                                        and     rei.certif = coalesce(dpr.certif,0)
                                                                        and     rei.effecdate <= pre.effecdate
                                                                        and     (rei.nulldate is null or rei.nulldate > pre.effecdate)
                                                        left 	join usinsug01.table173 t173 on t173.codigint = case when coalesce(rei.type,0) in (0,4) then 4 else rei.type end
                                                        left 	join usinsug01.contrproc ctc
                                                                        on		ctc.usercomp = case when coalesce(rei.type,0) not in (0,1,4) then rei.usercomp else null end
                                                                        and     ctc.company = case when coalesce(rei.type,0) not in (0,1,4) then rei.company else null end
                                                                        and     ctc.currency = case when coalesce(rei.type,0) not in (0,1,4) then rei.currency else null end
                                                                        and     ctc.branch = case when coalesce(rei.type,0) not in (0,1,4) then rei.branch else null end
                                                                        and     ctc.type = case when coalesce(rei.type,0) not in (0,1,4) then rei.type else null end
                                                                        and     ctc.number = case when coalesce(rei.type,0) not in (0,1,4) then rei.com_reinsu else null end
                                                        left 	join usinsug01.contr_comp ctp
                                                                        on		ctp.usercomp = ctc.usercomp
                                                                        and     ctp.company = ctc.company
                                                                        and     ctp.branch = ctc.branch
                                                                        and     ctp.currency = ctc.currency
                                                                        and     ctp.number = ctc.number
                                                                        and     ctp.year_contr = ctc.year_contr
                                                                        and     ctp.type = ctc.type
                                                                        and     ctp.nulldate is null
                                                        where 	pre.usercomp = 1
                                                        and 	pre.company = 1
                                                        and     pre.branch = 66 --pre.branch not in (1,66,23)
                                                        and 	cast(pre.effecdate as date) <= '12/31/2020'
                                                        and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                                        and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                                        and 	pre.statusva not in ('2','3')
                                                        group	by 1,2,3,4,5 limit 5) rea
                                join	usinsug01.premium pre on pre.ctid = rea.pre_id
                                join 	(select 'LPG' cia, '-' sep) par on 1 = 1     
                             ) AS TMP        
                             '''   

  DF_LPG_RBRSGRP_INSUNIX = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRSGRP_INSUNIX_LPG).load()

  L_RBRSGRP_VTIME_LPG_SIN_REC_SIN_REASEGURO = f'''
                                               (
                                                  select	'D' as INDDETREC,
                                                                'RBRSGRP' as TABLAIFRS17,
                                                                        pre."NRECEIPT" KRBRECPR,
                                                                        '' DORDRSG, --excluido
                                                                        '' DTPREG, --excluido
                                                                        '' TIOCPROC, --excluido
                                                                        '' TIOCFRM, --excluido
                                                                        '' TIOCTO, --excluido
                                                                        'PVG' KGIORIGM, --excluido
                                                                        '' DTRATADO,
                                                                        0 VTXCAPIT,
                                                                        '' VTXPREM, --descartado
                                                                        0 VTXCOMMD,
                                                                        coalesce((	select	max(case	when	dp0."SADDSUINI" in ('1', '3')
                                                                                                                                        then	coalesce(dp0."NCAPITAL", 0) 
                                                                                                                                        else	0 end)
                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                where	dp0."NRECEIPT" = pre."NRECEIPT"
                                                                                                and		dp0."NDIGIT" = pre."NDIGIT"
                                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                and		dp0."NDET_CODE" =rea.ndet_code),0) VMTCAPIT,
                                                                        coalesce((	select	sum(coalesce(dp0."NPREMIUM", 0) )
                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                where	dp0."NRECEIPT" = pre."NRECEIPT"
                                                                                                and		dp0."NDIGIT" = pre."NDIGIT"
                                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                and		dp0."NDET_CODE" =rea.ndet_code),0) VMTPREM,
                                                                        0 VMTCOMMD, --no aplica
                                                                        rea.ndet_code KRCTPCBT,
                                                                        rea.npremium_100  * 
                                                                                        (coalesce((	select 	"NSHARE"
                                                                                                                from	usvtimg01."COINSURAN" coi
                                                                                                                where	coi."SCERTYPE" = pre."SCERTYPE"
                                                                                                                and     coi."NBRANCH" = pre."NBRANCH"
                                                                                                                and     coi."NPRODUCT" = pre."NPRODUCT"
                                                                                                                and     coi."NPOLICY" = pre."NPOLICY"
                                                                                                                and 	coi."NCOMPANY" = 1
                                                                                                                and     CAST(coi."DEFFECDATE" AS DATE) <= CAST(PRE."DEFFECDATE" AS DATE)
                                                                                                                and     (coi."DNULLDATE" is null or CAST(coi."DNULLDATE" AS DATE) > CAST(PRE."DEFFECDATE" AS DATE))),100)/100) VMTRESG, --coaseguro sobre el 100% de retenci�n
                                                                        1 DCODRSG, --LP el �nico en este caso
                                                                        par.cia DCOMPA,
                                                                        '' DMARCA, --excluido
                                                                        '' DCDCORR --excluido
                                                        from	(	select 	pre.ctid pre_id,
                                                                                                dpr."NDET_CODE" ndet_code,
                                                                                                sum(coalesce(DPR."NCAPITAL" * case when dpr."SADDSUINI" in ('1','3') then 1 else 0 end,0)) dpr_cap,
                                                                                                sum(coalesce(case when DPR."STYPE_DETAI" in ('1','2','4','6') then dpr."NPREMIUM" else 0 end,0)) dpr_con,
                                                                                                sum(coalesce(DPR."NCOMMISION",0)) dpr_com,
                                                                                                sum(coalesce(dpr."NPREMIUM",0)) npremium_100
                                                                                FROM 	(	select 	distinct
                                                                                                                        pre.ctid pre_id,
                                                                                                                        pol.ctid pol_id
                                                                                                        from 	usvtimg01."PREMIUM" pre
                                                                                                        join 	usvtimg01."DETAIL_PRE" dpr
                                                                                                                        on		dpr."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                        and 	dpr."NDIGIT" = PRE."NDIGIT"
                                                                                                                        and		dpr."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                        and		dpr."NBILL_ITEM" not in (4,5,9,97)
                                                                                                        join 	usvtimg01."POLICY" POL
                                                                                                                        on 		pol."NBRANCH" = pre."NBRANCH"
                                                                                                                        and		pol."NPOLICY" = pre."NPOLICY"
                                                                                                                        and		pol."NPRODUCT" = pre."NPRODUCT"
                                                                                                        where	not exists
                                                                                                                        (	select 	1
                                                                                                                                from	usvtimg01."PREMIUM_CE" pr0
                                                                                                                                where 	pr0."NRECEIPT" = PRE."NRECEIPT")
                                                                                                        --and		PRE."NDIGIT" = 0 --and pre."NBRANCH" = 57 --and pre."NRECEIPT" = 218557555
                                                                                                        and		pre."NDIGIT" = 0
                                                                                                        --and pre."NBRANCH" = 57 and pre."NRECEIPT" = 218557555
                                                                                                        AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2020'
                                                                                                        AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2020')
                                                                                                        AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2020')
                                                                                                        AND 	PRE."SSTATUSVA" NOT IN ('2','3')
                                                                                                        and		0 =
                                                                                                                        case	when	not	("SPOLITYPE" = '2' AND pre."NBRANCH" = 57 AND pre."NPRODUCT" = 1)
                                                                                                                                        then	case	when 	exists
                                                                                                                                                                                        (	select	1
                                                                                                                                                                                                from	usvtimg01."REINSURAN" rei
                                                                                                                                                                                                where	rei."SCERTYPE" = pre."SCERTYPE" and rei."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                and		rei."NPRODUCT" = pre."NPRODUCT" and rei."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                and		rei."NCERTIF" = case when "SPOLITYPE" = '3' then 0 else coalesce(pre."NCERTIF",0) end
                                                                                                                                                                                                and		rei."NMODULEC" = dpr."NMODULEC" and rei."NBRANCH_REI" = dpr."NBRANCH_REI"
                                                                                                                                                                                                and		cast(rei."DEFFECDATE" as date) <= cast(pre."DEFFECDATE" as date)
                                                                                                                                                                                                and		(rei."DNULLDATE" IS NULL OR cast(rei."DNULLDATE" as date) > cast(pre."DEFFECDATE" as date)))
                                                                                                                                                                        then	case when "SPOLITYPE" = '3' then 2 else	1 end
                                                                                                                                                                        else	0 end
                                                                                                                                        when	("SPOLITYPE" = '2' and pre."NBRANCH" = 57 and pre."NPRODUCT" = 1)
                                                                                                                                        then	case	when 	exists
                                                                                                                                                                                        (	select	1
                                                                                                                                                                                                from	usvtimg01."REINSURAN" rei
                                                                                                                                                                                                where	rei."SCERTYPE" = pre."SCERTYPE" and rei."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                and		rei."NPRODUCT" = pre."NPRODUCT" and rei."NPOLICY" = pre."NPOLICY" and rei."NCERTIF" = coalesce(pre."NCERTIF",0)
                                                                                                                                                                                                and		rei."NMODULEC" = dpr."NMODULEC" and rei."NBRANCH_REI" = dpr."NBRANCH_REI"
                                                                                                                                                                                                and		cast(rei."DEFFECDATE" as date) <= cast(pre."DEFFECDATE" as date)
                                                                                                                                                                                                and		(rei."DNULLDATE" IS NULL OR cast(rei."DNULLDATE" as date) > cast(pre."DEFFECDATE" as date)))
                                                                                                                                                                        then	1
                                                                                                                                                                        else	case	when 	exists
                                                                                                                                                                                                                        (	select	1
                                                                                                                                                                                                                                from	usvtimg01."REINSURAN" rei
                                                                                                                                                                                                                                where	rei."SCERTYPE" = pre."SCERTYPE" and rei."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                                and		rei."NPRODUCT" = pre."NPRODUCT" and rei."NPOLICY" = pre."NPOLICY" and rei."NCERTIF" = 0
                                                                                                                                                                                                                                and		rei."NMODULEC" = dpr."NMODULEC" and rei."NBRANCH_REI" = dpr."NBRANCH_REI"
                                                                                                                                                                                                                                and		cast(rei."DEFFECDATE" as date) <= cast(pre."DEFFECDATE" as date)
                                                                                                                                                                                                                                and		(rei."DNULLDATE" IS NULL OR cast(rei."DNULLDATE" as date) > cast(pre."DEFFECDATE" as date)))
                                                                                                                                                                                                        then	2
                                                                                                                                                                                                        else	0 end end
                                                                                                                                        else	0 end
                                                                                                        ) pr0
                                                                                join	usvtimg01."PREMIUM" pre on pre.ctid = pr0.pre_id
                                                                                join 	usvtimg01."DETAIL_PRE" dpr
                                                                                                on		dpr."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                and 	dpr."NDIGIT" = PRE."NDIGIT"
                                                                                                and		dpr."STYPE_DETAI" in ('1','2','4','6')
                                                                                                and		dpr."NBILL_ITEM" not in (4,5,9,97)
                                                                                group 	by 1,2) rea
                                                        join	usvtimg01."PREMIUM" pre on pre.ctid = rea.pre_id
                                                        join 	(select 'LPG' cia, '-' sep) par on 1 = 1
                                                        --32.443s (todos los ramos) SIN REC, SIN REASEGURO
                                               ) AS TMP
                                               '''
  DF_LPG_RBRSGRP_VTIME_SIN_REC_SIN_REASEGURO = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRSGRP_VTIME_LPG_SIN_REC_SIN_REASEGURO).load()
                          
  L_RBRSGRP_VTIME_LPG_SIN_REC_CON_REASEGURO = f'''
                                                 (
                                                    select	'D' as INDDETREC,
                                                                'RBRSGRP' as TABLAIFRS17,
                                                                pre."NRECEIPT" KRBRECPR,
                                                                '' DORDRSG, --excluido
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                '' TIOCFRM, --excluido
                                                                '' TIOCTO, --excluido
                                                                'PVG' KGIORIGM, --excluido
                                                                coalesce( rea.rei_number,0) DTRATADO,
                                                                coalesce(rea.porc_capital,0) * 100 VTXCAPIT,
                                                                '' VTXPREM, --descartado
                                                                coalesce(rea.rei_commision,0) VTXCOMMD,
                                                                coalesce((	select	max(case	when	dp0."SADDSUINI" in ('1', '3')
                                                                                                                                then	coalesce(dp0."NCAPITAL", 0) 
                                                                                                                                else	0 end)
                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                        where	dp0."NRECEIPT" = pre."NRECEIPT"
                                                                                        and		dp0."NDIGIT" = pre."NDIGIT"
                                                                                        and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                        and		dp0."NDET_CODE" =rea.ndet_code),0) VMTCAPIT,
                                                                coalesce((	select	sum(coalesce(dp0."NPREMIUM", 0) )
                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                        where	dp0."NRECEIPT" = pre."NRECEIPT"
                                                                                        and		dp0."NDIGIT" = pre."NDIGIT"
                                                                                        and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                        and		dp0."NDET_CODE" =rea.ndet_code),0) VMTPREM,
                                                                coalesce(rea.rei_commision,0) * 
                                                                                (coalesce((	select 	"NSHARE"
                                                                                                        from	usvtimg01."COINSURAN" coi
                                                                                                        where	coi."SCERTYPE" = pre."SCERTYPE"
                                                                                                        and     coi."NBRANCH" = pre."NBRANCH"
                                                                                                        and     coi."NPRODUCT" = pre."NPRODUCT"
                                                                                                        and     coi."NPOLICY" = pre."NPOLICY"
                                                                                                        and 	coi."NCOMPANY" = 1
                                                                                                        and     CAST(coi."DEFFECDATE" AS DATE) <= CAST(PRE."DEFFECDATE" AS DATE)
                                                                                                        and     (coi."DNULLDATE" is null or CAST(coi."DNULLDATE" AS DATE) > CAST(PRE."DEFFECDATE" AS DATE))),100)/100) VMTCOMMD,
                                                                rea.ndet_code KRCTPCBT,
                                                                coalesce(rea.rei_npremium,0) *
                                                                                (coalesce((	select 	"NSHARE"
                                                                                                        from	usvtimg01."COINSURAN" coi
                                                                                                        where	coi."SCERTYPE" = pre."SCERTYPE"
                                                                                                        and     coi."NBRANCH" = pre."NBRANCH"
                                                                                                        and     coi."NPRODUCT" = pre."NPRODUCT"
                                                                                                        and     coi."NPOLICY" = pre."NPOLICY"
                                                                                                        and 	coi."NCOMPANY" = 1
                                                                                                        and     CAST(coi."DEFFECDATE" AS DATE) <= CAST(PRE."DEFFECDATE" AS DATE)
                                                                                                        and     (coi."DNULLDATE" is null or CAST(coi."DNULLDATE" AS DATE) > CAST(PRE."DEFFECDATE" AS DATE))),100)/100) VMTRESG,
                                                                rea.rei_ncompany DCODRSG,
                                                                par.cia DCOMPA,
                                                                '' DMARCA, --excluido
                                                                '' DCDCORR --excluido
                                                        from 	(	select	dpr.pre_id,
                                                                                                dpr.npremium_100,
                                                                                                dpr.ndet_code,
                                                                                                case when coalesce(rei."NTYPE_REIN",0) not in (0,1,4,11,12) then rei."NNUMBER" else null end rei_number,
                                                                                                case	when	dpr.flag_rea <> 0	
                                                                                                                then	case	when	rei."NTYPE_REIN" = 1
                                                                                                                                                then	1
                                                                                                                                                else	case	when	rei."NTYPE_REIN" not in (4,11,12)
                                                                                                                                                        then	pcr."NCOMPANY"  --tabla contrmaster no existe en vt
                                                                                                                                                        else	rei."NCOMPANY" end end
                                                                                                                else	1 end rei_ncompany,
                                                                                                coalesce((sum(coalesce(rei."NCAPITAL", 0)) /
                                                                                                                        nullif(sum(coalesce((	select	sum(case	when	dp0."SADDSUINI" in ('1', '3')
                                                                                                                                                                                                                then	coalesce(dp0."NCAPITAL", 0) 
                                                                                                                                                                                                                else	0 end)
                                                                                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                                                                        where	dp0."NRECEIPT" = pre."NRECEIPT"
                                                                                                                                                                        and		dp0."NDIGIT" = pre."NDIGIT"
                                                                                                                                                                        and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                                                        and		dp0."NBRANCH_REI" = dpr.nbranch_rei),0)),0)),0) porc_capital,
                                                                                                sum(dpr.npremium_100 *
                                                                                                                coalesce(	case	when	dpr.flag_rea <> 0
                                                                                                                                                        then	case	when	rei."NTYPE_REIN" in (11,12)
                                                                                                                                                                                        then	rei."NCOMMISSI" 
                                                                                                                                                                                        else	0 end
                                                                                                                                                        else	100 end,0) /100 *
                                                                                                                coalesce(	case	when	dpr.flag_rea <> 0
                                                                                                                                                        then	case	when rei."NTYPE_REIN" in (1,4,11,12)
                                                                                                                                                                                        then 100
                                                                                                                                                                                        else pcr."NCOMISION" end --tabla contrmaster no existe en vt (se desconoce si el campo de comisiones es tal)
                                                                                                                                                        else	100 end,0) /100) rei_commision,
                                                                                                sum((dpr.npremium_100 * 
                                                                                                                coalesce(	case	when	dpr.flag_rea <> 0
                                                                                                                                                        then	case	when	rei."NTYPE_REIN" in (11,12)
                                                                                                                                                                                        then	rei."NPREM_SHARE"
                                                                                                                                                                                        else	rei."NSHARE" end
                                                                                                                                                        else	100 end,0) /100 *
                                                                                                                coalesce(	case	when	dpr.flag_rea <> 0
                                                                                                                                                        then	case	when rei."NTYPE_REIN" in (1,4,11,12)
                                                                                                                                                                                        then 100
                                                                                                                                                                                        else pcr."NSHARE" end --tabla contrmaster no existe en vt
                                                                                                                                                        else	100 end,0) /100)) rei_npremium
                                                                                from	(	select 	pre.ctid pre_id,
                                                                                                                        dpr."NBRANCH_REI" nbranch_rei,
                                                                                                                        dpr."NDET_CODE" ndet_code,
                                                                                                                        case	when	not exists
                                                                                                                                                        (   SELECT  1
                                                                                                                                                                from	usvtimg01."REINSURAN" REI
                                                                                                                                                                WHERE   REI."SCERTYPE" = pre."SCERTYPE" and	REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                and		REI."NPRODUCT" =pre."NPRODUCT" and REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                and		REI."NCERTIF" = coalesce(pre."NCERTIF",0) and REI."NMODULEC" = dpr."NMODULEC"
                                                                                                                                                                and		REI."NBRANCH_REI" = dpr."NBRANCH_REI" and	CAST(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                                        then 	0
                                                                                                                                        else	case	when	NOT ("SPOLITYPE" = '2' AND pre."NBRANCH" = 57 AND pre."NPRODUCT" = 1)
                                                                                                                                                                        THEN    CASE    WHEN    EXISTS
                                                                                                                                                                        (   select	1
                                                                                                                                                                                                                                from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                where 	REI."SCERTYPE" = pre."SCERTYPE" AND REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                                and		REI."NPRODUCT" =pre."NPRODUCT" AND REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                                                and		REI."NCERTIF" = case when "SPOLITYPE" = '3' then 0 else coalesce(pre."NCERTIF",0) end
                                                                                                                                                                                                                                and		REI."NMODULEC" = dpr."NMODULEC" AND REI."NBRANCH_REI" = dpr."NBRANCH_REI"
                                                                                                                                                                                                                                and		CAST(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                                                                                                        then	case 	when	"SPOLITYPE" = '3'
                                                                                                                                                                                                then	2
                                                                                                                                                                                                else	1 end
                                                                                                                                                                                                        else 	0 END 
                                                                                                                                                                        when	("SPOLITYPE" = '2' AND pre."NBRANCH" = 57 AND pre."NPRODUCT" = 1)
                                                                                                                                                                        THEN    CASE    WHEN    EXISTS
                                                                                                                                                                        (   select	1
                                                                                                                                                                                                                                from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                where	REI."SCERTYPE" = pre."SCERTYPE" AND REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                                and		REI."NPRODUCT" =pre."NPRODUCT" AND REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                                                and		REI."NCERTIF" = coalesce(pre."NCERTIF",0)
                                                                                                                                                                                                                                and		REI."NMODULEC" = dpr."NMODULEC" AND REI."NBRANCH_REI" = dpr."NBRANCH_REI"
                                                                                                                                                                                                                                and		cast(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                                                                                                        then	1
                                                                                                                                                                                                        else	case	when	EXISTS
                                                                                                                                                                                                                                                        (	select	1
                                                                                                                                                                                                                                                                from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                                                where	REI."SCERTYPE" = pre."SCERTYPE" AND REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                                                                and		REI."NPRODUCT" = pre."NPRODUCT" AND REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                                                                                and		REI."NCERTIF" = 0 AND  REI."NMODULEC" = dpr."NMODULEC"
                                                                                                                                                                                                                                                                and		REI."NBRANCH_REI" = dpr."NBRANCH_REI"
                                                                                                                                                                                                                                                                and		cast(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                                                                                                                                        then	2
                                                                                                                                                                                                                                        else	0 END END
                                                                                                                                                                        else	0 END end flag_rea,
                                                                                                                        sum(coalesce(DPR."NCAPITAL" * case when dpr."SADDSUINI" in ('1','3') then 1 else 0 end,0)) dpr_cap,
                                                                                                                        sum(coalesce(case when DPR."STYPE_DETAI" in ('1','2','4','6') then dpr."NPREMIUM" else 0 end,0)) dpr_con,
                                                                                                                        sum(coalesce(DPR."NCOMMISION",0)) dpr_com,
                                                                                                                        sum(coalesce(dpr."NPREMIUM",0)) npremium_100
                                                                                                        FROM 	(	select 	distinct
                                                                                                                                                pre.ctid pre_id,
                                                                                                                                                pol.ctid pol_id
                                                                                                                                from 	usvtimg01."PREMIUM" pre
                                                                                                                                join 	usvtimg01."DETAIL_PRE" dpr
                                                                                                                                                on		dpr."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                                and 	dpr."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                and		dpr."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                                and		dpr."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                join 	usvtimg01."POLICY" POL
                                                                                                                                                on 		pol."NBRANCH" = pre."NBRANCH"
                                                                                                                                                and		pol."NPOLICY" = pre."NPOLICY"
                                                                                                                                                and		pol."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                where	not exists
                                                                                                                                                (	select 	1
                                                                                                                                                        from	usvtimg01."PREMIUM_CE" pr0
                                                                                                                                                        where 	pr0."NRECEIPT" = PRE."NRECEIPT")
                                                                                                                                --and		PRE."NDIGIT" = 0 --and pre."NBRANCH" = 57 --and pre."NRECEIPT" = 218557555
                                                                                                                                and		pre."NDIGIT" = 0
                                                                                                                                --and pre."NBRANCH" = 57 and pre."NRECEIPT" = 218557555
                                                                                                                                AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2020'
                                                                                                                                AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2020')
                                                                                                                                AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2020')
                                                                                                                                AND 	PRE."SSTATUSVA" NOT IN ('2','3')
                                                                                                                                and		0 <>
                                                                                                                                                case	when	not	("SPOLITYPE" = '2' AND pre."NBRANCH" = 57 AND pre."NPRODUCT" = 1)
                                                                                                                                                                then	case	when 	exists
                                                                                                                                                                                                                (	select	1
                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" rei
                                                                                                                                                                                                                        where	rei."SCERTYPE" = pre."SCERTYPE" and rei."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                        and		rei."NPRODUCT" = pre."NPRODUCT" and rei."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                                        and		rei."NCERTIF" = case when "SPOLITYPE" = '3' then 0 else coalesce(pre."NCERTIF",0) end
                                                                                                                                                                                                                        and		rei."NMODULEC" = dpr."NMODULEC" and rei."NBRANCH_REI" = dpr."NBRANCH_REI"
                                                                                                                                                                                                                        and		cast(rei."DEFFECDATE" as date) <= cast(pre."DEFFECDATE" as date)
                                                                                                                                                                                                                        and		(rei."DNULLDATE" IS NULL OR cast(rei."DNULLDATE" as date) > cast(pre."DEFFECDATE" as date)))
                                                                                                                                                                                                then	case when "SPOLITYPE" = '3' then 2 else	1 end
                                                                                                                                                                                                else	0 end
                                                                                                                                                                when	("SPOLITYPE" = '2' and pre."NBRANCH" = 57 and pre."NPRODUCT" = 1)
                                                                                                                                                                then	case	when 	exists
                                                                                                                                                                                                                (	select	1
                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" rei
                                                                                                                                                                                                                        where	rei."SCERTYPE" = pre."SCERTYPE" and rei."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                        and		rei."NPRODUCT" = pre."NPRODUCT" and rei."NPOLICY" = pre."NPOLICY" and rei."NCERTIF" = coalesce(pre."NCERTIF",0)
                                                                                                                                                                                                                        and		rei."NMODULEC" = dpr."NMODULEC" and rei."NBRANCH_REI" = dpr."NBRANCH_REI"
                                                                                                                                                                                                                        and		cast(rei."DEFFECDATE" as date) <= cast(pre."DEFFECDATE" as date)
                                                                                                                                                                                                                        and		(rei."DNULLDATE" IS NULL OR cast(rei."DNULLDATE" as date) > cast(pre."DEFFECDATE" as date)))
                                                                                                                                                                                                then	1
                                                                                                                                                                                                else	case	when 	exists
                                                                                                                                                                                                                                                (	select	1
                                                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" rei
                                                                                                                                                                                                                                                        where	rei."SCERTYPE" = pre."SCERTYPE" and rei."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                                                        and		rei."NPRODUCT" = pre."NPRODUCT" and rei."NPOLICY" = pre."NPOLICY" and rei."NCERTIF" = 0
                                                                                                                                                                                                                                                        and		rei."NMODULEC" = dpr."NMODULEC" and rei."NBRANCH_REI" = dpr."NBRANCH_REI"
                                                                                                                                                                                                                                                        and		cast(rei."DEFFECDATE" as date) <= cast(pre."DEFFECDATE" as date)
                                                                                                                                                                                                                                                        and		(rei."DNULLDATE" IS NULL OR cast(rei."DNULLDATE" as date) > cast(pre."DEFFECDATE" as date)))
                                                                                                                                                                                                                                then	2
                                                                                                                                                                                                                                else	0 end end
                                                                                                                                                                else	0 end
                                                                                                                                ) pr0
                                                                                                        join	usvtimg01."PREMIUM" pre on pre.ctid = pr0.pre_id
                                                                                                        join 	usvtimg01."DETAIL_PRE" dpr
                                                                                                                        on		dpr."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                        and 	dpr."NDIGIT" = PRE."NDIGIT"
                                                                                                                        and		dpr."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                        and		dpr."NBILL_ITEM" not in (4,5,9,97)
                                                                                                        join 	usvtimg01."POLICY" POL on pol.ctid = pr0.pol_id
                                                                                                        group 	by 1,2,3,4 limit 10) dpr
                                                                                join	usvtimg01."PREMIUM" pre on pre.ctid = dpr.pre_id
                                                                                join	usvtimg01."REINSURAN" REI
                                                                                                ON		REI."SCERTYPE" = '2'
                                                                                                AND     REI."NBRANCH" = pre."NBRANCH"
                                                                                                AND     REI."NPRODUCT" = pre."NPRODUCT"
                                                                                                AND     REI."NPOLICY" = pre."NPOLICY"
                                                                                                AND     REI."NCERTIF" = case flag_rea when 1 then coalesce(pre."NCERTIF",0) when 2 then 0 end
                                                                                                AND     REI."NBRANCH_REI" = dpr.nbranch_rei
                                                                                                AND     cast(REI."DEFFECDATE" as date) <= cast(pre."DEFFECDATE" as date)
                                                                                                AND     (REI."DNULLDATE" IS NULL OR cast(REI."DNULLDATE" as date) > cast(pre."DEFFECDATE" as date))
                                                                                                AND     REI."NSHARE" > 0
                                                                                left 	JOIN 	usvtimg01."CONTRMASTER" cnm
                                                                                                ON		CNM."NNUMBER" = REI."NNUMBER"
                                                                                                AND     CNM."NBRANCH" = REI."NBRANCH_REI"
                                                                                                AND     CNM."NTYPE" = REI."NTYPE_REIN"
                                                                                LEFT 	JOIN 	usvtimg01."PART_CONTR" PCR
                                                                                                ON		PCR."NTYPE_REL" = CNM."NTYPE_REL"
                                                                                                AND     PCR."NNUMBER" = CNM."NNUMBER"
                                                                                                AND     PCR."NBRANCH" = CNM."NBRANCH"
                                                                                                AND     cast(PCR."DSTARTDATE" as date) <= cast(CNM."DSTARTDATE" as date)
                                                                                                AND     (PCR."DNULLDATE" IS NULL OR cast(PCR."DNULLDATE" as date) > cast(CNM."DSTARTDATE" as date))
                                                                                                AND     PCR."NTYPE" = CNM."NTYPE"
                                                                                group 	by 1,2,3,4,5 limit 10) rea
                                                        join	usvtimg01."PREMIUM" pre on pre.ctid = rea.pre_id
                                                        join 	(select 'LPG' cia, '-' sep) par on 1 = 1
                                                        --5m21s (todos los ramos) EN DESARROLLO reaseguro
                                                        --11.218 desarrollo 3 limit 10 
                                                 ) AS TMP        
                                                 '''
  DF_LPG_RBRSGRP_VTIME_SIN_REC_CON_REASEGURO = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRSGRP_VTIME_LPG_SIN_REC_CON_REASEGURO).load()
            
  L_RBRSGRP_VTIME_LPG_CON_REC_SIN_REASEGURO = f'''
                                                (
                                                  select	'D' as INDDETREC,
                                                                'RBRSGRP' as TABLAIFRS17,
                                                                        pre."NRECEIPT" KRBRECPR,
                                                                        '' DORDRSG, --excluido
                                                                        '' DTPREG, --excluido
                                                                        '' TIOCPROC, --excluido
                                                                        '' TIOCFRM, --excluido
                                                                        '' TIOCTO, --excluido
                                                                        'PVG' KGIORIGM, --excluido
                                                                        '' DTRATADO,
                                                                        0 VTXCAPIT,
                                                                        '' VTXPREM, --descartado
                                                                        0 VTXCOMMD,
                                                                        coalesce((	select	max(case	when	dp0."SADDSUINI" in ('1', '3')
                                                                                                                                        then	coalesce(dp0."NCAPITAL", 0) 
                                                                                                                                        else	0 end)
                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                where	dp0."NRECEIPT" = pre."NRECEIPT"
                                                                                                and		dp0."NDIGIT" = pre."NDIGIT"
                                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                and		dp0."NDET_CODE" =rea.ndet_code),0) VMTCAPIT,
                                                                        coalesce((	select	sum(coalesce(dp0."NPREMIUM", 0) )
                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                where	dp0."NRECEIPT" = pre."NRECEIPT"
                                                                                                and		dp0."NDIGIT" = pre."NDIGIT"
                                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                and		dp0."NDET_CODE" =rea.ndet_code),0) VMTPREM,
                                                                        0 VMTCOMMD, --no aplica
                                                                        rea.ndet_code KRCTPCBT,
                                                                        rea.npremium_100  * 
                                                                                        (coalesce((	select 	"NSHARE"
                                                                                                                from	usvtimg01."COINSURAN" coi
                                                                                                                where	coi."SCERTYPE" = pre."SCERTYPE"
                                                                                                                and     coi."NBRANCH" = pre."NBRANCH"
                                                                                                                and     coi."NPRODUCT" = pre."NPRODUCT"
                                                                                                                and     coi."NPOLICY" = pre."NPOLICY"
                                                                                                                and 	coi."NCOMPANY" = 1
                                                                                                                and     CAST(coi."DEFFECDATE" AS DATE) <= CAST(PRE."DEFFECDATE" AS DATE)
                                                                                                                and     (coi."DNULLDATE" is null or CAST(coi."DNULLDATE" AS DATE) > CAST(PRE."DEFFECDATE" AS DATE))),100)/100) VMTRESG, --coaseguro sobre el 100% de retenci�n
                                                                        1 DCODRSG, --LP el �nico en este caso
                                                                        par.cia DCOMPA,
                                                                        '' DMARCA, --excluido
                                                                        '' DCDCORR --excluido
                                                        from	(	select 	pre.ctid pre_id,
                                                                                                dpr."NDET_CODE" ndet_code,
                                                                                                sum(coalesce(DPR."NCAPITAL" * case when dpr."SADDSUINI" in ('1','3') then 1 else 0 end,0)) dpr_cap,
                                                                                                sum(coalesce(case when DPR."STYPE_DETAI" in ('1','2','4','6') then dpr."NPREMIUM" else 0 end,0)) dpr_con,
                                                                                                sum(coalesce(DPR."NCOMMISION",0)) dpr_com,
                                                                                                sum(coalesce(dpr."NPREMIUM",0)) npremium_100
                                                                                from 	(	select 	distinct pre.ctid pre_id
                                                                                                        from	usvtimg01."PREMIUM" pre
                                                                                                        join	usvtimg01."POLICY" pol
                                                                                                                        on		pol."NBRANCH" = pre."NBRANCH"
                                                                                                                        and		pol."NPOLICY" = pre."NPOLICY"
                                                                                                                        and		pol."NPRODUCT" = pre."NPRODUCT"
                                                                                                        join 	usvtimg01."PREMIUM_CE" prc
                                                                                                                        on		prc."SCERTYPE" = pre."SCERTYPE"
                                                                                                                        and		prc."NBRANCH" = pre."NBRANCH"
                                                                                                                        and		prc."NPRODUCT" = pre."NPRODUCT"
                                                                                                                        and		prc."NPOLICY" = pre."NPOLICY"
                                                                                                                        and		prc."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                        and 	prc."NCERTIF" is not null
                                                                                                                        and		prc."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                        and		prc."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                        and 	prc."NDIGIT" = PRE."NDIGIT"
                                                                                                                        and		0 =
                                                                                                                                        case
                                                                                                                                        when	NOT ("SPOLITYPE" = '2' AND pre."NBRANCH" = 57 AND pre."NPRODUCT" = 1)
                                                                                                                                        then	case	when	EXISTS
                                                                                                                                                                                        (	SELECT  1
                                                                                                                                                                                                FROM	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                WHERE	REI."SCERTYPE" = pre."SCERTYPE" 
                                                                                                                                                                                                AND     REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                and		REI."NPRODUCT" =pre."NPRODUCT" 
                                                                                                                                                                                                AND 	REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                and		REI."NCERTIF" = case when "SPOLITYPE" = '3' then 0 else prc."NCERTIF"end
                                                                                                                                                                                                and		REI."NMODULEC" = prc."NMODULEC" AND REI."NBRANCH_REI" = prc."NBRANCH_REI"
                                                                                                                                                                                                and		CAST(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                                                                        then	case	when	"SPOLITYPE" = '3'
                                                                                                                                                                        then	2
                                                                                                                                                                        else	1 end
                                                                                                                                                                        else 	0 end 
                                                                                                                                        when	("SPOLITYPE" = '2' AND pre."NBRANCH" = 57 AND pre."NPRODUCT" = 1)
                                                                                                                                        then 	case	when	EXISTS
                                                                                                                                                                                        (	SELECT	1
                                                                                                                                                                                                FROM	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                WHERE	REI."SCERTYPE" = pre."SCERTYPE" 
                                                                                                                                                                                                AND 	REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                AND		REI."NPRODUCT" =pre."NPRODUCT" 
                                                                                                                                                                                                AND 	REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                AND		REI."NCERTIF" = coalesce(prc."NCERTIF",coalesce(pre."NCERTIF",0))
                                                                                                                                                                                                AND		REI."NMODULEC" = prc."NMODULEC" 
                                                                                                                                                                                                AND 	REI."NBRANCH_REI" = prc."NBRANCH_REI"
                                                                                                                                                                                                AND		cast(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                                                                AND		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                                                                        then    1
                                                                                                                                                                        else 	case 	when	EXISTS
                                                                                                                                                                                                                        (   SELECT  1
                                                                                                                                                                                                                                from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                where	REI."SCERTYPE" = pre."SCERTYPE" AND REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                                and		REI."NPRODUCT" = pre."NPRODUCT" AND REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                                                and		REI."NCERTIF" = 0 AND  REI."NMODULEC" = prc."NMODULEC"
                                                                                                                                                                                                                                and		REI."NBRANCH_REI" = prc."NBRANCH_REI"
                                                                                                                                                                                                                                and		cast(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                                                                THEN    2
                                                                                                                                                                ELSE    0 END END
                                                                                                                                ELSE    0 END
                                                                                                        WHERE 	PRE."NDIGIT" = 0 
                                                                                                        AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2021'
                                                                                                        AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2021')
                                                                                                        AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2021')
                                                                                                        AND 	PRE."SSTATUSVA" NOT IN ('2','3') limit 5) pr0
                                                                                join	usvtimg01."PREMIUM" pre on pre.ctid = pr0.pre_id
                                                                                join 	usvtimg01."DETAIL_PRE" dpr
                                                                                                on		dpr."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                and 	dpr."NDIGIT" = PRE."NDIGIT"
                                                                                                and		dpr."STYPE_DETAI" in ('1','2','4','6')
                                                                                                and		dpr."NBILL_ITEM" not in (4,5,9,97)
                                                                                group 	by 1,2 limit 5) rea
                                                        join	usvtimg01."PREMIUM" pre on pre.ctid = rea.pre_id
                                                        join 	(select 'LPG' cia, '-' sep) par on 1 = 1
                                                        --11m23s (todos los ramos) CON PRC SIN REASEGURO (Desarrollo)
                                                        --48.351s (todos los ramos) CON PRC SIN REASEGURO (Producci�n)             
                                                )AS TMP
                                                '''
  DF_LPG_RBRSGRP_VTIME_CON_REC_SIN_REASEGURO = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRSGRP_VTIME_LPG_SIN_REC_CON_REASEGURO).load()
  
  DF_LPG_RBRSGRP_VTIME = DF_LPG_RBRSGRP_VTIME_SIN_REC_SIN_REASEGURO.union(DF_LPG_RBRSGRP_VTIME_SIN_REC_CON_REASEGURO).union(DF_LPG_RBRSGRP_VTIME_CON_REC_SIN_REASEGURO)

  DF_RBRSGRP = DF_LPG_RBRSGRP_INSUNIX.union(DF_LPG_RBRSGRP_VTIME)

  return DF_RBRSGRP