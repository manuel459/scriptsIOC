def get_data(GLUE_CONTEXT, CONNECTION):

  L_RBRECIN_INSUNIX_LPG_OTROS_RAMOS = f'''
                                        (
                                             
                                                select	        'D' INDDETREC,
                                                                'RBRECIN' TABLAIFRS17,
                                                                '' PK,
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                cast(clh.operdate as varchar) TIOCFRM,
                                                                '' TIOCTO, --excluido
                                                                '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                                                '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                                                '' KRCTPRCI, --no disponible
                                                                'PIG' KGIORIGM, --excluido
                                                                cast(cla.branch as varchar) KGCRAMO,
                                                                cast(cla.policy as varchar) DNUMAPO,
                                                                cast(cla.certif as varchar) DNMCERT,
                                                                cast(cla.claim as varchar) DNUMSIN,
                                                                cast(cla.num_case as varchar) DNUMSSIN, --no disponible
                                                                '' DNUMPENS, --excluido
                                                                cla.claim || '-' || clh.transac DNUMREC,
                                                                '' DNMAGRE, --no disponible
                                                                '' NSAGREG, --excluido
                                                                '' NSEQSIN, --excluido
                                                                coalesce(cast(cla.occurdat as varchar),'') DANOSIN,
                                                                coalesce(cast(clh.operdate as varchar),'') TEMISSAO,
                                                                '' TINICIO, --no disponible
                                                                '' TTERMO, --no disponible
                                                                '' TESTADO, --no disponible
                                                                coalesce((	select	cast(max(cl0.operdate) as varchar)
                                                                                        from	usinsug01.claim_his cl0
                                                                                        where 	cl0.claim = clh.claim
                                                                                        and		cl0.transac = 	
                                                                                                        (	select 	max(csp.transac)
                                                                                                                from	usinsug01.claim_pay_sap csp
                                                                                                                where	csp.claim = cl0.claim 
                                                                                                                and 	csp.transac_pay = clh.transac)),'') TPGCOB,
                                                                '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIALBE
                                                                '' KRCESPRI, --no disponible
                                                                '' KRCESTRI, --no disponible
                                                                '' KRCMOSTI, --excluido
                                                                cast(cla.moneda_cod as varchar) KRCMOEDA,
                                                                '' VCAMBIO, --no disponible
                                                                cast(coalesce(	case    when    clh.currency = cla.moneda_cod
                                                                                then    clh.amount
                                                                                when 	clh.currency <> cla.moneda_cod
                                                                                then    case    when    cla.moneda_cod = 1
                                                                                                then    clh.amount *
                                                                                                        case    when    clh.currency = 2
                                                                                                                then    (   select  max(clh0.exchange)
                                                                                                                        from    usinsug01.claim_his clh0
                                                                                                                        where 	clh0.claim = clh.claim
                                                                                                                        and     clh0.transac =
                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                        from    usinsug01.claim_his clh1
                                                                                                                                        where clh1.claim = clh.claim
                                                                                                                                        and     clh1.transac <= clh.transac
                                                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                                                else    1 end
                                                                                                when	cla.moneda_cod = 2
                                                                                                then    clh.amount /
                                                                                                        case    when    clh.currency = 1
                                                                                                                then    (   select  max(clh0.exchange)
                                                                                                                        from    usinsug01.claim_his clh0
                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                        and     clh0.transac =
                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                        from    usinsug01.claim_his clh1
                                                                                                                                        where	clh1.claim = clh.claim
                                                                                                                                        and     clh1.transac <= clh.transac
                                                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                                                else    1 end
                                                                                                else    0 end
                                                                                else    0 end,0) as numeric(12,2)) VMTTOTRI,
                                                                '' VMTIVA, --no disponible
                                                                '' VMTIRSRT, --no disponible
                                                                '' VTXIRSRT, --no disponible
                                                                '' VMTLIQRC, --excluido
                                                                cast(coalesce(	case    when    clh.currency = cla.moneda_cod
                                                                                then    clh.amount
                                                                                when 	clh.currency <> cla.moneda_cod
                                                                                then    case    when    cla.moneda_cod = 1
                                                                                                then    clh.amount *
                                                                                                        case    when    clh.currency = 2
                                                                                                                then    (   select  max(clh0.exchange)
                                                                                                                        from    usinsug01.claim_his clh0
                                                                                                                        where 	clh0.claim = clh.claim
                                                                                                                        and     clh0.transac =
                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                        from    usinsug01.claim_his clh1
                                                                                                                                        where clh1.claim = clh.claim
                                                                                                                                        and     clh1.transac <= clh.transac
                                                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                                                else    1 end
                                                                                                when	cla.moneda_cod = 2
                                                                                                then    clh.amount /
                                                                                                        case    when    clh.currency = 1
                                                                                                                then    (   select  max(clh0.exchange)
                                                                                                                        from    usinsug01.claim_his clh0
                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                        and     clh0.transac =
                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                        from    usinsug01.claim_his clh1
                                                                                                                                        where	clh1.claim = clh.claim
                                                                                                                                        and     clh1.transac <= clh.transac
                                                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                                                else    1 end
                                                                                                else    0 end
                                                                                else    0 end,0)
                                                                        * (case when cla.bussityp = '2' then 100 else cla.share_coa end/100) as numeric(12,2)) VMTCOSEG,
                                                                cast(coalesce(	case    when    clh.currency = cla.moneda_cod
                                                                                then    clh.amount
                                                                                when 	clh.currency <> cla.moneda_cod
                                                                                then    case    when    cla.moneda_cod = 1
                                                                                                then    clh.amount *
                                                                                                        case    when    clh.currency = 2
                                                                                                                then    (   select  max(clh0.exchange)
                                                                                                                        from    usinsug01.claim_his clh0
                                                                                                                        where 	clh0.claim = clh.claim
                                                                                                                        and     clh0.transac =
                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                        from    usinsug01.claim_his clh1
                                                                                                                                        where clh1.claim = clh.claim
                                                                                                                                        and     clh1.transac <= clh.transac
                                                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                                                else    1 end
                                                                                                when	cla.moneda_cod = 2
                                                                                                then    clh.amount /
                                                                                                        case    when    clh.currency = 1
                                                                                                                then    (   select  max(clh0.exchange)
                                                                                                                        from    usinsug01.claim_his clh0
                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                        and     clh0.transac =
                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                        from    usinsug01.claim_his clh1
                                                                                                                                        where	clh1.claim = clh.claim
                                                                                                                                        and     clh1.transac <= clh.transac
                                                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                                                else    1 end
                                                                                                else    0 end
                                                                                else    0 end,0)
                                                                        * (case when cla.bussityp = '2' then 100 else cla.share_coa end/100)
                                                                        * (cla.share_rea/100) as numeric(12,2)) VMTRESSG,
                                                                cla.branch || '-' || cla.product || '-' || coalesce(cla.sub_product,0) KABPRODT,
                                                                '' KRCFMPGI, --excluido
                                                                '' KMEDCB, --excluido
                                                                '' KMEDPG, --excluido
                                                                '' DUSRORI, --excluido
                                                                '' DUSRAPR, --excluido
                                                                case	when cla.bussityp = '1' and (cla.share_coa = 100 and cla.share_rea = 100) then '1' --LP compa��a l�der, siniestro al 100%
                                                                                when cla.bussityp = '1' and (cla.share_coa <> 100 or cla.share_rea <> 100) then '2' --LP compa��a l�der, siniestro repartido
                                                                                when cla.bussityp = '2' and cla.share_rea = 100 then '3' --LP compa��a no l�der, siniestro al 100%
                                                                                when cla.bussityp = '2' and cla.share_rea <> 100 then '4' --LP compa��a no l�der, siniestro repartido
                                                                                else '0' end KRCTPCSG,
                                                                '' DCODENPG, --excluido
                                                                '' DCODENRC, --excluido
                                                                '' VMFAT, --no disponible
                                                                'LPG' DCOMPA,
                                                                '' DMARCA, --excluido
                                                                '' TDAPROV, --excluido
                                                                '' DNMACJUD, --excluido
                                                                '' KRCTPERP, --excluido
                                                                '' KRBRECIN_MP, --excluido
                                                                '' TMIGPARA, --excluido
                                                                '' KRBRECIN_MD, --excluido
                                                                '' TMIGDE, --excluido
                                                                'LPG -' || cla.claim KSBSIN,
                                                                cla.branch || '-' || cla.product || '-' || cla.policy || '-' || cla.certif KABAPOL,
                                                                '' KABAPOL_EFT, --excluido
                                                                coalesce(cast(cla.date_origi as varchar),'') TINICIOA,
                                                                cast(coalesce(cla.branch_bal,0) as varchar) KGCRAMO_SAP,
                                                                '' KCBCONTA_AE, --excluido
                                                                '' KCBCONTA_PD, --excluido
                                                                '' KCBCONTA_FN, --excluido
                                                                '' KRCCATFIS, --excluido
                                                                '' KRCTPRET, --excluido
                                                                '' KRCZNRET, --excluido
                                                                '' KRCTPRND, --excluido
                                                                '' TCRIASO, --excluido
                                                                '' DNUCHEQ, --excluido
                                                                '' DNIB, --excluido
                                                                cla.client KEBENTID_TO,
                                                                '' TCONTAB, --excluido
                                                                coalesce(cast(cla.occurdat as varchar),'') DANOABER,
                                                                '' DINDIBNR, --excluido
                                                                '' DINDDESD, --excluido
                                                                '' DINDASVI, --excluido
                                                                '' KRCTRESI, --excluido
                                                                '' VMTSUIRS, --excluido
                                                                '' DNISS, --excluido
                                                                '' DPRESERV, --excluido
                                                                '' KRCTPDCP, --no disponible
                                                                '' DNUMDOC, --excluido
                                                                '' TDOCPGPR, --excluido
                                                                '' DREGACUM, --excluido
                                                                '' VMTCNTSS, --excluido
                                                                '' VMTRETSS, --excluido
                                                                '' VMTSUJSS, --excluido
                                                                '' KRCTPPGP, --excluido
                                                                '' DCDRTIRS, --excluido
                                                                '' DNIF, --excluido
                                                                '' DARQUIVO, --excluido
                                                                '' TARQUIVO, --excluido
                                                                '' VMTINDSTR, --excluido
                                                                '' VMTPRVERC, --excluido
                                                                '' VMTPRVINI, --excluido
                                                                '' VMTINDCTR --excluido
                                                from    (   select	case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                        and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                then    cla.claim
                                                                                else    null end claim,
                                                                                        cla.usercomp,
                                                                                        cla.company,
                                                                                        cla.branch,
                                                                                        cla.policy,
                                                                                        cla.certif,
                                                                                        cla.occurdat,
                                                                                        case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                        and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                        then	coalesce(coalesce(
                                                                                                                                                (	select	max(sclient_vig)
                                                                                                                                                        from	usinsug01.wbstblclidepequi
                                                                                                                                                        where	sclient_old = 
                                                                                                                                                                        (	select	evi.scod_vt
                                                                                                                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                                                                                                                where	evi.scod_inx = cla.client)),
                                                                                                                                                (	select	evi.scod_vt
                                                                                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                                                                                        where	evi.scod_inx = cla.client)),
                                                                                                                                cla.client)
                                                                                                        else	null end client,
                                                                                        pol.product,
                                                                                        pol.bussityp,
                                                                                        case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                        and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                then    case	coalesce(cla.certif,0)
                                                                                                                                        when	0
                                                                                                                                        then	pol.date_origi
                                                                                                                                        else	(	select	cer.date_origi
                                                                                                                                                                from 	usinsug01.certificat cer
                                                                                                                                                                where	cer.usercomp = cla.usercomp 
                                                                                                                                                                and 	cer.company = cla.company
                                                                                                                                                                and 	cer.certype = pol.certype
                                                                                                                                                                and 	cer.branch = cla.branch 
                                                                                                                                                                and 	cer.policy = cla.policy
                                                                                                                                                                and		cer.certif = cla.certif)
                                                                                                                                        end
                                                                                                        else	null end date_origi,
                                                                                        case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                        and 2 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                then    coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                                from    usinsug01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                                (   select  max(cpl.currency)
                                                                                                                from    usinsug01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0)
                                                                                else    0 end moneda_cod,
                                                                                        case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                        and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                        then	case	pol.bussityp
                                                                                                                                        when	'2'
                                                                                                                                        then 	100 - coalesce(pol.leadshare,0)
                                                                                                                                        when	'3'
                                                                                                                                        then 	null
                                                                                                                                        else	coalesce((	select	coi.share
                                                                                                                                                                                from	usinsug01.coinsuran coi
                                                                                                                                                                                where   coi.usercomp = cla.usercomp and coi.company = cla.company and coi.certype = pol.certype
                                                                                                                                                                                and     coi.branch = cla.branch and coi.policy = cla.policy and coi.effecdate <= cla.occurdat
                                                                                                                                                                                and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                                                                                and 	coalesce(coi.companyc,0) in (1,12)),100) end 
                                                                                                        else	0 end share_coa,
                                                                                        case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                        and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                        then	coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                                                                from    usinsug01.reinsuran rei
                                                                                                                                                where   rei.usercomp = cla.usercomp and rei.company = cla.company and rei.certype = pol.certype
                                                                                                                                                and		rei.branch = cla.branch and rei.policy = cla.policy and rei.certif = cla.certif
                                                                                                                                                and		rei.effecdate <= cla.occurdat and (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                                                                and     coalesce(rei.type,0) = 1),100)
                                                                                                        else	0 end share_rea,
                                                                                        case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                        and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                        then    coalesce((	select 	acc.branch_bal
                                                                                                                                                from	usinsug01.acc_autom2 acc
                                                                                                                                                where	ctid =
                                                                                                                                                                coalesce(
                                                                                                                                                        (   select  min(ctid) --b�squeda normal en acc_autom2(ramo;producto;concepto 1 para todos, excepto incendio)
                                                                                                                                                        from    usinsug01.acc_autom2 abe
                                                                                                                                                        where   abe.branch = cla.branch
                                                                                                                                                        and 	abe.product = pol.product
                                                                                                                                                        and 	abe.concept_fac = 1),
                                                                                                                                                        (   select  min(ctid) --b�squeda igual a anterior, pero en caso el producto no exista en acc_autom2 (error LP)
                                                                                                                                                        from    usinsug01.acc_autom2 abe
                                                                                                                                                        where   abe.branch = cla.branch
                                                                                                                                                        and 	abe.concept_fac = 1))),0)
                                                                                                        else	null end branch_bal,
                                                                                        case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                        and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                        then	(	select	sub_product
                                                                                                                                from	usinsug01.pol_subproduct
                                                                                                                                where	usercomp = cla.usercomp
                                                                                                                                and 	company = cla.company
                                                                                                                                and		certype = pol.certype
                                                                                                                                and		branch = cla.branch
                                                                                                                                and		policy = cla.policy
                                                                                                                                and		product = pol.product) 
                                                                                                        else	0 end sub_product,
                                                                                        case 	when 	(cla.staclaim <> '6' and cla.branch = 66 and pol.certype = '2')
                                                                                                        then	(	select 	min(num_case)
                                                                                                                                from	usinsug01.claim_case
                                                                                                                                where 	usercomp = cla.usercomp
                                                                                                                                and 	company = cla.company
                                                                                                                                and 	claim = cla.claim)
                                                                                                        else	0 end num_case,
                                                                                        par.fecha
                                                                from    usinsug01.policy pol
                                                                join	(select cast('12/31/2015' as date) fecha) par on 1 = 1 --ejecutar al a�o 2021 (2015 es para pruebas)
                                                                join	usinsug01.claim cla
                                                                        on		cla.usercomp = pol.usercomp
                                                                        and     cla.company = pol.company
                                                                        and     cla.branch = pol.branch
                                                                        and     cla.policy = pol.policy
                                                                        and		cla.branch <> 1 --no alterar (el query es excluyente para este ramo)
                                                                        and     exists
                                                                                                        (   select  1
                                                                                        from    (	select	cast(codigint as varchar(2)) operation
                                                                                                                                        from	usinsug01.table140
                                                                                                                                        where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                                                                                join	usinsug01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                        where   coalesce (clh.claim,0) = cla.claim
                                                                                        and     clh.operdate >= par.fecha)) cla
                                                join 	usinsug01.claim_his clh
                                                                on		clh.claim = cla.claim
                                                                and		clh.operdate <= cla.fecha
                                                join	(	select	cast(codigint as varchar(2)) operation
                                                                        from	usinsug01.table140
                                                                        where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl on trim(clh.oper_type) = tcl.operation      
                                        ) AS TMP          
                                        '''   
   
  DF_LPG_INSUNIX_OTROS_RAMOS = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBRECIN_INSUNIX_LPG_OTROS_RAMOS).load()
  
  L_RBRECIN_INSUNIX_LPG_INCENDIOS = f'''
                                        (
                                                select	        'D' INDDETREC,
                                                                'RBRECIN' TABLAIFRS17,
                                                                '' PK,
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                cast(clh.operdate as varchar) TIOCFRM,
                                                                '' TIOCTO, --excluido
                                                                '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                                                '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                                                '' KRCTPRCI, --no disponible
                                                                'PIG' KGIORIGM, --excluido
                                                                cast(clh.branch as varchar) KGCRAMO,
                                                                cast(clh.policy as varchar) DNUMAPO,
                                                                cast(clh.certif as varchar) DNMCERT,
                                                                cast(clh.claim as varchar) DNUMSIN,
                                                                '0' DNUMSSIN,
                                                                '' DNUMPENS, --excluido
                                                                clh.claim || '-' || clh.transac DNUMREC,
                                                                '' DNMAGRE, --no disponible
                                                                '' NSAGREG, --excluido
                                                                '' NSEQSIN, --excluido
                                                                coalesce(cast(clh.occurdat as varchar),'') DANOSIN,
                                                                coalesce(cast(clh.operdate as varchar),'') TEMISSAO,
                                                                '' TINICIO, --no disponible
                                                                '' TTERMO, --no disponible
                                                                '' TESTADO, --no disponible
                                                                coalesce((	select	cast(max(cl0.operdate) as varchar)
                                                                                        from	usinsug01.claim_his cl0
                                                                                        where 	cl0.claim = clh.claim
                                                                                        and		cl0.transac = 	
                                                                                                        (	select 	max(csp.transac)
                                                                                                                from	usinsug01.claim_pay_sap csp
                                                                                                                where	csp.claim = cl0.claim 
                                                                                                                and 	csp.transac_pay = clh.transac)),'') TPGCOB,
                                                                '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIALBE
                                                                '' KRCESPRI, --no disponible
                                                                '' KRCESTRI, --no disponible
                                                                '' KRCMOSTI, --excluido
                                                                cast(clh.moneda_cod as varchar) KRCMOEDA,
                                                                '' VCAMBIO, --no disponible
                                                                cast(clh.monto_trans as numeric(12,2)) VMTTOTRI,
                                                                '' VMTIVA, --no disponible
                                                                '' VMTIRSRT, --no disponible
                                                                '' VTXIRSRT, --no disponible
                                                                '' VMTLIQRC, --excluido
                                                                cast(	coalesce  (   case  when    clm.currency = clh.moneda_cod
                                                                                        then    clm.amount
                                                                                        else	case    when    clh.moneda_cod = 1
                                                                                                        then    clm.amount *
                                                                                                                case    when    clm.currency = 2
                                                                                                                        then    clh.clh_exchange
                                                                                                                        else    1 end
                                                                                                        when	clh.moneda_cod = 2
                                                                                                        then    clm.amount /
                                                                                                                case    when    clm.currency = 1
                                                                                                                        then    clh.clh_exchange
                                                                                                                        else	1 end
                                                                                                        else    0 end
                                                                                        end, 0) * (case when clh.bussityp = '2' then 100 else clh.share_coa end/100) as numeric(12,2)) VMTCOSEG,
                                                                cast(	coalesce  (   case  when    clm.currency = clh.moneda_cod
                                                                                        then    clm.amount
                                                                                        else	case    when    clh.moneda_cod = 1
                                                                                                        then    clm.amount *
                                                                                                                case    when    clm.currency = 2
                                                                                                                        then    clh.clh_exchange
                                                                                                                        else    1 end
                                                                                                        when	clh.moneda_cod = 2
                                                                                                        then    clm.amount /
                                                                                                                case    when    clm.currency = 1
                                                                                                                        then    clh.clh_exchange
                                                                                                                        else	1 end
                                                                                                        else    0 end
                                                                                        end, 0)
                                                                                        * (case when clh.bussityp = '2' then 100 else clh.share_coa end/100)
                                                                                        * (clh.share_rea/100) as numeric(12,2)) VMTRESSG,
                                                                clh.branch || '-' || clh.product || '- 0' KABPRODT,
                                                                '' KRCFMPGI, --excluido
                                                                '' KMEDCB, --excluido
                                                                '' KMEDPG, --excluido
                                                                '' DUSRORI, --excluido
                                                                '' DUSRAPR, --excluido
                                                                case	when clh.bussityp = '1' and (clh.share_coa = 100 and clh.share_rea = 100) then '1' --LP compa��a l�der, siniestro al 100%
                                                                                when clh.bussityp = '1' and (clh.share_coa <> 100 or clh.share_rea <> 100) then '2' --LP compa��a l�der, siniestro repartido
                                                                                when clh.bussityp = '2' and clh.share_rea = 100 then '3' --LP compa��a no l�der, siniestro al 100%
                                                                                when clh.bussityp = '2' and clh.share_rea <> 100 then '4' --LP compa��a no l�der, siniestro repartido
                                                                                else '0' end KRCTPCSG,
                                                                '' DCODENPG, --excluido
                                                                '' DCODENRC, --excluido
                                                                '' VMFAT, --no disponible
                                                                'LPG' DCOMPA,
                                                                '' DMARCA, --excluido
                                                                '' TDAPROV, --excluido
                                                                '' DNMACJUD, --excluido
                                                                '' KRCTPERP, --excluido
                                                                '' KRBRECIN_MP, --excluido
                                                                '' TMIGPARA, --excluido
                                                                '' KRBRECIN_MD, --excluido
                                                                '' TMIGDE, --excluido
                                                                'LPG -' || clh.claim KSBSIN,
                                                                clh.branch || '-' || clh.product || '-' || clh.policy || '-' || clh.certif KABAPOL,
                                                                '' KABAPOL_EFT, --excluido
                                                                coalesce(cast(clh.date_origi as varchar),'') TINICIOA,
                                                                coalesce((	select	cast(gco.bill_item as varchar)
                                                                                        from	usinsug01.gen_cover gco
                                                                                        where	gco.ctid = 
                                                                                                        coalesce((	select  max(ctid)
                                                                                                                from	usinsug01.gen_cover
                                                                                                                where	usercomp = clh.usercomp and company = clh.company and branch = clh.branch
                                                                                                                and		product = clh.product and currency = clh.moneda_cod and modulec = clh.modulec_cla
                                                                                                                and		cover =	clm.cover and effecdate <= clh.occurdat and (nulldate is null or nulldate > clh.occurdat)
                                                                                                                and     statregt <> '4'), --variaci�n 3 reg. v�lido
                                                                                                                (   select  max(ctid)
                                                                                                                from	usinsug01.gen_cover
                                                                                                                where	usercomp = clh.usercomp and company = clh.company and branch = clh.branch
                                                                                                                and		product = clh.product and currency = clh.moneda_cod
                                                                                                                and		cover = clm.cover and effecdate <= clh.occurdat and (nulldate is null or nulldate > clh.occurdat)
                                                                                                                and     statregt <> '4'))),'0') KGCRAMO_SAP,
                                                                '' KCBCONTA_AE, --excluido
                                                                '' KCBCONTA_PD, --excluido
                                                                '' KCBCONTA_FN, --excluido
                                                                '' KRCCATFIS, --excluido
                                                                '' KRCTPRET, --excluido
                                                                '' KRCZNRET, --excluido
                                                                '' KRCTPRND, --excluido
                                                                '' TCRIASO, --excluido
                                                                '' DNUCHEQ, --excluido
                                                                '' DNIB, --excluido
                                                                clh.client KEBENTID_TO,
                                                                '' TCONTAB, --excluido
                                                                coalesce(cast(clh.occurdat as varchar),'') DANOABER,
                                                                '' DINDIBNR, --excluido
                                                                '' DINDDESD, --excluido
                                                                '' DINDASVI, --excluido
                                                                '' KRCTRESI, --excluido
                                                                '' VMTSUIRS, --excluido
                                                                '' DNISS, --excluido
                                                                '' DPRESERV, --excluido
                                                                '' KRCTPDCP, --no disponible
                                                                '' DNUMDOC, --excluido
                                                                '' TDOCPGPR, --excluido
                                                                '' DREGACUM, --excluido
                                                                '' VMTCNTSS, --excluido
                                                                '' VMTRETSS, --excluido
                                                                '' VMTSUJSS, --excluido
                                                                '' KRCTPPGP, --excluido
                                                                '' DCDRTIRS, --excluido
                                                                '' DNIF, --excluido
                                                                '' DARQUIVO, --excluido
                                                                '' TARQUIVO, --excluido
                                                                '' VMTINDSTR, --excluido
                                                                '' VMTPRVERC, --excluido
                                                                '' VMTPRVINI, --excluido
                                                                '' VMTINDCTR --excluido
                                                from    (   select  cla.claim,
                                                                                        cla.usercomp,
                                                                                        cla.company,
                                                                                        cla.branch,
                                                                                        cla.policy,
                                                                                        cla.certif,
                                                                                        cla.occurdat,
                                                                                        cla.client,
                                                                                        cla.product,
                                                                                        cla.bussityp,
                                                                                        cla.moneda_cod,
                                                                                        cla.share_coa,
                                                                                        cla.share_rea,
                                                                                        cla.date_origi,
                                                                                        cla.modulec_cla,
                                                                clh.transac,
                                                                clh.oper_type,
                                                                clh.operdate,
                                                                case    when    moneda_cod = 1
                                                                        then    case    when    clh.currency = 2
                                                                                        then    (   select  max(clh0.exchange)
                                                                                                        from    usinsug01.claim_his clh0
                                                                                                        where	clh0.claim = clh.claim
                                                                                                        and     clh0.transac =
                                                                                                                (   select  max(clh1.transac)
                                                                                                                from    usinsug01.claim_his clh1
                                                                                                                where	clh1.claim = clh.claim
                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                        else 1 end
                                                                        when 	moneda_cod = 2
                                                                        then    case    when    clh.currency = 1
                                                                                        then    (   select  max(clh0.exchange)
                                                                                                        from    usinsug01.claim_his clh0
                                                                                                        where 	clh0.claim = clh.claim
                                                                                                        and     clh0.transac =
                                                                                                                (   select  max(clh1.transac)
                                                                                                                from    usinsug01.claim_his clh1
                                                                                                                where	clh1.claim = clh.claim
                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                        else    1 end
                                                                        else    0 end clh_exchange,
                                                                                        coalesce    (   case    when    clh.currency = cla.moneda_cod
                                                                                        then    clh.amount
                                                                                        when 	clh.currency <> cla.moneda_cod
                                                                                        then    case    when    cla.moneda_cod = 1
                                                                                                        then    clh.amount *
                                                                                                                case    when    clh.currency = 2
                                                                                                                        then    (   select  max(clh0.exchange)
                                                                                                                                        from    usinsug01.claim_his clh0
                                                                                                                                        where 	clh0.claim = clh.claim
                                                                                                                                        and     clh0.transac =
                                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                                from    usinsug01.claim_his clh1
                                                                                                                                                where clh1.claim = clh.claim
                                                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                                                        else    1 end
                                                                                                        when	cla.moneda_cod = 2
                                                                                                        then    clh.amount /
                                                                                                                case    when    clh.currency = 1
                                                                                                                        then    (   select  max(clh0.exchange)
                                                                                                                                        from    usinsug01.claim_his clh0
                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                        and     clh0.transac =
                                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                                from    usinsug01.claim_his clh1
                                                                                                                                                where	clh1.claim = clh.claim
                                                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                                                        else    1 end
                                                                                                        else    0 end
                                                                                        else    0 end,0) monto_trans
                                                        from    (   select  case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                        then    cla.claim
                                                                                        else    null end claim,
                                                                                                                cla.usercomp,
                                                                                                                cla.company,
                                                                                                                cla.branch,
                                                                                                                cla.policy,
                                                                                                                cla.certif,
                                                                                                                cla.occurdat,
                                                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                                then	coalesce(coalesce(
                                                                                                                                                                        (	select	max(sclient_vig)
                                                                                                                                                                                from	usinsug01.wbstblclidepequi
                                                                                                                                                                                where	sclient_old = 
                                                                                                                                                                                                (	select	evi.scod_vt
                                                                                                                                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                                                                                                                                        where	evi.scod_inx = cla.client)),
                                                                                                                                                                        (	select	evi.scod_vt
                                                                                                                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                                                                                                                where	evi.scod_inx = cla.client)),
                                                                                                                                                        cla.client)
                                                                                                                                else	null end client,
                                                                                                                pol.product,
                                                                                                                pol.bussityp,
                                                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                        then    case	coalesce(cla.certif,0)
                                                                                                                                                                when	0
                                                                                                                                                                then	pol.date_origi
                                                                                                                                                                else	(	select	cer.date_origi
                                                                                                                                                                                        from 	usinsug01.certificat cer
                                                                                                                                                                                        where	cer.usercomp = cla.usercomp 
                                                                                                                                                                                        and 	cer.company = cla.company
                                                                                                                                                                                        and 	cer.certype = pol.certype
                                                                                                                                                                                        and 	cer.branch = cla.branch 
                                                                                                                                                                                        and 	cer.policy = cla.policy
                                                                                                                                                                                        and		cer.certif = cla.certif)
                                                                                                                                                                end
                                                                                                                                else	null end date_origi,
                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                        then    coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                                        from    usinsug01.curren_pol cpl
                                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                                (   select  max(cpl.currency)
                                                                                                                        from    usinsug01.curren_pol cpl
                                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0)
                                                                                        else    0 end moneda_cod,
                                                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                                then	case	pol.bussityp
                                                                                                                                                                when	'2'
                                                                                                                                                                then 	100 - coalesce(pol.leadshare,0)
                                                                                                                                                                when	'3'
                                                                                                                                                                then 	null
                                                                                                                                                                else	coalesce((	select	coi.share
                                                                                                                                                                                                        from	usinsug01.coinsuran coi
                                                                                                                                                                                                        where   coi.usercomp = cla.usercomp and coi.company = cla.company and coi.certype = pol.certype
                                                                                                                                                                                                        and     coi.branch = cla.branch and coi.policy = cla.policy and coi.effecdate <= cla.occurdat
                                                                                                                                                                                                        and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                                                                                                        and 	coalesce(coi.companyc,0) in (1,12)),100) end 
                                                                                                                                else	0 end share_coa,
                                                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                                then	coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                                                                                        from    usinsug01.reinsuran rei
                                                                                                                                                                        where   rei.usercomp = cla.usercomp and rei.company = cla.company and rei.certype = pol.certype
                                                                                                                                                                        and		rei.branch = cla.branch and rei.policy = cla.policy and rei.certif = cla.certif
                                                                                                                                                                        and		rei.effecdate <= cla.occurdat and (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                                                                                        and     coalesce(rei.type,0) = 1),100)
                                                                                                                                else	0 end share_rea,
                                                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                                then	coalesce((	select  min(coalesce(modulec,0))
                                                                                                                                                        from    usinsug01.modules
                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                        and		branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                        and     coalesce(modulec,0) > 0),0)
                                                                                                                                else	null end modulec_cla,
                                                                                                                par.fecha
                                                                        from    usinsug01.policy pol
                                                                        join	(select cast('12/31/2015' as date) fecha) par on 1 = 1 --ejecutar al a�o 2021 (2015 es para pruebas)
                                                                        join	usinsug01.claim cla
                                                                                        on		cla.usercomp = pol.usercomp
                                                                                        and     cla.company = pol.company
                                                                                        and     cla.branch = pol.branch
                                                                                        and     cla.policy = pol.policy
                                                                                        and		cla.branch = 1 --no alterar (el query es exclusivo para este ramo)
                                                                                        and     exists
                                                                                                                                (   select  1
                                                                                                from    (	select	cast(codigint as varchar(2)) operation
                                                                                                                                                                from	usinsug01.table140
                                                                                                                                                                where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                                                                                                        join	usinsug01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                                where   coalesce (clh.claim,0) = cla.claim
                                                                                                and     clh.operdate >= par.fecha)) cla
                                                                join 	usinsug01.claim_his clh
                                                                                on		clh.claim = cla.claim
                                                                                and		clh.operdate <= cla.fecha
                                                                join	(	select	cast(codigint as varchar(2)) operation
                                                                                                                from	usinsug01.table140
                                                                                                                where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl on trim(clh.oper_type) = tcl.operation) clh
                                                join 	usinsug01.cl_m_cover clm
                                                on		clm.usercomp = clh.usercomp
                                                and 	clm.company = clh.company
                                                and 	clm.claim = clh.claim
                                                and 	clm.movement = clh.transac     
                                        ) AS TMP
                                        '''
  DF_LPG_INSUNIX_INCENDIO = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBRECIN_INSUNIX_LPG_INCENDIOS).load()
  

  DF_LPG_INSUNIX = DF_LPG_INSUNIX_OTROS_RAMOS.union(DF_LPG_INSUNIX_INCENDIO)
         
  L_RBRECIN_INSUNIX_LPV = f'''
                             (  
                                        select	        'D' INDDETREC,
                                                        'RBRECIN' TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG, --excluido
                                                        '' TIOCPROC, --excluido
                                                        cast(clh.operdate as varchar) TIOCFRM,
                                                        '' TIOCTO, --excluido
                                                        '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                                        '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                                        '' KRCTPRCI, --no disponible
                                                        'PIV' KGIORIGM, --excluido
                                                        cast(cla.branch as varchar) KGCRAMO,
                                                        cast(cla.policy as varchar) DNUMAPO,
                                                        cast(cla.certif as varchar) DNMCERT,
                                                        cast(cla.claim as varchar) DNUMSIN,
                                                        '' DNUMSSIN, --no disponible
                                                        '' DNUMPENS, --excluido
                                                        cla.claim || '-' || clh.transac DNUMREC,
                                                        '' DNMAGRE, --no disponible
                                                        '' NSAGREG, --excluido
                                                        '' NSEQSIN, --excluido
                                                        coalesce(cast(cla.occurdat as varchar),'') DANOSIN,
                                                        coalesce(cast(clh.operdate as varchar),'') TEMISSAO,
                                                        '' TINICIO, --no disponible
                                                        '' TTERMO, --no disponible
                                                        '' TESTADO, --no disponible
                                                        /*		
                                                        coalesce((	select	cast(max(cl0.operdate) as varchar)
                                                                                from	usinsuv01.claim_his cl0
                                                                                where 	cl0.claim = clh.claim
                                                                                and		cl0.transac = 	
                                                                                                (	select 	max(csp.transac)
                                                                                                        from	usinsuv01.claim_pay_sap csp
                                                                                                        where	csp.claim = cl0.claim 
                                                                                                        and 	csp.transac_pay = clh.transac)),'')
                                                        */ '' TPGCOB,
                                                        '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIALBE
                                                        '' KRCESPRI, --no disponible
                                                        '' KRCESTRI, --no disponible
                                                        '' KRCMOSTI, --excluido
                                                        cast(cla.moneda_cod as varchar) KRCMOEDA,
                                                        '' VCAMBIO, --no disponible
                                                        cast(coalesce(	case    when    clh.currency = cla.moneda_cod
                                                                        then    clh.amount
                                                                        when 	clh.currency <> cla.moneda_cod
                                                                        then    case    when    cla.moneda_cod = 1
                                                                                        then    clh.amount *
                                                                                                case    when    clh.currency = 2
                                                                                                        then    (   select  max(clh0.exchange)
                                                                                                                from    usinsuv01.claim_his clh0
                                                                                                                where 	clh0.claim = clh.claim
                                                                                                                and     clh0.transac =
                                                                                                                        (   select  max(clh1.transac)
                                                                                                                                from    usinsuv01.claim_his clh1
                                                                                                                                where clh1.claim = clh.claim
                                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                                        else    1 end
                                                                                        when	cla.moneda_cod = 2
                                                                                        then    clh.amount /
                                                                                                case    when    clh.currency = 1
                                                                                                        then    (   select  max(clh0.exchange)
                                                                                                                from    usinsuv01.claim_his clh0
                                                                                                                where	clh0.claim = clh.claim
                                                                                                                and     clh0.transac =
                                                                                                                        (   select  max(clh1.transac)
                                                                                                                                from    usinsuv01.claim_his clh1
                                                                                                                                where	clh1.claim = clh.claim
                                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                                        else    1 end
                                                                                        else    0 end
                                                                        else    0 end,0) as numeric(12,2)) VMTTOTRI,
                                                        '' VMTIVA, --no disponible
                                                        '' VMTIRSRT, --no disponible
                                                        '' VTXIRSRT, --no disponible
                                                        '' VMTLIQRC, --excluido
                                                        cast(coalesce(	case    when    clh.currency = cla.moneda_cod
                                                                        then    clh.amount
                                                                        when 	clh.currency <> cla.moneda_cod
                                                                        then    case    when    cla.moneda_cod = 1
                                                                                        then    clh.amount *
                                                                                                case    when    clh.currency = 2
                                                                                                        then    (   select  max(clh0.exchange)
                                                                                                                from    usinsuv01.claim_his clh0
                                                                                                                where 	clh0.claim = clh.claim
                                                                                                                and     clh0.transac =
                                                                                                                        (   select  max(clh1.transac)
                                                                                                                                from    usinsuv01.claim_his clh1
                                                                                                                                where clh1.claim = clh.claim
                                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                                        else    1 end
                                                                                        when	cla.moneda_cod = 2
                                                                                        then    clh.amount /
                                                                                                case    when    clh.currency = 1
                                                                                                        then    (   select  max(clh0.exchange)
                                                                                                                from    usinsuv01.claim_his clh0
                                                                                                                where	clh0.claim = clh.claim
                                                                                                                and     clh0.transac =
                                                                                                                        (   select  max(clh1.transac)
                                                                                                                                from    usinsuv01.claim_his clh1
                                                                                                                                where	clh1.claim = clh.claim
                                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                                        else    1 end
                                                                                        else    0 end
                                                                        else    0 end,0)
                                                                * (case when cla.bussityp = '2' then 100 else share_coa end/100) as numeric(12,2)) VMTCOSEG,
                                                        cast(coalesce(	case    when    clh.currency = cla.moneda_cod
                                                                        then    clh.amount
                                                                        when 	clh.currency <> cla.moneda_cod
                                                                        then    case    when    cla.moneda_cod = 1
                                                                                        then    clh.amount *
                                                                                                case    when    clh.currency = 2
                                                                                                        then    (   select  max(clh0.exchange)
                                                                                                                from    usinsuv01.claim_his clh0
                                                                                                                where 	clh0.claim = clh.claim
                                                                                                                and     clh0.transac =
                                                                                                                        (   select  max(clh1.transac)
                                                                                                                                from    usinsuv01.claim_his clh1
                                                                                                                                where clh1.claim = clh.claim
                                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                                        else    1 end
                                                                                        when	cla.moneda_cod = 2
                                                                                        then    clh.amount /
                                                                                                case    when    clh.currency = 1
                                                                                                        then    (   select  max(clh0.exchange)
                                                                                                                from    usinsuv01.claim_his clh0
                                                                                                                where	clh0.claim = clh.claim
                                                                                                                and     clh0.transac =
                                                                                                                        (   select  max(clh1.transac)
                                                                                                                                from    usinsuv01.claim_his clh1
                                                                                                                                where	clh1.claim = clh.claim
                                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                                        else    1 end
                                                                                        else    0 end
                                                                        else    0 end,0)
                                                                * (case when cla.bussityp = '2' then 100 else share_coa end/100) as numeric(12,2)) VMTRESSG,
                                                        cla.branch || '-' || cla.product KABPRODT,
                                                        '' KRCFMPGI, --excluido
                                                        '' KMEDCB, --excluido
                                                        '' KMEDPG, --excluido
                                                        '' DUSRORI, --excluido
                                                        '' DUSRAPR, --excluido
                                                        case	when cla.bussityp = '1' and cla.share_coa = 100 then '1' --LP compa��a l�der, siniestro al 100%
                                                                        when cla.bussityp = '1' and cla.share_coa <> 100 then '2' --LP compa��a l�der, siniestro repartido
                                                                        when cla.bussityp = '2' then '3' --LP compa��a no l�der, siniestro al 100%
                                                                        else '0' end KRCTPCSG,
                                                        '' DCODENPG, --excluido
                                                        '' DCODENRC, --excluido
                                                        '' VMFAT, --no disponible
                                                        'LPV' DCOMPA,
                                                        '' DMARCA, --excluido
                                                        '' TDAPROV, --excluido
                                                        '' DNMACJUD, --excluido
                                                        '' KRCTPERP, --excluido
                                                        '' KRBRECIN_MP, --excluido
                                                        '' TMIGPARA, --excluido
                                                        '' KRBRECIN_MD, --excluido
                                                        '' TMIGDE, --excluido
                                                        'LPV -' || cla.claim KSBSIN,
                                                        cla.branch || '-' || cla.product || '-' || cla.policy || '-' || cla.certif KABAPOL,
                                                        '' KABAPOL_EFT, --excluido
                                                        cast(cla.date_origi as date) TINICIOA,
                                                        cast(coalesce(cla.branch_gyp,0) as varchar) KGCRAMO_SAP,
                                                        '' KCBCONTA_AE, --excluido
                                                        '' KCBCONTA_PD, --excluido
                                                        '' KCBCONTA_FN, --excluido
                                                        '' KRCCATFIS, --excluido
                                                        '' KRCTPRET, --excluido
                                                        '' KRCZNRET, --excluido
                                                        '' KRCTPRND, --excluido
                                                        '' TCRIASO, --excluido
                                                        '' DNUCHEQ, --excluido
                                                        '' DNIB, --excluido
                                                        cla.client KEBENTID_TO,
                                                        '' TCONTAB, --excluido
                                                        coalesce(cast(cla.occurdat as varchar),'') DANOABER,
                                                        '' DINDIBNR, --excluido
                                                        '' DINDDESD, --excluido
                                                        '' DINDASVI, --excluido
                                                        '' KRCTRESI, --excluido
                                                        '' VMTSUIRS, --excluido
                                                        '' DNISS, --excluido
                                                        '' DPRESERV, --excluido
                                                        '' KRCTPDCP, --no disponible
                                                        '' DNUMDOC, --excluido
                                                        '' TDOCPGPR, --excluido
                                                        '' DREGACUM, --excluido
                                                        '' VMTCNTSS, --excluido
                                                        '' VMTRETSS, --excluido
                                                        '' VMTSUJSS, --excluido
                                                        '' KRCTPPGP, --excluido
                                                        '' DCDRTIRS, --excluido
                                                        '' DNIF, --excluido
                                                        '' DARQUIVO, --excluido
                                                        '' TARQUIVO, --excluido
                                                        '' VMTINDSTR, --excluido
                                                        '' VMTPRVERC, --excluido
                                                        '' VMTPRVINI, --excluido
                                                        '' VMTINDCTR --excluido
                                        from    (   select	case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                and 2 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                        then    cla.claim
                                                                        else    null end claim,
                                                                                cla.usercomp,
                                                                                cla.company,
                                                                                cla.branch,
                                                                                cla.policy,
                                                                                cla.certif,
                                                                                cla.occurdat,
                                                                                coalesce(coalesce(
                                                                                                        (	select	max(sclient_vig)
                                                                                                                from	usinsug01.wbstblclidepequi
                                                                                                                where	sclient_old = 
                                                                                                                                (	select	evi.scod_vt
                                                                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                                                                        where	evi.scod_inx = cla.client)),
                                                                                                        (	select	evi.scod_vt
                                                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                                                where	evi.scod_inx = cla.client)),
                                                                                        cla.client) client,
                                                                                pol.product,
                                                                                pol.bussityp,
                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                and 2 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                        then    case	coalesce(cla.certif,0)
                                                                                                                                when	0
                                                                                                                                then	pol.date_origi
                                                                                                                                else	(	select	cer.date_origi
                                                                                                                                                        from 	usinsuv01.certificat cer
                                                                                                                                                        where	cer.usercomp = cla.usercomp 
                                                                                                                                                        and 	cer.company = cla.company
                                                                                                                                                        and 	cer.certype = pol.certype
                                                                                                                                                        and 	cer.branch = cla.branch 
                                                                                                                                                        and 	cer.policy = cla.policy
                                                                                                                                                        and		cer.certif = cla.certif)
                                                                                                                                end
                                                                                                else	null end date_origi,
                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                and 2 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                        then    coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                        from    usinsuv01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                        (   select  max(cpl.currency)
                                                                                                        from    usinsuv01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0)
                                                                        else    0 end moneda_cod,
                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                and 2 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                then	case	pol.bussityp
                                                                                                                                when	'2'
                                                                                                                                then 	100 - coalesce(pol.leadshare,0)
                                                                                                                                when	'3'
                                                                                                                                then 	null
                                                                                                                                else	coalesce((	select	coi.share
                                                                                                                                                                        from	usinsuv01.coinsuran coi
                                                                                                                                                                        where   coi.usercomp = cla.usercomp and coi.company = cla.company and coi.certype = pol.certype
                                                                                                                                                                        and     coi.branch = cla.branch and coi.policy = cla.policy and coi.effecdate <= cla.occurdat
                                                                                                                                                                        and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                                                                        and 	coalesce(coi.companyc,0) in (1,12)),100) end 
                                                                                                else	0 end share_coa,
                                                                                case    when    cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                and 2 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                then    case	when	cla.branch = 31
                                                                                                                                then	case	when	substr(pol.titularc,0,1) = 'E'
                                                                                                                                                then 	73
                                                                                                                                                else	case	when
                                                                                                                                                                (	select  sum(ili.quantity)
                                                                                                                                                                from    usinsuv01.insured_li ili
                                                                                                                                                                where   ili.usercomp = cla.usercomp
                                                                                                                                                                and     ili.company = cla.company
                                                                                                                                                                and     ili.certype = pol.certype
                                                                                                                                                                and     ili.branch = cla.branch
                                                                                                                                                                and     ili.policy = cla.policy
                                                                                                                                                                and     ili.certif = 0
                                                                                                                                                                and     ili.effecdate <= pol.effecdate
                                                                                                                                                                and     (ili.nulldate is null or ili.nulldate > pol.effecdate)
                                                                                                                                                and     quantity is not null) = 1
                                                                                                                                                                then	82
                                                                                                                                                                else	(	select  min(sbs.cod_sbs_gyp)
                                                                                                                                                                                                                        from    usinsuv01.product_sbs sbs
                                                                                                                                                                                                                        where   sbs.usercomp = cla.usercomp
                                                                                                                                                                                                                        and     sbs.company = cla.company
                                                                                                                                                                                                                        and     sbs.branch = cla.branch
                                                                                                                                                                                                                        and     sbs.product = pol.product
                                                                                                                                                                                                                        and     sbs.effecdate <= cla.occurdat
                                                                                                                                                                                                                        and     (sbs.nulldate is null or sbs.nulldate > cla.occurdat))
                                                                                                                                                                                                end end
                                                                                                                                when	(cla.branch = 75 and pol.product = 1)
                                                                                                                                then 	case	(	select 	type_cla
                                                                                                                                                                        from	usinsuv01.life_prev
                                                                                                                                                                        where	ctid = 
                                                                                                                                                                        (select coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                                                                        (   select max(ctid) from usinsuv01.life_prev
                                                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                                                and effecdate <= cla.occurdat
                                                                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                                and statusva not in ('2','3')),
                                                                                                                                                                        (   select max(ctid) from usinsuv01.life_prev
                                                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                                                and effecdate <= cla.occurdat
                                                                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                                                        (   select max(ctid) from usinsuv01.life_prev
                                                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                                                and effecdate <= cla.occurdat
                                                                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                                                        (   select min(ctid) from usinsuv01.life_prev
                                                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                                                and effecdate > cla.occurdat
                                                                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                                                        (   select min(ctid) from usinsuv01.life_prev
                                                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                                                        (   select min(ctid) from usinsuv01.life_prev
                                                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                                                and statusva in ('2','3')))))
                                                                                                                                                                when	4 then 76
                                                                                                                                                                when	5 then 76
                                                                                                                                                                when	2 then 94
                                                                                                                                                                when	3 then 94
                                                                                                                                                                when	1 then 95
                                                                                                                                                                else	0 end 
                                                                                                                                else 	coalesce((	select  distinct(sbs.cod_sbs_gyp)
                                                                                                                                                                        from    usinsuv01.product_sbs tnb,
                                                                                                                                                                                        usinsuv01.anexo1_sbs sbs
                                                                                                                                                                        where   tnb.branch = cla.branch
                                                                                                                                                                        and 	tnb.product = pol.product
                                                                                                                                                                        and 	tnb.nulldate is null
                                                                                                                                                                        and 	sbs.cod_sbs_bal = tnb.cod_sbs_bal
                                                                                                                                                                        and     sbs.cod_sbs_gyp = tnb.cod_sbs_gyp),0) end
                                                                                                else	null end branch_gyp,
                                                                                par.fecha
                                                        from    usinsuv01.policy pol
                                                        join	(select cast('12/31/2015' as date) fecha) par on 1 = 1 --ejecutar al a�o 2021 (2015 es para pruebas)
                                                        join	usinsuv01.claim cla
                                                                on		cla.usercomp = pol.usercomp
                                                                and     cla.company = pol.company
                                                                and     cla.branch = pol.branch
                                                                and     cla.policy = pol.policy
                                                                and     exists
                                                                                                (   select  1
                                                                                from    (	select	cast(codigint as varchar(2)) operation
                                                                                                                                from	usinsug01.table140
                                                                                                                                where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                                                                        join	usinsuv01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                where   coalesce (clh.claim,0) = cla.claim
                                                                                and     clh.operdate >= par.fecha)) cla
                                        join 	usinsuv01.claim_his clh
                                                        on		clh.claim = cla.claim
                                                        and		clh.operdate <= cla.fecha
                                        join	(	select	cast(codigint as varchar(2)) operation
                                                                from	usinsug01.table140
                                                                where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl on trim(clh.oper_type) = tcl.operation
                             ) AS TMP        
                             '''   
        
  DF_LPV_INSUNIX = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBRECIN_INSUNIX_LPV).load()

  L_DF_RBRECIN_INSUNIX = DF_LPG_INSUNIX.union(DF_LPV_INSUNIX)

  L_RBRECIN_VTIME_LPG = f'''
                        (
                          select	'D' as INDDETREC,
                                        'RBRECIN' as TABLAIFRS17,
                                        /*par.cia || par.sep1 || clh."NCLAIM" || par.sep2 || clh."NCASE_NUM" || par.sep1 || clh."NTRANSAC" || par.sep1 || coalesce(clm."NCOVER",0)*/ '' PK,
                                        '' DTPREG,
                                        '' TIOCPROC,
                                        cast (clh."DOPERDATE" as varchar) TIOCFRM,
                                        '' TIOCTO,
                                        '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                        '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                        '' KRCTPRCI, --no disponible
                                        'PVG' KGIORIGM,
                                        cast (clh."NBRANCH" as VARCHAR) KGCRAMO,
                                        cast (clh."NPOLICY" as VARCHAR ) DNUMAPO,
                                        cast (clh."NCERTIF" as VARCHAR ) DNMCERT,
                                        cast (clh."NCLAIM" as VARCHAR ) DNUMSIN,
                                        cast (clh."NCASE_NUM" as VARCHAR ) DNUMSSIN,
                                        '' DNUMPENS,
                                        clh."NCLAIM" || '-'|| clh."NCASE_NUM" ||'-' || clh."NTRANSAC" DNUMREC,
                                        '' DNMAGRE, --no disponible
                                        '' NSAGREG,
                                        '' NSEQSIN,
                                        cast (clh."DOCCURDAT" as  varchar) DANOSIN,
                                        cast (clh."DOPERDATE" as varchar) TEMISSAO,
                                        '' TINICIO, --no disponible
                                        '' TTERMO, --no disponible
                                        '' TESTADO, --no disponible
                                        '' TPGCOB, --PENDIENTE TABLA EQUIVALENTE A INSUNIX: CLAIM_PAY_SAP
                                        '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIALBE
                                        '' KRCESPRI, --no disponible
                                        '' KRCESTRI, --no disponible
                                        '' KRCMOSTI,
                                        cast (clh.moneda_cod as varchar) KRCMOEDA,
                                        '' VCAMBIO, --no disponible
                                        cast (clh.monto_trans as numeric (12,2)) VMTTOTRI,
                                        '' VMTIVA, --no disponible
                                        '' VMTIRSRT, --no disponible
                                        '' VTXIRSRT, --no disponible
                                        '' VMTLIQRC,
                                        cast (coalesce	(	case	when	clh.MONEDA_COD = 1
                                                                                        then	case	when	clm."NCURRENCY" = 1
                                                                                                                        then	clm."NAMOUNT"
                                                                                                                        when	clm."NCURRENCY" = 2
                                                                                                                        then	clm."NAMOUNT" * clm."NEXCHANGE"
                                                                                                                        else	0 end
                                                                                        when	clh.MONEDA_COD = 2
                                                                                        then	case	when	clm."NCURRENCY" = 2
                                                                                                                        then	clm."NAMOUNT"
                                                                                                                        when	clm."NCURRENCY" = 1
                                                                                                                        then	clm."NLOC_AMOUNT"
                                                                                                                        else	0 end
                                                                                        else	0 end, 0) * (case when clh."SBUSSITYP" = '2' then 100 else nshare_coa end/100)as numeric (12,2)) VMTCOSEG,
                                        cast (coalesce	(	case	when	clh.MONEDA_COD = 1
                                                                                        then	case	when	clm."NCURRENCY" = 1
                                                                                                                        then	clm."NAMOUNT"
                                                                                                                        when	clm."NCURRENCY" = 2
                                                                                                                        then	clm."NAMOUNT" * clm."NEXCHANGE"
                                                                                                                        else	0 end
                                                                                        when	clh.MONEDA_COD = 2
                                                                                        then	case	when	clm."NCURRENCY" = 2
                                                                                                                        then	clm."NAMOUNT"
                                                                                                                        when	clm."NCURRENCY" = 1
                                                                                                                        then	clm."NLOC_AMOUNT"
                                                                                                                        else	0 end
                                                                                        else	0 end, 0) * (case when clh."SBUSSITYP" = '2' then 100 else nshare_coa end/100) 
                                                                                        * (nshare_rea / 100) as numeric (12,2)) VMTRESSG,
                                                        clh."NBRANCH" || '-' || clh."NPRODUCT" KABPRODT,
                                        '' KRCFMPGI,
                                        '' KMEDCB,
                                        '' KMEDPG,
                                        '' DUSRORI,
                                        '' DUSRAPR,		
                                        case	when clh."SBUSSITYP" = '1' and (clh.nshare_coa = 100 and clh.nshare_rea = 100) then '1' --LP compa��a l�der, siniestro al 100%
                                                        when clh."SBUSSITYP" = '1' and (clh.nshare_rea <> 100 or clh.nshare_rea <> 100) then '2' --LP compa��a l�der, siniestro repartido
                                                        when clh."SBUSSITYP" = '2' and clh.nshare_rea = 100 then '3' --LP compa��a no l�der, siniestro al 100%
                                                        when clh."SBUSSITYP" = '2' and clh.nshare_rea <> 100 then '4' --LP compa��a no l�der, siniestro repartido
                                                        else '0' 
                                        end KRCTPCSG,
                                        '' DCODENPG,
                                        '' DCODENRC,
                                        '' VMFAT, --no disponible
                                        'LPG' DCOMPA,
                                        '' DMARCA,
                                        '' TDAPROV,
                                        '' DNMACJUD,
                                        '' KRCTPERP,
                                        '' KRBRECIN_MP,
                                        '' TMIGPARA,
                                        '' KRBRECIN_MD,
                                        '' TMIGDE,
                                        clh."NCLAIM" || '-' || 'LPG'  KSBSIN,
                                        clh."NBRANCH" || '-' || clh."NPOLICY" || '-' || clh."NCERTIF" KABAPOL,
                                        '' KABAPOL_EFT,
                                        cast (clh.ddate_origi as VARCHAR) TINICIOA,
                                                cast (case	when	clh."NBRANCH" = 21
                                                                then	(   select  gco."NBRANCH_LED"
                                                                        from    usvtimg01."LIFE_COVER" gco
                                                                        where   gco."NCOVER" = clm."NCOVER"
                                                                        and     gco."NPRODUCT" = clh."NPRODUCT"
                                                                        and     gco."NMODULEC" = clm."NMODULEC"
                                                                        and     gco."NBRANCH" = clh."NBRANCH"
                                                                        and     cast(gco."DEFFECDATE" as date) <= "DOCCURDAT"
                                                                        and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > "DOCCURDAT")
                                                                        and     gco."SSTATREGT" <> '4')
                                                                        else	(   select  gco."NBRANCH_LED"
                                                                        from    usvtimg01."GEN_COVER" gco
                                                                        where   gco."NCOVER" = clm."NCOVER"
                                                                        and     gco."NPRODUCT" = clh."NPRODUCT"
                                                                        and     gco."NMODULEC" = clm."NMODULEC"
                                                                        and     gco."NBRANCH" = clh."NBRANCH"
                                                                        and     cast(gco."DEFFECDATE" as date) <= "DOCCURDAT"
                                                                        and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > "DOCCURDAT")
                                                                        and     gco."SSTATREGT" <> '4') 
                                                        end as VARCHAR)KGCRAMO_SAP,
                                        '' KCBCONTA_AE,
                                        '' KCBCONTA_PD,
                                        '' KCBCONTA_FN,
                                        '' KRCCATFIS,
                                        '' KRCTPRET,
                                        '' KRCZNRET,
                                        '' KRCTPRND,
                                        '' TCRIASO,
                                        '' DNUCHEQ,
                                        '' DNIB,
                                        clh."SCLIENT" KEBENTID_TO,
                                        '' TCONTAB,
                                        cast (clh."DOCCURDAT" as VARCHAR ) DANOABER,
                                        '' DINDIBNR,
                                        '' DINDDESD,
                                        '' DINDASVI,
                                        '' KRCTRESI,
                                        '' VMTSUIRS,
                                        '' DNISS,
                                        '' DPRESERV,
                                        '' KRCTPDCP, --no disponible,
                                        '' DNUMDOC,
                                        '' TDOCPGPR,
                                        '' DREGACUM,
                                        '' VMTCNTSS,
                                        '' VMTRETSS,
                                        '' VMTSUJSS,
                                        '' KRCTPPGP,
                                        '' DCDRTIRS,
                                        '' DNIF,
                                        '' DARQUIVO,
                                        '' TARQUIVO,
                                        '' VMTINDSTR,
                                        '' VMTPRVERC,
                                        '' VMTPRVINI,
                                        '' VMTINDCTR
                                from    (   select  cla.*,
                                                                        clh."NCASE_NUM",
                                                                        clh."NDEMAN_TYPE",
                                                clh."NTRANSAC",
                                                clh."NOPER_TYPE",
                                                cast(clh."DOPERDATE" as date) "DOPERDATE",
                                                                        case	when	cla.moneda_cod = 1
                                                                                        then	case	when	clh."NCURRENCY" = 1
                                                                                                                        then	clh."NAMOUNT"
                                                                                                                        when	clh."NCURRENCY" = 2
                                                                                                                        then	clh."NAMOUNT" * clh."NEXCHANGE"
                                                                                                                        else	0 end
                                                                                        when	cla.moneda_cod = 2
                                                                                        then	case	when	clh."NCURRENCY" = 2
                                                                                                                        then	clh."NAMOUNT"
                                                                                                                        when	clh."NCURRENCY" = 1
                                                                                                                        then	clh."NLOC_AMOUNT"
                                                                                                                        else	0 end
                                                                                                        else	0 end monto_trans
                                        from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                        then    cla."NCLAIM"
                                                                        else    null end "NCLAIM",
                                                                cla."SCERTYPE",
                                                                cla."NBRANCH",   
                                                                cla."NPOLICY",
                                                                cla."NCERTIF",
                                                                cla."SCLIENT",
                                                                pol."NPRODUCT",
                                                                pol."SBUSSITYP",
                                                                cast(cla."DOCCURDAT" as date) "DOCCURDAT",
                                                                                                case	coalesce(cla."NCERTIF",0)
                                                                                                                when	0
                                                                                                                then	pol."DDATE_ORIGI" 
                                                                                                                else	(	select	cer."DDATE_ORIGI"
                                                                                                                                        from 	usvtimg01."CERTIFICAT" cer
                                                                                                                                        where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                                                                        and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                                                                        and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                                                                        and		cer."NCERTIF" = cla."NCERTIF"
                                                                                                                                        and		CER."NDIGIT" = 0)
                                                                                                                end ddate_origi,
                                                                case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE"  = '2'
                                                                        then    coalesce(   coalesce((  select  max(cpl."NCURRENCY")
                                                                                                        from    usvtimg01."CURREN_POL" cpl
                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                                                                        and     cpl."NPOLICY" = cla."NPOLICY"
                                                                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                                (   select  max(cpl."NCURRENCY")
                                                                                                        from    usvtimg01."CURREN_POL" cpl
                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                                                                        and     cpl."NPOLICY" = cla."NPOLICY")),0)
                                                                        else    0 end MONEDA_COD,
                                                                        case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE"  = '2'
                                                                                                                then	case	pol."SBUSSITYP"
                                                                                                                                                when	'2'
                                                                                                                                                then 	100 - coalesce(pol."NLEADSHARE",0)
                                                                                                                                                when	'3'
                                                                                                                                                then 	null
                                                                                                                                                else	coalesce((  select 	"NSHARE"
                                                                                                                                                                                        from	usvtimg01."COINSURAN" coi
                                                                                                                                                                                        where	coi."SCERTYPE" = cla."SCERTYPE"
                                                                                                                                                                                        and     coi."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                        and     coi."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                        and     coi."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                        and 	coi."NCOMPANY" is not null
                                                                                                                                                                                        and     coi."DEFFECDATE" <= cla."DOCCURDAT"
                                                                                                                                                                                        and     (coi."DNULLDATE" is null or coi."DNULLDATE" > cla."DOCCURDAT")
                                                                                                                                                                                        and		coi."NCOMPANY" in (1)),100) end 
                                                                                                                else	0 end nshare_coa,
                                                                                                100 nshare_rea --PENDIENTE
                                                                                from    usvtimg01."POLICY" pol
                                                        join    usvtimg01."CLAIM" cla on  cla."SCERTYPE" = pol."SCERTYPE"
                                                                                and cla."NPOLICY" = pol."NPOLICY"
                                                                                and cla."NBRANCH" = pol."NBRANCH"
                                                        join    (select  distinct clh."NCLAIM"
                                                                from    (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                        where	"NCONDITION" in (73)) csv --solo pagos
                                                                join  usvtimg01."CLAIM_HIS" clh
                                                                on    coalesce (clh."NCLAIM",0) > 0
                                                                and   clh."NOPER_TYPE" = csv."SVALUE"
                                                                and   clh."DOPERDATE" >= '12/31/2018') clh
                                                        on  clh."NCLAIM" = cla."NCLAIM") cla
                                                                        join usvtimg01."CLAIM_HIS" clh on clh."NCLAIM" = cla."NCLAIM"
                                                                        join (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                from	usvtimg01."CONDITION_SERV" cs 
                                                                                where	"NCONDITION" in (73)) csv --solo pagos
                                                                on      clh."NOPER_TYPE" = csv."SVALUE"
                                                        and     clh."DOPERDATE" <= '12/31/2023') clh
                                join 	usvtimg01."CL_M_COVER" clm
                                on	clm."NCLAIM" = clh."NCLAIM"
                                and 	clm."NCASE_NUM" = clh."NCASE_NUM"
                                and 	clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                and 	clm."NTRANSAC" = clh."NTRANSAC"
                                --6.589s (TODOS)
                        ) AS TMP       
                        '''

  DF_LPG_VTIME = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBRECIN_VTIME_LPG).load()

  L_RBRECIN_VTIME_LPV = f'''
                        (
                          select	'D' as INDDETREC,
                                        'RBRECIN' as TABLAIFRS17,
                                        /*par.cia || par.sep1 || clh."NCLAIM" || par.sep2 || clh."NCASE_NUM" || par.sep1 || clh."NTRANSAC" || par.sep1 || coalesce(clm."NCOVER",0)*/ ''  PK,
                                        '' DTPREG,
                                        '' TIOCPROC,
                                        cast (clh."DOPERDATE" as varchar)  TIOCFRM,
                                        '' TIOCTO,
                                        '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                        '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                        '' KRCTPRCI, --no disponible
                                        'PVV' KGIORIGM,
                                        cast (clh."NBRANCH" as varchar ) KGCRAMO,
                                        cast (clh."NPOLICY" as varchar ) DNUMAPO,
                                        cast (clh."NCERTIF" as varchar ) DNMCERT,
                                        cast (clh."NCLAIM" as varchar ) DNUMSIN,
                                        cast (clh."NCASE_NUM" as varchar ) DNUMSSIN,
                                        '' DNUMPENS,
                                        clh."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" DNUMREC,
                                        '' DNMAGRE, --no disponible
                                        '' NSAGREG,
                                        '' NSEQSIN,
                                        cast (clh."DOCCURDAT" as varchar ) DANOSIN,
                                        cast ( clh."DOPERDATE" as varchar) TEMISSAO,
                                        '' TINICIO, --no disponible
                                        '' TTERMO, --no disponible
                                        '' TESTADO, --no disponible
                                        '' TPGCOB, --PENDIENTE TABLA EQUIVALENTE A INSUNIX: CLAIM_PAY_SAP
                                        '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIALBE
                                        '' KRCESPRI, --no disponible
                                        '' KRCESTRI, --no disponible
                                        '' KRCMOSTI,
                                        cast (clh.moneda_cod as varchar) KRCMOEDA,
                                        '' VCAMBIO, --no disponible
                                        cast (clh.monto_trans as numeric (12,2)) VMTTOTRI,
                                        '' VMTIVA, --no disponible
                                        '' VMTIRSRT, --no disponible
                                        '' VTXIRSRT, --no disponible
                                        '' VMTLIQRC,
                                        cast (coalesce	(	case	when	clh.MONEDA_COD = 1
                                                                                        then	case	when	clm."NCURRENCY" = 1
                                                                                                                        then	clm."NAMOUNT"
                                                                                                                        when	clm."NCURRENCY" = 2
                                                                                                                        then	clm."NAMOUNT" * clm."NEXCHANGE"
                                                                                                                        else	0 end
                                                                                        when	clh.MONEDA_COD = 2
                                                                                        then	case	when	clm."NCURRENCY" = 2
                                                                                                                        then	clm."NAMOUNT"
                                                                                                                        when	clm."NCURRENCY" = 1
                                                                                                                        then	clm."NLOC_AMOUNT"
                                                                                                                        else	0 end
                                                                                        else	0 end, 0) * (case when clh."SBUSSITYP" = '2' then 100 else nshare_coa end/100) as numeric(12,2)) VMTCOSEG,
                                        cast (coalesce	(	case	when	clh.MONEDA_COD = 1
                                                                                        then	case	when	clm."NCURRENCY" = 1
                                                                                                                        then	clm."NAMOUNT"
                                                                                                                        when	clm."NCURRENCY" = 2
                                                                                                                        then	clm."NAMOUNT" * clm."NEXCHANGE"
                                                                                                                        else	0 end
                                                                                        when	clh.MONEDA_COD = 2
                                                                                        then	case	when	clm."NCURRENCY" = 2
                                                                                                                        then	clm."NAMOUNT"
                                                                                                                        when	clm."NCURRENCY" = 1
                                                                                                                        then	clm."NLOC_AMOUNT"
                                                                                                                        else	0 end
                                                                                        else	0 end, 0) * (case when clh."SBUSSITYP" = '2' then 100 else nshare_coa end/100) 
                                                                                        * (nshare_rea / 100)as numeric (12,2) ) VMTRESSG,
                                                        clh."NBRANCH" || '-' || clh."NPRODUCT" KABPRODT,
                                        '' KRCFMPGI,
                                        '' KMEDCB,
                                        '' KMEDPG,
                                        '' DUSRORI,
                                        '' DUSRAPR,		
                                        case	when clh."SBUSSITYP" = '1' and (clh.nshare_coa = 100 and clh.nshare_rea = 100) then '1' --LP compa��a l�der, siniestro al 100%
                                                        when clh."SBUSSITYP" = '1' and (clh.nshare_rea <> 100 or clh.nshare_rea <> 100) then '2' --LP compa��a l�der, siniestro repartido
                                                        when clh."SBUSSITYP" = '2' and clh.nshare_rea = 100 then '3' --LP compa��a no l�der, siniestro al 100%
                                                        when clh."SBUSSITYP" = '2' and clh.nshare_rea <> 100 then '4' --LP compa��a no l�der, siniestro repartido
                                                        else '0' 
                                                end KRCTPCSG,
                                                '' DCODENPG,
                                                        '' DCODENRC,
                                                        '' VMFAT, --no disponible
                                                        'LPV' DCOMPA,
                                                        '' DMARCA,
                                                        '' TDAPROV,
                                                        '' DNMACJUD,
                                                        '' KRCTPERP,
                                                        '' KRBRECIN_MP,
                                                        '' TMIGPARA,
                                                        '' KRBRECIN_MD,
                                                        '' TMIGDE,
                                                        clh."NCLAIM" || '-' || 'LPV'  KSBSIN,
                                                        clh."NBRANCH" || '-' || clh."NPOLICY" || '-' || clh."NCERTIF" KABAPOL,
                                                        '' KABAPOL_EFT,
                                                        cast (clh.ddate_origi as varchar) TINICIOA,
                                                cast ((   select  gco."NBRANCH_LED"
                                                from    usvtimv01."LIFE_COVER" gco
                                                where   gco."NCOVER" = clm."NCOVER"
                                                and     gco."NPRODUCT" = clh."NPRODUCT"
                                                and     gco."NMODULEC" = clm."NMODULEC"
                                                and     gco."NBRANCH" = clh."NBRANCH"
                                                and     cast(gco."DEFFECDATE" as date) <= "DOCCURDAT"
                                                and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > "DOCCURDAT")
                                                and     gco."SSTATREGT" <> '4')as varchar) KGCRAMO_SAP,
                                                '' KCBCONTA_AE,
                                        '' KCBCONTA_PD,
                                        '' KCBCONTA_FN,
                                        '' KRCCATFIS,
                                        '' KRCTPRET,
                                        '' KRCZNRET,
                                        '' KRCTPRND,
                                        '' TCRIASO,
                                        '' DNUCHEQ,
                                        '' DNIB,    
                                        clh."SCLIENT" KEBENTID_TO,
                                        '' TCONTAB,
                                        cast (clh."DOCCURDAT" as varchar) DANOABER,
                                        '' DINDIBNR,
                                        '' DINDDESD,
                                        '' DINDASVI,
                                        '' KRCTRESI,
                                        '' VMTSUIRS,
                                        '' DNISS,
                                        '' DPRESERV,
                                        '' KRCTPDCP, --no disponible
                                        '' DNUMDOC,
                                        '' TDOCPGPR,
                                        '' DREGACUM,
                                        '' VMTCNTSS,
                                        '' VMTRETSS,
                                        '' VMTSUJSS,
                                        '' KRCTPPGP,
                                        '' DCDRTIRS,
                                        '' DNIF,
                                        '' DARQUIVO,
                                        '' TARQUIVO,
                                        '' VMTINDSTR,
                                        '' VMTPRVERC,
                                        '' VMTPRVINI,
                                        '' VMTINDCTR
                                from    (   select  cla.*,
                                                                        clh."NCASE_NUM",
                                                                        clh."NDEMAN_TYPE",
                                                clh."NTRANSAC",
                                                clh."NOPER_TYPE",
                                                cast(clh."DOPERDATE" as date) "DOPERDATE",
                                                                        case	when	cla.moneda_cod = 1
                                                                                        then	case	when	clh."NCURRENCY" = 1
                                                                                                                        then	clh."NAMOUNT"
                                                                                                                        when	clh."NCURRENCY" = 2
                                                                                                                        then	clh."NAMOUNT" * clh."NEXCHANGE"
                                                                                                                        else	0 end
                                                                                        when	cla.moneda_cod = 2
                                                                                        then	case	when	clh."NCURRENCY" = 2
                                                                                                                        then	clh."NAMOUNT"
                                                                                                                        when	clh."NCURRENCY" = 1
                                                                                                                        then	clh."NLOC_AMOUNT"
                                                                                                                        else	0 end
                                                                                                        else	0 end monto_trans
                                        from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                        then    cla."NCLAIM"
                                                                        else    null end "NCLAIM",
                                                                cla."SCERTYPE",
                                                                cla."NBRANCH",   
                                                                cla."NPOLICY",
                                                                cla."NCERTIF",
                                                                cla."SCLIENT",
                                                                pol."NPRODUCT",
                                                                pol."SBUSSITYP",
                                                                cast(cla."DOCCURDAT" as date) "DOCCURDAT",
                                                                                                case	coalesce(cla."NCERTIF",0)
                                                                                                                when	0
                                                                                                                then	pol."DDATE_ORIGI" 
                                                                                                                else	(	select	cer."DDATE_ORIGI"
                                                                                                                                        from 	usvtimv01."CERTIFICAT" cer
                                                                                                                                        where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                                                                        and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                                                                        and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                                                                        and		cer."NCERTIF" = cla."NCERTIF"
                                                                                                                                        and		CER."NDIGIT" = 0)
                                                                                                                end ddate_origi,
                                                                case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE"  = '2'
                                                                        then    coalesce(   coalesce((  select  max(cpl."NCURRENCY")
                                                                                                        from    usvtimv01."CURREN_POL" cpl
                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                                                                        and     cpl."NPOLICY" = cla."NPOLICY"
                                                                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                                (   select  max(cpl."NCURRENCY")
                                                                                                        from    usvtimv01."CURREN_POL" cpl
                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                                                                        and     cpl."NPOLICY" = cla."NPOLICY")),0)
                                                                        else    0 end MONEDA_COD,
                                                                        case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE"  = '2'
                                                                                                                then	case	pol."SBUSSITYP"
                                                                                                                                                when	'2'
                                                                                                                                                then 	100 - coalesce(pol."NLEADSHARE",0)
                                                                                                                                                when	'3'
                                                                                                                                                then 	null
                                                                                                                                                else	coalesce((  select 	"NSHARE"
                                                                                                                                                                                        from	usvtimv01."COINSURAN" coi
                                                                                                                                                                                        where	coi."SCERTYPE" = cla."SCERTYPE"
                                                                                                                                                                                        and     coi."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                        and     coi."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                        and     coi."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                        and 	coi."NCOMPANY" is not null
                                                                                                                                                                                        and     coi."DEFFECDATE" <= cla."DOCCURDAT"
                                                                                                                                                                                        and     (coi."DNULLDATE" is null or coi."DNULLDATE" > cla."DOCCURDAT")
                                                                                                                                                                                        and		coi."NCOMPANY" in (1)),100) end 
                                                                                                                else	0 end nshare_coa,
                                                                                                100 nshare_rea --NO HAY REASEGUROS EN LPV VT
                                                                                from    usvtimv01."POLICY" pol
                                                        join    usvtimv01."CLAIM" cla on cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                                and cla."NPOLICY" = pol."NPOLICY"
                                                                                                                and cla."NBRANCH" = pol."NBRANCH"
                                                        join   (   select  distinct clh."NCLAIM"
                                                                from    (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                from	usvtimv01."CONDITION_SERV" cs 
                                                                                                                                where	"NCONDITION" in (73)) csv --solo pagos
                                                                join        usvtimv01."CLAIM_HIS" clh
                                                                on   coalesce (clh."NCLAIM",0) > 0
                                                                and     clh."NOPER_TYPE" = csv."SVALUE"
                                                                and     clh."DOPERDATE" >= '12/31/2015') clh
                                                        on  clh."NCLAIM" = cla."NCLAIM") cla
                                                                join usvtimv01."CLAIM_HIS" clh on clh."NCLAIM" = cla."NCLAIM"
                                                                join (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                from	usvtimv01."CONDITION_SERV" cs 
                                                                                where	"NCONDITION" in (73)) csv --solo pagos
                                                        on  	clh."NOPER_TYPE" = csv."SVALUE"
                                        and     clh."DOPERDATE" <= '12/31/2023') clh
                                join 	usvtimv01."CL_M_COVER" clm
                                on	clm."NCLAIM" = clh."NCLAIM"
                                and 	clm."NCASE_NUM" = clh."NCASE_NUM"
                                and 	clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                and 	clm."NTRANSAC" = clh."NTRANSAC"
                                --6.589s (TODOS)      
                        ) AS TMP       
                        '''

  DF_LPV_VTIME = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBRECIN_VTIME_LPV).load()

  L_DF_RBRECIN_VTIME = DF_LPG_VTIME.union(DF_LPV_VTIME)

  L_RBRECIN_INSIS = f'''
                        (
                                SELECT	
                                        'D' AS INDDETREC,
                                        'RBRECIN' AS TABLAIFRS17,
                                        '' PK,
                                        '' DTPREG, --EXCLUIDO
                                        '' TIOCPROC, --EXCLUIDO
                                        CAST(CLH."REGISTRATION_DATE" AS DATE) TIOCFRM,
                                        '' TIOCTO, --EXCLUIDO
                                        '' KRITPREG, --INDETERMINADO (PENDIENTE COORDINAR CON FID/LP)
                                        '' KRITPRGI, --INDETERMINADO (PENDIENTE COORDINAR CON FID/LP)
                                        '' KRCTPRCI, --NO DISPONIBLE
                                        'PNV' KGIORIGM, --EXCLUIDO
                                        POL."ATTR1" KGCRAMO,
                                        POL."POLICY_NO" DNUMAPO,
                                        CASE WHEN CL0.PEP_ID IS NOT NULL THEN (SELECT "POLICY_NO" FROM USINSIV01."POLICY" WHERE "POLICY_ID" = CL0.PEP_ID) ELSE '' END DNMCERT,
                                        CLA."CLAIM_REGID" DNUMSIN,
                                        '' DNUMSSIN, --NO DISPONIBLE
                                        '' DNUMPENS, --EXCLUIDO
                                        CLA."CLAIM_REGID" || PAR.SEP || ROUND(CLH."RESERV_SEQ",0) DNUMREC,
                                        '' DNMAGRE, --NO DISPONIBLE
                                        '' NSAGREG, --EXCLUIDO
                                        '' NSEQSIN, --EXCLUIDO
                                        CAST(CLA."EVENT_DATE" AS DATE) DANOSIN,
                                        CAST(CLH."REGISTRATION_DATE" AS DATE) TEMISSAO,
                                        '' TINICIO, --NO DISPONIBLE
                                        '' TTERMO, --NO DISPONIBLE
                                        '' TESTADO, --NO DISPONIBLE
                                        '' TPGCOB, --NO SE DISPONE DE ACCESOS A LA TABLA INTERFAZ_INSUDB.WBSTBLCLAIM_DOC_SAP
                                        '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIALBE
                                        '' KRCESPRI, --NO DISPONIBLE
                                        '' KRCESTRI, --NO DISPONIBLE
                                        '' KRCMOSTI, --EXCLUIDO
                                        CL0.MONEDA_COD KRCMOEDA,
                                        '' VCAMBIO, --NO DISPONIBLE
                                        CLH."RESERV_AMNT" VMTTOTRI,
                                        '' VMTIVA, --NO DISPONIBLE
                                        '' VMTIRSRT, --NO DISPONIBLE
                                        '' VTXIRSRT, --NO DISPONIBLE
                                        '' VMTLIQRC, --EXCLUIDO
                                        CLH."RESERV_AMNT" VMTCOSEG,
                                        CLH."RESERV_AMNT" VMTRESSG, --PENDIENTE DETERMINA REGLAS EN RI_CEDED_CLAIMS
                                        POL."ATTR1"  || PAR.SEP || POL."ATTR4" KABPRODT,
                                        '' KRCFMPGI, --EXCLUIDO
                                        '' KMEDCB, --EXCLUIDO
                                        '' KMEDPG, --EXCLUIDO
                                        '' DUSRORI, --EXCLUIDO
                                        '' DUSRAPR, --EXCLUIDO
                                        CASE	WHEN NOT EXISTS (SELECT 1 FROM USINSIV01."RI_CEDED_CLAIMS" RCC WHERE RCC."CLAIM_ID" = CLA."CLAIM_ID" AND RCC."CLAIM_OBJ_SEQ" = CLO."CLAIM_OBJ_SEQ")
                                                                THEN 1 --LP COMPA��A L�DER, SINIESTRO AL 100%
                                                        WHEN EXISTS (SELECT 1 FROM USINSIV01."RI_CEDED_CLAIMS" RCC WHERE RCC."CLAIM_ID" = CLA."CLAIM_ID" AND RCC."CLAIM_OBJ_SEQ" = CLO."CLAIM_OBJ_SEQ")
                                                                THEN 2 --LP COMPA��A L�DER, SINIESTRO REPARTIDO
                                                        ELSE 0 END KSCTPCSG,
                                        '' DCODENPG, --EXCLUIDO
                                        '' DCODENRC, --EXCLUIDO
                                        '' VMFAT, --NO DISPONIBLE
                                        PAR.CIA DCOMPA,
                                        '' DMARCA, --EXCLUIDO
                                        '' TDAPROV, --EXCLUIDO
                                        '' DNMACJUD, --EXCLUIDO
                                        '' KRCTPERP, --EXCLUIDO
                                        '' KRBRECIN_MP, --EXCLUIDO
                                        '' TMIGPARA, --EXCLUIDO
                                        '' KRBRECIN_MD, --EXCLUIDO
                                        '' TMIGDE, --EXCLUIDO
                                        PAR.CIA || PAR.SEP || CLH."CLAIM_ID" KSBSIN,
                                        POL."ATTR1" || PAR.SEP || POL."POLICY_NO" ||
                                                CASE WHEN CL0.PEP_ID IS NOT NULL THEN PAR.SEP || (SELECT "POLICY_NO" FROM USINSIV01."POLICY" WHERE "POLICY_ID" = CL0.PEP_ID) ELSE '' END KABAPOL,
                                        '' KABAPOL_EFT, --EXCLUIDO
                                        CAST(CLA."EVENT_DATE" AS DATE) TINICIOA,
                                        POL."ATTR1" KGCRAMO_SAP,
                                        '' KCBCONTA_AE, --EXCLUIDO
                                        '' KCBCONTA_PD, --EXCLUIDO
                                        '' KCBCONTA_FN, --EXCLUIDO
                                        '' KRCCATFIS, --EXCLUIDO
                                        '' KRCTPRET, --EXCLUIDO
                                        '' KRCZNRET, --EXCLUIDO
                                        '' KRCTPRND, --EXCLUIDO
                                        '' TCRIASO, --EXCLUIDO
                                        '' DNUCHEQ, --EXCLUIDO
                                        '' DNIB, --EXCLUIDO
                                        CLO."MAN_ID" KEBENTID_TO,
                                        '' TCONTAB, --EXCLUIDO
                                        CAST(CLA."EVENT_DATE" AS DATE) DANOABER,
                                        '' DINDIBNR, --EXCLUIDO
                                        '' DINDDESD, --EXCLUIDO
                                        '' DINDASVI, --EXCLUIDO
                                        '' KRCTRESI, --EXCLUIDO
                                        '' VMTSUIRS, --EXCLUIDO
                                        '' DNISS, --EXCLUIDO
                                        '' DPRESERV, --EXCLUIDO
                                        '' KRCTPDCP, --NO DISPONIBLE
                                        '' DNUMDOC, --EXCLUIDO
                                        '' TDOCPGPR, --EXCLUIDO
                                        '' DREGACUM, --EXCLUIDO
                                        '' VMTCNTSS, --EXCLUIDO
                                        '' VMTRETSS, --EXCLUIDO
                                        '' VMTSUJSS, --EXCLUIDO
                                        '' KRCTPPGP, --EXCLUIDO
                                        '' DCDRTIRS, --EXCLUIDO
                                        '' DNIF, --EXCLUIDO
                                        '' DARQUIVO, --EXCLUIDO
                                        '' TARQUIVO, --EXCLUIDO
                                        '' VMTINDSTR, --EXCLUIDO
                                        '' VMTPRVERC, --EXCLUIDO
                                        '' VMTPRVINI, --EXCLUIDO
                                        '' VMTINDCTR --EXCLUIDO
                                FROM	(	SELECT	CLA."CLAIM_ID" CLAIM_ID,
                                                (SELECT "MASTER_POLICY_ID" FROM USINSIV01."POLICY_ENG_POLICIES" WHERE "POLICY_ID" = CLA."POLICY_ID") PEP_ID,
                                                POL.CTID POL_ID,
                                                CASE	COALESCE((	SELECT	DISTINCT "AV_CURRENCY"
                                                                                        FROM	USINSIV01."INSURED_OBJECT"
                                                                                        WHERE	"POLICY_ID" = POL."POLICY_ID" LIMIT 1),'')
                                                                WHEN 'USD' THEN 2
                                                                WHEN 'PEN' THEN 1
                                                                ELSE 0 END MONEDA_COD
                                FROM	USINSIV01."CLAIM" CLA
                                JOIN	USINSIV01."POLICY" POL ON POL."POLICY_ID" = CLA."POLICY_ID"
                                WHERE	EXISTS
                                                (	SELECT	1
                                                        FROM	USINSIV01."CLAIM_RESERVE_HISTORY"
                                                        WHERE	"CLAIM_ID" = CLA."CLAIM_ID"
                                                        AND		"OP_TYPE" IN ('PAYMCONF','PAYMINV')
                                                        AND		CAST("REGISTRATION_DATE" AS DATE) >= '01-01-2018' --'2021-12-31'
                                                        /*AND		"CLAIM_ID" IN (20700000990, 20700000119)*/)) CL0
                                JOIN	USINSIV01."CLAIM_OBJECTS"  CLO ON CLO."CLAIM_ID" = CL0.CLAIM_ID
                                JOIN	USINSIV01."CLAIM_RESERVE_HISTORY" CLH
                                ON		CLH."CLAIM_ID" = CLO."CLAIM_ID"
                                AND		CLH."REQUEST_ID" = CLO."REQUEST_ID"
                                AND		CLH."OP_TYPE" IN ('PAYMCONF','PAYMINV')
                                AND		CAST(CLH."REGISTRATION_DATE" AS DATE) <= '12-31-2023'
                                JOIN	USINSIV01."CLAIM" CLA ON CLA."CLAIM_ID" = CL0.CLAIM_ID
                                JOIN	USINSIV01."POLICY" POL ON POL.CTID = CL0.POL_ID
                                JOIN	(SELECT	'LPV' CIA, '-' SEP) PAR ON 1 = 1   
                        ) AS TMP
                         '''
   
  L_DF_RBRECIN_INSIS= GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_RBRECIN_INSIS).load()
 
  L_DF_RBRECIN = L_DF_RBRECIN_INSUNIX.union(L_DF_RBRECIN_VTIME).union(L_DF_RBRECIN_INSIS)

  return L_DF_RBRECIN
