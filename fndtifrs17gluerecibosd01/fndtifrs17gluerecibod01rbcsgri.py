def get_data(GLUE_CONTEXT, CONNECTION):

    L_RBCSGRI_INSUNIX_LPG_NEGO1 = f'''
                                      (
                                        select 	'D' as INDDETREC,
                                                'RBCSGRI' as TABLAIFRS17,
                                                /*par.cia || par.sep || clh.claim || par.sep || clh.transac || par.sep || coalesce(clm.cover,0) || par.sep || coalesce(coi.companyc,0)*/ '' PK,
                                                '' DTPREG,
                                                '' TIOCPROC,
                                                '' TIOCFRM,
                                                '' TIOCTO,
                                                'PIG' KGIORIGM,
                                                'LPG' || '-' || clh.claim || '-' || clh.transac || '-' || coalesce(clm.cover,0) KRBRECIN,
                                                '' DORDCSG,
                                                case 	when	coalesce(coi.companyc,0) in (1,12)
                                                        then	'1'
                                                        else	'2'end DPLANO,
                                                cast (coalesce(coi.share,100) as numeric(7,4)) VTXCSGI,
                                        cast (coalesce  (   case  when    clm.currency = clh.moneda_cod
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
                                                                end, 0) * coalesce(coi.share,100) / 100  as numeric (12,2))VMTCSGI,
                                                cast (clm.cover as varchar) KRCTPCBT,
                                                '' KRCTPNDI, --no disponible
                                                clh.bene_code KRBPENS,
                                                '' KRBRENDA,
                                                cast (coi.companyc as varchar) DCODCSG,
                                                '' DORDDSPIN,
                                                'LPG' DCOMPA,
                                                '' DMARCA,
                                                '' DINDDESD,
                                        case 	when	coalesce(coi.companyc,0) in (1,12)
                                                        then	'1'
                                                        else	'2' end KRCTPQTP
                                        from    (   select  cla.*,
                                                        clh.transac,
                                                        clh.oper_type,
                                                        clh.operdate,
                                                        clh.bene_code,
                                                        case    when    moneda_cod = 1
                                                                then    case    when    clh.currency = 2
                                                                                then    (   select  max(clh0.exchange)
                                                                                                from    usinsug01.claim_his clh0
                                                                                                where	clh0.claim = clh.claim
                                                                                                and     clh0.transac =
                                                                                                        (   select  max(clh1.transac)
                                                                                                        from    usinsug01.claim_his clh1
                                                                                                        where clh1.claim = clh.claim
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
                                                                else    0 end clh_exchange
                                                from    (   select  case    when    (cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                then    cla.claim
                                                                                else    null end claim,
                                                                        cla.usercomp,
                                                                        cla.company,
                                                                        pol.certype,
                                                                        cla.branch,
                                                                        cla.policy,
                                                                        cla.occurdat,
                                                                        case    when    (cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                then    coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                                from    usinsug01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                        (   select  max(cpl.currency)
                                                                                                                from    usinsug01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0)
                                                                                else    0 end moneda_cod
                                                                from    usinsug01.policy pol
                                                                join    usinsug01.claim cla on  cla.usercomp = pol.usercomp
                                                                                            and cla.company = pol.company
                                                                                            and cla.branch = pol.branch
                                                                                            and cla.policy = pol.policy
                                                                join    (   select  distinct clh.claim
                                                                        from    (	select	cast(codigint as varchar(2)) operation
                                                                                        from	usinsug01.table140
                                                                                        where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                                        join       usinsug01.claim_his clh
                                                                        ON   coalesce (clh.claim,0) > 0
                                                                        and     trim(clh.oper_type) = tcl.operation
                                                                        and     clh.operdate >= '12/31/2021') clh
                                                                ON 	    clh.claim = cla.claim
                                                                and		pol.bussityp = '1') cla
                                                join     usinsug01.claim_his clh on clh.claim = cla.claim
                                                join     (	select	cast(codigint as varchar(2)) operation
                                                                from	usinsug01.table140
                                                                where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                ON      trim(clh.oper_type) = tcl.operation
                                                and     clh.operdate <= '12/31/2023') clh
                                        join 	usinsug01.cl_m_cover clm on clm.usercomp = clh.usercomp
                                                                        and clm.company = clh.company
                                                                        and clm.claim = clh.claim
                                                                        and clm.movement = clh.transac
                                        join 	usinsug01.coinsuran coi on 	coi.usercomp = clh.usercomp 
                                                                        and     coi.company = clh.company
                                                                        and     coi.certype = clh.certype
                                                                        and     coi.branch = clh.branch
                                                                        and     coi.policy = clh.policy
                                                                        and     coi.effecdate <= clh.occurdat
                                                                        and     (coi.nulldate is null or coi.nulldate > clh.occurdat)
                                        --11.78s (todos) prod
                                        --1 m 13 s  dev                           	       
                                      )AS TMP 
                                      '''
    DF_LPG_INSUNIX_NEGO1 = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBCSGRI_INSUNIX_LPG_NEGO1).load()
    
    L_RBCSGRI_INSUNIX_LPG_NEGO2 = f'''
                                      (
                                        select  'D' as INDDETREC,
                                                'RBCSGRI' as TABLAIFRS17,
                                                /*par.cia || par.sep || clh.claim || par.sep || clh.transac || par.sep || coalesce(clm.cover,0) || par.sep || 1*/'' PK,
                                                '' DTPREG,
                                                '' TIOCPROC,
                                                '' TIOCFRM,
                                                '' TIOCTO,
                                                'PIG' KGIORIGM,
                                                'LPG' || '-' || clh.claim || '-' || clh.transac || '-' || coalesce(clm.cover,0) KRBRECIN,
                                                '' DORDCSG,
                                                '2' DPLANO,	
                                                cast ('100' - coalesce(clh.leadshare) as numeric (7,4) )VTXCSGI,
                                                cast (coalesce  (   case  when    clm.currency = clh.moneda_cod
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
                                                                end, 0)as numeric (12,2) )VMTCSGI,
                                                cast (clm.cover as varchar) KRCTPCBT,
                                                '' KRCTPNDI, --no disponible
                                                clh.bene_code KRBPENS,
                                                '' KRBRENDA,
                                                '1' DCODCSG,
                                                '' DORDDSPIN,
                                                'LPG' DCOMPA, 
                                                '' DMARCA,
                                                '' DINDDESD,
                                                '1' KRCTPQTP
                                        from    (   select  cla.*,
                                                        clh.transac,
                                                        clh.oper_type,
                                                        clh.operdate,
                                                        clh.bene_code,
                                                        case    when    moneda_cod = 1
                                                                then    case    when    clh.currency = 2
                                                                                then    (   select  max(clh0.exchange)
                                                                                                from    usinsug01.claim_his clh0
                                                                                                where	clh0.claim = clh.claim
                                                                                                and     clh0.transac =
                                                                                                        (   select  max(clh1.transac)
                                                                                                        from    usinsug01.claim_his clh1
                                                                                                        where clh1.claim = clh.claim
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
                                                                                                        where clh1.claim = clh.claim
                                                                                                        and     clh1.transac <= clh.transac
                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                else    1 end
                                                                else    0 end clh_exchange
                                                from    (   select  case    when    (cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                then    cla.claim
                                                                                else    null end claim,
                                                                        cla.usercomp,
                                                                        cla.company,
                                                                        cla.branch,
                                                                        cla.policy,
                                                                        cla.occurdat,
                                                                        pol.certype,
                                                                        pol.leadshare,
                                                                        case    when    (cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                then    coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                                from    usinsug01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                        (   select  max(cpl.currency)
                                                                                                                from    usinsug01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0)
                                                                                else    0 end moneda_cod
                                                                from    usinsug01.policy pol
                                                                join    usinsug01.claim cla on cla.usercomp = pol.usercomp
                                                                                        and cla.company = pol.company
                                                                                        and cla.branch = pol.branch
                                                                                        and cla.policy = pol.policy
                                                                join    (   select  distinct clh.claim
                                                                        from    (	select	cast(codigint as varchar(2)) operation
                                                                                        from	usinsug01.table140
                                                                                        where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                                        join     usinsug01.claim_his clh
                                                                        on      coalesce (clh.claim,0) > 0
                                                                        and     trim(clh.oper_type) = tcl.operation
                                                                        and     clh.operdate >= '12/31/2021') clh
                                                                on      clh.claim = cla.claim
                                                                and		pol.bussityp = '2') cla
                                                join  usinsug01.claim_his clh on clh.claim = cla.claim
                                                join    (	select	cast(codigint as varchar(2)) operation
                                                                from	usinsug01.table140
                                                                where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                on     trim(clh.oper_type) = tcl.operation
                                                and    clh.operdate <= '12/31/2023') clh 
                                        join 	usinsug01.cl_m_cover clm on clm.usercomp = clh.usercomp
                                                                        and clm.company = clh.company
                                                                        and clm.claim = clh.claim
                                                                        and clm.movement = clh.transac
                                        --8.785s (todos) prod
                                        --1m 43 s dev							                         	       
                                      )AS TMP 
                                      '''
    DF_LPG_INSUNIX_NEGO2 = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBCSGRI_INSUNIX_LPG_NEGO2).load()
    
    L_RBCSGRI_INSUNIX_LPV_NEGO1 = f'''
                                      (
                                        select 	'D' as INDDETREC,
                                                'RBCSGRI' as TABLAIFRS17,
                                                /*par.cia || par.sep || clh.claim || par.sep || clh.transac || par.sep || coalesce(clm.cover,0) || par.sep || coalesce(coi.companyc,0)*/ '' PK,
                                                '' DTPREG,
                                                '' TIOCPROC,
                                                '' TIOCFRM,
                                                '' TIOCTO,
                                                'PIV' KGIORIGM,
                                                'LPV' || '-' || clh.claim || '-' || clh.transac || '-' || coalesce(clm.cover,0) KRBRECIN,
                                                '' DORDCSG,
                                                case 	when	coalesce(coi.companyc,0) in (1,12)
                                                        then	'1'
                                                        else	'2' end DPLANO,
                                                cast (coalesce(coi.share,100) as numeric (7,4)) VTXCSGI,
                                                cast (coalesce  (   case  when    clm.currency = clh.moneda_cod
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
                                                                end, 0) * coalesce(coi.share,100) / 100 as numeric (12,2)) VMTCSGI,
                                                cast (clm.cover as varchar) KRCTPCBT,
                                                '' KRCTPNDI, --no disponible
                                                clh.bene_code KRBPENS,
                                                '' KRBRENDA,
                                                cast (coi.companyc as varchar) DCODCSG,
                                                '' DORDDSPIN,
                                                'LPV' DCOMPA,
                                                '' DMARCA,
                                                '' DINDDESD,
                                                case 	when	coalesce(coi.companyc,0) in (1,12)
                                                        then	'1'
                                                        else	'2' end KRCTPQTP
                                        from    (   select  cla.*,
                                                        clh.transac,
                                                        clh.oper_type,
                                                        clh.operdate,
                                                        clh.bene_code,
                                                        case    when    moneda_cod = 1
                                                                then    case    when    clh.currency = 2
                                                                                then    (   select  max(clh0.exchange)
                                                                                                from    usinsuv01.claim_his clh0
                                                                                                where	clh0.claim = clh.claim
                                                                                                and     clh0.transac =
                                                                                                        (   select  max(clh1.transac)
                                                                                                        from    usinsuv01.claim_his clh1
                                                                                                        where	clh1.claim = clh.claim
                                                                                                        and     clh1.transac <= clh.transac
                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                else 1 end
                                                                when 	moneda_cod = 2
                                                                then    case    when    clh.currency = 1
                                                                                then    (   select  max(clh0.exchange)
                                                                                                from    usinsuv01.claim_his clh0
                                                                                                where 	clh0.claim = clh.claim
                                                                                                and     clh0.transac =
                                                                                                        (   select  max(clh1.transac)
                                                                                                        from    usinsuv01.claim_his clh1
                                                                                                        where	clh1.claim = clh.claim
                                                                                                        and     clh1.transac <= clh.transac
                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                else    1 end
                                                                else    0 end clh_exchange
                                                from    (   select  case    when    cla.staclaim <> '6' and pol.certype = '2'
                                                                                then    cla.claim
                                                                                else    null end claim,
                                                                        cla.usercomp,
                                                                        cla.company,
                                                                        pol.certype,
                                                                        cla.branch,
                                                                        cla.policy,
                                                                        cla.occurdat,
                                                                        case    when    cla.staclaim <> '6' and pol.certype = '2'
                                                                                then    coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                                from    usinsuv01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                        (   select  max(cpl.currency)
                                                                                                                from    usinsuv01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0)
                                                                                else    0 end moneda_cod
                                                                from    usinsuv01.policy pol
                                                                join 	usinsuv01.claim cla on  cla.usercomp = pol.usercomp
                                                                                        and     cla.company = pol.company
                                                                                        and     cla.branch = pol.branch
                                                                                        and     cla.policy = pol.policy
                                                                join    (   select  distinct clh.claim
                                                                        from    (	select	cast(codigint as varchar(2)) operation
                                                                                        from	usinsug01.table140
                                                                                        where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                                        join usinsuv01.claim_his clh
                                                                        on   coalesce (clh.claim,0) > 0
                                                                        and     trim(clh.oper_type) = tcl.operation
                                                                        and     clh.operdate >= '12/31/2015') clh
                                                                on  	clh.claim = cla.claim
                                                                and		pol.bussityp = '1') cla
                                                join    usinsuv01.claim_his clh on clh.claim = cla.claim
                                                join 	(	select	cast(codigint as varchar(2)) operation
                                                                from	usinsug01.table140
                                                                where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                on      trim(clh.oper_type) = tcl.operation
                                                and     clh.operdate <= '12/31/2023') clh
                                        join    usinsuv01.cl_m_cover clm on 	clm.usercomp = clh.usercomp
                                                                        and 	clm.company = clh.company
                                                                        and 	clm.claim = clh.claim
                                                                        and 	clm.movement = clh.transac
                                        join 	usinsuv01.coinsuran coi on      coi.usercomp = clh.usercomp
                                                                        and     coi.company = clh.company
                                                                        and     coi.certype = clh.certype
                                                                        and     coi.branch = clh.branch
                                                                        and     coi.policy = clh.policy
                                                                        and     coi.effecdate <= clh.occurdat
                                                                        and     (coi.nulldate is null or coi.nulldate > clh.occurdat)
                                        --11.138s (todos) prod
                                        --26.467 s dev                         	       
                                      )AS TMP 
                                      '''
    DF_LPV_INSUNIX_NEGO1 = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBCSGRI_INSUNIX_LPV_NEGO1).load()
    
    L_RBCSGRI_INSUNIX_LPV_NEGO2 = f'''
                                      (
                                        select 	'D' as INDDETREC,
                                                'RBCSGRI' as TABLAIFRS17,
                                                /*par.cia || par.sep || clh.claim || par.sep || clh.transac || par.sep || coalesce(clm.cover,0) || par.sep || 1*/ '' PK,
                                                '' DTPREG,
                                                '' TIOCPROC,
                                                '' TIOCFRM,
                                                '' TIOCTO,
                                                'PIV' KGIORIGM,
                                                'LPV' || '-' || clh.claim || '-'|| clh.transac || '-' || coalesce(clm.cover,0) KRBRECIN,
                                                '' DORDCSG,
                                                '2' DPLANO,
                                                cast (100 - coalesce(clh.leadshare)as numeric(7,4)) VTXCSGI,
                                                cast (coalesce  (   case  when    clm.currency = clh.moneda_cod
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
                                                                end, 0) as numeric (12,2)) VMTCSGI,
                                                cast (clm.cover as varchar) KRCTPCBT,
                                                '' KRCTPNDI, --no disponible
                                                clh.bene_code KRBPENS,
                                                '' KRBRENDA,
                                                '1' DCODCSG,
                                                '' DORDDSPIN,
                                                'LPV' DCOMPA, 
                                                '' DMARCA,
                                                '' DINDDESD,
                                                '1' KRCTPQTP
                                        from    (   select  cla.*,
                                                        clh.transac,
                                                        clh.oper_type,
                                                        clh.operdate,
                                                        clh.bene_code,
                                                        case    when    moneda_cod = 1
                                                                then    case    when    clh.currency = 2
                                                                                then    (   select  max(clh0.exchange)
                                                                                                from    usinsuv01.claim_his clh0
                                                                                                where	clh0.claim = clh.claim
                                                                                                and     clh0.transac =
                                                                                                        (   select  max(clh1.transac)
                                                                                                        from    usinsuv01.claim_his clh1
                                                                                                        where clh1.claim = clh.claim
                                                                                                        and     clh1.transac <= clh.transac
                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                else 1 end
                                                                when 	moneda_cod = 2
                                                                then    case    when    clh.currency = 1
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
                                                                else    0 end clh_exchange
                                                from    (   select  case    when    cla.staclaim <> '6' and pol.certype = '2'
                                                                                then    cla.claim
                                                                                else    null end claim,
                                                                        cla.usercomp,
                                                                        cla.company,
                                                                        cla.branch,
                                                                        cla.policy,
                                                                        cla.occurdat,
                                                                        pol.certype,
                                                                        pol.leadshare,
                                                                        case    when    cla.staclaim <> '6' and pol.certype = '2'
                                                                                then    coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                                from    usinsuv01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                        (   select  max(cpl.currency)
                                                                                                                from    usinsuv01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0)
                                                                                else    0 end moneda_cod
                                                                from    usinsuv01.policy pol
                                                                join    usinsuv01.claim cla on 	cla.usercomp = pol.usercomp
                                                                                        and     cla.company = pol.company
                                                                                        and     cla.branch = pol.branch
                                                                                        and     cla.policy = pol.policy
                                                                join    (   select  distinct clh.claim
                                                                        from    (	select	cast(codigint as varchar(2)) operation
                                                                                        from	usinsug01.table140
                                                                                        where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                                        join   usinsuv01.claim_his clh
                                                                        ON   coalesce (clh.claim,0) > 0
                                                                        and     trim(clh.oper_type) = tcl.operation
                                                                        and     clh.operdate >= '12/31/2015') clh on  clh.claim = cla.claim
                                                                                                                and pol.bussityp = '2'	
                                                        ) cla
                                                join  usinsuv01.claim_his clh on 	clh.claim = cla.claim
                                                join 		(	select	cast(codigint as varchar(2)) operation
                                                                from	usinsug01.table140
                                                                where	codigint in (select operation from usinsug01.tab_cl_ope where pay_amount = 1)) tcl --solo pagos
                                                on  trim(clh.oper_type) = tcl.operation
                                                and clh.operdate <= '12/31/2023') clh
                                        join 	usinsuv01.cl_m_cover clm on 	clm.usercomp = clh.usercomp
                                                                        and 	clm.company = clh.company
                                                                        and 	clm.claim = clh.claim
                                                                        and 	clm.movement = clh.transac
                                        --4.22s (todos) PROD
                                        --2.291 s dev                    	       
                                      )AS TMP 
                                      '''
    DF_LPV_INSUNIX_NEGO2 = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBCSGRI_INSUNIX_LPV_NEGO2).load()
    

    L_DF_RBCSGRI_INSUNIX = DF_LPG_INSUNIX_NEGO1.union(DF_LPG_INSUNIX_NEGO2).union(DF_LPV_INSUNIX_NEGO1).union(DF_LPV_INSUNIX_NEGO2)

    L_RBCSGRI_VTIME_LPG_NEGO1  = f'''
                                     (
                                        select 	'D' as INDDETREC,
                                                'RBCSGRI' as TABLAIFRS17,
                                                /*'LPV' || '-' || clh."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" || '-' || coalesce(clm."NCOVER",0) || '-' || coalesce(coi."NCOMPANY",0)*/ '' PK,
                                                '' DTPREG,
                                                '' TIOCPROC,
                                                '' TIOCFRM,
                                                '' TIOCTO,
                                                'PVG' KGIORIGM,
                                                'LPG' || '-' || clh."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" || '-' || coalesce(clm."NCOVER",0) KRBRECIN,
                                                '' DORDCSG,
                                                case 	when	coalesce(coi."NCOMPANY",0) in (1,12)
                                                                then	'1'
                                                                else	'2' end DPLANO,
                                                cast (coalesce(coi."NSHARE",100) as numeric (7,4)) VTXCSGI,
                                                cast (case	when	clh.MONEDA_COD = 1
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
                                                                else	0 
                                                end * coalesce(coi."NSHARE",100) / 100 as numeric (12,2)) VMTCSGI,
                                                cast (clm."NCOVER" as varchar) KRCTPCBT,
                                                '' KRCTPNDI, --no disponible
                                                clh."SCLIENT" KRBPENS,
                                                '' KRBRENDA,
                                                cast (coi."NCOMPANY" as varchar) DCODCSG,
                                                '' DORDDSPIN,
                                                'LPG' DCOMPA,
                                                '' DMARCA,
                                                '' DINDDESD,
                                                case 	when	coalesce(coi."NCOMPANY",0) in (1,12)
                                                                then	'1'
                                                                else	'2' end KRCTPQTP
                                        from    (   select  cla.*,
                                                        clh."NCASE_NUM",
                                                        clh."NDEMAN_TYPE",
                                                                clh."NTRANSAC",
                                                                clh."NOPER_TYPE",
                                                                clh."SCLIENT",
                                                                cast(clh."DOPERDATE" as date) "DOPERDATE"
                                                from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                then    cla."NCLAIM"
                                                                                else    null end "NCLAIM",
                                                                        cla."SCERTYPE",
                                                                        cla."NBRANCH",   
                                                                        cla."NPOLICY",
                                                                        cla."NCERTIF",
                                                                        pol."NPRODUCT",
                                                                        cast(cla."DOCCURDAT" as date) "DOCCURDAT",
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
                                                                                else    0 end MONEDA_COD
                                                                from    usvtimg01."POLICY" pol
                                                                join    usvtimg01."CLAIM" cla on  cla."SCERTYPE" = pol."SCERTYPE"
                                                                                        and cla."NPOLICY" = pol."NPOLICY"
                                                                                        and cla."NBRANCH" = pol."NBRANCH"
                                                                join   (   select  distinct clh."NCLAIM"
                                                                        from    (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                from	usvtimg01."CONDITION_SERV" cs 
                                                                                where	"NCONDITION" in (73)) csv --solo pagos
                                                                        join    usvtimg01."CLAIM_HIS" clh
                                                                        on     coalesce (clh."NCLAIM",0) > 0
                                                                        and     clh."NOPER_TYPE" = csv."SVALUE"
                                                                        and     clh."DOPERDATE" >= '12/31/2015') clh
                                                                on      clh."NCLAIM" = cla."NCLAIM"
                                                                and		pol."SBUSSITYP" = '1') cla
                                                                join 	usvtimg01."CLAIM_HIS" clh on clh."NCLAIM" = cla."NCLAIM"
                                                                join   (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                                        where	"NCONDITION" in (73)) csv --solo pagos
                                                                on      clh."NOPER_TYPE" = csv."SVALUE"
                                                and     clh."DOPERDATE" <= '12/31/2023') clh
                                        join usvtimg01."CL_M_COVER" clm on clm."NCLAIM" = clh."NCLAIM"
                                                                        and clm."NCASE_NUM" = clh."NCASE_NUM"
                                                                        and clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                        and clm."NTRANSAC" = clh."NTRANSAC"
                                        join usvtimg01."COINSURAN" coi  on coi."SCERTYPE" = clh."SCERTYPE"
                                                                        and coi."NBRANCH" = clh."NBRANCH"
                                                                        and coi."NPRODUCT" = clh."NPRODUCT"
                                                                        and coi."NPOLICY" = clh."NPOLICY"
                                                                        and coi."NCOMPANY" is not null
                                                                        and coi."DEFFECDATE" <= clh."DOCCURDAT"
                                                                        and (coi."DNULLDATE" is null or coi."DNULLDATE" > clh."DOCCURDAT")
                                        --6.589s (TODOS)
                                     ) AS TMP    
                                     '''

    DF_LPG_VTIME_NEGO1 = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBCSGRI_VTIME_LPG_NEGO1).load()

    L_RBCSGRI_VTIME_LPG_NEGO2  = f'''
                                     (
                                        select 	'D' as INDDETREC,
                                                'RBCSGRI' as TABLAIFRS17,
                                                /*par.cia || par.sep1 || clh."NCLAIM" || par.sep2 || clh."NCASE_NUM" || par.sep1 || clh."NTRANSAC" || par.sep1 || coalesce(clm."NCOVER",0) || par.sep1 || 1*/'' PK,
                                                '' DTPREG,
                                                '' TIOCPROC,
                                                '' TIOCFRM,
                                                '' TIOCTO,
                                                'PVG' KGIORIGM,
                                                'LPG' || '-' || clh."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" || '-' || coalesce(clm."NCOVER",0) KRBRECIN,
                                                '' DORDCSG,
                                                '2' DPLANO,
                                                cast (100 - coalesce(clh."NLEADSHARE",0) as numeric(7,4)) VTXCSGI,
                                                cast (case	when	clh.MONEDA_COD = 1
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
                                                                else	0 end as numeric(12,2) ) VMTCSGI,
                                                cast (clm."NCOVER" as varchar) KRCTPCBT,
                                                '' KRCTPNDI, --no disponible
                                                clh."SCLIENT" KRBPENS,
                                                '' KRBRENDA,
                                                '1' DCODCSG,
                                                '' DORDDSPIN,
                                                'LPG' DCOMPA,
                                                '' DMARCA,
                                                '' DINDDESD,
                                                '1' KRCTPQTP
                                        from    (   select  cla.*,
                                                                                clh."NCASE_NUM",
                                                                                clh."NDEMAN_TYPE",
                                                        clh."NTRANSAC",
                                                        clh."NOPER_TYPE",
                                                        clh."SCLIENT",
                                                        cast(clh."DOPERDATE" as date) "DOPERDATE"
                                                from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                then    cla."NCLAIM"
                                                                                else    null end "NCLAIM",
                                                                        cla."SCERTYPE",
                                                                        cla."NBRANCH",   
                                                                        cla."NPOLICY",
                                                                        cla."NCERTIF",
                                                                        pol."NPRODUCT",
                                                                        pol."NLEADSHARE",
                                                                        cast(cla."DOCCURDAT" as date) "DOCCURDAT",
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
                                                                                else    0 end MONEDA_COD
                                                                                        from    usvtimg01."POLICY" pol
                                                                join    usvtimg01."CLAIM" cla on  cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                                        and cla."NPOLICY" = pol."NPOLICY"
                                                                                                                        and cla."NBRANCH" = pol."NBRANCH"
                                                                join    (   select  distinct clh."NCLAIM"
                                                                        from    (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                from	usvtimg01."CONDITION_SERV" cs 
                                                                                where	"NCONDITION" in (73)) csv --solo pagos
                                                                        join    usvtimg01."CLAIM_HIS" clh
                                                                        on   coalesce (clh."NCLAIM",0) > 0
                                                                        and     clh."NOPER_TYPE" = csv."SVALUE"
                                                                        and     clh."DOPERDATE" >= '12/31/2015') clh
                                                                on  clh."NCLAIM" = cla."NCLAIM"
                                                                and	pol."SBUSSITYP" = '2') cla
                                                                join usvtimg01."CLAIM_HIS" clh on clh."NCLAIM" = cla."NCLAIM"
                                                                join 	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                                        where	"NCONDITION" in (73)) csv --solo pagos
                                                                on      clh."NOPER_TYPE" = csv."SVALUE"
                                                and     clh."DOPERDATE" <= '12/31/2023') clh
                                        join 	usvtimg01."CL_M_COVER" clm  on  clm."NCLAIM" = clh."NCLAIM"
                                                                        and clm."NCASE_NUM" = clh."NCASE_NUM"
                                        --6.589s (TODOS)
                                        --16.908 s DEV                                    
                                     ) AS TMP    
                                     '''

    DF_LPG_VTIME_NEGO2 = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBCSGRI_VTIME_LPG_NEGO2).load()

    
    L_RBCSGRI_VTIME_LPV_NEGO1  = f'''
                                    (
                                        select 	'D' as INDDETREC,
                                                'RBCSGRI' as TABLAIFRS17,
                                                        /*par.cia || par.sep1 || clh."NCLAIM" || par.sep2 || clh."NCASE_NUM" || par.sep1 || clh."NTRANSAC" || par.sep1 || coalesce(clm."NCOVER",0) || par.sep1 || coalesce(coi."NCOMPANY",0)*/ '' PK,
                                                        '' DTPREG,
                                                        '' TIOCPROC,
                                                        '' TIOCFRM,
                                                        '' TIOCTO,
                                                        'PVV' KGIORIGM,
                                                        'LPV' || '-' || clh."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" || '-' || coalesce(clm."NCOVER",0) KRBRECIN,
                                                        '' DORDCSG,
                                                        case 	when	coalesce(coi."NCOMPANY",0) in (1,12)
                                                                        then	'1'
                                                                        else	'2' end DPLANO,
                                                        cast (coalesce(coi."NSHARE",100) as numeric (7,4)) VTXCSGI,
                                                        cast (case	when	clh.MONEDA_COD = 1
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
                                                                        else	0 end * coalesce(coi."NSHARE",100) / 100 as numeric (12,2) )VMTCSGI,
                                                        cast (clm."NCOVER" as varchar) KRCTPCBT,
                                                        '' KRCTPNDI, --no disponible
                                                        clh."SCLIENT" KRBPENS,
                                                        '' KRBRENDA,
                                                        cast (coi."NCOMPANY" as varchar) DCODCSG,
                                                        '' DORDDSPIN,
                                                        'LPV' DCOMPA,
                                                        '' DMARCA,
                                                        '' DINDDESD,
                                                        case 	when	coalesce(coi."NCOMPANY",0) in (1,12)
                                                                        then	'1'
                                                                        else	'2' end KRCTPQTP
                                        from    (   select  cla.*,
                                                                                clh."NCASE_NUM",
                                                                                clh."NDEMAN_TYPE",
                                                        clh."NTRANSAC",
                                                        clh."NOPER_TYPE",
                                                        clh."SCLIENT",
                                                        cast(clh."DOPERDATE" as date) "DOPERDATE"
                                                from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                then    cla."NCLAIM"
                                                                                else    null end "NCLAIM",
                                                                        cla."SCERTYPE",
                                                                        cla."NBRANCH",   
                                                                        cla."NPOLICY",
                                                                        cla."NCERTIF",
                                                                        pol."NPRODUCT",
                                                                        cast(cla."DOCCURDAT" as date) "DOCCURDAT",
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
                                                                                else    0 end MONEDA_COD
                                                                                        from    usvtimv01."POLICY" pol
                                                                join    usvtimv01."CLAIM" cla 	on  cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                                                and cla."NPOLICY" = pol."NPOLICY"
                                                                                                                                and cla."NBRANCH" = pol."NBRANCH"
                                                                join    (   select  distinct clh."NCLAIM"
                                                                        from    (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                        from	usvtimv01."CONDITION_SERV" cs 
                                                                                                                                        where	"NCONDITION" in (73)) csv --solo pagos
                                                                        join    usvtimv01."CLAIM_HIS" clh
                                                                        on    coalesce (clh."NCLAIM",0) > 0
                                                                        and     clh."NOPER_TYPE" = csv."SVALUE"
                                                                        and     clh."DOPERDATE" >= '12/31/2012') clh
                                                                on      clh."NCLAIM" = cla."NCLAIM"
                                                                and		pol."SBUSSITYP" = '1') cla
                                                                join 	usvtimv01."CLAIM_HIS" clh on clh."NCLAIM" = cla."NCLAIM"
                                                                join 	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                        from	usvtimv01."CONDITION_SERV" cs 
                                                                                        where	"NCONDITION" in (73)) csv --solo pagos
                                                                on 	    clh."NOPER_TYPE" = csv."SVALUE"
                                                and     clh."DOPERDATE" <= '12/31/2023') clh
                                        join 	usvtimv01."CL_M_COVER" clm  on  clm."NCLAIM" = clh."NCLAIM"
                                                                        and clm."NCASE_NUM" = clh."NCASE_NUM"
                                                                        and clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                        and clm."NTRANSAC" = clh."NTRANSAC"
                                        join 	usvtimv01."COINSURAN" coi   on  coi."SCERTYPE" = clh."SCERTYPE"
                                                                        and coi."NBRANCH" = clh."NBRANCH"
                                                                        and coi."NPRODUCT" = clh."NPRODUCT"
                                                                        and coi."NPOLICY" = clh."NPOLICY"
                                                                        and coi."NCOMPANY" is not null
                                                                        and coi."DEFFECDATE" <= clh."DOCCURDAT"
                                                                        and (coi."DNULLDATE" is null or coi."DNULLDATE" > clh."DOCCURDAT")
                                        --6.589s (TODOS)
                                        --123 ms dev
                                    ) AS TMP
                                    '''

    DF_LPV_VTIME_NEGO1 = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBCSGRI_VTIME_LPV_NEGO1).load()

    L_RBCSGRI_VTIME_LPV_NEGO2  = f'''
                                    (
                                      select 	'D' as INDDETREC,
                                                'RBCSGRI' as TABLAIFRS17,
                                                /*par.cia || par.sep1 || clh."NCLAIM" || par.sep2 || clh."NCASE_NUM" || par.sep1 || clh."NTRANSAC" || par.sep1 || coalesce(clm."NCOVER",0) || par.sep1 || 1*/ '' PK,
                                                '' DTPREG,
                                                '' TIOCPROC,
                                                '' TIOCFRM,
                                                '' TIOCTO,
                                                'PVV' KGIORIGM,
                                                'LPV' ||'-' || clh."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" || '-'|| coalesce(clm."NCOVER",0) KRBRECIN,
                                                '' DORDCSG,
                                                '2' DPLANO,
                                                cast (100 - coalesce(clh."NLEADSHARE",0) as numeric (7,4)) VTXCSGI,
                                                cast (case	when	clh.MONEDA_COD = 1
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
                                                                else	0 end as numeric (12,2))VMTCSGI,
                                                cast (clm."NCOVER" as varchar) KRCTPCBT,
                                                '' KRCTPNDI, --no disponible
                                                clh."SCLIENT" KRBPENS,
                                                '' KRBRENDA,
                                                '1' DCODCSG,
                                                '' DORDDSPIN,
                                                'LPV' DCOMPA,
                                                '' DMARCA,
                                                '' DINDDESD,
                                                '1' KRCTPQTP
                                        from    (   select  cla.*,
                                                        clh."NCASE_NUM",
                                                        clh."NDEMAN_TYPE",
                                                                clh."NTRANSAC",
                                                                clh."NOPER_TYPE",
                                                                clh."SCLIENT",
                                                        cast(clh."DOPERDATE" as date) "DOPERDATE"
                                                from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                then    cla."NCLAIM"
                                                                                else    null end "NCLAIM",
                                                                        cla."SCERTYPE",
                                                                        cla."NBRANCH",   
                                                                        cla."NPOLICY",
                                                                        cla."NCERTIF",
                                                                        pol."NPRODUCT",
                                                                        pol."NLEADSHARE",
                                                                        cast(cla."DOCCURDAT" as date) "DOCCURDAT",
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
                                                                                else    0 end MONEDA_COD
                                                                                        from    usvtimv01."POLICY" pol
                                                                join    usvtimv01."CLAIM" cla on   cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                and  cla."NPOLICY" = pol."NPOLICY"
                                                                                                and  cla."NBRANCH" = pol."NBRANCH"
                                                                join    (   select  distinct clh."NCLAIM"
                                                                        from    (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                from	usvtimv01."CONDITION_SERV" cs 
                                                                                where	"NCONDITION" in (73)) csv --solo pagos
                                                                        join 	usvtimv01."CLAIM_HIS" clh
                                                                        on   coalesce (clh."NCLAIM",0) > 0
                                                                        and     clh."NOPER_TYPE" = csv."SVALUE"
                                                                        and     clh."DOPERDATE" >= '12/31/2021') clh
                                                                on      clh."NCLAIM" = cla."NCLAIM"
                                                                and		pol."SBUSSITYP" = '2') cla
                                                                join usvtimv01."CLAIM_HIS" clh on clh."NCLAIM" = cla."NCLAIM"
                                                                join 	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                        from	usvtimv01."CONDITION_SERV" cs 
                                                                        where	"NCONDITION" in (73)) csv --solo pagos
                                                                on      clh."NOPER_TYPE" = csv."SVALUE"
                                                and     clh."DOPERDATE" <= '12/31/2023') clh
                                        join 	usvtimv01."CL_M_COVER" clm
                                        on 		clm."NCLAIM" = clh."NCLAIM"
                                        and 	clm."NCASE_NUM" = clh."NCASE_NUM"
                                        and 	clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                        and 	clm."NTRANSAC" = clh."NTRANSAC"  
                                    ) AS TMP
                                    '''

    DF_LPV_VTIME_NEGO2 = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("fetchsize",10000).option("dbtable",L_RBCSGRI_VTIME_LPV_NEGO2).load()

    L_DF_RBCSGRI_VTIME = DF_LPG_VTIME_NEGO1.union(DF_LPG_VTIME_NEGO2).union(DF_LPV_VTIME_NEGO1).union(DF_LPV_VTIME_NEGO2)


#     L_RBCSGRI_INSIS = f'''
#                             (
#                             ) AS TMP
#                              '''
#     L_DF_RBCSGRI_INSIS = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_RBCSGRI_INSIS).load()

    L_DF_RBCSGRI = L_DF_RBCSGRI_INSUNIX.union(L_DF_RBCSGRI_VTIME)#.union(L_DF_RBCSGRI_INSIS)

    return L_DF_RBCSGRI  
