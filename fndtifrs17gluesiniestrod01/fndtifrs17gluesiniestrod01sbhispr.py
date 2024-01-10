def get_data(glue_context, connection):

        
        L_SBHISPR_INSUNIX_LPG = '''
                                (
                                   select       'D' as INDDETREC,
                                                'SBHISPR' as TABLAIFRS17,
                                                '' as PK,
                                                '' as DTPREG,
                                                '' as TIOCPROC,
                                                clh.operdate as TIOCFRM,
                                                '' as TIOCTO,
                                                'PIG' as KGIORIGM,
                                                clh.claim  as KSBSIN,
                                                cast (clh.claim as varchar) as  DNUMSIN,
                                                cast (clh.policy as varchar )  as  DNUMAPO,
                                                cast (clh.certif  as varchar )as DNMCERT,
                                                cast (clm.cover as varchar) as KGCTPCBT,
                                                cast (clh.operdate as varchar ) as TPROVI,
                                                clh.transac as DSEQMOV,
                                                trim(clh.oper_type) as  KSCTPMPR,
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
                                                                end, 0) * case when clh.tipo = 1 then 1 else 0 end as varchar )  as VMTPROVI,
                                                cast ( coalesce  (   case  when    clm.currency = clh.moneda_cod
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
                                                                end, 0) * case when clh.tipo = 2 then 1 else 0 end  as varchar) as VMTPRVAR,
                                                '' as TULTALT,
                                                '' as DUSRUPD,
                                                'LPG' DCOMPA,
                                                '' as DMARCA,
                                                '' as KSBSUBSN,
                                                '' as KSCMTDPR,
                                                clh.aseg_cod KEBENTID_SN,
                                                '' as DENTIDSO,
                                                '' as DIDADELES,
                                                '' as TCRIACAO
                                        from    (   select  cla.*,
                                                        clh.transac,
                                                        clh.oper_type,
                                                        clh.operdate,
                                                        tcl.tipo,
                                                        case    when    moneda_cod = 1
                                                                then    case    when    clh.currency = 2
                                                                                then    (   select  max(clh0.exchange)
                                                                                                from    usinsug01.claim_his clh0
                                                                                                where clh0.claim = clh.claim
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
                                                        from    (  select case    when    (cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                        then    cla.claim
                                                                        else    null 
                                                                        end claim,
                                                                        cla.policy,
                                                                        cla.certif,
                                                                        cla.usercomp,
                                                                        cla.company,
                                                                        case    when    (cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                then    coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                                from    usinsug01.curren_pol cpl
                                                                                                                where cpl.usercomp = cla.usercomp
                                                                                                                and     cpl.company = cla.company
                                                                                                                and     cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch
                                                                                                                and     cpl.policy = cla.policy
                                                                                                                and     cpl.certif = cla.certif),
                                                                                                                ( select  max(cpl.currency)
                                                                                                                        from    usinsug01.curren_pol cpl
                                                                                                                        where cpl.usercomp = cla.usercomp
                                                                                                                        and     cpl.company = cla.company
                                                                                                                        and     cpl.certype = pol.certype
                                                                                                                        and     cpl.branch = cla.branch
                                                                                                                        and     cpl.policy = cla.policy)),0)
                                                                                else    0 end moneda_cod,
                                                                        coalesce(	coalesce((	select  max(bene_code)
                                                                                                from    usinsug01.claimbenef
                                                                                                where   usercomp = cla.usercomp
                                                                                                and     company = cla.company
                                                                                                and     claim = cla.claim
                                                                                                and     bene_type = 2),
                                                                                        (	select  max(bene_code)
                                                                                                from    usinsug01.claimbenef
                                                                                                where   usercomp = cla.usercomp
                                                                                                and     company = cla.company
                                                                                                and     claim = cla.claim
                                                                                                and     bene_type = 8)),
                                                                                coalesce((	select  max(bene_code)
                                                                                                from    usinsug01.claimbenef
                                                                                                where   usercomp = cla.usercomp
                                                                                                and     company = cla.company
                                                                                                and     claim = cla.claim
                                                                                                and     bene_type = 19),
                                                                                        (	select  max(bene_code)
                                                                                                from    usinsug01.claimbenef
                                                                                                where   usercomp = cla.usercomp
                                                                                                and     company = cla.company
                                                                                                and     claim = cla.claim
                                                                                                and     bene_type = 18))) aseg_cod
                                                                from    usinsug01.policy pol
                                                                join    usinsug01.claim cla on  cla.usercomp = pol.usercomp
                                                                                        and cla.company = pol.company
                                                                                        and cla.branch = pol.branch
                                                                                        and cla.policy = pol.policy 
                                                                join    (   select  distinct clh.claim
                                                                        from    (   select  cast(tcl.operation as varchar(2)) operation
                                                                                        from    usinsug01.tab_cl_ope tcl
                                                                                        where   (tcl.reserve = 1 or tcl.ajustes = 1)) tcl --solo reservas y ajustes
                                                                        join usinsug01.claim_his clh
                                                                        on   coalesce (clh.claim,0) > 0
                                                                        and     trim(clh.oper_type) = tcl.operation
                                                                        and     clh.operdate >= '12/31/2021') clh
                                                                on clh.claim = cla.claim limit 10) cla
                                                        join usinsug01.claim_his clh on clh.claim = cla.claim
                                                        join ( select  case    when tcl.reserve = 1 then 1
                                                                                when tcl.ajustes = 1 then 2
                                                                                when tcl.pay_amount = 1 then 3
                                                                                else 0 end tipo,
                                                                        cast(tcl.operation as varchar(2)) operation
                                                                from    usinsug01.tab_cl_ope tcl
                                                                where   (tcl.reserve = 1 or tcl.ajustes = 1)) tcl --solo reservas y ajustes
                                                on      trim(clh.oper_type) = tcl.operation
                                                and     clh.operdate <= '12/31/2023'
                                                limit 10) clh
                                        join 	usinsug01.cl_m_cover clm 	on 	clm.usercomp = clh.usercomp
                                                                        and clm.company = clh.company
                                                                        and clm.claim = clh.claim
                                                                        and clm.movement = clh.transac
                                                                        --1m13s dev3  
                                ) AS TMP
                                '''
        
        L_DF_SBHISPR_INSUNIX_LPG = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBHISPR_INSUNIX_LPG).load()

        L_SBHISPR_INSUNIX_LPV = '''
                                (
                                   select	'D' as INDDETREC,
                                                'SBHISPR' as TABLAIFRS17,
                                                '' as  PK,
                                                '' as DTPREG,
                                                '' as TIOCPROC,
                                                clh.operdate as TIOCFRM,
                                                '' as TIOCTO,
                                                'PIV' as KGIORIGM,
                                                clh.claim  as KSBSIN,
                                                cast (clh.claim as VARCHAR ) as  DNUMSIN,
                                                cast (clh.policy as VARCHAR ) as  DNUMAPO,
                                                cast (clh.certif as VARCHAR ) as  DNMCERT,
                                                cast ( clm.cover as VARCHAR )  as KGCTPCBT,
                                                cast ( clh.operdate as VARCHAR ) as  TPROVI,
                                                clh.transac as DSEQMOV,
                                                trim(clh.oper_type) KSCTPMPR,
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
                                                                end, 0) * case when clh.tipo = 1 then 1 else 0 end as varchar) as VMTPROVI,
                                                cast ( coalesce  (   case  when    clm.currency = clh.moneda_cod
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
                                                                end, 0) * case when clh.tipo = 2 then 1 else 0 end as varchar) as VMTPRVAR,
                                                ''  as TULTALT,  
                                                '' as DUSRUPD,
                                                'LPV' as  DCOMPA,
                                                '' as DMARCA,
                                                '' as KSBSUBSN,
                                                '' as KSCMTDPR,
                                                clh.aseg_cod KEBENTID_SN,
                                                '' as DENTIDSO,
                                                '' as DIDADELES,
                                                '' as TCRIACAO
                                        from    (   select  cla.*,
                                                        clh.transac,
                                                        clh.oper_type,
                                                        clh.operdate,
                                                        tcl.tipo,
                                                        case    when    moneda_cod = 1
                                                                then    case    when    clh.currency = 1
                                                                                then    1
                                                                                when	clh.currency = 2
                                                                                then    (   select  max(clh0.exchange)
                                                                                                from    usinsuv01.claim_his clh0
                                                                                                where clh0.claim = clh.claim
                                                                                                and     clh0.transac =
                                                                                                        (   select  max(clh1.transac)
                                                                                                        from    usinsuv01.claim_his clh1
                                                                                                        where clh1.claim = clh.claim
                                                                                                        and     clh1.transac <= clh.transac
                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                else 1 end
                                                                when moneda_cod = 2
                                                                then    case    when    clh.currency = 2
                                                                                then    1
                                                                                when clh.currency = 1
                                                                                then    (   select  max(clh0.exchange)
                                                                                                from   usinsuv01.claim_his clh0
                                                                                                where clh0.claim = clh.claim
                                                                                                and     clh0.transac =
                                                                                                        (   select  max(clh1.transac)
                                                                                                        from    usinsuv01.claim_his clh1
                                                                                                        where clh1.claim = clh.claim
                                                                                                        and     clh1.transac <= clh.transac
                                                                                                        and     clh1.exchange not in (1,0)))
                                                                                else    1 end
                                                                else    0 end clh_exchange
                                                from    (   select  case    when    (cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                then    cla.claim
                                                                                else    null end claim,
                                                                        cla.policy,
                                                                        cla.certif,
                                                                        cla.usercomp,
                                                                        cla.company,
                                                                        case    when    (cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                then    coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                                from    usinsuv01.curren_pol cpl
                                                                                                                where cpl.usercomp = cla.usercomp
                                                                                                                and     cpl.company = cla.company
                                                                                                                and     cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch
                                                                                                                and     cpl.policy = cla.policy
                                                                                                                and     cpl.certif = cla.certif),
                                                                                                        (   select  max(cpl.currency)
                                                                                                                from    usinsuv01.curren_pol cpl
                                                                                                                where cpl.usercomp = cla.usercomp
                                                                                                                and     cpl.company = cla.company
                                                                                                                and     cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch
                                                                                                                and     cpl.policy = cla.policy)),0)
                                                                                else    0 end moneda_cod,
                                                                        coalesce(	coalesce((	select  max(bene_code)
                                                                                                from    usinsuv01.claimbenef
                                                                                                where   usercomp = cla.usercomp
                                                                                                and     company = cla.company
                                                                                                and     claim = cla.claim
                                                                                                and     bene_type = 2),
                                                                                        (	select  max(bene_code)
                                                                                                from    usinsuv01.claimbenef
                                                                                                where   usercomp = cla.usercomp
                                                                                                and     company = cla.company
                                                                                                and     claim = cla.claim
                                                                                                and     bene_type = 8)),
                                                                                coalesce((	select  max(bene_code)
                                                                                                from    usinsuv01.claimbenef
                                                                                                where   usercomp = cla.usercomp
                                                                                                and     company = cla.company
                                                                                                and     claim = cla.claim
                                                                                                and     bene_type = 19),
                                                                                        (	select  max(bene_code)
                                                                                                from    usinsuv01.claimbenef
                                                                                                where   usercomp = cla.usercomp
                                                                                                and     company = cla.company
                                                                                                and     claim = cla.claim
                                                                                                and     bene_type = 18))) aseg_cod
                                                                from    usinsuv01.policy pol
                                                                join    usinsuv01.claim cla on 	cla.usercomp = pol.usercomp
                                                                                        and cla.company = pol.company
                                                                                        and cla.branch = pol.branch
                                                                                        and cla.policy = pol.policy
                                                        JOIN        (select  distinct clh.claim
                                                                        from    (   select  cast(tcl.operation as varchar(2)) operation
                                                                                        from    usinsug01.tab_cl_ope tcl
                                                                                        where   (tcl.reserve = 1 or tcl.ajustes = 1)) tcl --solo reservas y ajustes
                                                                        join     usinsuv01.claim_his clh
                                                                        on   coalesce (clh.claim,0) > 0
                                                                        and     trim(clh.oper_type) = tcl.operation
                                                                        and     clh.operdate >= '12/31/2018') clh
                                                                on   clh.claim = cla.claim limit 10) cla
                                                        join usinsuv01.claim_his clh on clh.claim = cla.claim
                                                        join  (   select  case    when tcl.reserve = 1 then 1
                                                                                when tcl.ajustes = 1 then 2
                                                                                when tcl.pay_amount = 1 then 3
                                                                                else 0 end tipo,
                                                                        cast(tcl.operation as varchar(2)) operation
                                                                from    usinsug01.tab_cl_ope tcl
                                                                where   (tcl.reserve = 1 or tcl.ajustes = 1)) tcl --solo reservas y ajustes
                                                on    trim(clh.oper_type) = tcl.operation
                                                and     clh.operdate <= '12/31/2023'
                                                limit 10) clh
                                        join 	usinsuv01.cl_m_cover clm
                                        on	    clm.usercomp = clh.usercomp
                                        and 	clm.company = clh.company
                                        and 	clm.claim = clh.claim
                                        and 	clm.movement = clh.transac
                                        --2.556s dev 3
                                ) AS TMP
                                '''
        
        L_DF_SBHISPR_INSUNIX_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBHISPR_INSUNIX_LPV).load()

        L_DF_SBHISPR_INSUNIX = L_DF_SBHISPR_INSUNIX_LPG.union(L_DF_SBHISPR_INSUNIX_LPV)
        
        L_SBHISPR_VTIME_LPG = '''
                                (
                                   select       'D' as INDDETREC,
                                                'SBHISPR' as TABLAIFRS17,
                                                '' as PK,
                                                '' as DTPREG,
                                                '' as TIOCPROC,
                                                cast (clh."DOPERDATE" as date) as TIOCFRM,
                                                '' as TIOCTO,
                                                'PVG' as KGIORIGM,
                                                clh."NCLAIM"  as  "KSBSIN",
                                                cast (clh."NCLAIM" as VARCHAR ) as DNUMSIN,
                                                cast (clh."NPOLICY"  as VARCHAR ) as DNUMAPO,
                                                cast (clh."NCERTIF" as VARCHAR ) as DNMCERT,
                                                cast (clm."NCOVER"as VARCHAR ) as  KGCTPCBT,
                                                cast (clh."DOPERDATE" as DATE) as TPROVI,
                                                clh."NTRANSAC"  as DSEQMOV,
                                                cast (clh."NOPER_TYPE" as VARCHAR) as KSCTPMPR,
                                                cast (coalesce  (case    when    clh.TIPO = 1
                                                        then    case	when	clh.MONEDA_COD = 1
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
                                                                        else	0 end
                                                        else  0 end, 0) as varchar) as VMTPROVI,
                                                cast ( coalesce (case    when    clh.TIPO = 2
                                                        then    case	when	clh.MONEDA_COD = 1
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
                                                                        else	0 end
                                                        else  0 end,0) as varchar) as VMTPRVAR,
                                                '' as TULTALT,
                                                '' as DUSRUPD,
                                                'LPG' as DCOMPA,
                                                '' as DMARCA,
                                                '' as KSBSUBSN,
                                                '' as KSCMTDPR,
                                                clh.SBENE_CODE as KEBENTID_SN,
                                                '' as DENTIDSO,
                                                '' as DIDADELES,
                                                '' as TCRIACAO
                                        from    (   select  cla.*,
                                                        clh."NCASE_NUM",
                                                        clh."NDEMAN_TYPE",
                                                        clh."NTRANSAC",
                                                        clh."NOPER_TYPE",
                                                        clh."DOPERDATE",
                                                        coalesce(	coalesce(	coalesce((	select  max("SCLIENT")
                                                                                                from    usvtimg01."CLAIMBENEF" 
                                                                                                where   "NCLAIM" = clh."NCLAIM"
                                                                                                and 	"NCASE_NUM" = clh."NCASE_NUM"
                                                                                                and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                                and 	"NBENE_TYPE" = 2),
                                                                                        (	select  max("SCLIENT")
                                                                                                from    usvtimg01."CLAIMBENEF" 
                                                                                                where   "NCLAIM" = clh."NCLAIM"
                                                                                                and 	"NCASE_NUM" = clh."NCASE_NUM"
                                                                                                and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                                and 	"NBENE_TYPE" = 7)),
                                                                                        (	select  max("SCLIENT")
                                                                                        from    usvtimg01."CLAIMBENEF" 
                                                                                        where   "NCLAIM" = clh."NCLAIM"
                                                                                        and 	"NCASE_NUM" = clh."NCASE_NUM"
                                                                                        and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                        and 	"NBENE_TYPE" = 14)),
                                                                                coalesce((	select  max("SCLIENT")
                                                                                        from    usvtimg01."CLAIMBENEF" 
                                                                                        where   "NCLAIM" = clh."NCLAIM"
                                                                                        and 	"NCASE_NUM" = clh."NCASE_NUM"
                                                                                        and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                        and 	"NBENE_TYPE" = 119),
                                                                                        (	select  max("SCLIENT")
                                                                                        from    usvtimg01."CLAIMBENEF" 
                                                                                        where   "NCLAIM" = clh."NCLAIM"
                                                                                        and 	"NCASE_NUM" = clh."NCASE_NUM"
                                                                                        and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                        and 	"NBENE_TYPE" = 118))) SBENE_CODE,
                                                        csv.TIPO
                                                from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                then    cla."NCLAIM"
                                                                                else    null end "NCLAIM",
                                                                        cla."NPOLICY",
                                                                        cla."NCERTIF",
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
                                                                                        where	"NCONDITION" in (71,72)) csv --solo reservas y ajustes
                                                                        join     usvtimg01."CLAIM_HIS" clh
                                                                        ON   coalesce (clh."NCLAIM",0) > 0
                                                                        and     clh."NOPER_TYPE" = csv."SVALUE"
                                                                        and     clh."DOPERDATE" >= '12/31/2018') clh
                                                                on   clh."NCLAIM" = cla."NCLAIM") cla
                                                        join usvtimg01."CLAIM_HIS" clh on 	clh."NCLAIM" = cla."NCLAIM"
                                                        join (	select	case 	when	"NCONDITION" = 71 then 1
                                                                                when	"NCONDITION" = 72 then 2
                                                                                when	"NCONDITION" = 73 then 3
                                                                                else	0 end TIPO,
                                                                        cast("SVALUE" as INT4) "SVALUE"
                                                                from	usvtimg01."CONDITION_SERV" cs 
                                                                where	"NCONDITION" in (71,72)) csv --solo reservas y ajustes
                                                on    clh."NOPER_TYPE" = csv."SVALUE" ) clh
                                        join usvtimg01."CL_M_COVER" clm
                                        ON	clm."NCLAIM" = clh."NCLAIM"
                                        and 	clm."NCASE_NUM" = clh."NCASE_NUM"
                                        and 	clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                        and 	clm."NTRANSAC" = clh."NTRANSAC"
                                        --2.638s dev 3
                                ) AS TMP        
                                '''  
        
        L_DF_SBHISPR_VTIME_LPG = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBHISPR_VTIME_LPG).load()

        L_SBHISPR_VTIME_LPV = '''
                                (
                                   select       'D' as INDDETREC,
                                                'SBHISPR' as TABLAIFRS17,
                                                '' as  PK,
                                                '' as DTPREG,
                                                '' as  TIOCPROC,
                                                cast (clh."DOPERDATE" as date) as TIOCFRM,
                                                '' as TIOCTO,
                                                'PVV' as KGIORIGM,
                                                clh."NCLAIM"  as KSBSIN,
                                                cast (clh."NCLAIM" as VARCHAR )as  DNUMSIN,
                                                cast (clh."NPOLICY"as VARCHAR )as DNUMAPO,
                                                cast (clh."NCERTIF" as VARCHAR ) as  DNMCERT,
                                                cast ( clm."NCOVER" as VARCHAR ) as  KGCTPCBT,
                                                cast ( clh."DOPERDATE" as date) as  TPROVI,
                                                clh."NTRANSAC" as DSEQMOV,
                                                cast (clh."NOPER_TYPE"  as varchar ) as KSCTPMPR ,
                                                cast ( coalesce (case    when    clh.TIPO = 1
                                                        then    case	when	clh.MONEDA_COD = 1
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
                                                                        else	0 end
                                                        else  0 end, 0)as varchar ) as VMTPROVI,
                                                cast (coalesce ( case    when    clh.TIPO = 2
                                                        then    case	when	clh.MONEDA_COD = 1
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
                                                                        else	0 end
                                                        else  0 end, 0 ) as varchar ) as VMTPRVAR,
                                                '' as TULTALT,
                                                '' as DUSRUPD,
                                                'LPV'as DCOMPA,
                                                '' as DMARCA,
                                                '' as KSBSUBSN,
                                                '' as KSCMTDPR,
                                                clh.SBENE_CODE as KEBENTID_SN,
                                                '' as DENTIDSO,
                                                '' as DIDADELES,
                                                '' as TCRIACAO 
                                        from    (   select  cla.*,
                                                        clh."NCASE_NUM",
                                                        clh."NDEMAN_TYPE",
                                                        clh."NTRANSAC",
                                                        clh."NOPER_TYPE",
                                                        clh."DOPERDATE",
                                                        coalesce(	coalesce(	coalesce((	select  max("SCLIENT")
                                                                                                from    usvtimv01."CLAIMBENEF" 
                                                                                                where   "NCLAIM" = clh."NCLAIM"
                                                                                                and 	"NCASE_NUM" = clh."NCASE_NUM"
                                                                                                and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                                and 	"NBENE_TYPE" = 2),
                                                                                        (	select  max("SCLIENT")
                                                                                                from    usvtimv01."CLAIMBENEF" 
                                                                                                where   "NCLAIM" = clh."NCLAIM"
                                                                                                and 	"NCASE_NUM" = clh."NCASE_NUM"
                                                                                                and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                                and 	"NBENE_TYPE" = 7)),
                                                                                        (	select  max("SCLIENT")
                                                                                        from    usvtimv01."CLAIMBENEF" 
                                                                                        where   "NCLAIM" = clh."NCLAIM"
                                                                                        and 	"NCASE_NUM" = clh."NCASE_NUM"
                                                                                        and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                        and 	"NBENE_TYPE" = 14)),
                                                                                coalesce((	select  max("SCLIENT")
                                                                                        from    usvtimv01."CLAIMBENEF" 
                                                                                        where   "NCLAIM" = clh."NCLAIM"
                                                                                        and 	"NCASE_NUM" = clh."NCASE_NUM"
                                                                                        and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                        and 	"NBENE_TYPE" = 119),
                                                                                        (	select  max("SCLIENT")
                                                                                        from    usvtimv01."CLAIMBENEF" 
                                                                                        where   "NCLAIM" = clh."NCLAIM"
                                                                                        and 	"NCASE_NUM" = clh."NCASE_NUM"
                                                                                        and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                        and 	"NBENE_TYPE" = 118))) SBENE_CODE,
                                                        csv.TIPO
                                                from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                then    cla."NCLAIM"
                                                                                else    null end "NCLAIM",
                                                                        cla."NPOLICY",
                                                                        cla."NCERTIF",
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
                                                                join    usvtimv01."CLAIM" cla on cla."SCERTYPE" = pol."SCERTYPE"
                                                                                        and  cla."NPOLICY" = pol."NPOLICY"
                                                                                        and  cla."NBRANCH" = pol."NBRANCH"
                                                                join    (   select  distinct clh."NCLAIM"
                                                                        from    (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                        from	usvtimv01."CONDITION_SERV" cs 
                                                                                        where	"NCONDITION" in (71,72)) csv --solo reservas y ajustes
                                                                        join        usvtimv01."CLAIM_HIS" clh
                                                                        ON   coalesce (clh."NCLAIM",0) > 0
                                                                        and     clh."NOPER_TYPE" = csv."SVALUE"
                                                                        and     clh."DOPERDATE" >= '12/31/2018') clh
                                                        on    clh."NCLAIM" = cla."NCLAIM") cla
                                                        join usvtimv01."CLAIM_HIS" clh on clh."NCLAIM" = cla."NCLAIM"
                                                        join (	select	case 	when	"NCONDITION" = 71 then 1
                                                                                when	"NCONDITION" = 72 then 2
                                                                                when	"NCONDITION" = 73 then 3
                                                                                else	0 end TIPO,
                                                                        cast("SVALUE" as INT4) "SVALUE"
                                                                from	usvtimv01."CONDITION_SERV" cs 
                                                                where	"NCONDITION" in (71,72)) csv --solo reservas y ajustes
                                                on  	 clh."NOPER_TYPE" = csv."SVALUE"
                                                and     clh."DOPERDATE" <= '12/31/2023') clh
                                        join    usvtimv01."CL_M_COVER" clm
                                        ON	clm."NCLAIM" = clh."NCLAIM"
                                        and 	clm."NCASE_NUM" = clh."NCASE_NUM"
                                        and 	clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                        and 	clm."NTRANSAC" = clh."NTRANSAC"
                                        --328ms (todos)     
                                ) AS TMP        
                                '''  
        
        L_DF_SBHISPR_VTIME_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBHISPR_VTIME_LPV).load()
        
        L_DF_SBHISPR_VTIME =    L_DF_SBHISPR_VTIME_LPG.union(L_DF_SBHISPR_VTIME_LPV) 

        L_SBHISPR_INSIS =  '''
                                (
                                   select	'D' as INDDETREC,
                                                'SBHISPR' as TABLAIFRS17,
                                                ''  PK,
                                                '' DTPREG,
                                                '' TIOCPROC,
                                                cast (cl0."REGISTRATION_DATE" as varchar ) as  TIOCFRM,
                                                '' TIOCTO,
                                                'PNV' KGIORIGM,
                                                cla."CLAIM_REGID"  KSBSIN,
                                                cla."CLAIM_REGID" DNUMSIN,
                                                coalesce (case 	when	cl0.pep_id is not null
                                                                then	(select "POLICY_NO" from usinsiv01."POLICY" where "POLICY_ID" = cl0.pep_id)
                                                                else	pol."POLICY_NO" end, '') DNUMAPO,
                                                case 	when	cl0.pep_id is not null
                                                                then	pol."POLICY_NO"
                                                                else	'' end DNMCERT,
                                                coalesce (cl0."COVER_TYPE", '') KGCTPCBT,
                                                cast (cl0."REGISTRATION_DATE" as varchar ) TPROVI,
                                                cl0."RESERV_SEQ" DSEQMOV,
                                                cl0."OP_TYPE" KSCTPMPR,
                                                cast (cl0.reserva as varchar ) VMTPROVI,
                                                cast ( cl0.ajustes as varchar ) VMTPRVAR,
                                                '' TULTALT,
                                                '' DUSRUPD,
                                                'LPV' DCOMPA,
                                                '' DMARCA,
                                                '' KSBSUBSN,
                                                '' KSCMTDPR,
                                                cast ( coalesce ( cl0.rol_a_man_id,0)  as varchar ) KEBENTID_SN,
                                                '' DENTIDSO,
                                                '' DIDADELES,
                                                '' TCRIACAO
                                        from	(	select	cla.cla_id,
                                                                                cla.pol_id,
                                                                                cla.pep_id,
                                                                                cla.moneda_cod,
                                                                                cla.ncapital,
                                                                                cla.rol_a_man_id,
                                                                                clo."COVER_TYPE",
                                                                                clh."RESERV_SEQ",
                                                                                clh."OP_TYPE",
                                                                                cast(clh."REGISTRATION_DATE" as date) "REGISTRATION_DATE",
                                                                                case when clh."OP_TYPE" in ('REG') then "RESERV_CHANGE" else 0 end reserva, --REV.
                                                                                case when clh."OP_TYPE" in ('EST','CLC') then "RESERV_CHANGE" else 0 end ajustes, --REV.
                                                                                case when clh."OP_TYPE" in ('PAYMCONF','PAYMINV') then "RESERV_CHANGE" else 0 end pagos --REV.
                                                                from	(	select	cla."CLAIM_ID",
                                                                                                        (select "MASTER_POLICY_ID" from usinsiv01."POLICY_ENG_POLICIES" where "POLICY_ID" = CLA."POLICY_ID") pep_id,
                                                                                                        cla.ctid cla_id,
                                                                                                        pol.ctid pol_id,
                                                                                                        case	coalesce((	select	distinct "AV_CURRENCY"
                                                                                                                                                from	usinsiv01."INSURED_OBJECT"
                                                                                                                                                where	"POLICY_ID" = pol."POLICY_ID" limit 1),'')
                                                                                                                        when 'USD' then 2
                                                                                                                        when 'PEN' then 1
                                                                                                                        else 0 end MONEDA_COD,
                                                                                                        0 ncapital,
                                        /*
                                                                                cast(coalesce((  select  max("INSURED_VALUE")
                                                                                                from    "GEN_RISK_COVERED" grc
                                                                                                where   grc."POLICY_ID" = pol."POLICY_ID"
                                                                                                and     cast(cla."EVENT_DATE" as date)
                                                                                                                between cast(grc."INSR_BEGIN" as date) and cast(grc."INSR_END" as date)),0) as float) ncapital
                                        */
                                                                                coalesce(
                                                                                        (
                                                                                                select  min(acc."MAN_ID")
                                                                                                from    usinsiv01."INSURED_OBJECT" obj--Io
                                                                                                JOIN    usinsiv01."O_ACCINSURED" acc on acc."OBJECT_ID" = obj."OBJECT_ID"--Oa
                                                                                                join    usinsiv01."P_PEOPLE" peo on  peo."MAN_ID" = acc."MAN_ID"--pio
                                                                                                where   obj."POLICY_ID" = cla."POLICY_ID"
                                                                                                and     cast(cla."EVENT_DATE" as date) between cast(obj."INSR_BEGIN" as date) and cast(coalesce(obj."INSR_END",'12/31/9999') as date)  
                                                                                                and     coalesce(acc."ACCINS_TYPE",'0') = 
                                                                                                        case obj."INSR_TYPE" 
                                                                                                                when 2007 then '0' 
                                                                                                                when 2004 then '0' 
                                                                                                                when 2001 then '0' 
                                                                                                                else '1' 
                                                                                                        end   
                                                                                                                        ),
                                                                                                (  
                                                                                                select  min(acc."MAN_ID")
                                                                                                from    usinsiv01."INSURED_OBJECT" obj--Io
                                                                                                join    usinsiv01."O_ACCINSURED" acc on acc."OBJECT_ID" = obj."OBJECT_ID" --Oa
                                                                                                join    usinsiv01."P_PEOPLE" peo on peo."MAN_ID" = acc."MAN_ID" --pio
                                                                                                where   obj."POLICY_ID" = cla."POLICY_ID"
                                                                                                and		cast(cla."EVENT_DATE" as date) between cast(obj."INSR_BEGIN" as date) and cast(coalesce(obj."INSR_END",'12/31/9999') as date)    
                                                                                                and     coalesce(acc."ACCINS_TYPE",'0') = 
                                                                                                        case obj."INSR_TYPE" 
                                                                                                                when 2007 then '0' 
                                                                                                                when 2004 then '0' 
                                                                                                                when 2001 then '0' 
                                                                                                                else '1' 
                                                                                                        end
                                                                                                )
                                                                                                ) rol_a_man_id
                                                                                        from	(	select	distinct "CLAIM_ID"
                                                                                                                from	usinsiv01."CLAIM_RESERVE_HISTORY"
                                                                                                                where	/*"CLAIM_ID" = 20700000027 --20700000028 20700000051
                                                                                                                and		*/"OP_TYPE" IN ('REG','EST','CLC')
                                                                                                                and		cast("REGISTRATION_DATE" as date) >= '12-31-2018') clh
                                                                                        join 	usinsiv01."CLAIM" cla on cla."CLAIM_ID" = clh."CLAIM_ID"
                                                                                        join 	usinsiv01."POLICY" pol on pol."POLICY_ID" = cla."POLICY_ID"
                                                                                        ) cla
                                                                join usinsiv01."CLAIM_OBJECTS" clo on clo."CLAIM_ID" = cla."CLAIM_ID"
                                                                join usinsiv01."CLAIM_RESERVE_HISTORY" clh 	on clh."CLAIM_ID" = clo."CLAIM_ID"	
                                                                                                                                                        and		clh."REQUEST_ID" = clo."REQUEST_ID"
                                                                                                                                                        and		clh."OP_TYPE" IN ('REG','EST','CLC')
                                                                                                                                                        and		cast(clh."REGISTRATION_DATE" as date) <= '12-31-2023'/*
                                                                group	by 1,2,3,4,5,6*/) cl0
                                        join usinsiv01."CLAIM" cla on cla.ctid = cl0.cla_id
                                        join usinsiv01."POLICY" pol on pol.ctid = cl0.pol_id  
                                ) AS TMP    
                                ''' 

        L_DF_SBHISPR_INSIS = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_SBHISPR_INSIS).load()
        
        
        L_DF_SBHISPR = L_DF_SBHISPR_INSUNIX.union(L_DF_SBHISPR_VTIME).union(L_DF_SBHISPR_INSIS)

        return L_DF_SBHISPR 