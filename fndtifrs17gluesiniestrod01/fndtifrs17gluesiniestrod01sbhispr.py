def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):

    L_SBHISPR_INSUNIX = f'''
                            (
                              (
                                -----------------LPG INSUNIX
                                select  'D' as INDDETREC,
                                        'SBHISPR' as TABLAIFRS17,
                                        ''/*par.cia || par.sep || clh.claim || par.sep || clh.transac || par.sep || coalesce(clm.cover,0)*/as PK,
                                        '' as DTPREG,
                                        '' as TIOCPROC,
                                        '' as TIOCFRM,
                                        '' as TIOCTO,
                                        'PIG' as KGIORIGM,
                                        clh.claim || '-' || 'LPG' as KSBSIN,
                                        cast (clh.claim as varchar) as  DNUMSIN,
                                        cast (clh.policy as varchar )  as  DNUMAPO,
                                        cast (clh.certif  as varchar )as DNMCERT,
                                        cast (clm.cover as varchar) as KGCTPCBT,
                                        cast (clh.operdate as varchar ) as TPROVI,
                                        '' as DSEQMOV,
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
                                            from    (   select  case    when    (cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                        then    cla.claim
                                                                        else    null end claim,
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
                                                                                                    (   select  max(cpl.currency)
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
                                                                    on   coalesce (clh.claim,0) = 34724785 --> 0
                                                                    and     trim(clh.oper_type) = tcl.operation
                                                                    and     clh.operdate >= '12/31/2021') clh
                                                        on clh.claim = cla.claim) cla
                                                    join usinsug01.claim_his clh on clh.claim = cla.claim
                                                    join ( select  case    when tcl.reserve = 1 then 1
                                                                        when tcl.ajustes = 1 then 2
                                                                        when tcl.pay_amount = 1 then 3
                                                                        else 0 end tipo,
                                                                cast(tcl.operation as varchar(2)) operation
                                                        from    usinsug01.tab_cl_ope tcl
                                                        where   (tcl.reserve = 1 or tcl.ajustes = 1)) tcl --solo reservas y ajustes
                                            on      trim(clh.oper_type) = tcl.operation
                                            and     clh.operdate <= '12/31/2023') clh
                                join 	usinsug01.cl_m_cover clm 	on 	clm.usercomp = clh.usercomp
                                                                    and clm.company = clh.company
                                                                    and clm.claim = clh.claim
                                                                    and clm.movement = clh.transac
                                )
                                union all 
                                (
                                -------------------LPV 

                                select	'D' as INDDETREC,
                                        'SBHISPR' as TABLAIFRS17,
                                        /*par.cia || par.sep || clh.claim || par.sep || clh.transac || par.sep || coalesce(clm.cover,0)*/ '' as  PK,
                                        '' as DTPREG,
                                        '' as TIOCPROC,
                                        '' as TIOCFRM,
                                        '' as TIOCTO,
                                        'PIV' as KGIORIGM,
                                        clh.claim || '-' || 'LPV' as KSBSIN,
                                        cast (clh.claim as VARCHAR ) as  DNUMSIN,
                                        cast (clh.policy as VARCHAR ) as  DNUMAPO,
                                        cast (clh.certif as VARCHAR ) as  DNMCERT,
                                        cast ( clm.cover as VARCHAR )  as KGCTPCBT,
                                        cast ( clh.operdate as VARCHAR ) as  TPROVI,
                                        '' as DSEQMOV,
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
                                                                    and     clh.operdate >= '12/31/2021') clh
                                                        on   clh.claim = cla.claim) cla
                                                    join usinsuv01.claim_his clh on clh.claim = cla.claim
                                                    join  (   select  case    when tcl.reserve = 1 then 1
                                                                        when tcl.ajustes = 1 then 2
                                                                        when tcl.pay_amount = 1 then 3
                                                                        else 0 end tipo,
                                                                cast(tcl.operation as varchar(2)) operation
                                                        from    usinsug01.tab_cl_ope tcl
                                                        where   (tcl.reserve = 1 or tcl.ajustes = 1)) tcl --solo reservas y ajustes
                                            on    trim(clh.oper_type) = tcl.operation
                                            and     clh.operdate <= '12/31/2023') clh
                                join 	usinsuv01.cl_m_cover clm
                                on	    clm.usercomp = clh.usercomp
                                and 	clm.company = clh.company
                                and 	clm.claim = clh.claim
                                and 	clm.movement = clh.transac
                                )  
                            ) AS TMP
                            '''
     
    L_DF_SBHISPR_INSUNIX = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_SBHISPR_INSUNIX).load()

    L_SBHISPR_VTIME = f'''
                            (
                             
                                (
                                    ----------------VTIME LPG
                                    select 'D' as INDDETREC,
                                            'SBHISPR' as TABLAIFRS17,
                                            /*par.cia || par.sep1 || clh."NCLAIM" || par.sep2 || clh."NCASE_NUM" || par.sep1 || clh."NTRANSAC" || par.sep1 || coalesce(clm."NCOVER",0) */  '' as PK,
                                            '' as DTPREG,
                                            '' as TIOCPROC,
                                            '' as TIOCFRM,
                                            '' as TIOCTO,
                                            'PVG' as KGIORIGM,
                                            clh."NCLAIM" || '-' || 'LPG' as  "KSBSIN",
                                            cast (clh."NCLAIM" as VARCHAR ) as DNUMSIN,
                                            cast (clh."NPOLICY"  as VARCHAR ) as DNUMAPO,
                                            cast (clh."NCERTIF" as VARCHAR ) as DNMCERT,
                                            cast (clm."NCOVER"as VARCHAR ) as  KGCTPCBT,
                                            cast (clh."DOPERDATE" as DATE) as TPROVI,
                                            '' as DSEQMOV,
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
                                                                        and     clh."DOPERDATE" >= '12/31/2021') clh
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
                                    )
                                    union all 
                                    (
                                    ------------------LPV VTIME
                                    select  'D' as INDDETREC,
                                            'SBHISPR' as TABLAIFRS17,
                                            /*par.cia || par.sep1 || clh."NCLAIM" || par.sep2 || clh."NCASE_NUM" || par.sep1 || clh."NTRANSAC" || par.sep1 || coalesce(clm."NCOVER",0)*/ '' as  PK,
                                            '' as DTPREG,
                                            '' as  TIOCPROC,
                                            '' as TIOCFRM,
                                            '' as TIOCTO,
                                            'PVV' as KGIORIGM,
                                            clh."NCLAIM" || '-'|| 'LPV'  as KSBSIN,
                                            cast (clh."NCLAIM" as VARCHAR )as  DNUMSIN,
                                            cast (clh."NPOLICY"as VARCHAR )as DNUMAPO,
                                            cast (clh."NCERTIF" as VARCHAR ) as  DNMCERT,
                                            cast ( clm."NCOVER" as VARCHAR ) as  KGCTPCBT,
                                            cast ( clh."DOPERDATE" as date) as  TPROVI,
                                            '' as DSEQMOV,
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
                                                                        and     clh."DOPERDATE" >= '12/31/2021') clh
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
                                    --1.880s (todos)
                                    )
                            ) AS TMP    
                            '''  
    L_DF_SBHISPR_VTIME = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_SBHISPR_VTIME).load()
  
    L_DF_SBHISPR = L_DF_SBHISPR_INSUNIX.union(L_DF_SBHISPR_VTIME)#.union(L_DF_ABPRCOB_INSIS)

    return L_DF_SBHISPR 